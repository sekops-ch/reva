// Copyright 2024-2026 Sekops Sarl
// Author: Bernard Gutermann <bernard.gutermann@sekops.ch>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kvfs

import (
	"context"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// gcLockTTL bounds how long one sweep can hold the cluster-wide lock
// before another pod can steal it. Long enough to cover a real sweep on
// a bucket the size of `oc-uploads`; not so long that a sweep stalled by
// a recently-killed pod blocks everyone for hours. Exposed as a variable
// so unit tests can override; production code never writes it.
var gcLockTTL = 30 * time.Minute

// GCResult holds the outcome of a single GC run.
type GCResult struct {
	BlobsScanned          int
	BlobsDeleted          int
	WouldDelete           int
	ChildrenReconciled    int // stale or duplicate oc-children entries removed
	TrashReconciled       int // stale oc-trash entries removed (node already restored)
	ExpiredUploadsCleaned int // sessions past expiry, or with no expiry and older than the session TTL
	CorruptUploadsReaped  int // unparseable upload entries older than the session TTL
	Errors                int
	Duration              time.Duration

	// Identity-orphan reaping (owner no longer a live user).
	OrphanPersonalSpacesDeleted int // dead-owner personal spaces reaped
	OrphanProjectSpaces         int // dead-owner project/virtual spaces SURFACED (not deleted)

	// Internal residue sweep (data whose space no longer exists in oc-spaces).
	ResidueKeysDeleted  int // KV entries of deleted spaces removed
	ResidueBlobsDeleted int // S3 blobs of deleted spaces removed
}

// blobGC runs periodic garbage collection that reconciles a 1:1:1
// correspondence between live spaces, their KV metadata, and their S3 blobs.
// Beyond reaping unreferenced blobs within live spaces, it also (a) reaps
// orphaned personal spaces whose owner is no longer a live user (via the
// injected resolver, reusing the standard DeleteStorageSpace path), and
// (b) sweeps residue — KV entries and S3 blobs whose owning space no longer
// exists in oc-spaces (crash-mid-delete / best-effort delete failures).
type blobGC struct {
	store    MetadataStore
	blob     BlobStore
	opts     *Options
	log      *zerolog.Logger
	stopCh   chan struct{}
	holderID string

	// resolver answers which space owners are still live users. Nil disables
	// identity reaping (e.g. the storage-system instance, which has no
	// personal spaces); the residue sweep still runs. See resolver.go.
	resolver UserResolver

	// reapSpace deletes a space's contents + entry through the SAME crash-safe
	// path as DeleteStorageSpace (deleteSpaceContents + DeleteSpace under a
	// detached commit context). Injected by the driver in New() so the GC has
	// exactly one deletion implementation to reuse. Nil in unit tests that do
	// not exercise reaping.
	reapSpace func(ctx context.Context, spaceID, rootID string) error
}

// newBlobGC creates a new GC instance.
func newBlobGC(store MetadataStore, blob BlobStore, opts *Options, log *zerolog.Logger) *blobGC {
	host, _ := os.Hostname()
	if host == "" {
		host = uuid.New().String()
	}
	return &blobGC{
		store:    store,
		blob:     blob,
		opts:     opts,
		log:      log,
		stopCh:   make(chan struct{}),
		holderID: host,
	}
}

// Start begins the GC background loop. It returns immediately.
func (gc *blobGC) Start() {
	if gc.opts.GCRunOnStart {
		go gc.runInitialSweepWithRetry()
	}

	go gc.loop()
}

// initialSweepRetryInterval is how long the initial sweep waits between
// attempts when the gc-sweep lock is held by another (possibly recently-
// killed) pod. Smaller than gcLockTTL so a stale lock from a killed pod
// can be reclaimed within minutes rather than the full sweep interval.
// Exposed as a variable so unit tests can override without polluting
// production behaviour.
var initialSweepRetryInterval = 5 * time.Minute

// runInitialSweepWithRetry executes the initial GC sweep, retrying with
// a bounded backoff if another pod currently holds the sweep lock. Without
// the retry, a stale lock left behind by a recently-killed pod would block
// the initial sweep, and the next attempt would not happen until
// GCIntervalDuration() (typically 24 h) later. Retries are capped at
// 2 × gcLockTTL so this goroutine cannot leak indefinitely on a
// genuinely contested lock.
func (gc *blobGC) runInitialSweepWithRetry() {
	gc.log.Info().Msg("gc: running initial sweep on start")
	deadline := time.Now().Add(2 * gcLockTTL)
	for {
		result := gc.Run(context.Background())
		if result.Duration > 0 {
			return // sweep actually ran (lock acquired, sweep completed)
		}
		if time.Now().After(deadline) {
			gc.log.Warn().
				Dur("waited", 2*gcLockTTL).
				Msg("gc: initial sweep retry deadline exceeded; deferring to the interval ticker")
			return
		}
		select {
		case <-gc.stopCh:
			return
		case <-time.After(initialSweepRetryInterval):
		}
	}
}

// Stop signals the GC loop to exit.
func (gc *blobGC) Stop() {
	close(gc.stopCh)
}

func (gc *blobGC) loop() {
	interval := gc.opts.GCIntervalDuration()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	gc.log.Info().
		Dur("interval", interval).
		Dur("min_age", gc.opts.GCMinAgeDuration()).
		Bool("dry_run", gc.opts.GCDryRun).
		Msg("gc: background loop started")

	for {
		select {
		case <-gc.stopCh:
			gc.log.Info().Msg("gc: background loop stopped")
			return
		case <-ticker.C:
			gc.Run(context.Background())
		}
	}
}

// Run executes a single GC sweep. Safe to call from tests directly.
func (gc *blobGC) Run(ctx context.Context) GCResult {
	acquired, err := gc.store.TryAcquireLock("gc-sweep", gc.holderID, gcLockTTL)
	if err != nil {
		gc.log.Warn().Err(err).Msg("gc: failed to acquire sweep lock")
		return GCResult{}
	}
	if !acquired {
		gc.log.Info().Msg("gc: another pod holds the sweep lock, skipping this cycle")
		return GCResult{}
	}
	defer gc.store.ReleaseLock("gc-sweep", gc.holderID)

	start := time.Now()
	result := GCResult{}

	gc.log.Debug().Bool("dry_run", gc.opts.GCDryRun).Msg("gc: sweep starting")

	finalize := func() {
		result.Duration = time.Since(start)
		GCRunDuration.Observe(result.Duration.Seconds())
	}

	// One raw walk serves the whole sweep: the reference set and residue
	// sweep use the parsed sessions, the upload reaper additionally needs
	// each entry's age and unparseable entries (which typed listings hide).
	uploadEntries, err := gc.store.ListAllUploadEntries()
	if err != nil {
		gc.log.Error().Err(err).Msg("gc: failed to list uploads, aborting sweep")
		GCErrors.Inc()
		result.Errors++
		finalize()
		return result
	}
	uploads := make([]*UploadSession, 0, len(uploadEntries))
	for _, e := range uploadEntries {
		if e.Session != nil {
			uploads = append(uploads, e.Session)
		}
	}

	spaces, err := gc.store.ListSpaces(nil)
	if err != nil {
		gc.log.Error().Err(err).Msg("gc: failed to list spaces")
		GCErrors.Inc()
		result.Errors++
		finalize()
		return result
	}

	minAge := gc.opts.GCMinAgeDuration()
	cutoff := time.Now().Add(-minAge)

	// (1) Identity reaping: delete orphaned personal spaces whose owner is no
	// longer a live user; surface orphaned project/virtual spaces for manual
	// review (never auto-deleted). This may remove entries from oc-spaces, so
	// we re-list afterwards to keep the residue sweep and per-space blob loop
	// operating on the post-reap live set.
	gc.reapIdentityOrphans(ctx, spaces, &result)

	spaces, err = gc.store.ListSpaces(nil)
	if err != nil {
		gc.log.Error().Err(err).Msg("gc: failed to re-list spaces after identity reaping")
		GCErrors.Inc()
		result.Errors++
		finalize()
		return result
	}

	liveSpaceIDs := make(map[string]struct{}, len(spaces))
	for _, sp := range spaces {
		liveSpaceIDs[sp.ID] = struct{}{}
	}

	// (2) Internal residue sweep: reap KV entries and S3 blobs whose owning
	// space no longer exists in oc-spaces (crash-mid-delete / best-effort
	// delete residue, including anything identity reaping above could not
	// finish). Pure internal consistency — no identity signal needed.
	gc.reconcileResidue(ctx, liveSpaceIDs, uploads, cutoff, &result)

	for _, space := range spaces {
		refs, err := gc.buildSpaceReferenceSet(space.ID, uploads)
		if err != nil {
			gc.log.Error().Err(err).Str("space_id", space.ID).Msg("gc: failed to build reference set for space, skipping")
			GCErrors.Inc()
			result.Errors++
			continue
		}

		gc.log.Debug().Str("space_id", space.ID).Int("referenced_blobs", len(refs)).Msg("gc: space reference set built")

		blobs, err := gc.blob.ListBlobs(ctx, space.ID+"/")
		if err != nil {
			gc.log.Error().Err(err).Str("space_id", space.ID).Msg("gc: failed to list blobs for space")
			GCErrors.Inc()
			result.Errors++
			continue
		}

		for _, blob := range blobs {
			result.BlobsScanned++
			GCBlobsScanned.Inc()

			if refs[blob.Key] {
				continue
			}

			if !blob.LastModified.IsZero() && blob.LastModified.After(cutoff) {
				gc.log.Debug().
					Str("key", blob.Key).
					Time("last_modified", blob.LastModified).
					Msg("gc: orphan too young, skipping")
				continue
			}

			if gc.opts.GCDryRun {
				result.WouldDelete++
				GCWouldDelete.Inc()
				gc.log.Info().
					Str("key", blob.Key).
					Int64("size", blob.Size).
					Time("last_modified", blob.LastModified).
					Msg("gc: would delete orphaned blob (dry-run)")
			} else {
				if err := gc.blob.Delete(ctx, blob.Key); err != nil {
					gc.log.Error().Err(err).Str("key", blob.Key).Msg("gc: failed to delete orphaned blob")
					GCErrors.Inc()
					result.Errors++
					continue
				}
				result.BlobsDeleted++
				GCBlobsDeleted.Inc()
				gc.log.Info().
					Str("key", blob.Key).
					Int64("size", blob.Size).
					Msg("gc: deleted orphaned blob")
			}
		}

		result.ChildrenReconciled += gc.reconcileChildren(ctx, space.ID)
		result.TrashReconciled += gc.reconcileTrash(ctx, space.ID)
	}

	gc.cleanExpiredUploads(ctx, uploadEntries, &result)

	// Compact upload tombstones once per sweep. DeleteUpload leaves a
	// marker per key; uploads churn orders of magnitude faster than any
	// other bucket, so without compaction the markers come to dominate
	// every WatchAll-based uploads listing (sampler, GC, per-space scans).
	if !gc.opts.GCDryRun {
		if err := gc.store.PurgeDeletedUploads(); err != nil {
			gc.log.Warn().Err(err).Msg("gc: failed to purge upload tombstones")
			GCErrors.Inc()
			result.Errors++
		}
	}

	finalize()

	gc.log.Info().
		Int("scanned", result.BlobsScanned).
		Int("deleted", result.BlobsDeleted).
		Int("would_delete", result.WouldDelete).
		Int("expired_uploads_cleaned", result.ExpiredUploadsCleaned).
		Int("corrupt_uploads_reaped", result.CorruptUploadsReaped).
		Int("children_reconciled", result.ChildrenReconciled).
		Int("trash_reconciled", result.TrashReconciled).
		Int("orphan_personal_spaces_deleted", result.OrphanPersonalSpacesDeleted).
		Int("orphan_project_spaces_surfaced", result.OrphanProjectSpaces).
		Int("residue_keys_deleted", result.ResidueKeysDeleted).
		Int("residue_blobs_deleted", result.ResidueBlobsDeleted).
		Int("errors", result.Errors).
		Dur("duration", result.Duration).
		Msg("gc: sweep completed")

	return result
}

// reapIdentityOrphans deletes spaces whose owner is no longer a live user.
// Personal spaces are reaped through the standard DeleteStorageSpace path
// (gc.reapSpace); orphaned project/virtual spaces are only SURFACED (logged +
// counted on a gauge) for manual review, never auto-deleted — a shared space
// outlives the deprovisioning of the single user who happened to create it.
//
// Safety: identity reaping is skipped entirely when the resolver is nil
// (e.g. storage-system) or when the resolver cannot determine liveness for
// the whole sweep (returns an error). Per-owner, only a *definitive* not-live
// answer (present in the map as false) triggers a reap; an owner absent from
// the map is "unknown" and is never reaped. This makes it impossible for a
// degraded identity backend to delete a live user's space.
func (gc *blobGC) reapIdentityOrphans(ctx context.Context, spaces []*SpaceEntry, result *GCResult) {
	if gc.resolver == nil {
		return // identity reaping disabled for this instance
	}

	owners := make([]string, 0, len(spaces))
	seen := make(map[string]struct{}, len(spaces))
	for _, sp := range spaces {
		if sp.Owner == "" {
			continue
		}
		if _, dup := seen[sp.Owner]; dup {
			continue
		}
		seen[sp.Owner] = struct{}{}
		owners = append(owners, sp.Owner)
	}

	liveness, err := gc.resolver.ResolveLiveness(ctx, owners)
	if err != nil {
		gc.log.Warn().Err(err).Msg("gc: identity reaping skipped (resolver unavailable) — no spaces reaped this cycle")
		GCIdentityReapingSkipped.Inc()
		return
	}

	projectOrphans := 0
	for _, sp := range spaces {
		live, determined := liveness[sp.Owner]
		if !determined || live {
			// Unknown owner (never reap) or live owner (keep).
			continue
		}

		// Owner is definitively gone.
		if sp.Type != "personal" {
			projectOrphans++
			gc.log.Warn().
				Str("space_id", sp.ID).
				Str("type", sp.Type).
				Str("owner", sp.Owner).
				Str("name", sp.Name).
				Msg("gc: orphaned non-personal space requires MANUAL review (owner gone; NOT deleted)")
			continue
		}

		if gc.opts.GCDryRun {
			result.WouldDelete++
			GCWouldDelete.Inc()
			gc.log.Info().
				Str("space_id", sp.ID).
				Str("owner", sp.Owner).
				Str("name", sp.Name).
				Msg("gc: would reap orphaned personal space (dry-run)")
			continue
		}

		if gc.reapSpace == nil {
			// No deletion path wired (unit context); cannot reap.
			continue
		}
		if err := gc.reapSpace(ctx, sp.ID, sp.RootID); err != nil {
			gc.log.Error().Err(err).Str("space_id", sp.ID).Msg("gc: failed to reap orphaned personal space")
			GCErrors.Inc()
			result.Errors++
			continue
		}
		result.OrphanPersonalSpacesDeleted++
		GCOrphanPersonalSpacesDeleted.Inc()
		gc.log.Info().
			Str("space_id", sp.ID).
			Str("owner", sp.Owner).
			Str("name", sp.Name).
			Msg("gc: reaped orphaned personal space (dead owner)")
	}

	result.OrphanProjectSpaces = projectOrphans
	GCOrphanProjectSpaces.Set(float64(projectOrphans))
}

// reconcileResidue reaps data whose owning space no longer exists in
// oc-spaces. This covers crash-mid-DeleteStorageSpace and best-effort delete
// failures (deleteSpaceContents discards per-item errors), plus anything the
// identity-reaping pass above could not finish. It needs NO identity signal —
// a node/blob whose space is gone is unambiguously stale.
//
// Memory discipline mirrors the rest of GC: it never enumerates the whole
// system. Ghost spaces are discovered cheaply — from the S3 top-level prefix
// list and the already-loaded uploads slice — and each ghost is then cleaned
// with per-space (bounded) reads, so peak memory stays proportional to the
// largest single ghost space, not the total node count.
func (gc *blobGC) reconcileResidue(ctx context.Context, liveSpaceIDs map[string]struct{}, uploads []*UploadSession, cutoff time.Time, result *GCResult) {
	isLive := func(spaceID string) bool {
		_, ok := liveSpaceIDs[spaceID]
		return ok
	}

	// Discover ghost space IDs cheaply: S3 prefixes (one delimiter list) plus
	// any in-flight upload whose space is gone (uploads are already in memory).
	ghosts := map[string]struct{}{}
	prefixes, err := gc.blob.ListSpacePrefixes(ctx)
	if err != nil {
		gc.log.Error().Err(err).Msg("gc: residue sweep failed to list S3 space prefixes")
		GCErrors.Inc()
		result.Errors++
	}
	for _, sid := range prefixes {
		if sid != "" && !isLive(sid) {
			ghosts[sid] = struct{}{}
		}
	}
	for _, u := range uploads {
		if u.SpaceID != "" && !isLive(u.SpaceID) {
			ghosts[u.SpaceID] = struct{}{}
		}
	}

	for sid := range ghosts {
		gc.reapGhostSpace(ctx, sid, cutoff, result)
	}
}

// reapGhostSpace removes all KV metadata and S3 blobs for a single space that
// no longer exists in oc-spaces. All reads are per-space (bounded). Safe
// without an age gate on KV entries because space creation writes the
// oc-spaces entry FIRST (PutSpace rev=0, atomic create-or-fail), so a
// missing space entry means the space was deleted, not mid-provision; the S3
// blob delete still honours the age cutoff as a second belt.
func (gc *blobGC) reapGhostSpace(ctx context.Context, spaceID string, cutoff time.Time, result *GCResult) {
	dryRun := gc.opts.GCDryRun

	delKey := func() {
		if dryRun {
			result.WouldDelete++
			GCWouldDelete.Inc()
			return
		}
		result.ResidueKeysDeleted++
		GCResidueKeysDeleted.Inc()
	}

	if nodes, err := gc.store.ListNodesBySpace(spaceID); err == nil {
		for _, n := range nodes {
			if !dryRun {
				gc.store.DeleteNode(n.SpaceID, n.ID)
				if n.Type == NodeTypeDir {
					gc.store.DeleteChildren(n.SpaceID, n.ID)
				}
			}
			delKey()
		}
	} else {
		gc.log.Warn().Err(err).Str("space_id", spaceID).Msg("gc: residue sweep failed to list nodes for ghost space")
	}
	if versions, err := gc.store.ListVersionsBySpace(spaceID); err == nil {
		for _, v := range versions {
			if !dryRun {
				gc.store.DeleteVersion(v.SpaceID, v.NodeID, v.Key)
			}
			delKey()
		}
	} else {
		gc.log.Warn().Err(err).Str("space_id", spaceID).Msg("gc: residue sweep failed to list versions for ghost space")
	}
	if trash, err := gc.store.ListTrash(spaceID); err == nil {
		for _, t := range trash {
			if !dryRun {
				gc.store.DeleteTrash(t.SpaceID, t.Key)
			}
			delKey()
		}
	} else {
		gc.log.Warn().Err(err).Str("space_id", spaceID).Msg("gc: residue sweep failed to list trash for ghost space")
	}
	if ups, err := gc.store.ListUploadsBySpace(spaceID); err == nil {
		for _, u := range ups {
			if !dryRun {
				if u.S3MultipartID != "" {
					gc.blob.AbortMultipartUpload(ctx, BlobKey(u.SpaceID, u.BlobID), u.S3MultipartID)
				}
				gc.store.DeleteUpload(u.ID)
			}
			delKey()
		}
	} else {
		gc.log.Warn().Err(err).Str("space_id", spaceID).Msg("gc: residue sweep failed to list uploads for ghost space")
	}

	// Bulk-reap every S3 object under the ghost prefix (age-gated). This
	// covers both node-referenced blobs and any blob whose node delete already
	// succeeded — the whole prefix belongs to a space that no longer exists.
	blobs, err := gc.blob.ListBlobs(ctx, spaceID+"/")
	if err != nil {
		gc.log.Error().Err(err).Str("space_id", spaceID).Msg("gc: residue sweep failed to list blobs for ghost space")
		GCErrors.Inc()
		result.Errors++
		return
	}
	for _, b := range blobs {
		if !b.LastModified.IsZero() && b.LastModified.After(cutoff) {
			continue // too young — never reap an in-flight provision
		}
		if dryRun {
			result.WouldDelete++
			GCWouldDelete.Inc()
			gc.log.Info().Str("key", b.Key).Msg("gc: would reap residual blob of deleted space (dry-run)")
			continue
		}
		if err := gc.blob.Delete(ctx, b.Key); err != nil {
			gc.log.Error().Err(err).Str("key", b.Key).Msg("gc: failed to delete residual blob")
			GCErrors.Inc()
			result.Errors++
			continue
		}
		result.ResidueBlobsDeleted++
		GCResidueBlobsDeleted.Inc()
	}
	if !dryRun && (result.ResidueKeysDeleted > 0 || result.ResidueBlobsDeleted > 0) {
		gc.log.Info().Str("space_id", spaceID).Msg("gc: reaped residue of a deleted space")
	}
}

// reconcileChildren walks oc-children for a single space and resolves any
// entry whose recorded child does not match the corresponding node's own
// ParentID. This catches the two interesting crash modes for Move (which is a
// 3-phase CAS sequence at kvfs.go: node, add-to-new, remove-from-old):
//
//  1. A child entry references a nodeID whose `node.ParentID` points at a
//     different directory (the "duplicate children" case the upstream review
//     on opencloud-eu/reva#625 raised).
//  2. A child entry references a nodeID that no longer exists at all (the
//     dangling-ref case).
//
// Resolution: the node's own ParentID is authoritative. Any children entry
// whose nodeID disagrees with truth is removed from that parent's children
// map. The node itself is left untouched.
//
// Best-effort interim safety net until a proper Move journal / WAL lands.
// Honors the GC dry-run flag.
func (gc *blobGC) reconcileChildren(ctx context.Context, spaceID string) int {
	nodes, err := gc.store.ListNodesBySpace(spaceID)
	if err != nil {
		gc.log.Warn().Err(err).Str("space_id", spaceID).Msg("gc: reconcileChildren: list nodes failed")
		return 0
	}

	// Truth table: nodeID -> authoritative ParentID.
	truth := make(map[string]string, len(nodes))
	parents := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		truth[n.ID] = n.ParentID
		if n.Type == NodeTypeDir {
			parents[n.ID] = struct{}{}
		}
	}

	reconciled := 0
	for parentID := range parents {
		children, rev, err := gc.store.GetChildren(spaceID, parentID)
		if err != nil {
			continue
		}
		dirty := false
		for name, childID := range children {
			actual, exists := truth[childID]
			if exists && actual == parentID {
				continue
			}
			// Either dangling (no node) or stale (node belongs elsewhere).
			reason := "stale"
			if !exists {
				reason = "dangling"
			}
			if gc.opts.GCDryRun {
				gc.log.Info().
					Str("space_id", spaceID).
					Str("parent_id", parentID).
					Str("child_id", childID).
					Str("name", name).
					Str("reason", reason).
					Str("actual_parent", actual).
					Msg("gc: would remove stray children entry (dry-run)")
				reconciled++
				GCChildrenReconciled.Inc()
				continue
			}
			delete(children, name)
			dirty = true
			reconciled++
			GCChildrenReconciled.Inc()
			gc.log.Info().
				Str("space_id", spaceID).
				Str("parent_id", parentID).
				Str("child_id", childID).
				Str("name", name).
				Str("reason", reason).
				Str("actual_parent", actual).
				Msg("gc: removed stray children entry")
		}
		if dirty {
			if err := gc.store.PutChildren(spaceID, parentID, children, rev); err != nil {
				gc.log.Warn().Err(err).
					Str("space_id", spaceID).
					Str("parent_id", parentID).
					Msg("gc: reconcileChildren: PutChildren failed; skipping this parent for the cycle")
			}
		}
	}

	return reconciled
}

// reconcileTrash walks oc-trash for a single space and removes any trash
// entry whose referenced node is already live AND reachable in the tree.
// This catches the RestoreRecycleItem crash mode: PutNode + updateChildren
// succeeded but DeleteTrash did not, leaving the node both restored AND
// in trash.
//
// KVFS keeps directory nodes alive in oc-nodes even while trashed (for
// subtree restorability). A trashed directory has a node entry but is NOT
// listed in any parent's children map. A restored directory IS reachable.
// The reachability check distinguishes the two cases.
//
// Resolution: the node is live and reachable; the trash entry is stale.
// Honors the GC dry-run flag.
func (gc *blobGC) reconcileTrash(ctx context.Context, spaceID string) int {
	nodes, err := gc.store.ListNodesBySpace(spaceID)
	if err != nil {
		gc.log.Warn().Err(err).Str("space_id", spaceID).Msg("gc: reconcileTrash: list nodes failed")
		return 0
	}

	liveNodes := make(map[string]*NodeEntry, len(nodes))
	for _, n := range nodes {
		liveNodes[n.ID] = n
	}

	// Build the set of node IDs reachable from some parent's children map.
	reachable := make(map[string]struct{})
	for _, n := range nodes {
		if n.Type != NodeTypeDir {
			continue
		}
		children, _, err := gc.store.GetChildren(spaceID, n.ID)
		if err != nil {
			continue
		}
		for _, childID := range children {
			reachable[childID] = struct{}{}
		}
	}

	trash, err := gc.store.ListTrash(spaceID)
	if err != nil {
		gc.log.Warn().Err(err).Str("space_id", spaceID).Msg("gc: reconcileTrash: list trash failed")
		return 0
	}

	reconciled := 0
	for _, t := range trash {
		node, isLive := liveNodes[t.NodeID]
		if !isLive {
			continue
		}
		if _, ok := reachable[t.NodeID]; !ok {
			continue
		}
		// Node is live AND reachable — trash entry is stale.
		if gc.opts.GCDryRun {
			gc.log.Info().
				Str("space_id", spaceID).
				Str("trash_key", t.Key).
				Str("node_id", t.NodeID).
				Str("parent_id", node.ParentID).
				Msg("gc: would remove stale trash entry (node is live and reachable) (dry-run)")
			reconciled++
			GCTrashReconciled.Inc()
			continue
		}
		if err := gc.store.DeleteTrash(spaceID, t.Key); err != nil {
			gc.log.Warn().Err(err).
				Str("space_id", spaceID).
				Str("trash_key", t.Key).
				Msg("gc: reconcileTrash: DeleteTrash failed; skipping this entry")
			continue
		}
		reconciled++
		GCTrashReconciled.Inc()
		gc.log.Info().
			Str("space_id", spaceID).
			Str("trash_key", t.Key).
			Str("node_id", t.NodeID).
			Str("parent_id", node.ParentID).
			Msg("gc: removed stale trash entry (node already restored)")
	}

	return reconciled
}

// buildSpaceReferenceSet builds the set of S3 blob keys referenced by a
// single space's metadata (nodes, versions, trash) plus any in-flight
// uploads for that space. Called once per space during GC to keep peak
// memory proportional to the largest space, not the total system.
func (gc *blobGC) buildSpaceReferenceSet(spaceID string, uploads []*UploadSession) (map[string]bool, error) {
	refs := make(map[string]bool)

	nodes, err := gc.store.ListNodesBySpace(spaceID)
	if err != nil {
		return nil, err
	}
	for _, n := range nodes {
		if n.BlobID != "" {
			refs[BlobKey(n.SpaceID, n.BlobID)] = true
		}
	}

	versions, err := gc.store.ListVersionsBySpace(spaceID)
	if err != nil {
		return nil, err
	}
	for _, v := range versions {
		if v.BlobID != "" {
			refs[BlobKey(v.SpaceID, v.BlobID)] = true
		}
	}

	for _, u := range uploads {
		if u.SpaceID == spaceID && u.BlobID != "" {
			refs[BlobKey(u.SpaceID, u.BlobID)] = true
		}
	}

	trashEntries, err := gc.store.ListTrash(spaceID)
	if err != nil {
		return nil, err
	}
	for _, t := range trashEntries {
		if t.Node.BlobID != "" {
			refs[BlobKey(t.SpaceID, t.Node.BlobID)] = true
		}
	}

	return refs, nil
}

// cleanExpiredUploads reaps dead upload sessions and aborts any associated
// S3 multipart uploads. Three classes are reaped:
//   - expired:   the session is past its Expires stamp;
//   - no-expiry: the session has no Expires stamp (legacy/partial write)
//     and its KV revision is older than the session TTL;
//   - corrupt:   the stored value no longer unmarshals and its KV revision
//     is older than the session TTL.
//
// The latter two are age-gated on the server-side Created timestamp so a
// fresh in-flight session can never be reaped; a zero Created never reaps.
// Staged upload bodies need no explicit cleanup here: the NATS staging
// stream expires them via MaxAge (default equals the session TTL — keep
// the two in lockstep) and disk staging is pod-local emptyDir.
func (gc *blobGC) cleanExpiredUploads(ctx context.Context, entries []*UploadEntry, result *GCResult) {
	now := time.Now()
	nowUnix := now.Unix()
	ttlCutoff := now.Add(-uploadSessionTTL)

	for _, e := range entries {
		var reason string
		switch {
		case e.Session != nil && e.Session.Expires > 0 && e.Session.Expires < nowUnix:
			reason = "expired"
		case e.Session != nil && e.Session.Expires == 0 && !e.Created.IsZero() && e.Created.Before(ttlCutoff):
			reason = "no-expiry"
		case e.Session == nil && !e.Created.IsZero() && e.Created.Before(ttlCutoff):
			reason = "corrupt"
		default:
			continue
		}

		if gc.opts.GCDryRun {
			result.WouldDelete++
			GCWouldDelete.Inc()
			gc.log.Info().Str("session_id", e.Key).Str("reason", reason).Msg("gc: would reap upload session (dry-run)")
			continue
		}

		if e.Session != nil && e.Session.S3MultipartID != "" {
			gc.blob.AbortMultipartUpload(ctx, BlobKey(e.Session.SpaceID, e.Session.BlobID), e.Session.S3MultipartID)
		}
		// The bucket key is the session ID, so one delete path covers both
		// parseable and corrupt entries.
		gc.store.DeleteUpload(e.Key)
		if reason == "corrupt" {
			result.CorruptUploadsReaped++
			GCCorruptUploadsReaped.Inc()
		} else {
			result.ExpiredUploadsCleaned++
			GCExpiredUploadsCleaned.Inc()
		}
		gc.log.Debug().Str("session_id", e.Key).Str("reason", reason).Msg("gc: reaped upload session")
	}
}
