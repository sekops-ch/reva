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

const gcLockTTL = 30 * time.Minute

// GCResult holds the outcome of a single GC run.
type GCResult struct {
	BlobsScanned        int
	BlobsDeleted        int
	WouldDelete         int
	ChildrenReconciled  int // stale or duplicate oc-children entries removed
	Errors              int
	Duration            time.Duration
}

// blobGC runs periodic garbage collection to find and remove orphaned S3 blobs
// that are no longer referenced by any node, version, upload, or trash entry.
type blobGC struct {
	store    MetadataStore
	blob     BlobStore
	opts     *Options
	log      *zerolog.Logger
	stopCh   chan struct{}
	holderID string

	// uploadAgeReporter, if set, is invoked once per GC tick with the
	// current uploads slice so the kvfs driver can publish authoritative
	// liveness metrics (oldest age, total session count) without spinning
	// a dedicated walker goroutine.
	uploadAgeReporter func(uploads []*UploadSession)
}

// SetUploadAgeReporter installs a callback invoked once per GC tick with
// the current uploads slice. Used by the kvfs driver to keep the
// kvfs_oldest_upload_age_seconds + kvfs_upload_sessions_total gauges
// authoritative.
func (gc *blobGC) SetUploadAgeReporter(fn func(uploads []*UploadSession)) {
	gc.uploadAgeReporter = fn
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
		go func() {
			gc.log.Info().Msg("gc: running initial sweep on start")
			gc.Run(context.Background())
		}()
	}

	go gc.loop()
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

	uploads, err := gc.store.ListAllUploads()
	if err != nil {
		gc.log.Error().Err(err).Msg("gc: failed to list uploads, aborting sweep")
		GCErrors.Inc()
		result.Errors++
		finalize()
		return result
	}
	if gc.uploadAgeReporter != nil {
		gc.uploadAgeReporter(uploads)
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
	}

	expiredUploads := gc.cleanExpiredUploads(ctx, uploads)

	finalize()

	gc.log.Info().
		Int("scanned", result.BlobsScanned).
		Int("deleted", result.BlobsDeleted).
		Int("would_delete", result.WouldDelete).
		Int("expired_uploads_cleaned", expiredUploads).
		Int("children_reconciled", result.ChildrenReconciled).
		Int("errors", result.Errors).
		Dur("duration", result.Duration).
		Msg("gc: sweep completed")

	return result
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
				continue
			}
			delete(children, name)
			dirty = true
			reconciled++
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

// cleanExpiredUploads removes upload sessions past their expiry and aborts
// any associated S3 multipart uploads. Reuses the uploads list from buildReferenceSet.
func (gc *blobGC) cleanExpiredUploads(ctx context.Context, uploads []*UploadSession) int {
	now := time.Now().Unix()
	cleaned := 0
	for _, u := range uploads {
		if u.Expires > 0 && u.Expires < now {
			if u.S3MultipartID != "" {
				gc.blob.AbortMultipartUpload(ctx, BlobKey(u.SpaceID, u.BlobID), u.S3MultipartID)
			}
			gc.store.DeleteUpload(u.ID)
			cleaned++
			gc.log.Debug().Str("session_id", u.ID).Msg("gc: cleaned expired upload session")
		}
	}
	return cleaned
}
