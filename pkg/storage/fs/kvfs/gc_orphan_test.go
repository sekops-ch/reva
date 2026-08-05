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
	"errors"
	"strings"
	"testing"
	"time"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/rs/zerolog"
)

// mockUserResolver answers liveness from a fixed map. An owner absent from the
// map is "unknown" (never reaped). A non-nil err fails the whole lookup.
type mockUserResolver struct {
	live              map[string]bool // ownerID -> determined liveness (LDAP result)
	serviceAccountIDs map[string]bool // service account IDs always treated as alive
	err               error
	seen              []string // owner IDs the GC asked about (for assertions)

	// Warm-up simulation: the first failFirstErr calls return a
	// whole-lookup error (gateway not ready); the next unknownFirst calls
	// return an empty map (every owner unknown). Both drain per call, so a
	// bounded warm-up retry eventually reaches the healthy answer below.
	failFirstErr int
	unknownFirst int
	calls        int
}

func (m *mockUserResolver) ResolveLiveness(ctx context.Context, ownerIDs []string) (map[string]bool, error) {
	m.seen = append(m.seen, ownerIDs...)
	m.calls++
	if m.err != nil {
		return nil, m.err
	}
	if m.failFirstErr > 0 {
		m.failFirstErr--
		return nil, errors.New("mock: gateway not ready (warm-up)")
	}
	out := make(map[string]bool, len(ownerIDs))
	if m.unknownFirst > 0 {
		m.unknownFirst--
		return out, nil // every owner unknown this attempt
	}
	for _, id := range ownerIDs {
		if m.serviceAccountIDs[id] {
			out[id] = true
			continue
		}
		if v, ok := m.live[id]; ok {
			out[id] = v
		}
	}
	return out, nil
}

// recordingReaper returns a reapSpace closure that records which spaces it was
// asked to reap and removes the oc-spaces entry from the mock store.
func recordingReaper(store *mockMetadataStore, reaped *[]string) func(context.Context, string, string) error {
	return func(_ context.Context, spaceID, _ string) error {
		*reaped = append(*reaped, spaceID)
		return store.DeleteSpace(spaceID)
	}
}

// --- identity reaping ---

func TestGCReapIdentityOrphans_PersonalReapedProjectSurfaced(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)

	gc.resolver = &mockUserResolver{live: map[string]bool{
		"alice": true,  // live
		"ghost": false, // dead
		"gp":    false, // dead (owns a project space)
		// "maybe" intentionally absent -> unknown
	}}
	var reaped []string
	gc.reapSpace = recordingReaper(store, &reaped)

	spaces := []*SpaceEntry{
		{ID: "live-p", Type: "personal", Owner: "alice", RootID: "r1", Name: "Alice"},
		{ID: "dead-p", Type: "personal", Owner: "ghost", RootID: "r2", Name: "Ghost"},
		{ID: "dead-proj", Type: "project", Owner: "gp", RootID: "r3", Name: "Shared"},
		{ID: "unknown-p", Type: "personal", Owner: "maybe", RootID: "r4", Name: "Maybe"},
	}

	var result GCResult
	gc.reapIdentityOrphans(context.Background(), spaces, &result)

	if got := strings.Join(reaped, ","); got != "dead-p" {
		t.Fatalf("reaped = %q, want only the dead personal space \"dead-p\"", got)
	}
	if result.OrphanPersonalSpacesDeleted != 1 {
		t.Errorf("OrphanPersonalSpacesDeleted = %d, want 1", result.OrphanPersonalSpacesDeleted)
	}
	if result.OrphanProjectSpaces != 1 {
		t.Errorf("OrphanProjectSpaces (surfaced) = %d, want 1", result.OrphanProjectSpaces)
	}
	if v := getGaugeValue(t, "kvfs_gc_orphan_project_spaces", nil); v != 1 {
		t.Errorf("gauge kvfs_gc_orphan_project_spaces = %v, want 1", v)
	}
}

func TestGCReapIdentityOrphans_DryRunDeletesNothing(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, true) // dry-run

	gc.resolver = &mockUserResolver{live: map[string]bool{"ghost": false}}
	var reaped []string
	gc.reapSpace = recordingReaper(store, &reaped)

	spaces := []*SpaceEntry{{ID: "dead-p", Type: "personal", Owner: "ghost", RootID: "r"}}

	var result GCResult
	gc.reapIdentityOrphans(context.Background(), spaces, &result)

	if len(reaped) != 0 {
		t.Errorf("dry-run must not reap; reaped=%v", reaped)
	}
	if result.OrphanPersonalSpacesDeleted != 0 {
		t.Errorf("dry-run OrphanPersonalSpacesDeleted = %d, want 0", result.OrphanPersonalSpacesDeleted)
	}
	if result.WouldDelete != 1 {
		t.Errorf("dry-run WouldDelete = %d, want 1", result.WouldDelete)
	}
}

func TestGCReapIdentityOrphans_ResolverErrorSkipsAll(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)

	gc.resolver = &mockUserResolver{err: errors.New("user backend down")}
	var reaped []string
	gc.reapSpace = recordingReaper(store, &reaped)

	spaces := []*SpaceEntry{{ID: "dead-p", Type: "personal", Owner: "ghost", RootID: "r"}}

	skippedBefore := getCounterValue(t, "kvfs_gc_identity_reaping_skipped_total", nil)
	var result GCResult
	gc.reapIdentityOrphans(context.Background(), spaces, &result)

	if len(reaped) != 0 {
		t.Errorf("a resolver error must reap NOTHING; reaped=%v", reaped)
	}
	if d := getCounterValue(t, "kvfs_gc_identity_reaping_skipped_total", nil) - skippedBefore; d != 1 {
		t.Errorf("identity_reaping_skipped delta = %v, want 1", d)
	}
}

func TestGCReapIdentityOrphans_NilResolverDisabled(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false) // resolver left nil
	var reaped []string
	gc.reapSpace = recordingReaper(store, &reaped)

	spaces := []*SpaceEntry{{ID: "dead-p", Type: "personal", Owner: "ghost", RootID: "r"}}

	var result GCResult
	gc.reapIdentityOrphans(context.Background(), spaces, &result)

	if len(reaped) != 0 || result.OrphanPersonalSpacesDeleted != 0 {
		t.Errorf("nil resolver must disable identity reaping; reaped=%v", reaped)
	}
}

// --- residue sweep ---

func TestGCReconcileResidue_ReapsGhostSpaceKeepsLive(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)

	// Ghost space "ghost" — its oc-spaces entry is gone, but a node + blob remain.
	store.nodes["ghost.n1"] = &NodeEntry{ID: "n1", SpaceID: "ghost", BlobID: "b1", Type: NodeTypeFile}
	store.nodeRevs["ghost.n1"] = 1
	blob.blobs[BlobKey("ghost", "b1")] = []byte("residue")
	blob.blobs[BlobKey("ghost", "orphan-blob")] = []byte("blob-only residue")

	// Live space "live" — must be untouched.
	store.nodes["live.n2"] = &NodeEntry{ID: "n2", SpaceID: "live", BlobID: "b2", Type: NodeTypeFile}
	store.nodeRevs["live.n2"] = 1
	blob.blobs[BlobKey("live", "b2")] = []byte("keep")

	live := map[string]struct{}{"live": {}}
	var result GCResult
	gc.reconcileResidue(context.Background(), live, nil, time.Now(), &result)

	if _, _, err := store.GetNode("ghost", "n1"); err == nil {
		t.Error("ghost node should have been reaped")
	}
	if _, ok := blob.blobs[BlobKey("ghost", "b1")]; ok {
		t.Error("ghost node blob should have been reaped")
	}
	if _, ok := blob.blobs[BlobKey("ghost", "orphan-blob")]; ok {
		t.Error("ghost blob-only residue should have been reaped")
	}
	if _, _, err := store.GetNode("live", "n2"); err != nil {
		t.Error("live node must NOT be reaped")
	}
	if _, ok := blob.blobs[BlobKey("live", "b2")]; !ok {
		t.Error("live blob must NOT be reaped")
	}
	if result.ResidueKeysDeleted < 1 || result.ResidueBlobsDeleted < 1 {
		t.Errorf("residue counters too low: keys=%d blobs=%d", result.ResidueKeysDeleted, result.ResidueBlobsDeleted)
	}
}

func TestGCReconcileResidue_YoungBlobProtected(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)

	key := BlobKey("ghost", "freshblob")
	blob.blobs[key] = []byte("just uploaded")
	blob.blobTimes = map[string]time.Time{key: time.Now()}

	live := map[string]struct{}{} // ghost is not live
	cutoff := time.Now().Add(-1 * time.Hour)
	var result GCResult
	gc.reconcileResidue(context.Background(), live, nil, cutoff, &result)

	if _, ok := blob.blobs[key]; !ok {
		t.Error("a young residual blob must be protected by the age cutoff")
	}
}

func TestGCReconcileResidue_DryRunDeletesNothing(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, true) // dry-run

	store.nodes["ghost.n1"] = &NodeEntry{ID: "n1", SpaceID: "ghost", BlobID: "b1", Type: NodeTypeFile}
	blob.blobs[BlobKey("ghost", "b1")] = []byte("x")

	var result GCResult
	gc.reconcileResidue(context.Background(), map[string]struct{}{}, nil, time.Now(), &result)

	if _, _, err := store.GetNode("ghost", "n1"); err != nil {
		t.Error("dry-run must NOT delete the ghost node")
	}
	if _, ok := blob.blobs[BlobKey("ghost", "b1")]; !ok {
		t.Error("dry-run must NOT delete the ghost blob")
	}
	if result.WouldDelete == 0 {
		t.Error("dry-run should report WouldDelete > 0")
	}
}

// --- end-to-end: Run() wires identity reaping + residue through the real driver path ---

func TestGCRun_IdentityReapingEndToEnd(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	opts := gcOpts(false)
	opts.GCMinAge = "0s"
	log := zerolog.Nop()
	d := testDriver(store, blob)
	d.opts = opts
	d.gc = newBlobGC(store, blob, opts, &log)
	// Reuse the real deletion path, exactly like New() wires it.
	d.gc.reapSpace = func(ctx context.Context, spaceID, rootID string) error {
		cctx, cancel := d.commitPhase(ctx)
		defer cancel()
		if err := d.deleteSpaceContents(cctx, spaceID, rootID); err != nil {
			return err
		}
		return d.store.DeleteSpace(spaceID)
	}
	d.gc.resolver = &mockUserResolver{live: map[string]bool{"alice": true, "ghost": false, "gp": false}}

	// Live personal space (alice).
	setupSpaceWithFile(store, "live", "root-l", "lf", "live.txt")
	store.spaces["live"].Owner = "alice"
	store.nodes["live.lf"].Owner = "alice"
	blob.blobs[BlobKey("live", "old-blob")] = []byte("alice data")

	// Dead-owner personal space (ghost) with a file + blob — should be fully reaped.
	setupSpaceWithFile(store, "dead", "root-d", "df", "dead.txt")
	store.spaces["dead"].Owner = "ghost"
	blob.blobs[BlobKey("dead", "old-blob")] = []byte("ghost data")

	// Dead-owner project space — should be SURFACED, not deleted.
	store.spaces["proj"] = &SpaceEntry{ID: "proj", Type: "project", Owner: "gp", RootID: "root-p", Name: "Shared"}
	store.spaceRevs["proj"] = 1

	result := d.gc.Run(context.Background())

	// Dead personal space fully gone.
	if _, _, err := store.GetSpace("dead"); err == nil {
		t.Error("dead personal space entry should be gone")
	}
	if n := countSpaceKeys(store, blob, "dead"); n != 0 {
		t.Errorf("dead space left %d residual keys/blobs, want 0", n)
	}
	if result.OrphanPersonalSpacesDeleted != 1 {
		t.Errorf("OrphanPersonalSpacesDeleted = %d, want 1", result.OrphanPersonalSpacesDeleted)
	}

	// Project space surfaced but preserved.
	if _, _, err := store.GetSpace("proj"); err != nil {
		t.Error("project space must NOT be deleted")
	}
	if result.OrphanProjectSpaces != 1 {
		t.Errorf("OrphanProjectSpaces = %d, want 1", result.OrphanProjectSpaces)
	}

	// Live space preserved.
	if _, _, err := store.GetSpace("live"); err != nil {
		t.Error("live space must NOT be deleted")
	}
	if _, ok := blob.blobs[BlobKey("live", "old-blob")]; !ok {
		t.Error("live blob must NOT be deleted")
	}
}

// countSpaceKeys returns the number of KV entries + S3 blobs still associated
// with a space — used to assert zero residue after a delete/reap.
func countSpaceKeys(store *mockMetadataStore, blob *mockBlobStore, spaceID string) int {
	store.mu.Lock()
	n := 0
	for k := range store.nodes {
		if strings.HasPrefix(k, spaceID+".") {
			n++
		}
	}
	for k := range store.children {
		if strings.HasPrefix(k, spaceID+".") {
			n++
		}
	}
	for _, v := range store.versions {
		if v.SpaceID == spaceID {
			n++
		}
	}
	for k := range store.trash {
		if strings.HasPrefix(k, spaceID+".") {
			n++
		}
	}
	for _, u := range store.uploads {
		if u.SpaceID == spaceID {
			n++
		}
	}
	store.mu.Unlock()

	blob.mu.Lock()
	for k := range blob.blobs {
		if strings.HasPrefix(k, spaceID+"/") {
			n++
		}
	}
	blob.mu.Unlock()
	return n
}

// --- CRUD invariant: DeleteStorageSpace leaves zero residue at every layer ---

func TestDeleteStorageSpace_LeavesNoResidue(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
	blob.blobs[BlobKey("s1", "old-blob")] = []byte("file data")

	// A version, a trash entry, and an upload — every layer deleteSpaceContents must reap.
	store.versions = append(store.versions, &VersionEntry{Key: "v1", NodeID: "f1", SpaceID: "s1", BlobID: "ver-blob"})
	blob.blobs[BlobKey("s1", "ver-blob")] = []byte("old version")
	store.trash["s1.t1"] = &TrashEntry{Key: "t1", SpaceID: "s1", Node: NodeEntry{BlobID: "trash-blob", SpaceID: "s1"}}
	blob.blobs[BlobKey("s1", "trash-blob")] = []byte("trashed")
	store.uploads["u1"] = &UploadSession{ID: "u1", SpaceID: "s1", BlobID: "up-blob"}
	blob.blobs[BlobKey("s1", "up-blob")] = []byte("uploading")

	// An unrelated live space that must survive untouched.
	setupSpaceWithFile(store, "s2", "root2", "f2", "keep.txt")
	blob.blobs[BlobKey("s2", "keep-blob")] = []byte("keep")

	err := d.DeleteStorageSpace(context.Background(), &provider.DeleteStorageSpaceRequest{
		Id: &provider.StorageSpaceId{OpaqueId: "s1"},
	})
	if err != nil {
		t.Fatalf("DeleteStorageSpace: %v", err)
	}

	if _, _, err := store.GetSpace("s1"); err == nil {
		t.Error("space entry should be gone")
	}
	if n := countSpaceKeys(store, blob, "s1"); n != 0 {
		t.Errorf("DeleteStorageSpace left %d residual keys/blobs for s1, want 0", n)
	}
	// The other space is untouched.
	if n := countSpaceKeys(store, blob, "s2"); n == 0 {
		t.Error("unrelated space s2 was wrongly reaped")
	}
}

// --- resolver construction ---

func TestNewCS3UserResolver_RequiresFullConfig(t *testing.T) {
	log := zerolog.Nop()
	if _, err := newCS3UserResolver("", "id", "secret", nil, &log); err == nil {
		t.Error("missing gateway addr must error (→ nil resolver, reaping disabled)")
	}
	if _, err := newCS3UserResolver("addr", "", "secret", nil, &log); err == nil {
		t.Error("missing service account id must error")
	}
	if _, err := newCS3UserResolver("addr", "id", "", nil, &log); err == nil {
		t.Error("missing service account secret must error")
	}
	r, err := newCS3UserResolver("127.0.0.1:9142", "id", "secret", []string{"sa-1", "sa-2"}, &log)
	if err != nil || r == nil {
		t.Errorf("full config should build a resolver; got r=%v err=%v", r, err)
	}
	csr := r.(*cs3UserResolver)
	if !csr.serviceAccountIDs["sa-1"] || !csr.serviceAccountIDs["sa-2"] {
		t.Errorf("service account IDs not populated: %v", csr.serviceAccountIDs)
	}
}

func TestGCReapIdentityOrphans_ServiceAccountSpaceNeverReaped(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)

	gc.resolver = &mockUserResolver{live: map[string]bool{
		"alice":  true,
		"sa-bot": false, // LDAP says NOT_FOUND, but it's a service account
		"ghost":  false, // genuinely dead
	}, serviceAccountIDs: map[string]bool{"sa-bot": true}}
	var reaped []string
	gc.reapSpace = recordingReaper(store, &reaped)

	spaces := []*SpaceEntry{
		{ID: "live-p", Type: "personal", Owner: "alice", RootID: "r1", Name: "Alice"},
		{ID: "sa-p", Type: "personal", Owner: "sa-bot", RootID: "r2", Name: "ServiceBot"},
		{ID: "dead-p", Type: "personal", Owner: "ghost", RootID: "r3", Name: "Ghost"},
	}

	var result GCResult
	gc.reapIdentityOrphans(context.Background(), spaces, &result)

	if got := strings.Join(reaped, ","); got != "dead-p" {
		t.Fatalf("reaped = %q, want only \"dead-p\" (service account space must survive)", got)
	}
	if result.OrphanPersonalSpacesDeleted != 1 {
		t.Errorf("OrphanPersonalSpacesDeleted = %d, want 1", result.OrphanPersonalSpacesDeleted)
	}
}

// --- identity-reaping visibility ---

// TestGCIdentityUnknownOwnersGauge — per-owner "unknown" answers (owners absent
// from the resolver map) are counted on kvfs_gc_identity_unknown_owners so a
// resolver-degraded sweep is distinguishable from a clean one.
func TestGCIdentityUnknownOwnersGauge(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)
	gc.resolver = &mockUserResolver{live: map[string]bool{
		"alice": true,
		"ghost": false,
		// "maybe" and "later" absent -> unknown (2)
	}}
	gc.reapSpace = recordingReaper(store, new([]string))

	spaces := []*SpaceEntry{
		{ID: "s1", Type: "personal", Owner: "alice", RootID: "r1"},
		{ID: "s2", Type: "personal", Owner: "ghost", RootID: "r2"},
		{ID: "s3", Type: "personal", Owner: "maybe", RootID: "r3"},
		{ID: "s4", Type: "personal", Owner: "later", RootID: "r4"},
	}

	var result GCResult
	gc.reapIdentityOrphans(context.Background(), spaces, &result)

	if v := getGaugeValue(t, "kvfs_gc_identity_unknown_owners", nil); v != 2 {
		t.Errorf("kvfs_gc_identity_unknown_owners = %v, want 2", v)
	}
}

// TestGCIdentityUnknownOwnersGauge_ZeroWhenClean — a sweep that classifies every
// owner resets the gauge to 0 (correct healthy value; the >0 alert reads it right).
func TestGCIdentityUnknownOwnersGauge_ZeroWhenClean(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)
	gc.resolver = &mockUserResolver{live: map[string]bool{"alice": true, "ghost": false}}
	gc.reapSpace = recordingReaper(store, new([]string))

	spaces := []*SpaceEntry{
		{ID: "s1", Type: "personal", Owner: "alice", RootID: "r1"},
		{ID: "s2", Type: "personal", Owner: "ghost", RootID: "r2"},
	}

	var result GCResult
	gc.reapIdentityOrphans(context.Background(), spaces, &result)

	if v := getGaugeValue(t, "kvfs_gc_identity_unknown_owners", nil); v != 0 {
		t.Errorf("kvfs_gc_identity_unknown_owners = %v, want 0 on a clean sweep", v)
	}
}

// TestGCIdentityUnknownOwnersGauge_AllOnWholeError — a whole-lookup failure
// leaves every distinct owner unknown; the gauge reflects that AND the existing
// skip counter still fires (the two signals coexist).
func TestGCIdentityUnknownOwnersGauge_AllOnWholeError(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)
	gc.resolver = &mockUserResolver{err: errors.New("gateway down")}
	gc.reapSpace = recordingReaper(store, new([]string))

	spaces := []*SpaceEntry{
		{ID: "s1", Type: "personal", Owner: "alice", RootID: "r1"},
		{ID: "s2", Type: "personal", Owner: "ghost", RootID: "r2"},
	}

	skippedBefore := getCounterValue(t, "kvfs_gc_identity_reaping_skipped_total", nil)
	var result GCResult
	gc.reapIdentityOrphans(context.Background(), spaces, &result)

	if v := getGaugeValue(t, "kvfs_gc_identity_unknown_owners", nil); v != 2 {
		t.Errorf("kvfs_gc_identity_unknown_owners = %v, want 2 (all owners unknown on whole error)", v)
	}
	if d := getCounterValue(t, "kvfs_gc_identity_reaping_skipped_total", nil) - skippedBefore; d != 1 {
		t.Errorf("identity_reaping_skipped delta = %v, want 1", d)
	}
}

// TestGCIdentityWarmupRetrySucceeds — the FIRST sweep retries a not-ready
// gateway (whole-lookup errors) up to the bound, then reaps once it answers.
// This is a RESOLVER-RPC retry, not a KV retry. Bite check: with a single
// attempt (no warm-up) the first error would skip reaping.
func TestGCIdentityWarmupRetrySucceeds(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	withShortResolverWarmup(t, 5, 2*time.Millisecond)
	gc := testGC(store, blob, false)
	gc.resolver = &mockUserResolver{
		live:         map[string]bool{"ghost": false},
		failFirstErr: 2, // gateway not ready for the first 2 attempts
	}
	var reaped []string
	gc.reapSpace = recordingReaper(store, &reaped)

	spaces := []*SpaceEntry{{ID: "dead-p", Type: "personal", Owner: "ghost", RootID: "r"}}

	var result GCResult
	gc.reapIdentityOrphans(context.Background(), spaces, &result) // first sweep

	if len(reaped) != 1 {
		t.Fatalf("warm-up should retry past the not-ready gateway and reap; reaped=%v", reaped)
	}
	if v := getGaugeValue(t, "kvfs_gc_identity_unknown_owners", nil); v != 0 {
		t.Errorf("unknown_owners after successful warm-up = %v, want 0", v)
	}
}

// TestGCIdentityWarmupRetriesUnknowns — warm-up also drains transient per-owner
// unknowns (empty map), reaching a clean classification within the bound.
func TestGCIdentityWarmupRetriesUnknowns(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	withShortResolverWarmup(t, 5, 2*time.Millisecond)
	gc := testGC(store, blob, false)
	gc.resolver = &mockUserResolver{
		live:         map[string]bool{"ghost": false},
		unknownFirst: 2, // first 2 attempts return "all unknown"
	}
	var reaped []string
	gc.reapSpace = recordingReaper(store, &reaped)

	spaces := []*SpaceEntry{{ID: "dead-p", Type: "personal", Owner: "ghost", RootID: "r"}}

	var result GCResult
	gc.reapIdentityOrphans(context.Background(), spaces, &result)

	if len(reaped) != 1 {
		t.Fatalf("warm-up should retry past transient unknowns and reap; reaped=%v", reaped)
	}
}

// TestGCIdentityWarmupBounded — warm-up gives up after the bound on a
// persistently-unready gateway (no hang), skipping reaping as before.
func TestGCIdentityWarmupBounded(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	withShortResolverWarmup(t, 3, 2*time.Millisecond)
	gc := testGC(store, blob, false)
	resolver := &mockUserResolver{failFirstErr: 100} // never becomes ready
	gc.resolver = resolver
	var reaped []string
	gc.reapSpace = recordingReaper(store, &reaped)

	spaces := []*SpaceEntry{{ID: "dead-p", Type: "personal", Owner: "ghost", RootID: "r"}}

	skippedBefore := getCounterValue(t, "kvfs_gc_identity_reaping_skipped_total", nil)
	var result GCResult
	gc.reapIdentityOrphans(context.Background(), spaces, &result)

	if len(reaped) != 0 {
		t.Errorf("a persistently unready gateway must reap nothing; reaped=%v", reaped)
	}
	if resolver.calls != 3 {
		t.Errorf("resolver calls = %d, want exactly the warm-up bound (3)", resolver.calls)
	}
	if d := getCounterValue(t, "kvfs_gc_identity_reaping_skipped_total", nil) - skippedBefore; d != 1 {
		t.Errorf("identity_reaping_skipped delta = %v, want 1", d)
	}
}

// TestGCIdentityWarmupFirstSweepOnly — warm-up applies only to the first sweep;
// a subsequent sweep makes a single resolver attempt.
func TestGCIdentityWarmupFirstSweepOnly(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	withShortResolverWarmup(t, 5, 2*time.Millisecond)
	gc := testGC(store, blob, false)
	gc.reapSpace = recordingReaper(store, new([]string))
	spaces := []*SpaceEntry{{ID: "s1", Type: "personal", Owner: "alice", RootID: "r"}}

	// First sweep consumes the warm-up allowance (resolver healthy, one call).
	gc.resolver = &mockUserResolver{live: map[string]bool{"alice": true}}
	var r1 GCResult
	gc.reapIdentityOrphans(context.Background(), spaces, &r1)

	// Second sweep: resolver would need warm-up but must NOT retry.
	second := &mockUserResolver{failFirstErr: 100}
	gc.resolver = second
	var r2 GCResult
	gc.reapIdentityOrphans(context.Background(), spaces, &r2)

	if second.calls != 1 {
		t.Errorf("second sweep resolver calls = %d, want 1 (no warm-up after the first sweep)", second.calls)
	}
}
