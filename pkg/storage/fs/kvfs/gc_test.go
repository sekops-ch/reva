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
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func gcOpts(dryRun bool) *Options {
	o := &Options{
		GCEnabled:  true,
		GCInterval: "1h",
		GCMinAge:   "1h",
		GCDryRun:   dryRun,
	}
	o.init()
	return o
}

func testGC(store *mockMetadataStore, blob *mockBlobStore, dryRun bool) *blobGC {
	log := zerolog.Nop()
	return newBlobGC(store, blob, gcOpts(dryRun), &log)
}

// --- buildSpaceReferenceSet tests ---

func TestBuildSpaceReferenceSetEmpty(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, true)

	refs, err := gc.buildSpaceReferenceSet("any-space", nil)
	if err != nil {
		t.Fatalf("buildSpaceReferenceSet: %v", err)
	}
	if len(refs) != 0 {
		t.Errorf("expected empty reference set, got %d", len(refs))
	}
}

func TestBuildSpaceReferenceSetFromNodes(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, true)

	store.nodes["space-1.node-1"] = &NodeEntry{
		ID: "node-1", SpaceID: "space-1", BlobID: "blob-a", Type: NodeTypeFile,
	}
	store.nodes["space-1.node-2"] = &NodeEntry{
		ID: "node-2", SpaceID: "space-1", BlobID: "blob-b", Type: NodeTypeFile,
	}
	// Dir node has no BlobID
	store.nodes["space-1.dir-1"] = &NodeEntry{
		ID: "dir-1", SpaceID: "space-1", Type: NodeTypeDir,
	}

	refs, err := gc.buildSpaceReferenceSet("space-1", nil)
	if err != nil {
		t.Fatalf("buildSpaceReferenceSet: %v", err)
	}

	keyA := BlobKey("space-1", "blob-a")
	keyB := BlobKey("space-1", "blob-b")

	if !refs[keyA] {
		t.Errorf("expected %q in reference set", keyA)
	}
	if !refs[keyB] {
		t.Errorf("expected %q in reference set", keyB)
	}
	if len(refs) != 2 {
		t.Errorf("expected 2 references, got %d", len(refs))
	}
}

func TestBuildSpaceReferenceSetFromVersions(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, true)

	store.versions = append(store.versions, &VersionEntry{
		Key: "v1", NodeID: "node-1", SpaceID: "space-1", BlobID: "blob-v1",
	})

	refs, err := gc.buildSpaceReferenceSet("space-1", nil)
	if err != nil {
		t.Fatalf("buildSpaceReferenceSet: %v", err)
	}

	key := BlobKey("space-1", "blob-v1")
	if !refs[key] {
		t.Errorf("expected version blob %q in reference set", key)
	}
}

func TestBuildSpaceReferenceSetFromUploads(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, true)

	uploads := []*UploadSession{
		{ID: "upload-1", SpaceID: "space-1", BlobID: "blob-upload"},
	}

	refs, err := gc.buildSpaceReferenceSet("space-1", uploads)
	if err != nil {
		t.Fatalf("buildSpaceReferenceSet: %v", err)
	}

	key := BlobKey("space-1", "blob-upload")
	if !refs[key] {
		t.Errorf("expected upload blob %q in reference set", key)
	}
}

func TestBuildSpaceReferenceSetFromTrash(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, true)

	store.trash["space-1.trash-1"] = &TrashEntry{
		Key: "trash-1", SpaceID: "space-1",
		Node: NodeEntry{BlobID: "blob-trashed", SpaceID: "space-1"},
	}

	refs, err := gc.buildSpaceReferenceSet("space-1", nil)
	if err != nil {
		t.Fatalf("buildSpaceReferenceSet: %v", err)
	}

	key := BlobKey("space-1", "blob-trashed")
	if !refs[key] {
		t.Errorf("expected trash blob %q in reference set", key)
	}
}

func TestBuildSpaceReferenceSetCombined(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, true)

	store.nodes["s1.n1"] = &NodeEntry{ID: "n1", SpaceID: "s1", BlobID: "b1", Type: NodeTypeFile}
	store.versions = append(store.versions, &VersionEntry{Key: "v1", NodeID: "n1", SpaceID: "s1", BlobID: "b2"})
	store.trash["s1.t1"] = &TrashEntry{Key: "t1", SpaceID: "s1", Node: NodeEntry{BlobID: "b4", SpaceID: "s1"}}

	uploads := []*UploadSession{
		{ID: "u1", SpaceID: "s1", BlobID: "b3"},
	}

	refs, err := gc.buildSpaceReferenceSet("s1", uploads)
	if err != nil {
		t.Fatalf("buildSpaceReferenceSet: %v", err)
	}

	if len(refs) != 4 {
		t.Errorf("expected 4 references from all sources, got %d", len(refs))
	}
}

func TestBuildSpaceReferenceSetIsolation(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, true)

	store.nodes["s1.n1"] = &NodeEntry{ID: "n1", SpaceID: "s1", BlobID: "b1", Type: NodeTypeFile}
	store.nodes["s2.n2"] = &NodeEntry{ID: "n2", SpaceID: "s2", BlobID: "b2", Type: NodeTypeFile}
	store.versions = append(store.versions,
		&VersionEntry{Key: "v1", NodeID: "n1", SpaceID: "s1", BlobID: "vb1"},
		&VersionEntry{Key: "v2", NodeID: "n2", SpaceID: "s2", BlobID: "vb2"},
	)
	store.trash["s1.t1"] = &TrashEntry{Key: "t1", SpaceID: "s1", Node: NodeEntry{BlobID: "tb1", SpaceID: "s1"}}
	store.trash["s2.t2"] = &TrashEntry{Key: "t2", SpaceID: "s2", Node: NodeEntry{BlobID: "tb2", SpaceID: "s2"}}

	uploads := []*UploadSession{
		{ID: "u1", SpaceID: "s1", BlobID: "ub1"},
		{ID: "u2", SpaceID: "s2", BlobID: "ub2"},
	}

	refs, err := gc.buildSpaceReferenceSet("s1", uploads)
	if err != nil {
		t.Fatalf("buildSpaceReferenceSet: %v", err)
	}

	if len(refs) != 4 {
		t.Errorf("expected 4 refs for s1, got %d", len(refs))
	}
	if !refs[BlobKey("s1", "b1")] {
		t.Error("s1 node blob should be in ref set")
	}
	if !refs[BlobKey("s1", "vb1")] {
		t.Error("s1 version blob should be in ref set")
	}
	if !refs[BlobKey("s1", "tb1")] {
		t.Error("s1 trash blob should be in ref set")
	}
	if !refs[BlobKey("s1", "ub1")] {
		t.Error("s1 upload blob should be in ref set")
	}
	if refs[BlobKey("s2", "b2")] {
		t.Error("s2 node blob should NOT be in s1 ref set")
	}
	if refs[BlobKey("s2", "vb2")] {
		t.Error("s2 version blob should NOT be in s1 ref set")
	}
}

// --- GC Run tests ---

func TestGCRunNoOrphans(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)

	setupSpaceEmpty(store, "space-1", "root-1")

	store.nodes["space-1.file-1"] = &NodeEntry{
		ID: "file-1", SpaceID: "space-1", BlobID: "blob-1", Type: NodeTypeFile,
	}

	blobKey := BlobKey("space-1", "blob-1")
	blob.blobs[blobKey] = []byte("data")

	result := gc.Run(context.Background())

	if result.BlobsScanned != 1 {
		t.Errorf("scanned = %d, want 1", result.BlobsScanned)
	}
	if result.BlobsDeleted != 0 {
		t.Errorf("deleted = %d, want 0", result.BlobsDeleted)
	}
	if result.Errors != 0 {
		t.Errorf("errors = %d, want 0", result.Errors)
	}

	if _, ok := blob.blobs[blobKey]; !ok {
		t.Error("referenced blob should NOT be deleted")
	}
}

func TestGCRunDeletesOrphans(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	opts := gcOpts(false)
	opts.GCMinAge = "0s"
	log := zerolog.Nop()
	gc := newBlobGC(store, blob, opts, &log)

	setupSpaceEmpty(store, "space-1", "root-1")

	referencedKey := BlobKey("space-1", "blob-ref")
	orphanKey := BlobKey("space-1", "blob-orphan")

	store.nodes["space-1.file-1"] = &NodeEntry{
		ID: "file-1", SpaceID: "space-1", BlobID: "blob-ref", Type: NodeTypeFile,
	}

	blob.blobs[referencedKey] = []byte("keep")
	blob.blobs[orphanKey] = []byte("delete me")

	result := gc.Run(context.Background())

	if result.BlobsScanned != 2 {
		t.Errorf("scanned = %d, want 2", result.BlobsScanned)
	}
	if result.BlobsDeleted != 1 {
		t.Errorf("deleted = %d, want 1", result.BlobsDeleted)
	}
	if _, ok := blob.blobs[referencedKey]; !ok {
		t.Error("referenced blob should NOT be deleted")
	}
	if _, ok := blob.blobs[orphanKey]; ok {
		t.Error("orphan blob should be deleted")
	}
}

func TestGCRunDryRunDoesNotDelete(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	opts := gcOpts(true)
	opts.GCMinAge = "0s"
	log := zerolog.Nop()
	gc := newBlobGC(store, blob, opts, &log)

	setupSpaceEmpty(store, "space-1", "root-1")

	orphanKey := BlobKey("space-1", "blob-orphan")
	blob.blobs[orphanKey] = []byte("should survive")

	result := gc.Run(context.Background())

	if result.BlobsDeleted != 0 {
		t.Errorf("deleted = %d, want 0 in dry-run", result.BlobsDeleted)
	}
	if result.WouldDelete != 1 {
		t.Errorf("would_delete = %d, want 1", result.WouldDelete)
	}
	if _, ok := blob.blobs[orphanKey]; !ok {
		t.Error("dry-run should NOT delete the blob")
	}
}

func TestGCRunMinAgeProtection(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	opts := gcOpts(false)
	opts.GCMinAge = "1h"
	log := zerolog.Nop()
	gc := newBlobGC(store, blob, opts, &log)

	setupSpaceEmpty(store, "space-1", "root-1")

	orphanKey := BlobKey("space-1", "blob-young")
	blob.blobs[orphanKey] = []byte("too young")
	// mockBlobStore.ListBlobs returns BlobInfo with zero LastModified,
	// but we need to set it. Let me enhance the mock to support this.
	// For now, since mockBlobStore returns zero LastModified, the cutoff
	// check `blob.LastModified.After(cutoff)` with cutoff = now - 1h
	// will be false for zero time (which is before cutoff), so the blob
	// WILL be eligible for deletion. To test MinAge properly, we need
	// to set LastModified on the BlobInfo.

	// Override the mock's ListBlobs to return a recent timestamp
	blob.mu.Lock()
	blob.blobTimes = map[string]time.Time{
		orphanKey: time.Now(), // just created
	}
	blob.mu.Unlock()

	result := gc.Run(context.Background())

	if result.BlobsDeleted != 0 {
		t.Errorf("deleted = %d, want 0 (blob too young)", result.BlobsDeleted)
	}
	if _, ok := blob.blobs[orphanKey]; !ok {
		t.Error("young blob should NOT be deleted")
	}
}

func TestGCRunOrphanFromDeletedNode(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	opts := gcOpts(false)
	opts.GCMinAge = "0s"
	log := zerolog.Nop()
	gc := newBlobGC(store, blob, opts, &log)

	setupSpaceEmpty(store, "space-1", "root-1")

	// Blob exists in S3 but the node that referenced it was deleted
	orphanKey := BlobKey("space-1", "blob-deleted-node")
	blob.blobs[orphanKey] = []byte("orphaned by node deletion")

	result := gc.Run(context.Background())

	if result.BlobsDeleted != 1 {
		t.Errorf("deleted = %d, want 1", result.BlobsDeleted)
	}
	if _, ok := blob.blobs[orphanKey]; ok {
		t.Error("orphan from deleted node should be removed")
	}
}

func TestGCRunProtectsVersionBlobs(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	opts := gcOpts(false)
	opts.GCMinAge = "0s"
	log := zerolog.Nop()
	gc := newBlobGC(store, blob, opts, &log)

	setupSpaceEmpty(store, "space-1", "root-1")

	store.versions = append(store.versions, &VersionEntry{
		Key: "v1", NodeID: "node-1", SpaceID: "space-1", BlobID: "blob-versioned",
	})

	versionKey := BlobKey("space-1", "blob-versioned")
	blob.blobs[versionKey] = []byte("version content")

	result := gc.Run(context.Background())

	if result.BlobsDeleted != 0 {
		t.Errorf("deleted = %d, want 0 (version blob protected)", result.BlobsDeleted)
	}
	if _, ok := blob.blobs[versionKey]; !ok {
		t.Error("version-referenced blob should NOT be deleted")
	}
}

func TestGCRunProtectsUploadBlobs(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	opts := gcOpts(false)
	opts.GCMinAge = "0s"
	log := zerolog.Nop()
	gc := newBlobGC(store, blob, opts, &log)

	setupSpaceEmpty(store, "space-1", "root-1")

	store.uploads["upload-1"] = &UploadSession{
		ID: "upload-1", SpaceID: "space-1", BlobID: "blob-uploading",
	}

	uploadKey := BlobKey("space-1", "blob-uploading")
	blob.blobs[uploadKey] = []byte("in-progress upload")

	result := gc.Run(context.Background())

	if result.BlobsDeleted != 0 {
		t.Errorf("deleted = %d, want 0 (upload blob protected)", result.BlobsDeleted)
	}
}

func TestGCRunProtectsTrashBlobs(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	opts := gcOpts(false)
	opts.GCMinAge = "0s"
	log := zerolog.Nop()
	gc := newBlobGC(store, blob, opts, &log)

	setupSpaceEmpty(store, "space-1", "root-1")

	store.trash["space-1.trash-1"] = &TrashEntry{
		Key: "trash-1", SpaceID: "space-1",
		Node: NodeEntry{BlobID: "blob-trashed", SpaceID: "space-1"},
	}

	trashKey := BlobKey("space-1", "blob-trashed")
	blob.blobs[trashKey] = []byte("trashed content")

	result := gc.Run(context.Background())

	if result.BlobsDeleted != 0 {
		t.Errorf("deleted = %d, want 0 (trash blob protected)", result.BlobsDeleted)
	}
}

func TestGCRunMultipleSpaces(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	opts := gcOpts(false)
	opts.GCMinAge = "0s"
	log := zerolog.Nop()
	gc := newBlobGC(store, blob, opts, &log)

	setupSpaceEmpty(store, "space-1", "root-1")
	setupSpaceEmpty(store, "space-2", "root-2")

	// Referenced blob in space-1
	store.nodes["space-1.file-1"] = &NodeEntry{
		ID: "file-1", SpaceID: "space-1", BlobID: "blob-ref", Type: NodeTypeFile,
	}
	blob.blobs[BlobKey("space-1", "blob-ref")] = []byte("keep")

	// Orphan in space-1
	blob.blobs[BlobKey("space-1", "blob-orphan-1")] = []byte("delete")

	// Referenced blob in space-2
	store.nodes["space-2.file-2"] = &NodeEntry{
		ID: "file-2", SpaceID: "space-2", BlobID: "blob-ref-2", Type: NodeTypeFile,
	}
	blob.blobs[BlobKey("space-2", "blob-ref-2")] = []byte("keep")

	// Orphan in space-2
	blob.blobs[BlobKey("space-2", "blob-orphan-2")] = []byte("delete")

	result := gc.Run(context.Background())

	if result.BlobsScanned != 4 {
		t.Errorf("scanned = %d, want 4", result.BlobsScanned)
	}
	if result.BlobsDeleted != 2 {
		t.Errorf("deleted = %d, want 2", result.BlobsDeleted)
	}
}

func TestGCRunNoSpaces(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)

	result := gc.Run(context.Background())

	if result.BlobsScanned != 0 {
		t.Errorf("scanned = %d, want 0", result.BlobsScanned)
	}
	if result.Errors != 0 {
		t.Errorf("errors = %d, want 0", result.Errors)
	}
}

func TestGCRunDuration(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, true)

	result := gc.Run(context.Background())

	if result.Duration <= 0 {
		t.Error("duration should be positive")
	}
}

// --- Options tests ---

func TestGCOptionsDefaults(t *testing.T) {
	o := &Options{}
	o.init()

	if o.GCInterval != "24h" {
		t.Errorf("GCInterval default = %q, want %q", o.GCInterval, "24h")
	}
	if o.GCMinAge != "24h" {
		t.Errorf("GCMinAge default = %q, want %q", o.GCMinAge, "24h")
	}
}

func TestGCIntervalDuration(t *testing.T) {
	o := &Options{GCInterval: "30m"}
	if o.GCIntervalDuration() != 30*time.Minute {
		t.Errorf("GCIntervalDuration = %v, want 30m", o.GCIntervalDuration())
	}
}

func TestGCIntervalDurationInvalid(t *testing.T) {
	o := &Options{GCInterval: "not-a-duration"}
	if o.GCIntervalDuration() != 24*time.Hour {
		t.Errorf("invalid GCInterval should default to 24h, got %v", o.GCIntervalDuration())
	}
}

func TestGCMinAgeDuration(t *testing.T) {
	o := &Options{GCMinAge: "2h"}
	if o.GCMinAgeDuration() != 2*time.Hour {
		t.Errorf("GCMinAgeDuration = %v, want 2h", o.GCMinAgeDuration())
	}
}

func TestGCMinAgeDurationInvalid(t *testing.T) {
	o := &Options{GCMinAge: "bad"}
	if o.GCMinAgeDuration() != 24*time.Hour {
		t.Errorf("invalid GCMinAge should default to 24h, got %v", o.GCMinAgeDuration())
	}
}

// --- Start/Stop lifecycle ---

func TestGCStartStop(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, true)

	gc.Start()
	time.Sleep(10 * time.Millisecond)
	gc.Stop()
	// Should not panic or deadlock
}

func TestGCStartWithRunOnStart(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	opts := gcOpts(true)
	opts.GCRunOnStart = true
	log := zerolog.Nop()
	gc := newBlobGC(store, blob, opts, &log)

	setupSpaceEmpty(store, "space-1", "root-1")
	orphanKey := BlobKey("space-1", "blob-orphan")
	blob.blobs[orphanKey] = []byte("orphan")

	gc.Start()
	// Give the initial sweep goroutine time to run
	time.Sleep(50 * time.Millisecond)
	gc.Stop()

	// In dry-run mode, blob should still exist
	if _, ok := blob.blobs[orphanKey]; !ok {
		t.Error("dry-run should not delete blobs")
	}
}

// --- GC metrics tests ---

func TestGCMetricsRegistered(t *testing.T) {
	// Force-initialize the GC metrics
	GCBlobsScanned.Inc()
	GCBlobsDeleted.Inc()
	GCWouldDelete.Inc()
	GCErrors.Inc()
	GCRunDuration.Observe(1.0)
	GCExpiredUploadsCleaned.Inc()
	GCCorruptUploadsReaped.Inc()

	metrics := gatherKVFSMetrics(t)

	expectedMetrics := []string{
		"kvfs_gc_blobs_scanned_total",
		"kvfs_gc_blobs_deleted_total",
		"kvfs_gc_would_delete_total",
		"kvfs_gc_errors_total",
		"kvfs_gc_run_duration_seconds",
		"kvfs_gc_expired_uploads_cleaned_total",
		"kvfs_gc_corrupt_uploads_reaped_total",
	}
	for _, name := range expectedMetrics {
		if _, ok := metrics[name]; !ok {
			t.Errorf("metric %q not found in gathered metrics", name)
		}
	}
}

func TestGCRunUpdatesMetrics(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	opts := gcOpts(false)
	opts.GCMinAge = "0s"
	log := zerolog.Nop()
	gc := newBlobGC(store, blob, opts, &log)

	setupSpaceEmpty(store, "space-1", "root-1")

	blob.blobs[BlobKey("space-1", "orphan-1")] = []byte("x")
	blob.blobs[BlobKey("space-1", "orphan-2")] = []byte("y")

	scannedBefore := getCounterValue(t, "kvfs_gc_blobs_scanned_total", nil)
	deletedBefore := getCounterValue(t, "kvfs_gc_blobs_deleted_total", nil)

	gc.Run(context.Background())

	scannedAfter := getCounterValue(t, "kvfs_gc_blobs_scanned_total", nil)
	deletedAfter := getCounterValue(t, "kvfs_gc_blobs_deleted_total", nil)

	if scannedAfter-scannedBefore != 2 {
		t.Errorf("gc_blobs_scanned delta = %v, want 2", scannedAfter-scannedBefore)
	}
	if deletedAfter-deletedBefore != 2 {
		t.Errorf("gc_blobs_deleted delta = %v, want 2", deletedAfter-deletedBefore)
	}
}

func TestGCDryRunUpdatesWouldDeleteMetric(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	opts := gcOpts(true)
	opts.GCMinAge = "0s"
	log := zerolog.Nop()
	gc := newBlobGC(store, blob, opts, &log)

	setupSpaceEmpty(store, "space-1", "root-1")
	blob.blobs[BlobKey("space-1", "orphan")] = []byte("x")

	wouldDeleteBefore := getCounterValue(t, "kvfs_gc_would_delete_total", nil)

	gc.Run(context.Background())

	wouldDeleteAfter := getCounterValue(t, "kvfs_gc_would_delete_total", nil)
	if wouldDeleteAfter-wouldDeleteBefore != 1 {
		t.Errorf("gc_would_delete delta = %v, want 1", wouldDeleteAfter-wouldDeleteBefore)
	}
}

// --- kvfsDriver integration ---

func TestDriverShutdownStopsGC(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	log := zerolog.Nop()
	opts := gcOpts(true)

	d := &kvfsDriver{
		store: store,
		blob:  blob,
		opts:  opts,
		log:   &log,
		gc:    newBlobGC(store, blob, opts, &log),
	}
	d.gc.Start()
	time.Sleep(10 * time.Millisecond)

	err := d.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestDriverShutdownNoGC(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	log := zerolog.Nop()

	d := &kvfsDriver{
		store: store,
		blob:  blob,
		opts:  &Options{},
		log:   &log,
	}

	err := d.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Shutdown without GC: %v", err)
	}
}

// --- GC lock tests ---

func TestGC_AcquiresLockAndReleases(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	opts := gcOpts(false)
	opts.GCMinAge = "0s"
	log := zerolog.Nop()
	gc := newBlobGC(store, blob, opts, &log)

	setupSpaceEmpty(store, "space-1", "root-1")
	blob.blobs[BlobKey("space-1", "orphan")] = []byte("x")

	result := gc.Run(context.Background())

	if result.BlobsDeleted != 1 {
		t.Errorf("deleted = %d, want 1 (sweep should have run)", result.BlobsDeleted)
	}

	store.mu.Lock()
	_, lockHeld := store.locks["gc-sweep"]
	store.mu.Unlock()
	if lockHeld {
		t.Error("lock should be released after sweep completes")
	}
}

func TestGC_SecondPodSkips(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	opts := gcOpts(false)
	opts.GCMinAge = "0s"
	log := zerolog.Nop()

	gcA := newBlobGC(store, blob, opts, &log)
	gcA.holderID = "pod-a"
	gcB := newBlobGC(store, blob, opts, &log)
	gcB.holderID = "pod-b"

	setupSpaceEmpty(store, "space-1", "root-1")
	blob.blobs[BlobKey("space-1", "orphan")] = []byte("x")

	// Pod A holds the lock
	store.mu.Lock()
	store.locks["gc-sweep"] = &kvLockEntry{
		Holder:    "pod-a",
		ExpiresAt: time.Now().Add(30 * time.Minute).UnixNano(),
	}
	store.mu.Unlock()

	result := gcB.Run(context.Background())

	if result.BlobsScanned != 0 {
		t.Errorf("scanned = %d, want 0 (pod-b should skip)", result.BlobsScanned)
	}
	if result.BlobsDeleted != 0 {
		t.Errorf("deleted = %d, want 0 (pod-b should skip)", result.BlobsDeleted)
	}

	if _, ok := blob.blobs[BlobKey("space-1", "orphan")]; !ok {
		t.Error("blob should not be deleted by skipped pod")
	}
}

func TestGC_StaleLockReclaimed(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	opts := gcOpts(false)
	opts.GCMinAge = "0s"
	log := zerolog.Nop()
	gc := newBlobGC(store, blob, opts, &log)
	gc.holderID = "pod-b"

	setupSpaceEmpty(store, "space-1", "root-1")
	blob.blobs[BlobKey("space-1", "orphan")] = []byte("x")

	// Pre-insert an expired lock from a dead pod
	store.mu.Lock()
	store.locks["gc-sweep"] = &kvLockEntry{
		Holder:    "pod-a-dead",
		ExpiresAt: time.Now().Add(-1 * time.Minute).UnixNano(),
	}
	store.mu.Unlock()

	result := gc.Run(context.Background())

	if result.BlobsDeleted != 1 {
		t.Errorf("deleted = %d, want 1 (stale lock should be reclaimed)", result.BlobsDeleted)
	}
}

// --- reconcileChildren tests ---

func TestReconcileChildren_StaleEntryFromAbortedMove(t *testing.T) {
	// Simulates the post-crash state after a Move that completed Phase 2a
	// (node updated to new parent) and Phase 2b (added to new parent's
	// children) but crashed before Phase 2c (remove from old parent's
	// children). Expected outcome: GC reconcileChildren removes the stale
	// entry from old parent, trusting node.ParentID as truth.
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)

	store.nodes["s1.old-dir"] = &NodeEntry{ID: "old-dir", SpaceID: "s1", Type: NodeTypeDir}
	store.nodes["s1.new-dir"] = &NodeEntry{ID: "new-dir", SpaceID: "s1", Type: NodeTypeDir}
	store.nodes["s1.victim"] = &NodeEntry{ID: "victim", SpaceID: "s1", ParentID: "new-dir", Name: "file.txt", Type: NodeTypeFile}
	store.nodeRevs["s1.old-dir"] = 1
	store.nodeRevs["s1.new-dir"] = 1
	store.nodeRevs["s1.victim"] = 1
	store.children["s1.old-dir"] = ChildMap{"file.txt": "victim"}
	store.childRevs["s1.old-dir"] = 1
	store.children["s1.new-dir"] = ChildMap{"file.txt": "victim"}
	store.childRevs["s1.new-dir"] = 1

	reconciled := gc.reconcileChildren(context.Background(), "s1")

	if reconciled != 1 {
		t.Errorf("reconciled = %d, want 1", reconciled)
	}
	oldChildren, _, _ := store.GetChildren("s1", "old-dir")
	if _, present := oldChildren["file.txt"]; present {
		t.Error("stale entry should have been removed from old-dir")
	}
	newChildren, _, _ := store.GetChildren("s1", "new-dir")
	if newChildren["file.txt"] != "victim" {
		t.Error("entry on new-dir should remain")
	}
}

func TestReconcileChildren_DanglingEntryAfterDelete(t *testing.T) {
	// A children entry pointing at a node that no longer exists.
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)

	store.nodes["s1.dir"] = &NodeEntry{ID: "dir", SpaceID: "s1", Type: NodeTypeDir}
	store.nodeRevs["s1.dir"] = 1
	store.children["s1.dir"] = ChildMap{"ghost.txt": "no-such-node"}
	store.childRevs["s1.dir"] = 1

	reconciled := gc.reconcileChildren(context.Background(), "s1")
	if reconciled != 1 {
		t.Errorf("reconciled = %d, want 1", reconciled)
	}
	children, _, _ := store.GetChildren("s1", "dir")
	if _, present := children["ghost.txt"]; present {
		t.Error("dangling entry should have been removed")
	}
}

func TestReconcileChildren_HealthyChildrenUntouched(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)

	store.nodes["s1.dir"] = &NodeEntry{ID: "dir", SpaceID: "s1", Type: NodeTypeDir}
	store.nodes["s1.f1"] = &NodeEntry{ID: "f1", SpaceID: "s1", ParentID: "dir", Name: "a.txt", Type: NodeTypeFile}
	store.nodes["s1.f2"] = &NodeEntry{ID: "f2", SpaceID: "s1", ParentID: "dir", Name: "b.txt", Type: NodeTypeFile}
	store.nodeRevs["s1.dir"] = 1
	store.nodeRevs["s1.f1"] = 1
	store.nodeRevs["s1.f2"] = 1
	store.children["s1.dir"] = ChildMap{"a.txt": "f1", "b.txt": "f2"}
	store.childRevs["s1.dir"] = 1

	reconciled := gc.reconcileChildren(context.Background(), "s1")
	if reconciled != 0 {
		t.Errorf("reconciled = %d, want 0 on a healthy tree", reconciled)
	}
	children, _, _ := store.GetChildren("s1", "dir")
	if len(children) != 2 {
		t.Errorf("children count = %d, want 2", len(children))
	}
}

func TestReconcileChildren_DryRunDoesNotMutate(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, true) // dry-run

	store.nodes["s1.old-dir"] = &NodeEntry{ID: "old-dir", SpaceID: "s1", Type: NodeTypeDir}
	store.nodes["s1.new-dir"] = &NodeEntry{ID: "new-dir", SpaceID: "s1", Type: NodeTypeDir}
	store.nodes["s1.victim"] = &NodeEntry{ID: "victim", SpaceID: "s1", ParentID: "new-dir", Name: "file.txt", Type: NodeTypeFile}
	store.nodeRevs["s1.old-dir"] = 1
	store.nodeRevs["s1.new-dir"] = 1
	store.nodeRevs["s1.victim"] = 1
	store.children["s1.old-dir"] = ChildMap{"file.txt": "victim"}
	store.childRevs["s1.old-dir"] = 1
	store.children["s1.new-dir"] = ChildMap{"file.txt": "victim"}
	store.childRevs["s1.new-dir"] = 1

	reconciled := gc.reconcileChildren(context.Background(), "s1")
	if reconciled != 1 {
		t.Errorf("reconciled (counter) = %d, want 1 even in dry-run", reconciled)
	}
	oldChildren, _, _ := store.GetChildren("s1", "old-dir")
	if _, present := oldChildren["file.txt"]; !present {
		t.Error("dry-run must not mutate; stale entry still expected on old-dir")
	}
}

// --- reconcileTrash tests ---

func TestReconcileTrash_StaleEntryFromAbortedRestore(t *testing.T) {
	// A file was successfully restored (live in oc-nodes + listed in its
	// parent's children) but the DeleteTrash call never completed.
	// Expected: GC removes the stale trash entry; node remains live.
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)

	store.nodes["s1.dir"] = &NodeEntry{ID: "dir", SpaceID: "s1", Type: NodeTypeDir}
	store.nodes["s1.victim"] = &NodeEntry{ID: "victim", SpaceID: "s1", ParentID: "dir", Name: "file.txt", Type: NodeTypeFile}
	store.nodeRevs["s1.dir"] = 1
	store.nodeRevs["s1.victim"] = 1
	store.children["s1.dir"] = ChildMap{"file.txt": "victim"}
	store.childRevs["s1.dir"] = 1
	store.trash["s1.t1"] = &TrashEntry{Key: "t1", NodeID: "victim", SpaceID: "s1"}

	reconciled := gc.reconcileTrash(context.Background(), "s1")

	if reconciled != 1 {
		t.Errorf("reconciled = %d, want 1", reconciled)
	}
	if _, err := store.GetTrash("s1", "t1"); err == nil {
		t.Error("stale trash entry should have been deleted")
	}
	node, _, _ := store.GetNode("s1", "victim")
	if node == nil {
		t.Error("live node must not be deleted by trash reconciler")
	}
}

func TestReconcileTrash_LegitimateTrashUntouched(t *testing.T) {
	// A trash entry whose node does NOT exist in oc-nodes (normal trash).
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)

	store.nodes["s1.dir"] = &NodeEntry{ID: "dir", SpaceID: "s1", Type: NodeTypeDir}
	store.nodeRevs["s1.dir"] = 1
	store.children["s1.dir"] = ChildMap{}
	store.childRevs["s1.dir"] = 1
	store.trash["s1.t1"] = &TrashEntry{Key: "t1", NodeID: "deleted-node", SpaceID: "s1"}

	reconciled := gc.reconcileTrash(context.Background(), "s1")
	if reconciled != 0 {
		t.Errorf("reconciled = %d, want 0 for legitimate trash", reconciled)
	}
	if _, err := store.GetTrash("s1", "t1"); err != nil {
		t.Error("legitimate trash entry must be preserved")
	}
}

func TestReconcileTrash_TrashedDirectoryUntouched(t *testing.T) {
	// A directory node exists in oc-nodes (KVFS keeps dir nodes alive
	// during trash) but is NOT listed in any parent's children map.
	// The trash entry should be preserved — the directory is still trashed.
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)

	store.nodes["s1.root"] = &NodeEntry{ID: "root", SpaceID: "s1", Type: NodeTypeDir}
	store.nodes["s1.trashed-dir"] = &NodeEntry{ID: "trashed-dir", SpaceID: "s1", ParentID: "root", Type: NodeTypeDir}
	store.nodeRevs["s1.root"] = 1
	store.nodeRevs["s1.trashed-dir"] = 1
	store.children["s1.root"] = ChildMap{} // trashed-dir NOT listed
	store.childRevs["s1.root"] = 1
	store.children["s1.trashed-dir"] = ChildMap{} // empty subtree
	store.childRevs["s1.trashed-dir"] = 1
	store.trash["s1.t1"] = &TrashEntry{Key: "t1", NodeID: "trashed-dir", SpaceID: "s1"}

	reconciled := gc.reconcileTrash(context.Background(), "s1")
	if reconciled != 0 {
		t.Errorf("reconciled = %d, want 0 for trashed directory", reconciled)
	}
	if _, err := store.GetTrash("s1", "t1"); err != nil {
		t.Error("trash entry for unreachable directory must be preserved")
	}
}

func TestReconcileTrash_DryRunDoesNotMutate(t *testing.T) {
	// Same setup as StaleEntryFromAbortedRestore but with dry-run.
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, true) // dry-run

	store.nodes["s1.dir"] = &NodeEntry{ID: "dir", SpaceID: "s1", Type: NodeTypeDir}
	store.nodes["s1.victim"] = &NodeEntry{ID: "victim", SpaceID: "s1", ParentID: "dir", Name: "file.txt", Type: NodeTypeFile}
	store.nodeRevs["s1.dir"] = 1
	store.nodeRevs["s1.victim"] = 1
	store.children["s1.dir"] = ChildMap{"file.txt": "victim"}
	store.childRevs["s1.dir"] = 1
	store.trash["s1.t1"] = &TrashEntry{Key: "t1", NodeID: "victim", SpaceID: "s1"}

	reconciled := gc.reconcileTrash(context.Background(), "s1")
	if reconciled != 1 {
		t.Errorf("reconciled (counter) = %d, want 1 even in dry-run", reconciled)
	}
	if _, err := store.GetTrash("s1", "t1"); err != nil {
		t.Error("dry-run must not delete; stale trash entry still expected")
	}
}

// --- runInitialSweepWithRetry tests ---

// withShortGCTimings shrinks the GC retry/lease timings for the duration
// of one unit test so the retry path is exercisable in milliseconds rather
// than the production budget. The retry budget tracks 2×ttl (the old
// "2 × lock TTL" deadline the retry tests rely on) and the lease renew
// interval tracks ttl/3. Restores defaults on cleanup.
func withShortGCTimings(t *testing.T, ttl, retry time.Duration) {
	prevTTL := gcLeaseTTL
	prevRenew := gcLeaseRenewInterval
	prevRetry := initialSweepRetryInterval
	prevBudget := initialSweepRetryBudget
	gcLeaseTTL = ttl
	gcLeaseRenewInterval = ttl / 3
	if gcLeaseRenewInterval <= 0 {
		gcLeaseRenewInterval = time.Millisecond
	}
	initialSweepRetryInterval = retry
	initialSweepRetryBudget = 2 * ttl
	t.Cleanup(func() {
		gcLeaseTTL = prevTTL
		gcLeaseRenewInterval = prevRenew
		initialSweepRetryInterval = prevRetry
		initialSweepRetryBudget = prevBudget
	})
}

// withShortGCLease overrides just the sweep-lease TTL and renew interval, for
// the lease/heartbeat/fencing tests that need fine control over both.
func withShortGCLease(t *testing.T, ttl, renew time.Duration) {
	prevTTL := gcLeaseTTL
	prevRenew := gcLeaseRenewInterval
	gcLeaseTTL = ttl
	gcLeaseRenewInterval = renew
	t.Cleanup(func() {
		gcLeaseTTL = prevTTL
		gcLeaseRenewInterval = prevRenew
	})
}

// lockHolder safely reads the current holder of a lock via the mock's
// own mutex so the race detector stays happy.
func lockHolder(store *mockMetadataStore, key string) string {
	store.mu.Lock()
	defer store.mu.Unlock()
	if entry, ok := store.locks[key]; ok {
		return entry.Holder
	}
	return ""
}

// TestRunInitialSweepWithRetry_LockBusyThenSucceeds — lock initially held
// by a "stale prior-pod" entry, expires shortly, retry loop acquires it
// on the next iteration and sweeps successfully.
func TestRunInitialSweepWithRetry_LockBusyThenSucceeds(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	withShortGCTimings(t, 200*time.Millisecond, 50*time.Millisecond)
	gc := testGC(store, blob, false)
	gc.holderID = "new-pod"

	store.mu.Lock()
	store.locks["gc-sweep"] = &kvLockEntry{
		Holder:    "prior-pod",
		ExpiresAt: time.Now().Add(100 * time.Millisecond).UnixNano(),
	}
	store.mu.Unlock()

	done := make(chan struct{})
	go func() {
		gc.runInitialSweepWithRetry()
		close(done)
	}()

	select {
	case <-done:
		if h := lockHolder(store, "gc-sweep"); h != "" {
			t.Errorf("lock entry still present after successful sweep; got holder=%q", h)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runInitialSweepWithRetry did not return within 3 s")
	}
}

// TestRunInitialSweepWithRetry_DeadlineExceeded — lock stays held for the
// entire test window; retry loop gives up after 2 × gcLockTTL without
// panicking or leaking the goroutine.
func TestRunInitialSweepWithRetry_DeadlineExceeded(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	// Tiny TTL + retry interval so the 2×TTL deadline fires quickly.
	withShortGCTimings(t, 200*time.Millisecond, 100*time.Millisecond)
	gc := testGC(store, blob, false)
	gc.holderID = "new-pod"

	store.mu.Lock()
	store.locks["gc-sweep"] = &kvLockEntry{
		Holder:    "stuck-pod",
		ExpiresAt: time.Now().Add(1 * time.Hour).UnixNano(),
	}
	store.mu.Unlock()

	done := make(chan struct{})
	go func() {
		gc.runInitialSweepWithRetry()
		close(done)
	}()

	select {
	case <-done:
		if h := lockHolder(store, "gc-sweep"); h != "stuck-pod" {
			t.Errorf("stuck-pod's lock entry mangled: holder=%q", h)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runInitialSweepWithRetry deadline path did not return")
	}
}

// TestRunInitialSweepWithRetry_StopAbortsRetry — closing stopCh during the
// retry wait returns promptly without finishing the sweep.
func TestRunInitialSweepWithRetry_StopAbortsRetry(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	withShortGCTimings(t, 5*time.Second, 5*time.Second)
	gc := testGC(store, blob, false)
	gc.holderID = "new-pod"

	store.mu.Lock()
	store.locks["gc-sweep"] = &kvLockEntry{
		Holder:    "other-pod",
		ExpiresAt: time.Now().Add(1 * time.Hour).UnixNano(),
	}
	store.mu.Unlock()

	done := make(chan struct{})
	go func() {
		gc.runInitialSweepWithRetry()
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	close(gc.stopCh)

	select {
	case <-done:
		// returned via stopCh path
	case <-time.After(2 * time.Second):
		t.Fatal("runInitialSweepWithRetry did not honour stopCh")
	}
}

// TestGCRunDoesNotWriteUploadAgeGauge guards the single-writer invariant: the
// upload-staleness gauges belong to the sampler, never to gc.Run.
func TestGCRunDoesNotWriteUploadAgeGauge(t *testing.T) {
	// Sentinel that the helper would never produce for the snapshot below.
	const sentinel = 123456.0
	OldestUploadAgeSeconds.WithLabelValues("oc").Set(sentinel)
	UploadSessionsTotal.WithLabelValues("oc").Set(sentinel)

	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)

	setupSpaceEmpty(store, "space-1", "root-1")
	store.uploads["upload-1"] = &UploadSession{
		ID: "upload-1", SpaceID: "space-1", BlobID: "blob-uploading",
		Expires: time.Now().Add(uploadSessionTTL).Unix(),
	}

	gc.Run(context.Background())

	oc := map[string]string{"prefix": "oc"}
	if v := getGaugeValue(t, "kvfs_oldest_upload_age_seconds", oc); v != sentinel {
		t.Errorf("gc.Run wrote kvfs_oldest_upload_age_seconds (=%v); the gauge must be owned by the sampler, not GC", v)
	}
	if v := getGaugeValue(t, "kvfs_upload_sessions_total", oc); v != sentinel {
		t.Errorf("gc.Run wrote kvfs_upload_sessions_total (=%v); the gauge must be owned by the sampler, not GC", v)
	}

	// Reset so other tests see a clean gauge.
	updateUploadAgeMetrics("oc", nil)
}

// --- Upload-session reap policy tests ---

// reapEntries runs cleanExpiredUploads over the store's current uploads
// bucket and returns the result. Shared by the reap-policy tests below.
func reapEntries(t *testing.T, gc *blobGC, store *mockMetadataStore) GCResult {
	t.Helper()
	entries, err := store.ListAllUploadEntries()
	if err != nil {
		t.Fatalf("ListAllUploadEntries: %v", err)
	}
	var result GCResult
	gc.cleanExpiredUploads(context.Background(), entries, &result)
	return result
}

func TestCleanExpiredUploadsReapsExpired(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)

	store.uploads["expired-1"] = &UploadSession{
		ID: "expired-1", SpaceID: "s1", BlobID: "b1",
		Expires: time.Now().Add(-time.Hour).Unix(),
	}
	store.uploads["live-1"] = &UploadSession{
		ID: "live-1", SpaceID: "s1", BlobID: "b2",
		Expires: time.Now().Add(uploadSessionTTL).Unix(),
	}

	result := reapEntries(t, gc, store)

	if _, ok := store.uploads["expired-1"]; ok {
		t.Error("expired session not reaped")
	}
	if _, ok := store.uploads["live-1"]; !ok {
		t.Error("live session reaped")
	}
	if result.ExpiredUploadsCleaned != 1 {
		t.Errorf("ExpiredUploadsCleaned = %d, want 1", result.ExpiredUploadsCleaned)
	}
	if result.CorruptUploadsReaped != 0 {
		t.Errorf("CorruptUploadsReaped = %d, want 0", result.CorruptUploadsReaped)
	}
}

func TestCleanExpiredUploadsReapsNoExpiryAfterTTL(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)

	// Expires==0 was previously immortal: the reaper only matched
	// Expires>0, so a legacy/partial-write session lived forever.
	store.uploads["no-expiry"] = &UploadSession{ID: "no-expiry", SpaceID: "s1"}
	store.uploadCreated["no-expiry"] = time.Now().Add(-uploadSessionTTL - time.Hour)

	result := reapEntries(t, gc, store)

	if _, ok := store.uploads["no-expiry"]; ok {
		t.Error("over-TTL no-expiry session not reaped")
	}
	if result.ExpiredUploadsCleaned != 1 {
		t.Errorf("ExpiredUploadsCleaned = %d, want 1", result.ExpiredUploadsCleaned)
	}
}

func TestCleanExpiredUploadsKeepsYoungNoExpiry(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)

	store.uploads["young"] = &UploadSession{ID: "young", SpaceID: "s1"}
	store.uploadCreated["young"] = time.Now().Add(-time.Hour)

	result := reapEntries(t, gc, store)

	if _, ok := store.uploads["young"]; !ok {
		t.Error("young no-expiry session reaped before TTL")
	}
	if result.ExpiredUploadsCleaned != 0 {
		t.Errorf("ExpiredUploadsCleaned = %d, want 0", result.ExpiredUploadsCleaned)
	}
}

func TestCleanExpiredUploadsReapsCorruptAfterTTL(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)

	// Corrupt entries are invisible to typed listings (listAll skips
	// unmarshal failures) and were therefore immortal too.
	store.corruptUploads["corrupt-1"] = time.Now().Add(-uploadSessionTTL - time.Hour)

	result := reapEntries(t, gc, store)

	if _, ok := store.corruptUploads["corrupt-1"]; ok {
		t.Error("over-TTL corrupt entry not reaped")
	}
	if result.CorruptUploadsReaped != 1 {
		t.Errorf("CorruptUploadsReaped = %d, want 1", result.CorruptUploadsReaped)
	}
	if result.ExpiredUploadsCleaned != 0 {
		t.Errorf("ExpiredUploadsCleaned = %d, want 0", result.ExpiredUploadsCleaned)
	}
}

func TestCleanExpiredUploadsKeepsYoungCorrupt(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)

	store.corruptUploads["corrupt-young"] = time.Now().Add(-time.Hour)

	result := reapEntries(t, gc, store)

	if _, ok := store.corruptUploads["corrupt-young"]; !ok {
		t.Error("young corrupt entry reaped before TTL")
	}
	if result.CorruptUploadsReaped != 0 {
		t.Errorf("CorruptUploadsReaped = %d, want 0", result.CorruptUploadsReaped)
	}
}

func TestCleanExpiredUploadsZeroCreatedNeverReaped(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)

	// Safety guard: a missing server timestamp must never cause a reap.
	store.uploads["no-ts"] = &UploadSession{ID: "no-ts", SpaceID: "s1"}
	store.uploadCreated["no-ts"] = time.Time{}
	store.corruptUploads["corrupt-no-ts"] = time.Time{}

	result := reapEntries(t, gc, store)

	if _, ok := store.uploads["no-ts"]; !ok {
		t.Error("zero-Created no-expiry session reaped")
	}
	if _, ok := store.corruptUploads["corrupt-no-ts"]; !ok {
		t.Error("zero-Created corrupt entry reaped")
	}
	if result.ExpiredUploadsCleaned != 0 || result.CorruptUploadsReaped != 0 {
		t.Errorf("result = %+v, want no reaps", result)
	}
}

func TestCleanExpiredUploadsDryRunDoesNotDelete(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, true)

	store.uploads["expired-mp"] = &UploadSession{
		ID: "expired-mp", SpaceID: "s1", BlobID: "b1", S3MultipartID: "mp-1",
		Expires: time.Now().Add(-time.Hour).Unix(),
	}
	store.corruptUploads["corrupt-old"] = time.Now().Add(-uploadSessionTTL - time.Hour)

	result := reapEntries(t, gc, store)

	if _, ok := store.uploads["expired-mp"]; !ok {
		t.Error("dry-run deleted an upload session")
	}
	if _, ok := store.corruptUploads["corrupt-old"]; !ok {
		t.Error("dry-run deleted a corrupt entry")
	}
	if len(blob.abortCalled) != 0 {
		t.Errorf("dry-run aborted multipart uploads: %v", blob.abortCalled)
	}
	if result.WouldDelete != 2 {
		t.Errorf("WouldDelete = %d, want 2", result.WouldDelete)
	}
	if result.ExpiredUploadsCleaned != 0 || result.CorruptUploadsReaped != 0 {
		t.Errorf("result = %+v, want no reap counts in dry-run", result)
	}
}

func TestCleanExpiredUploadsAbortsMultipartOnReap(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)

	store.uploads["expired-mp"] = &UploadSession{
		ID: "expired-mp", SpaceID: "s1", BlobID: "b1", S3MultipartID: "mp-1",
		Expires: time.Now().Add(-time.Hour).Unix(),
	}

	reapEntries(t, gc, store)

	if len(blob.abortCalled) != 1 || blob.abortCalled[0] != "mp-1" {
		t.Errorf("AbortMultipartUpload calls = %v, want [mp-1]", blob.abortCalled)
	}
}

func TestGCRunPurgesUploadTombstones(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)

	setupSpaceEmpty(store, "space-1", "root-1")
	gc.Run(context.Background())

	if store.purgeDeletedUploadsCalls != 1 {
		t.Errorf("PurgeDeletedUploads calls = %d, want 1", store.purgeDeletedUploadsCalls)
	}
}

func TestGCRunDryRunSkipsTombstonePurge(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, true)

	setupSpaceEmpty(store, "space-1", "root-1")
	gc.Run(context.Background())

	if store.purgeDeletedUploadsCalls != 0 {
		t.Errorf("PurgeDeletedUploads calls = %d, want 0 in dry-run", store.purgeDeletedUploadsCalls)
	}
}

func TestGCRunReapsUploadsAndCountsMetrics(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)

	setupSpaceEmpty(store, "space-1", "root-1")
	store.uploads["expired-1"] = &UploadSession{
		ID: "expired-1", SpaceID: "space-1", BlobID: "b1",
		Expires: time.Now().Add(-time.Hour).Unix(),
	}
	store.corruptUploads["corrupt-1"] = time.Now().Add(-uploadSessionTTL - time.Hour)

	expiredBefore := getCounterValue(t, "kvfs_gc_expired_uploads_cleaned_total", nil)
	corruptBefore := getCounterValue(t, "kvfs_gc_corrupt_uploads_reaped_total", nil)

	result := gc.Run(context.Background())

	if result.ExpiredUploadsCleaned != 1 {
		t.Errorf("ExpiredUploadsCleaned = %d, want 1", result.ExpiredUploadsCleaned)
	}
	if result.CorruptUploadsReaped != 1 {
		t.Errorf("CorruptUploadsReaped = %d, want 1", result.CorruptUploadsReaped)
	}
	if d := getCounterValue(t, "kvfs_gc_expired_uploads_cleaned_total", nil) - expiredBefore; d != 1 {
		t.Errorf("kvfs_gc_expired_uploads_cleaned_total delta = %v, want 1", d)
	}
	if d := getCounterValue(t, "kvfs_gc_corrupt_uploads_reaped_total", nil) - corruptBefore; d != 1 {
		t.Errorf("kvfs_gc_corrupt_uploads_reaped_total delta = %v, want 1", d)
	}
}
