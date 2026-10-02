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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/rs/zerolog"

	"github.com/opencloud-eu/reva/v2/pkg/storage"
)

// --- trimVersions unit tests ---

func TestTrimVersions_Unlimited(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	d.opts.MaxVersions = 0

	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	for i := 0; i < 10; i++ {
		store.versions = append(store.versions, &VersionEntry{
			Key: fmt.Sprintf("v%d", i), NodeID: "f1", SpaceID: "s1",
			BlobID: fmt.Sprintf("b%d", i), MTime: int64(i * 1000),
		})
	}

	d.trimVersions(context.Background(), "s1", "f1")

	versions, _ := store.ListVersions("s1", "f1")
	if len(versions) != 10 {
		t.Errorf("unlimited: expected 10 versions, got %d", len(versions))
	}
}

func TestTrimVersions_LimitThree(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	d.opts.MaxVersions = 3

	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	for i := 0; i < 5; i++ {
		blobID := fmt.Sprintf("vblob-%d", i)
		store.versions = append(store.versions, &VersionEntry{
			Key: fmt.Sprintf("v%d", i), NodeID: "f1", SpaceID: "s1",
			BlobID: blobID, MTime: int64((i + 1) * 1000), Size: 100,
		})
		blob.blobs[BlobKey("s1", blobID)] = []byte("data")
	}

	d.trimVersions(context.Background(), "s1", "f1")

	versions, _ := store.ListVersions("s1", "f1")
	if len(versions) != 3 {
		t.Fatalf("expected 3 versions after trim, got %d", len(versions))
	}

	// Verify newest 3 remain (MTime 5000, 4000, 3000)
	for _, v := range versions {
		if v.MTime < 3000 {
			t.Errorf("version with MTime %d should have been trimmed", v.MTime)
		}
	}

	// Trim removes entries only; every blob stays for GC to judge.
	for i := 0; i < 5; i++ {
		key := BlobKey("s1", fmt.Sprintf("vblob-%d", i))
		if _, exists := blob.blobs[key]; !exists {
			t.Errorf("blob vblob-%d must survive the trim", i)
		}
	}
	if len(blob.deleteCalled) != 0 {
		t.Errorf("trim must not delete blobs, deleted %v", blob.deleteCalled)
	}
}

func TestTrimVersions_UnderLimit(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	d.opts.MaxVersions = 5

	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	for i := 0; i < 3; i++ {
		store.versions = append(store.versions, &VersionEntry{
			Key: fmt.Sprintf("v%d", i), NodeID: "f1", SpaceID: "s1",
			BlobID: fmt.Sprintf("b%d", i), MTime: int64(i * 1000),
		})
	}

	d.trimVersions(context.Background(), "s1", "f1")

	versions, _ := store.ListVersions("s1", "f1")
	if len(versions) != 3 {
		t.Errorf("under limit: expected 3 versions, got %d", len(versions))
	}
}

func TestTrimVersions_ExactlyAtLimit(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	d.opts.MaxVersions = 3

	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	for i := 0; i < 3; i++ {
		store.versions = append(store.versions, &VersionEntry{
			Key: fmt.Sprintf("v%d", i), NodeID: "f1", SpaceID: "s1",
			BlobID: fmt.Sprintf("b%d", i), MTime: int64(i * 1000),
		})
	}

	d.trimVersions(context.Background(), "s1", "f1")

	versions, _ := store.ListVersions("s1", "f1")
	if len(versions) != 3 {
		t.Errorf("at limit: expected 3 versions, got %d", len(versions))
	}
}

func TestTrimVersions_RemovesEntriesOnly(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	d.opts.MaxVersions = 1

	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	for i := 0; i < 3; i++ {
		blobID := fmt.Sprintf("trimblob-%d", i)
		store.versions = append(store.versions, &VersionEntry{
			Key: fmt.Sprintf("v%d", i), NodeID: "f1", SpaceID: "s1",
			BlobID: blobID, MTime: int64((i + 1) * 1000),
		})
		blob.blobs[BlobKey("s1", blobID)] = []byte("ver")
	}

	d.trimVersions(context.Background(), "s1", "f1")

	versions, _ := store.ListVersions("s1", "f1")
	if len(versions) != 1 {
		t.Fatalf("expected 1 version after trim, got %d", len(versions))
	}
	if versions[0].MTime != 3000 {
		t.Errorf("remaining version should be newest (MTime=3000), got %d", versions[0].MTime)
	}
	for i := 0; i < 3; i++ {
		if _, exists := blob.blobs[BlobKey("s1", fmt.Sprintf("trimblob-%d", i))]; !exists {
			t.Errorf("trimblob-%d must survive the trim (GC reclaims unreferenced blobs)", i)
		}
	}
	if len(blob.deleteCalled) != 0 {
		t.Errorf("trim must not delete blobs, deleted %v", blob.deleteCalled)
	}
}

func TestTrimVersions_IsolatedPerNode(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	d.opts.MaxVersions = 2

	setupSpaceWithFile(store, "s1", "root", "f1", "a.txt")

	// 3 versions for f1, 3 versions for f2
	for i := 0; i < 3; i++ {
		store.versions = append(store.versions, &VersionEntry{
			Key: fmt.Sprintf("v1-%d", i), NodeID: "f1", SpaceID: "s1",
			BlobID: fmt.Sprintf("f1blob-%d", i), MTime: int64((i + 1) * 1000),
		})
		store.versions = append(store.versions, &VersionEntry{
			Key: fmt.Sprintf("v2-%d", i), NodeID: "f2", SpaceID: "s1",
			BlobID: fmt.Sprintf("f2blob-%d", i), MTime: int64((i + 1) * 1000),
		})
	}

	d.trimVersions(context.Background(), "s1", "f1")

	f1Versions, _ := store.ListVersions("s1", "f1")
	if len(f1Versions) != 2 {
		t.Errorf("f1 should have 2 versions, got %d", len(f1Versions))
	}

	f2Versions, _ := store.ListVersions("s1", "f2")
	if len(f2Versions) != 3 {
		t.Errorf("f2 should be untouched (3 versions), got %d", len(f2Versions))
	}
}

// --- Integration tests: Upload with MaxVersions ---

func TestUpload_TrimsVersions(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	d.opts.MaxVersions = 2

	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
	blob.blobs[BlobKey("s1", "old-blob")] = []byte("original")

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./file.txt",
	}

	// Upload 3 overwrites → creates 3 versions, trim keeps 2
	for i := 0; i < 3; i++ {
		content := []byte(fmt.Sprintf("content-%d", i))
		_, err := d.Upload(ctx, storage.UploadRequest{
			Ref:    ref,
			Body:   io.NopCloser(bytes.NewReader(content)),
			Length: int64(len(content)),
		}, nil)
		if err != nil {
			t.Fatalf("upload %d failed: %v", i, err)
		}
	}

	versions, _ := store.ListVersions("s1", "f1")
	if len(versions) != 2 {
		t.Errorf("expected 2 versions after 3 overwrites with MaxVersions=2, got %d", len(versions))
	}
}

func TestUpload_NoTrimWhenUnlimited(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	d.opts.MaxVersions = 0

	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
	blob.blobs[BlobKey("s1", "old-blob")] = []byte("original")

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./file.txt",
	}

	for i := 0; i < 5; i++ {
		content := []byte(fmt.Sprintf("content-%d", i))
		_, err := d.Upload(ctx, storage.UploadRequest{
			Ref:    ref,
			Body:   io.NopCloser(bytes.NewReader(content)),
			Length: int64(len(content)),
		}, nil)
		if err != nil {
			t.Fatalf("upload %d failed: %v", i, err)
		}
	}

	versions, _ := store.ListVersions("s1", "f1")
	if len(versions) != 5 {
		t.Errorf("expected 5 versions (unlimited), got %d", len(versions))
	}
}

// --- Restore / trim interplay ---

func fileRef() *provider.Reference {
	return &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./file.txt",
	}
}

func f1Ref() *provider.Reference {
	return &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"}}
}

// newVersionFixture returns a driver with file s1/f1 whose head is blob
// "old-blob" holding "original" at MTime 1000, so its snapshot is always the
// oldest entry.
func newVersionFixture(maxVersions int) (*kvfsDriver, *mockMetadataStore, *mockBlobStore) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	d.opts.MaxVersions = maxVersions
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
	blob.blobs[BlobKey("s1", "old-blob")] = []byte("original")
	return d, store, blob
}

func overwriteFile(d *kvfsDriver, content string) error {
	_, err := d.Upload(testContext(), storage.UploadRequest{
		Ref:    fileRef(),
		Body:   io.NopCloser(bytes.NewReader([]byte(content))),
		Length: int64(len(content)),
	}, nil)
	return err
}

func mustOverwrite(t *testing.T, d *kvfsDriver, content string) {
	t.Helper()
	if err := overwriteFile(d, content); err != nil {
		t.Fatalf("overwrite %q: %v", content, err)
	}
}

func readHead(t *testing.T, d *kvfsDriver) string {
	t.Helper()
	_, rc, err := d.Download(testContext(), f1Ref(), nil)
	if err != nil {
		t.Fatalf("download head: %v", err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read head: %v", err)
	}
	return string(b)
}

// versionKeyFor returns the key of f1's version entry whose blob holds content.
func versionKeyFor(t *testing.T, store *mockMetadataStore, blob *mockBlobStore, content string) string {
	t.Helper()
	versions, _ := store.ListVersions("s1", "f1")
	blob.mu.Lock()
	defer blob.mu.Unlock()
	for _, v := range versions {
		if string(blob.blobs[BlobKey("s1", v.BlobID)]) == content {
			return v.Key
		}
	}
	t.Fatalf("no version entry holds %q", content)
	return ""
}

func versionContents(t *testing.T, store *mockMetadataStore, blob *mockBlobStore) map[string]bool {
	t.Helper()
	versions, _ := store.ListVersions("s1", "f1")
	blob.mu.Lock()
	defer blob.mu.Unlock()
	got := map[string]bool{}
	for _, v := range versions {
		got[string(blob.blobs[BlobKey("s1", v.BlobID)])] = true
	}
	return got
}

// assertReferencedBlobsExist checks that every blob referenced by a node,
// version, trash entry or upload session of s1 is still stored.
func assertReferencedBlobsExist(t *testing.T, store *mockMetadataStore, blob *mockBlobStore) {
	t.Helper()
	var refs []string
	nodes, _ := store.ListNodesBySpace("s1")
	for _, n := range nodes {
		refs = append(refs, n.BlobID)
	}
	versions, _ := store.ListVersionsBySpace("s1")
	for _, v := range versions {
		refs = append(refs, v.BlobID)
	}
	trash, _ := store.ListTrash("s1")
	for _, e := range trash {
		refs = append(refs, e.Node.BlobID)
	}
	store.mu.Lock()
	for _, u := range store.uploads {
		if u.SpaceID == "s1" {
			refs = append(refs, u.BlobID)
		}
	}
	store.mu.Unlock()
	blob.mu.Lock()
	defer blob.mu.Unlock()
	for _, id := range refs {
		if id == "" {
			continue
		}
		if _, ok := blob.blobs[BlobKey("s1", id)]; !ok {
			t.Errorf("blob %s is still referenced but was deleted", id)
		}
	}
}

func ageAllBlobs(blob *mockBlobStore, at time.Time) {
	blob.mu.Lock()
	defer blob.mu.Unlock()
	blob.blobTimes = map[string]time.Time{}
	for k := range blob.blobs {
		blob.blobTimes[k] = at
	}
}

func setPutNodeErr(store *mockMetadataStore, err error) {
	store.mu.Lock()
	store.putNodeErr = err
	store.mu.Unlock()
}

func deleted(blob *mockBlobStore, key string) bool {
	blob.mu.Lock()
	defer blob.mu.Unlock()
	for _, k := range blob.deleteCalled {
		if k == key {
			return true
		}
	}
	return false
}

func TestTrimVersions_NeverDeletesSharedBlobs(t *testing.T) {
	cases := []struct {
		name string
		ref  func(store *mockMetadataStore)
	}{
		{"head", func(store *mockMetadataStore) { store.nodes["s1.f1"].BlobID = "shared" }},
		{"newer version", func(store *mockMetadataStore) {
			store.versions = append(store.versions, &VersionEntry{Key: "vC", NodeID: "f1", SpaceID: "s1", BlobID: "shared", MTime: 3000})
		}},
		{"trash entry", func(store *mockMetadataStore) {
			store.trash["s1.t1"] = &TrashEntry{Key: "t1", SpaceID: "s1", NodeID: "gone", Node: NodeEntry{SpaceID: "s1", BlobID: "shared"}}
		}},
		{"in-flight upload", func(store *mockMetadataStore) {
			store.uploads["u1"] = &UploadSession{ID: "u1", SpaceID: "s1", BlobID: "shared"}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, store, blob := newVersionFixture(1)
			store.versions = append(store.versions,
				&VersionEntry{Key: "vA", NodeID: "f1", SpaceID: "s1", BlobID: "shared", MTime: 1000},
				&VersionEntry{Key: "vB", NodeID: "f1", SpaceID: "s1", BlobID: "vb", MTime: 2000})
			blob.blobs[BlobKey("s1", "shared")] = []byte("shared")
			blob.blobs[BlobKey("s1", "vb")] = []byte("vb")
			tc.ref(store)

			d.trimVersions(context.Background(), "s1", "f1")

			if _, err := store.GetVersion("s1", "f1", "vA"); err == nil {
				t.Error("vA should have been trimmed")
			}
			if _, ok := blob.blobs[BlobKey("s1", "shared")]; !ok {
				t.Error("blob still referenced by the " + tc.name + " was deleted by the trim")
			}
		})
	}
}

func TestRestoreRevision_AtLimit_PreservesRestoredContent(t *testing.T) {
	d, store, blob := newVersionFixture(3)
	for _, c := range []string{"c0", "c1", "c2"} {
		mustOverwrite(t, d, c)
	}
	oldKey := versionKeyFor(t, store, blob, "original")

	if err := d.RestoreRevision(testContext(), f1Ref(), oldKey); err != nil {
		t.Fatalf("restore: %v", err)
	}

	if got := readHead(t, d); got != "original" {
		t.Errorf("head = %q after restoring the oldest version at the cap, want %q", got, "original")
	}
	if deleted(blob, BlobKey("s1", "old-blob")) {
		t.Error("the restored version's blob was deleted")
	}
	if versions, _ := store.ListVersions("s1", "f1"); len(versions) != 3 {
		t.Errorf("versions = %d, want exactly MaxVersions (3)", len(versions))
	}
}

func TestRestoreRevision_AtLimit_KeepsRestoredEntry(t *testing.T) {
	t.Run("max=3", func(t *testing.T) {
		d, store, blob := newVersionFixture(3)
		for _, c := range []string{"c0", "c1", "c2"} {
			mustOverwrite(t, d, c)
		}
		oldKey := versionKeyFor(t, store, blob, "original")
		c0Blob := ""
		for _, v := range store.versions {
			if string(blob.blobs[BlobKey("s1", v.BlobID)]) == "c0" {
				c0Blob = v.BlobID
			}
		}

		if err := d.RestoreRevision(testContext(), f1Ref(), oldKey); err != nil {
			t.Fatalf("restore: %v", err)
		}

		if _, err := store.GetVersion("s1", "f1", oldKey); err != nil {
			t.Error("the restored entry must stay listed")
		}
		got := versionContents(t, store, blob)
		want := map[string]bool{"original": true, "c1": true, "c2": true}
		if len(got) != 3 || !got["original"] || !got["c1"] || !got["c2"] {
			t.Errorf("versions hold %v, want %v (oldest other entry c0 trimmed)", got, want)
		}
		if _, ok := blob.blobs[BlobKey("s1", c0Blob)]; !ok {
			t.Error("the trimmed entry's blob must stay for GC")
		}
	})
	t.Run("max=1", func(t *testing.T) {
		d, store, blob := newVersionFixture(1)
		mustOverwrite(t, d, "c0")
		oldKey := versionKeyFor(t, store, blob, "original")

		if err := d.RestoreRevision(testContext(), f1Ref(), oldKey); err != nil {
			t.Fatalf("restore: %v", err)
		}

		versions, _ := store.ListVersions("s1", "f1")
		if len(versions) != 1 || versions[0].Key != oldKey {
			t.Errorf("want only the restored entry %s, got %d entries", oldKey, len(versions))
		}
		if got := readHead(t, d); got != "original" {
			t.Errorf("head = %q, want %q", got, "original")
		}
	})
}

func TestRestoreRevision_UnderLimit_KeepsRestoredEntry(t *testing.T) {
	d, store, blob := newVersionFixture(5)
	mustOverwrite(t, d, "c0")
	mustOverwrite(t, d, "c1")
	oldKey := versionKeyFor(t, store, blob, "original")

	if err := d.RestoreRevision(testContext(), f1Ref(), oldKey); err != nil {
		t.Fatalf("restore: %v", err)
	}

	got := versionContents(t, store, blob)
	if len(got) != 3 || !got["original"] || !got["c0"] || !got["c1"] {
		t.Errorf("versions hold %v, want original, c0 and the c1 snapshot", got)
	}
	if got := readHead(t, d); got != "original" {
		t.Errorf("head = %q, want %q", got, "original")
	}
}

func TestRestoreRevision_ThenOverwrites_KeepReferencedBlobs(t *testing.T) {
	d, store, blob := newVersionFixture(3)
	mustOverwrite(t, d, "c0")
	mustOverwrite(t, d, "c1")
	if err := d.RestoreRevision(testContext(), f1Ref(), versionKeyFor(t, store, blob, "original")); err != nil {
		t.Fatalf("restore: %v", err)
	}

	for _, c := range []string{"d0", "d1", "d2", "d3"} {
		mustOverwrite(t, d, c)
		assertReferencedBlobsExist(t, store, blob)
		if got := readHead(t, d); got != c {
			t.Fatalf("head = %q after overwrite %s", got, c)
		}
	}

	got := versionContents(t, store, blob)
	if len(got) != 3 || !got["d0"] || !got["d1"] || !got["d2"] {
		t.Errorf("versions hold %v, want d0, d1, d2", got)
	}
	versions, _ := store.ListVersions("s1", "f1")
	for _, v := range versions {
		_, rc, err := d.DownloadRevision(testContext(), f1Ref(), v.Key, nil)
		if err != nil {
			t.Errorf("version %s not downloadable: %v", v.Key, err)
			continue
		}
		rc.Close()
	}
}

func TestRestoreRevision_Twice_KeepsReferencedBlobs(t *testing.T) {
	d, store, blob := newVersionFixture(3)
	mustOverwrite(t, d, "c0")
	mustOverwrite(t, d, "c1")
	if err := d.RestoreRevision(testContext(), f1Ref(), versionKeyFor(t, store, blob, "original")); err != nil {
		t.Fatalf("first restore: %v", err)
	}
	c0Key := versionKeyFor(t, store, blob, "c0")
	if err := d.RestoreRevision(testContext(), f1Ref(), c0Key); err != nil {
		t.Fatalf("second restore: %v", err)
	}

	assertReferencedBlobsExist(t, store, blob)
	if got := readHead(t, d); got != "c0" {
		t.Errorf("head = %q, want c0", got)
	}
	if _, err := store.GetVersion("s1", "f1", c0Key); err != nil {
		t.Error("the restored c0 entry must stay listed")
	}
	if !versionContents(t, store, blob)["original"] {
		t.Error("the snapshot of the first restore's head (original) must still be listed and stored")
	}
	if versions, _ := store.ListVersions("s1", "f1"); len(versions) != 3 {
		t.Errorf("versions = %d, want 3", len(versions))
	}
}

func TestRestoreRevision_ThenFailedOverwrite_HeadSurvives(t *testing.T) {
	d, store, blob := newVersionFixture(3)
	mustOverwrite(t, d, "c0")
	mustOverwrite(t, d, "c1")
	if err := d.RestoreRevision(testContext(), f1Ref(), versionKeyFor(t, store, blob, "original")); err != nil {
		t.Fatalf("restore: %v", err)
	}

	setPutNodeErr(store, errors.New("injected"))
	if err := overwriteFile(d, "d0"); err == nil {
		t.Fatal("overwrite should fail while PutNode is broken")
	}
	setPutNodeErr(store, nil)

	if got := readHead(t, d); got != "original" {
		t.Errorf("head = %q after a failed overwrite, want %q", got, "original")
	}
	if _, ok := blob.blobs[BlobKey("s1", "old-blob")]; !ok {
		t.Error("the live head's blob was deleted")
	}
}

func TestRestoreRevision_FailedPutNodeAtLimit_KeepsVersion(t *testing.T) {
	d, store, blob := newVersionFixture(3)
	for _, c := range []string{"c0", "c1", "c2"} {
		mustOverwrite(t, d, c)
	}
	oldKey := versionKeyFor(t, store, blob, "original")

	setPutNodeErr(store, errors.New("injected"))
	if err := d.RestoreRevision(testContext(), f1Ref(), oldKey); err == nil {
		t.Fatal("restore should fail while PutNode is broken")
	}
	setPutNodeErr(store, nil)

	if _, err := store.GetVersion("s1", "f1", oldKey); err != nil {
		t.Error("a failed restore must not trim the version it tried to restore")
	}
	if versions, _ := store.ListVersions("s1", "f1"); len(versions) != 4 {
		t.Errorf("versions = %d, want 4 (no trim on a failed commit)", len(versions))
	}
	if got := readHead(t, d); got != "c2" {
		t.Errorf("head = %q, want c2", got)
	}
	r := testGC(store, blob, false).Run(context.Background())
	if r.BlobsDeleted != 0 {
		t.Errorf("GC deleted %d blobs, want 0 (all still referenced)", r.BlobsDeleted)
	}
	if _, ok := blob.blobs[BlobKey("s1", "old-blob")]; !ok {
		t.Error("the version's blob was lost")
	}
}

func TestUpload_FailedOverwriteDoesNotTrim(t *testing.T) {
	d, store, blob := newVersionFixture(2)
	mustOverwrite(t, d, "c0")
	mustOverwrite(t, d, "c1")
	oldKey := versionKeyFor(t, store, blob, "original")

	setPutNodeErr(store, errors.New("injected"))
	if err := overwriteFile(d, "c2"); err == nil {
		t.Fatal("overwrite should fail while PutNode is broken")
	}
	setPutNodeErr(store, nil)

	if versions, _ := store.ListVersions("s1", "f1"); len(versions) != 3 {
		t.Errorf("versions = %d, want 3 (first-try snapshot kept, no trim)", len(versions))
	}
	if _, err := store.GetVersion("s1", "f1", oldKey); err != nil {
		t.Error("a failed overwrite must not trim the oldest entry")
	}
	if _, ok := blob.blobs[BlobKey("s1", "old-blob")]; !ok {
		t.Error("old-blob was deleted by a failed overwrite")
	}
	if got := readHead(t, d); got != "c1" {
		t.Errorf("head = %q, want c1", got)
	}
}

func TestUpload_CASRetrySnapshotsOnceAndTrimsAfterCommit(t *testing.T) {
	d, store, blob := newVersionFixture(2)
	mustOverwrite(t, d, "c0")
	mustOverwrite(t, d, "c1")
	head, _, _ := store.GetNode("s1", "f1")
	c1Blob := head.BlobID

	store.injectCASFailures(1)
	mustOverwrite(t, d, "c2")

	versions, _ := store.ListVersions("s1", "f1")
	n := 0
	for _, v := range versions {
		if v.BlobID == c1Blob {
			n++
		}
	}
	if n != 1 {
		t.Errorf("%d entries snapshot c1, want exactly 1 (first attempt only)", n)
	}
	if len(versions) != 2 {
		t.Errorf("versions = %d, want 2", len(versions))
	}
	if deleted(blob, BlobKey("s1", "old-blob")) {
		t.Error("trim deleted old-blob inline")
	}
	if _, ok := blob.blobs[BlobKey("s1", "old-blob")]; !ok {
		t.Error("old-blob must stay stored until GC")
	}
}

func TestGC_ReapsTrimmedVersionBlobsAfterMinAge(t *testing.T) {
	d, store, blob := newVersionFixture(1)
	mustOverwrite(t, d, "c0")
	mustOverwrite(t, d, "c1")
	gc := testGC(store, blob, false)

	ageAllBlobs(blob, time.Now())
	if r1 := gc.Run(context.Background()); r1.BlobsDeleted != 0 {
		t.Errorf("young run deleted %d blobs, want 0", r1.BlobsDeleted)
	}
	if _, ok := blob.blobs[BlobKey("s1", "old-blob")]; !ok {
		t.Fatal("old-blob must survive until it is older than GCMinAge")
	}

	ageAllBlobs(blob, time.Now().Add(-2*time.Hour))
	r2 := gc.Run(context.Background())
	if r2.BlobsDeleted != 1 || r2.Errors != 0 {
		t.Errorf("aged run: deleted=%d errors=%d, want 1 and 0", r2.BlobsDeleted, r2.Errors)
	}
	if _, ok := blob.blobs[BlobKey("s1", "old-blob")]; ok {
		t.Error("the trimmed version's blob should be reclaimed by GC")
	}
	assertReferencedBlobsExist(t, store, blob)
}

func TestGC_KeepsBlobSharedByRestoreAfterTrim(t *testing.T) {
	d, store, blob := newVersionFixture(3)
	mustOverwrite(t, d, "c0")
	mustOverwrite(t, d, "c1")
	if err := d.RestoreRevision(testContext(), f1Ref(), versionKeyFor(t, store, blob, "original")); err != nil {
		t.Fatalf("restore: %v", err)
	}
	mustOverwrite(t, d, "d0")

	ageAllBlobs(blob, time.Now().Add(-2*time.Hour))
	r := testGC(store, blob, false).Run(context.Background())
	if r.Errors != 0 || r.BlobsDeleted != 0 {
		t.Errorf("GC: errors=%d deleted=%d, want 0 and 0", r.Errors, r.BlobsDeleted)
	}
	if _, ok := blob.blobs[BlobKey("s1", "old-blob")]; !ok {
		t.Error("old-blob is referenced by the newest snapshot and must survive")
	}
	assertReferencedBlobsExist(t, store, blob)
}

// restoreDuringSweepStore runs hook once inside the GC's version listing,
// i.e. after the sweep has already listed the space's nodes.
type restoreDuringSweepStore struct {
	*mockMetadataStore
	once sync.Once
	hook func()
}

func (s *restoreDuringSweepStore) ListVersionsBySpace(spaceID string) ([]*VersionEntry, error) {
	s.once.Do(s.hook)
	return s.mockMetadataStore.ListVersionsBySpace(spaceID)
}

func TestGC_RestoreDuringSweep_KeepsRestoredBlob(t *testing.T) {
	d, store, blob := newVersionFixture(3)
	for _, c := range []string{"c0", "c1", "c2"} {
		mustOverwrite(t, d, c)
	}
	oldKey := versionKeyFor(t, store, blob, "original")

	var restoreErr error
	wrapped := &restoreDuringSweepStore{mockMetadataStore: store, hook: func() {
		restoreErr = d.RestoreRevision(testContext(), f1Ref(), oldKey)
	}}
	log := zerolog.Nop()
	r := newBlobGC(wrapped, blob, gcOpts(false), &log).Run(context.Background())

	if restoreErr != nil {
		t.Fatalf("restore during sweep: %v", restoreErr)
	}
	if got := readHead(t, d); got != "original" {
		t.Errorf("head = %q after a restore raced a GC sweep, want %q", got, "original")
	}
	if _, ok := blob.blobs[BlobKey("s1", "old-blob")]; !ok {
		t.Error("GC deleted the restored head's blob")
	}
	if r.BlobsDeleted != 1 {
		t.Errorf("GC deleted %d blobs, want 1 (only the trimmed c0 entry's blob)", r.BlobsDeleted)
	}
}

func TestTrimVersions_ProtectsKeptKeys(t *testing.T) {
	cases := []struct {
		name  string
		max   int
		keep  []string
		mtime []int64
		want  []string
	}{
		{"kept entry takes a slot", 3, []string{"v0"}, []int64{1000, 2000, 3000, 4000, 5000}, []string{"v0", "v3", "v4"}},
		{"kept entry alone at max 1", 1, []string{"v0"}, []int64{1000, 2000, 3000, 4000, 5000}, []string{"v0"}},
		{"absent kept key takes no slot", 3, []string{"missing"}, []int64{1000, 2000, 3000, 4000, 5000}, []string{"v2", "v3", "v4"}},
		{"equal mtimes break on key", 2, nil, []int64{1000, 1000, 1000}, []string{"v0", "v1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newMockMetadataStore()
			blob := newMockBlobStore()
			d := testDriver(store, blob)
			d.opts.MaxVersions = tc.max
			setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
			for i, mt := range tc.mtime {
				store.versions = append(store.versions, &VersionEntry{
					Key: fmt.Sprintf("v%d", i), NodeID: "f1", SpaceID: "s1",
					BlobID: fmt.Sprintf("b%d", i), MTime: mt,
				})
			}

			d.trimVersions(context.Background(), "s1", "f1", tc.keep...)

			versions, _ := store.ListVersions("s1", "f1")
			got := map[string]bool{}
			for _, v := range versions {
				got[v.Key] = true
			}
			if len(got) != len(tc.want) {
				t.Fatalf("kept %v, want %v", got, tc.want)
			}
			for _, k := range tc.want {
				if !got[k] {
					t.Errorf("kept %v, want %v", got, tc.want)
				}
			}
		})
	}
}
