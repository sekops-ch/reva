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
	"fmt"
	"io"
	"testing"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

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

	// Verify oldest 2 blobs were deleted
	if _, exists := blob.blobs[BlobKey("s1", "vblob-0")]; exists {
		t.Error("oldest blob (vblob-0) should be deleted")
	}
	if _, exists := blob.blobs[BlobKey("s1", "vblob-1")]; exists {
		t.Error("second oldest blob (vblob-1) should be deleted")
	}

	// Verify newest 3 blobs remain
	for i := 2; i < 5; i++ {
		key := BlobKey("s1", fmt.Sprintf("vblob-%d", i))
		if _, exists := blob.blobs[key]; !exists {
			t.Errorf("blob vblob-%d should still exist", i)
		}
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

func TestTrimVersions_DeletesBlobs(t *testing.T) {
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

	// 2 blobs should be deleted (trimblob-0, trimblob-1), trimblob-2 remains
	if _, exists := blob.blobs[BlobKey("s1", "trimblob-0")]; exists {
		t.Error("trimblob-0 should be deleted")
	}
	if _, exists := blob.blobs[BlobKey("s1", "trimblob-1")]; exists {
		t.Error("trimblob-1 should be deleted")
	}
	if _, exists := blob.blobs[BlobKey("s1", "trimblob-2")]; !exists {
		t.Error("trimblob-2 (newest) should still exist")
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
