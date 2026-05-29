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
	"io"
	"testing"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
	"github.com/opencloud-eu/reva/v2/pkg/storage"
)

// setQuota sets a space's quota and root node size for quota tests.
func setQuota(store *mockMetadataStore, spaceID string, quota int64, usedSize int64) {
	store.spaces[spaceID].Quota = quota
	rootID := store.spaces[spaceID].RootID
	store.nodes[spaceID+"."+rootID].Size = usedSize
}

// --- Group 1: checkQuota unit tests ---

func TestCheckQuota_UnlimitedNeg1(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")
	setQuota(store, "s1", -1, 0)

	if err := d.checkQuota("s1", 1000, false, 0); err != nil {
		t.Fatalf("unlimited (-1) should allow any upload: %v", err)
	}
}

func TestCheckQuota_UnlimitedZero(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")
	setQuota(store, "s1", 0, 0)

	if err := d.checkQuota("s1", 1000, false, 0); err != nil {
		t.Fatalf("unlimited (0) should allow any upload: %v", err)
	}
}

func TestCheckQuota_NewFileWithinQuota(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")
	setQuota(store, "s1", 1000, 500)

	if err := d.checkQuota("s1", 400, false, 0); err != nil {
		t.Fatalf("400B into 500/1000 should succeed: %v", err)
	}
}

func TestCheckQuota_NewFileExactlyAtLimit(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")
	setQuota(store, "s1", 1000, 500)

	if err := d.checkQuota("s1", 500, false, 0); err != nil {
		t.Fatalf("500B into 500/1000 should succeed (exactly at limit): %v", err)
	}
}

func TestCheckQuota_NewFileExceedsByOneByte(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")
	setQuota(store, "s1", 1000, 500)

	err := d.checkQuota("s1", 501, false, 0)
	if err == nil {
		t.Fatal("501B into 500/1000 should fail")
	}
	if _, ok := err.(errtypes.IsInsufficientStorage); !ok {
		t.Errorf("expected InsufficientStorage, got %T: %v", err, err)
	}
}

func TestCheckQuota_OverwriteSmallerOK(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")
	setQuota(store, "s1", 1000, 900)

	if err := d.checkQuota("s1", 300, true, 500); err != nil {
		t.Fatalf("overwrite shrink 500→300 should always succeed: %v", err)
	}
}

func TestCheckQuota_OverwriteLargerOK(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")
	setQuota(store, "s1", 1000, 500)

	// Overwrite 400B file with 600B: net +200, used becomes 700 ≤ 1000
	if err := d.checkQuota("s1", 600, true, 400); err != nil {
		t.Fatalf("overwrite 400→600 with 500 used should succeed (700 ≤ 1000): %v", err)
	}
}

func TestCheckQuota_OverwriteLargerExceeds(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")
	setQuota(store, "s1", 1000, 900)

	// Overwrite 100B file with 300B: net +200, used becomes 1100 > 1000
	err := d.checkQuota("s1", 300, true, 100)
	if err == nil {
		t.Fatal("overwrite 100→300 with 900 used should fail (1100 > 1000)")
	}
	if _, ok := err.(errtypes.IsInsufficientStorage); !ok {
		t.Errorf("expected InsufficientStorage, got %T: %v", err, err)
	}
}

func TestCheckQuota_UsedExceedsQuota_NewFile(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")
	setQuota(store, "s1", 1000, 1200)

	err := d.checkQuota("s1", 1, false, 0)
	if err == nil {
		t.Fatal("any new file should fail when used > quota")
	}
	if _, ok := err.(errtypes.IsInsufficientStorage); !ok {
		t.Errorf("expected InsufficientStorage, got %T: %v", err, err)
	}
}

func TestCheckQuota_UsedExceedsQuota_ShrinkOK(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")
	setQuota(store, "s1", 1000, 1200)

	// Overwrite 500B with 200B: shrinking, should always succeed
	if err := d.checkQuota("s1", 200, true, 500); err != nil {
		t.Fatalf("shrinking overwrite should always succeed even if over quota: %v", err)
	}
}

// --- Group 2: Upload() quota tests ---

func TestUpload_QuotaExceeded_NewFile(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	setupSpaceEmpty(store, "s1", "root")
	setQuota(store, "s1", 500, 0)

	ctx := testContext()
	content := make([]byte, 501)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./bigfile.txt",
	}

	_, err := d.Upload(ctx, storage.UploadRequest{
		Ref:    ref,
		Body:   io.NopCloser(bytes.NewReader(content)),
		Length: int64(len(content)),
	}, nil)
	if err == nil {
		t.Fatal("upload exceeding quota should fail")
	}
	if _, ok := err.(errtypes.IsInsufficientStorage); !ok {
		t.Errorf("expected InsufficientStorage, got %T: %v", err, err)
	}

	// No blob should have been uploaded to S3
	if len(blob.blobs) != 0 {
		t.Errorf("expected no blobs in S3 after quota rejection, got %d", len(blob.blobs))
	}
}

func TestUpload_QuotaOK_NewFile(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	setupSpaceEmpty(store, "s1", "root")
	setQuota(store, "s1", 500, 0)

	ctx := testContext()
	content := make([]byte, 400)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./file.txt",
	}

	_, err := d.Upload(ctx, storage.UploadRequest{
		Ref:    ref,
		Body:   io.NopCloser(bytes.NewReader(content)),
		Length: int64(len(content)),
	}, nil)
	if err != nil {
		t.Fatalf("upload within quota should succeed: %v", err)
	}
}

func TestUpload_QuotaExceeded_Overwrite(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
	blob.blobs[BlobKey("s1", "old-blob")] = make([]byte, 100)
	// existing file is 100B, used=200, quota=500
	// overwrite 100→500: net +400, 200+400=600 > 500
	setQuota(store, "s1", 500, 200)

	ctx := testContext()
	content := make([]byte, 500)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./file.txt",
	}

	_, err := d.Upload(ctx, storage.UploadRequest{
		Ref:    ref,
		Body:   io.NopCloser(bytes.NewReader(content)),
		Length: int64(len(content)),
	}, nil)
	if err == nil {
		t.Fatal("overwrite exceeding quota should fail")
	}
	if _, ok := err.(errtypes.IsInsufficientStorage); !ok {
		t.Errorf("expected InsufficientStorage, got %T: %v", err, err)
	}
}

func TestUpload_QuotaOK_Overwrite_Shrink(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
	blob.blobs[BlobKey("s1", "old-blob")] = make([]byte, 100)
	setQuota(store, "s1", 500, 400) // used=400

	ctx := testContext()
	content := make([]byte, 50) // replace 100B with 50B: shrink
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./file.txt",
	}

	_, err := d.Upload(ctx, storage.UploadRequest{
		Ref:    ref,
		Body:   io.NopCloser(bytes.NewReader(content)),
		Length: int64(len(content)),
	}, nil)
	if err != nil {
		t.Fatalf("shrinking overwrite should succeed: %v", err)
	}
}

func TestUpload_QuotaUnlimited(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	setupSpaceEmpty(store, "s1", "root")
	setQuota(store, "s1", -1, 0)

	ctx := testContext()
	content := make([]byte, 99999)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./file.txt",
	}

	_, err := d.Upload(ctx, storage.UploadRequest{
		Ref:    ref,
		Body:   io.NopCloser(bytes.NewReader(content)),
		Length: int64(len(content)),
	}, nil)
	if err != nil {
		t.Fatalf("unlimited quota should allow any upload: %v", err)
	}
}

// --- Group 3: InitiateUpload() quota tests ---

func TestInitiateUpload_QuotaExceeded(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	setupSpaceEmpty(store, "s1", "root")
	setQuota(store, "s1", 500, 400)

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./newfile.txt",
	}

	_, err := d.InitiateUpload(ctx, ref, 200, map[string]string{})
	if err == nil {
		t.Fatal("InitiateUpload exceeding quota should fail")
	}
	if _, ok := err.(errtypes.IsInsufficientStorage); !ok {
		t.Errorf("expected InsufficientStorage, got %T: %v", err, err)
	}
}

func TestInitiateUpload_QuotaOK(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	setupSpaceEmpty(store, "s1", "root")
	setQuota(store, "s1", 500, 400)

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./newfile.txt",
	}

	result, err := d.InitiateUpload(ctx, ref, 100, map[string]string{})
	if err != nil {
		t.Fatalf("InitiateUpload within quota should succeed: %v", err)
	}
	if _, ok := result["simple"]; !ok {
		t.Error("result should contain 'simple' protocol")
	}
}

func TestInitiateUpload_DeferredLength(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	setupSpaceEmpty(store, "s1", "root")
	setQuota(store, "s1", 500, 400)

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./newfile.txt",
	}

	// uploadLength=0 means deferred length — cannot check quota
	result, err := d.InitiateUpload(ctx, ref, 0, map[string]string{})
	if err != nil {
		t.Fatalf("deferred-length InitiateUpload should not check quota: %v", err)
	}
	if _, ok := result["simple"]; !ok {
		t.Error("result should contain 'simple' protocol")
	}
}
