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
	"crypto/sha1"
	"encoding/hex"
	"io"
	"testing"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	"github.com/opencloud-eu/reva/v2/pkg/storage"
)

func sha1Hex(data []byte) string {
	h := sha1.Sum(data)
	return hex.EncodeToString(h[:])
}

func TestUpload_ComputesSHA1Checksum(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceEmpty(store, "space-1", "root-1")

	ctx := testContext()
	content := []byte("hello checksum")
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "root-1"},
		Path:       "./checksummed.txt",
	}

	ri, err := d.Upload(ctx, storage.UploadRequest{
		Ref:    ref,
		Body:   io.NopCloser(bytes.NewReader(content)),
		Length: int64(len(content)),
	}, nil)
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	expected := "sha1:" + sha1Hex(content)

	// Check the node in store
	children, _, _ := store.GetChildren("space-1", "root-1")
	nodeID := children["checksummed.txt"]
	node, _, _ := store.GetNode("space-1", nodeID)

	if node.Checksum != expected {
		t.Errorf("node checksum = %q, want %q", node.Checksum, expected)
	}

	// Check it's exposed in ResourceInfo
	if ri.Checksum == nil {
		t.Fatal("expected non-nil checksum in ResourceInfo")
	}
	if ri.Checksum.Type != provider.ResourceChecksumType_RESOURCE_CHECKSUM_TYPE_SHA1 {
		t.Errorf("checksum type = %v, want SHA1", ri.Checksum.Type)
	}
	if ri.Checksum.Sum != sha1Hex(content) {
		t.Errorf("checksum sum = %q, want %q", ri.Checksum.Sum, sha1Hex(content))
	}
}

func TestUpload_Overwrite_UpdatesChecksum(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceWithFile(store, "space-1", "root-1", "file-1", "existing.txt")

	ctx := testContext()
	newContent := []byte("new content for checksum")
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "root-1"},
		Path:       "./existing.txt",
	}

	_, err := d.Upload(ctx, storage.UploadRequest{
		Ref:    ref,
		Body:   io.NopCloser(bytes.NewReader(newContent)),
		Length: int64(len(newContent)),
	}, nil)
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	expected := "sha1:" + sha1Hex(newContent)
	node, _, _ := store.GetNode("space-1", "file-1")
	if node.Checksum != expected {
		t.Errorf("checksum = %q, want %q", node.Checksum, expected)
	}
}

func TestFinishUpload_ComputesSHA1Checksum(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceEmpty(store, "space-1", "root-1")

	content := []byte("tus checksum data")

	// Modern disk-staged path (S3MultipartID == ""): the body is staged in the
	// upload cache and pushed to S3 once at FinishUpload, where the SHA-1 is
	// computed in-band over the staged bytes.
	session := &UploadSession{
		ID:       "sess-cs-1",
		SpaceID:  "space-1",
		Filename: "tus-cs.txt",
		ParentID: "root-1",
		Size:     int64(len(content)),
		Offset:   0,
		Storage:  map[string]string{},
		BlobID:   "tus-blob-cs",
		OwnerID:  "test-user-id",
	}
	store.uploads["sess-cs-1"] = session

	ctx := testContext()
	upload := &kvfsUpload{session: session, driver: d}

	// WriteChunk stages the body to the upload cache.
	_, err := upload.WriteChunk(ctx, 0, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("WriteChunk failed: %v", err)
	}

	UploadInFlight.WithLabelValues("tus").Add(1)
	err = upload.FinishUpload(ctx)
	if err != nil {
		t.Fatalf("FinishUpload failed: %v", err)
	}

	expected := "sha1:" + sha1Hex(content)

	// Find the created node
	children, _, _ := store.GetChildren("space-1", "root-1")
	nodeID := children["tus-cs.txt"]
	if nodeID == "" {
		t.Fatal("expected file node to exist")
	}

	node, _, _ := store.GetNode("space-1", nodeID)
	if node.Checksum != expected {
		t.Errorf("TUS checksum = %q, want %q", node.Checksum, expected)
	}
}

func TestFinishUpload_MultipleChunks_Checksum(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceEmpty(store, "space-1", "root-1")

	chunk1 := []byte("first chunk ")
	chunk2 := []byte("second chunk")
	fullContent := append(chunk1, chunk2...)

	// Modern disk-staged path: both chunks land in the upload cache and the
	// SHA-1 is computed over the reassembled body at FinishUpload. (Multiple
	// chunks on ONE instance — see TestFinishUpload_MultiRequest_Checksum for
	// the cross-request lifecycle that reproduces the hasher-reset bug.)
	session := &UploadSession{
		ID:       "sess-mc-1",
		SpaceID:  "space-1",
		Filename: "multi-chunk.txt",
		ParentID: "root-1",
		Size:     int64(len(fullContent)),
		Offset:   0,
		Storage:  map[string]string{},
		BlobID:   "mc-blob",
		OwnerID:  "test-user-id",
	}
	store.uploads["sess-mc-1"] = session

	ctx := testContext()
	upload := &kvfsUpload{session: session, driver: d}

	_, err := upload.WriteChunk(ctx, 0, bytes.NewReader(chunk1))
	if err != nil {
		t.Fatalf("WriteChunk 1 failed: %v", err)
	}

	_, err = upload.WriteChunk(ctx, int64(len(chunk1)), bytes.NewReader(chunk2))
	if err != nil {
		t.Fatalf("WriteChunk 2 failed: %v", err)
	}

	UploadInFlight.WithLabelValues("tus").Add(1)
	err = upload.FinishUpload(ctx)
	if err != nil {
		t.Fatalf("FinishUpload failed: %v", err)
	}

	expected := "sha1:" + sha1Hex(fullContent)
	children, _, _ := store.GetChildren("space-1", "root-1")
	nodeID := children["multi-chunk.txt"]
	node, _, _ := store.GetNode("space-1", nodeID)
	if node.Checksum != expected {
		t.Errorf("multi-chunk checksum = %q, want %q", node.Checksum, expected)
	}
}

// TestFinishUpload_MultiRequest_Checksum reproduces the real TUS lifecycle
// where every PATCH is a separate HTTP request: GetUpload mints a fresh
// *kvfsUpload per request. A correct stored checksum must reflect the FULL
// staged body, not just the bytes of the request that happened to run
// FinishUpload. This is the regression guard for the per-request hasher-reset
// bug — pre-fix the stored SHA-1 was computed over only the final chunk.
func TestFinishUpload_MultiRequest_Checksum(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	ctx := testContext()

	setupSpaceEmpty(store, "space-1", "root-1")

	chunk1 := []byte("first chunk of a multi-PATCH upload ")
	chunk2 := []byte("second chunk delivered in a separate request")
	full := append(append([]byte{}, chunk1...), chunk2...)

	// Modern disk-staged path (S3MultipartID == ""). No pre-init multipart.
	store.uploads["sess-mr-1"] = &UploadSession{
		ID:       "sess-mr-1",
		SpaceID:  "space-1",
		Filename: "multi-request.txt",
		ParentID: "root-1",
		Size:     int64(len(full)),
		Offset:   0,
		Storage:  map[string]string{},
		BlobID:   "mr-blob",
		OwnerID:  "test-user-id",
	}

	// Request 1 — PATCH chunk1 on a fresh instance, then discarded (the HTTP
	// request ends; tusd does not retain the upload object).
	u1, err := d.GetUpload(ctx, "sess-mr-1")
	if err != nil {
		t.Fatalf("GetUpload (req 1): %v", err)
	}
	if _, err := u1.(*kvfsUpload).WriteChunk(ctx, 0, bytes.NewReader(chunk1)); err != nil {
		t.Fatalf("WriteChunk (req 1): %v", err)
	}

	// Request 2 — PATCH chunk2 completes the upload and triggers FinishUpload,
	// all on a brand-new instance (hasher reset), exactly as tusd does.
	u2, err := d.GetUpload(ctx, "sess-mr-1")
	if err != nil {
		t.Fatalf("GetUpload (req 2): %v", err)
	}
	ku2 := u2.(*kvfsUpload)
	if _, err := ku2.WriteChunk(ctx, int64(len(chunk1)), bytes.NewReader(chunk2)); err != nil {
		t.Fatalf("WriteChunk (req 2): %v", err)
	}
	UploadInFlight.WithLabelValues("tus").Add(1)
	if err := ku2.FinishUpload(ctx); err != nil {
		t.Fatalf("FinishUpload: %v", err)
	}

	children, _, _ := store.GetChildren("space-1", "root-1")
	nodeID := children["multi-request.txt"]
	if nodeID == "" {
		t.Fatal("expected file node to exist")
	}
	node, _, _ := store.GetNode("space-1", nodeID)

	// The blob that landed in S3 must be the FULL reassembled body (this was
	// already correct pre-fix — the staging cache is offset-addressed).
	if got := blob.blobs[BlobKey("space-1", "mr-blob")]; !bytes.Equal(got, full) {
		t.Fatalf("stored blob = %d bytes, want %d (full body)", len(got), len(full))
	}
	if node.Size != int64(len(full)) {
		t.Errorf("node.Size = %d, want %d", node.Size, len(full))
	}
	// ...and the stored checksum must match that full body. Pre-fix this was
	// "sha1:"+sha1Hex(chunk2) because the hasher was reset on the req-2 instance.
	want := "sha1:" + sha1Hex(full)
	if node.Checksum != want {
		t.Errorf("multi-request checksum = %q, want %q", node.Checksum, want)
	}
}

func TestParseChecksum_SHA1(t *testing.T) {
	cs := parseChecksum("sha1:da39a3ee5e6b4b0d3255bfef95601890afd80709")
	if cs == nil {
		t.Fatal("expected non-nil checksum")
	}
	if cs.Type != provider.ResourceChecksumType_RESOURCE_CHECKSUM_TYPE_SHA1 {
		t.Errorf("type = %v, want SHA1", cs.Type)
	}
	if cs.Sum != "da39a3ee5e6b4b0d3255bfef95601890afd80709" {
		t.Errorf("sum = %q, want empty-string sha1", cs.Sum)
	}
}

func TestParseChecksum_MD5(t *testing.T) {
	cs := parseChecksum("md5:d41d8cd98f00b204e9800998ecf8427e")
	if cs == nil {
		t.Fatal("expected non-nil checksum")
	}
	if cs.Type != provider.ResourceChecksumType_RESOURCE_CHECKSUM_TYPE_MD5 {
		t.Errorf("type = %v, want MD5", cs.Type)
	}
}

func TestParseChecksum_Empty(t *testing.T) {
	cs := parseChecksum("")
	if cs != nil {
		t.Error("expected nil for empty checksum")
	}
}

func TestChecksum_InResourceInfo(t *testing.T) {
	node := &NodeEntry{
		ID:       "n1",
		SpaceID:  "s1",
		Name:     "test.txt",
		Type:     NodeTypeFile,
		Checksum: "sha1:abc123",
		MimeType: "text/plain",
	}
	ri := node.ToResourceInfo("test.txt")
	if ri.Checksum == nil {
		t.Fatal("expected checksum in ResourceInfo")
	}
	if ri.Checksum.Type != provider.ResourceChecksumType_RESOURCE_CHECKSUM_TYPE_SHA1 {
		t.Errorf("type = %v, want SHA1", ri.Checksum.Type)
	}
	if ri.Checksum.Sum != "abc123" {
		t.Errorf("sum = %q, want abc123", ri.Checksum.Sum)
	}
}

func TestChecksum_PreservedInVersion(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceWithFile(store, "space-1", "root-1", "file-1", "versioned.txt")
	store.nodes["space-1.file-1"].Checksum = "sha1:oldchecksum"

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "root-1"},
		Path:       "./versioned.txt",
	}

	_, err := d.Upload(ctx, storage.UploadRequest{
		Ref:    ref,
		Body:   io.NopCloser(bytes.NewReader([]byte("new data"))),
		Length: 8,
	}, nil)
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	versions, _ := store.ListVersions("space-1", "file-1")
	if len(versions) != 1 {
		t.Fatalf("expected 1 version, got %d", len(versions))
	}
	if versions[0].Checksum != "sha1:oldchecksum" {
		t.Errorf("version checksum = %q, want sha1:oldchecksum", versions[0].Checksum)
	}
}
