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
	"testing"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	"github.com/opencloud-eu/reva/v2/pkg/storage"
)

// --- Commit-phase crash-safety contract regressions ---
//
// Every multi-step CRUD op runs its durable mutation phase under the
// detached context from [kvfsDriver.commitPhase], so client/gateway
// cancellation cannot leave orphan state. These tests pre-cancel the
// request ctx BEFORE invoking each op and assert the operation still
// completes correctly. They also guard against a future maintainer
// adding a ctx.Done() check inside a kvstore helper and accidentally
// breaking atomicity.

// preCanceledCtx returns a context that is already canceled at the
// moment it is returned. Equivalent to the production scenario where
// the client closed the socket before the request was fully processed.
func preCanceledCtx() context.Context {
	ctx, cancel := context.WithCancel(testContext())
	cancel()
	return ctx
}

func TestCrashSafe_Upload_SimpleCommitsAfterCancel(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	setupSpaceEmpty(store, "s1", "root")

	ctx := preCanceledCtx()
	body := []byte("hello world")
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./newfile.txt",
	}
	ri, err := d.Upload(ctx, storage.UploadRequest{
		Ref:    ref,
		Body:   bytesReadCloser(body),
		Length: int64(len(body)),
	}, nil)
	if err != nil {
		t.Fatalf("Upload should succeed despite pre-canceled ctx: %v", err)
	}
	if ri == nil {
		t.Fatal("Upload returned nil ResourceInfo")
	}

	// File should be visible in S3 AND in NATS KV.
	if len(blob.blobs) != 1 {
		t.Errorf("expected exactly one blob in S3, got %d", len(blob.blobs))
	}
	children, _, _ := store.GetChildren("s1", "root")
	if _, exists := children["newfile.txt"]; !exists {
		t.Error("file node not committed — Upload was interrupted by canceled ctx")
	}
}

func TestCrashSafe_Move_CompletesAfterCancel(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupTwoFoldersForCrashSafe(store)

	ctx := preCanceledCtx()
	oldRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	newRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./folder-b/file.txt",
	}
	if err := d.Move(ctx, oldRef, newRef); err != nil {
		t.Fatalf("Move should succeed despite pre-canceled ctx: %v", err)
	}

	// Node moved to new parent.
	node, _, _ := store.GetNode("s1", "f1")
	if node.ParentID != "db" {
		t.Errorf("expected ParentID 'db' after move, got %q", node.ParentID)
	}
	// Old parent should no longer reference it (the duplicate-children
	// risk surface, guarded by the detached commit ctx).
	oldChildren, _, _ := store.GetChildren("s1", "da")
	if _, exists := oldChildren["file.txt"]; exists {
		t.Error("file still listed in OLD parent after Move — Move atomicity broken")
	}
	newChildren, _, _ := store.GetChildren("s1", "db")
	if newChildren["file.txt"] != "f1" {
		t.Error("file not added to new parent after Move")
	}
}

func TestCrashSafe_Delete_CompletesAfterCancel(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "victim.txt")

	ctx := preCanceledCtx()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	if err := d.Delete(ctx, ref); err != nil {
		t.Fatalf("Delete should succeed despite pre-canceled ctx: %v", err)
	}

	// Node should be removed from parent's children.
	children, _, _ := store.GetChildren("s1", "root")
	if _, exists := children["victim.txt"]; exists {
		t.Error("file still in parent's children after Delete")
	}

	// Trash should contain the snapshot.
	trash, err := store.ListTrash("s1")
	if err != nil {
		t.Fatalf("ListTrash: %v", err)
	}
	if len(trash) != 1 {
		t.Errorf("expected 1 trash entry, got %d", len(trash))
	}
}

func TestCrashSafe_CreateDir_CompletesAfterCancel(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")

	ctx := preCanceledCtx()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./newdir",
	}
	if err := d.CreateDir(ctx, ref); err != nil {
		t.Fatalf("CreateDir should succeed despite pre-canceled ctx: %v", err)
	}

	children, _, _ := store.GetChildren("s1", "root")
	newID, ok := children["newdir"]
	if !ok {
		t.Fatal("newdir not added to parent's children")
	}
	if _, _, err := store.GetNode("s1", newID); err != nil {
		t.Errorf("dir node not created: %v", err)
	}
}

func TestCrashSafe_PurgeRecycleItem_CompletesAfterCancel(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	setupSpaceWithFile(store, "s1", "root", "f1", "trashed.txt")

	// Pre-populate trash to skip the delete step (we want to test purge in isolation).
	trashEntry := &TrashEntry{
		Key:          "trash-key-1",
		NodeID:       "f1",
		SpaceID:      "s1",
		OriginalPath: "/trashed.txt",
		DeletionTime: 1000,
		Node:         *store.nodes["s1.f1"],
	}
	if err := store.PutTrash(trashEntry); err != nil {
		t.Fatalf("PutTrash: %v", err)
	}
	// Also put a blob in S3 to verify it gets deleted.
	blob.blobs[BlobKey("s1", "old-blob")] = []byte("legacy content")

	ctx := preCanceledCtx()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
	}
	if err := d.PurgeRecycleItem(ctx, ref, "trash-key-1", ""); err != nil {
		t.Fatalf("PurgeRecycleItem should succeed despite pre-canceled ctx: %v", err)
	}

	// Trash entry should be gone.
	if _, err := store.GetTrash("s1", "trash-key-1"); err == nil {
		t.Error("trash entry still present after Purge")
	}
	// S3 blob should be gone (the blob.Delete call uses commitCtx and must
	// run; if it short-circuited on ctx-cancel the blob would survive).
	if _, exists := blob.blobs[BlobKey("s1", "old-blob")]; exists {
		t.Error("blob still present in S3 after Purge — blob.Delete was interrupted by canceled ctx")
	}
}

func TestCrashSafe_RestoreRecycleItem_CompletesAfterCancel(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")

	// Pre-populate trash with a file snapshot.
	originalNode := &NodeEntry{
		ID: "f-restore", SpaceID: "s1", Name: "restored.txt",
		Type: NodeTypeFile, BlobID: "blob-x", Size: 50,
		MTime: 1000, ETag: "etag-old", Owner: "test-user-id",
		MimeType: "text/plain",
	}
	trashEntry := &TrashEntry{
		Key:          "restore-key-1",
		NodeID:       "f-restore",
		SpaceID:      "s1",
		OriginalPath: "/restored.txt",
		DeletionTime: 1000,
		Node:         *originalNode,
	}
	if err := store.PutTrash(trashEntry); err != nil {
		t.Fatalf("PutTrash: %v", err)
	}

	ctx := preCanceledCtx()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
	}
	if err := d.RestoreRecycleItem(ctx, ref, "restore-key-1", "", nil); err != nil {
		t.Fatalf("RestoreRecycleItem should succeed despite pre-canceled ctx: %v", err)
	}

	// File should be back in parent's children.
	children, _, _ := store.GetChildren("s1", "root")
	if _, exists := children["restored.txt"]; !exists {
		t.Error("file not re-added to parent after Restore")
	}
	// Trash entry should be gone (the "still in trash AND restored" leak guard).
	if _, err := store.GetTrash("s1", "restore-key-1"); err == nil {
		t.Error("trash entry still present after Restore — restore-trash atomicity broken")
	}
}

// --- Helpers ---

// bytesReadCloser wraps a byte slice as an io.ReadCloser for Upload tests.
type bytesRC struct{ *bytes.Reader }

func (b *bytesRC) Close() error { return nil }

func bytesReadCloser(p []byte) *bytesRC { return &bytesRC{bytes.NewReader(p)} }

// setupTwoFoldersForCrashSafe mirrors move_test.go's setupTwoFolders but
// kept in this file so the crash-safe suite is self-contained even if
// move_test.go ever changes shape.
func setupTwoFoldersForCrashSafe(store *mockMetadataStore) {
	store.spaces["s1"] = &SpaceEntry{
		ID: "s1", Type: "personal", Owner: "test-user-id",
		Name: "Test", RootID: "root", Quota: -1,
	}
	store.spaceRevs["s1"] = 1
	store.nodes["s1.root"] = &NodeEntry{
		ID: "root", SpaceID: "s1", Name: "root", Type: NodeTypeDir,
		Owner: "test-user-id", MimeType: "httpd/unix-directory",
	}
	store.nodeRevs["s1.root"] = 1
	store.children["s1.root"] = ChildMap{"folder-a": "da", "folder-b": "db"}
	store.childRevs["s1.root"] = 1

	store.nodes["s1.da"] = &NodeEntry{
		ID: "da", SpaceID: "s1", ParentID: "root",
		Name: "folder-a", Type: NodeTypeDir, Size: 100,
		Owner: "test-user-id", MimeType: "httpd/unix-directory",
	}
	store.nodeRevs["s1.da"] = 1
	store.children["s1.da"] = ChildMap{"file.txt": "f1"}
	store.childRevs["s1.da"] = 1

	store.nodes["s1.db"] = &NodeEntry{
		ID: "db", SpaceID: "s1", ParentID: "root",
		Name: "folder-b", Type: NodeTypeDir, Size: 0,
		Owner: "test-user-id", MimeType: "httpd/unix-directory",
	}
	store.nodeRevs["s1.db"] = 1
	store.children["s1.db"] = ChildMap{}
	store.childRevs["s1.db"] = 1

	store.nodes["s1.f1"] = &NodeEntry{
		ID: "f1", SpaceID: "s1", ParentID: "da",
		Name: "file.txt", Type: NodeTypeFile, BlobID: "blob-1",
		BlobSize: 100, Size: 100, MTime: 1000, ETag: "old-etag",
		Owner: "test-user-id", MimeType: "text/plain",
	}
	store.nodeRevs["s1.f1"] = 1
}

