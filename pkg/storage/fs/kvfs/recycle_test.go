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
	"testing"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
)

// setupSpaceWithDir creates a space with a directory containing files.
// Returns (dirID, []fileIDs).
func setupSpaceWithDir(store *mockMetadataStore, spaceID, rootID, dirID, dirName string, files map[string]string) {
	setupSpaceEmpty(store, spaceID, rootID)

	store.nodes[spaceID+"."+dirID] = &NodeEntry{
		ID: dirID, SpaceID: spaceID, ParentID: rootID,
		Name: dirName, Type: NodeTypeDir, Size: 0,
		Owner: "test-user-id", MimeType: "httpd/unix-directory",
	}
	store.nodeRevs[spaceID+"."+dirID] = 1
	store.children[spaceID+"."+rootID] = ChildMap{dirName: dirID}
	store.childRevs[spaceID+"."+rootID] = 1

	dirChildren := make(ChildMap)
	var totalSize int64
	for filename, fileID := range files {
		blobID := "blob-" + fileID
		store.nodes[spaceID+"."+fileID] = &NodeEntry{
			ID: fileID, SpaceID: spaceID, ParentID: dirID,
			Name: filename, Type: NodeTypeFile,
			BlobID: blobID, BlobSize: 100, Size: 100,
			Owner: "test-user-id", MimeType: "text/plain",
		}
		store.nodeRevs[spaceID+"."+fileID] = 1
		dirChildren[filename] = fileID
		totalSize += 100
	}
	store.children[spaceID+"."+dirID] = dirChildren
	store.childRevs[spaceID+"."+dirID] = 1

	// Update dir size
	store.nodes[spaceID+"."+dirID].Size = totalSize
}

func TestDeleteDir_KeepsNodesAlive(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	files := map[string]string{"a.txt": "f1", "b.txt": "f2"}
	setupSpaceWithDir(store, "s1", "root", "dir1", "mydir", files)

	blob.blobs[BlobKey("s1", "blob-f1")] = []byte("content a")
	blob.blobs[BlobKey("s1", "blob-f2")] = []byte("content b")

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "dir1"},
	}

	err := d.Delete(ctx, ref)
	if err != nil {
		t.Fatalf("Delete dir failed: %v", err)
	}

	// Dir node must still exist in KV
	dirNode, _, err := store.GetNode("s1", "dir1")
	if err != nil {
		t.Fatal("directory node should still exist after trash")
	}
	if dirNode.Name != "mydir" {
		t.Errorf("expected dir name mydir, got %s", dirNode.Name)
	}

	// Children must still exist
	children, _, err := store.GetChildren("s1", "dir1")
	if err != nil {
		t.Fatal("children map should still exist after trash")
	}
	if len(children) != 2 {
		t.Errorf("expected 2 children, got %d", len(children))
	}

	// File nodes must still exist
	for _, fileID := range files {
		if _, _, err := store.GetNode("s1", fileID); err != nil {
			t.Errorf("file node %s should still exist after dir trash", fileID)
		}
	}

	// Blobs must still exist
	for _, fileID := range files {
		blobKey := BlobKey("s1", "blob-"+fileID)
		if _, ok := blob.blobs[blobKey]; !ok {
			t.Errorf("blob %s should still exist after dir trash", blobKey)
		}
	}

	// Dir should be removed from parent's children
	rootChildren, _, _ := store.GetChildren("s1", "root")
	if _, exists := rootChildren["mydir"]; exists {
		t.Error("dir should be removed from parent's children map")
	}

	// Trash entry should exist
	trashItems, _ := store.ListTrash("s1")
	if len(trashItems) != 1 {
		t.Fatalf("expected 1 trash entry, got %d", len(trashItems))
	}
	if trashItems[0].Node.Type != NodeTypeDir {
		t.Error("trash entry should be a directory")
	}
}

func TestDeleteDir_RestorePreservesChildren(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	files := map[string]string{"a.txt": "f1", "b.txt": "f2"}
	setupSpaceWithDir(store, "s1", "root", "dir1", "mydir", files)

	blob.blobs[BlobKey("s1", "blob-f1")] = []byte("content a")
	blob.blobs[BlobKey("s1", "blob-f2")] = []byte("content b")

	ctx := testContext()
	dirRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "dir1"},
	}

	// Delete the directory
	err := d.Delete(ctx, dirRef)
	if err != nil {
		t.Fatalf("Delete dir failed: %v", err)
	}

	// Get the trash key
	trashItems, _ := store.ListTrash("s1")
	if len(trashItems) != 1 {
		t.Fatalf("expected 1 trash entry, got %d", len(trashItems))
	}
	trashKey := trashItems[0].Key

	// Restore the directory
	spaceRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1"},
	}
	err = d.RestoreRecycleItem(ctx, spaceRef, trashKey, "", nil)
	if err != nil {
		t.Fatalf("RestoreRecycleItem failed: %v", err)
	}

	// Dir should be back in parent's children
	rootChildren, _, _ := store.GetChildren("s1", "root")
	restoredID, exists := rootChildren["mydir"]
	if !exists {
		t.Fatal("restored dir should be in parent's children")
	}
	if restoredID != "dir1" {
		t.Errorf("expected restored dir ID dir1, got %s", restoredID)
	}

	// Children should still be accessible
	dirChildren, _, err := store.GetChildren("s1", "dir1")
	if err != nil {
		t.Fatal("children map should exist after restore")
	}
	if len(dirChildren) != 2 {
		t.Errorf("expected 2 children after restore, got %d", len(dirChildren))
	}

	// File nodes should still be readable
	for filename, fileID := range files {
		node, _, err := store.GetNode("s1", fileID)
		if err != nil {
			t.Errorf("file node %s should exist after restore", fileID)
			continue
		}
		if node.Name != filename {
			t.Errorf("expected file name %s, got %s", filename, node.Name)
		}
		if node.BlobID == "" {
			t.Errorf("file %s should have a blob ID after restore", filename)
		}
	}

	// Blobs should still be downloadable
	for _, fileID := range files {
		blobKey := BlobKey("s1", "blob-"+fileID)
		if _, ok := blob.blobs[blobKey]; !ok {
			t.Errorf("blob %s should still exist after restore", blobKey)
		}
	}

	// Trash should be empty
	trashItems, _ = store.ListTrash("s1")
	if len(trashItems) != 0 {
		t.Errorf("expected 0 trash entries after restore, got %d", len(trashItems))
	}
}

func TestDeleteDir_NestedRestore(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	// Create: root/parent/child/file.txt
	setupSpaceEmpty(store, "s1", "root")

	store.nodes["s1.parent"] = &NodeEntry{
		ID: "parent", SpaceID: "s1", ParentID: "root",
		Name: "parent", Type: NodeTypeDir, Size: 100,
		Owner: "test-user-id", MimeType: "httpd/unix-directory",
	}
	store.nodeRevs["s1.parent"] = 1
	store.children["s1.root"] = ChildMap{"parent": "parent"}
	store.childRevs["s1.root"] = 1

	store.nodes["s1.child"] = &NodeEntry{
		ID: "child", SpaceID: "s1", ParentID: "parent",
		Name: "child", Type: NodeTypeDir, Size: 100,
		Owner: "test-user-id", MimeType: "httpd/unix-directory",
	}
	store.nodeRevs["s1.child"] = 1
	store.children["s1.parent"] = ChildMap{"child": "child"}
	store.childRevs["s1.parent"] = 1

	store.nodes["s1.file1"] = &NodeEntry{
		ID: "file1", SpaceID: "s1", ParentID: "child",
		Name: "file.txt", Type: NodeTypeFile,
		BlobID: "nested-blob-id", BlobSize: 100, Size: 100,
		Owner: "test-user-id", MimeType: "text/plain",
	}
	store.nodeRevs["s1.file1"] = 1
	store.children["s1.child"] = ChildMap{"file.txt": "file1"}
	store.childRevs["s1.child"] = 1

	blob.blobs[BlobKey("s1", "nested-blob-id")] = []byte("nested content")

	ctx := testContext()

	// Delete the top-level parent dir
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "parent"},
	}
	err := d.Delete(ctx, ref)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify all nodes still exist
	for _, id := range []string{"parent", "child", "file1"} {
		if _, _, err := store.GetNode("s1", id); err != nil {
			t.Errorf("node %s should still exist after trash", id)
		}
	}

	// Restore
	trashItems, _ := store.ListTrash("s1")
	spaceRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1"},
	}
	err = d.RestoreRecycleItem(ctx, spaceRef, trashItems[0].Key, "", nil)
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Verify the full tree is traversable: root → parent → child → file.txt
	rootChildren, _, _ := store.GetChildren("s1", "root")
	if _, ok := rootChildren["parent"]; !ok {
		t.Fatal("parent should be in root's children after restore")
	}

	parentChildren, _, _ := store.GetChildren("s1", "parent")
	if _, ok := parentChildren["child"]; !ok {
		t.Fatal("child should be in parent's children after restore")
	}

	childChildren, _, _ := store.GetChildren("s1", "child")
	if _, ok := childChildren["file.txt"]; !ok {
		t.Fatal("file.txt should be in child's children after restore")
	}

	// Blob should still be there
	if _, ok := blob.blobs[BlobKey("s1", "nested-blob-id")]; !ok {
		t.Error("blob should still exist after restore")
	}
}

func TestPurgeDir_CleansDescendants(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	files := map[string]string{"a.txt": "f1", "b.txt": "f2"}
	setupSpaceWithDir(store, "s1", "root", "dir1", "mydir", files)

	blob.blobs[BlobKey("s1", "blob-f1")] = []byte("content a")
	blob.blobs[BlobKey("s1", "blob-f2")] = []byte("content b")

	ctx := testContext()

	// Delete the directory
	dirRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "dir1"},
	}
	err := d.Delete(ctx, dirRef)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Get trash key
	trashItems, _ := store.ListTrash("s1")
	trashKey := trashItems[0].Key

	// Purge the directory
	spaceRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1"},
	}
	err = d.PurgeRecycleItem(ctx, spaceRef, trashKey, "")
	if err != nil {
		t.Fatalf("PurgeRecycleItem failed: %v", err)
	}

	// All nodes should be gone
	if _, _, err := store.GetNode("s1", "dir1"); err != ErrNodeNotFound {
		t.Error("dir node should be deleted after purge")
	}
	for _, fileID := range files {
		if _, _, err := store.GetNode("s1", fileID); err != ErrNodeNotFound {
			t.Errorf("file node %s should be deleted after purge", fileID)
		}
	}

	// All blobs should be gone
	for _, fileID := range files {
		blobKey := BlobKey("s1", "blob-"+fileID)
		if _, ok := blob.blobs[blobKey]; ok {
			t.Errorf("blob %s should be deleted after purge", blobKey)
		}
	}

	// Trash should be empty
	trashItems, _ = store.ListTrash("s1")
	if len(trashItems) != 0 {
		t.Errorf("expected 0 trash entries after purge, got %d", len(trashItems))
	}
}

func TestEmptyRecycle_CleansDirectoryDescendants(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	files := map[string]string{"a.txt": "f1"}
	setupSpaceWithDir(store, "s1", "root", "dir1", "mydir", files)

	blob.blobs[BlobKey("s1", "blob-f1")] = []byte("content")

	ctx := testContext()

	// Delete the directory
	dirRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "dir1"},
	}
	if err := d.Delete(ctx, dirRef); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Empty recycle
	spaceRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1"},
	}
	if err := d.EmptyRecycle(ctx, spaceRef); err != nil {
		t.Fatalf("EmptyRecycle failed: %v", err)
	}

	// All nodes should be gone
	if _, _, err := store.GetNode("s1", "dir1"); err != ErrNodeNotFound {
		t.Error("dir node should be deleted after empty recycle")
	}
	if _, _, err := store.GetNode("s1", "f1"); err != ErrNodeNotFound {
		t.Error("file node should be deleted after empty recycle")
	}

	// Blob should be gone
	if _, ok := blob.blobs[BlobKey("s1", "blob-f1")]; ok {
		t.Error("blob should be deleted after empty recycle")
	}
}

func TestDeleteFile_StillDeletesNode(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceWithFile(store, "s1", "root", "f1", "test.txt")
	blob.blobs[BlobKey("s1", "old-blob")] = []byte("content")

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}

	err := d.Delete(ctx, ref)
	if err != nil {
		t.Fatalf("Delete file failed: %v", err)
	}

	// File node SHOULD be deleted (files use snapshot-based restore)
	if _, _, err := store.GetNode("s1", "f1"); err != ErrNodeNotFound {
		t.Error("file node should be deleted after trash (uses snapshot restore)")
	}

	// Blob should still exist (cleaned on purge, not trash)
	if _, ok := blob.blobs[BlobKey("s1", "old-blob")]; !ok {
		t.Error("blob should still exist after file trash")
	}

	// Should have a trash entry
	trashItems, _ := store.ListTrash("s1")
	if len(trashItems) != 1 {
		t.Fatalf("expected 1 trash entry, got %d", len(trashItems))
	}
	if trashItems[0].Node.Type != NodeTypeFile {
		t.Error("trash entry should be a file")
	}
}

func TestTrashOrderingSafety(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceWithFile(store, "s1", "root", "f1", "test.txt")

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}

	err := d.Delete(ctx, ref)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify: trash entry exists (was created before parent children update)
	trashItems, _ := store.ListTrash("s1")
	if len(trashItems) == 0 {
		t.Fatal("trash entry should exist")
	}

	// Verify: node removed from parent's children
	rootChildren, _, _ := store.GetChildren("s1", "root")
	if _, exists := rootChildren["test.txt"]; exists {
		t.Error("file should be removed from parent's children")
	}
}

func TestRestoreDir_OldFormatFallback(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceEmpty(store, "s1", "root")

	// Simulate an old-format trash entry where the node was already deleted
	// (pre-fix behavior). The node does NOT exist in the store.
	store.trash["s1.old-trash"] = &TrashEntry{
		Key:          "old-trash",
		NodeID:       "dead-dir",
		SpaceID:      "s1",
		OriginalPath: "/old-dir",
		DeletionTime: 1000,
		Node: NodeEntry{
			ID: "dead-dir", SpaceID: "s1", ParentID: "root",
			Name: "old-dir", Type: NodeTypeDir, Size: 500,
			Owner: "test-user-id", MimeType: "httpd/unix-directory",
		},
	}

	ctx := testContext()
	spaceRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1"},
	}

	err := d.RestoreRecycleItem(ctx, spaceRef, "old-trash", "", nil)
	if err != nil {
		t.Fatalf("RestoreRecycleItem (old format) failed: %v", err)
	}

	// The directory should be re-created from snapshot (empty, no children)
	node, _, err := store.GetNode("s1", "dead-dir")
	if err != nil {
		t.Fatal("restored directory should exist")
	}
	if node.Type != NodeTypeDir {
		t.Error("restored node should be a directory")
	}

	// Should be in parent's children
	rootChildren, _, _ := store.GetChildren("s1", "root")
	if _, exists := rootChildren["old-dir"]; !exists {
		t.Error("restored dir should be in parent's children")
	}

	// Children map should exist (empty)
	dirChildren, _, _ := store.GetChildren("s1", "dead-dir")
	if dirChildren == nil {
		t.Error("restored dir should have an empty children map")
	}
}

func TestDeleteSpaceContents_CleansUpTrashedDirNodes(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	files := map[string]string{"a.txt": "f1"}
	setupSpaceWithDir(store, "s1", "root", "dir1", "mydir", files)

	blob.blobs[BlobKey("s1", "blob-f1")] = []byte("content")

	ctx := testContext()

	// Delete the directory (keeps nodes alive)
	dirRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "dir1"},
	}
	if err := d.Delete(ctx, dirRef); err != nil {
		t.Fatalf("Delete dir failed: %v", err)
	}

	// Now delete the entire space
	if err := d.DeleteStorageSpace(ctx, &provider.DeleteStorageSpaceRequest{
		Id: &provider.StorageSpaceId{OpaqueId: "s1"},
	}); err != nil {
		t.Fatalf("DeleteStorageSpace failed: %v", err)
	}

	// ALL nodes should be gone (both live tree and trashed dir descendants)
	for _, id := range []string{"root", "dir1", "f1"} {
		if _, _, err := store.GetNode("s1", id); err != ErrNodeNotFound {
			t.Errorf("node %s should be deleted after space deletion", id)
		}
	}

	// All blobs should be gone
	if _, ok := blob.blobs[BlobKey("s1", "blob-f1")]; ok {
		t.Error("blob should be deleted after space deletion")
	}

	// Trash should be empty
	trashItems, _ := store.ListTrash("s1")
	if len(trashItems) != 0 {
		t.Errorf("expected 0 trash entries, got %d", len(trashItems))
	}
}
