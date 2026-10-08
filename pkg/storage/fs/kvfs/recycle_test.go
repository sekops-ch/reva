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
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
	"github.com/opencloud-eu/reva/v2/pkg/storage"
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
			Name: "old-dir", Type: NodeTypeDir, Size: 500, MTime: 1000, ETag: "old-etag",
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

	// It comes back empty: a fresh date and etag, and no size for the parents.
	if node.MTime == 1000 || node.ETag == "old-etag" || node.ETag == "" {
		t.Errorf("restored empty dir: MTime %d, ETag %q; want a fresh date and etag", node.MTime, node.ETag)
	}
	if node.Size != 0 {
		t.Errorf("restored empty dir: Size %d, want 0", node.Size)
	}
	if root, _, _ := store.GetNode("s1", "root"); root.Size != 0 {
		t.Errorf("root Size %d, want 0: the lost subtree adds nothing", root.Size)
	}
}

func trashKeyOf(t *testing.T, store *mockMetadataStore, nodeID string) string {
	t.Helper()
	items, _ := store.ListTrash("s1")
	for _, it := range items {
		if it.NodeID == nodeID {
			return it.Key
		}
	}
	t.Fatalf("no trash entry for %s", nodeID)
	return ""
}

// A restored file keeps its date and etag, so clients that kept a copy download nothing.
func TestRestoreFile_KeepsDateAndEtag(t *testing.T) {
	for _, target := range []string{"", "/renamed.txt"} {
		t.Run("target="+target, func(t *testing.T) {
			store := newMockMetadataStore()
			d := testDriver(store, newMockBlobStore())
			setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
			ctx := testContext()
			if err := d.Delete(ctx, f1Ref()); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			rootBefore, _, _ := store.GetNode("s1", "root")

			var restoreRef *provider.Reference
			if target != "" {
				restoreRef = &provider.Reference{Path: target}
			}
			spaceRef := &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1"}}
			if err := d.RestoreRecycleItem(ctx, spaceRef, trashKeyOf(t, store, "f1"), "", restoreRef); err != nil {
				t.Fatalf("RestoreRecycleItem: %v", err)
			}

			node, _, err := store.GetNode("s1", "f1")
			if err != nil {
				t.Fatalf("restored file: %v", err)
			}
			if node.MTime != 1000 || node.ETag != "old-etag" {
				t.Errorf("restored file: MTime %d, ETag %q; want 1000, old-etag", node.MTime, node.ETag)
			}
			if root, _, _ := store.GetNode("s1", "root"); root.ETag == rootBefore.ETag {
				t.Error("the parent's etag must change")
			}
		})
	}
}

// A restored directory keeps its date and gets a fresh etag, so clients list it again; its
// files keep theirs.
func TestRestoreDir_KeepsDateFreshEtag(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithDir(store, "s1", "root", "dir1", "mydir", map[string]string{"a.txt": "f1"})
	store.nodes["s1.dir1"].MTime, store.nodes["s1.dir1"].ETag = 1000, "dir-etag"
	store.nodes["s1.f1"].MTime, store.nodes["s1.f1"].ETag = 2000, "f1-etag"
	ctx := testContext()
	dirRef := &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "dir1"}}
	if err := d.Delete(ctx, dirRef); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	spaceRef := &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1"}}
	if err := d.RestoreRecycleItem(ctx, spaceRef, trashKeyOf(t, store, "dir1"), "", nil); err != nil {
		t.Fatalf("RestoreRecycleItem: %v", err)
	}

	dir, _, _ := store.GetNode("s1", "dir1")
	if dir.MTime != 1000 {
		t.Errorf("restored dir MTime %d, want 1000", dir.MTime)
	}
	if dir.ETag == "dir-etag" || dir.ETag == "" {
		t.Errorf("restored dir ETag %q, want a fresh etag", dir.ETag)
	}
	if f, _, _ := store.GetNode("s1", "f1"); f.MTime != 2000 || f.ETag != "f1-etag" {
		t.Errorf("file in restored dir: MTime %d, ETag %q; want 2000, f1-etag", f.MTime, f.ETag)
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

// takeName makes name in parent map to otherID, as a create by another request would.
func takeName(store *mockMetadataStore, parentID, name, otherID string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.nodes["s1."+otherID] = &NodeEntry{
		ID: otherID, SpaceID: "s1", ParentID: parentID, Name: name, Type: NodeTypeFile,
		BlobID: "blob-" + otherID, Size: 7, Owner: "test-user-id", MimeType: "text/plain",
	}
	store.nodeRevs["s1."+otherID] = 1
	store.children["s1."+parentID][name] = otherID
	store.childRevs["s1."+parentID]++
}

// lateClashStore lets another request take a name right after the first read of a parent's
// children.
type lateClashStore struct {
	*mockMetadataStore
	parentID, name, otherID string
	reads                   int
}

func (s *lateClashStore) GetChildren(spaceID, parentID string) (ChildMap, uint64, error) {
	children, rev, err := s.mockMetadataStore.GetChildren(spaceID, parentID)
	if parentID == s.parentID {
		if s.reads++; s.reads == 1 {
			takeName(s.mockMetadataStore, s.parentID, s.name, s.otherID)
		}
	}
	return children, rev, err
}

func isAlreadyExists(err error) bool {
	_, ok := err.(errtypes.IsAlreadyExists)
	return ok
}

// trashCase deletes one item and returns how to restore it and what a refused restore must
// leave unchanged.
type trashCase struct {
	name   string
	nodeID string
	setup  func(store *mockMetadataStore, d *kvfsDriver) (trashKey, restoreName string)
	intact func(t *testing.T, store *mockMetadataStore)
}

func trashCases() []trashCase {
	return []trashCase{
		{
			name: "file", nodeID: "f1",
			setup: func(store *mockMetadataStore, d *kvfsDriver) (string, string) {
				setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
				if err := d.Delete(testContext(), f1Ref()); err != nil {
					panic(err)
				}
				items, _ := store.ListTrash("s1")
				return items[0].Key, "file.txt"
			},
			intact: func(t *testing.T, store *mockMetadataStore) {
				if _, _, err := store.GetNode("s1", "f1"); err != ErrNodeNotFound {
					t.Errorf("file node after a refused restore: err %v, want it still trashed", err)
				}
			},
		},
		{
			name: "directory", nodeID: "dir1",
			setup: func(store *mockMetadataStore, d *kvfsDriver) (string, string) {
				setupSpaceWithDir(store, "s1", "root", "dir1", "mydir", map[string]string{"a.txt": "fa"})
				store.nodes["s1.dir1"].ETag = "dir-etag"
				dirRef := &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "dir1"}}
				if err := d.Delete(testContext(), dirRef); err != nil {
					panic(err)
				}
				items, _ := store.ListTrash("s1")
				return items[0].Key, "mydir"
			},
			intact: func(t *testing.T, store *mockMetadataStore) {
				dir, _, err := store.GetNode("s1", "dir1")
				if err != nil || dir.ParentID != "root" || dir.Name != "mydir" || dir.ETag != "dir-etag" {
					t.Errorf("trashed dir after a refused restore: %+v (err %v), want it unchanged", dir, err)
				}
			},
		},
		{
			name: "old-format directory", nodeID: "dead-dir",
			setup: func(store *mockMetadataStore, d *kvfsDriver) (string, string) {
				setupSpaceEmpty(store, "s1", "root")
				store.trash["s1.old-trash"] = &TrashEntry{
					Key: "old-trash", NodeID: "dead-dir", SpaceID: "s1", OriginalPath: "/old-dir", DeletionTime: 1000,
					Node: NodeEntry{
						ID: "dead-dir", SpaceID: "s1", ParentID: "root", Name: "old-dir", Type: NodeTypeDir,
						Owner: "test-user-id", MimeType: "httpd/unix-directory",
					},
				}
				return "old-trash", "old-dir"
			},
			intact: func(t *testing.T, store *mockMetadataStore) {
				if _, _, err := store.GetNode("s1", "dead-dir"); err != ErrNodeNotFound {
					t.Errorf("old-format dir node after a refused restore: err %v, want none", err)
				}
				store.mu.Lock()
				_, ok := store.children["s1.dead-dir"]
				store.mu.Unlock()
				if ok {
					t.Error("old-format dir children map after a refused restore, want none")
				}
			},
		},
	}
}

func assertStillTrashed(t *testing.T, store *mockMetadataStore, key string) {
	t.Helper()
	if _, err := store.GetTrash("s1", key); err != nil {
		t.Errorf("trash entry %s: %v, want it kept", key, err)
	}
}

// A restore never takes a name that maps to another node; the item stays in the trash and
// restores under another name.
func TestRestore_NameTakenByAnotherNode(t *testing.T) {
	spaceRef := &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1"}}
	for _, late := range []bool{false, true} {
		for _, c := range trashCases() {
			t.Run(fmt.Sprintf("%s/late=%v", c.name, late), func(t *testing.T) {
				store := newMockMetadataStore()
				d := testDriver(store, newMockBlobStore())
				key, name := c.setup(store, d)
				if late {
					d.store = &lateClashStore{mockMetadataStore: store, parentID: "root", name: name, otherID: "f9"}
				} else {
					takeName(store, "root", name, "f9")
				}

				if err := d.RestoreRecycleItem(testContext(), spaceRef, key, "", nil); !isAlreadyExists(err) {
					t.Fatalf("RestoreRecycleItem onto a taken name = %v, want AlreadyExists", err)
				}
				if children, _, _ := store.GetChildren("s1", "root"); children[name] != "f9" {
					t.Errorf("root[%q] = %q, want f9 kept", name, children[name])
				}
				c.intact(t, store)
				assertStillTrashed(t, store, key)

				d.store = store
				if err := d.RestoreRecycleItem(testContext(), spaceRef, key, "", &provider.Reference{Path: "/elsewhere"}); err != nil {
					t.Fatalf("restore under another name: %v", err)
				}
				if children, _, _ := store.GetChildren("s1", "root"); children["elsewhere"] != c.nodeID || children[name] != "f9" {
					t.Errorf("root children = %v, want elsewhere=%s and %s=f9", children, c.nodeID, name)
				}
			})
		}
	}
}

// A failed parent update undoes the node write, so the item stays restorable.
func TestRestore_ParentUpdateFailureUndoesTheNode(t *testing.T) {
	spaceRef := &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1"}}
	for _, c := range trashCases() {
		t.Run(c.name, func(t *testing.T) {
			store := newMockMetadataStore()
			d := testDriver(store, newMockBlobStore())
			key, name := c.setup(store, d)
			store.putChildrenErr = errors.New("children bucket unavailable")

			if err := d.RestoreRecycleItem(testContext(), spaceRef, key, "", nil); err == nil {
				t.Fatal("RestoreRecycleItem with a failing parent update succeeded")
			}
			c.intact(t, store)
			assertStillTrashed(t, store, key)

			store.putChildrenErr = nil
			if err := d.RestoreRecycleItem(testContext(), spaceRef, key, "", nil); err != nil {
				t.Fatalf("retried restore: %v", err)
			}
			if children, _, _ := store.GetChildren("s1", "root"); children[name] != c.nodeID {
				t.Errorf("root[%q] = %q, want %s", name, children[name], c.nodeID)
			}
		})
	}
}

// A restored item carries no lock, as decomposedfs drops the lock on delete; a stale lock would
// block every later upload and delete of the restored item.
func TestRestore_DropsTheLock(t *testing.T) {
	spaceRef := &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1"}}
	lock := func() *LockEntry { return &LockEntry{LockID: "lock-123", Type: 1, UserID: "alice"} }
	for _, c := range trashCases() {
		t.Run(c.name, func(t *testing.T) {
			store := newMockMetadataStore()
			d := testDriver(store, newMockBlobStore())
			key, _ := c.setup(store, d)
			if tr := store.trash["s1."+key]; tr != nil {
				tr.Node.Lock = lock()
			}
			if n := store.nodes["s1."+c.nodeID]; n != nil {
				n.Lock = lock()
			}

			if err := d.RestoreRecycleItem(testContext(), spaceRef, key, "", nil); err != nil {
				t.Fatalf("RestoreRecycleItem: %v", err)
			}
			node, _, err := store.GetNode("s1", c.nodeID)
			if err != nil {
				t.Fatalf("restored node: %v", err)
			}
			if node.Lock != nil {
				t.Errorf("restored node lock = %+v, want none", node.Lock)
			}
		})
	}
}

// trashedShareFixture holds folder F (file a.txt, folder sub) in s1's root, shared with bob as an
// editor, and deletes F as its owner.
func trashedShareFixture(t *testing.T) (*kvfsDriver, *mockMetadataStore) {
	t.Helper()
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithDir(store, "s1", "root", "F", "F", map[string]string{"a.txt": "fa"})
	store.nodes["s1.sub"] = &NodeEntry{ID: "sub", SpaceID: "s1", ParentID: "F", Name: "sub", Type: NodeTypeDir, Owner: "test-user-id"}
	store.nodeRevs["s1.sub"] = 1
	store.children["s1.F"]["sub"] = "sub"
	store.children["s1.sub"] = ChildMap{}
	store.childRevs["s1.sub"] = 1
	store.nodes["s1.F"].Grants = map[string]*GrantEntry{
		"u:bob": {GranteeType: "user", GranteeID: "bob", Permissions: permissionsToUint32(&provider.ResourcePermissions{
			Stat: true, ListContainer: true, InitiateFileDownload: true, InitiateFileUpload: true, CreateContainer: true, Move: true,
			Delete: true,
		})},
	}
	if err := d.Delete(testContext(), &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "F"}}); err != nil {
		t.Fatalf("Delete F: %v", err)
	}
	return d, store
}

// A trashed folder is no write target: its subtree stays in KV only for a restore, so writes
// through a share or a link on it would be lost on purge and count against the quota for good.
func TestTrashedFolder_IsNoWriteTarget(t *testing.T) {
	writes := map[string]func(d *kvfsDriver, ctx context.Context, ref *provider.Reference) error{
		"TouchFile": func(d *kvfsDriver, ctx context.Context, ref *provider.Reference) error {
			return d.TouchFile(ctx, ref, false, "")
		},
		"CreateDir": func(d *kvfsDriver, ctx context.Context, ref *provider.Reference) error {
			return d.CreateDir(ctx, ref)
		},
		"InitiateUpload": func(d *kvfsDriver, ctx context.Context, ref *provider.Reference) error {
			_, err := d.InitiateUpload(ctx, ref, 0, map[string]string{})
			return err
		},
		"Upload": func(d *kvfsDriver, ctx context.Context, ref *provider.Reference) error {
			_, err := d.Upload(ctx, storage.UploadRequest{Ref: ref, Body: io.NopCloser(strings.NewReader("lost")), Length: 4}, nil)
			return err
		},
		"Move into": func(d *kvfsDriver, ctx context.Context, ref *provider.Reference) error {
			return d.Move(ctx, &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "loose"}}, ref)
		},
	}
	for opName, write := range writes {
		for refName, path := range map[string]string{"top level": "./new.txt", "subfolder": "./sub/new.txt"} {
			for who, ctx := range map[string]context.Context{"grantee": testContextWithUser("bob", nil), "owner": testContext()} {
				t.Run(opName+"/"+refName+"/"+who, func(t *testing.T) {
					d, store := trashedShareFixture(t)
					store.nodes["s1.loose"] = &NodeEntry{ID: "loose", SpaceID: "s1", ParentID: "root", Name: "loose.txt", Type: NodeTypeFile, Owner: "test-user-id"}
					store.nodeRevs["s1.loose"] = 1
					store.children["s1.root"]["loose.txt"] = "loose"
					store.nodes["s1.loose"].Grants = store.nodes["s1.F"].Grants
					rootSize := store.nodes["s1.root"].Size
					children := childrenSnapshot(store)

					err := write(d, ctx, &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "F"}, Path: path})
					if !isNotFound(err) {
						t.Fatalf("%s into the trashed F = %v (%T), want NotFound", opName, err, err)
					}
					if got := childrenSnapshot(store); !reflect.DeepEqual(got, children) {
						t.Errorf("children changed: %v", got)
					}
					if store.nodes["s1.root"].Size != rootSize {
						t.Errorf("root size %d, want %d", store.nodes["s1.root"].Size, rootSize)
					}
				})
			}
		}
	}

	t.Run("a file in the trashed folder", func(t *testing.T) {
		d, _ := trashedShareFixture(t)
		_, err := d.InitiateUpload(testContextWithUser("bob", nil), &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "fa"}, Path: "."}, 0, map[string]string{})
		if !isNotFound(err) {
			t.Errorf("empty save to a file in the trashed F = %v, want NotFound", err)
		}
	})

	t.Run("restored folder takes writes again", func(t *testing.T) {
		d, store := trashedShareFixture(t)
		if !store.nodes["s1.F"].Trashed {
			t.Fatal("Delete did not mark the folder trashed")
		}
		spaceRef := &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1"}}
		if err := d.RestoreRecycleItem(testContext(), spaceRef, trashKeyOf(t, store, "F"), "", nil); err != nil {
			t.Fatalf("restore F: %v", err)
		}
		if store.nodes["s1.F"].Trashed {
			t.Error("the restore did not clear the mark")
		}
		if err := d.TouchFile(testContextWithUser("bob", nil), &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "F"}, Path: "./new.txt"}, false, ""); err != nil {
			t.Errorf("TouchFile into the restored F: %v", err)
		}
	})
}

func fRef() *provider.Reference {
	return &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "F"}}
}

// A trashed folder and its content are no source of a move or a delete either: decomposedfs's
// trashed subtree is out of reach, and here a move would bring a still-marked folder back.
func TestTrashedFolder_IsNoSourceOfMoveOrDelete(t *testing.T) {
	t.Run("move the folder by id", func(t *testing.T) {
		d, store := trashedShareFixture(t)
		err := d.Move(testContext(), fRef(), &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"}, Path: "./G"})
		if !isNotFound(err) {
			t.Fatalf("Move of the trashed F = %v, want NotFound", err)
		}
		if children, _, _ := store.GetChildren("s1", "root"); len(children) != 0 {
			t.Errorf("root children = %v, want none", children)
		}
	})
	t.Run("delete the folder again by id", func(t *testing.T) {
		d, store := trashedShareFixture(t)
		// A new folder takes the trashed one's name.
		store.nodes["s1.F2"] = &NodeEntry{ID: "F2", SpaceID: "s1", ParentID: "root", Name: "F", Type: NodeTypeDir, Owner: "test-user-id"}
		store.nodeRevs["s1.F2"] = 1
		store.children["s1.root"]["F"] = "F2"
		if err := d.Delete(testContext(), fRef()); !isNotFound(err) {
			t.Fatalf("second Delete of the trashed F = %v, want NotFound", err)
		}
		if children, _, _ := store.GetChildren("s1", "root"); children["F"] != "F2" {
			t.Errorf("root children = %v, want the new F kept", children)
		}
		if items, _ := store.ListTrash("s1"); len(items) != 1 {
			t.Errorf("trash entries = %d, want 1", len(items))
		}
	})
	t.Run("delete inside the trashed share", func(t *testing.T) {
		d, store := trashedShareFixture(t)
		rootSize := store.nodes["s1.root"].Size
		err := d.Delete(testContextWithUser("bob", nil), &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "F"}, Path: "./a.txt"})
		if !isNotFound(err) {
			t.Fatalf("Delete inside the trashed F = %v, want NotFound", err)
		}
		if store.nodes["s1.root"].Size != rootSize {
			t.Errorf("root size %d, want %d", store.nodes["s1.root"].Size, rootSize)
		}
	})
}

// renameDuringDeleteStore renames the folder being deleted right after Delete writes its trash
// entry, as a concurrent MOVE would.
type renameDuringDeleteStore struct {
	*mockMetadataStore
	done bool
}

func (s *renameDuringDeleteStore) PutTrash(t *TrashEntry) error {
	if err := s.mockMetadataStore.PutTrash(t); err != nil {
		return err
	}
	if !s.done {
		s.done = true
		s.mockMetadataStore.mu.Lock()
		n := s.mockMetadataStore.nodes["s1."+t.NodeID]
		delete(s.mockMetadataStore.children["s1."+n.ParentID], n.Name)
		n.Name = "G"
		s.mockMetadataStore.children["s1."+n.ParentID]["G"] = n.ID
		s.mockMetadataStore.mu.Unlock()
	}
	return nil
}

// Delete marks only the folder it unlinked: one renamed meanwhile stays live and unmarked.
func TestDelete_MarksOnlyTheUnlinkedFolder(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithDir(store, "s1", "root", "F", "F", map[string]string{"a.txt": "fa"})
	d.store = &renameDuringDeleteStore{mockMetadataStore: store}
	if err := d.Delete(testContext(), fRef()); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if children, _, _ := store.GetChildren("s1", "root"); children["G"] != "F" {
		t.Fatalf("root children = %v, want the renamed G kept", children)
	}
	if store.nodes["s1.F"].Trashed {
		t.Error("the live, renamed folder is marked trashed")
	}
}

// An old-format restore never brings a mark back.
func TestRestoreDir_OldFormatClearsTheMark(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")
	store.trash["s1.old-trash"] = &TrashEntry{
		Key: "old-trash", NodeID: "dead-dir", SpaceID: "s1", OriginalPath: "/old-dir", DeletionTime: 1000,
		Node: NodeEntry{ID: "dead-dir", SpaceID: "s1", ParentID: "root", Name: "old-dir", Type: NodeTypeDir, Owner: "test-user-id", Trashed: true},
	}
	spaceRef := &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1"}}
	if err := d.RestoreRecycleItem(testContext(), spaceRef, "old-trash", "", nil); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if store.nodes["s1.dead-dir"].Trashed {
		t.Error("the restored directory is marked trashed")
	}
}

// A trash mark is only a hint: racing renames, moves or restores can leave it on a folder that is
// back in the tree. A marked folder that its parent still lists is live.
func TestStaleTrashMark_OnALiveFolderIsIgnored(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithDir(store, "s1", "root", "F", "F", map[string]string{"a.txt": "fa"})
	store.nodes["s1.F"].Trashed = true // stale: F is still listed in root
	bob := testContextWithUser("bob", nil)
	store.nodes["s1.F"].Grants = map[string]*GrantEntry{
		"u:bob": {GranteeType: "user", GranteeID: "bob", Permissions: permissionsToUint32(&provider.ResourcePermissions{
			Stat: true, ListContainer: true, InitiateFileUpload: true, CreateContainer: true, Move: true, Delete: true,
		})},
	}
	if err := d.TouchFile(bob, &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "F"}, Path: "./new.txt"}, false, ""); err != nil {
		t.Errorf("TouchFile into the live F with a stale mark: %v", err)
	}
	if err := d.Delete(bob, &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "F"}, Path: "./a.txt"}); err != nil {
		t.Errorf("Delete inside the live F with a stale mark: %v", err)
	}
	if err := d.Move(testContext(), fRef(), &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"}, Path: "./G"}); err != nil {
		t.Errorf("Move of the live F with a stale mark: %v", err)
	}
}
