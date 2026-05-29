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

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	ctxpkg "github.com/opencloud-eu/reva/v2/pkg/ctx"
	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
)

func contextWithLockID(ctx context.Context, lockID string) context.Context {
	return ctxpkg.ContextSetLockID(ctx, lockID)
}

// setupTwoFolders creates a space with root → folder-a (containing file.txt) and folder-b (empty).
func setupTwoFolders(store *mockMetadataStore) {
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

// --- Group 1: Authorization ---

func TestMove_SourceNonMemberDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextWithUser("stranger", nil)
	oldRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	newRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./renamed.txt",
	}

	err := d.Move(ctx, oldRef, newRef)
	if err == nil {
		t.Fatal("expected error for non-member")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

func TestMove_SourceReadOnlyDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	rootNode := store.nodes["s1.root"]
	rootNode.Grants = map[string]*GrantEntry{
		"u:bob": {GranteeType: "user", GranteeID: "bob", Permissions: permissionsToUint32(&provider.ResourcePermissions{
			Stat: true, InitiateFileDownload: true, ListContainer: true,
		})},
	}

	ctx := testContextWithUser("bob", nil)
	oldRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	newRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./renamed.txt",
	}

	err := d.Move(ctx, oldRef, newRef)
	if err == nil {
		t.Fatal("expected error for read-only user")
	}
	if _, ok := err.(errtypes.IsPermissionDenied); !ok {
		t.Errorf("expected PermissionDenied, got %T: %v", err, err)
	}
}

func TestMove_DestDirNoCreateContainerDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithDir(store, "s1", "root", "d1", "mydir", nil)

	// Grant bob Move + Stat on root (source auth), but NOT CreateContainer on root (dest auth)
	rootNode := store.nodes["s1.root"]
	rootNode.Grants = map[string]*GrantEntry{
		"u:bob": {GranteeType: "user", GranteeID: "bob", Permissions: permissionsToUint32(&provider.ResourcePermissions{
			Stat: true, Move: true, ListContainer: true,
		})},
	}

	ctx := testContextWithUser("bob", nil)
	oldRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "d1"},
	}
	newRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./renamed-dir",
	}

	err := d.Move(ctx, oldRef, newRef)
	if err == nil {
		t.Fatal("expected error for missing CreateContainer on dest")
	}
	if _, ok := err.(errtypes.IsPermissionDenied); !ok {
		t.Errorf("expected PermissionDenied, got %T: %v", err, err)
	}
}

func TestMove_DestFileNoUploadDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupTwoFolders(store)

	rootNode := store.nodes["s1.root"]
	rootNode.Grants = map[string]*GrantEntry{
		"u:bob": {GranteeType: "user", GranteeID: "bob", Permissions: permissionsToUint32(&provider.ResourcePermissions{
			Stat: true, Move: true, ListContainer: true,
		})},
	}

	ctx := testContextWithUser("bob", nil)
	oldRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	newRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./folder-b/file.txt",
	}

	err := d.Move(ctx, oldRef, newRef)
	if err == nil {
		t.Fatal("expected error for missing InitiateFileUpload on dest")
	}
	if _, ok := err.(errtypes.IsPermissionDenied); !ok {
		t.Errorf("expected PermissionDenied, got %T: %v", err, err)
	}
}

func TestMove_OwnerAllowed(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextWithUser("test-user-id", nil)
	oldRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	newRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./renamed.txt",
	}

	err := d.Move(ctx, oldRef, newRef)
	if err != nil {
		t.Fatalf("owner should be allowed to move: %v", err)
	}

	node, _, _ := store.GetNode("s1", "f1")
	if node.Name != "renamed.txt" {
		t.Errorf("expected name 'renamed.txt', got %q", node.Name)
	}
}

// --- Group 2: Safety Checks ---

func TestMove_SpaceRootRejected(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextWithUser("test-user-id", nil)
	oldRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
	}
	newRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./newname",
	}

	err := d.Move(ctx, oldRef, newRef)
	if err == nil {
		t.Fatal("expected error for moving space root")
	}
	if _, ok := err.(errtypes.IsBadRequest); !ok {
		t.Errorf("expected BadRequest, got %T: %v", err, err)
	}
}

func TestMove_LockedFileRejected(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	node := store.nodes["s1.f1"]
	node.Lock = &LockEntry{LockID: "lock-123", Type: 1, UserID: "alice"}

	ctx := testContextWithUser("test-user-id", nil)
	oldRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	newRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./renamed.txt",
	}

	err := d.Move(ctx, oldRef, newRef)
	if err == nil {
		t.Fatal("expected error for locked file")
	}
	if _, ok := err.(errtypes.IsLocked); !ok {
		t.Errorf("expected Locked, got %T: %v", err, err)
	}
}

func TestMove_LockedFileWithCorrectLockID(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	node := store.nodes["s1.f1"]
	node.Lock = &LockEntry{LockID: "lock-123", Type: 1, UserID: "alice"}

	ctx := testContextWithUser("test-user-id", nil)
	ctx = contextWithLockID(ctx, "lock-123")
	oldRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	newRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./renamed.txt",
	}

	err := d.Move(ctx, oldRef, newRef)
	if err != nil {
		t.Fatalf("move with correct lock ID should succeed: %v", err)
	}
}

func TestMove_CrossSpaceRejected(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
	setupSpaceEmpty(store, "s2", "root2")

	ctx := testContextWithUser("test-user-id", nil)
	oldRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	newRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s2", OpaqueId: "root2"},
		Path:       "./file.txt",
	}

	err := d.Move(ctx, oldRef, newRef)
	if err == nil {
		t.Fatal("expected error for cross-space move")
	}
	if _, ok := err.(errtypes.IsBadRequest); !ok {
		t.Errorf("expected BadRequest, got %T: %v", err, err)
	}
}

// --- Group 3: Collision Check ---

func TestMove_CollisionReturnsAlreadyExists(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupTwoFolders(store)

	// Add a file "existing.txt" in folder-b
	store.nodes["s1.f2"] = &NodeEntry{
		ID: "f2", SpaceID: "s1", ParentID: "db",
		Name: "existing.txt", Type: NodeTypeFile,
		Owner: "test-user-id", MimeType: "text/plain",
	}
	store.nodeRevs["s1.f2"] = 1
	store.children["s1.db"] = ChildMap{"existing.txt": "f2"}
	store.childRevs["s1.db"] = 1

	ctx := testContextWithUser("test-user-id", nil)
	oldRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	newRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./folder-b/existing.txt",
	}

	err := d.Move(ctx, oldRef, newRef)
	if err == nil {
		t.Fatal("expected error for collision")
	}
	if _, ok := err.(errtypes.IsAlreadyExists); !ok {
		t.Errorf("expected AlreadyExists, got %T: %v", err, err)
	}

	// Verify node metadata was reverted (rollback)
	node, _, _ := store.GetNode("s1", "f1")
	if node.ParentID != "da" || node.Name != "file.txt" {
		t.Errorf("node metadata should be reverted: got parent=%s name=%s", node.ParentID, node.Name)
	}
}

func TestMove_SameNodeRenameSucceeds(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextWithUser("test-user-id", nil)
	oldRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	newRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./renamed.txt",
	}

	err := d.Move(ctx, oldRef, newRef)
	if err != nil {
		t.Fatalf("rename should succeed: %v", err)
	}

	node, _, _ := store.GetNode("s1", "f1")
	if node.Name != "renamed.txt" {
		t.Errorf("expected name 'renamed.txt', got %q", node.Name)
	}

	children, _, _ := store.GetChildren("s1", "root")
	if children["renamed.txt"] != "f1" {
		t.Error("expected f1 in children under 'renamed.txt'")
	}
	if _, exists := children["file.txt"]; exists {
		t.Error("old name 'file.txt' should be removed from children")
	}
}

func TestMove_OverwriteSelfNoCollision(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextWithUser("test-user-id", nil)
	oldRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	// Move to same name in same parent (no-op move, should not trigger collision)
	newRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./file.txt",
	}

	err := d.Move(ctx, oldRef, newRef)
	if err != nil {
		t.Fatalf("self-move should succeed: %v", err)
	}
}

// --- Group 4: Ordering Verification ---

func TestMove_NodeUpdatedBeforeChildrenMap(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupTwoFolders(store)

	ctx := testContextWithUser("test-user-id", nil)
	oldRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	newRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./folder-b/file.txt",
	}

	err := d.Move(ctx, oldRef, newRef)
	if err != nil {
		t.Fatalf("move should succeed: %v", err)
	}

	// After successful move, node should point to new parent
	node, _, _ := store.GetNode("s1", "f1")
	if node.ParentID != "db" {
		t.Errorf("expected ParentID 'db', got %q", node.ParentID)
	}
	if node.Name != "file.txt" {
		t.Errorf("expected Name 'file.txt', got %q", node.Name)
	}

	// New parent should contain the node
	newChildren, _, _ := store.GetChildren("s1", "db")
	if newChildren["file.txt"] != "f1" {
		t.Error("expected f1 in new parent children")
	}

	// Old parent should NOT contain the node
	oldChildren, _, _ := store.GetChildren("s1", "da")
	if _, exists := oldChildren["file.txt"]; exists {
		t.Error("old parent should not contain the moved file")
	}
}

// --- Group 5: Functional ---

func TestMove_RenameInPlace(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "old-name.txt")

	ctx := testContextWithUser("test-user-id", nil)
	oldRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	newRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./new-name.txt",
	}

	err := d.Move(ctx, oldRef, newRef)
	if err != nil {
		t.Fatalf("rename should succeed: %v", err)
	}

	node, _, _ := store.GetNode("s1", "f1")
	if node.Name != "new-name.txt" {
		t.Errorf("expected 'new-name.txt', got %q", node.Name)
	}
	if node.ParentID != "root" {
		t.Errorf("parent should stay 'root', got %q", node.ParentID)
	}
	if node.MTime == 1000 {
		t.Error("MTime should be updated")
	}
	if node.ETag == "old-etag" {
		t.Error("ETag should be updated")
	}

	children, _, _ := store.GetChildren("s1", "root")
	if _, ok := children["old-name.txt"]; ok {
		t.Error("old name should be gone from children")
	}
	if children["new-name.txt"] != "f1" {
		t.Error("new name should be in children")
	}
}

func TestMove_AcrossDirectories(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupTwoFolders(store)

	ctx := testContextWithUser("test-user-id", nil)
	oldRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	newRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./folder-b/moved-file.txt",
	}

	err := d.Move(ctx, oldRef, newRef)
	if err != nil {
		t.Fatalf("cross-directory move should succeed: %v", err)
	}

	node, _, _ := store.GetNode("s1", "f1")
	if node.ParentID != "db" {
		t.Errorf("expected ParentID 'db', got %q", node.ParentID)
	}
	if node.Name != "moved-file.txt" {
		t.Errorf("expected name 'moved-file.txt', got %q", node.Name)
	}

	// Verify children maps
	oldChildren, _, _ := store.GetChildren("s1", "da")
	if _, exists := oldChildren["file.txt"]; exists {
		t.Error("file should be removed from old parent")
	}

	newChildren, _, _ := store.GetChildren("s1", "db")
	if newChildren["moved-file.txt"] != "f1" {
		t.Error("file should appear in new parent")
	}
}
