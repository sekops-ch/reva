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
	"io"
	"sync"
	"sync/atomic"
	"testing"

	user "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	types "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"

	ctxpkg "github.com/opencloud-eu/reva/v2/pkg/ctx"
	"github.com/opencloud-eu/reva/v2/pkg/storage"
)

func deleteSpaceRequest(spaceID string) *provider.DeleteStorageSpaceRequest {
	return &provider.DeleteStorageSpaceRequest{
		Id: &provider.StorageSpaceId{OpaqueId: spaceID},
	}
}

func TestDeleteStorageSpace_MissingSpaceID(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	err := d.DeleteStorageSpace(context.Background(), &provider.DeleteStorageSpaceRequest{})
	if err == nil {
		t.Fatal("expected error for missing space ID")
	}
}

func TestDeleteStorageSpace_EmptySpace(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceEmpty(store, "space-1", "root-1")

	err := d.DeleteStorageSpace(context.Background(), deleteSpaceRequest("space-1"))
	if err != nil {
		t.Fatalf("DeleteStorageSpace failed: %v", err)
	}

	if _, _, err := store.GetSpace("space-1"); err != ErrSpaceNotFound {
		t.Error("space entry should be deleted")
	}
	if _, _, err := store.GetNode("space-1", "root-1"); err != ErrNodeNotFound {
		t.Error("root node should be deleted")
	}
}

func TestDeleteStorageSpace_CleansUpNodes(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceWithFile(store, "space-1", "root-1", "file-1", "test.txt")

	err := d.DeleteStorageSpace(context.Background(), deleteSpaceRequest("space-1"))
	if err != nil {
		t.Fatalf("DeleteStorageSpace failed: %v", err)
	}

	if _, _, err := store.GetNode("space-1", "file-1"); err != ErrNodeNotFound {
		t.Error("file node should be deleted")
	}
	if _, _, err := store.GetNode("space-1", "root-1"); err != ErrNodeNotFound {
		t.Error("root node should be deleted")
	}
}

func TestDeleteStorageSpace_CleansUpBlobs(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceWithFile(store, "space-1", "root-1", "file-1", "test.txt")
	blobKey := BlobKey("space-1", "old-blob")
	blob.blobs[blobKey] = []byte("data")

	err := d.DeleteStorageSpace(context.Background(), deleteSpaceRequest("space-1"))
	if err != nil {
		t.Fatalf("DeleteStorageSpace failed: %v", err)
	}

	if _, exists := blob.blobs[blobKey]; exists {
		t.Error("blob should be deleted from S3")
	}
}

func TestDeleteStorageSpace_CleansUpTrash(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceEmpty(store, "space-1", "root-1")

	trashBlobKey := BlobKey("space-1", "trash-blob")
	blob.blobs[trashBlobKey] = []byte("trashed data")

	store.trash["space-1.t1"] = &TrashEntry{
		Key: "t1", NodeID: "tn1", SpaceID: "space-1",
		Node: NodeEntry{Type: NodeTypeFile, BlobID: "trash-blob", Size: 12},
	}
	store.trash["space-1.t2"] = &TrashEntry{
		Key: "t2", NodeID: "tn2", SpaceID: "space-1",
		Node: NodeEntry{Type: NodeTypeDir, Size: 0},
	}

	err := d.DeleteStorageSpace(context.Background(), deleteSpaceRequest("space-1"))
	if err != nil {
		t.Fatalf("DeleteStorageSpace failed: %v", err)
	}

	items, _ := store.ListTrash("space-1")
	if len(items) != 0 {
		t.Errorf("expected 0 trash items, got %d", len(items))
	}
	if _, exists := blob.blobs[trashBlobKey]; exists {
		t.Error("trash blob should be deleted from S3")
	}
}

func TestDeleteStorageSpace_CleansUpVersions(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceWithFile(store, "space-1", "root-1", "file-1", "test.txt")

	verBlobKey := BlobKey("space-1", "ver-blob")
	blob.blobs[verBlobKey] = []byte("version data")

	store.versions = append(store.versions, &VersionEntry{
		Key: "v1", NodeID: "file-1", SpaceID: "space-1",
		BlobID: "ver-blob", BlobSize: 12, Size: 12,
	})
	// Version from another space — should NOT be deleted
	store.versions = append(store.versions, &VersionEntry{
		Key: "v2", NodeID: "other-file", SpaceID: "other-space",
		BlobID: "other-blob", BlobSize: 5, Size: 5,
	})

	err := d.DeleteStorageSpace(context.Background(), deleteSpaceRequest("space-1"))
	if err != nil {
		t.Fatalf("DeleteStorageSpace failed: %v", err)
	}

	if _, exists := blob.blobs[verBlobKey]; exists {
		t.Error("version blob should be deleted from S3")
	}

	// Other space's version should remain
	remaining, _ := store.ListAllVersions()
	if len(remaining) != 1 {
		t.Fatalf("expected 1 remaining version, got %d", len(remaining))
	}
	if remaining[0].SpaceID != "other-space" {
		t.Error("remaining version should be from other-space")
	}
}

func TestDeleteStorageSpace_CleansUpUploads(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceEmpty(store, "space-1", "root-1")

	uploadBlobKey := BlobKey("space-1", "upload-blob")
	blob.blobs[uploadBlobKey] = []byte("partial")

	store.uploads["up-1"] = &UploadSession{
		ID:            "up-1",
		SpaceID:       "space-1",
		BlobID:        "upload-blob",
		S3MultipartID: "mp-123",
		Storage:       map[string]string{},
	}
	// Upload from another space
	store.uploads["up-2"] = &UploadSession{
		ID:      "up-2",
		SpaceID: "other-space",
		BlobID:  "other-blob",
		Storage: map[string]string{},
	}

	err := d.DeleteStorageSpace(context.Background(), deleteSpaceRequest("space-1"))
	if err != nil {
		t.Fatalf("DeleteStorageSpace failed: %v", err)
	}

	if _, err := store.GetUpload("up-1"); err != ErrNotFound {
		t.Error("upload session should be deleted")
	}
	if _, err := store.GetUpload("up-2"); err != nil {
		t.Error("other space's upload should remain")
	}

	if len(blob.abortCalled) != 1 || blob.abortCalled[0] != "mp-123" {
		t.Errorf("expected abort called with mp-123, got %v", blob.abortCalled)
	}
}

func TestDeleteStorageSpace_NestedDirectories(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceEmpty(store, "space-1", "root-1")

	// Build tree: root-1 -> dir-1 -> dir-2 -> file-1
	store.nodes["space-1.dir-1"] = &NodeEntry{
		ID: "dir-1", SpaceID: "space-1", ParentID: "root-1",
		Name: "subdir", Type: NodeTypeDir, Owner: "test-user-id",
	}
	store.nodeRevs["space-1.dir-1"] = 1
	store.children["space-1.root-1"] = ChildMap{"subdir": "dir-1"}

	store.nodes["space-1.dir-2"] = &NodeEntry{
		ID: "dir-2", SpaceID: "space-1", ParentID: "dir-1",
		Name: "nested", Type: NodeTypeDir, Owner: "test-user-id",
	}
	store.nodeRevs["space-1.dir-2"] = 1
	store.children["space-1.dir-1"] = ChildMap{"nested": "dir-2"}

	store.nodes["space-1.file-1"] = &NodeEntry{
		ID: "file-1", SpaceID: "space-1", ParentID: "dir-2",
		Name: "deep.txt", Type: NodeTypeFile, BlobID: "deep-blob",
		BlobSize: 42, Size: 42, Owner: "test-user-id",
	}
	store.nodeRevs["space-1.file-1"] = 1
	store.children["space-1.dir-2"] = ChildMap{"deep.txt": "file-1"}

	deepBlobKey := BlobKey("space-1", "deep-blob")
	blob.blobs[deepBlobKey] = []byte("deep content")

	err := d.DeleteStorageSpace(context.Background(), deleteSpaceRequest("space-1"))
	if err != nil {
		t.Fatalf("DeleteStorageSpace failed: %v", err)
	}

	for _, nodeID := range []string{"root-1", "dir-1", "dir-2", "file-1"} {
		if _, _, err := store.GetNode("space-1", nodeID); err != ErrNodeNotFound {
			t.Errorf("node %s should be deleted", nodeID)
		}
	}

	if _, exists := blob.blobs[deepBlobKey]; exists {
		t.Error("deeply nested blob should be deleted")
	}

	if _, _, err := store.GetSpace("space-1"); err != ErrSpaceNotFound {
		t.Error("space entry should be deleted")
	}
}

// --- DisableVersioning confirmation test for simple Upload path ---

func TestUpload_DisableVersioning_NoVersionCreated(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	d.opts.DisableVersioning = true

	setupSpaceWithFile(store, "space-1", "root-1", "file-1", "test.txt")

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "root-1"},
		Path:       "./test.txt",
	}

	_, err := d.Upload(ctx, storage.UploadRequest{
		Ref:    ref,
		Body:   io.NopCloser(bytes.NewReader([]byte("new content"))),
		Length: 11,
	}, nil)
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	versions, _ := store.ListVersions("space-1", "file-1")
	if len(versions) != 0 {
		t.Errorf("expected 0 versions with DisableVersioning=true, got %d", len(versions))
	}
}

func TestDeleteStorageSpace_DoesNotAffectOtherSpaces(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceWithFile(store, "space-1", "root-1", "file-1", "test.txt")
	setupSpaceWithFile(store, "space-2", "root-2", "file-2", "other.txt")

	blob.blobs[BlobKey("space-1", "old-blob")] = []byte("s1 data")
	blob.blobs[BlobKey("space-2", "old-blob")] = []byte("s2 data")

	err := d.DeleteStorageSpace(context.Background(), deleteSpaceRequest("space-1"))
	if err != nil {
		t.Fatalf("DeleteStorageSpace failed: %v", err)
	}

	// space-2 should be untouched
	if _, _, err := store.GetSpace("space-2"); err != nil {
		t.Error("space-2 should still exist")
	}
	if _, _, err := store.GetNode("space-2", "file-2"); err != nil {
		t.Error("space-2 file should still exist")
	}
	if _, exists := blob.blobs[BlobKey("space-2", "old-blob")]; !exists {
		t.Error("space-2 blob should still exist")
	}
}

// --- Per-space listing tests ---

func TestListVersionsBySpace_FiltersCorrectly(t *testing.T) {
	store := newMockMetadataStore()

	store.versions = append(store.versions,
		&VersionEntry{Key: "v1", NodeID: "f1", SpaceID: "s1", BlobID: "b1", Size: 10},
		&VersionEntry{Key: "v2", NodeID: "f2", SpaceID: "s1", BlobID: "b2", Size: 20},
		&VersionEntry{Key: "v3", NodeID: "f3", SpaceID: "s2", BlobID: "b3", Size: 30},
	)

	s1Versions, err := store.ListVersionsBySpace("s1")
	if err != nil {
		t.Fatalf("ListVersionsBySpace failed: %v", err)
	}
	if len(s1Versions) != 2 {
		t.Fatalf("expected 2 versions for s1, got %d", len(s1Versions))
	}

	s2Versions, err := store.ListVersionsBySpace("s2")
	if err != nil {
		t.Fatalf("ListVersionsBySpace failed: %v", err)
	}
	if len(s2Versions) != 1 {
		t.Fatalf("expected 1 version for s2, got %d", len(s2Versions))
	}
	if s2Versions[0].Key != "v3" {
		t.Errorf("expected version v3, got %s", s2Versions[0].Key)
	}

	empty, err := store.ListVersionsBySpace("nonexistent")
	if err != nil {
		t.Fatalf("ListVersionsBySpace for nonexistent space failed: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("expected 0 versions for nonexistent space, got %d", len(empty))
	}
}

func TestListUploadsBySpace_FiltersCorrectly(t *testing.T) {
	store := newMockMetadataStore()

	store.uploads["u1"] = &UploadSession{ID: "u1", SpaceID: "s1", BlobID: "b1", Storage: map[string]string{}}
	store.uploads["u2"] = &UploadSession{ID: "u2", SpaceID: "s1", BlobID: "b2", Storage: map[string]string{}}
	store.uploads["u3"] = &UploadSession{ID: "u3", SpaceID: "s2", BlobID: "b3", Storage: map[string]string{}}

	s1Uploads, err := store.ListUploadsBySpace("s1")
	if err != nil {
		t.Fatalf("ListUploadsBySpace failed: %v", err)
	}
	if len(s1Uploads) != 2 {
		t.Fatalf("expected 2 uploads for s1, got %d", len(s1Uploads))
	}

	s2Uploads, err := store.ListUploadsBySpace("s2")
	if err != nil {
		t.Fatalf("ListUploadsBySpace failed: %v", err)
	}
	if len(s2Uploads) != 1 {
		t.Fatalf("expected 1 upload for s2, got %d", len(s2Uploads))
	}
	if s2Uploads[0].ID != "u3" {
		t.Errorf("expected upload u3, got %s", s2Uploads[0].ID)
	}

	empty, err := store.ListUploadsBySpace("nonexistent")
	if err != nil {
		t.Fatalf("ListUploadsBySpace for nonexistent space failed: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("expected 0 uploads for nonexistent space, got %d", len(empty))
	}
}

func TestDeleteSpaceContents_PerSpaceCleanup(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceWithFile(store, "s1", "r1", "f1", "a.txt")
	setupSpaceWithFile(store, "s2", "r2", "f2", "b.txt")

	blob.blobs[BlobKey("s1", "old-blob")] = []byte("s1")
	blob.blobs[BlobKey("s2", "old-blob")] = []byte("s2")

	store.versions = append(store.versions,
		&VersionEntry{Key: "v1", NodeID: "f1", SpaceID: "s1", BlobID: "vb1", Size: 10},
		&VersionEntry{Key: "v2", NodeID: "f2", SpaceID: "s2", BlobID: "vb2", Size: 20},
	)
	blob.blobs[BlobKey("s1", "vb1")] = []byte("ver1")
	blob.blobs[BlobKey("s2", "vb2")] = []byte("ver2")

	store.uploads["u1"] = &UploadSession{
		ID: "u1", SpaceID: "s1", BlobID: "ub1",
		S3MultipartID: "mp-1", Storage: map[string]string{},
	}
	store.uploads["u2"] = &UploadSession{
		ID: "u2", SpaceID: "s2", BlobID: "ub2",
		S3MultipartID: "mp-2", Storage: map[string]string{},
	}

	store.trash["s1.t1"] = &TrashEntry{
		Key: "t1", NodeID: "tn1", SpaceID: "s1",
		Node: NodeEntry{Type: NodeTypeFile, BlobID: "tb1", Size: 5},
	}
	blob.blobs[BlobKey("s1", "tb1")] = []byte("trash")

	err := d.DeleteStorageSpace(context.Background(), deleteSpaceRequest("s1"))
	if err != nil {
		t.Fatalf("DeleteStorageSpace failed: %v", err)
	}

	// s1 fully cleaned
	if _, _, err := store.GetSpace("s1"); err != ErrSpaceNotFound {
		t.Error("s1 space should be deleted")
	}
	if _, exists := blob.blobs[BlobKey("s1", "vb1")]; exists {
		t.Error("s1 version blob should be deleted")
	}
	if _, err := store.GetUpload("u1"); err != ErrNotFound {
		t.Error("s1 upload should be deleted")
	}

	// s2 fully intact
	if _, _, err := store.GetSpace("s2"); err != nil {
		t.Error("s2 space should still exist")
	}
	if _, _, err := store.GetNode("s2", "f2"); err != nil {
		t.Error("s2 file should still exist")
	}
	s2Versions, _ := store.ListVersionsBySpace("s2")
	if len(s2Versions) != 1 {
		t.Errorf("expected 1 s2 version remaining, got %d", len(s2Versions))
	}
	if _, err := store.GetUpload("u2"); err != nil {
		t.Error("s2 upload should still exist")
	}
	if _, exists := blob.blobs[BlobKey("s2", "vb2")]; !exists {
		t.Error("s2 version blob should still exist")
	}
}

// --- ListStorageSpaces TYPE_USER filter ---
//
// Helpers below intentionally bypass the driver's CreateStorageSpace and
// poke the mock store directly so each test wires the exact (owner, type,
// grants) topology it exercises. testContextWithUser already exists for
// caller identity; the filter helpers are local to keep this block
// self-contained.

func userFilter(userID string) *provider.ListStorageSpacesRequest_Filter {
	return &provider.ListStorageSpacesRequest_Filter{
		Type: provider.ListStorageSpacesRequest_Filter_TYPE_USER,
		Term: &provider.ListStorageSpacesRequest_Filter_User{
			User: &user.UserId{OpaqueId: userID},
		},
	}
}

func ownerFilter(ownerID string) *provider.ListStorageSpacesRequest_Filter {
	return &provider.ListStorageSpacesRequest_Filter{
		Type: provider.ListStorageSpacesRequest_Filter_TYPE_OWNER,
		Term: &provider.ListStorageSpacesRequest_Filter_Owner{
			Owner: &user.UserId{OpaqueId: ownerID},
		},
	}
}

func spaceTypeFilter(spaceType string) *provider.ListStorageSpacesRequest_Filter {
	return &provider.ListStorageSpacesRequest_Filter{
		Type: provider.ListStorageSpacesRequest_Filter_TYPE_SPACE_TYPE,
		Term: &provider.ListStorageSpacesRequest_Filter_SpaceType{SpaceType: spaceType},
	}
}

// setupPersonalSpaceFor wires a personal space for ownerID directly into
// the mock store. Mirrors setupSpaceEmpty but with caller-controlled
// owner so we can build multi-user fixtures.
func setupPersonalSpaceFor(store *mockMetadataStore, spaceID, ownerID string) {
	store.spaces[spaceID] = &SpaceEntry{
		ID: spaceID, Type: "personal", Owner: ownerID,
		Name: ownerID, RootID: spaceID, Quota: -1,
	}
	store.spaceRevs[spaceID] = 1
	store.nodes[spaceID+"."+spaceID] = &NodeEntry{
		ID: spaceID, SpaceID: spaceID, Name: "root", Type: NodeTypeDir,
		Owner: ownerID, MimeType: "httpd/unix-directory",
	}
	store.nodeRevs[spaceID+"."+spaceID] = 1
	store.children[spaceID+"."+spaceID] = ChildMap{}
	store.childRevs[spaceID+"."+spaceID] = 1
}

func setupProjectSpaceFor(store *mockMetadataStore, spaceID, ownerID string, grants map[string]*GrantEntry) {
	store.spaces[spaceID] = &SpaceEntry{
		ID: spaceID, Type: "project", Owner: ownerID,
		Name: "Project " + spaceID, RootID: spaceID, Quota: -1,
	}
	store.spaceRevs[spaceID] = 1
	store.nodes[spaceID+"."+spaceID] = &NodeEntry{
		ID: spaceID, SpaceID: spaceID, Name: "root", Type: NodeTypeDir,
		Owner: ownerID, MimeType: "httpd/unix-directory",
		Grants: grants,
	}
	store.nodeRevs[spaceID+"."+spaceID] = 1
	store.children[spaceID+"."+spaceID] = ChildMap{}
	store.childRevs[spaceID+"."+spaceID] = 1
}

func TestListStorageSpaces_UserFilter_OwnerMatch(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupPersonalSpaceFor(store, "space-alice", "alice")
	setupPersonalSpaceFor(store, "space-bob", "bob")

	ctx := testContextWithUser("alice", nil)
	spaces, err := d.ListStorageSpaces(ctx, []*provider.ListStorageSpacesRequest_Filter{userFilter("alice")}, true)
	if err != nil {
		t.Fatalf("ListStorageSpaces failed: %v", err)
	}
	if len(spaces) != 1 {
		t.Fatalf("expected 1 space for alice, got %d", len(spaces))
	}
	if spaces[0].Id.OpaqueId != "space-alice" {
		t.Errorf("expected space-alice, got %s", spaces[0].Id.OpaqueId)
	}
}

func TestListStorageSpaces_UserFilter_GrantMatch(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupProjectSpaceFor(store, "proj-1", "alice", map[string]*GrantEntry{
		"u:bob": {GranteeType: "user", GranteeID: "bob", Permissions: permissionsToUint32(&provider.ResourcePermissions{
			Stat: true, InitiateFileDownload: true, ListContainer: true,
		})},
	})

	// Cross-user lookup: ctx user is admin, filter is for bob.
	ctx := testContextWithUser("admin", nil)
	spaces, err := d.ListStorageSpaces(ctx, []*provider.ListStorageSpacesRequest_Filter{userFilter("bob")}, true)
	if err != nil {
		t.Fatalf("ListStorageSpaces failed: %v", err)
	}
	if len(spaces) != 1 {
		t.Fatalf("expected 1 space for bob (via direct user grant), got %d", len(spaces))
	}
	if spaces[0].Id.OpaqueId != "proj-1" {
		t.Errorf("expected proj-1, got %s", spaces[0].Id.OpaqueId)
	}
}

func TestListStorageSpaces_UserFilter_GroupGrantMatch(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupProjectSpaceFor(store, "proj-1", "alice", map[string]*GrantEntry{
		"g:engineering": {GranteeType: "group", GranteeID: "engineering", Permissions: permissionsToUint32(&provider.ResourcePermissions{
			Stat: true, InitiateFileDownload: true, ListContainer: true,
		})},
	})

	// ctx user IS the subject (self-lookup), groups attached via ctx.
	ctx := testContextWithUser("carol", []string{"engineering"})
	spaces, err := d.ListStorageSpaces(ctx, []*provider.ListStorageSpacesRequest_Filter{userFilter("carol")}, true)
	if err != nil {
		t.Fatalf("ListStorageSpaces failed: %v", err)
	}
	if len(spaces) != 1 {
		t.Fatalf("expected 1 space for carol (via group grant), got %d", len(spaces))
	}
	if spaces[0].Id.OpaqueId != "proj-1" {
		t.Errorf("expected proj-1, got %s", spaces[0].Id.OpaqueId)
	}
}

func TestListStorageSpaces_UserFilter_NoMatch(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupPersonalSpaceFor(store, "space-alice", "alice")

	ctx := testContextWithUser("bob", nil)
	spaces, err := d.ListStorageSpaces(ctx, []*provider.ListStorageSpacesRequest_Filter{userFilter("bob")}, true)
	if err != nil {
		t.Fatalf("ListStorageSpaces returned error: %v", err)
	}
	if len(spaces) != 0 {
		t.Fatalf("expected 0 spaces for bob, got %d", len(spaces))
	}
}

func TestListStorageSpaces_UserFilter_CombinedWithSpaceType(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupPersonalSpaceFor(store, "space-alice", "alice")
	setupProjectSpaceFor(store, "proj-alice", "alice", nil)
	setupPersonalSpaceFor(store, "space-bob", "bob")

	ctx := testContextWithUser("alice", nil)
	spaces, err := d.ListStorageSpaces(ctx, []*provider.ListStorageSpacesRequest_Filter{
		spaceTypeFilter("personal"),
		userFilter("alice"),
	}, true)
	if err != nil {
		t.Fatalf("ListStorageSpaces failed: %v", err)
	}
	if len(spaces) != 1 {
		t.Fatalf("expected 1 result (alice's personal), got %d", len(spaces))
	}
	if spaces[0].Id.OpaqueId != "space-alice" {
		t.Errorf("expected space-alice, got %s", spaces[0].Id.OpaqueId)
	}
	if spaces[0].SpaceType != "personal" {
		t.Errorf("expected personal type, got %s", spaces[0].SpaceType)
	}
}

func TestListStorageSpaces_UserFilter_ReplacesUnfilteredBehaviour(t *testing.T) {
	// Regression scenario: pre-fix, the TYPE_USER filter was dropped on
	// the floor and TWO personal spaces were returned, causing Graph's
	// /users/{id}/drive to 500 with "expected to find a single drive but
	// fetched more".
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupPersonalSpaceFor(store, "space-alice", "alice")
	setupPersonalSpaceFor(store, "space-bob", "bob")

	ctx := testContextWithUser("alice", nil)
	spaces, err := d.ListStorageSpaces(ctx, []*provider.ListStorageSpacesRequest_Filter{
		spaceTypeFilter("personal"),
		userFilter("alice"),
	}, true)
	if err != nil {
		t.Fatalf("ListStorageSpaces failed: %v", err)
	}
	if len(spaces) != 1 {
		t.Fatalf("expected exactly 1 personal drive for alice in a multi-user cluster, got %d (Graph would 500)", len(spaces))
	}
}

func TestListStorageSpaces_OwnerFilter_StillStricter(t *testing.T) {
	// Semantic guarantee: TYPE_OWNER ⊂ TYPE_USER. Alice owns nothing
	// but is a grantee on bob's project. Filter OWNER=alice -> empty;
	// filter USER=alice -> bob's project.
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupProjectSpaceFor(store, "proj-bob", "bob", map[string]*GrantEntry{
		"u:alice": {GranteeType: "user", GranteeID: "alice", Permissions: permissionsToUint32(&provider.ResourcePermissions{
			Stat: true, ListContainer: true,
		})},
	})

	ctx := testContextWithUser("admin", nil)
	byOwner, err := d.ListStorageSpaces(ctx, []*provider.ListStorageSpacesRequest_Filter{ownerFilter("alice")}, true)
	if err != nil {
		t.Fatalf("ListStorageSpaces(owner=alice) failed: %v", err)
	}
	if len(byOwner) != 0 {
		t.Errorf("TYPE_OWNER=alice should return 0 (alice owns nothing), got %d", len(byOwner))
	}

	byUser, err := d.ListStorageSpaces(ctx, []*provider.ListStorageSpacesRequest_Filter{userFilter("alice")}, true)
	if err != nil {
		t.Fatalf("ListStorageSpaces(user=alice) failed: %v", err)
	}
	if len(byUser) != 1 {
		t.Errorf("TYPE_USER=alice should return 1 (granted on bob's project), got %d", len(byUser))
	}
}

// --- Deterministic personal-space ID + atomic dedup ---

// userCtxWithDisplayName wraps testContextWithUser to also seed DisplayName,
// which CreateStorageSpace uses indirectly (req.Name comes from the
// caller, but spaceToCS3 logs based on the SpaceEntry.Name; safer to set
// both ends explicitly).
func userCtxWithDisplayName(userID, displayName string) context.Context {
	return ctxpkg.ContextSetUser(context.Background(), &user.User{
		Id:          &user.UserId{OpaqueId: userID},
		DisplayName: displayName,
	})
}

func createPersonalReq(name string) *provider.CreateStorageSpaceRequest {
	return &provider.CreateStorageSpaceRequest{
		Type: "personal",
		Name: name,
	}
}

func createProjectReqWithSpaceID(name, opaqueKey, hint string) *provider.CreateStorageSpaceRequest {
	return &provider.CreateStorageSpaceRequest{
		Type: "project",
		Name: name,
		Opaque: &types.Opaque{
			Map: map[string]*types.OpaqueEntry{
				opaqueKey: {Decoder: "plain", Value: []byte(hint)},
			},
		},
	}
}

func TestCreateStorageSpace_PersonalIsDeterministic(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	ctx := userCtxWithDisplayName("alice", "Alice")

	first, err := d.CreateStorageSpace(ctx, createPersonalReq("Alice"))
	if err != nil {
		t.Fatalf("first CreateStorageSpace failed: %v", err)
	}
	second, err := d.CreateStorageSpace(ctx, createPersonalReq("Alice"))
	if err != nil {
		t.Fatalf("second CreateStorageSpace failed: %v", err)
	}
	if first.StorageSpace.Id.OpaqueId != second.StorageSpace.Id.OpaqueId {
		t.Errorf("expected identical space IDs across calls, got %s vs %s",
			first.StorageSpace.Id.OpaqueId, second.StorageSpace.Id.OpaqueId)
	}
	personalCount := 0
	for _, s := range store.spaces {
		if s.Type == "personal" && s.Owner == "alice" {
			personalCount++
		}
	}
	if personalCount != 1 {
		t.Errorf("expected exactly 1 personal space for alice in store, got %d", personalCount)
	}
}

func TestCreateStorageSpace_PersonalIDEqualsOwner(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	ctx := userCtxWithDisplayName("alice-uuid-1234", "Alice")

	resp, err := d.CreateStorageSpace(ctx, createPersonalReq("Alice"))
	if err != nil {
		t.Fatalf("CreateStorageSpace failed: %v", err)
	}
	if resp.StorageSpace.Id.OpaqueId != "alice-uuid-1234" {
		t.Errorf("personal space ID should equal owner ID, got %s", resp.StorageSpace.Id.OpaqueId)
	}
}

func TestCreateStorageSpace_PersonalRaceProducesOne(t *testing.T) {
	// 20 goroutines all racing to create alice's personal space. After
	// the race, exactly one personal space exists in the store, every
	// goroutine got the same ID back, and none returned an error.
	// Reduces the parallel sign-in race that historically produced
	// duplicate personal spaces to a unit-test fixture.
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	ctx := userCtxWithDisplayName("alice", "Alice")

	const N = 20
	var (
		wg       sync.WaitGroup
		errCount atomic.Int32
		ids      sync.Map
	)
	wg.Add(N)
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			<-start
			resp, err := d.CreateStorageSpace(ctx, createPersonalReq("Alice"))
			if err != nil {
				errCount.Add(1)
				return
			}
			ids.Store(resp.StorageSpace.Id.OpaqueId, struct{}{})
		}()
	}
	close(start)
	wg.Wait()

	if got := errCount.Load(); got != 0 {
		t.Errorf("expected 0 errors across %d racing creates, got %d", N, got)
	}
	idCount := 0
	var seenID string
	ids.Range(func(k, _ any) bool {
		idCount++
		seenID = k.(string)
		return true
	})
	if idCount != 1 {
		t.Errorf("expected all %d goroutines to see the same space ID, saw %d distinct", N, idCount)
	}
	if seenID != "alice" {
		t.Errorf("expected deterministic ID 'alice', got %q", seenID)
	}
	personalCount := 0
	for _, s := range store.spaces {
		if s.Type == "personal" && s.Owner == "alice" {
			personalCount++
		}
	}
	if personalCount != 1 {
		t.Errorf("expected exactly 1 personal space in store after race, got %d", personalCount)
	}
}

func TestCreateStorageSpace_AcceptsSpaceIdOpaqueKey(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	ctx := userCtxWithDisplayName("alice", "Alice")

	// "space_id" — the gateway / CreateHome convention.
	resp, err := d.CreateStorageSpace(ctx, createProjectReqWithSpaceID("Proj", "space_id", "proj-deadbeef"))
	if err != nil {
		t.Fatalf("CreateStorageSpace failed: %v", err)
	}
	if resp.StorageSpace.Id.OpaqueId != "proj-deadbeef" {
		t.Errorf("expected space_id hint to be honoured, got %s", resp.StorageSpace.Id.OpaqueId)
	}
}

func TestCreateStorageSpace_AcceptsSpaceidOpaqueKey(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	ctx := userCtxWithDisplayName("alice", "Alice")

	// "spaceid" — the legacy / decomposedfs convention.
	resp, err := d.CreateStorageSpace(ctx, createProjectReqWithSpaceID("Proj", "spaceid", "proj-cafef00d"))
	if err != nil {
		t.Fatalf("CreateStorageSpace failed: %v", err)
	}
	if resp.StorageSpace.Id.OpaqueId != "proj-cafef00d" {
		t.Errorf("expected spaceid hint to be honoured, got %s", resp.StorageSpace.Id.OpaqueId)
	}
}

func TestCreateStorageSpace_PersonalIgnoresOpaqueIdHint(t *testing.T) {
	// Pin the rule: personal-type ALWAYS uses owner-derived ID. A future
	// caller passing an opaque hint must not be able to reopen the race.
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	ctx := userCtxWithDisplayName("alice", "Alice")

	req := createPersonalReq("Alice")
	req.Opaque = &types.Opaque{
		Map: map[string]*types.OpaqueEntry{
			"space_id": {Decoder: "plain", Value: []byte("not-alices-id")},
		},
	}
	resp, err := d.CreateStorageSpace(ctx, req)
	if err != nil {
		t.Fatalf("CreateStorageSpace failed: %v", err)
	}
	if resp.StorageSpace.Id.OpaqueId != "alice" {
		t.Errorf("personal space ID must equal owner ID regardless of opaque hint, got %s", resp.StorageSpace.Id.OpaqueId)
	}
}
