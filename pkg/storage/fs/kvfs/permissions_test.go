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
	"io"
	"strings"
	"testing"

	user "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	ctxpkg "github.com/opencloud-eu/reva/v2/pkg/ctx"
	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
	"github.com/opencloud-eu/reva/v2/pkg/storage"
)

func testContextWithUser(userID string, groups []string) context.Context {
	return ctxpkg.ContextSetUser(context.Background(), &user.User{
		Id:     &user.UserId{OpaqueId: userID},
		Groups: groups,
	})
}

func testContextServiceAccount() context.Context {
	return ctxpkg.ContextSetUser(context.Background(), &user.User{
		Id: &user.UserId{OpaqueId: "service-account", Type: user.UserType_USER_TYPE_SERVICE},
	})
}

// --- assemblePermissions ---

func TestAssemblePermissions_OwnerGetsAll(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextWithUser("test-user-id", nil)
	node, _, _ := store.GetNode("s1", "f1")

	rp := d.assemblePermissions(ctx, "s1", node)
	if !rp.Stat || !rp.InitiateFileDownload || !rp.InitiateFileUpload || !rp.Delete {
		t.Error("owner should have all permissions")
	}
}

func TestAssemblePermissions_ServiceAccountGetsAll(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextServiceAccount()
	node, _, _ := store.GetNode("s1", "f1")

	rp := d.assemblePermissions(ctx, "s1", node)
	if !rp.Stat || !rp.InitiateFileDownload || !rp.Delete || !rp.PurgeRecycle {
		t.Error("service account should have all permissions")
	}
}

func TestAssemblePermissions_NoGrantGetsNone(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextWithUser("other-user", nil)
	node, _, _ := store.GetNode("s1", "f1")

	rp := d.assemblePermissions(ctx, "s1", node)
	if rp.Stat || rp.InitiateFileDownload || rp.InitiateFileUpload || rp.Delete {
		t.Error("non-member should have no permissions")
	}
}

func TestAssemblePermissions_NoUserGetsNone(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	node, _, _ := store.GetNode("s1", "f1")

	rp := d.assemblePermissions(context.Background(), "s1", node)
	if rp.Stat || rp.InitiateFileDownload {
		t.Error("unauthenticated context should have no permissions")
	}
}

func TestAssemblePermissions_UserGrantOnRoot(t *testing.T) {
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
	node, _, _ := store.GetNode("s1", "f1")
	rp := d.assemblePermissions(ctx, "s1", node)

	if !rp.Stat || !rp.InitiateFileDownload {
		t.Error("bob should have read permissions from root grant")
	}
	if rp.InitiateFileUpload || rp.Delete {
		t.Error("bob should NOT have write/delete permissions")
	}
}

func TestAssemblePermissions_GroupGrant(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	rootNode := store.nodes["s1.root"]
	rootNode.Grants = map[string]*GrantEntry{
		"g:editors": {GranteeType: "group", GranteeID: "editors", Permissions: permissionsToUint32(&provider.ResourcePermissions{
			Stat: true, InitiateFileDownload: true, InitiateFileUpload: true,
			ListContainer: true, CreateContainer: true,
		})},
	}

	ctx := testContextWithUser("charlie", []string{"editors", "viewers"})
	node, _, _ := store.GetNode("s1", "f1")
	rp := d.assemblePermissions(ctx, "s1", node)

	if !rp.Stat || !rp.InitiateFileUpload {
		t.Error("charlie (member of editors) should have read+write permissions")
	}
	if rp.Delete {
		t.Error("charlie should NOT have delete permission")
	}
}

func TestAssemblePermissions_InheritedGrant(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")

	subID := "sub-dir"
	fileID := "child-file"
	store.nodes["s1."+subID] = &NodeEntry{
		ID: subID, SpaceID: "s1", ParentID: "root",
		Name: "subdir", Type: NodeTypeDir, Owner: "test-user-id",
	}
	store.nodeRevs["s1."+subID] = 1
	store.nodes["s1."+fileID] = &NodeEntry{
		ID: fileID, SpaceID: "s1", ParentID: subID,
		Name: "deep.txt", Type: NodeTypeFile, Owner: "test-user-id",
	}
	store.nodeRevs["s1."+fileID] = 1

	rootNode := store.nodes["s1.root"]
	rootNode.Grants = map[string]*GrantEntry{
		"u:alice": {GranteeType: "user", GranteeID: "alice", Permissions: permissionsToUint32(&provider.ResourcePermissions{
			Stat: true, InitiateFileDownload: true, ListContainer: true,
		})},
	}

	ctx := testContextWithUser("alice", nil)
	node, _, _ := store.GetNode("s1", fileID)
	rp := d.assemblePermissions(ctx, "s1", node)

	if !rp.Stat || !rp.InitiateFileDownload {
		t.Error("alice should inherit read permissions from root grant")
	}
}

// --- Operation-level permission gates ---

func TestGetMD_NonMemberReturnsNotFound(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "secret.txt")

	ctx := testContextWithUser("stranger", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}

	_, err := d.GetMD(ctx, ref, nil, nil)
	if err == nil {
		t.Fatal("expected error for non-member")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

func TestDownload_NonMemberDenied(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	setupSpaceWithFile(store, "s1", "root", "f1", "secret.txt")
	blob.blobs[BlobKey("s1", "old-blob")] = []byte("data")

	ctx := testContextWithUser("stranger", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}

	_, _, err := d.Download(ctx, ref, nil)
	if err == nil {
		t.Fatal("expected error for non-member")
	}
}

func TestUpload_ReadOnlyGrantDenied(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	setupSpaceWithFile(store, "s1", "root", "f1", "readonly.txt")

	rootNode := store.nodes["s1.root"]
	rootNode.Grants = map[string]*GrantEntry{
		"u:bob": {GranteeType: "user", GranteeID: "bob", Permissions: permissionsToUint32(&provider.ResourcePermissions{
			Stat: true, InitiateFileDownload: true, ListContainer: true,
		})},
	}

	ctx := testContextWithUser("bob", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./readonly.txt",
	}

	_, err := d.Upload(ctx, storage.UploadRequest{
		Ref:    ref,
		Body:   io.NopCloser(strings.NewReader("hack")),
		Length: 4,
	}, nil)
	if err == nil {
		t.Fatal("expected permission denied for read-only grant")
	}
}

func TestDelete_NonMemberDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "secret.txt")

	ctx := testContextWithUser("stranger", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}

	err := d.Delete(ctx, ref)
	if err == nil {
		t.Fatal("expected error for non-member delete")
	}
}

func TestPurgeRecycleItem_NonMemberDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")
	store.trash["s1.tk1"] = &TrashEntry{
		Key: "tk1", NodeID: "n1", SpaceID: "s1",
		Node: NodeEntry{Type: NodeTypeFile, BlobID: "b1"},
	}

	ctx := testContextWithUser("stranger", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1"},
	}

	err := d.PurgeRecycleItem(ctx, ref, "tk1", "")
	if err == nil {
		t.Fatal("expected permission denied for non-member purge")
	}
	if _, ok := err.(errtypes.IsPermissionDenied); !ok {
		t.Errorf("expected PermissionDenied, got %T: %v", err, err)
	}
}

func TestListStorageSpaces_FiltersNonMember(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "owned", "root-owned")
	setupSpaceEmpty(store, "other", "root-other")

	store.spaces["other"].Owner = "someone-else"
	store.nodes["other.root-other"].Owner = "someone-else"

	ctx := testContextWithUser("test-user-id", nil)
	spaces, err := d.ListStorageSpaces(ctx, nil, false)
	if err != nil {
		t.Fatalf("ListStorageSpaces failed: %v", err)
	}

	for _, s := range spaces {
		if s.Id.OpaqueId == "other" {
			t.Error("non-member should not see 'other' space")
		}
	}
}

func TestListStorageSpaces_IncludesGrantedSpace(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "project", "root-proj")

	store.spaces["project"].Owner = "someone-else"
	store.spaces["project"].Type = "project"
	store.nodes["project.root-proj"].Owner = "someone-else"
	store.nodes["project.root-proj"].Grants = map[string]*GrantEntry{
		"u:test-user-id": {GranteeType: "user", GranteeID: "test-user-id", Permissions: permissionsToUint32(&provider.ResourcePermissions{
			Stat: true, InitiateFileDownload: true, ListContainer: true,
		})},
	}

	ctx := testContextWithUser("test-user-id", nil)
	spaces, err := d.ListStorageSpaces(ctx, nil, false)
	if err != nil {
		t.Fatalf("ListStorageSpaces failed: %v", err)
	}

	found := false
	for _, s := range spaces {
		if s.Id.OpaqueId == "project" {
			found = true
		}
	}
	if !found {
		t.Error("user with grant should see 'project' space")
	}
}

// --- Trash idempotency (F9) ---

func TestPurgeRecycleItem_MissingReturns404(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1"},
	}

	err := d.PurgeRecycleItem(ctx, ref, "nonexistent-key", "")
	if err == nil {
		t.Fatal("expected error for missing trash item")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

func TestRestoreRecycleItem_MissingReturns404(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1"},
	}

	err := d.RestoreRecycleItem(ctx, ref, "nonexistent-key", "", nil)
	if err == nil {
		t.Fatal("expected error for missing trash item")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

func TestPurgeRecycleItem_DoublePurge(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")
	store.trash["s1.tk1"] = &TrashEntry{
		Key: "tk1", NodeID: "n1", SpaceID: "s1",
		Node: NodeEntry{Type: NodeTypeFile},
	}

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1"},
	}

	err := d.PurgeRecycleItem(ctx, ref, "tk1", "")
	if err != nil {
		t.Fatalf("first purge should succeed: %v", err)
	}

	err = d.PurgeRecycleItem(ctx, ref, "tk1", "")
	if err == nil {
		t.Fatal("second purge should fail")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound on double purge, got %T: %v", err, err)
	}
}

// --- AddGrant + assemblePermissions end-to-end ---

func TestAddGrant_StoresGrantOnNode(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "shared.txt")

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	grant := &provider.Grant{
		Grantee: &provider.Grantee{
			Type: provider.GranteeType_GRANTEE_TYPE_USER,
			Id:   &provider.Grantee_UserId{UserId: &user.UserId{OpaqueId: "bob"}},
		},
		Permissions: &provider.ResourcePermissions{
			Stat: true, InitiateFileDownload: true,
		},
	}

	err := d.AddGrant(ctx, ref, grant)
	if err != nil {
		t.Fatalf("AddGrant failed: %v", err)
	}

	node, _, err := store.GetNode("s1", "f1")
	if err != nil {
		t.Fatalf("GetNode failed: %v", err)
	}
	ge, ok := node.Grants["u:bob"]
	if !ok {
		t.Fatal("grant not found on node after AddGrant")
	}
	if ge.GranteeID != "bob" {
		t.Errorf("expected granteeID 'bob', got '%s'", ge.GranteeID)
	}
	p := uint32ToPermissions(ge.Permissions)
	if !p.Stat || !p.InitiateFileDownload {
		t.Error("stored grant should have Stat and InitiateFileDownload")
	}
	if p.Delete || p.InitiateFileUpload {
		t.Error("stored grant should NOT have Delete or InitiateFileUpload")
	}
}

func TestAssemblePermissions_FindsAddedGrant(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "shared.txt")

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	grant := &provider.Grant{
		Grantee: &provider.Grantee{
			Type: provider.GranteeType_GRANTEE_TYPE_USER,
			Id:   &provider.Grantee_UserId{UserId: &user.UserId{OpaqueId: "bob"}},
		},
		Permissions: &provider.ResourcePermissions{
			Stat: true, InitiateFileDownload: true, ListContainer: true,
		},
	}

	if err := d.AddGrant(ctx, ref, grant); err != nil {
		t.Fatalf("AddGrant failed: %v", err)
	}

	bobCtx := testContextWithUser("bob", nil)
	node, _, _ := store.GetNode("s1", "f1")
	rp := d.assemblePermissions(bobCtx, "s1", node)

	if !rp.Stat || !rp.InitiateFileDownload {
		t.Error("bob should have read permissions after AddGrant")
	}
	if rp.Delete || rp.InitiateFileUpload {
		t.Error("bob should NOT have write/delete permissions")
	}
}

func TestAssemblePermissions_FindsGrantOnSpaceRoot(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	store.spaces["s1"].Owner = "alice"
	store.nodes["s1.root"].Owner = "alice"

	aliceCtx := testContextWithUser("alice", nil)
	rootRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
	}
	grant := &provider.Grant{
		Grantee: &provider.Grantee{
			Type: provider.GranteeType_GRANTEE_TYPE_USER,
			Id:   &provider.Grantee_UserId{UserId: &user.UserId{OpaqueId: "bob"}},
		},
		Permissions: &provider.ResourcePermissions{
			Stat: true, InitiateFileDownload: true, InitiateFileUpload: true,
			ListContainer: true, CreateContainer: true, Delete: true,
		},
	}

	if err := d.AddGrant(aliceCtx, rootRef, grant); err != nil {
		t.Fatalf("AddGrant on root failed: %v", err)
	}

	bobCtx := testContextWithUser("bob", nil)
	childNode, _, _ := store.GetNode("s1", "f1")
	rp := d.assemblePermissions(bobCtx, "s1", childNode)

	if !rp.Stat || !rp.InitiateFileUpload || !rp.Delete {
		t.Error("bob should inherit all granted permissions from space root")
	}
}

// --- modifyGrant CAS retry ---

func TestModifyGrant_CASRetry(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	store.nodes["s1.f1"].Grants = map[string]*GrantEntry{
		"u:existing": {GranteeType: "user", GranteeID: "existing", Permissions: 1},
	}

	grant := &provider.Grant{
		Grantee: &provider.Grantee{
			Type: provider.GranteeType_GRANTEE_TYPE_USER,
			Id:   &provider.Grantee_UserId{UserId: &user.UserId{OpaqueId: "bob"}},
		},
		Permissions: &provider.ResourcePermissions{Stat: true},
	}

	err := d.modifyGrant("s1", "f1", grant, "add")
	if err != nil {
		t.Fatalf("modifyGrant should succeed: %v", err)
	}

	node, _, _ := store.GetNode("s1", "f1")
	if _, ok := node.Grants["u:bob"]; !ok {
		t.Error("grant should exist after modifyGrant")
	}
	if _, ok := node.Grants["u:existing"]; !ok {
		t.Error("existing grant should be preserved")
	}
}

// --- spaceToCS3 Opaque grants ---

func TestSpaceToCS3_OpaqueGrants(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "proj", "root-proj")

	store.spaces["proj"].Type = "project"
	store.spaces["proj"].Owner = "alice"
	store.nodes["proj.root-proj"].Owner = "alice"
	store.nodes["proj.root-proj"].Grants = map[string]*GrantEntry{
		"u:bob": {GranteeType: "user", GranteeID: "bob", Permissions: permissionsToUint32(&provider.ResourcePermissions{
			Stat: true, InitiateFileDownload: true, InitiateFileUpload: true,
		})},
		"g:devs": {GranteeType: "group", GranteeID: "devs", Permissions: permissionsToUint32(&provider.ResourcePermissions{
			Stat: true, ListContainer: true,
		})},
	}

	rootNode, _, _ := store.GetNode("proj", "root-proj")
	ss := d.spaceToCS3(store.spaces["proj"], rootNode)

	if ss.Opaque == nil {
		t.Fatal("Opaque should not be nil")
	}
	grantsEntry, ok := ss.Opaque.Map["grants"]
	if !ok {
		t.Fatal("Opaque should contain 'grants' key")
	}
	if grantsEntry.Decoder != "json" {
		t.Errorf("grants decoder should be 'json', got '%s'", grantsEntry.Decoder)
	}

	groupsEntry, ok := ss.Opaque.Map["groups"]
	if !ok {
		t.Fatal("Opaque should contain 'groups' key for group grants")
	}
	if groupsEntry.Decoder != "json" {
		t.Errorf("groups decoder should be 'json', got '%s'", groupsEntry.Decoder)
	}
}

// --- ListStorageSpaces +grant filter ---

func TestListStorageSpaces_GrantFilter(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "alice-space", "root-alice")

	store.spaces["alice-space"].Owner = "alice"
	store.spaces["alice-space"].Type = "personal"
	store.nodes["alice-space.root-alice"].Owner = "alice"

	ctx := testContextWithUser("bob", nil)

	filters := []*provider.ListStorageSpacesRequest_Filter{
		{
			Type: provider.ListStorageSpacesRequest_Filter_TYPE_ID,
			Term: &provider.ListStorageSpacesRequest_Filter_Id{
				Id: &provider.StorageSpaceId{OpaqueId: "alice-space"},
			},
		},
		{
			Type: provider.ListStorageSpacesRequest_Filter_TYPE_SPACE_TYPE,
			Term: &provider.ListStorageSpacesRequest_Filter_SpaceType{
				SpaceType: "+grant",
			},
		},
	}

	spaces, err := d.ListStorageSpaces(ctx, filters, false)
	if err != nil {
		t.Fatalf("ListStorageSpaces failed: %v", err)
	}

	// Without grant, bob should not see the space (filtered by access check)
	if len(spaces) != 0 {
		t.Error("bob should not see alice-space without a grant")
	}

	// Add a grant for bob
	store.nodes["alice-space.root-alice"].Grants = map[string]*GrantEntry{
		"u:bob": {GranteeType: "user", GranteeID: "bob", Permissions: permissionsToUint32(&provider.ResourcePermissions{
			Stat: true,
		})},
	}

	spaces, err = d.ListStorageSpaces(ctx, filters, false)
	if err != nil {
		t.Fatalf("ListStorageSpaces failed: %v", err)
	}
	if len(spaces) != 1 {
		t.Errorf("bob should see alice-space with grant, got %d spaces", len(spaces))
	}
}

// --- ListRecycle key filtering (F10) ---

func TestListRecycle_SpecificKeyReturnsItem(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")
	store.trash["s1.tk1"] = &TrashEntry{
		Key: "tk1", NodeID: "n1", SpaceID: "s1",
		Node: NodeEntry{Type: NodeTypeFile, Size: 42},
	}
	store.trash["s1.tk2"] = &TrashEntry{
		Key: "tk2", NodeID: "n2", SpaceID: "s1",
		Node: NodeEntry{Type: NodeTypeFile, Size: 10},
	}

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1"},
	}

	items, err := d.ListRecycle(ctx, ref, "tk1", "")
	if err != nil {
		t.Fatalf("ListRecycle with key should succeed: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if items[0].Key != "tk1" {
		t.Errorf("expected key 'tk1', got '%s'", items[0].Key)
	}
}

func TestListRecycle_MissingKeyReturns404(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1"},
	}

	_, err := d.ListRecycle(ctx, ref, "nonexistent", "")
	if err == nil {
		t.Fatal("expected error for missing trash key")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

func TestListRecycle_EmptyKeyListsAll(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")
	store.trash["s1.tk1"] = &TrashEntry{
		Key: "tk1", NodeID: "n1", SpaceID: "s1",
		Node: NodeEntry{Type: NodeTypeFile},
	}
	store.trash["s1.tk2"] = &TrashEntry{
		Key: "tk2", NodeID: "n2", SpaceID: "s1",
		Node: NodeEntry{Type: NodeTypeFile},
	}

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1"},
	}

	items, err := d.ListRecycle(ctx, ref, "", "")
	if err != nil {
		t.Fatalf("ListRecycle with empty key should succeed: %v", err)
	}
	if len(items) != 2 {
		t.Errorf("expected 2 items, got %d", len(items))
	}
}

func TestListRecycle_PurgedKeyReturns404(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")
	store.trash["s1.tk1"] = &TrashEntry{
		Key: "tk1", NodeID: "n1", SpaceID: "s1",
		Node: NodeEntry{Type: NodeTypeFile},
	}

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1"},
	}

	// Purge the item
	err := d.PurgeRecycleItem(ctx, ref, "tk1", "")
	if err != nil {
		t.Fatalf("PurgeRecycleItem should succeed: %v", err)
	}

	// Now ListRecycle with the same key should return 404
	_, err = d.ListRecycle(ctx, ref, "tk1", "")
	if err == nil {
		t.Fatal("expected NotFound after purge")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

// --- Authorization checks on grant operations ---

func TestAddGrant_NonMemberDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextWithUser("stranger", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	grant := &provider.Grant{
		Grantee: &provider.Grantee{
			Type: provider.GranteeType_GRANTEE_TYPE_USER,
			Id:   &provider.Grantee_UserId{UserId: &user.UserId{OpaqueId: "evil"}},
		},
		Permissions: &provider.ResourcePermissions{Stat: true},
	}

	err := d.AddGrant(ctx, ref, grant)
	if err == nil {
		t.Fatal("expected error for non-member")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

func TestRemoveGrant_NonMemberDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextWithUser("stranger", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	grant := &provider.Grant{
		Grantee: &provider.Grantee{
			Type: provider.GranteeType_GRANTEE_TYPE_USER,
			Id:   &provider.Grantee_UserId{UserId: &user.UserId{OpaqueId: "someone"}},
		},
	}

	err := d.RemoveGrant(ctx, ref, grant)
	if err == nil {
		t.Fatal("expected error for non-member")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

func TestUpdateGrant_NonMemberDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextWithUser("stranger", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	grant := &provider.Grant{
		Grantee: &provider.Grantee{
			Type: provider.GranteeType_GRANTEE_TYPE_USER,
			Id:   &provider.Grantee_UserId{UserId: &user.UserId{OpaqueId: "someone"}},
		},
		Permissions: &provider.ResourcePermissions{Stat: true},
	}

	err := d.UpdateGrant(ctx, ref, grant)
	if err == nil {
		t.Fatal("expected error for non-member")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

func TestListGrants_NonMemberDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextWithUser("stranger", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}

	_, err := d.ListGrants(ctx, ref)
	if err == nil {
		t.Fatal("expected error for non-member")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

func TestAddGrant_ReadOnlyGrantDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	store.nodes["s1.root"].Grants = map[string]*GrantEntry{
		"u:bob": {GranteeType: "user", GranteeID: "bob", Permissions: permissionsToUint32(&provider.ResourcePermissions{
			Stat: true, InitiateFileDownload: true, ListContainer: true,
		})},
	}

	ctx := testContextWithUser("bob", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	grant := &provider.Grant{
		Grantee: &provider.Grantee{
			Type: provider.GranteeType_GRANTEE_TYPE_USER,
			Id:   &provider.Grantee_UserId{UserId: &user.UserId{OpaqueId: "evil"}},
		},
		Permissions: &provider.ResourcePermissions{Stat: true},
	}

	err := d.AddGrant(ctx, ref, grant)
	if err == nil {
		t.Fatal("expected error for read-only grant")
	}
	if _, ok := err.(errtypes.IsPermissionDenied); !ok {
		t.Errorf("expected PermissionDenied (user has Stat), got %T: %v", err, err)
	}
}

// --- Authorization checks on revision operations ---

func TestListRevisions_NonMemberDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextWithUser("stranger", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}

	_, err := d.ListRevisions(ctx, ref)
	if err == nil {
		t.Fatal("expected error for non-member")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

func TestDownloadRevision_NonMemberDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextWithUser("stranger", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}

	_, _, err := d.DownloadRevision(ctx, ref, "v1", nil)
	if err == nil {
		t.Fatal("expected error for non-member")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

func TestDownloadRevision_ReadOnlyNoVersionPermDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	store.nodes["s1.root"].Grants = map[string]*GrantEntry{
		"u:bob": {GranteeType: "user", GranteeID: "bob", Permissions: permissionsToUint32(&provider.ResourcePermissions{
			Stat: true, InitiateFileDownload: true, ListContainer: true,
		})},
	}

	ctx := testContextWithUser("bob", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}

	_, _, err := d.DownloadRevision(ctx, ref, "v1", nil)
	if err == nil {
		t.Fatal("expected error for user without ListFileVersions")
	}
	if _, ok := err.(errtypes.IsPermissionDenied); !ok {
		t.Errorf("expected PermissionDenied (has Stat but not ListFileVersions), got %T: %v", err, err)
	}
}

func TestRestoreRevision_NonMemberDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextWithUser("stranger", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}

	err := d.RestoreRevision(ctx, ref, "v1")
	if err == nil {
		t.Fatal("expected error for non-member")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

// --- Authorization checks on metadata operations ---

func TestSetArbitraryMetadata_NonMemberDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextWithUser("stranger", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}

	err := d.SetArbitraryMetadata(ctx, ref, &provider.ArbitraryMetadata{
		Metadata: map[string]string{"key": "val"},
	})
	if err == nil {
		t.Fatal("expected error for non-member")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

func TestSetArbitraryMetadata_ReadOnlyDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	store.nodes["s1.root"].Grants = map[string]*GrantEntry{
		"u:bob": {GranteeType: "user", GranteeID: "bob", Permissions: permissionsToUint32(&provider.ResourcePermissions{
			Stat: true, InitiateFileDownload: true, ListContainer: true,
		})},
	}

	ctx := testContextWithUser("bob", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}

	err := d.SetArbitraryMetadata(ctx, ref, &provider.ArbitraryMetadata{
		Metadata: map[string]string{"key": "val"},
	})
	if err == nil {
		t.Fatal("expected error for read-only grant")
	}
	if _, ok := err.(errtypes.IsPermissionDenied); !ok {
		t.Errorf("expected PermissionDenied, got %T: %v", err, err)
	}
}

func TestUnsetArbitraryMetadata_NonMemberDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextWithUser("stranger", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}

	err := d.UnsetArbitraryMetadata(ctx, ref, []string{"key"})
	if err == nil {
		t.Fatal("expected error for non-member")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

// --- Lock check tests ---

func TestSetArbitraryMetadata_LockedDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	store.nodes["s1.f1"].Lock = &LockEntry{
		LockID: "lock-123",
		Type:   0,
		UserID: "test-user-id",
	}

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}

	err := d.SetArbitraryMetadata(ctx, ref, &provider.ArbitraryMetadata{
		Metadata: map[string]string{"key": "val"},
	})
	if err == nil {
		t.Fatal("expected error for locked resource")
	}
	if _, ok := err.(errtypes.IsLocked); !ok {
		t.Errorf("expected Locked, got %T: %v", err, err)
	}
}

func TestUnsetArbitraryMetadata_LockedDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	store.nodes["s1.f1"].Lock = &LockEntry{
		LockID: "lock-456",
		Type:   0,
		UserID: "test-user-id",
	}

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}

	err := d.UnsetArbitraryMetadata(ctx, ref, []string{"key"})
	if err == nil {
		t.Fatal("expected error for locked resource")
	}
	if _, ok := err.(errtypes.IsLocked); !ok {
		t.Errorf("expected Locked, got %T: %v", err, err)
	}
}

func TestRestoreRevision_LockedDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	store.nodes["s1.f1"].Lock = &LockEntry{
		LockID: "lock-789",
		Type:   0,
		UserID: "test-user-id",
	}

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}

	err := d.RestoreRevision(ctx, ref, "v1")
	if err == nil {
		t.Fatal("expected error for locked resource")
	}
	if _, ok := err.(errtypes.IsLocked); !ok {
		t.Errorf("expected Locked, got %T: %v", err, err)
	}
}

// --- Authorization checks on label operations ---

func TestAddLabel_NonMemberDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextWithUser("stranger", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}

	err := d.AddLabel(ctx, ref, &user.UserId{OpaqueId: "stranger"}, "favorite")
	if err == nil {
		t.Fatal("expected error for non-member")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

func TestRemoveLabel_NonMemberDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextWithUser("stranger", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}

	err := d.RemoveLabel(ctx, ref, &user.UserId{OpaqueId: "stranger"}, "favorite")
	if err == nil {
		t.Fatal("expected error for non-member")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

// --- Authorization checks on lock operations ---

func TestGetLock_NonMemberDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextWithUser("stranger", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}

	_, err := d.GetLock(ctx, ref)
	if err == nil {
		t.Fatal("expected error for non-member")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

func TestSetLock_NonMemberDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextWithUser("stranger", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	lock := &provider.Lock{LockId: "l1", Type: provider.LockType_LOCK_TYPE_EXCL}

	err := d.SetLock(ctx, ref, lock)
	if err == nil {
		t.Fatal("expected error for non-member")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

func TestRefreshLock_NonMemberDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextWithUser("stranger", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	lock := &provider.Lock{LockId: "l2"}

	err := d.RefreshLock(ctx, ref, lock, "l1")
	if err == nil {
		t.Fatal("expected error for non-member")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

func TestUnlock_NonMemberDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextWithUser("stranger", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	lock := &provider.Lock{LockId: "l1"}

	err := d.Unlock(ctx, ref, lock)
	if err == nil {
		t.Fatal("expected error for non-member")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

// --- Authorization checks on TouchFile ---

func TestTouchFile_ExistingFile_NonMemberDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContextWithUser("stranger", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}

	err := d.TouchFile(ctx, ref, false, "")
	if err == nil {
		t.Fatal("expected error for non-member")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

func TestTouchFile_NewFile_NonMemberDenied(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")

	ctx := testContextWithUser("stranger", nil)
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./newfile.txt",
	}

	err := d.TouchFile(ctx, ref, false, "")
	if err == nil {
		t.Fatal("expected error for non-member creating file via touch")
	}
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

// --- Positive regression tests (owner still works) ---

func TestGrantOps_OwnerAllowed(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}

	grant := &provider.Grant{
		Grantee: &provider.Grantee{
			Type: provider.GranteeType_GRANTEE_TYPE_USER,
			Id:   &provider.Grantee_UserId{UserId: &user.UserId{OpaqueId: "bob"}},
		},
		Permissions: &provider.ResourcePermissions{Stat: true, InitiateFileDownload: true},
	}

	if err := d.AddGrant(ctx, ref, grant); err != nil {
		t.Fatalf("owner AddGrant should succeed: %v", err)
	}

	grants, err := d.ListGrants(ctx, ref)
	if err != nil {
		t.Fatalf("owner ListGrants should succeed: %v", err)
	}
	if len(grants) != 1 {
		t.Errorf("expected 1 grant, got %d", len(grants))
	}

	if err := d.RemoveGrant(ctx, ref, grant); err != nil {
		t.Fatalf("owner RemoveGrant should succeed: %v", err)
	}
}

func TestRevisionOps_OwnerAllowed(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	store.versions = append(store.versions, &VersionEntry{
		Key: "v1", NodeID: "f1", SpaceID: "s1",
		BlobID: "version-blob-id-1234", BlobSize: 50, Size: 50,
		MTime: 500, ETag: "v1-etag",
	})
	blob.blobs[BlobKey("s1", "version-blob-id-1234")] = []byte("version data")

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}

	versions, err := d.ListRevisions(ctx, ref)
	if err != nil {
		t.Fatalf("owner ListRevisions should succeed: %v", err)
	}
	if len(versions) != 1 {
		t.Errorf("expected 1 version, got %d", len(versions))
	}

	_, reader, err := d.DownloadRevision(ctx, ref, "v1", nil)
	if err != nil {
		t.Fatalf("owner DownloadRevision should succeed: %v", err)
	}
	if reader != nil {
		reader.Close()
	}

	if err := d.RestoreRevision(ctx, ref, "v1"); err != nil {
		t.Fatalf("owner RestoreRevision should succeed: %v", err)
	}
}

func TestLockOps_OwnerAllowed(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}

	lock := &provider.Lock{LockId: "lock-1", Type: provider.LockType_LOCK_TYPE_EXCL}

	if err := d.SetLock(ctx, ref, lock); err != nil {
		t.Fatalf("owner SetLock should succeed: %v", err)
	}

	got, err := d.GetLock(ctx, ref)
	if err != nil {
		t.Fatalf("owner GetLock should succeed: %v", err)
	}
	if got == nil || got.LockId != "lock-1" {
		t.Errorf("expected lock-1, got %v", got)
	}

	newLock := &provider.Lock{LockId: "lock-2"}
	if err := d.RefreshLock(ctx, ref, newLock, "lock-1"); err != nil {
		t.Fatalf("owner RefreshLock should succeed: %v", err)
	}

	unlockRef := &provider.Lock{LockId: "lock-2"}
	if err := d.Unlock(ctx, ref, unlockRef); err != nil {
		t.Fatalf("owner Unlock should succeed: %v", err)
	}
}
