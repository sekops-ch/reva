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
	"fmt"
	"testing"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
)

// buildDirChain creates a space with a chain of nested directories
// c1/c2/.../cn under the root (n dirs total; the subtree below c1 is n-1
// levels deep, the subtree below the space root is n levels deep).
// Returns the top dir ID ("c1").
func buildDirChain(store *mockMetadataStore, spaceID, rootID string, n int) string {
	setupSpaceEmpty(store, spaceID, rootID)
	parentID := rootID
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("c%d", i)
		store.nodes[spaceID+"."+id] = &NodeEntry{
			ID: id, SpaceID: spaceID, ParentID: parentID,
			Name: id, Type: NodeTypeDir,
			Owner: "test-user-id", MimeType: "httpd/unix-directory",
		}
		store.nodeRevs[spaceID+"."+id] = 1
		store.children[spaceID+"."+parentID] = ChildMap{id: id}
		store.childRevs[spaceID+"."+parentID] = 1
		parentID = id
	}
	store.children[spaceID+"."+parentID] = ChildMap{}
	store.childRevs[spaceID+"."+parentID] = 1
	return "c1"
}

// depthDriver returns a test driver with an explicit MaxDeleteDepth so the
// depth tests don't need to build 100-deep chains.
func depthDriver(store *mockMetadataStore, blob *mockBlobStore, limit int) *kvfsDriver {
	d := testDriver(store, blob)
	d.opts = &Options{MaxDeleteDepth: limit}
	return d
}

// trashChainTop deletes the chain's top dir into the trash and returns the
// trash key.
func trashChainTop(t *testing.T, d *kvfsDriver, store *mockMetadataStore, spaceID, topID string) string {
	t.Helper()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: spaceID, OpaqueId: topID},
	}
	if err := d.Delete(testContext(), ref); err != nil {
		t.Fatalf("Delete(%s) failed: %v", topID, err)
	}
	trashItems, _ := store.ListTrash(spaceID)
	if len(trashItems) != 1 {
		t.Fatalf("expected 1 trash entry, got %d", len(trashItems))
	}
	return trashItems[0].Key
}

// A chain exactly at the bound must purge cleanly (boundary case: nesting
// below the delete root == MaxDeleteDepth).
func TestPurgeRecycleItem_DepthWithinLimit(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := depthDriver(store, blob, 5)

	buildDirChain(store, "s1", "root", 6) // 5 levels below c1
	key := trashChainTop(t, d, store, "s1", "c1")

	spaceRef := &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1"}}
	if err := d.PurgeRecycleItem(testContext(), spaceRef, key, ""); err != nil {
		t.Fatalf("PurgeRecycleItem at the bound must succeed, got: %v", err)
	}

	for i := 1; i <= 6; i++ {
		id := fmt.Sprintf("c%d", i)
		if _, _, err := store.GetNode("s1", id); err != ErrNodeNotFound {
			t.Errorf("node %s should be deleted after purge", id)
		}
	}
	if trashItems, _ := store.ListTrash("s1"); len(trashItems) != 0 {
		t.Errorf("expected empty trash after purge, got %d entries", len(trashItems))
	}
}

// One level past the bound must fail with a BadRequest and leave the item
// intact (connected tree, still restorable) — never a panic, never a
// half-orphaned subtree.
func TestPurgeRecycleItem_DepthExceeded(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := depthDriver(store, blob, 5)

	buildDirChain(store, "s1", "root", 7) // 6 levels below c1
	key := trashChainTop(t, d, store, "s1", "c1")

	spaceRef := &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1"}}
	err := d.PurgeRecycleItem(testContext(), spaceRef, key, "")
	if err == nil {
		t.Fatal("expected a depth-bound error, got nil")
	}
	if _, ok := err.(errtypes.BadRequest); !ok {
		t.Fatalf("expected errtypes.BadRequest, got %T: %v", err, err)
	}

	// The trash root and the deepest node must both still exist: the abort
	// happened before the root was touched, and deletion is bottom-up.
	if _, _, err := store.GetNode("s1", "c1"); err != nil {
		t.Error("trash root c1 must survive an aborted purge")
	}
	if _, _, err := store.GetNode("s1", "c7"); err != nil {
		t.Error("deepest node c7 must survive an aborted purge")
	}
	trashItems, _ := store.ListTrash("s1")
	if len(trashItems) != 1 {
		t.Fatalf("trash entry must survive an aborted purge, got %d entries", len(trashItems))
	}

	// And the item is still restorable.
	if err := d.RestoreRecycleItem(testContext(), spaceRef, key, "", nil); err != nil {
		t.Fatalf("RestoreRecycleItem after aborted purge failed: %v", err)
	}
	rootChildren, _, _ := store.GetChildren("s1", "root")
	if _, ok := rootChildren["c1"]; !ok {
		t.Error("restored chain top should be back in the root's children")
	}
}

// EmptyRecycle aborts with the same BadRequest when it hits an over-deep
// item; the over-deep item stays in the trash.
func TestEmptyRecycle_DepthExceededAborts(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := depthDriver(store, blob, 5)

	buildDirChain(store, "s1", "root", 7)
	trashChainTop(t, d, store, "s1", "c1")

	spaceRef := &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1"}}
	err := d.EmptyRecycle(testContext(), spaceRef)
	if err == nil {
		t.Fatal("expected a depth-bound error, got nil")
	}
	if _, ok := err.(errtypes.BadRequest); !ok {
		t.Fatalf("expected errtypes.BadRequest, got %T: %v", err, err)
	}
	if _, _, err := store.GetNode("s1", "c1"); err != nil {
		t.Error("over-deep item must survive an aborted EmptyRecycle")
	}
	if trashItems, _ := store.ListTrash("s1"); len(trashItems) != 1 {
		t.Errorf("over-deep trash entry must survive, got %d entries", len(trashItems))
	}
}

// DeleteStorageSpace propagates the bound too (nesting is counted below the
// space root there) and leaves the space entry in place on abort.
func TestDeleteStorageSpace_DepthGuard(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := depthDriver(store, blob, 5)

	buildDirChain(store, "s-deep", "root", 6) // 6 levels below the space root
	err := d.DeleteStorageSpace(testContext(), &provider.DeleteStorageSpaceRequest{
		Id: &provider.StorageSpaceId{OpaqueId: "s-deep"},
	})
	if err == nil {
		t.Fatal("expected a depth-bound error, got nil")
	}
	if _, ok := err.(errtypes.BadRequest); !ok {
		t.Fatalf("expected errtypes.BadRequest, got %T: %v", err, err)
	}
	if _, _, err := store.GetSpace("s-deep"); err != nil {
		t.Error("space entry must survive an aborted DeleteStorageSpace")
	}

	store2 := newMockMetadataStore()
	d2 := depthDriver(store2, newMockBlobStore(), 5)
	buildDirChain(store2, "s-ok", "root", 5) // exactly at the bound
	if err := d2.DeleteStorageSpace(testContext(), &provider.DeleteStorageSpaceRequest{
		Id: &provider.StorageSpaceId{OpaqueId: "s-ok"},
	}); err != nil {
		t.Fatalf("DeleteStorageSpace at the bound must succeed, got: %v", err)
	}
	if _, _, err := store2.GetSpace("s-ok"); err == nil {
		t.Error("space entry should be gone after successful DeleteStorageSpace")
	}
}

// With no configured bound the package default (100) applies — proven at
// the exact boundary with a real 101-deep purge.
func TestPurgeRecycleItem_DefaultDepthBoundary(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob) // opts = &Options{} → resolveMaxDeleteDepth → 100

	buildDirChain(store, "s1", "root", 101) // 100 levels below c1
	key := trashChainTop(t, d, store, "s1", "c1")
	spaceRef := &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1"}}
	if err := d.PurgeRecycleItem(testContext(), spaceRef, key, ""); err != nil {
		t.Fatalf("purge at the default bound must succeed, got: %v", err)
	}

	store2 := newMockMetadataStore()
	d2 := testDriver(store2, newMockBlobStore())
	buildDirChain(store2, "s2", "root", 102) // 101 levels below c1
	key2 := trashChainTop(t, d2, store2, "s2", "c1")
	spaceRef2 := &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s2"}}
	err := d2.PurgeRecycleItem(testContext(), spaceRef2, key2, "")
	if err == nil {
		t.Fatal("expected the default bound to reject a 101-deep purge")
	}
	if _, ok := err.(errtypes.BadRequest); !ok {
		t.Fatalf("expected errtypes.BadRequest, got %T: %v", err, err)
	}
}
