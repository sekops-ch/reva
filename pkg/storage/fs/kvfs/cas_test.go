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
	"strings"
	"testing"
	"time"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	typesv1 "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"

	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
	"github.com/opencloud-eu/reva/v2/pkg/storage"
)

func TestCreateDir_CASRetry_Succeeds(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceEmpty(store, "space-1", "root-1")

	// Inject 2 CAS failures on PutChildren — less than max 10 retries
	store.injectChildCASFailures(2)

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "root-1"},
		Path:       "./newdir",
	}

	err := d.CreateDir(ctx, ref)
	if err != nil {
		t.Fatalf("CreateDir should succeed after retries, got: %v", err)
	}

	children, _, _ := store.GetChildren("space-1", "root-1")
	if _, ok := children["newdir"]; !ok {
		t.Error("expected newdir in parent's children")
	}

	nodeID := children["newdir"]
	node, _, err := store.GetNode("space-1", nodeID)
	if err != nil {
		t.Fatalf("node not found: %v", err)
	}
	if node.Type != NodeTypeDir {
		t.Errorf("type = %v, want dir", node.Type)
	}
}

func TestCreateDir_CASExhausted_ReturnsError(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceEmpty(store, "space-1", "root-1")

	// Inject more CAS failures than max retries (10)
	store.injectChildCASFailures(20)

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "root-1"},
		Path:       "./newdir",
	}

	err := d.CreateDir(ctx, ref)
	if err == nil {
		t.Fatal("expected error after exhausting CAS retries")
	}
	if err.Error() != "kvfs: CreateDir failed after max CAS retries" {
		t.Errorf("unexpected error: %v", err)
	}

	// No directory should be left behind
	children, _, _ := store.GetChildren("space-1", "root-1")
	if len(children) != 0 {
		t.Errorf("expected empty children, got %v", children)
	}
}

func TestCreateDir_AlreadyExists_NoRetry(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceEmpty(store, "space-1", "root-1")

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "root-1"},
		Path:       "./existingdir",
	}

	// Create the directory first
	err := d.CreateDir(ctx, ref)
	if err != nil {
		t.Fatalf("first CreateDir failed: %v", err)
	}

	// Second create should fail with AlreadyExists, not retry
	err = d.CreateDir(ctx, ref)
	if err == nil {
		t.Fatal("expected AlreadyExists error")
	}
}

func TestCreateDir_CleansUpOnCASFailure(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceEmpty(store, "space-1", "root-1")

	// Exhaust all retries
	store.injectChildCASFailures(20)

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "root-1"},
		Path:       "./orphandir",
	}

	d.CreateDir(ctx, ref)

	// Verify no orphan nodes remain — each failed attempt should have
	// cleaned up its PutNode before retrying
	allNodes, _ := store.ListAllNodes()
	for _, n := range allNodes {
		if n.Name == "orphandir" {
			t.Errorf("orphan node %s should have been cleaned up", n.ID)
		}
	}
}

func TestCasBackoff_Bounded(t *testing.T) {
	start := time.Now()
	casBackoff(0) // [0, 10ms] full jitter
	d0 := time.Since(start)

	start = time.Now()
	casBackoff(20) // attempt >> 6: should saturate at the 1s cap
	d10 := time.Since(start)

	if d0 > 50*time.Millisecond {
		t.Errorf("attempt 0 took %v, expected under 50ms (cap 10ms + slack)", d0)
	}
	if d10 > 1100*time.Millisecond {
		t.Errorf("attempt 20 took %v, expected under 1.1s (1s cap + slack)", d10)
	}
}

func TestUpload_CASBackoff_StillSucceeds(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceEmpty(store, "space-1", "root-1")

	// Inject PutNode CAS failures — should retry with backoff and succeed
	store.injectCASFailures(3)

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "root-1"},
		Path:       "./backoff-test.txt",
	}

	content := []byte("backoff test content")
	_, err := d.Upload(ctx, storage.UploadRequest{
		Ref:    ref,
		Body:   io.NopCloser(bytes.NewReader(content)),
		Length: int64(len(content)),
	}, nil)
	if err != nil {
		t.Fatalf("Upload should succeed after CAS retries with backoff: %v", err)
	}

	children, _, _ := store.GetChildren("space-1", "root-1")
	if _, ok := children["backoff-test.txt"]; !ok {
		t.Error("expected file in parent's children")
	}
}

func TestTreeSize_CASBackoff_StillPropagates(t *testing.T) {
	store := newMockMetadataStore()
	d := treeSizeDriver(store)

	store.nodes["s1.root"] = &NodeEntry{ID: "root", SpaceID: "s1", Size: 0}
	store.nodeRevs["s1.root"] = 1
	store.nodes["s1.child"] = &NodeEntry{ID: "child", SpaceID: "s1", ParentID: "root", Size: 100}
	store.nodeRevs["s1.child"] = 1

	// Inject 1 CAS failure — should retry with backoff and succeed
	store.injectCASFailures(1)

	d.propagateTreeSize(testContext(), "s1", "child", 50)

	node, _, _ := store.GetNode("s1", "child")
	if node.Size != 150 {
		t.Errorf("child size = %d, want 150", node.Size)
	}
	root, _, _ := store.GetNode("s1", "root")
	if root.Size != 50 {
		t.Errorf("root size = %d, want 50", root.Size)
	}
}

// --- putNodeWithCASCheck tests ---

func TestPutNodeWithCASCheck_RetrySucceeds(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
	store.injectCASFailures(2)

	err := d.putNodeWithCASCheck("s1", "f1", func(node *NodeEntry) error {
		if node.Metadata == nil {
			node.Metadata = make(map[string]string)
		}
		node.Metadata["key"] = "val"
		return nil
	})
	if err != nil {
		t.Fatalf("expected success after retries, got: %v", err)
	}

	node, _, _ := store.GetNode("s1", "f1")
	if node.Metadata["key"] != "val" {
		t.Errorf("metadata not set after CAS retry")
	}
}

func TestPutNodeWithCASCheck_Exhausted(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
	store.injectCASFailures(20)

	err := d.putNodeWithCASCheck("s1", "f1", func(node *NodeEntry) error {
		return nil
	})
	if err != ErrCASConflict {
		t.Fatalf("expected ErrCASConflict, got: %v", err)
	}
}

func TestPutNodeWithCASCheck_BusinessErrorAborts(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	err := d.putNodeWithCASCheck("s1", "f1", func(node *NodeEntry) error {
		return errtypes.Locked("test lock")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "lock") {
		t.Errorf("expected locked error, got: %v", err)
	}
}

func TestPutNodeWithCASCheck_NoChangeSkipsPut(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
	oldNode, oldRev, _ := store.GetNode("s1", "f1")
	oldMTime := oldNode.MTime

	err := d.putNodeWithCASCheck("s1", "f1", func(node *NodeEntry) error {
		return errNoChange
	})
	if err != nil {
		t.Fatalf("expected nil, got: %v", err)
	}

	node, rev, _ := store.GetNode("s1", "f1")
	if node.MTime != oldMTime {
		t.Errorf("node should be unchanged")
	}
	if rev != oldRev {
		t.Errorf("revision should be unchanged (no PutNode)")
	}
}

// --- Integration wiring tests ---

func TestSetMetadata_CASRetry(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
	store.injectCASFailures(2)

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	md := &provider.ArbitraryMetadata{
		Metadata: map[string]string{"foo": "bar"},
	}

	err := d.SetArbitraryMetadata(ctx, ref, md)
	if err != nil {
		t.Fatalf("SetArbitraryMetadata should succeed after CAS retries: %v", err)
	}

	node, _, _ := store.GetNode("s1", "f1")
	if node.Metadata["foo"] != "bar" {
		t.Error("metadata 'foo' not set after CAS retry")
	}
}

func TestSetLock_CASRetry(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
	store.injectCASFailures(2)

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	lock := &provider.Lock{
		LockId: "lock-1",
		Type:   provider.LockType_LOCK_TYPE_EXCL,
		Expiration: &typesv1.Timestamp{Seconds: uint64(time.Now().Add(time.Hour).Unix())},
	}

	err := d.SetLock(ctx, ref, lock)
	if err != nil {
		t.Fatalf("SetLock should succeed after CAS retries: %v", err)
	}

	node, _, _ := store.GetNode("s1", "f1")
	if node.Lock == nil || node.Lock.LockID != "lock-1" {
		t.Error("lock not set after CAS retry")
	}
}

func TestSetLock_AlreadyLocked_AbortsRetry(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")

	key := "s1.f1"
	store.nodes[key].Lock = &LockEntry{LockID: "existing-lock", Type: 0}

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	lock := &provider.Lock{
		LockId: "new-lock",
		Type:   provider.LockType_LOCK_TYPE_EXCL,
	}

	err := d.SetLock(ctx, ref, lock)
	if err == nil {
		t.Fatal("expected locked error")
	}
	if !strings.Contains(err.Error(), "locked") {
		t.Errorf("expected locked error, got: %v", err)
	}
}
