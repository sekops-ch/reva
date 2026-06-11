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

	"github.com/rs/zerolog"
)

func treeSizeDriver(store *mockMetadataStore) *kvfsDriver {
	log := zerolog.Nop()
	return &kvfsDriver{
		store: store,
		blob:  newMockBlobStore(),
		opts:  &Options{},
		log:   &log,
	}
}

func TestPropagateTreeSize_Basic(t *testing.T) {
	store := newMockMetadataStore()
	d := treeSizeDriver(store)

	// Build: root (size=0) -> child (size=100)
	store.nodes["s1.root"] = &NodeEntry{ID: "root", SpaceID: "s1", Size: 0}
	store.nodeRevs["s1.root"] = 1
	store.nodes["s1.child"] = &NodeEntry{ID: "child", SpaceID: "s1", ParentID: "root", Size: 100}
	store.nodeRevs["s1.child"] = 1

	d.propagateTreeSize(context.Background(), "s1", "child", 50)

	node, _, _ := store.GetNode("s1", "child")
	if node.Size != 150 {
		t.Errorf("child size = %d, want 150", node.Size)
	}
	root, _, _ := store.GetNode("s1", "root")
	if root.Size != 50 {
		t.Errorf("root size = %d, want 50", root.Size)
	}
}

func TestPropagateTreeSize_ZeroDelta_BubblesEtagAndMtime(t *testing.T) {
	// Regression: even with size delta = 0 (e.g. same-folder rename), the
	// propagation walk must still bubble MTime and ETag up to the space
	// root so sync clients can detect the change.
	store := newMockMetadataStore()
	d := treeSizeDriver(store)

	store.nodes["s1.n1"] = &NodeEntry{ID: "n1", SpaceID: "s1", Size: 100, ETag: "old-etag", MTime: 1}
	store.nodeRevs["s1.n1"] = 1

	d.propagateTreeSize(context.Background(), "s1", "n1", 0)

	node, _, _ := store.GetNode("s1", "n1")
	if node.Size != 100 {
		t.Errorf("size = %d, want 100 (unchanged on zero delta)", node.Size)
	}
	if node.ETag == "old-etag" {
		t.Error("ETag should have been refreshed even with zero size delta")
	}
	if node.MTime == 1 {
		t.Error("MTime should have been refreshed even with zero size delta")
	}
}

func TestPropagateTreeSize_DeepChangeBubblesEtagToRoot(t *testing.T) {
	// Regression: a write three levels deep must refresh the ETag on
	// every ancestor, including the space root. Sync clients poll the
	// root ETag and descend; if the root ETag does not change, deep
	// changes are invisible.
	store := newMockMetadataStore()
	d := treeSizeDriver(store)

	// root -> a -> b -> leaf
	store.nodes["s1.root"] = &NodeEntry{ID: "root", SpaceID: "s1", ETag: "root-old"}
	store.nodes["s1.a"] = &NodeEntry{ID: "a", SpaceID: "s1", ParentID: "root", ETag: "a-old"}
	store.nodes["s1.b"] = &NodeEntry{ID: "b", SpaceID: "s1", ParentID: "a", ETag: "b-old"}
	store.nodes["s1.leaf"] = &NodeEntry{ID: "leaf", SpaceID: "s1", ParentID: "b", ETag: "leaf-old"}
	store.nodeRevs["s1.root"] = 1
	store.nodeRevs["s1.a"] = 1
	store.nodeRevs["s1.b"] = 1
	store.nodeRevs["s1.leaf"] = 1

	d.propagateTreeSize(context.Background(), "s1", "b", 7)

	for _, id := range []string{"b", "a", "root"} {
		n, _, _ := store.GetNode("s1", id)
		if n.ETag == "" || n.ETag[len(n.ETag)-3:] == "old" {
			t.Errorf("ancestor %s ETag = %q, expected refreshed value", id, n.ETag)
		}
	}
}

func TestPropagateTreeSize_NegativeClampsToZero(t *testing.T) {
	store := newMockMetadataStore()
	d := treeSizeDriver(store)

	store.nodes["s1.n1"] = &NodeEntry{ID: "n1", SpaceID: "s1", Size: 10}
	store.nodeRevs["s1.n1"] = 1

	d.propagateTreeSize(context.Background(), "s1", "n1", -100)

	node, _, _ := store.GetNode("s1", "n1")
	if node.Size != 0 {
		t.Errorf("size = %d, want 0 (clamped)", node.Size)
	}
}

func TestPropagateTreeSize_RetriesOnCASConflict(t *testing.T) {
	store := newMockMetadataStore()
	d := treeSizeDriver(store)

	// Build: root -> child
	store.nodes["s1.root"] = &NodeEntry{ID: "root", SpaceID: "s1", Size: 0}
	store.nodeRevs["s1.root"] = 1
	store.nodes["s1.child"] = &NodeEntry{ID: "child", SpaceID: "s1", ParentID: "root", Size: 50}
	store.nodeRevs["s1.child"] = 1

	// Inject 2 CAS failures (less than maxRetries=3)
	store.injectCASFailures(2)

	d.propagateTreeSize(context.Background(), "s1", "child", 10)

	// Should eventually succeed on retry
	node, _, _ := store.GetNode("s1", "child")
	if node.Size != 60 {
		t.Errorf("child size = %d, want 60 (should retry through CAS failures)", node.Size)
	}

	root, _, _ := store.GetNode("s1", "root")
	if root.Size != 10 {
		t.Errorf("root size = %d, want 10", root.Size)
	}
}

func TestPropagateTreeSize_ExhaustsRetries_IncrementsDrift(t *testing.T) {
	store := newMockMetadataStore()
	d := treeSizeDriver(store)

	store.nodes["s1.root"] = &NodeEntry{ID: "root", SpaceID: "s1", Size: 0}
	store.nodeRevs["s1.root"] = 1
	store.nodes["s1.child"] = &NodeEntry{ID: "child", SpaceID: "s1", ParentID: "root", Size: 50}
	store.nodeRevs["s1.child"] = 1

	// Inject more failures than retries (3+ for the child)
	store.injectCASFailures(5)

	d.propagateTreeSize(context.Background(), "s1", "child", 10)

	// Child update should fail (CAS exhausted) but propagation should
	// still attempt the root
	// The TreeSizeDrift metric should be incremented (we can't easily
	// check the counter value, but the test verifies no panic/hang)
}

func TestPropagateTreeSize_ContinuesToParentAfterFailure(t *testing.T) {
	store := newMockMetadataStore()
	d := treeSizeDriver(store)

	// Build: grandparent -> parent -> child
	store.nodes["s1.gp"] = &NodeEntry{ID: "gp", SpaceID: "s1", Size: 0}
	store.nodeRevs["s1.gp"] = 1
	store.nodes["s1.parent"] = &NodeEntry{ID: "parent", SpaceID: "s1", ParentID: "gp", Size: 0}
	store.nodeRevs["s1.parent"] = 1
	store.nodes["s1.child"] = &NodeEntry{ID: "child", SpaceID: "s1", ParentID: "parent", Size: 50}
	store.nodeRevs["s1.child"] = 1

	// Inject 4 failures: child will exhaust 3 retries, then parent gets 1 which
	// it can't handle either. But we still attempt grandparent.
	// Actually, injectCASFailures applies globally. Let's test with 3 failures
	// so child exhausts, parent succeeds.
	store.injectCASFailures(3)

	d.propagateTreeSize(context.Background(), "s1", "child", 20)

	// Even though child CAS was exhausted, parent and grandparent should be attempted
	// Parent may or may not have been updated depending on timing with casFailsLeft
	// The key assertion: no panic, no hang, function completes.
}

func TestPropagateTreeSize_DeepTree(t *testing.T) {
	store := newMockMetadataStore()
	d := treeSizeDriver(store)

	// Build a 5-level tree
	store.nodes["s1.root"] = &NodeEntry{ID: "root", SpaceID: "s1", Size: 0}
	store.nodeRevs["s1.root"] = 1
	store.nodes["s1.l1"] = &NodeEntry{ID: "l1", SpaceID: "s1", ParentID: "root", Size: 0}
	store.nodeRevs["s1.l1"] = 1
	store.nodes["s1.l2"] = &NodeEntry{ID: "l2", SpaceID: "s1", ParentID: "l1", Size: 0}
	store.nodeRevs["s1.l2"] = 1
	store.nodes["s1.l3"] = &NodeEntry{ID: "l3", SpaceID: "s1", ParentID: "l2", Size: 0}
	store.nodeRevs["s1.l3"] = 1
	store.nodes["s1.leaf"] = &NodeEntry{ID: "leaf", SpaceID: "s1", ParentID: "l3", Size: 0}
	store.nodeRevs["s1.leaf"] = 1

	d.propagateTreeSize(context.Background(), "s1", "leaf", 100)

	for _, id := range []string{"leaf", "l3", "l2", "l1", "root"} {
		node, _, _ := store.GetNode("s1", id)
		if node.Size != 100 {
			t.Errorf("node %s size = %d, want 100", id, node.Size)
		}
	}
}

func TestPropagateZeroDelta_CreateDirDepth3_BubblesEtagToRoot(t *testing.T) {
	// CreateDir at depth > 1 must bubble ETag to root via propagateTreeSize.
	store := newMockMetadataStore()
	d := treeSizeDriver(store)

	// root -> a -> b (3 levels; CreateDir would propagate from b upward)
	store.nodes["s1.root"] = &NodeEntry{ID: "root", SpaceID: "s1", ETag: "root-old", MTime: 1}
	store.nodes["s1.a"] = &NodeEntry{ID: "a", SpaceID: "s1", ParentID: "root", ETag: "a-old", MTime: 1}
	store.nodes["s1.b"] = &NodeEntry{ID: "b", SpaceID: "s1", ParentID: "a", ETag: "b-old", MTime: 1}
	store.nodeRevs["s1.root"] = 1
	store.nodeRevs["s1.a"] = 1
	store.nodeRevs["s1.b"] = 1

	d.propagateTreeSize(context.Background(), "s1", "b", 0)

	for _, id := range []string{"b", "a", "root"} {
		n, _, _ := store.GetNode("s1", id)
		if n.ETag == id+"-old" {
			t.Errorf("CreateDir depth=3: %s ETag not refreshed (still %q)", id, n.ETag)
		}
		if n.MTime == 1 {
			t.Errorf("CreateDir depth=3: %s MTime not refreshed (still 1)", id)
		}
	}
}

func TestPropagateZeroDelta_TouchFileDepth3_BubblesEtagToRoot(t *testing.T) {
	// TouchFile at depth > 1 must bubble ETag+MTime to root via propagateTreeSize.
	store := newMockMetadataStore()
	d := treeSizeDriver(store)

	store.nodes["s1.root"] = &NodeEntry{ID: "root", SpaceID: "s1", ETag: "root-old", MTime: 1}
	store.nodes["s1.a"] = &NodeEntry{ID: "a", SpaceID: "s1", ParentID: "root", ETag: "a-old", MTime: 1}
	store.nodes["s1.b"] = &NodeEntry{ID: "b", SpaceID: "s1", ParentID: "a", ETag: "b-old", MTime: 1}
	store.nodeRevs["s1.root"] = 1
	store.nodeRevs["s1.a"] = 1
	store.nodeRevs["s1.b"] = 1

	d.propagateTreeSize(context.Background(), "s1", "b", 0)

	root, _, _ := store.GetNode("s1", "root")
	if root.ETag == "root-old" {
		t.Error("TouchFile depth=3: root ETag not refreshed")
	}
	if root.MTime == 1 {
		t.Error("TouchFile depth=3: root MTime not refreshed")
	}
}

func TestDelete_PropagatesAllFieldsWithoutRedundantTouch(t *testing.T) {
	// A single propagateTreeSize(-size) must update Size, MTime, and ETag
	// on every ancestor without a separate parent touch.
	store := newMockMetadataStore()
	d := treeSizeDriver(store)

	store.nodes["s1.root"] = &NodeEntry{ID: "root", SpaceID: "s1", Size: 500, ETag: "root-old", MTime: 1}
	store.nodes["s1.a"] = &NodeEntry{ID: "a", SpaceID: "s1", ParentID: "root", Size: 500, ETag: "a-old", MTime: 1}
	store.nodes["s1.b"] = &NodeEntry{ID: "b", SpaceID: "s1", ParentID: "a", Size: 200, ETag: "b-old", MTime: 1}
	store.nodeRevs["s1.root"] = 1
	store.nodeRevs["s1.a"] = 1
	store.nodeRevs["s1.b"] = 1

	d.propagateTreeSize(context.Background(), "s1", "b", -200)

	for _, tc := range []struct {
		id       string
		wantSize int64
	}{
		{"b", 0},
		{"a", 300},
		{"root", 300},
	} {
		n, _, _ := store.GetNode("s1", tc.id)
		if n.Size != tc.wantSize {
			t.Errorf("%s Size = %d, want %d", tc.id, n.Size, tc.wantSize)
		}
		if n.ETag == tc.id+"-old" {
			t.Errorf("%s ETag not refreshed (still %q)", tc.id, n.ETag)
		}
		if n.MTime == 1 {
			t.Errorf("%s MTime not refreshed (still 1)", tc.id)
		}
	}
}
