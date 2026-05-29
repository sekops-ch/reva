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
)

// --- GetNodes unit tests ---

func TestGetNodes_Empty(t *testing.T) {
	store := newMockMetadataStore()

	nodes, err := store.GetNodes("s1", nil)
	if err != nil {
		t.Fatalf("GetNodes(nil) failed: %v", err)
	}
	if len(nodes) != 0 {
		t.Errorf("expected empty result for nil input, got %d", len(nodes))
	}

	nodes, err = store.GetNodes("s1", []string{})
	if err != nil {
		t.Fatalf("GetNodes([]) failed: %v", err)
	}
	if len(nodes) != 0 {
		t.Errorf("expected empty result for empty slice, got %d", len(nodes))
	}
}

func TestGetNodes_AllFound(t *testing.T) {
	store := newMockMetadataStore()
	store.nodes["s1.a"] = &NodeEntry{ID: "a", SpaceID: "s1", Name: "file-a.txt", Type: NodeTypeFile}
	store.nodeRevs["s1.a"] = 1
	store.nodes["s1.b"] = &NodeEntry{ID: "b", SpaceID: "s1", Name: "file-b.txt", Type: NodeTypeFile}
	store.nodeRevs["s1.b"] = 1
	store.nodes["s1.c"] = &NodeEntry{ID: "c", SpaceID: "s1", Name: "dir-c", Type: NodeTypeDir}
	store.nodeRevs["s1.c"] = 1

	nodes, err := store.GetNodes("s1", []string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("GetNodes failed: %v", err)
	}
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}
	if nodes["a"].Name != "file-a.txt" {
		t.Errorf("node a name = %q, want file-a.txt", nodes["a"].Name)
	}
	if nodes["c"].Type != NodeTypeDir {
		t.Errorf("node c should be a directory")
	}
}

func TestGetNodes_PartialMissing(t *testing.T) {
	store := newMockMetadataStore()
	store.nodes["s1.a"] = &NodeEntry{ID: "a", SpaceID: "s1", Name: "exists.txt", Type: NodeTypeFile}
	store.nodeRevs["s1.a"] = 1

	nodes, err := store.GetNodes("s1", []string{"a", "missing-1", "missing-2"})
	if err != nil {
		t.Fatalf("GetNodes failed: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node (missing silently omitted), got %d", len(nodes))
	}
	if _, ok := nodes["a"]; !ok {
		t.Error("existing node 'a' should be in result")
	}
}

func TestGetNodes_CrossSpaceIsolation(t *testing.T) {
	store := newMockMetadataStore()
	store.nodes["s1.a"] = &NodeEntry{ID: "a", SpaceID: "s1", Name: "s1-file.txt", Type: NodeTypeFile}
	store.nodeRevs["s1.a"] = 1
	store.nodes["s2.a"] = &NodeEntry{ID: "a", SpaceID: "s2", Name: "s2-file.txt", Type: NodeTypeFile}
	store.nodeRevs["s2.a"] = 1

	nodes, err := store.GetNodes("s1", []string{"a"})
	if err != nil {
		t.Fatalf("GetNodes failed: %v", err)
	}
	if nodes["a"].Name != "s1-file.txt" {
		t.Errorf("should get s1 node, got name=%q", nodes["a"].Name)
	}
}

// --- ListFolder tests ---

func TestListFolder_BatchFetch(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	setupSpaceEmpty(store, "s1", "root")

	children := make(ChildMap)
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("f%d", i)
		name := fmt.Sprintf("file-%d.txt", i)
		store.nodes["s1."+id] = &NodeEntry{
			ID: id, SpaceID: "s1", ParentID: "root",
			Name: name, Type: NodeTypeFile, Size: int64(i * 100),
			Owner: "test-user-id",
		}
		store.nodeRevs["s1."+id] = 1
		children[name] = id
	}
	store.children["s1.root"] = children

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       ".",
	}

	result, err := d.ListFolder(ctx, ref, nil, nil)
	if err != nil {
		t.Fatalf("ListFolder failed: %v", err)
	}
	if len(result) != 20 {
		t.Fatalf("expected 20 children, got %d", len(result))
	}

	names := make(map[string]bool)
	for _, ri := range result {
		names[ri.Name] = true
		if ri.PermissionSet == nil {
			t.Errorf("child %s should have permissions", ri.Name)
		}
	}
	for i := 0; i < 20; i++ {
		name := fmt.Sprintf("file-%d.txt", i)
		if !names[name] {
			t.Errorf("missing child %s", name)
		}
	}
}

func TestListFolder_MissingChild(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	setupSpaceEmpty(store, "s1", "root")

	store.nodes["s1.f1"] = &NodeEntry{
		ID: "f1", SpaceID: "s1", ParentID: "root",
		Name: "exists.txt", Type: NodeTypeFile, Owner: "test-user-id",
	}
	store.nodeRevs["s1.f1"] = 1

	store.children["s1.root"] = ChildMap{
		"exists.txt":  "f1",
		"missing.txt": "ghost-id",
	}

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       ".",
	}

	result, err := d.ListFolder(ctx, ref, nil, nil)
	if err != nil {
		t.Fatalf("ListFolder should not error on missing child: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 child (missing silently skipped), got %d", len(result))
	}
	if result[0].Name != "exists.txt" {
		t.Errorf("expected exists.txt, got %s", result[0].Name)
	}
}

func TestListFolder_EmptyDirectory(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	setupSpaceEmpty(store, "s1", "root")

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       ".",
	}

	result, err := d.ListFolder(ctx, ref, nil, nil)
	if err != nil {
		t.Fatalf("ListFolder on empty dir failed: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected 0 children, got %d", len(result))
	}
}

func TestListFolder_MixedTypes(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	setupSpaceEmpty(store, "s1", "root")

	store.nodes["s1.f1"] = &NodeEntry{
		ID: "f1", SpaceID: "s1", ParentID: "root",
		Name: "readme.txt", Type: NodeTypeFile, Size: 100, Owner: "test-user-id",
	}
	store.nodeRevs["s1.f1"] = 1
	store.nodes["s1.d1"] = &NodeEntry{
		ID: "d1", SpaceID: "s1", ParentID: "root",
		Name: "subdir", Type: NodeTypeDir, Owner: "test-user-id",
	}
	store.nodeRevs["s1.d1"] = 1

	store.children["s1.root"] = ChildMap{
		"readme.txt": "f1",
		"subdir":     "d1",
	}

	ctx := testContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       ".",
	}

	result, err := d.ListFolder(ctx, ref, nil, nil)
	if err != nil {
		t.Fatalf("ListFolder failed: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 children, got %d", len(result))
	}

	typeMap := make(map[string]provider.ResourceType)
	for _, ri := range result {
		typeMap[ri.Name] = ri.Type
	}
	if typeMap["readme.txt"] != provider.ResourceType_RESOURCE_TYPE_FILE {
		t.Error("readme.txt should be a file")
	}
	if typeMap["subdir"] != provider.ResourceType_RESOURCE_TYPE_CONTAINER {
		t.Error("subdir should be a container")
	}
}
