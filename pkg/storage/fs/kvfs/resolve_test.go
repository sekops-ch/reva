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
	"testing"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
	"github.com/opencloud-eu/reva/v2/pkg/storage"
)

// folderTree builds a space whose root and folder F hold the same names, so a path resolved
// against the wrong base lands on a different node: root{x.txt=rx, sub=rsub, F}, F{x.txt=fx, sub=fsub}.
func folderTree(store *mockMetadataStore) {
	setupSpaceEmpty(store, "s1", "root")
	add := func(id, parent, name string, typ NodeType) {
		store.nodes["s1."+id] = &NodeEntry{ID: id, SpaceID: "s1", ParentID: parent, Name: name, Type: typ,
			Owner: "test-user-id", BlobID: "blob-" + id, Size: 3, BlobSize: 3, ETag: "etag-" + id}
		store.nodeRevs["s1."+id] = 1
		if typ == NodeTypeDir {
			store.nodes["s1."+id].BlobID, store.nodes["s1."+id].Size, store.nodes["s1."+id].BlobSize = "", 0, 0
			store.children["s1."+id] = ChildMap{}
			store.childRevs["s1."+id] = 1
		}
		store.children["s1."+parent][name] = id
	}
	add("rx", "root", "x.txt", NodeTypeFile)
	add("rsub", "root", "sub", NodeTypeDir)
	add("F", "root", "F", NodeTypeDir)
	add("fx", "F", "x.txt", NodeTypeFile)
	add("fsub", "F", "sub", NodeTypeDir)
}

func idRef(id, path string) *provider.Reference {
	return &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: id}, Path: path}
}

func childIDs(store *mockMetadataStore, dir string) ChildMap {
	out := ChildMap{}
	for k, v := range store.children["s1."+dir] {
		out[k] = v
	}
	return out
}

func assertChildren(t *testing.T, store *mockMetadataStore, dir string, want ChildMap) {
	t.Helper()
	got := childIDs(store, dir)
	if len(got) != len(want) {
		t.Errorf("%s children = %v, want %v", dir, got, want)
		return
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s children = %v, want %v", dir, got, want)
			return
		}
	}
}

var rootTree = ChildMap{"x.txt": "rx", "sub": "rsub", "F": "F"}

func TestFolderRelativeUpload_LandsInTheFolder(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	folderTree(store)
	ctx := testContext()

	// overwrite of F/x.txt through a folder-relative reference
	result, err := d.InitiateUpload(ctx, idRef("F", "./x.txt"), 3, map[string]string{})
	if err != nil {
		t.Fatalf("InitiateUpload: %v", err)
	}
	if _, err := d.Upload(ctx, storage.UploadRequest{Ref: &provider.Reference{Path: "/" + result["simple"]},
		Body: io.NopCloser(bytes.NewReader([]byte("new"))), Length: 3}, nil); err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if store.nodes["s1.rx"].BlobID != "blob-rx" {
		t.Error("root's x.txt was overwritten; the upload named F/x.txt")
	}
	if store.nodes["s1.fx"].BlobID == "blob-fx" {
		t.Error("F/x.txt was not overwritten")
	}

	// new file two levels down, where the root has a folder of the same name
	result, err = d.InitiateUpload(ctx, idRef("F", "./sub/new.txt"), 3, map[string]string{})
	if err != nil {
		t.Fatalf("InitiateUpload sub: %v", err)
	}
	session, err := store.GetUpload(result["tus"])
	if err != nil {
		t.Fatalf("TUS session: %v", err)
	}
	if session.ParentID != "fsub" {
		t.Errorf("TUS session parent = %q, want fsub (F/sub)", session.ParentID)
	}
	assertChildren(t, store, "root", rootTree)
}

func TestFolderRelativeCreates_LandInTheFolder(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	folderTree(store)
	ctx := testContext()

	if err := d.TouchFile(ctx, idRef("F", "./empty.txt"), false, ""); err != nil {
		t.Fatalf("TouchFile: %v", err)
	}
	if err := d.CreateDir(ctx, idRef("F", "./newdir")); err != nil {
		t.Fatalf("CreateDir: %v", err)
	}
	if err := d.Move(ctx, idRef("rx", "."), idRef("F", "./moved.txt")); err != nil {
		t.Fatalf("Move: %v", err)
	}
	got := childIDs(store, "F")
	for _, name := range []string{"empty.txt", "newdir", "moved.txt"} {
		if got[name] == "" {
			t.Errorf("F has no %s; children %v", name, got)
		}
	}
	if got["moved.txt"] != "rx" {
		t.Errorf("F/moved.txt = %q, want the moved node rx", got["moved.txt"])
	}
	assertChildren(t, store, "root", ChildMap{"sub": "rsub", "F": "F"})
}

// A share recipient holds grants on the shared folder only, and the share jail addresses the
// folder by id with a relative path.
func TestFolderRelativeUpload_RecipientWithFolderGrant(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	folderTree(store)
	store.nodes["s1.F"].Grants = map[string]*GrantEntry{
		"u:bob": {GranteeType: "user", GranteeID: "bob", Permissions: permissionsToUint32(&provider.ResourcePermissions{
			Stat: true, ListContainer: true, InitiateFileUpload: true, InitiateFileDownload: true,
			CreateContainer: true, Move: true, Delete: true,
		})},
	}
	bob := testContextWithUser("bob", nil)

	if _, err := d.InitiateUpload(bob, idRef("F", "./new.txt"), 3, map[string]string{}); err != nil {
		t.Errorf("recipient upload into the shared folder: %v", err)
	}
	if err := d.CreateDir(bob, idRef("F", "./bobdir")); err != nil {
		t.Errorf("recipient mkdir in the shared folder: %v", err)
	}
	if _, err := d.InitiateUpload(bob, idRef("root", "./new.txt"), 3, map[string]string{}); err == nil {
		t.Error("recipient upload into the owner's root succeeded; want refused")
	}
	assertChildren(t, store, "root", rootTree)
}

func TestStaleIDWithDot_CreatesNothing(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	folderTree(store)

	err := d.TouchFile(testContext(), idRef("gone", "."), false, "")
	if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Errorf("TouchFile on a missing id = %v, want NotFound", err)
	}
	assertChildren(t, store, "root", rootTree)
}

func TestPathThroughAFile_Refused(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	folderTree(store)

	err := d.CreateDir(testContext(), idRef("root", "./x.txt/sub"))
	if _, ok := err.(errtypes.IsPreconditionFailed); !ok {
		t.Errorf("CreateDir below a file = %v, want PreconditionFailed", err)
	}
	if len(store.children["s1.rx"]) != 0 {
		t.Errorf("a file got children: %v", store.children["s1.rx"])
	}
}

func TestResolveParent(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	folderTree(store)
	ctx := testContext()
	rootless := &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1"}, Path: "./a.txt"}

	for _, tc := range []struct {
		label        string
		ref          *provider.Reference
		parent, name string
	}{
		{"root-relative", idRef("root", "./a.txt"), "root", "a.txt"},
		{"folder-relative", idRef("F", "./a.txt"), "F", "a.txt"},
		{"folder-relative two levels, same names at the root", idRef("F", "./sub/a.txt"), "fsub", "a.txt"},
		{"no node id: the space root", rootless, "root", "a.txt"},
		{"file id with '.'", idRef("fx", "."), "F", "x.txt"},
		{"file id with no path", idRef("fx", ""), "F", "x.txt"},
		{"file id with '/'", idRef("fx", "/"), "F", "x.txt"},
		{"folder id with '.'", idRef("F", "."), "root", "F"},
		{"leading slash is relative too", idRef("F", "/sub/a.txt"), "fsub", "a.txt"},
	} {
		space, parent, name, err := d.resolveParent(ctx, tc.ref)
		if err != nil {
			t.Errorf("%s: %v", tc.label, err)
			continue
		}
		if space != "s1" || parent.ID != tc.parent || name != tc.name {
			t.Errorf("%s: got (%s, %s, %s), want (s1, %s, %s)", tc.label, space, parent.ID, name, tc.parent, tc.name)
		}
	}

	isBadRequest := func(err error) bool { _, ok := err.(errtypes.IsBadRequest); return ok }
	isNotFound := func(err error) bool { _, ok := err.(errtypes.IsNotFound); return ok }
	isPrecondition := func(err error) bool { _, ok := err.(errtypes.IsPreconditionFailed); return ok }
	for _, tc := range []struct {
		label string
		ref   *provider.Reference
		want  func(error) bool
	}{
		{"the space root has no parent", idRef("root", "."), isBadRequest},
		{"'..' is not a name", idRef("F", "./.."), isBadRequest},
		{"a missing id", idRef("gone", "."), isNotFound},
		{"a missing folder on the path", idRef("F", "./missing/a.txt"), isNotFound},
		{"'..' never climbs above the referenced node", idRef("F", "../x.txt"), isNotFound},
		{"a path through a file", idRef("F", "./x.txt/a.txt"), isPrecondition},
		{"a path relative to a file", idRef("fx", "./a.txt"), isPrecondition},
		{"no space id", &provider.Reference{ResourceId: &provider.ResourceId{OpaqueId: "F"}, Path: "./a"}, isBadRequest},
	} {
		if _, _, _, err := d.resolveParent(ctx, tc.ref); !tc.want(err) {
			t.Errorf("%s: err = %v (%T)", tc.label, err, err)
		}
	}
}

// Writing below a file tells only those who may see the file that it is one; anyone else gets the
// answer for a missing parent.
func TestPathThroughAHiddenFile_LooksMissing(t *testing.T) {
	ops := map[string]func(d *kvfsDriver, ctx context.Context, ref *provider.Reference) error{
		"CreateDir": func(d *kvfsDriver, ctx context.Context, ref *provider.Reference) error { return d.CreateDir(ctx, ref) },
		"TouchFile": func(d *kvfsDriver, ctx context.Context, ref *provider.Reference) error {
			return d.TouchFile(ctx, ref, false, "")
		},
		"InitiateUpload(0)": func(d *kvfsDriver, ctx context.Context, ref *provider.Reference) error {
			_, err := d.InitiateUpload(ctx, ref, 0, map[string]string{})
			return err
		},
		"InitiateUpload(5)": func(d *kvfsDriver, ctx context.Context, ref *provider.Reference) error {
			_, err := d.InitiateUpload(ctx, ref, 5, map[string]string{})
			return err
		},
	}
	at := func(path string) *provider.Reference {
		return &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"}, Path: path}
	}
	kind := func(err error) string {
		switch err.(type) {
		case nil:
			return "nil"
		case errtypes.IsNotFound:
			return "NotFound"
		case errtypes.IsPermissionDenied:
			return "PermissionDenied"
		case errtypes.IsPreconditionFailed:
			return "PreconditionFailed"
		}
		return "other: " + err.Error()
	}
	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			store := newMockMetadataStore()
			d := testDriver(store, newMockBlobStore())
			setupSpaceWithDir(store, "s1", "root", "docs", "Documents", map[string]string{"salaries.xlsx": "sal"})
			eve := testContextWithUser("eve", nil)

			file, missing := op(d, eve, at("./Documents/salaries.xlsx/x")), op(d, eve, at("./Documents/nothing/x"))
			if kind(file) != kind(missing) {
				t.Errorf("an outsider tells a hidden file (%s) from a missing name (%s)", kind(file), kind(missing))
			}
			if err := op(d, testContext(), at("./Documents/salaries.xlsx/x")); kind(err) != "PreconditionFailed" {
				t.Errorf("the owner gets %s, want PreconditionFailed", kind(err))
			}
		})
	}
}
