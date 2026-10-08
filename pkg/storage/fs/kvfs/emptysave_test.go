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
	"reflect"
	"strings"
	"testing"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
	revaevents "github.com/opencloud-eu/reva/v2/pkg/events"
)

// emptySaveFixture holds file s1/f1 "file.txt" with content "original" and folder s1/sub.
func emptySaveFixture() (*kvfsDriver, *mockMetadataStore, *mockBlobStore, *mockStream) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(store, blob, stream)
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
	blob.blobs[BlobKey("s1", "old-blob")] = []byte("original")
	store.nodes["s1.sub"] = &NodeEntry{
		ID: "sub", SpaceID: "s1", ParentID: "root", Name: "sub", Type: NodeTypeDir,
		ETag: "sub-etag", MTime: 500, Owner: "test-user-id", MimeType: "httpd/unix-directory",
	}
	store.nodeRevs["s1.sub"] = 1
	store.children["s1.root"]["sub"] = "sub"
	store.children["s1.sub"] = ChildMap{}
	store.childRevs["s1.sub"] = 1
	return d, store, blob, stream
}

func pathRef(path string) *provider.Reference {
	return &provider.Reference{ResourceId: &provider.ResourceId{StorageId: "st", SpaceId: "s1", OpaqueId: "root"}, Path: path}
}

func nodeSnapshot(store *mockMetadataStore) map[string]NodeEntry {
	store.mu.Lock()
	defer store.mu.Unlock()
	out := map[string]NodeEntry{}
	for k, n := range store.nodes {
		out[k] = *n
	}
	return out
}

func childrenSnapshot(store *mockMetadataStore) map[string]ChildMap {
	store.mu.Lock()
	defer store.mu.Unlock()
	out := map[string]ChildMap{}
	for k, cm := range store.children {
		cp := ChildMap{}
		for name, id := range cm {
			cp[name] = id
		}
		out[k] = cp
	}
	return out
}

func assertTreeUnchanged(t *testing.T, store *mockMetadataStore, nodes map[string]NodeEntry, children map[string]ChildMap, versions int) {
	t.Helper()
	if got := nodeSnapshot(store); !reflect.DeepEqual(got, nodes) {
		t.Errorf("nodes changed:\n got %v\nwant %v", got, nodes)
	}
	if got := childrenSnapshot(store); !reflect.DeepEqual(got, children) {
		t.Errorf("children changed:\n got %v\nwant %v", got, children)
	}
	if got := len(store.versions); got != versions {
		t.Errorf("versions = %d, want %d", got, versions)
	}
}

// --- TouchFile: only creates; an existing node is AlreadyExists, as in decomposedfs ---

func TestTouchFile_ExistingNodeIsAlreadyExists(t *testing.T) {
	for name, ref := range map[string]*provider.Reference{
		"file by id":   {ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"}},
		"file by path": pathRef("./file.txt"),
		"folder":       pathRef("./sub"),
	} {
		t.Run(name, func(t *testing.T) {
			d, store, _, stream := emptySaveFixture()
			nodes, children := nodeSnapshot(store), childrenSnapshot(store)
			if err := d.TouchFile(testContext(), ref, false, "1000000000"); !isAlreadyExists(err) {
				t.Fatalf("TouchFile = %v, want AlreadyExists", err)
			}
			assertTreeUnchanged(t, store, nodes, children, 0)
			if stream.eventCount() != 0 {
				t.Errorf("events = %d, want none", stream.eventCount())
			}
		})
	}
}

func TestTouchFile_MarkProcessingFlagsTheExistingFile(t *testing.T) {
	d, store, _, _ := emptySaveFixture()
	before := *store.nodes["s1.f1"]
	if err := d.TouchFile(testContext(), pathRef("./file.txt"), true, ""); err != nil {
		t.Fatalf("TouchFile(markprocessing): %v", err)
	}
	after := *store.nodes["s1.f1"]
	if !after.Processing {
		t.Error("the file is not flagged as processing")
	}
	after.Processing = false
	if !reflect.DeepEqual(after, before) {
		t.Errorf("markprocessing changed more than the flag: %+v", after)
	}
}

func TestTouchFile_ViewerIsPermissionDenied(t *testing.T) {
	d, store, _, _ := emptySaveFixture()
	store.nodes["s1.root"].Grants = map[string]*GrantEntry{
		"u:bob": {GranteeType: "user", GranteeID: "bob", Permissions: permissionsToUint32(&provider.ResourcePermissions{Stat: true, ListContainer: true})},
	}
	if err := d.TouchFile(testContextWithUser("bob", nil), pathRef("./file.txt"), false, ""); !isPermissionDenied(err) {
		t.Errorf("TouchFile by a viewer = %v, want PermissionDenied", err)
	}
}

// A name whose entry points at a missing node is not free: the create refuses it rather than
// orphaning whatever the entry is meant to hold.
func TestTouchFile_DanglingEntryIsAlreadyExists(t *testing.T) {
	d, store, _, _ := emptySaveFixture()
	store.children["s1.root"]["ghost.txt"] = "ghost"
	nodes, children := nodeSnapshot(store), childrenSnapshot(store)
	if err := d.TouchFile(testContext(), pathRef("./ghost.txt"), false, ""); !isAlreadyExists(err) {
		t.Fatalf("TouchFile over a dangling entry = %v, want AlreadyExists", err)
	}
	assertTreeUnchanged(t, store, nodes, children, 0)
}

func TestTouchFile_ReadErrorsChangeNothing(t *testing.T) {
	for name, c := range map[string]struct {
		ref  *provider.Reference
		fail func(*failingStore)
	}{
		"path lookup": {pathRef("./sub/new.txt"), func(s *failingStore) { s.failChildrenOf = "sub" }},
		"node read":   {&provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"}}, func(s *failingStore) { s.failNode = "f1" }},
	} {
		t.Run(name, func(t *testing.T) {
			d, store, _, _ := emptySaveFixture()
			fs := &failingStore{mockMetadataStore: store}
			c.fail(fs)
			d.store = fs
			nodes, children := nodeSnapshot(store), childrenSnapshot(store)
			if err := d.TouchFile(testContext(), c.ref, false, ""); err == nil || isAlreadyExists(err) {
				t.Fatalf("TouchFile with a read error = %v, want the error", err)
			}
			assertTreeUnchanged(t, store, nodes, children, 0)
		})
	}
}

// --- Empty uploads commit at initiation, as in decomposedfs ---

func TestEmptyUpload_TruncatesAndKeepsAVersion(t *testing.T) {
	d, store, blob, stream := emptySaveFixture()
	res, err := d.InitiateUpload(testContext(), pathRef("./file.txt"), 0, map[string]string{"mtime": "1000000000"})
	if err != nil {
		t.Fatalf("InitiateUpload(0): %v", err)
	}
	if res["simple"] == "" || res["tus"] != "" || len(store.uploads) != 0 {
		t.Errorf("result %v, sessions %d; want the simple protocol only and no session", res, len(store.uploads))
	}
	head := store.nodes["s1.f1"]
	if head.Size != 0 || head.BlobID != "" || head.Checksum != emptyChecksum || head.MimeType != "text/plain" || head.MTime != y2001 {
		t.Errorf("head = %+v, want an empty text/plain file dated %d", head, y2001)
	}
	if stream.eventCount() != 1 {
		t.Fatalf("events = %d, want 1", stream.eventCount())
	}
	if ev, ok := stream.lastEvent().payload.(revaevents.FileUploaded); !ok || ev.Ref.GetResourceId().GetOpaqueId() != "f1" {
		t.Errorf("event = %#v, want FileUploaded for f1", stream.lastEvent().payload)
	}
	if got := versionContents(t, store, blob); len(got) != 1 || !got["original"] {
		t.Errorf("versions = %v, want the replaced content", got)
	}
	if got := readHead(t, d); got != "" {
		t.Errorf("head reads %q, want nothing", got)
	}
}

func TestEmptyUpload_IfMatch(t *testing.T) {
	t.Run("stale etag on content", func(t *testing.T) {
		d, store, _, _ := emptySaveFixture()
		nodes, children := nodeSnapshot(store), childrenSnapshot(store)
		_, err := d.InitiateUpload(testContext(), pathRef("./file.txt"), 0, map[string]string{"if-match": "stale"})
		if _, ok := err.(errtypes.IsAborted); !ok {
			t.Fatalf("stale If-Match = %v, want Aborted", err)
		}
		assertTreeUnchanged(t, store, nodes, children, 0)
	})
	t.Run("current etag", func(t *testing.T) {
		d, store, _, _ := emptySaveFixture()
		if _, err := d.InitiateUpload(testContext(), pathRef("./file.txt"), 0, map[string]string{"if-match": "old-etag"}); err != nil {
			t.Fatalf("current If-Match: %v", err)
		}
		if store.nodes["s1.f1"].Size != 0 {
			t.Error("the file was not emptied")
		}
	})
	t.Run("replay on an emptied file", func(t *testing.T) {
		d, store, _, _ := emptySaveFixture()
		if _, err := d.InitiateUpload(testContext(), pathRef("./file.txt"), 0, map[string]string{"if-match": "old-etag"}); err != nil {
			t.Fatalf("first empty save: %v", err)
		}
		emptied := *store.nodes["s1.f1"]
		if _, err := d.InitiateUpload(testContext(), pathRef("./file.txt"), 0, map[string]string{"if-match": "old-etag"}); err != nil {
			t.Fatalf("replayed empty save: %v", err)
		}
		if got := *store.nodes["s1.f1"]; got.ETag != emptied.ETag || len(store.versions) != 1 {
			t.Errorf("a replay changed the file (etag %q -> %q) or added a version (%d)", emptied.ETag, got.ETag, len(store.versions))
		}
	})
}

func TestEmptyUpload_NewFile(t *testing.T) {
	d, store, _, _ := emptySaveFixture()
	if _, err := d.InitiateUpload(testContext(), pathRef("./sub/new.txt"), 0, map[string]string{"mtime": "1000000000"}); err != nil {
		t.Fatalf("InitiateUpload(0) of a new name: %v", err)
	}
	children, _, _ := store.GetChildren("s1", "sub")
	n, _, err := store.GetNode("s1", children["new.txt"])
	if err != nil {
		t.Fatalf("new file: %v", err)
	}
	if n.Size != 0 || n.Checksum != emptyChecksum || n.Owner != "test-user-id" || n.MimeType != "text/plain" || n.MTime != y2001 || n.Type != NodeTypeFile {
		t.Errorf("new file = %+v", n)
	}
	if len(store.uploads) != 0 {
		t.Errorf("%d sessions, want none", len(store.uploads))
	}
}

// A folder is never the target of an upload, whether named by path or by its own id.
func TestUpload_FolderTargetIsRefused(t *testing.T) {
	for _, length := range []int64{0, 5} {
		for name, ref := range map[string]*provider.Reference{
			"by path": pathRef("./sub"),
			"by id":   {ResourceId: &provider.ResourceId{StorageId: "st", SpaceId: "s1", OpaqueId: "sub"}, Path: "."},
		} {
			d, store, _, _ := emptySaveFixture()
			nodes, children := nodeSnapshot(store), childrenSnapshot(store)
			_, err := d.InitiateUpload(testContext(), ref, length, map[string]string{})
			if _, ok := err.(errtypes.IsPreconditionFailed); !ok {
				t.Errorf("%s, length %d: InitiateUpload = %v, want PreconditionFailed", name, length, err)
			}
			assertTreeUnchanged(t, store, nodes, children, 0)
		}
	}
}

// A TUS commit whose name turned into a folder meanwhile refuses to write into it.
func TestFinishUpload_RefusesAFolder(t *testing.T) {
	d, store, _, _ := emptySaveFixture()
	ctx := testContext()
	res, err := d.InitiateUpload(ctx, pathRef("./later-a-folder"), 5, map[string]string{})
	if err != nil {
		t.Fatalf("InitiateUpload: %v", err)
	}
	store.nodes["s1.dx"] = &NodeEntry{ID: "dx", SpaceID: "s1", ParentID: "root", Name: "later-a-folder", Type: NodeTypeDir, Owner: "test-user-id"}
	store.nodeRevs["s1.dx"] = 1
	store.children["s1.root"]["later-a-folder"] = "dx"
	before := *store.nodes["s1.dx"]

	up, err := d.GetUpload(ctx, res["tus"])
	if err != nil {
		t.Fatalf("GetUpload: %v", err)
	}
	if _, err := up.WriteChunk(ctx, 0, strings.NewReader("hello")); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	if err := up.FinishUpload(ctx); err == nil {
		t.Fatal("FinishUpload into a folder succeeded")
	}
	if got := *store.nodes["s1.dx"]; !reflect.DeepEqual(got, before) {
		t.Errorf("the folder changed: %+v", got)
	}
}

func TestEmptyUpload_SnapshotFailureKeepsTheContent(t *testing.T) {
	d, store, _, _ := emptySaveFixture()
	fs := &failingStore{mockMetadataStore: store, failPutVersion: true}
	d.store = fs
	nodes, children := nodeSnapshot(store), childrenSnapshot(store)
	if _, err := d.InitiateUpload(testContext(), pathRef("./file.txt"), 0, map[string]string{}); err == nil {
		t.Fatal("an empty save whose snapshot failed succeeded")
	}
	assertTreeUnchanged(t, store, nodes, children, 0)
}

// An empty save that loses the CAS race keeps the content its retry replaces, as its first attempt does.
func TestEmptyUpload_CASRetryKeepsTheRacingWritersContent(t *testing.T) {
	d, store, blob, _ := emptySaveFixture()
	d.store = &racingWriterStore{mockMetadataStore: store, blob: blob}
	if _, err := d.InitiateUpload(testContext(), pathRef("./file.txt"), 0, map[string]string{}); err != nil {
		t.Fatalf("InitiateUpload(0): %v", err)
	}
	if got := versionContents(t, store, blob); len(got) != 2 || !got["original"] || !got["racer"] {
		t.Errorf("versions hold %v, want original and racer", got)
	}
	if got := readHead(t, d); got != "" {
		t.Errorf("head reads %q, want nothing", got)
	}
}

func TestEmptyUpload_CASRetrySnapshotFailureKeepsTheRacingWritersContent(t *testing.T) {
	d, store, blob, _ := emptySaveFixture()
	d.store = &racingWriterStore{mockMetadataStore: store, blob: blob, failRacerVersion: true}
	if _, err := d.InitiateUpload(testContext(), pathRef("./file.txt"), 0, map[string]string{}); err == nil {
		t.Fatal("an empty save whose retry could not keep the replaced content succeeded")
	}
	if head, _, _ := store.GetNode("s1", "f1"); head.BlobID != "racer-blob" {
		t.Errorf("head blob = %q, want racer-blob (the refused save changes nothing)", head.BlobID)
	}
}

func TestEmptyUpload_ReadErrorsChangeNothing(t *testing.T) {
	for name, fail := range map[string]func(*failingStore){
		"children read": func(s *failingStore) { s.failChildrenOf = "root" },
		"target read":   func(s *failingStore) { s.failNode = "f1" },
	} {
		t.Run(name, func(t *testing.T) {
			d, store, _, _ := emptySaveFixture()
			fs := &failingStore{mockMetadataStore: store}
			fail(fs)
			d.store = fs
			nodes, children := nodeSnapshot(store), childrenSnapshot(store)
			if _, err := d.InitiateUpload(testContext(), pathRef("./file.txt"), 0, map[string]string{}); err == nil {
				t.Fatal("an empty save with a read error succeeded")
			}
			assertTreeUnchanged(t, store, nodes, children, 0)
		})
	}
}

// A file-level grant (an OCS file share) authorizes an empty save, as it does any upload.
func TestEmptyUpload_FileShareGrantee(t *testing.T) {
	d, store, _, _ := emptySaveFixture()
	store.nodes["s1.f1"].Grants = map[string]*GrantEntry{
		"u:bob": {GranteeType: "user", GranteeID: "bob", Permissions: permissionsToUint32(&provider.ResourcePermissions{
			Stat: true, InitiateFileDownload: true, InitiateFileUpload: true,
		})},
	}
	if _, err := d.InitiateUpload(testContextWithUser("bob", nil), &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"}, Path: "."}, 0, map[string]string{}); err != nil {
		t.Fatalf("empty save by a file-share grantee: %v", err)
	}
	if store.nodes["s1.f1"].Size != 0 {
		t.Error("the file was not emptied")
	}
}

// --- Empty content reads as nothing, without asking the blob store ---

func TestDownload_EmptyFileReadsNothing(t *testing.T) {
	d, store, _, _ := emptySaveFixture()
	store.nodes["s1.f1"].BlobID, store.nodes["s1.f1"].Size = "", 0
	if got := readHead(t, d); got != "" {
		t.Errorf("empty file reads %q", got)
	}
	store.versions = append(store.versions, &VersionEntry{Key: "ve", NodeID: "f1", SpaceID: "s1", MTime: 1})
	_, rc, err := d.DownloadRevision(testContext(), f1Ref(), "f1.REV.ve", nil)
	if err != nil {
		t.Fatalf("DownloadRevision of an empty version: %v", err)
	}
	if got := readAll(t, rc); got != "" {
		t.Errorf("empty version reads %q", got)
	}
}
