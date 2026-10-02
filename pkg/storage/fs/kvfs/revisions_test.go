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
	"io"
	"strings"
	"testing"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
	revaevents "github.com/opencloud-eu/reva/v2/pkg/events"
)

// Revision keys are spelled out literally so the tests pin the wire format.

func revisionRef(opaqueID string) *provider.Reference {
	return &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: opaqueID},
		Path:       ".",
	}
}

// twoRevisionFixture leaves f1 with head "content-1" and one version entry
// holding "content-0" (plus the "original" entry), and returns that entry.
func twoRevisionFixture(t *testing.T) (*kvfsDriver, *mockMetadataStore, *mockBlobStore, *VersionEntry) {
	t.Helper()
	d, store, blob := newVersionFixture(0)
	mustOverwrite(t, d, "content-0")
	mustOverwrite(t, d, "content-1")
	key := versionKeyFor(t, store, blob, "content-0")
	v, err := store.GetVersion("s1", "f1", key)
	if err != nil {
		t.Fatalf("version entry: %v", err)
	}
	return d, store, blob, v
}

func readAll(t *testing.T, rc io.ReadCloser) string {
	t.Helper()
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(b)
}

func isNotFound(err error) bool {
	_, ok := err.(errtypes.IsNotFound)
	return ok
}

func isPermissionDenied(err error) bool {
	_, ok := err.(errtypes.IsPermissionDenied)
	return ok
}

func TestListRevisions_ReturnsRevisionKeys(t *testing.T) {
	d, store, _ := newVersionFixture(0)
	mustOverwrite(t, d, "c0")
	mustOverwrite(t, d, "c1")

	revs, err := d.ListRevisions(testContext(), f1Ref())
	if err != nil {
		t.Fatalf("ListRevisions: %v", err)
	}
	if len(revs) != 2 {
		t.Fatalf("revisions = %d, want 2", len(revs))
	}
	for _, r := range revs {
		entry, ok := strings.CutPrefix(r.Key, "f1.REV.")
		if !ok {
			t.Errorf("key %q is not a revision key of f1", r.Key)
			continue
		}
		v, err := store.GetVersion("s1", "f1", entry)
		if err != nil {
			t.Errorf("key %q does not name a stored entry", r.Key)
			continue
		}
		if r.Size != uint64(v.Size) || r.Etag != v.ETag || r.Mtime != uint64(v.MTime/1e9) {
			t.Errorf("revision %q metadata differs from its entry", r.Key)
		}
	}
}

func TestGetMD_RevisionKeyDescribesVersion(t *testing.T) {
	d, store, _, v := twoRevisionFixture(t)
	head, _, _ := store.GetNode("s1", "f1")

	ri, err := d.GetMD(testContext(), revisionRef("f1.REV."+v.Key), nil, nil)
	if err != nil {
		t.Fatalf("GetMD: %v", err)
	}
	if ri.GetId().GetOpaqueId() != "f1.REV."+v.Key || ri.GetId().GetSpaceId() != "s1" {
		t.Errorf("Id = %v, want the revision key in space s1", ri.GetId())
	}
	if ri.Type != provider.ResourceType_RESOURCE_TYPE_FILE {
		t.Errorf("Type = %v, want FILE", ri.Type)
	}
	if ri.Size != uint64(len("content-0")) || ri.Etag != v.ETag {
		t.Errorf("size/etag = %d/%q, want the version's %d/%q", ri.Size, ri.Etag, len("content-0"), v.ETag)
	}
	if ri.GetMtime().GetSeconds() != uint64(v.MTime/1e9) || ri.GetMtime().GetNanos() != uint32(v.MTime%1e9) {
		t.Errorf("mtime = %v, want the version's", ri.GetMtime())
	}
	if ri.GetChecksum().GetType() != provider.ResourceChecksumType_RESOURCE_CHECKSUM_TYPE_SHA1 ||
		ri.GetChecksum().GetSum() != sha1Hex([]byte("content-0")) {
		t.Errorf("checksum = %v, want the version's SHA-1", ri.GetChecksum())
	}
	if ri.MimeType != head.MimeType || ri.Name != "file.txt" || !strings.HasSuffix(ri.Path, "file.txt") {
		t.Errorf("mime/name/path = %q/%q/%q", ri.MimeType, ri.Name, ri.Path)
	}
	if ri.GetParentId().GetOpaqueId() != "root" {
		t.Errorf("ParentId = %v, want root", ri.GetParentId())
	}
	if !ri.GetPermissionSet().GetStat() {
		t.Error("PermissionSet must be set")
	}
}

func TestDownload_RevisionKeyServesVersion(t *testing.T) {
	t.Run("get", func(t *testing.T) {
		d, _, _, v := twoRevisionFixture(t)
		ri, rc, err := d.Download(testContext(), revisionRef("f1.REV."+v.Key), nil)
		if err != nil {
			t.Fatalf("Download: %v", err)
		}
		if got := readAll(t, rc); got != "content-0" {
			t.Errorf("body = %q, want content-0", got)
		}
		if ri.GetId().GetOpaqueId() != "f1.REV."+v.Key {
			t.Errorf("Id = %v, want the revision key (the dataprovider writes Id.StorageId)", ri.GetId())
		}
		if ri.Path != "file.txt" || ri.GetChecksum().GetSum() != sha1Hex([]byte("content-0")) {
			t.Errorf("path/checksum = %q/%v", ri.Path, ri.GetChecksum())
		}
	})
	t.Run("head", func(t *testing.T) {
		d, _, blob, v := twoRevisionFixture(t)
		blob.mu.Lock()
		delete(blob.blobs, BlobKey("s1", v.BlobID))
		blob.mu.Unlock()
		ri, rc, err := d.Download(testContext(), revisionRef("f1.REV."+v.Key), func(*provider.ResourceInfo) bool { return false })
		if err != nil || rc != nil || ri == nil {
			t.Fatalf("HEAD-style download: ri=%v rc=%v err=%v (must not touch the blob store)", ri, rc, err)
		}
	})
}

func TestDownloadRevision_AcceptsRevisionAndBareKeys(t *testing.T) {
	for _, form := range []string{"bare", "revision"} {
		t.Run(form, func(t *testing.T) {
			d, _, _, v := twoRevisionFixture(t)
			key := v.Key
			if form == "revision" {
				key = "f1.REV." + v.Key
			}
			ri, rc, err := d.DownloadRevision(testContext(), f1Ref(), key, nil)
			if err != nil {
				t.Fatalf("DownloadRevision(%s): %v", form, err)
			}
			if got := readAll(t, rc); got != "content-0" {
				t.Errorf("body = %q, want content-0", got)
			}
			if ri.GetId().GetOpaqueId() != "f1.REV."+v.Key {
				t.Errorf("Id = %v, want the revision key", ri.GetId())
			}
		})
	}
}

func TestRestoreRevision_AcceptsRevisionAndBareKeys(t *testing.T) {
	for _, form := range []string{"bare", "revision"} {
		t.Run(form, func(t *testing.T) {
			d, _, _, v := twoRevisionFixture(t)
			key := v.Key
			if form == "revision" {
				key = "f1.REV." + v.Key
			}
			if err := d.RestoreRevision(testContext(), f1Ref(), key); err != nil {
				t.Fatalf("RestoreRevision(%s): %v", form, err)
			}
			if got := readHead(t, d); got != "content-0" {
				t.Errorf("head = %q, want content-0", got)
			}
		})
	}
}

func TestRestoreRevision_RevisionKeyAtLimit_KeepsRestoredEntry(t *testing.T) {
	d, store, blob := newVersionFixture(3)
	for _, c := range []string{"c0", "c1", "c2"} {
		mustOverwrite(t, d, c)
	}
	oldKey := versionKeyFor(t, store, blob, "original")

	if err := d.RestoreRevision(testContext(), f1Ref(), "f1.REV."+oldKey); err != nil {
		t.Fatalf("restore: %v", err)
	}

	if _, err := store.GetVersion("s1", "f1", oldKey); err != nil {
		t.Error("the restored entry must stay listed when restored by revision key")
	}
	if versions, _ := store.ListVersions("s1", "f1"); len(versions) != 3 {
		t.Errorf("versions = %d, want 3", len(versions))
	}
	if got := readHead(t, d); got != "original" {
		t.Errorf("head = %q, want original", got)
	}
}

// mismatchFixture adds a second file f2 and one version entry per file.
func mismatchFixture(t *testing.T) (*kvfsDriver, *mockMetadataStore) {
	t.Helper()
	d, store, blob := newVersionFixture(0)
	store.nodes["s1.f2"] = &NodeEntry{
		ID: "f2", SpaceID: "s1", ParentID: "root", Name: "other.txt", Type: NodeTypeFile,
		BlobID: "f2-blob", BlobSize: 5, Size: 5, MTime: 1000, ETag: "f2-etag",
		Owner: "test-user-id", MimeType: "text/plain",
	}
	store.nodeRevs["s1.f2"] = 1
	store.children["s1.root"]["other.txt"] = "f2"
	store.versions = append(store.versions,
		&VersionEntry{Key: "v1", NodeID: "f1", SpaceID: "s1", BlobID: "v1-blob", Size: 2, MTime: 500, ETag: "v1-etag"},
		&VersionEntry{Key: "v2", NodeID: "f2", SpaceID: "s1", BlobID: "v2-blob", Size: 2, MTime: 500, ETag: "v2-etag"})
	blob.blobs[BlobKey("s1", "v1-blob")] = []byte("v1")
	blob.blobs[BlobKey("s1", "v2-blob")] = []byte("v2")
	return d, store
}

func TestRevisionKey_NodeMismatchNotFound(t *testing.T) {
	d, store := mismatchFixture(t)
	ctx := testContext()

	if _, _, err := d.DownloadRevision(ctx, f1Ref(), "f2.REV.v2", nil); !isNotFound(err) {
		t.Errorf("DownloadRevision(f1, f2.REV.v2) = %v, want NotFound", err)
	}
	if _, _, err := d.DownloadRevision(ctx, f1Ref(), "f2.REV.v1", nil); !isNotFound(err) {
		t.Errorf("DownloadRevision(f1, f2.REV.v1) = %v, want NotFound (no aliasing through another node)", err)
	}
	if err := d.RestoreRevision(ctx, f1Ref(), "f2.REV.v1"); !isNotFound(err) {
		t.Errorf("RestoreRevision(f1, f2.REV.v1) = %v, want NotFound", err)
	}
	if head, _, _ := store.GetNode("s1", "f1"); head.BlobID != "old-blob" {
		t.Error("a rejected restore must leave the head unchanged")
	}
	if _, err := d.GetMD(ctx, revisionRef("f2.REV.v1"), nil, nil); !isNotFound(err) {
		t.Errorf("GetMD(f2.REV.v1) = %v, want NotFound", err)
	}
	if _, _, err := d.Download(ctx, revisionRef("f2.REV.v1"), nil); !isNotFound(err) {
		t.Errorf("Download(f2.REV.v1) = %v, want NotFound", err)
	}

	if _, err := d.GetMD(ctx, revisionRef("f1.REV.v1"), nil, nil); err != nil {
		t.Errorf("GetMD(f1.REV.v1): %v", err)
	}
	_, rc, err := d.Download(ctx, revisionRef("f1.REV.v1"), nil)
	if err != nil {
		t.Fatalf("Download(f1.REV.v1): %v", err)
	}
	if got := readAll(t, rc); got != "v1" {
		t.Errorf("Download(f1.REV.v1) = %q, want v1", got)
	}
}

func TestRevisionKey_MalformedOrUnknownNotFound(t *testing.T) {
	d, _ := mismatchFixture(t)
	ctx := testContext()

	for _, key := range []string{".REV.v1", "f1.REV.", ".REV.", "f1.REV.missing", "missing", "f1.REV.v1.x", "f1.REV.*"} {
		if _, _, err := d.DownloadRevision(ctx, f1Ref(), key, nil); !isNotFound(err) {
			t.Errorf("DownloadRevision(%q) = %v, want NotFound", key, err)
		}
		if err := d.RestoreRevision(ctx, f1Ref(), key); !isNotFound(err) {
			t.Errorf("RestoreRevision(%q) = %v, want NotFound", key, err)
		}
		if !strings.Contains(key, ".REV.") {
			continue
		}
		if _, err := d.GetMD(ctx, revisionRef(key), nil, nil); !isNotFound(err) {
			t.Errorf("GetMD(%q) = %v, want NotFound", key, err)
		}
		if _, _, err := d.Download(ctx, revisionRef(key), nil); !isNotFound(err) {
			t.Errorf("Download(%q) = %v, want NotFound", key, err)
		}
	}
	child := &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1.REV.v1"}, Path: "./child"}
	if _, err := d.GetMD(ctx, child, nil, nil); !isNotFound(err) {
		t.Errorf("GetMD(revision key + child path) = %v, want NotFound", err)
	}
}

func TestRevisionKey_Permissions(t *testing.T) {
	grant := func(store *mockMetadataStore, node string, rp *provider.ResourcePermissions) {
		store.nodes["s1."+node].Grants = map[string]*GrantEntry{
			"u:bob": {GranteeType: "user", GranteeID: "bob", Permissions: permissionsToUint32(rp)},
		}
	}
	bob := testContextWithUser("bob", nil)

	t.Run("no ListFileVersions", func(t *testing.T) {
		d, store := mismatchFixture(t)
		grant(store, "root", &provider.ResourcePermissions{Stat: true, InitiateFileDownload: true, ListContainer: true})
		if _, err := d.GetMD(bob, revisionRef("f1.REV.v1"), nil, nil); !isPermissionDenied(err) {
			t.Errorf("GetMD = %v, want PermissionDenied", err)
		}
		if _, _, err := d.Download(bob, revisionRef("f1.REV.v1"), nil); !isPermissionDenied(err) {
			t.Errorf("Download = %v, want PermissionDenied", err)
		}
	})
	t.Run("versions without download", func(t *testing.T) {
		d, store := mismatchFixture(t)
		grant(store, "root", &provider.ResourcePermissions{Stat: true, ListFileVersions: true})
		if _, err := d.GetMD(bob, revisionRef("f1.REV.v1"), nil, nil); err != nil {
			t.Errorf("GetMD = %v, want allowed", err)
		}
		if _, _, err := d.Download(bob, revisionRef("f1.REV.v1"), nil); !isPermissionDenied(err) {
			t.Errorf("Download = %v, want PermissionDenied", err)
		}
	})
	t.Run("versions with download, no restore", func(t *testing.T) {
		d, store := mismatchFixture(t)
		grant(store, "root", &provider.ResourcePermissions{Stat: true, ListFileVersions: true, InitiateFileDownload: true})
		if _, err := d.GetMD(bob, revisionRef("f1.REV.v1"), nil, nil); err != nil {
			t.Errorf("GetMD = %v, want allowed", err)
		}
		if _, _, err := d.Download(bob, revisionRef("f1.REV.v1"), nil); err != nil {
			t.Errorf("Download = %v, want allowed", err)
		}
		if err := d.RestoreRevision(bob, f1Ref(), "f1.REV.v1"); !isPermissionDenied(err) {
			t.Errorf("RestoreRevision = %v, want PermissionDenied", err)
		}
	})
	t.Run("stranger", func(t *testing.T) {
		d, _ := mismatchFixture(t)
		stranger := testContextWithUser("mallory", nil)
		if _, err := d.GetMD(stranger, revisionRef("f1.REV.v1"), nil, nil); !isNotFound(err) {
			t.Errorf("GetMD = %v, want NotFound", err)
		}
		if _, _, err := d.Download(stranger, revisionRef("f1.REV.v1"), nil); !isNotFound(err) {
			t.Errorf("Download = %v, want NotFound", err)
		}
	})
	t.Run("grant on another file", func(t *testing.T) {
		d, store := mismatchFixture(t)
		grant(store, "f2", &provider.ResourcePermissions{Stat: true, ListFileVersions: true, InitiateFileDownload: true})
		if _, _, err := d.Download(bob, revisionRef("f1.REV.v1"), nil); !isNotFound(err) {
			t.Errorf("Download(f1.REV.v1) = %v, want NotFound (bob holds f2 only)", err)
		}
		_, rc, err := d.Download(bob, revisionRef("f2.REV.v2"), nil)
		if err != nil {
			t.Fatalf("Download(f2.REV.v2): %v", err)
		}
		if got := readAll(t, rc); got != "v2" {
			t.Errorf("Download(f2.REV.v2) = %q, want v2", got)
		}
	})
}

func TestRestoreRevision_EventKeyIsKeyAsReceived(t *testing.T) {
	for _, key := range []string{"ver-key-1", "file-1.REV.ver-key-1"} {
		t.Run(key, func(t *testing.T) {
			store := newMockMetadataStore()
			blob := newMockBlobStore()
			stream := newMockStream()
			d := testDriverWithStream(store, blob, stream)
			setupSpaceWithFile(store, "space-1", "root-1", "file-1", "versioned.txt")
			store.versions = append(store.versions, &VersionEntry{
				Key: "ver-key-1", NodeID: "file-1", SpaceID: "space-1",
				BlobID: "ver-blob-1", BlobSize: 50, Size: 50, MTime: 500, ETag: "ver-etag",
			})
			ref := &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "file-1"}}

			if err := d.RestoreRevision(eventTestContext(), ref, key); err != nil {
				t.Fatalf("RestoreRevision(%q): %v", key, err)
			}
			if stream.eventCount() != 1 {
				t.Fatalf("events = %d, want 1", stream.eventCount())
			}
			fvr, ok := stream.lastEvent().payload.(revaevents.FileVersionRestored)
			if !ok || fvr.Key != key {
				t.Errorf("event = %#v, want FileVersionRestored with key %q", stream.lastEvent().payload, key)
			}
		})
	}
}
