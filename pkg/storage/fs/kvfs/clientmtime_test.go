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
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/opencloud-eu/reva/v2/pkg/storage"
)

const y2001 = int64(1000000000) * int64(time.Second) // 2001-09-09T01:46:40Z

func rootChildRef(name string) *provider.Reference {
	return &provider.Reference{ResourceId: &provider.ResourceId{StorageId: "st", SpaceId: "s1", OpaqueId: "root"}, Path: "./" + name}
}

// simplePut uploads body through the simple protocol, as ocdav's PUT does.
func simplePut(t *testing.T, d *kvfsDriver, name, body string, md map[string]string) error {
	t.Helper()
	res, err := d.InitiateUpload(testContext(), rootChildRef(name), int64(len(body)), md)
	if err != nil {
		return err
	}
	_, err = d.Upload(testContext(), storage.UploadRequest{
		Ref:    &provider.Reference{Path: transportSimpleID(t, res["simple"])},
		Body:   io.NopCloser(strings.NewReader(body)),
		Length: int64(len(body)),
	}, nil)
	return err
}

// tusPut uploads body through a TUS session, as ocdav's TUS handler does.
func tusPut(t *testing.T, d *kvfsDriver, name, body string, md map[string]string) error {
	t.Helper()
	ctx := testContext()
	res, err := d.InitiateUpload(ctx, rootChildRef(name), int64(len(body)), md)
	if err != nil {
		return err
	}
	up, err := d.GetUpload(ctx, res["tus"])
	if err != nil {
		return err
	}
	if _, err := up.WriteChunk(ctx, 0, strings.NewReader(body)); err != nil {
		return err
	}
	return up.FinishUpload(ctx)
}

func rootChild(t *testing.T, store *mockMetadataStore, name string) *NodeEntry {
	t.Helper()
	children, _, _ := store.GetChildren("s1", "root")
	n, _, err := store.GetNode("s1", children[name])
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return n
}

func TestUpload_KeepsTheClientDate(t *testing.T) {
	for _, proto := range []struct {
		name string
		put  func(*testing.T, *kvfsDriver, string, string, map[string]string) error
	}{{"simple", simplePut}, {"tus", tusPut}} {
		t.Run(proto.name, func(t *testing.T) {
			store := newMockMetadataStore()
			d := testDriver(store, newMockBlobStore())
			setupSpaceEmpty(store, "s1", "root")
			before := time.Now().UnixNano()

			if err := proto.put(t, d, "a.txt", "one", map[string]string{"mtime": "1000000000"}); err != nil {
				t.Fatalf("create: %v", err)
			}
			first := rootChild(t, store, "a.txt")
			if first.MTime != y2001 {
				t.Errorf("created file MTime = %d, want %d", first.MTime, y2001)
			}
			if root, _, _ := store.GetNode("s1", "root"); root.MTime < before {
				t.Errorf("parent MTime = %d, want the propagation time (>= %d)", root.MTime, before)
			}

			if err := proto.put(t, d, "a.txt", "two", map[string]string{"mtime": "1000000000"}); err != nil {
				t.Fatalf("overwrite: %v", err)
			}
			second := rootChild(t, store, "a.txt")
			if second.MTime != y2001 || second.ETag == first.ETag {
				t.Errorf("overwrite with the same date: MTime %d, ETag %q (was %q); want %d and a new etag", second.MTime, second.ETag, first.ETag, y2001)
			}

			// The web sends fractional seconds; the fraction is read as nanoseconds, as in decomposedfs.
			if err := proto.put(t, d, "a.txt", "three", map[string]string{"mtime": "1756889140.565"}); err != nil {
				t.Fatalf("overwrite: %v", err)
			}
			if got := rootChild(t, store, "a.txt").MTime; got != 1756889140*int64(time.Second)+565 {
				t.Errorf("fractional date: MTime = %d", got)
			}
		})
	}
}

func TestUpload_UnusableClientDateFallsBackToCommitTime(t *testing.T) {
	for _, v := range []string{"", "null", "abc", "-5", "12.x", "1.5e9", "9223372037", "9223372036.854775808"} {
		store := newMockMetadataStore()
		d := testDriver(store, newMockBlobStore())
		setupSpaceEmpty(store, "s1", "root")
		before := time.Now().UnixNano()
		if err := simplePut(t, d, "a.txt", "x", map[string]string{"mtime": v}); err != nil {
			t.Fatalf("mtime %q: %v", v, err)
		}
		if got := rootChild(t, store, "a.txt").MTime; got < before || got > time.Now().UnixNano() {
			t.Errorf("mtime %q: MTime = %d, want the commit time", v, got)
		}
	}
}

func TestTouchFile_NewFileKeepsTheClientDate(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")
	if err := d.TouchFile(testContext(), rootChildRef("empty.txt"), false, "1000000000"); err != nil {
		t.Fatalf("TouchFile: %v", err)
	}
	if got := rootChild(t, store, "empty.txt").MTime; got != y2001 {
		t.Errorf("MTime = %d, want %d", got, y2001)
	}
}

// Version entries are trimmed by when they were taken, not by the client's dates: saves with
// descending dates keep the newest snapshots.
func TestTrim_DescendingClientDatesKeepTheNewestSnapshots(t *testing.T) {
	d, store, blob := newVersionFixture(3)
	for i, c := range []string{"c1", "c2", "c3", "c4", "c5"} {
		mtime := strconv.FormatInt(1000000000-int64(i)*86400, 10)
		if err := simplePut(t, d, "file.txt", c, map[string]string{"mtime": mtime}); err != nil {
			t.Fatalf("save %s: %v", c, err)
		}
	}
	got := versionContents(t, store, blob)
	if len(got) != 3 || !got["c2"] || !got["c3"] || !got["c4"] {
		t.Errorf("kept versions = %v, want c2, c3, c4", got)
	}
}

// A restore's snapshot of the replaced head is the newest entry, whatever the head's date.
func TestRestore_SnapshotOfTheReplacedHeadIsNewest(t *testing.T) {
	d, store, blob := newVersionFixture(2)
	for _, s := range []struct{ body, mtime string }{{"c1", "1000000000"}, {"c2", "900000000"}} {
		if err := simplePut(t, d, "file.txt", s.body, map[string]string{"mtime": s.mtime}); err != nil {
			t.Fatalf("save %s: %v", s.body, err)
		}
	}
	if err := d.RestoreRevision(testContext(), f1Ref(), "f1.REV."+versionKeyFor(t, store, blob, "original")); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got := versionContents(t, store, blob)
	if len(got) != 2 || !got["original"] || !got["c2"] {
		t.Errorf("kept versions = %v, want original (restored) and c2 (replaced head)", got)
	}
}

func TestParseClientMTime(t *testing.T) {
	for _, c := range []struct {
		in string
		ns int64
		ok bool
	}{
		{"1000000000", y2001, true},
		{"1756889140.565", 1756889140*int64(time.Second) + 565, true},
		{"0", 0, true},
		{"9223372036.854775807", math.MaxInt64, true},
		{"", 0, false},
		{"null", 0, false},
		{"abc", 0, false},
		{"-5", 0, false},
		{"12.x", 0, false},
		{"1.5e9", 0, false},
		{"9223372037", 0, false},
		{"9223372036.854775808", 0, false},
	} {
		ns, ok := parseClientMTime(c.in)
		if ns != c.ns || ok != c.ok {
			t.Errorf("parseClientMTime(%q) = %d, %v; want %d, %v", c.in, ns, ok, c.ns, c.ok)
		}
	}
}

// Entries without a SnapshotTime order by their MTime, the commit time when they were written.
func TestTrimVersions_EntriesWithAndWithoutSnapshotTime(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	d.opts.MaxVersions = 2
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
	store.versions = append(store.versions,
		&VersionEntry{Key: "old-1", NodeID: "f1", SpaceID: "s1", MTime: 1000},
		&VersionEntry{Key: "old-2", NodeID: "f1", SpaceID: "s1", MTime: 2000},
		&VersionEntry{Key: "new-1", NodeID: "f1", SpaceID: "s1", MTime: 500, SnapshotTime: 3000},
		&VersionEntry{Key: "new-2", NodeID: "f1", SpaceID: "s1", MTime: 100, SnapshotTime: 4000},
	)
	d.trimVersions(context.Background(), "s1", "f1")
	versions, _ := store.ListVersions("s1", "f1")
	var keys []string
	for _, v := range versions {
		keys = append(keys, v.Key)
	}
	if len(keys) != 2 || !strings.Contains(strings.Join(keys, ","), "new-1") || !strings.Contains(strings.Join(keys, ","), "new-2") {
		t.Errorf("kept %v, want new-1 and new-2", keys)
	}
}

// The new fields are omitted when empty, so entries without them encode as before, and pods
// with and without the fields read each other's entries.
func TestMsgpack_ClientDateFieldsCompatible(t *testing.T) {
	type earlierVersionEntry struct {
		Key      string `msgpack:"k"`
		NodeID   string `msgpack:"nid"`
		SpaceID  string `msgpack:"sid"`
		BlobID   string `msgpack:"bid"`
		BlobSize int64  `msgpack:"bs"`
		MTime    int64  `msgpack:"mt"`
		ETag     string `msgpack:"et"`
		Size     int64  `msgpack:"sz"`
		Checksum string `msgpack:"cksum"`
	}
	type earlierUploadSession struct {
		ID             string            `msgpack:"id"`
		SpaceID        string            `msgpack:"sid"`
		NodeID         string            `msgpack:"nid"`
		Filename       string            `msgpack:"fn"`
		ParentID       string            `msgpack:"pid"`
		Size           int64             `msgpack:"sz"`
		Offset         int64             `msgpack:"off"`
		Storage        map[string]string `msgpack:"meta"`
		Expires        int64             `msgpack:"exp"`
		BlobID         string            `msgpack:"bid"`
		S3MultipartID  string            `msgpack:"s3mid"`
		Parts          []PartInfo        `msgpack:"parts"`
		SizeIsDeferred bool              `msgpack:"sdef"`
		IfMatchEtag    string            `msgpack:"ifm"`
		OwnerID        string            `msgpack:"own"`
	}
	roundTrip := func(t *testing.T, from, into interface{}) {
		t.Helper()
		b, err := msgpack.Marshal(from)
		if err != nil {
			t.Fatalf("marshal %T: %v", from, err)
		}
		if err := msgpack.Unmarshal(b, into); err != nil {
			t.Fatalf("unmarshal %T into %T: %v", from, into, err)
		}
	}
	sameBytes := func(t *testing.T, a, b interface{}) {
		t.Helper()
		ba, _ := msgpack.Marshal(a)
		bb, _ := msgpack.Marshal(b)
		if string(ba) != string(bb) {
			t.Errorf("%T without the new field encodes differently from %T", a, b)
		}
	}

	v := VersionEntry{Key: "k", NodeID: "n", SpaceID: "s", BlobID: "b", BlobSize: 3, MTime: 7, ETag: "e", Size: 3, Checksum: "sha1:x"}
	ev := earlierVersionEntry{Key: "k", NodeID: "n", SpaceID: "s", BlobID: "b", BlobSize: 3, MTime: 7, ETag: "e", Size: 3, Checksum: "sha1:x"}
	sameBytes(t, v, ev)
	withTime := v
	withTime.SnapshotTime = 99
	var gotEarlier earlierVersionEntry
	roundTrip(t, withTime, &gotEarlier)
	if gotEarlier != ev {
		t.Errorf("an earlier pod reads %+v, want %+v", gotEarlier, ev)
	}
	var gotNew VersionEntry
	roundTrip(t, ev, &gotNew)
	if gotNew != v {
		t.Errorf("an entry without SnapshotTime reads as %+v, want %+v", gotNew, v)
	}

	u := UploadSession{ID: "u", SpaceID: "s", Filename: "f", ParentID: "p", Size: 5, Storage: map[string]string{}, OwnerID: "o", IfMatchEtag: "e"}
	eu := earlierUploadSession{ID: "u", SpaceID: "s", Filename: "f", ParentID: "p", Size: 5, Storage: map[string]string{}, OwnerID: "o", IfMatchEtag: "e"}
	sameBytes(t, u, eu)
	withDate := u
	withDate.ClientMTime = "1000000000"
	var gotEarlierU earlierUploadSession
	roundTrip(t, withDate, &gotEarlierU)
	if !reflect.DeepEqual(gotEarlierU, eu) {
		t.Errorf("an earlier pod reads %+v, want %+v", gotEarlierU, eu)
	}
	var gotNewU UploadSession
	roundTrip(t, withDate, &gotNewU)
	if gotNewU.ClientMTime != "1000000000" {
		t.Errorf("ClientMTime after a round trip = %q", gotNewU.ClientMTime)
	}
}
