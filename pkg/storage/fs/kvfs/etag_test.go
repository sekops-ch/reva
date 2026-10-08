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
	"testing"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
	"github.com/opencloud-eu/reva/v2/pkg/utils"
)

func quoted(etag string) string { return `"` + etag + `"` }

// Etags are stored bare and handed out in double quotes, as decomposedfs hands them out.
func TestEtags_HandedOutQuoted(t *testing.T) {
	d, store, _, v := twoRevisionFixture(t)
	ctx := testContext()
	head, _, _ := store.GetNode("s1", "f1")

	ri, err := d.GetMD(ctx, f1Ref(), nil, nil)
	if err != nil || ri.Etag != quoted(head.ETag) {
		t.Errorf("GetMD etag = %q (err %v), want %q", ri.GetEtag(), err, quoted(head.ETag))
	}
	entries, err := d.ListFolder(ctx, &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"}}, nil, nil)
	if err != nil || len(entries) != 1 || entries[0].Etag != quoted(head.ETag) {
		t.Errorf("ListFolder = %v (err %v), want one entry with etag %q", entries, err, quoted(head.ETag))
	}
	revs, err := d.ListRevisions(ctx, f1Ref())
	if err != nil {
		t.Fatalf("ListRevisions: %v", err)
	}
	for _, r := range revs {
		if len(r.Etag) < 2 || r.Etag[0] != '"' || r.Etag[len(r.Etag)-1] != '"' {
			t.Errorf("revision %s etag = %q, want it quoted", r.Key, r.Etag)
		}
	}
	if ri, err := d.GetMD(ctx, revisionRef("f1.REV."+v.Key), nil, nil); err != nil || ri.Etag != quoted(v.ETag) {
		t.Errorf("version etag = %q (err %v), want %q", ri.GetEtag(), err, quoted(v.ETag))
	}
	spaces, err := d.ListStorageSpaces(ctx, nil, false)
	if err != nil || len(spaces) != 1 {
		t.Fatalf("ListStorageSpaces = %v, %v", spaces, err)
	}
	root, _, _ := store.GetNode("s1", "root")
	if got := utils.ReadPlainFromOpaque(spaces[0].GetOpaque(), "etag"); got != quoted(root.ETag) {
		t.Errorf("space etag = %q, want %q", got, quoted(root.ETag))
	}
	if head.ETag[0] == '"' {
		t.Errorf("stored etag %q is quoted, want it bare", head.ETag)
	}
}

// If-Match matches an etag quoted or bare; anything else, "*" and weak etags included, fails as
// it always did.
func TestIfMatch_QuotedOrBare(t *testing.T) {
	for _, c := range []struct {
		name    string
		ifMatch func(etag string) string
		ok      bool
	}{
		{"quoted", quoted, true},
		{"bare", func(e string) string { return e }, true},
		{"quoted stale", func(string) string { return `"0123456789abcdef"` }, false},
		{"empty quotes", func(string) string { return `""` }, false},
		{"star", func(string) string { return "*" }, false},
		{"weak", func(e string) string { return "W/" + quoted(e) }, false},
		{"list", func(e string) string { return quoted(e) + ", " + quoted("x") }, false},
	} {
		for _, proto := range []struct {
			name string
			put  func(*testing.T, *kvfsDriver, string, string, map[string]string) error
		}{{"simple", simplePut}, {"tus", tusPut}, {"empty", func(t *testing.T, d *kvfsDriver, name, _ string, md map[string]string) error {
			_, err := d.InitiateUpload(testContext(), rootChildRef(name), 0, md)
			return err
		}}} {
			t.Run(c.name+"/"+proto.name, func(t *testing.T) {
				d, store, _ := newVersionFixture(0)
				etag := store.nodes["s1.f1"].ETag
				err := proto.put(t, d, "file.txt", "fresh", map[string]string{"if-match": c.ifMatch(etag)})
				if c.ok && err != nil {
					t.Fatalf("If-Match %q: %v", c.ifMatch(etag), err)
				}
				if !c.ok {
					if _, aborted := err.(errtypes.IsAborted); !aborted {
						t.Fatalf("If-Match %q = %v, want Aborted", c.ifMatch(etag), err)
					}
					if store.nodes["s1.f1"].ETag != etag {
						t.Error("a refused upload changed the file")
					}
				}
			})
		}
	}
}

func TestEtagHelpers(t *testing.T) {
	for _, c := range []struct {
		ifMatch, stored string
		match           bool
	}{
		{`"abc"`, "abc", true},
		{"abc", "abc", true},
		{`"abc"`, `"abc"`, true},
		{`"abd"`, "abc", false},
		{`""`, "abc", false},
		{"*", "abc", false},
		{`W/"abc"`, "abc", false},
		{`"abc", "def"`, "abc", false},
		{`""abc""`, "abc", false},
		{`"abc`, "abc", false},
	} {
		if got := etagMatches(c.ifMatch, c.stored); got != c.match {
			t.Errorf("etagMatches(%q, %q) = %v, want %v", c.ifMatch, c.stored, got, c.match)
		}
	}
	for in, want := range map[string]string{"": "", "abc": `"abc"`} {
		if got := quotedEtag(in); got != want {
			t.Errorf("quotedEtag(%q) = %q, want %q", in, got, want)
		}
	}
	for in, want := range map[string]string{`"abc"`: "abc", "abc": "abc", `""`: `""`, "*": "*", `W/"abc"`: `W/"abc"`, "": ""} {
		if got := storedIfMatch(in); got != want {
			t.Errorf("storedIfMatch(%q) = %q, want %q", in, got, want)
		}
	}
}

// The simple-upload id and the TUS session carry If-Match bare, so pods that compare raw values
// still match it while both run.
func TestIfMatch_TravelsBare(t *testing.T) {
	d, store, _ := newVersionFixture(0)
	etag := store.nodes["s1.f1"].ETag
	res, err := d.InitiateUpload(testContext(), rootChildRef("file.txt"), 5, map[string]string{"if-match": quoted(etag)})
	if err != nil {
		t.Fatalf("InitiateUpload: %v", err)
	}
	if _, params, err := d.parseUploadPath(transportSimpleID(t, res["simple"])); err != nil || params.IfMatch != etag {
		t.Errorf("simple id If-Match = %q (err %v), want %q", params.IfMatch, err, etag)
	}
	if s := store.uploads[res["tus"]]; s == nil || s.IfMatchEtag != etag {
		t.Errorf("session If-Match = %+v, want %q", s, etag)
	}
}
