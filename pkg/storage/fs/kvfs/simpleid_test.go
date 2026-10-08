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
	"net/url"
	"path"
	"strings"
	"testing"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
	"github.com/opencloud-eu/reva/v2/pkg/storage"
)

// transportSimpleID carries a simple-upload id the way the storage provider and the data
// server do: joined into the data URL's path, then decoded from the request path.
func transportSimpleID(t *testing.T, id string) string {
	t.Helper()
	u := url.URL{Scheme: "https", Host: "data", Path: path.Join("/data/simple", id)}
	got, err := url.Parse(u.String())
	if err != nil {
		t.Fatalf("parse %q: %v", u.String(), err)
	}
	return strings.TrimPrefix(got.Path, "/data/simple")
}

var awkwardNames = []string{
	"plain.txt", "What?.txt", "a&b=c.txt", "100%.txt", "#tag.txt", "sp ace+plus.txt",
	"über?.txt", "end?", "?start", "x??y", "q?if-match=evil&tus-sibling=evil",
}

func TestSimpleUploadID_RoundTrip(t *testing.T) {
	d := testDriver(newMockMetadataStore(), newMockBlobStore())
	for _, name := range awkwardNames {
		for _, p := range []simpleUploadParams{
			{},
			{IfMatch: `"0123abcd"`},
			{IfMatch: "0123abcd", TUSSibling: "6f1c2c1e-1b2f-4c55-9c39-3d8f9a7d2b10"},
			{TUSSibling: "6f1c2c1e-1b2f-4c55-9c39-3d8f9a7d2b10", MTime: "1756889140.565"},
		} {
			id := encodeSimpleUploadID("st$s1!root", "./sub/"+name, p)
			ref, got, err := d.parseUploadPath(transportSimpleID(t, id))
			if err != nil {
				t.Errorf("%q: %v", id, err)
				continue
			}
			rid := ref.GetResourceId()
			if rid.GetStorageId() != "st" || rid.GetSpaceId() != "s1" || rid.GetOpaqueId() != "root" || ref.GetPath() != "./sub/"+name {
				t.Errorf("%q: ref = %v, want st$s1!root ./sub/%s", id, ref, name)
			}
			if got != p {
				t.Errorf("%q: params = %+v, want %+v", id, got, p)
			}
		}
	}
}

// Ids minted before every id carried a query still parse.
func TestSimpleUploadID_EarlierFormatParses(t *testing.T) {
	d := testDriver(newMockMetadataStore(), newMockBlobStore())
	for id, want := range map[string]simpleUploadParams{
		"/st$s1!root/a.txt":                                  {},
		"/st$s1!root/a.txt?if-match=e1":                      {IfMatch: "e1"},
		"/st$s1!root/a.txt?tus-sibling=t1":                   {TUSSibling: "t1"},
		"/st$s1!root/a.txt?if-match=%22e1%22&tus-sibling=t1": {IfMatch: `"e1"`, TUSSibling: "t1"},
	} {
		ref, got, err := d.parseUploadPath(id)
		if err != nil || ref.GetPath() != "./a.txt" || got != want {
			t.Errorf("%q: ref %v, params %+v, err %v; want ./a.txt, %+v", id, ref, got, err, want)
		}
	}
}

// A simple upload lands under the requested name, whatever characters it holds.
func TestSimpleUpload_AwkwardNamesLandUnderTheirName(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceEmpty(store, "s1", "root")
	ctx := testContext()
	nameRef := func(name string) *provider.Reference {
		return &provider.Reference{ResourceId: &provider.ResourceId{StorageId: "st", SpaceId: "s1", OpaqueId: "root"}, Path: "./" + name}
	}
	upload := func(name, body string, md map[string]string) error {
		res, err := d.InitiateUpload(ctx, nameRef(name), int64(len(body)), md)
		if err != nil {
			return err
		}
		_, err = d.Upload(ctx, storage.UploadRequest{
			Ref:    &provider.Reference{Path: transportSimpleID(t, res["simple"])},
			Body:   io.NopCloser(strings.NewReader(body)),
			Length: int64(len(body)),
		}, nil)
		return err
	}

	for _, name := range awkwardNames {
		if err := upload(name, "body of "+name, map[string]string{}); err != nil {
			t.Errorf("upload %q: %v", name, err)
		}
	}
	children, _, _ := store.GetChildren("s1", "root")
	if len(children) != len(awkwardNames) {
		t.Errorf("children = %v, want exactly the %d uploaded names", children, len(awkwardNames))
	}
	for _, name := range awkwardNames {
		_, rc, err := d.Download(ctx, nameRef(name), nil)
		if err != nil {
			t.Errorf("download %q: %v", name, err)
			continue
		}
		if got := readAll(t, rc); got != "body of "+name {
			t.Errorf("%q holds %q", name, got)
		}
	}

	err := upload("What?.txt", "stale write", map[string]string{"if-match": "not-the-etag"})
	if _, ok := err.(errtypes.IsAborted); !ok {
		t.Errorf("stale If-Match on What?.txt = %v, want Aborted", err)
	}
	if _, rc, err := d.Download(ctx, nameRef("What?.txt"), nil); err != nil || readAll(t, rc) != "body of What?.txt" {
		t.Errorf("a refused upload changed What?.txt (err %v)", err)
	}
}
