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
	"errors"
	"testing"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
)

var errKVDown = errors.New("kv unavailable")

// failingStore fails chosen reads and session writes, as a KV outage would.
type failingStore struct {
	*mockMetadataStore
	failNode       string // GetNode of this node fails
	failChildrenOf string // GetChildren of this node fails
	failPutUpload  bool
	failPutVersion bool
}

func (s *failingStore) GetNode(spaceID, nodeID string) (*NodeEntry, uint64, error) {
	if nodeID == s.failNode {
		return nil, 0, errKVDown
	}
	return s.mockMetadataStore.GetNode(spaceID, nodeID)
}

func (s *failingStore) GetChildren(spaceID, parentID string) (ChildMap, uint64, error) {
	if parentID == s.failChildrenOf {
		return nil, 0, errKVDown
	}
	return s.mockMetadataStore.GetChildren(spaceID, parentID)
}

func (s *failingStore) PutUpload(u *UploadSession) error {
	if s.failPutUpload {
		return errKVDown
	}
	return s.mockMetadataStore.PutUpload(u)
}

func (s *failingStore) PutVersion(v *VersionEntry) error {
	if s.failPutVersion {
		return errKVDown
	}
	return s.mockMetadataStore.PutVersion(v)
}

// InitiateUpload never hands out a session it has not authorized, and only a missing parent of
// a non-empty upload, which can be read lag after a fresh MKCOL, still gets the simple protocol:
// its commit resolves and authorizes again.
func TestInitiateUpload_FailsClosed(t *testing.T) {
	notFound := func(err error) bool { _, ok := err.(errtypes.IsNotFound); return ok }
	badRequest := func(err error) bool { _, ok := err.(errtypes.IsBadRequest); return ok }
	precondition := func(err error) bool { _, ok := err.(errtypes.IsPreconditionFailed); return ok }
	kvDown := func(err error) bool { return errors.Is(err, errKVDown) }
	for _, c := range []struct {
		name   string
		path   string
		fail   func(*failingStore)
		length int64
		want   func(error) bool // nil: the simple protocol only
	}{
		{"parent read error", "./x.txt", func(s *failingStore) { s.failNode = "root" }, 5, kvDown},
		{"parent read error, empty", "./x.txt", func(s *failingStore) { s.failNode = "root" }, 0, kvDown},
		{"path read error", "./sub/x.txt", func(s *failingStore) { s.failChildrenOf = "root" }, 5, kvDown},
		{"missing parent folder", "./missing/x.txt", nil, 5, nil},
		{"missing parent folder, empty", "./missing/x.txt", nil, 0, notFound},
		{"invalid name", "./..", nil, 5, badRequest},
		{"the space root", ".", nil, 5, badRequest},
		{"parent is a file", "./file.txt/x", nil, 5, precondition},
		{"session write error", "./x.txt", func(s *failingStore) { s.failPutUpload = true }, 5, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			store := newMockMetadataStore()
			setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
			fs := &failingStore{mockMetadataStore: store}
			if c.fail != nil {
				c.fail(fs)
			}
			d := testDriver(store, newMockBlobStore())
			d.store = fs

			ref := &provider.Reference{ResourceId: &provider.ResourceId{StorageId: "st", SpaceId: "s1", OpaqueId: "root"}, Path: c.path}
			res, err := d.InitiateUpload(testContext(), ref, c.length, map[string]string{})
			if c.want == nil {
				if err != nil || res["simple"] == "" || res["tus"] != "" {
					t.Errorf("InitiateUpload = %v, %v; want the simple protocol only", res, err)
				}
			} else if !c.want(err) {
				t.Errorf("InitiateUpload = %v, %v (%T); want it refused", res, err, err)
			}
			if n := len(store.uploads); n != 0 {
				t.Errorf("%d upload session(s) created, want none", n)
			}
		})
	}
}
