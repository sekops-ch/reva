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
	"encoding/json"
	"encoding/xml"
	"testing"
	"time"

	user "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	types "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/opencloud-eu/reva/v2/pkg/utils"
)

func lockUser() *user.UserId {
	return &user.UserId{OpaqueId: "test-user-id", Idp: "https://idp.example"}
}

// webdavLock is a lock as ocdav's LOCK handler sets it.
func webdavLock(expires time.Time, locktime string) *provider.Lock {
	o := utils.AppendPlainToOpaque(nil, "lockownername", "Alice Example")
	o = utils.AppendPlainToOpaque(o, "locktime", locktime)
	return &provider.Lock{
		Opaque:     o,
		Type:       provider.LockType_LOCK_TYPE_EXCL,
		User:       lockUser(),
		LockId:     "urn:uuid:7c0a4b46-1c3a-4d4b-9a0e-3b9d2c1f0a11",
		Expiration: &types.Timestamp{Seconds: uint64(expires.Unix())},
	}
}

// discoveredLock decodes the lock ocdav's lock discovery reads from a resource info: the JSON
// "lock" entry of its opaque map.
func discoveredLock(t *testing.T, ri *provider.ResourceInfo) *provider.Lock {
	t.Helper()
	e := ri.GetOpaque().GetMap()["lock"]
	if e == nil {
		return nil
	}
	if e.GetDecoder() != "json" {
		t.Fatalf("lock entry decoder = %q, want json", e.GetDecoder())
	}
	l := &provider.Lock{}
	if err := json.Unmarshal(e.GetValue(), l); err != nil {
		t.Fatalf("lock entry: %v", err)
	}
	return l
}

func assertSameLock(t *testing.T, what string, got, want *provider.Lock) {
	t.Helper()
	if got == nil {
		t.Errorf("%s: no lock, want %s", what, want.GetLockId())
		return
	}
	if got.GetLockId() != want.GetLockId() || got.GetType() != want.GetType() || got.GetAppName() != want.GetAppName() ||
		got.GetUser().GetOpaqueId() != want.GetUser().GetOpaqueId() || got.GetUser().GetIdp() != want.GetUser().GetIdp() ||
		got.GetExpiration().GetSeconds() != want.GetExpiration().GetSeconds() {
		t.Errorf("%s: lock = %v, want %v", what, got, want)
	}
	for _, k := range []string{"lockownername", "locktime"} {
		if g, w := utils.ReadPlainFromOpaque(got.GetOpaque(), k), utils.ReadPlainFromOpaque(want.GetOpaque(), k); g != w {
			t.Errorf("%s: lock %s = %q, want %q", what, k, g, w)
		}
	}
}

// A lock shows in lock discovery as it was set, as in decomposedfs: ocdav renders
// <d:lockdiscovery> from the resource info's opaque "lock" entry.
func TestResourceInfo_ListsTheLockForDiscovery(t *testing.T) {
	for name, lock := range map[string]*provider.Lock{
		"webdav": webdavLock(time.Now().Add(time.Hour), "2026-10-04T08:00:00Z"),
		"app": {
			Type: provider.LockType_LOCK_TYPE_WRITE, AppName: "Collabora", User: lockUser(),
			LockId:     `{"S":"wopi-session-1","F":0}`,
			Expiration: &types.Timestamp{Seconds: uint64(time.Now().Add(30 * time.Minute).Unix())},
		},
		"no timeout": {Type: provider.LockType_LOCK_TYPE_EXCL, User: lockUser(), LockId: "urn:uuid:5b3f0c1e-9d2a-4f6e-8c7b-1a2b3c4d5e6f"},
	} {
		t.Run(name, func(t *testing.T) {
			store := newMockMetadataStore()
			d := testDriver(store, newMockBlobStore())
			setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
			ctx := testContext()
			if err := d.SetLock(ctx, f1Ref(), lock); err != nil {
				t.Fatalf("SetLock: %v", err)
			}

			ri, err := d.GetMD(ctx, f1Ref(), nil, nil)
			if err != nil {
				t.Fatalf("GetMD: %v", err)
			}
			assertSameLock(t, "GetMD", discoveredLock(t, ri), lock)
			assertSameLock(t, "GetMD ri.Lock", ri.GetLock(), lock)

			entries, err := d.ListFolder(ctx, &provider.Reference{ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"}}, nil, nil)
			if err != nil || len(entries) != 1 {
				t.Fatalf("ListFolder = %v, %v", entries, err)
			}
			assertSameLock(t, "ListFolder", discoveredLock(t, entries[0]), lock)

			got, err := d.GetLock(ctx, f1Ref())
			if err != nil {
				t.Fatalf("GetLock: %v", err)
			}
			assertSameLock(t, "GetLock", got, lock)
		})
	}
}

func TestResourceInfo_NothingToDiscoverWithoutALiveLock(t *testing.T) {
	for name, prepare := range map[string]func(*kvfsDriver, *mockMetadataStore) error{
		"never locked": func(*kvfsDriver, *mockMetadataStore) error { return nil },
		"expired": func(_ *kvfsDriver, store *mockMetadataStore) error {
			store.nodes["s1.f1"].Lock = &LockEntry{LockID: "urn:uuid:old", Type: int(provider.LockType_LOCK_TYPE_EXCL), UserID: "test-user-id", Expiry: time.Now().Add(-time.Minute).Unix()}
			return nil
		},
		"unlocked": func(d *kvfsDriver, _ *mockMetadataStore) error {
			lock := webdavLock(time.Now().Add(time.Hour), "2026-10-04T08:00:00Z")
			if err := d.SetLock(testContext(), f1Ref(), lock); err != nil {
				return err
			}
			return d.Unlock(testContext(), f1Ref(), lock)
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := newMockMetadataStore()
			d := testDriver(store, newMockBlobStore())
			setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
			if err := prepare(d, store); err != nil {
				t.Fatalf("prepare: %v", err)
			}
			ri, err := d.GetMD(testContext(), f1Ref(), nil, nil)
			if err != nil {
				t.Fatalf("GetMD: %v", err)
			}
			if l := discoveredLock(t, ri); l != nil || ri.GetLock() != nil {
				t.Errorf("lock discovery = %v (ri.Lock %v), want nothing", l, ri.GetLock())
			}
		})
	}
}

func TestRefreshLock_UpdatesWhatDiscoveryShows(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
	ctx := testContext()
	lock := webdavLock(time.Now().Add(time.Hour), "2026-10-04T08:00:00Z")
	if err := d.SetLock(ctx, f1Ref(), lock); err != nil {
		t.Fatalf("SetLock: %v", err)
	}
	// As ocdav refreshes: the same id, no existing id, a new locktime and no expiration.
	refreshed := webdavLock(time.Now(), "2026-10-04T09:00:00Z")
	refreshed.Expiration = nil
	if err := d.RefreshLock(ctx, f1Ref(), refreshed, ""); err != nil {
		t.Fatalf("RefreshLock: %v", err)
	}
	ri, err := d.GetMD(ctx, f1Ref(), nil, nil)
	if err != nil {
		t.Fatalf("GetMD: %v", err)
	}
	assertSameLock(t, "after refresh", discoveredLock(t, ri), refreshed)
}

// The new lock fields are omitted when empty, so lock entries without them encode as before, and
// pods with and without the fields read each other's entries.
func TestMsgpack_LockFieldsCompatible(t *testing.T) {
	type earlierLockEntry struct {
		LockID  string `msgpack:"lid"`
		Type    int    `msgpack:"lt"`
		UserID  string `msgpack:"uid"`
		AppName string `msgpack:"app,omitempty"`
		Expiry  int64  `msgpack:"exp,omitempty"`
	}
	bare := LockEntry{LockID: "l", Type: 2, UserID: "u", AppName: "a", Expiry: 9}
	earlier := earlierLockEntry{LockID: "l", Type: 2, UserID: "u", AppName: "a", Expiry: 9}
	bb, _ := msgpack.Marshal(bare)
	eb, _ := msgpack.Marshal(earlier)
	if string(bb) != string(eb) {
		t.Error("a lock entry without the new fields encodes differently from an earlier one")
	}

	full := bare
	full.UserIdp = "https://idp.example"
	full.Meta = map[string]string{"lockownername": "Alice Example"}
	b, err := msgpack.Marshal(full)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var gotEarlier earlierLockEntry
	if err := msgpack.Unmarshal(b, &gotEarlier); err != nil || gotEarlier != earlier {
		t.Errorf("an earlier pod reads %+v (err %v), want %+v", gotEarlier, err, earlier)
	}
	var gotNew LockEntry
	if err := msgpack.Unmarshal(eb, &gotNew); err != nil || gotNew.LockID != "l" || gotNew.UserIdp != "" || gotNew.Meta != nil {
		t.Errorf("an earlier entry reads as %+v (err %v)", gotNew, err)
	}
}

// ocdav writes the lock's lockownername and locktime into PROPFIND XML unescaped, so the copy in
// the resource info carries them escaped: a display name with '&' or '<' must neither break the
// listing nor add elements to it. The CS3 lock itself keeps the raw value.
func TestResourceInfo_LockOwnerNameIsSafeInXML(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
	ctx := testContext()
	name := `Tom & Jerry </oc:ownername><oc:permissions>RDNVW</oc:permissions><oc:ownername>`
	lock := webdavLock(time.Now().Add(time.Hour), "2026-10-04T08:00:00Z")
	lock.Opaque = utils.AppendPlainToOpaque(lock.Opaque, "lockownername", name)
	if err := d.SetLock(ctx, f1Ref(), lock); err != nil {
		t.Fatalf("SetLock: %v", err)
	}
	ri, err := d.GetMD(ctx, f1Ref(), nil, nil)
	if err != nil {
		t.Fatalf("GetMD: %v", err)
	}

	// The fragment ocdav builds from the entry, with its raw write.
	owner := utils.ReadPlainFromOpaque(discoveredLock(t, ri).GetOpaque(), "lockownername")
	fragment := `<activelock xmlns:oc="http://owncloud.org/ns"><oc:ownername>` + owner + `</oc:ownername></activelock>`
	var parsed struct {
		OwnerName []string `xml:"http://owncloud.org/ns ownername"`
		Other     []struct {
			XMLName xml.Name
		} `xml:",any"`
	}
	if err := xml.Unmarshal([]byte(fragment), &parsed); err != nil {
		t.Fatalf("the rendered lock is not well-formed XML: %v\n%s", err, fragment)
	}
	if len(parsed.OwnerName) != 1 || parsed.OwnerName[0] != name || len(parsed.Other) != 0 {
		t.Errorf("rendered owner = %q (extra elements %v), want exactly %q", parsed.OwnerName, parsed.Other, name)
	}

	got, err := d.GetLock(ctx, f1Ref())
	if err != nil {
		t.Fatalf("GetLock: %v", err)
	}
	if v := utils.ReadPlainFromOpaque(got.GetOpaque(), "lockownername"); v != name {
		t.Errorf("GetLock lockownername = %q, want the raw %q", v, name)
	}
}
