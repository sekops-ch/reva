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
	"time"

	user "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	types "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"

	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
	"github.com/opencloud-eu/reva/v2/pkg/utils"
)

// appLock is a lock as the WOPI connector sets it: an app name, no user.
func appLock(id string, expires time.Time) *provider.Lock {
	return &provider.Lock{
		LockId: id, AppName: "Collabora", Type: provider.LockType_LOCK_TYPE_WRITE,
		Expiration: &types.Timestamp{Seconds: uint64(expires.Unix())},
	}
}

// lockFixture holds file s1/f1, writable by bob through a root grant, locked by lock as the
// owner (test-user-id) when lock is not nil.
func lockFixture(t *testing.T, lock *provider.Lock) (*kvfsDriver, *mockMetadataStore) {
	t.Helper()
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
	store.nodes["s1.root"].Grants = map[string]*GrantEntry{
		"u:bob": {GranteeType: "user", GranteeID: "bob", Permissions: permissionsToUint32(&provider.ResourcePermissions{
			Stat: true, ListContainer: true, InitiateFileDownload: true, InitiateFileUpload: true,
		})},
	}
	if lock != nil {
		if err := d.SetLock(testContext(), f1Ref(), lock); err != nil {
			t.Fatalf("SetLock: %v", err)
		}
	}
	return d, store
}

func heldLock(store *mockMetadataStore) *provider.Lock {
	n, _, _ := store.GetNode("s1", "f1")
	return n.ToLock()
}

func isPreconditionFailed(err error) bool {
	_, ok := err.(errtypes.IsPreconditionFailed)
	return ok
}

func expireLock(store *mockMetadataStore) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.nodes["s1.f1"].Lock.Expiry = time.Now().Add(-time.Minute).Unix()
}

// A lock on a locked file is PreconditionFailed, as in decomposedfs. The WOPI connector then reads
// the current lock and answers 200 for its own id, which is how Collabora refreshes its lock every
// 15 minutes, and 409 for another id.
func TestSetLock_OnALiveLockIsPreconditionFailed(t *testing.T) {
	for name, again := range map[string]*provider.Lock{
		"same id":    appLock("wopi-1", time.Now().Add(30*time.Minute)),
		"another id": appLock("wopi-2", time.Now().Add(30*time.Minute)),
		"webdav":     webdavLock(time.Now().Add(time.Hour), "2026-10-04T08:00:00Z"),
	} {
		t.Run(name, func(t *testing.T) {
			d, store := lockFixture(t, appLock("wopi-1", time.Now().Add(30*time.Minute)))
			if err := d.SetLock(testContext(), f1Ref(), again); !isPreconditionFailed(err) {
				t.Errorf("SetLock on a live lock = %v (%T), want PreconditionFailed", err, err)
			}
			if got := heldLock(store); got.GetLockId() != "wopi-1" {
				t.Errorf("held lock = %v, want wopi-1 kept", got)
			}
		})
	}
	t.Run("expired lock is replaced", func(t *testing.T) {
		d, store := lockFixture(t, appLock("wopi-1", time.Now().Add(30*time.Minute)))
		expireLock(store)
		if err := d.SetLock(testContext(), f1Ref(), appLock("wopi-2", time.Now().Add(30*time.Minute))); err != nil {
			t.Fatalf("SetLock over an expired lock: %v", err)
		}
		if got := heldLock(store); got.GetLockId() != "wopi-2" {
			t.Errorf("held lock = %v, want wopi-2", got)
		}
	})
}

// RefreshLock replaces the lock named by existingLockID (WOPI UnlockAndRelock), else by the new
// lock's own id (a WebDAV refresh, WOPI REFRESH_LOCK), and only for its holder, as in decomposedfs.
func TestRefreshLock_FollowsDecomposedfs(t *testing.T) {
	later := time.Now().Add(time.Hour)
	holderRefresh := webdavLock(time.Now(), "2026-10-04T09:00:00Z")
	holderRefresh.Expiration = nil
	for _, c := range []struct {
		name     string
		held     *provider.Lock // set as the owner; nil for none
		expired  bool
		as       string // the caller
		refresh  *provider.Lock
		existing string
		check    func(error) bool // nil: accepted
		wantID   string           // the held lock's id afterwards
		wantExp  uint64           // its expiry afterwards, when accepted
	}{
		{"wopi refresh", appLock("wopi-1", time.Now().Add(30*time.Minute)), false, "test-user-id", appLock("wopi-1", later), "", nil, "wopi-1", uint64(later.Unix())},
		{"wopi unlock and relock", appLock("wopi-1", time.Now().Add(30*time.Minute)), false, "test-user-id", appLock("wopi-2", later), "wopi-1", nil, "wopi-2", uint64(later.Unix())},
		{"relock with a stale id", appLock("wopi-1", time.Now().Add(30*time.Minute)), false, "test-user-id", appLock("wopi-2", later), "wopi-0", isAborted, "wopi-1", 0},
		{"refresh of another id", appLock("wopi-1", time.Now().Add(30*time.Minute)), false, "test-user-id", appLock("wopi-9", later), "", isAborted, "wopi-1", 0},
		{"webdav refresh by the holder", webdavLock(time.Now().Add(time.Hour), "2026-10-04T08:00:00Z"), false, "test-user-id", holderRefresh, "", nil, holderRefresh.LockId, 0},
		{"no lock", nil, false, "test-user-id", appLock("wopi-1", later), "", isPreconditionFailed, "", 0},
		{"expired lock", appLock("wopi-1", time.Now().Add(30*time.Minute)), true, "test-user-id", appLock("wopi-1", later), "", isPreconditionFailed, "", 0},
		{"another user's lock", webdavLock(time.Now().Add(time.Hour), "2026-10-04T08:00:00Z"), false, "bob",
			&provider.Lock{LockId: holderRefresh.LockId, Type: provider.LockType_LOCK_TYPE_EXCL, User: &user.UserId{OpaqueId: "bob"}}, "", isPermissionDenied, holderRefresh.LockId, 0},
		{"the holder's name, another caller", webdavLock(time.Now().Add(time.Hour), "2026-10-04T08:00:00Z"), false, "bob", holderRefresh, "", isPermissionDenied, holderRefresh.LockId, 0},
		{"another app", appLock("wopi-1", time.Now().Add(30*time.Minute)), false, "test-user-id",
			&provider.Lock{LockId: "wopi-1", AppName: "OnlyOffice", Type: provider.LockType_LOCK_TYPE_WRITE}, "", isPermissionDenied, "wopi-1", 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			d, store := lockFixture(t, c.held)
			if c.expired {
				expireLock(store)
			}
			before := heldLock(store)
			err := d.RefreshLock(testContextWithUser(c.as, nil), f1Ref(), c.refresh, c.existing)
			if c.check == nil && err != nil {
				t.Fatalf("RefreshLock: %v", err)
			}
			if c.check != nil && !c.check(err) {
				t.Fatalf("RefreshLock = %v (%T), want it refused", err, err)
			}
			got := heldLock(store)
			if got.GetLockId() != c.wantID {
				t.Errorf("held lock = %v, want id %q", got, c.wantID)
			}
			if c.check != nil && before != nil && got != nil && got.GetExpiration().GetSeconds() != before.GetExpiration().GetSeconds() {
				t.Errorf("a refused refresh changed the expiry")
			}
			if c.check == nil && got.GetExpiration().GetSeconds() != c.wantExp {
				t.Errorf("expiry = %d, want %d", got.GetExpiration().GetSeconds(), c.wantExp)
			}
			if c.name == "webdav refresh by the holder" && utils.ReadPlainFromOpaque(got.GetOpaque(), "locktime") != "2026-10-04T09:00:00Z" {
				t.Errorf("locktime after a refresh = %q", utils.ReadPlainFromOpaque(got.GetOpaque(), "locktime"))
			}
		})
	}
}

// Unlock removes a lock only for its id and its holder, as in decomposedfs; lock discovery shows
// every reader the token, so the id alone is not enough.
func TestUnlock_FollowsDecomposedfs(t *testing.T) {
	for _, c := range []struct {
		name    string
		held    *provider.Lock
		expired bool
		as      string
		unlock  *provider.Lock
		check   func(error) bool // nil: removed
	}{
		{"wopi", appLock("wopi-1", time.Now().Add(30*time.Minute)), false, "test-user-id", &provider.Lock{LockId: "wopi-1", AppName: "Collabora"}, nil},
		{"webdav holder", webdavLock(time.Now().Add(time.Hour), "t"), false, "test-user-id",
			&provider.Lock{LockId: webdavLock(time.Now(), "t").LockId, User: &user.UserId{OpaqueId: "test-user-id"}}, nil},
		{"another id", appLock("wopi-1", time.Now().Add(30*time.Minute)), false, "test-user-id", &provider.Lock{LockId: "wopi-2", AppName: "Collabora"}, isLocked},
		{"no lock", nil, false, "test-user-id", &provider.Lock{LockId: "wopi-1", AppName: "Collabora"}, isAborted},
		{"expired lock", appLock("wopi-1", time.Now().Add(30*time.Minute)), true, "test-user-id", &provider.Lock{LockId: "wopi-1", AppName: "Collabora"}, isAborted},
		{"another user with the discovered token", webdavLock(time.Now().Add(time.Hour), "t"), false, "bob",
			&provider.Lock{LockId: webdavLock(time.Now(), "t").LockId, User: &user.UserId{OpaqueId: "bob"}}, isPermissionDenied},
		{"a webdav unlock of an app lock", appLock("wopi-1", time.Now().Add(30*time.Minute)), false, "test-user-id",
			&provider.Lock{LockId: "wopi-1", User: &user.UserId{OpaqueId: "test-user-id"}}, isPermissionDenied},
		{"a shared lock", &provider.Lock{LockId: "shared-1", Type: provider.LockType_LOCK_TYPE_SHARED, User: lockUser()}, false, "bob",
			&provider.Lock{LockId: "shared-1", User: &user.UserId{OpaqueId: "bob"}}, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			d, store := lockFixture(t, c.held)
			if c.expired {
				expireLock(store)
			}
			err := d.Unlock(testContextWithUser(c.as, nil), f1Ref(), c.unlock)
			if c.check == nil {
				if err != nil {
					t.Fatalf("Unlock: %v", err)
				}
				if got := heldLock(store); got != nil {
					t.Errorf("lock %v survived the unlock", got)
				}
				return
			}
			if !c.check(err) {
				t.Fatalf("Unlock = %v (%T), want it refused", err, err)
			}
			if c.held != nil && !c.expired && heldLock(store).GetLockId() != c.held.GetLockId() {
				t.Errorf("a refused unlock removed the lock")
			}
		})
	}
}
