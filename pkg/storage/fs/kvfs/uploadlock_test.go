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
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"testing"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	tusd "github.com/tus/tusd/v2/pkg/handler"

	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
	"github.com/opencloud-eu/reva/v2/pkg/storage"
)

func lockCtx(lockID string) context.Context {
	if lockID == "" {
		return testContext()
	}
	return contextWithLockID(testContext(), lockID)
}

func lockFile(store *mockMetadataStore, lockID string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.nodes["s1.f1"].Lock = &LockEntry{LockID: lockID, Type: 1, UserID: "alice"}
}

func isLocked(err error) bool  { _, ok := err.(errtypes.IsLocked); return ok }
func isAborted(err error) bool { _, ok := err.(errtypes.IsAborted); return ok }

// An upload to an existing file checks its lock at initiation, as decomposedfs does: a locked
// file needs its lock id, and a lock id on an unlocked file is refused. A new file has no lock.
func TestInitiateUpload_HonoursTheTargetsLock(t *testing.T) {
	for _, length := range []int64{0, 5} {
		for _, c := range []struct {
			name    string
			path    string
			lock    string // the file's lock id, "" for none
			lockID  string // the request's lock id
			refused func(error) bool
		}{
			{"locked, no lock id", "./file.txt", "lock-123", "", isLocked},
			{"locked, its lock id", "./file.txt", "lock-123", "lock-123", nil},
			{"locked, another lock id", "./file.txt", "lock-123", "lock-456", isAborted},
			{"unlocked, a lock id", "./file.txt", "", "lock-123", isAborted},
			{"new file, a lock id", "./new.txt", "", "lock-123", nil},
		} {
			t.Run(fmt.Sprintf("%s/length=%d", c.name, length), func(t *testing.T) {
				d, store, _, _ := emptySaveFixture()
				if c.lock != "" {
					lockFile(store, c.lock)
				}
				nodes, children := nodeSnapshot(store), childrenSnapshot(store)
				res, err := d.InitiateUpload(lockCtx(c.lockID), pathRef(c.path), length, map[string]string{})
				if c.refused != nil {
					if !c.refused(err) {
						t.Fatalf("length %d: InitiateUpload = %v, %v; want it refused", length, res, err)
					}
					assertTreeUnchanged(t, store, nodes, children, 0)
					if len(store.uploads) != 0 {
						t.Errorf("%d sessions, want none", len(store.uploads))
					}
					return
				}
				if err != nil {
					t.Fatalf("length %d: InitiateUpload: %v", length, err)
				}
				if length == 0 {
					return
				}
				if s := store.uploads[res["tus"]]; s == nil || s.LockID != c.lockID {
					t.Errorf("session %+v, want it to carry lock id %q", s, c.lockID)
				}
				if q := res["simple"][strings.LastIndex(res["simple"], "?")+1:]; !strings.Contains(q, "lock-id="+url.QueryEscape(c.lockID)) {
					t.Errorf("simple id %q does not carry the lock id", res["simple"])
				}
			})
		}
	}
}

// TouchFile stays free of lock checks: on an existing file it is AlreadyExists, and the empty
// upload that follows checks the lock.
func TestTouchFile_LockedFileIsAlreadyExists(t *testing.T) {
	d, store, _, _ := emptySaveFixture()
	lockFile(store, "lock-123")
	if err := d.TouchFile(testContext(), pathRef("./file.txt"), false, ""); !isAlreadyExists(err) {
		t.Errorf("TouchFile on a locked file = %v, want AlreadyExists", err)
	}
}

func commitSimpleID(ctx context.Context, t *testing.T, d *kvfsDriver, id, body string) error {
	t.Helper()
	_, err := d.Upload(ctx, storage.UploadRequest{
		Ref:    &provider.Reference{Path: transportSimpleID(t, id)},
		Body:   io.NopCloser(strings.NewReader(body)),
		Length: int64(len(body)),
	}, nil)
	return err
}

// The simple commit checks the lock again: with the lock id the data request names (X-Lock-Id),
// else with the one carried from initiation.
func TestSimpleUpload_HonoursTheLockAtCommit(t *testing.T) {
	for _, c := range []struct {
		name       string
		initLock   string // lock on the file at initiation
		initID     string // lock id at initiation
		commitLock string // lock on the file at commit
		commitID   string // X-Lock-Id at commit
		refused    func(error) bool
	}{
		{"locked after initiation", "", "", "lock-123", "", isLocked},
		{"carried lock id", "lock-123", "lock-123", "lock-123", "", nil},
		{"X-Lock-Id at commit", "", "", "lock-123", "lock-123", nil},
		{"lock replaced after initiation", "lock-123", "lock-123", "lock-789", "", isAborted},
		{"unlocked after initiation", "lock-123", "lock-123", "", "", isAborted},
	} {
		t.Run(c.name, func(t *testing.T) {
			d, store, blob, _ := emptySaveFixture()
			if c.initLock != "" {
				lockFile(store, c.initLock)
			}
			res, err := d.InitiateUpload(lockCtx(c.initID), pathRef("./file.txt"), 5, map[string]string{})
			if err != nil {
				t.Fatalf("InitiateUpload: %v", err)
			}
			store.nodes["s1.f1"].Lock = nil
			if c.commitLock != "" {
				lockFile(store, c.commitLock)
			}
			err = commitSimpleID(lockCtx(c.commitID), t, d, res["simple"], "fresh")
			if c.refused == nil {
				if err != nil {
					t.Fatalf("Upload: %v", err)
				}
				if got := readHead(t, d); got != "fresh" {
					t.Errorf("head = %q, want fresh", got)
				}
				return
			}
			if !c.refused(err) {
				t.Fatalf("Upload = %v, want it refused", err)
			}
			if got := string(blob.blobs[BlobKey("s1", store.nodes["s1.f1"].BlobID)]); got != "original" {
				t.Errorf("head = %q, want original", got)
			}
		})
	}
}

// A TUS commit refused by a lock discards its session: all bytes arrived, so a kept session would
// look complete to the client's HEAD and the save would be lost silently.
func TestFinishUpload_LockedDiscardsTheSession(t *testing.T) {
	d, store, blob, _ := emptySaveFixture()
	ctx := testContext()
	res, err := d.InitiateUpload(ctx, pathRef("./file.txt"), 5, map[string]string{})
	if err != nil {
		t.Fatalf("InitiateUpload: %v", err)
	}
	up, err := d.GetUpload(ctx, res["tus"])
	if err != nil {
		t.Fatalf("GetUpload: %v", err)
	}
	if _, err := up.WriteChunk(ctx, 0, strings.NewReader("fresh")); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	lockFile(store, "lock-123")
	inFlight := getGaugeValue(t, "kvfs_upload_in_flight", map[string]string{"protocol": "tus"})

	err = up.FinishUpload(ctx)
	var te tusd.Error
	if !errors.As(err, &te) || te.HTTPResponse.StatusCode != 423 {
		t.Fatalf("FinishUpload on a locked file = %v, want a 423 tusd error", err)
	}
	if _, err := store.GetUpload(res["tus"]); err != ErrNotFound {
		t.Errorf("the session survived a refusal by the lock (err %v)", err)
	}
	if rc, err := d.uploadCache.Reader(res["tus"]); err == nil {
		rc.Close()
		t.Error("the staged bytes survived a refusal by the lock")
	}
	if got := getGaugeValue(t, "kvfs_upload_in_flight", map[string]string{"protocol": "tus"}); got != inFlight-1 {
		t.Errorf("UploadInFlight = %v, want %v", got, inFlight-1)
	}
	if got := string(blob.blobs[BlobKey("s1", store.nodes["s1.f1"].BlobID)]); got != "original" {
		t.Errorf("head = %q, want original", got)
	}
}

// Only Locked discards: other refusals keep the session for a retry.
func TestFinishUpload_LockMismatchKeepsTheSession(t *testing.T) {
	d, store, _, _ := emptySaveFixture()
	lockFile(store, "lock-123")
	ctx := lockCtx("lock-123")
	res, err := d.InitiateUpload(ctx, pathRef("./file.txt"), 5, map[string]string{})
	if err != nil {
		t.Fatalf("InitiateUpload: %v", err)
	}
	up, err := d.GetUpload(ctx, res["tus"])
	if err != nil {
		t.Fatalf("GetUpload: %v", err)
	}
	if _, err := up.WriteChunk(ctx, 0, strings.NewReader("fresh")); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	lockFile(store, "lock-789")
	if err := up.FinishUpload(testContext()); !isAborted(err) {
		t.Fatalf("FinishUpload with a replaced lock = %v, want Aborted", err)
	}
	if _, err := store.GetUpload(res["tus"]); err != nil {
		t.Errorf("the session was dropped on a lock mismatch: %v", err)
	}
}

// The TUS commit uses the lock id carried from initiation.
func TestFinishUpload_CarriedLockID(t *testing.T) {
	d, store, _, _ := emptySaveFixture()
	lockFile(store, "lock-123")
	res, err := d.InitiateUpload(lockCtx("lock-123"), pathRef("./file.txt"), 5, map[string]string{})
	if err != nil {
		t.Fatalf("InitiateUpload: %v", err)
	}
	ctx := testContext()
	up, err := d.GetUpload(ctx, res["tus"])
	if err != nil {
		t.Fatalf("GetUpload: %v", err)
	}
	if _, err := up.WriteChunk(ctx, 0, strings.NewReader("fresh")); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	if err := up.FinishUpload(ctx); err != nil {
		t.Fatalf("FinishUpload with the carried lock id: %v", err)
	}
	if got := readHead(t, d); got != "fresh" {
		t.Errorf("head = %q, want fresh", got)
	}
}
