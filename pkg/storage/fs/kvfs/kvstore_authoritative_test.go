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

// Tests for the authoritative leader read confirming direct KV misses. A
// wrapper over the interface-typed KV handles simulates a replica whose
// materialized view trails a just-acknowledged write: its Get answers
// not-found (or a stale map) while the embedded server's stream — what the
// leader read asks — already holds the key.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func newTestKVStore(t *testing.T) *KVStore {
	t.Helper()
	_, url := embeddedNATSWithURL(t)
	s, err := NewKVStore(&Options{NATSNodes: []string{url}, BucketPrefix: "t"}, nil)
	if err != nil {
		t.Fatalf("NewKVStore: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

// lagKV answers not-found for the next `misses` Gets, like a trailing replica.
type lagKV struct {
	nats.KeyValue
	mu     sync.Mutex
	misses int
}

func (l *lagKV) Get(key string) (nats.KeyValueEntry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.misses > 0 {
		l.misses--
		return nil, nats.ErrKeyNotFound
	}
	return l.KeyValue.Get(key)
}

// errJS fails every GetLastMsg, like a stream mid leader election.
type errJS struct {
	nats.JetStreamContext
}

func (errJS) GetLastMsg(name, subject string, opts ...nats.JSOpt) (*nats.RawStreamMsg, error) {
	return nil, fmt.Errorf("no stream leader")
}

func TestGetUploadConfirmsMissWithLeaderRead(t *testing.T) {
	s := newTestKVStore(t)
	if err := s.PutUpload(&UploadSession{ID: "sess-1", SpaceID: "s1", Filename: "f"}); err != nil {
		t.Fatalf("PutUpload: %v", err)
	}
	s.uploads = &lagKV{KeyValue: s.uploads, misses: 1}

	got, err := s.GetUpload("sess-1")
	if err != nil {
		t.Fatalf("GetUpload after lagging direct read: %v (the miss was not confirmed with the leader)", err)
	}
	if got.ID != "sess-1" || got.SpaceID != "s1" {
		t.Fatalf("GetUpload returned wrong session: %+v", got)
	}
}

func TestGetUploadDeletedStaysNotFound(t *testing.T) {
	s := newTestKVStore(t)
	if err := s.PutUpload(&UploadSession{ID: "sess-2"}); err != nil {
		t.Fatalf("PutUpload: %v", err)
	}
	if err := s.DeleteUpload("sess-2"); err != nil {
		t.Fatalf("DeleteUpload: %v", err)
	}
	// Force the authoritative path: the leader read must fold the delete
	// tombstone into not-found, never resurrect the session.
	s.uploads = &lagKV{KeyValue: s.uploads, misses: 1}

	if _, err := s.GetUpload("sess-2"); err != ErrNotFound {
		t.Fatalf("GetUpload of deleted session = %v, want ErrNotFound", err)
	}
}

func TestGetNodeConfirmsMissWithLeaderRead(t *testing.T) {
	s := newTestKVStore(t)
	n := &NodeEntry{ID: "n1", SpaceID: "s1", ParentID: "root", Name: "file.txt"}
	if err := s.PutNode(n, 0); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	s.nodes = &lagKV{KeyValue: s.nodes, misses: 1}

	got, rev, err := s.GetNode("s1", "n1")
	if err != nil {
		t.Fatalf("GetNode after lagging direct read: %v", err)
	}
	if got.Name != "file.txt" || rev == 0 {
		t.Fatalf("GetNode returned node %+v rev %d, want file.txt with a real revision", got, rev)
	}
}

func TestGetNodeAbsentStaysNotFound(t *testing.T) {
	s := newTestKVStore(t)
	if _, _, err := s.GetNode("s1", "never-written"); err != ErrNodeNotFound {
		t.Fatalf("GetNode of absent node = %v, want ErrNodeNotFound", err)
	}
}

func TestConfirmChildAuthoritative(t *testing.T) {
	s := newTestKVStore(t)
	if err := s.PutChildren("s1", "p1", ChildMap{"a.txt": "n-a", "b.txt": "n-b"}, 0); err != nil {
		t.Fatalf("PutChildren: %v", err)
	}
	id, found, err := s.ConfirmChildAuthoritative("s1", "p1", "b.txt")
	if err != nil || !found || id != "n-b" {
		t.Fatalf("present name = (%q, %v, %v), want (n-b, true, nil)", id, found, err)
	}
	if _, found, err := s.ConfirmChildAuthoritative("s1", "p1", "nope.txt"); err != nil || found {
		t.Fatalf("absent name in an existing map = (%v, %v), want (false, nil)", found, err)
	}
	if _, found, err := s.ConfirmChildAuthoritative("s1", "never-written", "x"); err != nil || found {
		t.Fatalf("never-written key = (%v, %v), want (false, nil)", found, err)
	}
}

// The name-miss confirm must count what it ESTABLISHED, never a blanket hit:
// an ordinary absent name (every create resolves one) is confirmed_absent.
func TestAuthoritativeChildConfirm_AbsentNameCountsConfirmedAbsent(t *testing.T) {
	s := newTestKVStore(t)
	if err := s.PutChildren("s1", "p1", ChildMap{"a.txt": "n-a"}, 0); err != nil {
		t.Fatalf("PutChildren: %v", err)
	}
	bucket := s.children.Bucket()
	beforeHit := getCounterValue(t, "kvfs_kv_authoritative_reads_total",
		map[string]string{"bucket": bucket, "outcome": "hit"})
	beforeAbsent := getCounterValue(t, "kvfs_kv_authoritative_reads_total",
		map[string]string{"bucket": bucket, "outcome": "confirmed_absent"})

	d := &kvfsDriver{store: s}
	if _, err := d.resolveRelativePath(context.Background(), "s1", "p1", "nope.txt"); err != ErrNodeNotFound {
		t.Fatalf("resolve of absent name = %v, want ErrNodeNotFound", err)
	}

	afterHit := getCounterValue(t, "kvfs_kv_authoritative_reads_total",
		map[string]string{"bucket": bucket, "outcome": "hit"})
	afterAbsent := getCounterValue(t, "kvfs_kv_authoritative_reads_total",
		map[string]string{"bucket": bucket, "outcome": "confirmed_absent"})
	if afterAbsent-beforeAbsent != 1 {
		t.Errorf("confirmed_absent delta = %v, want 1", afterAbsent-beforeAbsent)
	}
	if afterHit-beforeHit != 0 {
		t.Errorf("hit delta = %v, want 0 (an ordinary miss is not a lag catch)", afterHit-beforeHit)
	}
}

func TestAuthoritativeChildConfirm_StaleReplicaCountsLagCaught(t *testing.T) {
	s := newTestKVStore(t)
	if err := s.PutChildren("s1", "p1", ChildMap{"file.txt": "n1"}, 0); err != nil {
		t.Fatalf("PutChildren: %v", err)
	}
	bucket := s.children.Bucket()
	before := getCounterValue(t, "kvfs_kv_authoritative_reads_total",
		map[string]string{"bucket": bucket, "outcome": "lag_caught"})
	s.children = &lagKV{KeyValue: s.children, misses: 1}

	d := &kvfsDriver{store: s}
	id, err := d.resolveRelativePath(context.Background(), "s1", "p1", "file.txt")
	if err != nil || id != "n1" {
		t.Fatalf("resolve through a lagging replica = (%q, %v), want (n1, nil)", id, err)
	}
	after := getCounterValue(t, "kvfs_kv_authoritative_reads_total",
		map[string]string{"bucket": bucket, "outcome": "lag_caught"})
	if after-before != 1 {
		t.Errorf("lag_caught delta = %v, want 1", after-before)
	}
}

func TestAuthoritativeRead_ErrorOutcomeCounted(t *testing.T) {
	s := newTestKVStore(t)
	if err := s.PutUpload(&UploadSession{ID: "sess-err"}); err != nil {
		t.Fatalf("PutUpload: %v", err)
	}
	bucket := s.uploads.Bucket()
	before := getCounterValue(t, "kvfs_kv_authoritative_reads_total",
		map[string]string{"bucket": bucket, "outcome": "error"})
	s.uploads = &lagKV{KeyValue: s.uploads, misses: 1}
	s.js = errJS{s.js}
	if _, err := s.GetUpload("sess-err"); err == nil {
		t.Fatal("GetUpload with failing leader read must error")
	}
	after := getCounterValue(t, "kvfs_kv_authoritative_reads_total",
		map[string]string{"bucket": bucket, "outcome": "error"})
	if after-before != 1 {
		t.Errorf("error delta = %v, want 1", after-before)
	}
}

func TestAuthoritativeReadDuration_Observed(t *testing.T) {
	s := newTestKVStore(t)
	if err := s.PutChildren("s1", "pd", ChildMap{"a": "n"}, 0); err != nil {
		t.Fatalf("PutChildren: %v", err)
	}
	bucket := s.children.Bucket()
	before := getHistogramCount(t, "kvfs_kv_authoritative_read_duration_seconds",
		map[string]string{"bucket": bucket})
	if _, _, err := s.ConfirmChildAuthoritative("s1", "pd", "a"); err != nil {
		t.Fatalf("ConfirmChildAuthoritative: %v", err)
	}
	after := getHistogramCount(t, "kvfs_kv_authoritative_read_duration_seconds",
		map[string]string{"bucket": bucket})
	if after-before != 1 {
		t.Errorf("duration observations delta = %v, want 1", after-before)
	}
}

func TestResolveAuthoritativeReadTimeout(t *testing.T) {
	cases := []struct {
		name string
		opts *Options
		want time.Duration
	}{
		{"configured", &Options{AuthoritativeReadTimeout: 9 * time.Second}, 9 * time.Second},
		{"zero means default", &Options{}, defaultAuthoritativeReadTimeout},
		{"negative means default", &Options{AuthoritativeReadTimeout: -1}, defaultAuthoritativeReadTimeout},
		{"nil options", nil, defaultAuthoritativeReadTimeout},
	}
	for _, tc := range cases {
		if got := resolveAuthoritativeReadTimeout(tc.opts); got != tc.want {
			t.Errorf("%s: resolveAuthoritativeReadTimeout = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// captureJS records how many per-call options each GetLastMsg received.
type captureJS struct {
	nats.JetStreamContext
	mu   sync.Mutex
	opts []int
}

func (c *captureJS) GetLastMsg(name, subject string, opts ...nats.JSOpt) (*nats.RawStreamMsg, error) {
	c.mu.Lock()
	c.opts = append(c.opts, len(opts))
	c.mu.Unlock()
	return c.JetStreamContext.GetLastMsg(name, subject, opts...)
}

func TestAuthoritativeReadPassesPerCallBound(t *testing.T) {
	s := newTestKVStore(t)
	if err := s.PutUpload(&UploadSession{ID: "sess-cap"}); err != nil {
		t.Fatalf("PutUpload: %v", err)
	}
	cap := &captureJS{JetStreamContext: s.js}
	s.js = cap
	s.uploads = &lagKV{KeyValue: s.uploads, misses: 1}
	if _, err := s.GetUpload("sess-cap"); err != nil {
		t.Fatalf("GetUpload: %v", err)
	}
	cap.mu.Lock()
	defer cap.mu.Unlock()
	if len(cap.opts) != 1 || cap.opts[0] != 1 {
		t.Fatalf("GetLastMsg per-call options = %v, want exactly one bound per read", cap.opts)
	}
}

func TestAuthoritativeReadErrorPropagates(t *testing.T) {
	s := newTestKVStore(t)
	if err := s.PutUpload(&UploadSession{ID: "sess-3"}); err != nil {
		t.Fatalf("PutUpload: %v", err)
	}
	s.uploads = &lagKV{KeyValue: s.uploads, misses: 1}
	s.js = errJS{s.js}

	_, err := s.GetUpload("sess-3")
	if err == nil || err == ErrNotFound {
		t.Fatalf("GetUpload with failing leader read = %v, want a surfaced error, never a silent not-found", err)
	}
	if !strings.Contains(err.Error(), "authoritative read") {
		t.Fatalf("error does not name the authoritative read: %v", err)
	}
}

// staleResolveStore serves a children map without the name on the normal read
// and the fresh map on the authoritative read — the stale-but-present case a
// KV miss never covers.
type staleResolveStore struct {
	MetadataStore
	stale, fresh ChildMap
}

func (s *staleResolveStore) GetChildren(spaceID, parentID string) (ChildMap, uint64, error) {
	return s.stale, 1, nil
}

func (s *staleResolveStore) ConfirmChildAuthoritative(spaceID, parentID, name string) (string, bool, error) {
	id, ok := s.fresh[name]
	return id, ok, nil
}

func TestResolveConfirmsStaleChildrenMap(t *testing.T) {
	d := &kvfsDriver{store: &staleResolveStore{
		stale: ChildMap{},
		fresh: ChildMap{"file.txt": "n1"},
	}}
	id, err := d.resolveRelativePath(context.Background(), "s1", "root", "file.txt")
	if err != nil {
		t.Fatalf("resolve with stale children map: %v (the absent name was not confirmed with the leader)", err)
	}
	if id != "n1" {
		t.Fatalf("resolved %q, want n1", id)
	}
}

func TestResolveAbsentNameStaysNotFound(t *testing.T) {
	d := &kvfsDriver{store: &staleResolveStore{stale: ChildMap{}, fresh: ChildMap{}}}
	if _, err := d.resolveRelativePath(context.Background(), "s1", "root", "nope.txt"); err != ErrNodeNotFound {
		t.Fatalf("resolve of absent name = %v, want ErrNodeNotFound", err)
	}
}

// The lag wrapper must not mask real write visibility: a store with no lag
// behaves identically through the same code path.
func TestGetUploadNoLagFastPath(t *testing.T) {
	s := newTestKVStore(t)
	if err := s.PutUpload(&UploadSession{ID: "sess-4"}); err != nil {
		t.Fatalf("PutUpload: %v", err)
	}
	start := time.Now()
	if _, err := s.GetUpload("sess-4"); err != nil {
		t.Fatalf("GetUpload: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("direct-path read took %v", elapsed)
	}
}
