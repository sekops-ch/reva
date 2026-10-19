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
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vmihailenco/msgpack/v5"
)

// fakeKVEntry is a minimal KeyValueEntry for listing tests.
type fakeKVEntry struct {
	key   string
	value []byte
}

func (e *fakeKVEntry) Bucket() string             { return "fake" }
func (e *fakeKVEntry) Key() string                { return e.key }
func (e *fakeKVEntry) Value() []byte              { return e.value }
func (e *fakeKVEntry) Revision() uint64           { return 1 }
func (e *fakeKVEntry) Created() time.Time         { return time.Unix(0, 0) }
func (e *fakeKVEntry) Delta() uint64              { return 0 }
func (e *fakeKVEntry) Operation() nats.KeyValueOp { return nats.KeyValuePut }

// fakeKeyWatcher replays a scripted updates channel and error channel.
type fakeKeyWatcher struct {
	updates chan nats.KeyValueEntry
	errs    chan error
}

func (w *fakeKeyWatcher) Context() context.Context           { return context.Background() }
func (w *fakeKeyWatcher) Updates() <-chan nats.KeyValueEntry { return w.updates }
func (w *fakeKeyWatcher) Stop() error                        { return nil }
func (w *fakeKeyWatcher) Error() <-chan error                { return w.errs }

// fakeWatchKV hands the scripted watcher to WatchAll and Watch.
type fakeWatchKV struct {
	nats.KeyValue
	w nats.KeyWatcher
}

func (kv *fakeWatchKV) WatchAll(opts ...nats.WatchOpt) (nats.KeyWatcher, error) {
	return kv.w, nil
}

func (kv *fakeWatchKV) Watch(keys string, opts ...nats.WatchOpt) (nats.KeyWatcher, error) {
	return kv.w, nil
}

func mustMarshalSpace(t *testing.T, id string) []byte {
	t.Helper()
	raw, err := msgpack.Marshal(&SpaceEntry{ID: id})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// scriptedWatcher queues the given entries (nil = the initial-values marker),
// optionally closing the updates channel, and queues errs on the error
// channel.
func scriptedWatcher(entries []nats.KeyValueEntry, closeUpdates bool, errs ...error) *fakeKeyWatcher {
	w := &fakeKeyWatcher{
		updates: make(chan nats.KeyValueEntry, len(entries)+1),
		errs:    make(chan error, len(errs)+1),
	}
	for _, e := range entries {
		w.updates <- e
	}
	if closeUpdates {
		close(w.updates)
	}
	for _, err := range errs {
		w.errs <- err
	}
	return w
}

// nats.go sends the end-of-listing marker AND queues ErrKeyWatcherTimeout
// when its init timer fires: the marker alone must not read as a complete
// listing.
func TestListAll_FailsOnWatcherTimeout(t *testing.T) {
	w := scriptedWatcher([]nats.KeyValueEntry{
		&fakeKVEntry{key: "s1", value: mustMarshalSpace(t, "s1")},
		nil,
	}, false, nats.ErrKeyWatcherTimeout)
	_, err := listAll[SpaceEntry](&fakeWatchKV{w: w}, "spaces")
	if err == nil {
		t.Fatal("listAll returned nil error on a timed-out listing, want a loud failure")
	}
	if !strings.Contains(err.Error(), "watcher error") {
		t.Fatalf("error %q does not name the watcher error", err)
	}
}

// A dead subscription closes the updates channel without a marker; the
// truncated result must not read as success.
func TestListAll_FailsOnClosedWithoutMarker(t *testing.T) {
	w := scriptedWatcher([]nats.KeyValueEntry{
		&fakeKVEntry{key: "s1", value: mustMarshalSpace(t, "s1")},
	}, true)
	close(w.errs)
	_, err := listAll[SpaceEntry](&fakeWatchKV{w: w}, "spaces")
	if err == nil {
		t.Fatal("listAll returned nil error on a listing closed without the marker")
	}
	if !strings.Contains(err.Error(), "initial-values marker") {
		t.Fatalf("error %q does not name the missing marker", err)
	}
}

// A corrupt entry must fail the listing with its bucket and key, never be
// silently skipped: GC verdicts treat absence from a listing as residue.
func TestListAll_FailsOnCorruptEntry(t *testing.T) {
	w := scriptedWatcher([]nats.KeyValueEntry{
		&fakeKVEntry{key: "bad-key", value: []byte("\x01not-msgpack")},
		nil,
	}, false)
	_, err := listAll[SpaceEntry](&fakeWatchKV{w: w}, "spaces")
	if err == nil {
		t.Fatal("listAll returned nil error on a corrupt entry")
	}
	if !strings.Contains(err.Error(), "bad-key") || !strings.Contains(err.Error(), "spaces") {
		t.Fatalf("error %q does not name the corrupt key and bucket", err)
	}
}

func TestListByPrefix_FailsOnWatcherTimeout(t *testing.T) {
	w := scriptedWatcher([]nats.KeyValueEntry{nil}, false, nats.ErrKeyWatcherTimeout)
	_, err := listByPrefix[NodeEntry](&fakeWatchKV{w: w}, "nodes", "sp1.")
	if err == nil {
		t.Fatal("listByPrefix returned nil error on a timed-out listing")
	}
}

// The uploads listing keeps its unmarshal tolerance but must still fail on a
// truncation signal.
func TestListAllUploadEntries_FailsOnWatcherTimeout(t *testing.T) {
	w := scriptedWatcher([]nats.KeyValueEntry{nil}, false, nats.ErrKeyWatcherTimeout)
	s := &KVStore{uploads: &fakeWatchKV{w: w}}
	_, err := s.ListAllUploadEntries()
	if err == nil {
		t.Fatal("ListAllUploadEntries returned nil error on a timed-out listing")
	}
}

// The happy path still lists every entry and ends cleanly on the marker.
func TestListAll_CompleteListingSucceeds(t *testing.T) {
	w := scriptedWatcher([]nats.KeyValueEntry{
		&fakeKVEntry{key: "s1", value: mustMarshalSpace(t, "s1")},
		&fakeKVEntry{key: "s2", value: mustMarshalSpace(t, "s2")},
		nil,
	}, false)
	close(w.errs)
	got, err := listAll[SpaceEntry](&fakeWatchKV{w: w}, "spaces")
	if err != nil {
		t.Fatalf("listAll: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("listed %d entries, want 2", len(got))
	}
}
