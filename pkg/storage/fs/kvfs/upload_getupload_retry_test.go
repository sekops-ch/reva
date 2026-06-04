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
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	tusd "github.com/tus/tusd/v2/pkg/handler"
)

// laggyStore wraps mockMetadataStore and rejects the first N GetUpload
// calls per session with ErrNotFound — simulating the NATS KV cross-
// replica read-your-write window that motivates the retry loop in
// kvfsDriver.GetUpload.
type laggyStore struct {
	*mockMetadataStore
	remainingNotFound map[string]int
	getCalls          int
}

func newLaggyStore(initialLag int, sessionID string) *laggyStore {
	return &laggyStore{
		mockMetadataStore: newMockMetadataStore(),
		remainingNotFound: map[string]int{sessionID: initialLag},
	}
}

func (l *laggyStore) GetUpload(uploadID string) (*UploadSession, error) {
	l.getCalls++
	if n, ok := l.remainingNotFound[uploadID]; ok && n > 0 {
		l.remainingNotFound[uploadID] = n - 1
		return nil, ErrNotFound
	}
	return l.mockMetadataStore.GetUpload(uploadID)
}

func testDriverWithStore(store MetadataStore) *kvfsDriver {
	logger := zerolog.Nop()
	tmpDir, err := os.MkdirTemp("", "kvfs-getupload-retry-*")
	if err != nil {
		panic(err)
	}
	return &kvfsDriver{
		store:       store,
		blob:        newMockBlobStore(),
		opts:        &Options{},
		log:         &logger,
		uploadCache: newDiskUploadCache(tmpDir),
	}
}

// TestGetUpload_HappyPath_NoRetry — first attempt succeeds, no sleep, no
// extra store calls.
func TestGetUpload_HappyPath_NoRetry(t *testing.T) {
	store := newLaggyStore(0, "sess-happy")
	session := makeSession("space-1", "parent-1", "happy.txt")
	session.ID = "sess-happy"
	_ = store.PutUpload(session)
	d := testDriverWithStore(store)

	start := time.Now()
	upload, err := d.GetUpload(context.Background(), "sess-happy")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("GetUpload: %v", err)
	}
	if upload == nil {
		t.Fatal("GetUpload returned nil upload")
	}
	if store.getCalls != 1 {
		t.Errorf("got %d store.GetUpload calls, want 1", store.getCalls)
	}
	// Sleep budget for 0 retries is 0 ms; allow up to 10 ms for scheduler jitter.
	if elapsed >= 10*time.Millisecond {
		t.Errorf("happy path took %v, want < 10ms (no sleeps expected)", elapsed)
	}
}

// TestGetUpload_ReplicationLag_SecondAttemptSucceeds — first attempt
// returns ErrNotFound (simulating a stale follower replica), second
// attempt succeeds. Asserts exactly 2 store calls and at least one
// 50 ms backoff slept.
func TestGetUpload_ReplicationLag_SecondAttemptSucceeds(t *testing.T) {
	store := newLaggyStore(1, "sess-lag")
	session := makeSession("space-1", "parent-1", "lag.txt")
	session.ID = "sess-lag"
	_ = store.PutUpload(session)
	d := testDriverWithStore(store)

	start := time.Now()
	upload, err := d.GetUpload(context.Background(), "sess-lag")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("GetUpload: %v", err)
	}
	if upload == nil {
		t.Fatal("GetUpload returned nil upload")
	}
	if store.getCalls != 2 {
		t.Errorf("got %d store.GetUpload calls, want 2 (one initial + one retry)",
			store.getCalls)
	}
	// One 50 ms sleep should have happened between attempts. Tolerate
	// scheduler jitter on the low side.
	if elapsed < 45*time.Millisecond {
		t.Errorf("retry took only %v, want >= 45ms (50ms backoff expected)", elapsed)
	}
	info, err := upload.GetInfo(context.Background())
	if err != nil {
		t.Fatalf("GetInfo: %v", err)
	}
	if info.ID != "sess-lag" {
		t.Errorf("returned upload id = %q, want %q", info.ID, "sess-lag")
	}
}

// TestGetUpload_PersistentNotFound_ExhaustsRetries — every attempt
// returns ErrNotFound. The driver exhausts the 3-attempt budget and
// surfaces tusd.ErrNotFound after the two 50 ms sleeps.
func TestGetUpload_PersistentNotFound_ExhaustsRetries(t *testing.T) {
	store := newLaggyStore(100, "sess-missing") // far beyond retry budget
	d := testDriverWithStore(store)

	start := time.Now()
	_, err := d.GetUpload(context.Background(), "sess-missing")
	elapsed := time.Since(start)
	if !errors.Is(err, tusd.ErrNotFound) {
		t.Fatalf("GetUpload: got %v, want tusd.ErrNotFound", err)
	}
	if store.getCalls != 3 {
		t.Errorf("got %d store.GetUpload calls, want 3 (full retry budget)",
			store.getCalls)
	}
	// Two 50 ms sleeps total — only sleep between attempts, not after the
	// last one. Floor at 95 ms; tolerate scheduler jitter on the low side.
	if elapsed < 95*time.Millisecond {
		t.Errorf("exhaustion took only %v, want >= 95ms (2 × 50ms backoffs)", elapsed)
	}
}
