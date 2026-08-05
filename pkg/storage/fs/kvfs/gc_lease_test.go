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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func init() {
	// Neutralise the production resolver warm-up (6 × 10s) for the whole test
	// binary so identity tests that see an unready/degraded resolver on the
	// first sweep don't pay a minute of real-time retries. The warm-up tests
	// opt back in with withShortResolverWarmup.
	gcResolverWarmupAttempts = 1
	gcResolverWarmupInterval = time.Millisecond
}

// withShortResolverWarmup enables a fast bounded warm-up for one test.
func withShortResolverWarmup(t *testing.T, attempts int, interval time.Duration) {
	pa := gcResolverWarmupAttempts
	pi := gcResolverWarmupInterval
	gcResolverWarmupAttempts = attempts
	gcResolverWarmupInterval = interval
	t.Cleanup(func() {
		gcResolverWarmupAttempts = pa
		gcResolverWarmupInterval = pi
	})
}

// racyBlobStore wraps mockBlobStore and records the maximum number of Delete
// calls in flight at once, so a race test can assert two live sweepers never
// issue destructive deletes concurrently. An optional per-delete sleep widens
// any true overlap window.
type racyBlobStore struct {
	*mockBlobStore
	inFlight int32
	maxSeen  int32
	sleep    time.Duration
}

func newRacyBlobStore(sleep time.Duration) *racyBlobStore {
	return &racyBlobStore{mockBlobStore: newMockBlobStore(), sleep: sleep}
}

func (r *racyBlobStore) Delete(ctx context.Context, key string) error {
	n := atomic.AddInt32(&r.inFlight, 1)
	for {
		m := atomic.LoadInt32(&r.maxSeen)
		if n <= m || atomic.CompareAndSwapInt32(&r.maxSeen, m, n) {
			break
		}
	}
	if r.sleep > 0 {
		time.Sleep(r.sleep)
	}
	err := r.mockBlobStore.Delete(ctx, key)
	atomic.AddInt32(&r.inFlight, -1)
	return err
}

func (r *racyBlobStore) maxConcurrentDeletes() int32 { return atomic.LoadInt32(&r.maxSeen) }

// seedOrphanSpaces creates n empty spaces each with `per` unreferenced orphan
// blobs (zero LastModified so they are always GC-eligible).
func seedOrphanSpaces(store *mockMetadataStore, blob *mockBlobStore, n, per int) int {
	total := 0
	for i := 0; i < n; i++ {
		sid := fmt.Sprintf("space-%d", i)
		setupSpaceEmpty(store, sid, "root-"+sid)
		for j := 0; j < per; j++ {
			blob.blobs[BlobKey(sid, fmt.Sprintf("orphan-%d", j))] = []byte("x")
			total++
		}
	}
	return total
}

// --- RenewLock contract (via the mock the heartbeat tests depend on) ---

func TestRenewLock_Contract(t *testing.T) {
	store := newMockMetadataStore()

	// Missing lock -> lost.
	if ok, err := store.RenewLock("gc-sweep", "p1", time.Minute); ok || err != nil {
		t.Fatalf("renew on missing lock = (%v,%v), want (false,nil)", ok, err)
	}

	// Held by us -> renewed, expiry advanced.
	store.setLock("gc-sweep", "p1", 10*time.Millisecond)
	before := lockExpiry(store, "gc-sweep")
	if ok, err := store.RenewLock("gc-sweep", "p1", time.Hour); !ok || err != nil {
		t.Fatalf("renew of our own lock = (%v,%v), want (true,nil)", ok, err)
	}
	if lockExpiry(store, "gc-sweep") <= before {
		t.Error("renew did not advance the lease expiry")
	}

	// Held by someone else -> lost.
	store.setLock("gc-sweep", "p2", time.Hour)
	if ok, _ := store.RenewLock("gc-sweep", "p1", time.Hour); ok {
		t.Error("renew succeeded on a lock held by another holder")
	}
}

func lockExpiry(store *mockMetadataStore, key string) int64 {
	store.mu.Lock()
	defer store.mu.Unlock()
	if e, ok := store.locks[key]; ok {
		return e.ExpiresAt
	}
	return 0
}

// --- Heartbeat fencing ---

// TestGCSweepLease_HeartbeatFencesOnLostLease proves the heartbeat cancels the
// sweep the moment RenewLock reports the lease definitively lost (stolen). This
// is the load-bearing guard against two live sweepers: bite check — without the
// "lost -> cancel" branch the context is never cancelled and the test fails.
func TestGCSweepLease_HeartbeatFencesOnLostLease(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	withShortGCLease(t, 200*time.Millisecond, 10*time.Millisecond)
	gc := testGC(store, blob, false)
	gc.holderID = "p1"

	store.setLock("gc-sweep", "p1", gcLeaseTTL)
	store.failRenewFor("p1") // next renewal reports the lease lost

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { gc.renewLeaseUntilDone(ctx, cancel); close(done) }()

	select {
	case <-ctx.Done():
		// fenced — correct
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat did not fence the sweep after the lease was lost")
	}
	<-done
}

// TestGCSweepLease_HeartbeatFencesOnSustainedTransientError proves a pod that
// cannot reach the store fences on doubt once it can no longer prove it still
// holds the lease (after gcLeaseTTL - gcLeaseRenewInterval).
func TestGCSweepLease_HeartbeatFencesOnSustainedTransientError(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	withShortGCLease(t, 200*time.Millisecond, 20*time.Millisecond)
	gc := testGC(store, blob, false)
	gc.holderID = "p1"

	store.setLock("gc-sweep", "p1", gcLeaseTTL)
	store.failRenewErrFor("p1") // every renewal is a transient error

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	done := make(chan struct{})
	go func() { gc.renewLeaseUntilDone(ctx, cancel); close(done) }()

	select {
	case <-ctx.Done():
		if waited := time.Since(start); waited < gcLeaseTTL-gcLeaseRenewInterval {
			t.Errorf("fenced too early on transient errors after %v (margin %v)", waited, gcLeaseTTL-gcLeaseRenewInterval)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat never fenced despite sustained transient renewal failure")
	}
	<-done
}

// TestGCSweepLease_HealthyRenewalBlocksSuccessor proves an actively-renewing
// holder keeps the lease (a successor cannot steal) and is never falsely fenced.
func TestGCSweepLease_HealthyRenewalBlocksSuccessor(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	withShortGCLease(t, 120*time.Millisecond, 20*time.Millisecond)
	gc := testGC(store, blob, false)
	gc.holderID = "p1"

	store.setLock("gc-sweep", "p1", gcLeaseTTL)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { gc.renewLeaseUntilDone(ctx, cancel); close(done) }()

	deadline := time.Now().Add(120 * time.Millisecond)
	for time.Now().Before(deadline) {
		if ok, _ := store.TryAcquireLock("gc-sweep", "p2", gcLeaseTTL); ok {
			t.Fatal("a successor stole the lease while the holder was actively renewing")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("heartbeat fenced a healthy, renewing lease")
	}
	cancel()
	<-done
}

// TestGCSweepLease_SuccessorAcquiresAfterHolderStopsRenewing maps to the
// acceptance: a holder that stops renewing (died mid-sweep) lets the short lease
// lapse, and a successor acquires within one lease + retry with NO external
// lock clearing.
func TestGCSweepLease_SuccessorAcquiresAfterHolderStopsRenewing(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	leaseTTL := 200 * time.Millisecond
	renew := 40 * time.Millisecond
	withShortGCLease(t, leaseTTL, renew)
	_ = blob

	if ok, _ := store.TryAcquireLock("gc-sweep", "p1", leaseTTL); !ok {
		t.Fatal("p1 should acquire the free lock")
	}
	// While p1 renews, a successor must stay blocked.
	for i := 0; i < 2; i++ {
		time.Sleep(renew)
		if ok, err := store.RenewLock("gc-sweep", "p1", leaseTTL); err != nil || !ok {
			t.Fatalf("p1 renew %d failed: ok=%v err=%v", i, ok, err)
		}
		if ok, _ := store.TryAcquireLock("gc-sweep", "p2", leaseTTL); ok {
			t.Fatalf("p2 stole the lock while p1 was still renewing (round %d)", i)
		}
	}

	// p1 "dies": renewals stop. After the lease lapses p2 acquires within one
	// retry cycle, without any external clearing.
	deadline := time.Now().Add(leaseTTL + renew + 200*time.Millisecond)
	acquired := false
	for time.Now().Before(deadline) {
		if ok, _ := store.TryAcquireLock("gc-sweep", "p2", leaseTTL); ok {
			acquired = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !acquired {
		t.Fatal("successor did not acquire within one lease after the holder stopped renewing")
	}
	if h := lockHolder(store, "gc-sweep"); h != "p2" {
		t.Errorf("expected p2 to hold the lock, got %q", h)
	}
}

// TestGCSweepLease_FenceStopsDestructiveSweep proves that once the sweep context
// is cancelled (lease lost, or SIGTERM cancelling the driver base ctx) the
// destructive loops stop deleting and the lock is released. Bite check: without
// the per-loop ctx.Err() fences the mock deletes every blob regardless of
// cancellation, so `remaining > 0` fails.
func TestGCSweepLease_FenceStopsDestructiveSweep(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	gc := testGC(store, blob, false)
	gc.opts.GCMinAge = "0s"

	// One large space so the INNER blob-delete loop is the sole gate — the test
	// bites the per-item fence, not just the outer per-space boundary.
	total := seedOrphanSpaces(store, blob, 1, 60)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var deleted int32
	blob.deleteHook = func(string) {
		// Cancel the sweep after a few deletes, mid-sweep.
		if atomic.AddInt32(&deleted, 1) == 5 {
			cancel()
		}
	}

	gc.Run(ctx)

	remaining := blobCount(blob)
	if remaining == 0 {
		t.Fatalf("fence did not stop the sweep: all %d blobs deleted despite mid-sweep cancellation", total)
	}
	if h := lockHolder(store, "gc-sweep"); h != "" {
		t.Errorf("lock not released after a fenced sweep; holder=%q", h)
	}
}

func blobCount(b *mockBlobStore) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.blobs)
}

// TestGCSweepLease_NoConcurrentDestructiveSweepers is the regression race guard.
// Two GCs contend for the lease while one is force-fenced mid-sweep; the CAS
// lock + heartbeat + release handoff must never let both delete at once, and the
// whole path must be -race clean. Run with `go test -race`.
func TestGCSweepLease_NoConcurrentDestructiveSweepers(t *testing.T) {
	store := newMockMetadataStore()
	racy := newRacyBlobStore(2 * time.Millisecond)
	withShortGCLease(t, 100*time.Millisecond, 15*time.Millisecond)

	total := seedOrphanSpaces(store, racy.mockBlobStore, 6, 12)
	log := zerolog.Nop()

	p1 := newBlobGC(store, racy, gcOpts(false), &log)
	p1.holderID = "p1"
	p1.opts.GCMinAge = "0s"
	p2 := newBlobGC(store, racy, gcOpts(false), &log)
	p2.holderID = "p2"
	p2.opts.GCMinAge = "0s"

	// p1 loses its lease on the first renewal: it fences early, releasing the
	// lock so p2 (retrying) picks up and finishes the sweep — a clean handoff.
	store.failRenewFor("p1")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); p1.Run(context.Background()) }()
	time.Sleep(10 * time.Millisecond) // let p1 acquire first
	go func() {
		defer wg.Done()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if r := p2.Run(context.Background()); r.Duration > 0 {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	wg.Wait()

	if v := racy.maxConcurrentDeletes(); v > 1 {
		t.Fatalf("two sweepers issued destructive deletes concurrently: max in-flight = %d", v)
	}
	if h := lockHolder(store, "gc-sweep"); h != "" {
		t.Errorf("lock not released after the handoff; holder=%q", h)
	}
	if remaining := blobCount(racy.mockBlobStore); remaining != 0 {
		t.Errorf("handoff did not complete the sweep: %d/%d blobs remain", remaining, total)
	}
}
