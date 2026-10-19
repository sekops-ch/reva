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
	"bytes"
	"crypto/rand"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
)

// nopCacheLogger returns the no-op logger used by tests that don't assert
// on log output.
func nopCacheLogger() *zerolog.Logger {
	l := zerolog.Nop()
	return &l
}

// embeddedNATS spins up an in-process NATS JetStream server for tests. The
// server uses a per-test TempDir for storage so concurrent tests don't
// collide. Returns a JetStreamContext bound to the server; the server is
// shut down via t.Cleanup.
func embeddedNATS(t *testing.T) nats.JetStreamContext {
	t.Helper()
	js, _ := embeddedNATSWithURL(t)
	return js
}

// embeddedNATSWithURL additionally exposes the server's client URL for
// tests that need to drive their own connection (e.g. NewKVStore).
func embeddedNATSWithURL(t *testing.T) (nats.JetStreamContext, string) {
	t.Helper()
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1, // pick a random free port
		NoSigs:    true,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	ns, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to create embedded NATS server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(3 * time.Second) {
		t.Fatalf("embedded NATS server not ready")
	}
	t.Cleanup(ns.Shutdown)

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	return js, ns.ClientURL()
}

func defaultNATSCacheOpts() natsStreamUploadCacheOptions {
	return natsStreamUploadCacheOptions{
		StreamName:    "TEST-UPLOADS",
		SubjectPrefix: "test.uploads",
		Storage:       "file",
		Replicas:      1,
		MaxAge:        1 * time.Hour,
		MaxBytes:      64 * 1024 * 1024, // 64 MiB — plenty for tests
		MaxChunkBytes: 1024,             // small chunk size to exercise splitting
	}
}

// TestNATSStreamUploadCache_SizeEmpty verifies Size returns 0 for a never-
// touched session ID (no messages on its subject).
func TestNATSStreamUploadCache_SizeEmpty(t *testing.T) {
	js := embeddedNATS(t)
	cache, err := newNATSStreamUploadCache(js, defaultNATSCacheOpts(), nopCacheLogger())
	if err != nil {
		t.Fatalf("newNATSStreamUploadCache: %v", err)
	}

	size, err := cache.Size("does-not-exist")
	if err != nil {
		t.Fatalf("Size on empty session: %v", err)
	}
	if size != 0 {
		t.Errorf("Size = %d, want 0", size)
	}
}

// TestNATSStreamUploadCache_AppendThenSize covers the basic
// Append → Size happy path and confirms Size returns the cumulative bytes.
func TestNATSStreamUploadCache_AppendThenSize(t *testing.T) {
	js := embeddedNATS(t)
	cache, err := newNATSStreamUploadCache(js, defaultNATSCacheOpts(), nopCacheLogger())
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	body := []byte("hello world chunk one")
	n, err := cache.Append("sess-1", 0, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if n != int64(len(body)) {
		t.Errorf("Append returned %d, want %d", n, len(body))
	}

	size, err := cache.Size("sess-1")
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if size != int64(len(body)) {
		t.Errorf("Size = %d, want %d", size, len(body))
	}
}

// TestNATSStreamUploadCache_MultipleAppends confirms cumulative Size and
// ordered Reader output for two sequential Appends on the same session.
func TestNATSStreamUploadCache_MultipleAppends(t *testing.T) {
	js := embeddedNATS(t)
	cache, err := newNATSStreamUploadCache(js, defaultNATSCacheOpts(), nopCacheLogger())
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	chunk1 := []byte("AAAA")
	chunk2 := []byte("BBBB")
	if _, err := cache.Append("sess", 0, bytes.NewReader(chunk1)); err != nil {
		t.Fatalf("Append1: %v", err)
	}
	if _, err := cache.Append("sess", int64(len(chunk1)), bytes.NewReader(chunk2)); err != nil {
		t.Fatalf("Append2: %v", err)
	}

	size, _ := cache.Size("sess")
	if size != int64(len(chunk1)+len(chunk2)) {
		t.Errorf("cumulative size = %d, want %d", size, len(chunk1)+len(chunk2))
	}

	r, err := cache.Reader("sess")
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	want := append(chunk1, chunk2...)
	if !bytes.Equal(got, want) {
		t.Errorf("read bytes = %q, want %q", got, want)
	}
}

// TestNATSStreamUploadCache_SplitsLargeAppend verifies that a single
// Append call larger than MaxChunkBytes gets split into multiple internal
// publishes — the Reader must reassemble them in the right order with no
// data loss.
func TestNATSStreamUploadCache_SplitsLargeAppend(t *testing.T) {
	js := embeddedNATS(t)
	opts := defaultNATSCacheOpts()
	opts.MaxChunkBytes = 256 // force several sub-messages
	cache, err := newNATSStreamUploadCache(js, opts, nopCacheLogger())
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	body := make([]byte, 2*1024) // 2 KiB → 8 sub-messages at 256 B each
	if _, err := rand.Read(body); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}

	n, err := cache.Append("big-session", 0, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if n != int64(len(body)) {
		t.Errorf("Append returned %d, want %d", n, len(body))
	}

	size, _ := cache.Size("big-session")
	if size != int64(len(body)) {
		t.Errorf("Size = %d, want %d", size, len(body))
	}

	r, _ := cache.Reader("big-session")
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("reassembled body differs (got %d bytes, want %d)", len(got), len(body))
	}
}

// TestNATSStreamUploadCache_CrossPod simulates two opencloud pods using
// the same NATS-backed cache: pod A writes chunk 0, pod B writes chunk 1,
// FinishUpload on either pod reads the full body. This is the cross-pod
// transparency the NATS backend exists to provide; the disk backend
// would fail this scenario.
func TestNATSStreamUploadCache_CrossPod(t *testing.T) {
	js := embeddedNATS(t)
	// Two independent cache instances sharing the same JS context simulate
	// two opencloud pods with shared NATS staging.
	cacheA, err := newNATSStreamUploadCache(js, defaultNATSCacheOpts(), nopCacheLogger())
	if err != nil {
		t.Fatalf("newA: %v", err)
	}
	cacheB, err := newNATSStreamUploadCache(js, defaultNATSCacheOpts(), nopCacheLogger())
	if err != nil {
		t.Fatalf("newB: %v", err)
	}

	chunkA := []byte("from-pod-A-chunk-zero")
	chunkB := []byte("from-pod-B-chunk-one")

	// Pod A writes the first chunk.
	if _, err := cacheA.Append("xpod", 0, bytes.NewReader(chunkA)); err != nil {
		t.Fatalf("cacheA.Append: %v", err)
	}

	// Pod B sees the live size.
	size, _ := cacheB.Size("xpod")
	if size != int64(len(chunkA)) {
		t.Errorf("cacheB sees size=%d, want %d (A's chunk should be visible)", size, len(chunkA))
	}

	// Pod B appends at the size pod A left behind — the TUS handler always
	// supplies offset == current size (see the UploadCache contract).
	if _, err := cacheB.Append("xpod", size, bytes.NewReader(chunkB)); err != nil {
		t.Fatalf("cacheB.Append: %v", err)
	}
	if got, _ := cacheB.Size("xpod"); got != int64(len(chunkA)+len(chunkB)) {
		t.Errorf("cumulative size after both pods = %d, want %d", got, len(chunkA)+len(chunkB))
	}

	// Either pod can read back the full body in order.
	for label, c := range map[string]*natsStreamUploadCache{"A": cacheA, "B": cacheB} {
		r, err := c.Reader("xpod")
		if err != nil {
			t.Fatalf("cache%s.Reader: %v", label, err)
		}
		got, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			t.Fatalf("cache%s ReadAll: %v", label, err)
		}
		want := append(chunkA, chunkB...)
		if !bytes.Equal(got, want) {
			t.Errorf("cache%s read = %q, want %q", label, got, want)
		}
	}
}

// TestNATSStreamUploadCache_Drop confirms Drop purges the session and
// subsequent Size returns 0.
func TestNATSStreamUploadCache_Drop(t *testing.T) {
	js := embeddedNATS(t)
	cache, err := newNATSStreamUploadCache(js, defaultNATSCacheOpts(), nopCacheLogger())
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	if _, err := cache.Append("sess-x", 0, bytes.NewReader([]byte("payload"))); err != nil {
		t.Fatalf("Append: %v", err)
	}
	size, _ := cache.Size("sess-x")
	if size == 0 {
		t.Fatal("size 0 before drop")
	}

	if err := cache.Drop("sess-x"); err != nil {
		t.Fatalf("Drop: %v", err)
	}

	size, _ = cache.Size("sess-x")
	if size != 0 {
		t.Errorf("size after drop = %d, want 0", size)
	}
}

// TestNATSStreamUploadCache_DropIdempotent verifies Drop on a non-existent
// session is a no-op (no error).
func TestNATSStreamUploadCache_DropIdempotent(t *testing.T) {
	js := embeddedNATS(t)
	cache, err := newNATSStreamUploadCache(js, defaultNATSCacheOpts(), nopCacheLogger())
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := cache.Drop("never-existed"); err != nil {
		t.Errorf("Drop on missing session: %v", err)
	}
}

// TestNATSStreamUploadCache_OffsetHeaderEncoding verifies the
// Upload-Offset header on the last message correctly reports the byte
// offset where that message starts (i.e., cumulative size BEFORE this
// message). Size = offset_of_last + len(last_msg).
func TestNATSStreamUploadCache_OffsetHeaderEncoding(t *testing.T) {
	js := embeddedNATS(t)
	opts := defaultNATSCacheOpts()
	opts.MaxChunkBytes = 8 // tiny → forces splits
	cache, err := newNATSStreamUploadCache(js, opts, nopCacheLogger())
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	// Append 30 bytes in one call — split into 4 messages (8+8+8+6).
	body := bytes.Repeat([]byte("A"), 30)
	if _, err := cache.Append("offset-test", 0, bytes.NewReader(body)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	size, err := cache.Size("offset-test")
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if size != 30 {
		t.Errorf("Size after splitting 30 B with 8 B chunks = %d, want 30", size)
	}
}

// TestNATSStreamUploadCache_LiveAppendThenReaderOnEmptyAfterDrop verifies
// the post-Drop "empty stream" path through Reader returns an empty body
// without hanging.
func TestNATSStreamUploadCache_ReaderOnEmpty(t *testing.T) {
	js := embeddedNATS(t)
	cache, err := newNATSStreamUploadCache(js, defaultNATSCacheOpts(), nopCacheLogger())
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	r, err := cache.Reader("not-existing")
	if err != nil {
		t.Fatalf("Reader on empty: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("read %d bytes, want 0", len(got))
	}
}

// TestNATSStreamUploadCache_AppendHonorsCallerOffset proves Append stamps the
// Upload-Offset header from the caller-supplied offset rather than re-probing
// Size() internally. Appending at a non-zero offset on a fresh session (where
// a Size() re-probe would read 0) must make Size() report offset+len — only
// possible if the passed offset drove the header. This guards the hot-path
// optimisation that dropped the per-chunk GetLastMsg round-trip.
func TestNATSStreamUploadCache_AppendHonorsCallerOffset(t *testing.T) {
	js := embeddedNATS(t)
	cache, err := newNATSStreamUploadCache(js, defaultNATSCacheOpts(), nopCacheLogger())
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	const off = int64(100)
	data := []byte("XY")
	if _, err := cache.Append("sess-off", off, bytes.NewReader(data)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	size, err := cache.Size("sess-off")
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if want := off + int64(len(data)); size != want {
		t.Fatalf("Size=%d, want %d (caller offset not honoured — Append may be re-probing)", size, want)
	}
}

// lagPendingJS emulates a lagging consumer replica with the real delivery path: the
// probe (GetLastMsg, a leader read) declares MORE bytes than the stream
// holds right now, exactly what a Reader whose ordered consumer sits on a
// lagging replica observes. SubscribeSync passes through to the live
// JetStream context and signals when the consumer exists, so the test can
// time the late chunk's arrival.
type lagPendingJS struct {
	nats.JetStreamContext
	declaredOff  int64
	declaredData []byte
	subscribed   chan struct{}
	subOnce      sync.Once
}

func (p *lagPendingJS) GetLastMsg(stream, subject string, opts ...nats.JSOpt) (*nats.RawStreamMsg, error) {
	h := nats.Header{}
	h.Set("Upload-Offset", strconv.FormatInt(p.declaredOff, 10))
	return &nats.RawStreamMsg{Subject: subject, Header: h, Data: p.declaredData}, nil
}

func (p *lagPendingJS) SubscribeSync(subj string, opts ...nats.SubOpt) (*nats.Subscription, error) {
	sub, err := p.JetStreamContext.SubscribeSync(subj, opts...)
	if err == nil {
		p.subOnce.Do(func() { close(p.subscribed) })
	}
	return sub, err
}

// A pending count of zero from the consumer's view must not end the body
// early: only chunk 1 is stored when the consumer delivers it (true
// NumPending == 0), while the leader-declared size includes chunk 2, which
// arrives later and must still be delivered.
func TestNATSStreamUploadCache_ReaderWaitsPastPrematurePendingZero(t *testing.T) {
	js := embeddedNATS(t)
	cache, err := newNATSStreamUploadCache(js, defaultNATSCacheOpts(), nopCacheLogger())
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	chunk1 := bytes.Repeat([]byte("a"), 512)
	chunk2 := bytes.Repeat([]byte("b"), 256)
	if _, err := cache.Append("premature", 0, bytes.NewReader(chunk1)); err != nil {
		t.Fatalf("Append chunk1: %v", err)
	}

	fake := &lagPendingJS{
		JetStreamContext: js,
		declaredOff:      int64(len(chunk1)),
		declaredData:     chunk2,
		subscribed:       make(chan struct{}),
	}
	cache.js = fake

	done := make(chan struct{})
	t.Cleanup(func() { <-done })
	go func() {
		defer close(done)
		<-fake.subscribed
		time.Sleep(300 * time.Millisecond)
		// PublishMsg passes through the fake to the live stream.
		_, _ = cache.Append("premature", int64(len(chunk1)), bytes.NewReader(chunk2))
	}()

	r, err := cache.Reader("premature")
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if want := len(chunk1) + len(chunk2); len(got) != want {
		t.Fatalf("read %d of %d bytes: the read ended at the consumer's pending count instead of the leader-declared size", len(got), want)
	}
}

// When the staged body never reaches the leader-declared size, the Reader
// must fail loudly at the idle timeout instead of returning a short body as
// success.
func TestNATSStreamUploadCache_ReaderShortBodyFailsLoudly(t *testing.T) {
	js := embeddedNATS(t)
	cache, err := newNATSStreamUploadCache(js, defaultNATSCacheOpts(), nopCacheLogger())
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	chunk1 := []byte("stored-chunk")
	if _, err := cache.Append("short", 0, bytes.NewReader(chunk1)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	cache.js = &lagPendingJS{
		JetStreamContext: js,
		declaredOff:      int64(len(chunk1)),
		declaredData:     []byte("never-arrives"),
		subscribed:       make(chan struct{}),
	}

	oldIdle := uploadCacheIdleTimeout
	uploadCacheIdleTimeout = 200 * time.Millisecond
	t.Cleanup(func() { uploadCacheIdleTimeout = oldIdle })

	r, err := cache.Reader("short")
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer r.Close()
	_, err = io.ReadAll(r)
	if err == nil {
		t.Fatal("ReadAll returned nil error on a short staged body, want a loud failure")
	}
	if !strings.Contains(err.Error(), "staged body incomplete") {
		t.Fatalf("error %q lacks the staged-body-incomplete signal", err)
	}
}

// failingGetLastJS wraps a live JetStreamContext and fails every GetLastMsg
// with a fixed error, emulating a leader election or transport failure.
type failingGetLastJS struct {
	nats.JetStreamContext
	err error
}

func (f *failingGetLastJS) GetLastMsg(stream, subject string, opts ...nats.JSOpt) (*nats.RawStreamMsg, error) {
	return nil, f.err
}

// A GetLastMsg failure that is not the not-found sentinel must reach the
// caller: reporting it as size 0 reads a transient NATS failure as "nothing
// staged".
func TestNATSStreamUploadCache_SizeErrorPropagates(t *testing.T) {
	js := embeddedNATS(t)
	cache, err := newNATSStreamUploadCache(js, defaultNATSCacheOpts(), nopCacheLogger())
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	cache.js = &failingGetLastJS{JetStreamContext: js, err: nats.ErrConnectionClosed}

	size, err := cache.Size("sess-err")
	if err == nil {
		t.Fatalf("Size = (%d, nil) on a failed GetLastMsg, want the error propagated", size)
	}
}

// A failed config-drift reconcile (UpdateStream) must stay non-fatal and
// emit a structured warning carrying the stream name. Storage-type changes
// are rejected by JetStream, which makes a deterministic reconcile failure.
func TestNATSStreamUploadCache_UpdateStreamFailureLogsWarning(t *testing.T) {
	js := embeddedNATS(t)
	if _, err := newNATSStreamUploadCache(js, defaultNATSCacheOpts(), nopCacheLogger()); err != nil {
		t.Fatalf("initial create: %v", err)
	}

	opts := defaultNATSCacheOpts()
	opts.Storage = "memory" // file → memory: JetStream refuses the update

	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	cache, err := newNATSStreamUploadCache(js, opts, &logger)
	if err != nil {
		t.Fatalf("UpdateStream failure must be non-fatal, got: %v", err)
	}
	if cache == nil {
		t.Fatal("expected a usable cache despite the reconcile failure")
	}

	out := buf.String()
	if !strings.Contains(out, "UpdateStream config reconcile failed") {
		t.Fatalf("expected the reconcile warning in log output, got: %q", out)
	}
	if !strings.Contains(out, opts.StreamName) {
		t.Fatalf("expected the stream name %q as a log field, got: %q", opts.StreamName, out)
	}
	if !strings.Contains(out, `"level":"warn"`) {
		t.Fatalf("expected warn level, got: %q", out)
	}
}
