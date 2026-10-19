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

// Proves a small blob body survives an S3-level retry with its full content.
// A stub S3 endpoint kills the first PUT mid-request; minio-go retries by
// rewinding the body. A plain stream rewinds to nothing (the retry announces
// ContentLength and sends an empty body), which is exactly how finalize used
// to hand the tee over; the buffered body resends every byte.

import (
	"bytes"
	"context"
	"crypto/sha1"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// decodeAWSChunked strips the aws-chunked framing (hex size;chunk-signature
// lines) minio-go wraps bodies in when streaming-signing over plain HTTP.
func decodeAWSChunked(raw []byte) []byte {
	var out []byte
	rest := raw
	for {
		i := bytes.Index(rest, []byte("\r\n"))
		if i < 0 {
			return out
		}
		header := rest[:i]
		rest = rest[i+2:]
		if semi := bytes.IndexByte(header, ';'); semi >= 0 {
			header = header[:semi]
		}
		n, err := strconv.ParseInt(string(header), 16, 64)
		if err != nil || n == 0 || int64(len(rest)) < n {
			return out
		}
		out = append(out, rest[:n]...)
		rest = rest[n:]
		rest = bytes.TrimPrefix(rest, []byte("\r\n"))
	}
}

type retryStub struct {
	attempts atomic.Int32
	received atomic.Pointer[[]byte]
}

func (st *retryStub) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusOK)
			return
		}
		n := st.attempts.Add(1)
		body, _ := io.ReadAll(r.Body)
		if strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING") {
			body = decodeAWSChunked(body)
		}
		if n == 1 {
			// Kill the connection mid-request: a classic retryable transport error.
			hj, ok := w.(http.Hijacker)
			if !ok {
				panic("stub server cannot hijack")
			}
			conn, _, err := hj.Hijack()
			if err != nil {
				panic(err)
			}
			conn.Close()
			return
		}
		st.received.Store(&body)
		w.Header().Set("ETag", `"stub-etag"`)
		w.WriteHeader(http.StatusOK)
	}
}

func newStubBlobstore(t *testing.T) (*S3Blobstore, *retryStub) {
	t.Helper()
	st := &retryStub{}
	srv := httptest.NewServer(st.handler())
	t.Cleanup(srv.Close)
	bs, err := NewS3Blobstore(srv.URL, "test", "bucket", "ak", "sk", "t", nil)
	if err != nil {
		t.Fatalf("NewS3Blobstore: %v", err)
	}
	return bs, st
}

func TestUploadSmallBodySurvivesRetry(t *testing.T) {
	payload := bytes.Repeat([]byte("kvfs-retry-"), 100_000) // ~1.1 MiB, under the buffer limit
	bs, st := newStubBlobstore(t)

	// The exact production shape: a hasher tee over a non-seekable stream.
	hasher := sha1.New()
	body := io.TeeReader(io.NopCloser(bytes.NewReader(payload)), hasher)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := bs.Upload(ctx, "s1/blob-1", body, int64(len(payload))); err != nil {
		t.Fatalf("Upload across a killed first attempt: %v", err)
	}
	if got := st.attempts.Load(); got < 2 {
		t.Fatalf("expected a retried PUT, saw %d attempt(s)", got)
	}
	received := st.received.Load()
	if received == nil || !bytes.Equal(*received, payload) {
		gotLen := -1
		if received != nil {
			gotLen = len(*received)
		}
		t.Fatalf("retried PUT carried %d bytes, want the full %d-byte body", gotLen, len(payload))
	}
	// The hasher saw the body exactly once, during staging.
	if sum := sha1.Sum(payload); !bytes.Equal(hasher.Sum(nil), sum[:]) {
		t.Fatalf("hasher does not match the staged payload")
	}
}

func TestUploadShortBodyFailsBeforeS3(t *testing.T) {
	bs, st := newStubBlobstore(t)
	err := bs.Upload(context.Background(), "s1/blob-2", bytes.NewReader(nil), 10)
	if err == nil {
		t.Fatal("Upload with a short body succeeded, want a loud staging error")
	}
	if st.attempts.Load() != 0 {
		t.Fatalf("short body reached S3 (%d attempts), want the failure before any PUT", st.attempts.Load())
	}
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (lb *lockedBuffer) Write(p []byte) (int, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.b.Write(p)
}

func (lb *lockedBuffer) String() string {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.b.String()
}

func newStubBlobstoreWithLog(t *testing.T) (*S3Blobstore, *retryStub, *lockedBuffer) {
	t.Helper()
	st := &retryStub{}
	srv := httptest.NewServer(st.handler())
	t.Cleanup(srv.Close)
	buf := &lockedBuffer{}
	log := zerolog.New(buf)
	bs, err := NewS3Blobstore(srv.URL, "test", "bucket", "ak", "sk", "t", &log)
	if err != nil {
		t.Fatalf("NewS3Blobstore: %v", err)
	}
	return bs, st, buf
}

// TestUploadFirstAttemptFailureIsLogged proves the FIRST attempt's failure is
// observable with its blob key. minio-go retries silently and returns only the
// last error, so without transport-level logging a push whose uploads all
// eventually succeed hides every transient S3/network fault.
func TestUploadFirstAttemptFailureIsLogged(t *testing.T) {
	payload := bytes.Repeat([]byte("kvfs-attempt-"), 90_000) // ~1.1 MiB
	bs, st, buf := newStubBlobstoreWithLog(t)

	hasher := sha1.New()
	body := io.TeeReader(io.NopCloser(bytes.NewReader(payload)), hasher)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := bs.Upload(ctx, "s1/blob-log", body, int64(len(payload))); err != nil {
		t.Fatalf("Upload across a killed first attempt: %v", err)
	}
	if got := st.attempts.Load(); got < 2 {
		t.Fatalf("expected a retried PUT, saw %d attempt(s)", got)
	}
	logged := buf.String()
	if !strings.Contains(logged, "blob upload attempt failed") ||
		!strings.Contains(logged, "s1/blob-log") {
		t.Fatalf("first attempt's failure not logged with its blob key; log: %q", logged)
	}
}

// TestAttemptLogTransport_SilentWithoutBlobKey: requests outside an Upload
// (no ctx value) pass through unlogged.
func TestAttemptLogTransport_SilentWithoutBlobKey(t *testing.T) {
	buf := &lockedBuffer{}
	log := zerolog.New(buf)
	rt := &attemptLogTransport{
		next: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return nil, errors.New("boom")
		}),
		log: &log,
	}
	req, _ := http.NewRequest(http.MethodGet, "http://s3.test/x", nil)
	if _, err := rt.RoundTrip(req); err == nil {
		t.Fatal("fake transport should error")
	}
	if buf.String() != "" {
		t.Fatalf("request without a blob key was logged: %q", buf.String())
	}

	req = req.WithContext(withBlobAttemptLog(req.Context(), "s1/k"))
	_, _ = rt.RoundTrip(req)
	if !strings.Contains(buf.String(), "s1/k") {
		t.Fatalf("keyed request's failure not logged: %q", buf.String())
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestUploadLongBodyFailsBeforeS3 guards the sized-buffer staging refactor:
// a reader longer than the declared size must fail before any PUT.
func TestUploadLongBodyFailsBeforeS3(t *testing.T) {
	bs, st := newStubBlobstore(t)
	err := bs.Upload(context.Background(), "s1/blob-3", bytes.NewReader(make([]byte, 20)), 10)
	if err == nil {
		t.Fatal("Upload with a long body succeeded, want a loud staging error")
	}
	if st.attempts.Load() != 0 {
		t.Fatalf("long body reached S3 (%d attempts), want the failure before any PUT", st.attempts.Load())
	}
}

// The attempt log must attribute round trips to the owning TUS session when
// the caller supplies one, and count round trips under that name.
func TestUploadAttemptLogCarriesSession(t *testing.T) {
	payload := bytes.Repeat([]byte("kvfs-session-"), 90_000)
	bs, st, buf := newStubBlobstoreWithLog(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	ctx = WithBlobSession(ctx, "sess-log-1")
	if err := bs.Upload(ctx, "s1/blob-sess", bytes.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatalf("Upload across a killed first attempt: %v", err)
	}
	if got := st.attempts.Load(); got < 2 {
		t.Fatalf("expected a retried PUT, saw %d attempt(s)", got)
	}
	logged := buf.String()
	if !strings.Contains(logged, `"session_id":"sess-log-1"`) {
		t.Fatalf("attempt log does not carry the session id; log: %q", logged)
	}
	if !strings.Contains(logged, `"round_trip"`) {
		t.Fatalf("attempt log does not count round trips; log: %q", logged)
	}
}
