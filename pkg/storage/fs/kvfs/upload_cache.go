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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

// UploadCache is the staging backend for TUS upload bodies. Implementations
// must support append-only writes during streaming and one final read at
// FinishUpload. Each implementation trades off per-chunk latency vs.
// multi-pod transparency and pod-restart resilience differently.
//
// The default implementation `diskUploadCache` mirrors what every other
// reva storage driver (decomposedfs, owncloudsql, posix, cephfs, ocis)
// does: one local file per session, append-only writes via io.Copy, read
// once at FinishUpload, deleted after. Microsecond latency per Append.
//
// An alternative `natsObjectUploadCache` writes via the NATS Object Store
// (R=3 replication, multi-pod transparent at a bandwidth cost). It's
// implemented but defaults off; operators flip to it via the
// STORAGE_USERS_KVFS_UPLOAD_BACKEND env var when they need cross-pod
// resume or pod-restart resilience and accept the ~3x bandwidth
// amplification.
type UploadCache interface {
	// Append writes data at the end of the session's staged body.
	// Implementations must be append-only — random-offset writes are not
	// supported. The TUS handler always calls with offset == current size.
	Append(sessionID string, src io.Reader) (n int64, err error)
	// Size returns the current staged byte count for the session.
	// Used by GetUpload to authoritatively report the resumable offset
	// (the persisted UploadSession.Offset may be slightly stale because we
	// only checkpoint periodically; the cache's view is always live).
	Size(sessionID string) (int64, error)
	// Reader returns the full staged body. Caller must Close.
	Reader(sessionID string) (io.ReadCloser, error)
	// Drop removes all staging state for the session. Idempotent.
	Drop(sessionID string) error
	// Close releases any resources held by the cache (e.g. file handles).
	Close() error
}

// --- Disk backend (default) -------------------------------------------------

// diskUploadCache stages upload bodies as plain files on a pod-local
// directory. Matches decomposedfs / owncloudsql / cephfs pattern.
//
// Trade-offs (documented for operators):
//   - μs-latency Append via the kernel page cache.
//   - No pod-restart resilience (emptyDir loses files on pod kill). Real
//     production reva deployments at scale accept this — TUS lets the
//     client resume from offset 0 if a session vanishes.
//   - Single-pod scope. Sticky sessions (ClientIP affinity, already
//     configured on the opencloud Service) handle the common case. Cross-
//     pod resume after the affinity hash flips is a rare edge case.
//   - Sized for max concurrent upload bytes. With 50 GiB emptyDir per pod
//     we accommodate ~50 simultaneous multi-GB uploads.
type diskUploadCache struct {
	dir          string
	bytesInFlight atomic.Int64 // updated on Append / Drop so a Prom gauge can read it
}

// newDiskUploadCache returns a cache backed by `dir`. `os.MkdirAll(dir, 0o700)`
// is the caller's responsibility (done in kvfsDriver.New).
func newDiskUploadCache(dir string) *diskUploadCache {
	return &diskUploadCache{dir: dir}
}

func (c *diskUploadCache) binPath(sessionID string) string {
	return filepath.Join(c.dir, sessionID+".bin")
}

func (c *diskUploadCache) Append(sessionID string, src io.Reader) (int64, error) {
	f, err := os.OpenFile(c.binPath(sessionID), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return 0, errors.Wrap(err, "diskUploadCache: open temp file")
	}
	defer f.Close()
	n, err := io.Copy(f, src)
	c.bytesInFlight.Add(n)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return n, errors.Wrap(err, "diskUploadCache: io.Copy")
	}
	return n, nil
}

func (c *diskUploadCache) Size(sessionID string) (int64, error) {
	stat, err := os.Stat(c.binPath(sessionID))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, errors.Wrap(err, "diskUploadCache: stat temp file")
	}
	return stat.Size(), nil
}

func (c *diskUploadCache) Reader(sessionID string) (io.ReadCloser, error) {
	f, err := os.Open(c.binPath(sessionID))
	if err != nil {
		return nil, errors.Wrap(err, "diskUploadCache: open temp file for read")
	}
	return f, nil
}

func (c *diskUploadCache) Drop(sessionID string) error {
	// Reclaim the bytesInFlight counter before deletion so the gauge tracks
	// authoritative on-disk usage even if the file system grace-period
	// keeps the bytes around briefly.
	if size, err := c.Size(sessionID); err == nil {
		c.bytesInFlight.Add(-size)
	}
	if err := os.Remove(c.binPath(sessionID)); err != nil && !os.IsNotExist(err) {
		return errors.Wrap(err, "diskUploadCache: remove temp file")
	}
	return nil
}

func (c *diskUploadCache) Close() error { return nil }

// BytesInFlight returns the current total staged bytes across all
// sessions on this pod. Read by the kvfs_temp_file_bytes_in_flight gauge.
func (c *diskUploadCache) BytesInFlight() int64 {
	return c.bytesInFlight.Load()
}

// --- NATS Stream backend (opt-in, cross-pod transparent) ------------------
//
// Each PATCH publishes one message to subject `{subjectPrefix}.{session-id}`.
// JetStream guarantees per-subject order so the subject is append-only by
// construction. Any pod can consume from the shared stream, which is what
// makes cross-pod transparency work; the trade-off is ~5-15 ms per chunk
// vs ~1 ms for local disk, and the in-flight body is bounded by the
// stream's MaxBytes cap. Each message carries an `Upload-Offset` header
// so `Size()` is a single `GetLastMsg` instead of a scan.
//
// NATS Object Store would look simpler, but its Put replaces the whole
// object (no native append), turning each PATCH into Get + concat + Put
// with 3x bandwidth amplification.

// natsStreamUploadCacheOptions holds the per-instance config for the cache.
// Defaults are filled in by Options.SetDefaults — see options.go.
type natsStreamUploadCacheOptions struct {
	// StreamName is the JetStream stream name. The stream is created on
	// first use if missing. Different KVStore instances (storage-users vs.
	// storage-system) get different stream names by prefix.
	StreamName string
	// SubjectPrefix is the subject root for per-session subjects.
	// Subject is `{SubjectPrefix}.{session-id}`.
	SubjectPrefix string
	// Storage is "file" or "memory". File is the default; memory is
	// bounded by the NATS server's `memoryStore.maxSize`, which is
	// typically too small for real upload bodies.
	Storage string
	// Replicas: 1 is the documented default for transient upload bodies.
	Replicas int
	// MaxAge bounds how long an abandoned upload can keep stream storage.
	// Matches UploadSession TTL.
	MaxAge time.Duration
	// MaxBytes caps total stream size across all in-flight uploads on this
	// instance. NATS will reject new publishes when full (Discard=New).
	MaxBytes int64
	// MaxChunkBytes is the largest single message we'll publish. Must be
	// <= NATS server `max_payload`. Larger PATCH bodies are split into
	// multiple messages.
	MaxChunkBytes int
}

// natsStreamUploadCache implements UploadCache against a JetStream Stream.
type natsStreamUploadCache struct {
	js   nats.JetStreamContext
	opts natsStreamUploadCacheOptions
}

// newNATSStreamUploadCache binds (or creates) the stream and returns a cache.
// Caller owns the JetStream context — we share it with KVStore via
// KVStore.JetStream() and never close it ourselves.
func newNATSStreamUploadCache(js nats.JetStreamContext, opts natsStreamUploadCacheOptions) (*natsStreamUploadCache, error) {
	if opts.StreamName == "" {
		return nil, errors.New("natsStreamUploadCache: stream name required")
	}
	if opts.SubjectPrefix == "" {
		return nil, errors.New("natsStreamUploadCache: subject prefix required")
	}
	if opts.MaxChunkBytes <= 0 {
		opts.MaxChunkBytes = 8 * 1024 * 1024 // 8 MiB — well under 16 MiB max_payload
	}
	if opts.Replicas <= 0 {
		opts.Replicas = 1
	}

	storage := nats.FileStorage
	if opts.Storage == "memory" {
		storage = nats.MemoryStorage
	}

	cfg := &nats.StreamConfig{
		Name:        opts.StreamName,
		Description: "kvfs TUS upload staging (cross-pod shared body cache)",
		Subjects:    []string{opts.SubjectPrefix + ".>"},
		Storage:     storage,
		Replicas:    opts.Replicas,
		MaxAge:      opts.MaxAge,
		MaxBytes:    opts.MaxBytes,
		// Discard new writes when full — operators get a clear backpressure
		// signal rather than silent loss of older in-flight uploads.
		Discard: nats.DiscardNew,
		// Per-message hard cap; must stay <= the NATS server's
		// `max_payload`. Caller-side we split larger PATCH bodies before
		// publishing.
		MaxMsgSize: 16 * 1024 * 1024,
	}

	if _, err := js.StreamInfo(opts.StreamName); err != nil {
		// Doesn't exist — create.
		if _, err := js.AddStream(cfg); err != nil {
			return nil, errors.Wrapf(err, "natsStreamUploadCache: create stream %q", opts.StreamName)
		}
	} else {
		// Exists — reconcile config drift (e.g. MaxBytes raised). Update
		// is a no-op if the config matches.
		if _, err := js.UpdateStream(cfg); err != nil {
			// Non-fatal — config-drift updates are best-effort. Log and
			// continue; the existing stream still works.
			fmt.Printf("natsStreamUploadCache: WARN — UpdateStream %q: %v\n", opts.StreamName, err)
		}
	}

	return &natsStreamUploadCache{js: js, opts: opts}, nil
}

func (c *natsStreamUploadCache) subject(sessionID string) string {
	return c.opts.SubjectPrefix + "." + sessionID
}

// Size returns the cumulative byte count for a session by reading the LAST
// message on the subject and inspecting its `Upload-Offset` header + data
// length. One cheap NATS round-trip per call; no full-stream scan.
func (c *natsStreamUploadCache) Size(sessionID string) (int64, error) {
	msg, err := c.js.GetLastMsg(c.opts.StreamName, c.subject(sessionID))
	if err != nil {
		if errors.Is(err, nats.ErrMsgNotFound) {
			return 0, nil
		}
		// Some nats.go versions return a different sentinel for "no
		// matching message". Treat 404-shaped errors as size=0.
		if msg == nil {
			return 0, nil
		}
		return 0, errors.Wrap(err, "natsStreamUploadCache: GetLastMsg")
	}
	var offset int64
	if v := msg.Header.Get("Upload-Offset"); v != "" {
		offset, _ = strconv.ParseInt(v, 10, 64)
	}
	return offset + int64(len(msg.Data)), nil
}

// Append publishes the src bytes as one or more append-only messages on the
// session's subject. Splits into MaxChunkBytes-sized sub-messages to stay
// under NATS `max_payload`. Each sub-message carries the `Upload-Offset`
// header for the data it contains, so Size() is O(1).
func (c *natsStreamUploadCache) Append(sessionID string, src io.Reader) (int64, error) {
	startOffset, err := c.Size(sessionID)
	if err != nil {
		return 0, errors.Wrap(err, "natsStreamUploadCache: probe size")
	}

	subj := c.subject(sessionID)
	buf := make([]byte, c.opts.MaxChunkBytes)
	var totalWritten int64

	for {
		n, readErr := io.ReadFull(src, buf)
		if n > 0 {
			msg := &nats.Msg{
				Subject: subj,
				Data:    append([]byte(nil), buf[:n]...),
				Header:  nats.Header{},
			}
			msg.Header.Set("Upload-Offset", strconv.FormatInt(startOffset+totalWritten, 10))
			if _, err := c.js.PublishMsg(msg); err != nil {
				return totalWritten, errors.Wrap(err, "natsStreamUploadCache: PublishMsg")
			}
			totalWritten += int64(n)
		}
		if readErr == nil {
			continue
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			return totalWritten, nil
		}
		return totalWritten, errors.Wrap(readErr, "natsStreamUploadCache: read src")
	}
}

// Reader streams the session's body in publish order. It opens an ordered
// consumer that delivers messages by stream sequence (which equals publish
// order on a single subject). Reads stop when the consumer reports
// NumPending=0 or hits a quiet period (no more messages pending).
//
// Callers (FinishUpload via blob.Upload) read the full body sequentially and
// push to S3.
func (c *natsStreamUploadCache) Reader(sessionID string) (io.ReadCloser, error) {
	// Quick existence check — if the stream has nothing for this subject,
	// return an empty reader rather than spin up a consumer.
	if _, err := c.js.GetLastMsg(c.opts.StreamName, c.subject(sessionID)); err != nil {
		if errors.Is(err, nats.ErrMsgNotFound) {
			return io.NopCloser(strings.NewReader("")), nil
		}
		return nil, errors.Wrap(err, "natsStreamUploadCache: pre-Reader probe")
	}

	pr, pw := io.Pipe()
	subj := c.subject(sessionID)

	go func() {
		defer pw.Close()

		sub, err := c.js.SubscribeSync(subj,
			nats.OrderedConsumer(),
			nats.DeliverAll(),
		)
		if err != nil {
			_ = pw.CloseWithError(errors.Wrap(err, "natsStreamUploadCache: SubscribeSync"))
			return
		}
		defer sub.Unsubscribe()

		// Ordered consumer redelivers; cap the wait per message — if NATS
		// goes quiet for 30 s we assume there are no more pending messages
		// and finish the stream. The blob.Upload caller then closes the
		// pipe and we're done.
		const idleTimeout = 30 * time.Second
		for {
			msg, err := sub.NextMsg(idleTimeout)
			if err != nil {
				if errors.Is(err, nats.ErrTimeout) {
					return
				}
				_ = pw.CloseWithError(errors.Wrap(err, "natsStreamUploadCache: NextMsg"))
				return
			}
			if _, err := pw.Write(msg.Data); err != nil {
				// Reader closed early (caller aborted). Stop draining.
				return
			}
			meta, mErr := msg.Metadata()
			if mErr == nil && meta != nil && meta.NumPending == 0 {
				return
			}
		}
	}()

	return pr, nil
}

// Drop purges all messages for the session's subject. Idempotent: if the
// subject has no messages (already dropped, or never used), returns nil.
func (c *natsStreamUploadCache) Drop(sessionID string) error {
	req := &nats.StreamPurgeRequest{
		Subject: c.subject(sessionID),
	}
	if err := c.js.PurgeStream(c.opts.StreamName, req); err != nil {
		// PurgeStream returns nil even when no messages matched, but be
		// defensive against version drift.
		return errors.Wrapf(err, "natsStreamUploadCache: PurgeStream subj=%q", c.subject(sessionID))
	}
	return nil
}

// Close releases nothing — the JetStreamContext is owned by KVStore.
func (c *natsStreamUploadCache) Close() error { return nil }
