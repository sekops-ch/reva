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
	"crypto/sha1"
	"encoding/hex"
	"hash"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	tusd "github.com/tus/tusd/v2/pkg/handler"

	ctxpkg "github.com/opencloud-eu/reva/v2/pkg/ctx"
	"github.com/opencloud-eu/reva/v2/pkg/events"
	"github.com/opencloud-eu/reva/v2/pkg/mime"
)

// kvfsUpload implements the tusd.Upload interface for one TUS session.
//
// WriteChunk appends body bytes to a local-disk staging cache (no remote
// I/O per chunk, matching decomposedfs / posix / cephfs / ocis). S3 sees
// the body once, in FinishUpload, via a single blob.Upload — minio-go
// picks single-PUT vs multipart internally based on size.
type kvfsUpload struct {
	session *UploadSession
	driver  *kvfsDriver
	hasher  hash.Hash
}

// WriteChunk appends bytes from src to the session's staging cache.
// Returns in microseconds for the disk backend. The body stream never
// stalls waiting for remote I/O.
//
// The session's Offset is updated in memory but NOT persisted to NATS
// per-chunk — the persisted UploadSession is allowed to be slightly
// stale because GetUpload reconciles the offset from the cache's
// authoritative size. PutUpload happens once at FinishUpload (then
// immediately DeleteUpload). This is the same checkpointing model
// decomposedfs uses ([decomposedfs/upload/upload.go:91]).
func (u *kvfsUpload) WriteChunk(ctx context.Context, offset int64, src io.Reader) (int64, error) {
	start := time.Now()
	defer func() { TUSPhaseDuration.WithLabelValues("write_chunk").Observe(time.Since(start).Seconds()) }()

	if u.hasher == nil {
		u.hasher = sha1.New()
	}
	teeReader := io.TeeReader(src, u.hasher)

	n, err := u.driver.uploadCache.Append(u.session.ID, offset, teeReader)
	if err != nil {
		// Context cancellation isn't a hard error here — partial chunks
		// stay on disk. The client retries (TUS resume) and finds the
		// authoritative offset via Size() at GetUpload time.
		if ctxErr := ctx.Err(); ctxErr == context.Canceled || ctxErr == context.DeadlineExceeded {
			UploadAbortedOnCancel.Inc()
			u.driver.log.Info().
				Str("session_id", u.session.ID).
				Int64("partial_bytes", n).
				Msg("kvfs: WriteChunk context cancelled — partial chunk retained on disk for resume")
			// Return the bytes we did write so tusd records the offset correctly.
			u.session.Offset += n
			return n, nil
		}
		return n, errors.Wrap(err, "kvfs: WriteChunk failed")
	}

	u.session.Offset += n
	return n, nil
}

// GetInfo returns the tusd FileInfo for this upload session.
func (u *kvfsUpload) GetInfo(ctx context.Context) (tusd.FileInfo, error) {
	return tusd.FileInfo{
		ID:             u.session.ID,
		Size:           u.session.Size,
		SizeIsDeferred: u.session.SizeIsDeferred,
		Offset:         u.session.Offset,
		MetaData:       u.session.Storage,
		Storage: map[string]string{
			"SpaceRoot":  u.session.SpaceID,
			"NodeId":     u.session.NodeID,
			"BlobId":     u.session.BlobID,
			"ParentId":   u.session.ParentID,
			"Filename":   u.session.Filename,
			"providerID": u.session.SpaceID,
		},
	}, nil
}

// GetReader returns a reader for the upload content. When the upload is
// still in progress it reads from the staging cache. The reva ocdav
// handler doesn't actually use this until after FinishUpload, so the
// cache path is the common case.
func (u *kvfsUpload) GetReader(ctx context.Context) (io.ReadCloser, error) {
	if u.session.Offset < u.session.Size && !u.session.SizeIsDeferred {
		return nil, tusd.ErrNotFound
	}
	// If the staging cache still has the body, prefer it (avoids an S3
	// round-trip when the consumer is co-located).
	if rc, err := u.driver.uploadCache.Reader(u.session.ID); err == nil {
		return rc, nil
	}
	// Cache already dropped (FinishUpload ran) — fall back to S3.
	blobKey := BlobKey(u.session.SpaceID, u.session.BlobID)
	return u.driver.blob.Download(ctx, blobKey)
}

// FinishUpload streams the staged body to S3, commits the file node, and
// cleans up the session entry + staging cache. Cleanup only runs when the
// whole commit succeeded; on any failure the session and staged bytes
// stay put so a subsequent attempt (client resume or TTL reaper) can
// retry. The commit is idempotent: S3 PUT on the same blob key overwrites
// and commitFileNode handles the "node already exists" path as an update
// via the existing CAS loop.
func (u *kvfsUpload) FinishUpload(ctx context.Context) error {
	finishStart := time.Now()
	defer func() { TUSPhaseDuration.WithLabelValues("finish").Observe(time.Since(finishStart).Seconds()) }()

	// Detached commit context: ignores request-ctx cancellation but
	// preserves ctx values (tracing, user) and adds a bounded ceiling.
	// See [kvfsDriver.commitPhase].
	commitCtx, cancel := u.driver.commitPhase(ctx)
	defer cancel()

	// Tag every commit attempt so the log can be grep'd by session ID to
	// reconstruct the timeline against kvfs_tus_phase_duration_seconds.
	reqCtxErr := ctx.Err()
	u.driver.log.Info().
		Str("session_id", u.session.ID).
		Str("space_id", u.session.SpaceID).
		Str("parent_id", u.session.ParentID).
		Int64("size", u.session.Offset).
		Bool("legacy_multipart", u.session.S3MultipartID != "").
		AnErr("req_ctx_err", reqCtxErr).
		Msg("kvfs: FinishUpload entry")

	// Conditional cleanup defer — the load-bearing fix for the orphaned-
	// blob class of bug. The session row and staged bytes are wiped ONLY
	// when the full commit succeeded. On any failure (including ctx
	// timeout in the commit phase, S3 errors, commitNode CAS exhaustion)
	// the session lives until either the next retry commits it or the
	// uploadSessionTTL reaper clears it.
	var commitSucceeded bool
	defer func() {
		if !commitSucceeded {
			u.driver.log.Warn().
				Str("session_id", u.session.ID).
				Msg("kvfs: FinishUpload did not commit — session + cache preserved for retry")
			return
		}
		if err := u.driver.uploadCache.Drop(u.session.ID); err != nil {
			u.driver.log.Warn().Err(err).Str("session_id", u.session.ID).Msg("kvfs: failed to drop upload cache entry")
		}
		u.driver.store.DeleteUpload(u.session.ID)
		UploadInFlight.WithLabelValues("tus").Dec()
	}()

	blobKey := BlobKey(u.session.SpaceID, u.session.BlobID)

	// Legacy compat for sessions created before disk-staging: if
	// S3MultipartID is set, finalize via the multipart-complete API.
	// New sessions never set it.
	if u.session.S3MultipartID != "" {
		blobStart := time.Now()
		err := u.driver.blob.CompleteMultipartUpload(commitCtx, blobKey, u.session.S3MultipartID, u.session.Parts)
		TUSPhaseDuration.WithLabelValues("blob_upload").Observe(time.Since(blobStart).Seconds())
		if err != nil {
			u.driver.log.Error().Err(err).Str("session_id", u.session.ID).Msg("kvfs: legacy multipart complete failed")
			return errors.Wrap(err, "kvfs: FinishUpload failed to complete legacy multipart")
		}
	} else {
		// Modern path: stream the staged body to S3 in a single Upload call.
		// minio-go's PutObject internally splits to multipart for sizes
		// over its threshold, so we don't need our own size discrimination.
		reader, err := u.driver.uploadCache.Reader(u.session.ID)
		if err != nil {
			return errors.Wrap(err, "kvfs: temp file missing at FinishUpload")
		}
		defer reader.Close()

		blobStart := time.Now()
		err = u.driver.blob.Upload(commitCtx, blobKey, reader, u.session.Offset)
		blobDur := time.Since(blobStart)
		TUSPhaseDuration.WithLabelValues("blob_upload").Observe(blobDur.Seconds())
		if err != nil {
			u.driver.log.Error().Err(err).Str("session_id", u.session.ID).Dur("dur", blobDur).Msg("kvfs: blob upload failed")
			return errors.Wrap(err, "kvfs: blob upload failed")
		}
		u.driver.log.Debug().Str("session_id", u.session.ID).Dur("dur", blobDur).Int64("size", u.session.Offset).Msg("kvfs: blob upload ok")
	}

	var checksum string
	if u.hasher != nil {
		checksum = "sha1:" + hex.EncodeToString(u.hasher.Sum(nil))
	}

	if hook := u.driver.commitFailHook; hook != nil {
		u.driver.log.Warn().Str("session_id", u.session.ID).Msg("kvfs: commit-fail test seam fired")
		return hook()
	}

	commitStart := time.Now()
	err := u.commitNode(commitCtx, checksum)
	commitDur := time.Since(commitStart)
	TUSPhaseDuration.WithLabelValues("commit_node").Observe(commitDur.Seconds())
	if err != nil {
		u.driver.log.Error().Err(err).Str("session_id", u.session.ID).Dur("dur", commitDur).Msg("kvfs: commitNode failed — rolling back blob")
		// Use commitCtx so the cleanup itself isn't cancelled by the
		// already-canceled request ctx.
		if delErr := u.driver.blob.Delete(commitCtx, blobKey); delErr != nil {
			u.driver.log.Warn().Err(delErr).Str("blob_key", blobKey).Msg("kvfs: rollback blob delete failed — leaving for GC")
		}
		return err
	}

	// Capture the user from ctx now, while the request is still alive.
	// publishEvent dispatches to the eventPublisher goroutine, where ctx
	// has been canceled and ContextMustGetUser would panic. Using the
	// non-panicking variant lets unauthenticated paths (which shouldn't
	// reach FinishUpload at all) drop the event instead of crash the pod.
	executant, _ := ctxpkg.ContextGetUser(ctx)
	u.driver.publishEvent(commitCtx, func() interface{} {
		if executant == nil {
			return nil
		}
		ref := spaceRef(u.session.SpaceID, u.session.ParentID)
		ref.Path = u.session.Filename
		return events.FileUploaded{
			SpaceOwner: executant.Id,
			Executant:  executant.Id,
			Ref:        ref,
			Owner:      executant.Id,
			Timestamp:  nowTimestamp(),
		}
	})

	commitSucceeded = true
	u.driver.log.Info().
		Str("session_id", u.session.ID).
		Dur("total_dur", time.Since(finishStart)).
		Dur("commit_node_dur", commitDur).
		Msg("kvfs: FinishUpload committed")

	return nil
}

// Terminate aborts the upload: drops the staging cache and deletes the
// session. Legacy sessions also abort the S3 multipart upload.
func (u *kvfsUpload) Terminate(ctx context.Context) error {
	if u.session.S3MultipartID != "" {
		blobKey := BlobKey(u.session.SpaceID, u.session.BlobID)
		u.driver.blob.AbortMultipartUpload(ctx, blobKey, u.session.S3MultipartID)
	}
	if err := u.driver.uploadCache.Drop(u.session.ID); err != nil {
		u.driver.log.Warn().Err(err).Str("session_id", u.session.ID).Msg("kvfs: failed to drop upload cache entry on terminate")
	}
	u.driver.store.DeleteUpload(u.session.ID)
	UploadInFlight.WithLabelValues("tus").Dec()
	return nil
}

// DeclareLength sets the total upload size for deferred-length uploads.
func (u *kvfsUpload) DeclareLength(ctx context.Context, length int64) error {
	u.session.Size = length
	u.session.SizeIsDeferred = false
	return u.driver.store.PutUpload(u.session)
}

// commitNode creates or updates the file node in the KV store after the
// blob upload is complete. Unchanged from the previous design — the
// metadata-commit phase is orthogonal to the body-staging refactor.
func (u *kvfsUpload) commitNode(ctx context.Context, checksum string) error {
	spaceID := u.session.SpaceID
	parentID := u.session.ParentID
	name := u.session.Filename
	blobID := u.session.BlobID
	totalSize := u.session.Offset
	mimeType := mime.Detect(false, name)

	// Defense-in-depth quota check: concurrent uploads may have filled quota
	// since InitiateUpload. Uses totalSize (actual bytes received).
	{
		var oldFileSize int64
		isOverwrite := false
		qChildren, _, err := u.driver.store.GetChildren(spaceID, parentID)
		if err == nil {
			if existingID, exists := qChildren[name]; exists {
				isOverwrite = true
				if existingNode, _, err := u.driver.store.GetNode(spaceID, existingID); err == nil {
					oldFileSize = existingNode.Size
				}
			}
		}
		if err := u.driver.checkQuota(spaceID, totalSize, isOverwrite, oldFileSize); err != nil {
			return err
		}
	}

	_, err := u.driver.commitFileNode(ctx, commitFileParams{
		SpaceID:     spaceID,
		ParentID:    parentID,
		Name:        name,
		BlobID:      blobID,
		Size:        totalSize,
		Checksum:    checksum,
		MimeType:    mimeType,
		IfMatchEtag: u.session.IfMatchEtag,
		OwnerID:     u.session.OwnerID,
	})
	return err
}

// --- kvfsDriver TUS DataStore interface ---

// UseIn tells the TUS upload middleware which extensions the kvfs driver supports.
func (d *kvfsDriver) UseIn(composer *tusd.StoreComposer) {
	composer.UseCore(d)
	composer.UseTerminater(d)
	composer.UseLengthDeferrer(d)
}

// NewUpload is called by the TUS handler for POST requests. In the reva/ocdav
// pattern, session creation is done via InitiateUpload (CS3 API), so this
// returns an error directing callers to use the CS3 path.
func (d *kvfsDriver) NewUpload(ctx context.Context, info tusd.FileInfo) (tusd.Upload, error) {
	return nil, errors.New("kvfs: use InitiateUpload (CS3 API) to start a new upload")
}

// GetUpload retrieves an existing TUS upload session by ID, reconciling
// the offset from the staging cache's authoritative size in case the
// persisted offset is stale (we don't checkpoint per chunk — see
// WriteChunk).
//
// Replicated KV reads can lag a just-ack'd PutUpload by a short
// cross-replica convergence window; a cross-pod request that races it
// gets ErrNotFound and tusd answers 404. The driver deliberately adds
// no read-your-write retry here — consistency handling belongs to the
// caller, and decomposedfs behaves the same way.
func (d *kvfsDriver) GetUpload(ctx context.Context, id string) (tusd.Upload, error) {
	session, err := d.store.GetUpload(id)
	if err != nil {
		if err == ErrNotFound {
			return nil, tusd.ErrNotFound
		}
		return nil, err
	}
	// Authoritative offset = bytes staged on disk. The persisted Offset
	// may be stale; the cache is always live (until FinishUpload drops it).
	if size, err := d.uploadCache.Size(id); err == nil && size > session.Offset {
		session.Offset = size
	}
	return &kvfsUpload{session: session, driver: d}, nil
}

// AsTerminatableUpload returns the upload as a TerminatableUpload.
func (d *kvfsDriver) AsTerminatableUpload(upload tusd.Upload) tusd.TerminatableUpload {
	return upload.(*kvfsUpload)
}

// AsLengthDeclarableUpload returns the upload as a LengthDeclarableUpload.
func (d *kvfsDriver) AsLengthDeclarableUpload(upload tusd.Upload) tusd.LengthDeclarableUpload {
	return upload.(*kvfsUpload)
}

// createTUSSession allocates a new TUS upload session. No S3 multipart
// state is created up front — the body is staged in the upload cache by
// WriteChunk and pushed to S3 once at FinishUpload.
func (d *kvfsDriver) createTUSSession(ctx context.Context, spaceID, parentID, name string, size int64, sizeIsDeferred bool, ifMatchEtag string) (string, error) {
	start := time.Now()
	defer func() { TUSPhaseDuration.WithLabelValues("initiate").Observe(time.Since(start).Seconds()) }()

	u := ctxpkg.ContextMustGetUser(ctx)
	sessionID := uuid.New().String()
	blobID := uuid.New().String()

	session := &UploadSession{
		ID:             sessionID,
		SpaceID:        spaceID,
		Filename:       name,
		ParentID:       parentID,
		Size:           size,
		Offset:         0,
		Storage:        map[string]string{},
		Expires:        time.Now().Add(uploadSessionTTL).Unix(),
		BlobID:         blobID,
		S3MultipartID:  "", // populated only by legacy pre-disk-cache sessions
		Parts:          nil,
		SizeIsDeferred: sizeIsDeferred,
		IfMatchEtag:    ifMatchEtag,
		OwnerID:        u.Id.OpaqueId,
	}

	if err := d.store.PutUpload(session); err != nil {
		return "", errors.Wrap(err, "kvfs: failed to persist TUS session")
	}

	UploadInFlight.WithLabelValues("tus").Inc()
	d.log.Debug().Str("session_id", sessionID).Str("blob_id", blobID).Str("space_id", spaceID).Str("name", name).Int64("size", size).Msg("TUS upload session created")
	return sessionID, nil
}
