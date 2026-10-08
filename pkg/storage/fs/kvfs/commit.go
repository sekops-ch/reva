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
	"io"
	"math"
	"time"

	"github.com/google/uuid"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	ctxpkg "github.com/opencloud-eu/reva/v2/pkg/ctx"
	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
	"github.com/opencloud-eu/reva/v2/pkg/events"
	"github.com/opencloud-eu/reva/v2/pkg/mime"
	"github.com/opencloud-eu/reva/v2/pkg/storage"
	"github.com/opencloud-eu/reva/v2/pkg/utils"
	"github.com/pkg/errors"
)

type commitFileParams struct {
	SpaceID     string
	ParentID    string
	Name        string
	BlobID      string
	Size        int64
	Checksum    string
	MimeType    string
	IfMatchEtag string
	OwnerID     string
	ClientMTime string // the client's modification time, as parseClientMTime reads it
	LockID      string // the lock id an overwrite must present

	// RequireSnapshot refuses the commit when the replaced content cannot be kept as a version.
	RequireSnapshot bool
}

// emptyChecksum is the SHA-1 of no bytes.
const emptyChecksum = "sha1:da39a3ee5e6b4b0d3255bfef95601890afd80709"

// commitEmpty commits an empty file at the upload target. It requires the snapshot: an empty
// save is the one an editor sends by mistake, so the replaced content must stay restorable.
func (d *kvfsDriver) commitEmpty(ctx context.Context, t *uploadTarget, params simpleUploadParams) error {
	u := ctxpkg.ContextMustGetUser(ctx)
	commitCtx, cancel := d.commitPhase(ctx)
	defer cancel()
	nodeID, err := d.commitFileNode(commitCtx, commitFileParams{
		SpaceID:         t.spaceID,
		ParentID:        t.parent.ID,
		Name:            t.name,
		Checksum:        emptyChecksum,
		MimeType:        mime.Detect(false, t.name),
		IfMatchEtag:     params.IfMatch,
		OwnerID:         u.Id.OpaqueId,
		ClientMTime:     params.MTime,
		LockID:          params.LockID,
		RequireSnapshot: true,
	})
	if err != nil {
		return err
	}
	d.publishEvent(commitCtx, func() interface{} {
		return events.FileUploaded{
			SpaceOwner: u.Id,
			Executant:  u.Id,
			Ref:        spaceRef(t.spaceID, nodeID),
			Owner:      u.Id,
			Timestamp:  nowTimestamp(),
		}
	})
	return nil
}

// parseClientMTime reads a client's modification time, "<seconds>[.<fraction>]" with the
// fraction taken as nanoseconds as in utils.MTimeToTime, as Unix nanoseconds. ok is false for
// an absent, unparsable or negative time and for one past what int64 nanoseconds hold.
func parseClientMTime(v string) (ns int64, ok bool) {
	if v == "" {
		return 0, false
	}
	t, err := utils.MTimeToTime(v)
	if err != nil || t.Before(time.Unix(0, 0)) || t.After(time.Unix(0, math.MaxInt64)) {
		return 0, false
	}
	return t.UnixNano(), true
}

// Upload creates or updates a resource with new content.
func (d *kvfsDriver) Upload(ctx context.Context, req storage.UploadRequest, uff storage.UploadFinishedFunc) (*provider.ResourceInfo, error) {
	ref := req.Ref

	var params simpleUploadParams
	if ref.GetResourceId() == nil || ref.GetResourceId().GetSpaceId() == "" {
		var parsedRef *provider.Reference
		var err error
		parsedRef, params, err = d.parseUploadPath(ref.GetPath())
		if err != nil {
			return nil, err
		}
		ref = parsedRef
	}

	spaceID, parentNode, name, err := d.resolveParent(ctx, ref)
	if err != nil {
		return nil, err
	}
	parentID := parentNode.ID

	u := ctxpkg.ContextMustGetUser(ctx)
	if u == nil {
		return nil, errtypes.InternalError("kvfs: no user in context")
	}
	// The data request's lock id (X-Lock-Id) wins over the one carried from initiation.
	lockID, _ := ctxpkg.ContextGetLockID(ctx)
	if lockID == "" {
		lockID = params.LockID
	}
	rp := d.assemblePermissions(ctx, spaceID, parentNode)
	if !rp.InitiateFileUpload {
		// For overwrites, check the existing file's permissions (file-level grants
		// from OCS shares are on the file node, not the parent).
		allowed := false
		children, _, _ := d.store.GetChildren(spaceID, parentID)
		if existingID, ok := children[name]; ok {
			if existingNode, _, err := d.store.GetNode(spaceID, existingID); err == nil {
				fileRP := d.assemblePermissions(ctx, spaceID, existingNode)
				if fileRP.InitiateFileUpload {
					allowed = true
				} else if fileRP.Stat {
					return nil, errtypes.PermissionDenied(ref.String())
				}
			}
		}
		if !allowed {
			if rp.Stat {
				return nil, errtypes.PermissionDenied(ref.String())
			}
			return nil, errtypes.NotFound(ref.String())
		}
	}

	// Quota check before uploading blob to S3
	{
		var oldFileSize int64
		isOverwrite := false
		qChildren, _, _ := d.store.GetChildren(spaceID, parentID)
		if existingID, exists := qChildren[name]; exists {
			isOverwrite = true
			if existingNode, _, err := d.store.GetNode(spaceID, existingID); err == nil {
				oldFileSize = existingNode.Size
			}
		}
		if err := d.checkQuota(spaceID, req.Length, isOverwrite, oldFileSize); err != nil {
			return nil, err
		}
	}

	blobID := uuid.New().String()

	// Detached commit context: from blob upload through commitFileNode,
	// the request ctx must not be allowed to cancel mid-operation —
	// see [kvfsDriver.commitPhase].
	commitCtx, cancel := d.commitPhase(ctx)
	defer cancel()

	// Upload blob to S3, computing SHA-1 checksum while streaming.
	// Note: req.Body still reads from the original ctx-bound stream;
	// commitCtx only governs the outbound S3 PUT side.
	hasher := sha1.New()
	teeBody := io.TeeReader(req.Body, hasher)
	d.log.Debug().Str("blob_key", BlobKey(spaceID, blobID)).Int64("length", req.Length).Msg("Upload: uploading blob to S3")
	if err := d.blob.Upload(commitCtx, BlobKey(spaceID, blobID), teeBody, req.Length); err != nil {
		d.log.Error().Err(err).Msg("Upload: blob upload failed")
		return nil, errors.Wrap(err, "kvfs: failed to upload blob")
	}
	checksum := "sha1:" + hex.EncodeToString(hasher.Sum(nil))

	mimeType := mime.Detect(false, name)

	UploadInFlight.WithLabelValues("simple").Inc()
	defer UploadInFlight.WithLabelValues("simple").Dec()

	nodeID, err := d.commitFileNode(commitCtx, commitFileParams{
		SpaceID:     spaceID,
		ParentID:    parentID,
		Name:        name,
		BlobID:      blobID,
		Size:        req.Length,
		Checksum:    checksum,
		MimeType:    mimeType,
		IfMatchEtag: params.IfMatch,
		OwnerID:     u.Id.OpaqueId,
		ClientMTime: params.MTime,
		LockID:      lockID,
	})
	if err != nil {
		// Rollback the blob so we don't leave an orphan in S3. Use
		// commitCtx so this cleanup isn't itself short-circuited by a
		// canceled request ctx.
		if delErr := d.blob.Delete(commitCtx, BlobKey(spaceID, blobID)); delErr != nil {
			d.log.Warn().Err(delErr).Str("blob_id", blobID).Msg("Upload: rollback blob delete failed — leaving for GC")
		}
		return nil, err
	}

	// The simple commit succeeded, so the TUS session minted by the same
	// InitiateUpload will never be used — reap it now instead of leaving
	// it for the TTL reaper. Best-effort: DeleteUpload tolerates a
	// missing key, and on failure the reaper still collects it.
	if params.TUSSibling != "" {
		if delErr := d.store.DeleteUpload(params.TUSSibling); delErr != nil {
			d.log.Warn().Err(delErr).Str("session_id", params.TUSSibling).Msg("Upload: failed to reap sibling TUS session — leaving for the TTL reaper")
		} else {
			UploadInFlight.WithLabelValues("tus").Dec()
		}
	}

	d.publishEvent(commitCtx, func() interface{} {
		return events.FileUploaded{
			SpaceOwner: u.Id,
			Executant:  u.Id,
			Ref:        spaceRef(spaceID, nodeID),
			Owner:      u.Id,
			Timestamp:  nowTimestamp(),
		}
	})

	// NATS JetStream KV's materialized view can lag the stream commit by a
	// short window under hot-parent concurrent writes. Without this retry
	// the next line dereferences a nil node and per-request recover()
	// returns 500 with a panic stack trace — clean error is better.
	node, _, err := d.store.GetNode(spaceID, nodeID)
	if err != nil || node == nil {
		for retry := 0; retry < 5; retry++ {
			time.Sleep(time.Duration(10*(1<<retry)) * time.Millisecond)
			if n, _, e := d.store.GetNode(spaceID, nodeID); e == nil && n != nil {
				node = n
				err = nil
				break
			}
		}
	}
	if node == nil {
		return nil, errors.Wrap(err, "kvfs: node not visible after commit")
	}
	ri := node.ToResourceInfo(name)
	// Stat replies get the mount id from the storage provider; this reply bypasses it.
	ri.Id.StorageId = d.opts.MountID
	return ri, nil
}

func (d *kvfsDriver) commitFileNode(ctx context.Context, p commitFileParams) (string, error) {
	now := time.Now().UnixNano()
	// The file keeps the client's date; the etag stays unique per commit.
	mtime := now
	if ns, ok := parseClientMTime(p.ClientMTime); ok {
		mtime = ns
	}

	// First decide: overwrite of an existing entry, or create of a new one?
	// We probe the children map once; the overwrite path still uses a node-CAS
	// retry loop (it must touch a specific existing node revision). The create
	// path delegates to applyChildIntent which handles the children-map CAS
	// internally with merge-on-conflict, so concurrent same-parent uploads no
	// longer thrash on a full re-read of the children map.

	children, _, err := d.store.GetChildren(p.SpaceID, p.ParentID)
	if err != nil {
		return "", err
	}

	if existingID, exists := children[p.Name]; exists {
		// Idempotency guard: when a prior FinishUpload committed but the
		// HTTP response was lost (504), the client retries with a new
		// session carrying the old If-Match etag. The node's etag moved
		// forward so the precondition check would fail. Detect by content
		// identity: same checksum + size means the desired state is
		// already achieved. Return success without re-versioning or
		// re-propagating tree size.
		if p.IfMatchEtag != "" && p.Checksum != "" {
			if node, _, err := d.store.GetNode(p.SpaceID, existingID); err == nil {
				if !etagMatches(p.IfMatchEtag, node.ETag) &&
					node.Checksum == p.Checksum &&
					node.Size == p.Size {
					IdempotentOverwriteDetected.Inc()
					d.log.Info().
						Str("node_id", existingID).
						Str("checksum", p.Checksum).
						Int64("size", p.Size).
						Str("stale_etag", p.IfMatchEtag).
						Str("current_etag", node.ETag).
						Msg("kvfs: idempotent overwrite — content already matches")
					return existingID, nil
				}
			}
		}

		// Overwrite path: CAS-update the existing node; its children map is unchanged.
		// An attempt keeps the head it meets as a version unless the previous attempt
		// met the same content, so a CAS retry also keeps a racing writer's content.
		metBlob := ""
		snapshotted := false
		var sizeDelta int64
		if err := d.casRetryLoop("upload", func() error {
			existingNode, nodeRev, err := d.store.GetNode(p.SpaceID, existingID)
			if err != nil {
				return err
			}
			if existingNode.Type != NodeTypeFile {
				return errtypes.PreconditionFailed("kvfs: the upload target is a folder")
			}

			if p.IfMatchEtag != "" && !etagMatches(p.IfMatchEtag, existingNode.ETag) {
				return errtypes.Aborted("if-match etag mismatch")
			}
			if err := checkLockID(p.LockID, existingNode); err != nil {
				return err
			}

			if !d.opts.DisableVersioning && existingNode.BlobID != "" && existingNode.BlobID != metBlob {
				version := &VersionEntry{
					Key:      uuid.New().String(),
					NodeID:   existingID,
					SpaceID:  p.SpaceID,
					BlobID:   existingNode.BlobID,
					BlobSize: existingNode.BlobSize,
					MTime:    existingNode.MTime,
					ETag:     existingNode.ETag,
					Size:     existingNode.Size,
					Checksum: existingNode.Checksum,

					// Taken after reading the head, so it sorts after the version the head's writer kept.
					SnapshotTime: time.Now().UnixNano(),
				}
				if err := d.store.PutVersion(version); err != nil {
					if p.RequireSnapshot {
						return errors.Wrap(err, "kvfs: failed to keep the replaced content as a version")
					}
					d.log.Warn().Err(err).Str("node_id", existingID).Msg("failed to create version entry")
				}
				snapshotted = true
			}
			metBlob = existingNode.BlobID

			sizeDelta = p.Size - existingNode.Size
			existingNode.BlobID = p.BlobID
			existingNode.BlobSize = p.Size
			existingNode.Size = p.Size
			existingNode.MTime = mtime
			existingNode.ETag = calculateEtag(existingID, now)
			existingNode.MimeType = p.MimeType
			existingNode.Checksum = p.Checksum
			existingNode.Processing = false

			return d.store.PutNode(existingNode, nodeRev)
		}); err != nil {
			if err == ErrCASConflict {
				return "", errors.New("kvfs: file overwrite failed after max CAS retries")
			}
			return "", err
		}

		// propagateTreeSize updates Size + MTime + ETag on the parent
		// and every ancestor via propagateOneAncestor.
		d.propagateTreeSize(ctx, p.SpaceID, p.ParentID, sizeDelta)
		// Trim only after the new head is committed: a failed overwrite keeps every version.
		if snapshotted {
			d.trimVersions(ctx, p.SpaceID, existingID)
		}
		return existingID, nil
	}

	// Create path: allocate the node first, then atomically add to parent's
	// children map via the intent helper.
	//
	// PutNode is retried with a fresh UUID on CAS conflict — UUID collisions
	// are vanishingly rare in practice but the unit suite exercises this
	// path via mock injection, so we defend it.
	var nodeID string
	if err := d.casRetryLoop("upload", func() error {
		nodeID = uuid.New().String()
		node := &NodeEntry{
			ID: nodeID, SpaceID: p.SpaceID, ParentID: p.ParentID,
			Name: p.Name, Type: NodeTypeFile,
			BlobID: p.BlobID, BlobSize: p.Size, Size: p.Size,
			MTime: mtime, ETag: calculateEtag(nodeID, now),
			Owner: p.OwnerID, MimeType: p.MimeType, Checksum: p.Checksum,
		}
		return d.store.PutNode(node, 0)
	}); err != nil {
		if err == ErrCASConflict {
			return "", errors.New("kvfs: file commit failed after max CAS retries")
		}
		return "", err
	}

	if _, err := d.applyChildIntent(p.SpaceID, p.ParentID, map[string]string{p.Name: nodeID}, nil); err != nil {
		d.store.DeleteNode(p.SpaceID, nodeID)
		CASExhausted.WithLabelValues("upload").Inc()
		return "", err
	}

	// propagateTreeSize covers the parent and all ancestors.
	d.propagateTreeSize(ctx, p.SpaceID, p.ParentID, p.Size)
	return nodeID, nil
}
