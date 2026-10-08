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
	"net/url"
	"strings"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	ctxpkg "github.com/opencloud-eu/reva/v2/pkg/ctx"
	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
	"github.com/opencloud-eu/reva/v2/pkg/storagespace"
	"github.com/opencloud-eu/reva/v2/pkg/utils"
)

// InitiateUpload returns protocols for initiating an upload.
// Returns both "simple" (PUT) and "tus" (resumable) protocols.
func (d *kvfsDriver) InitiateUpload(ctx context.Context, ref *provider.Reference, uploadLength int64, metadata map[string]string) (map[string]string, error) {
	rid := ref.GetResourceId()
	if rid == nil || rid.SpaceId == "" {
		return nil, errtypes.BadRequest("kvfs: InitiateUpload requires a resource ID with space ID")
	}

	formattedRef := storagespace.FormatResourceID(rid)
	relPath := ref.GetPath()
	if relPath == "" {
		relPath = "."
	}

	// Direct file reference (e.g. from sharesstorageprovider for file-level
	// shares): OpaqueId points to a file node, path is ".".  URL path
	// normalization strips "/.", so re-encode as space-root + full path.
	if (relPath == "." || relPath == "/") && rid.OpaqueId != "" && rid.OpaqueId != rid.SpaceId {
		if fullPath, err := d.buildPath(ctx, rid.SpaceId, rid.OpaqueId); err == nil && fullPath != "" {
			rootID := &provider.ResourceId{StorageId: rid.StorageId, SpaceId: rid.SpaceId, OpaqueId: rid.SpaceId}
			formattedRef = storagespace.FormatResourceID(rootID)
			relPath = fullPath
		}
	}

	lockID, _ := ctxpkg.ContextGetLockID(ctx)
	params := simpleUploadParams{IfMatch: storedIfMatch(metadata["if-match"]), MTime: metadata["mtime"], LockID: lockID}
	result := map[string]string{
		"simple": encodeSimpleUploadID(formattedRef, relPath, params),
	}

	// TUS protocol: create a persistent upload session backed by S3 multipart
	t, err := d.resolveUploadTarget(ctx, ref)
	if err != nil {
		// A missing parent can be read lag after a fresh MKCOL: a non-empty upload still gets the
		// simple protocol, whose commit resolves and authorizes again. Anything else fails closed.
		if _, notFound := err.(errtypes.IsNotFound); notFound && uploadLength > 0 {
			d.log.Warn().Err(err).Msg("InitiateUpload: parent not found, TUS protocol unavailable")
			return result, nil
		}
		return nil, err
	}

	// A parent without write access still allows overwriting a file that grants it itself (OCS
	// file shares grant on the file).
	if !d.assemblePermissions(ctx, t.spaceID, t.parent).InitiateFileUpload &&
		(t.existing == nil || !d.assemblePermissions(ctx, t.spaceID, t.existing).InitiateFileUpload) {
		return nil, errtypes.PermissionDenied(ref.String())
	}
	if t.existing != nil && t.existing.Type != NodeTypeFile {
		return nil, errtypes.PreconditionFailed("kvfs: the upload target is a folder")
	}
	if t.existing != nil {
		if err := checkLockID(lockID, t.existing); err != nil {
			return nil, err
		}
	}

	// An empty upload has no data transfer: it commits here, as in decomposedfs.
	if uploadLength == 0 {
		if err := d.commitEmpty(ctx, t, params); err != nil {
			return nil, err
		}
		return result, nil
	}

	var oldFileSize int64
	if t.existing != nil {
		oldFileSize = t.existing.Size
	}
	if err := d.checkQuota(t.spaceID, uploadLength, t.existing != nil, oldFileSize); err != nil {
		return nil, err
	}

	tusID, err := d.createTUSSession(ctx, tusSessionParams{
		SpaceID: t.spaceID, ParentID: t.parent.ID, Name: t.name, Size: uploadLength,
		IfMatch: params.IfMatch, ClientMTime: params.MTime, LockID: params.LockID,
	})
	if err != nil {
		d.log.Warn().Err(err).Msg("InitiateUpload: failed to create TUS session, TUS protocol unavailable")
		return result, nil
	}

	result["tus"] = tusID

	// The caller picks ONE of the offered protocols, but the TUS session
	// above is already persisted. Thread its ID through the simple ID so
	// a simple-PUT commit can reap its never-used sibling — otherwise
	// every simple upload strands one session until the TTL reaper runs.
	params.TUSSibling = tusID
	result["simple"] = encodeSimpleUploadID(formattedRef, relPath, params)

	return result, nil
}

// uploadTarget is where an upload lands; existing is the node its name holds, nil when the name
// is free or its entry dangles.
type uploadTarget struct {
	spaceID  string
	parent   *NodeEntry
	name     string
	existing *NodeEntry
}

// resolveUploadTarget resolves ref with resolveParent and reads the node its name holds.
func (d *kvfsDriver) resolveUploadTarget(ctx context.Context, ref *provider.Reference) (*uploadTarget, error) {
	spaceID, parent, name, err := d.resolveParent(ctx, ref)
	if err != nil {
		return nil, err
	}
	t := &uploadTarget{spaceID: spaceID, parent: parent, name: name}
	children, _, err := d.store.GetChildren(spaceID, parent.ID)
	if err != nil {
		return nil, err
	}
	if id, ok := children[name]; ok {
		n, _, err := d.store.GetNode(spaceID, id)
		switch {
		case err == nil:
			t.existing = n
		case err != ErrNodeNotFound:
			return nil, err
		}
	}
	return t, nil
}

// simpleUploadParams travel in the query of a simple-upload id.
type simpleUploadParams struct {
	IfMatch    string // if-match: the etag the overwritten file must still have
	TUSSibling string // tus-sibling: the TUS session minted alongside, reaped by the simple commit
	MTime      string // mtime: the client's modification time
	LockID     string // lock-id: the lock id the upload was initiated with
}

// encodeSimpleUploadID returns "<formattedRef>/<relPath>?<query>". The '?' is always there and
// the encoded query holds none, so the last '?' ends the path even when a file name contains one.
func encodeSimpleUploadID(formattedRef, relPath string, p simpleUploadParams) string {
	q := url.Values{}
	if p.IfMatch != "" {
		q.Set("if-match", p.IfMatch)
	}
	if p.TUSSibling != "" {
		q.Set("tus-sibling", p.TUSSibling)
	}
	if p.MTime != "" {
		q.Set("mtime", p.MTime)
	}
	if p.LockID != "" {
		q.Set("lock-id", p.LockID)
	}
	return formattedRef + "/" + relPath + "?" + q.Encode()
}

// parseUploadPath decodes a simple-upload id made by encodeSimpleUploadID.
func (d *kvfsDriver) parseUploadPath(p string) (*provider.Reference, simpleUploadParams, error) {
	// The simple handler prefixes with "/", clean it
	p = strings.TrimPrefix(p, "/")

	var params simpleUploadParams
	if idx := strings.LastIndex(p, "?"); idx >= 0 {
		if query, err := url.ParseQuery(p[idx+1:]); err == nil {
			params.IfMatch = query.Get("if-match")
			params.TUSSibling = query.Get("tus-sibling")
			params.MTime = query.Get("mtime")
			params.LockID = query.Get("lock-id")
		}
		p = p[:idx]
	}

	// Split on first "/" to separate spaceRef from file path
	parts := strings.SplitN(p, "/", 2)
	if len(parts) < 2 {
		return nil, params, errtypes.BadRequest("kvfs: invalid upload path format: " + p)
	}

	idPart := parts[0]
	filePath := parts[1]

	rid, err := storagespace.ParseID(idPart)
	if err != nil {
		return nil, params, errtypes.BadRequest("kvfs: failed to parse space ID from upload path: " + err.Error())
	}

	return &provider.Reference{
		ResourceId: &rid,
		Path:       utils.MakeRelativePath(filePath),
	}, params, nil
}
