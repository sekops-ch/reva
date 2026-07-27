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
	"time"

	"github.com/google/uuid"

	user "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	types "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"

	ctxpkg "github.com/opencloud-eu/reva/v2/pkg/ctx"
	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
	"github.com/opencloud-eu/reva/v2/pkg/storagespace"
	"github.com/opencloud-eu/reva/v2/pkg/utils"
	"github.com/pkg/errors"
)

// requestedSpaceIDOpaqueKeys are the opaque-map keys we accept as a
// caller-supplied space ID hint. The gateway's CreateHome sends
// "space_id" (with underscore); legacy / decomposedfs convention is
// "spaceid". Accepting both is strictly additive — at most one will be
// present in any given request.
var requestedSpaceIDOpaqueKeys = []string{"space_id", "spaceid"}

// requestedSpaceID extracts an explicit space-id hint from req.Opaque, or
// "" if the caller didn't supply one. Personal-type creates ignore the
// hint and derive the ID from the owner instead (see CreateStorageSpace).
func requestedSpaceID(req *provider.CreateStorageSpaceRequest) string {
	if req == nil || req.Opaque == nil {
		return ""
	}
	for _, k := range requestedSpaceIDOpaqueKeys {
		if entry, ok := req.Opaque.Map[k]; ok && len(entry.Value) > 0 {
			return string(entry.Value)
		}
	}
	return ""
}

// findExistingPersonal returns the (single) personal space owned by
// ownerID, or nil if none exists. Used both as the dedup fast-path and
// as the retry-after-CAS-conflict path in createPersonalSpace.
func (d *kvfsDriver) findExistingPersonal(ownerID string) *SpaceEntry {
	existing, err := d.store.ListSpaces(func(s *SpaceEntry) bool {
		return s.Type == "personal" && s.Owner == ownerID
	})
	if err != nil || len(existing) == 0 {
		return nil
	}
	return existing[0]
}

// CreateStorageSpace creates a new storage space with a root directory
// node. For type=personal the space ID is deterministically derived from
// the owner's ID — this is what makes concurrent CreateHome calls
// idempotent without a cross-pod lock: PutSpace(rev=0) acts as the
// atomic exclusion. Two pods racing into the same deterministic ID will
// see exactly one PutSpace succeed; the loser falls back through
// findExistingPersonal and returns the winner's space.
//
// For type=project / type=share / etc. the caller may supply an explicit
// ID via req.Opaque (key "space_id" or "spaceid"); otherwise a fresh
// UUID is minted.
func (d *kvfsDriver) CreateStorageSpace(ctx context.Context, req *provider.CreateStorageSpaceRequest) (*provider.CreateStorageSpaceResponse, error) {
	u := ctxpkg.ContextMustGetUser(ctx)

	spaceType := req.Type
	if spaceType == "" {
		spaceType = "personal"
	}
	ownerID := u.Id.OpaqueId

	// Personal-space dedup: fast path + CAS-retry path. Two attempts
	// suffice — the first races, the second observes the winner.
	if spaceType == "personal" {
		for attempt := 0; attempt < 2; attempt++ {
			if existing := d.findExistingPersonal(ownerID); existing != nil {
				d.log.Debug().Str("space_id", existing.ID).Str("type", spaceType).Str("owner", existing.Owner).Msg("personal space already exists, returning existing")
				rootNode, _, _ := d.store.GetNode(existing.ID, existing.RootID)
				return &provider.CreateStorageSpaceResponse{
					Status:       &rpc.Status{Code: rpc.Code_CODE_OK},
					StorageSpace: d.spaceToCS3(existing, rootNode),
				}, nil
			}
			resp, err := d.tryCreateSpace(ctx, req, spaceType, ownerID, ownerID)
			if err == nil {
				return resp, nil
			}
			if errors.Cause(err) == ErrCASConflict {
				// Lost the race; loop to surface the winner via findExistingPersonal.
				CASRetries.WithLabelValues("create_personal_space").Inc()
				continue
			}
			return nil, err
		}
		CASExhausted.WithLabelValues("create_personal_space").Inc()
		return nil, errors.New("kvfs: CreateStorageSpace(personal) failed to converge after retry")
	}

	spaceID := requestedSpaceID(req)
	if spaceID == "" {
		spaceID = uuid.New().String()
	}
	return d.tryCreateSpace(ctx, req, spaceType, ownerID, spaceID)
}

// tryCreateSpace executes the three-write sequence (PutNode +
// PutChildren + PutSpace, all at rev=0) for a fully-determined
// (spaceID, ownerID, spaceType) tuple. Returns ErrCASConflict (wrapped)
// if any of the three writes loses to a concurrent creator. Inline
// cleanup rolls back partial state so the next caller (or our own
// retry) starts from a clean slate.
//
// No commitPhase wrap: each Put is a single NATS KV write that doesn't
// take ctx; cleanup-on-error happens synchronously here.
func (d *kvfsDriver) tryCreateSpace(ctx context.Context, req *provider.CreateStorageSpaceRequest, spaceType, ownerID, spaceID string) (*provider.CreateStorageSpaceResponse, error) {
	rootID := spaceID // root node ID equals space ID
	now := time.Now().UnixNano()

	rootNode := &NodeEntry{
		ID:       rootID,
		SpaceID:  spaceID,
		ParentID: "",
		Name:     req.Name,
		Type:     NodeTypeDir,
		MTime:    now,
		ETag:     calculateEtag(rootID, now),
		Owner:    ownerID,
		MimeType: "httpd/unix-directory",
	}
	if err := d.store.PutNode(rootNode, 0); err != nil {
		return nil, errors.Wrap(err, "kvfs: failed to create root node")
	}
	if err := d.store.PutChildren(spaceID, rootID, ChildMap{}, 0); err != nil {
		d.store.DeleteNode(spaceID, rootID)
		return nil, errors.Wrap(err, "kvfs: failed to create root children")
	}

	quota := int64(-1) // unlimited
	if req.GetQuota() != nil {
		quota = int64(req.GetQuota().QuotaMaxBytes)
	}

	space := &SpaceEntry{
		ID:     spaceID,
		Type:   spaceType,
		Owner:  ownerID,
		Name:   req.Name,
		RootID: rootID,
		Quota:  quota,
		MTime:  now,
	}
	if err := d.store.PutSpace(space, 0); err != nil {
		d.store.DeleteNode(spaceID, rootID)
		d.store.DeleteChildren(spaceID, rootID)
		return nil, errors.Wrap(err, "kvfs: failed to create space")
	}

	d.log.Info().Str("space_id", spaceID).Str("type", spaceType).Str("owner", ownerID).Msg("created storage space")

	return &provider.CreateStorageSpaceResponse{
		Status:       &rpc.Status{Code: rpc.Code_CODE_OK},
		StorageSpace: d.spaceToCS3(space, rootNode),
	}, nil
}

// listSpacesFilter is the parsed representation of an inbound filter list.
// Parsing once up front keeps the per-space callback cheap and separates
// filter interpretation from filter application.
type listSpacesFilter struct {
	spaceTypes map[string]struct{} // empty = any type
	spaceID    string              // empty = any id
	ownerID    string              // empty = any owner
	userID     string              // empty = no TYPE_USER constraint
	userGroups []string            // groups for the userID, sourced from ctx when ctx user matches
}

// parseListSpacesFilter builds the internal filter representation.
// TYPE_USER matches decomposedfs semantics ("user has any access" = owner
// OR direct grantee OR group grantee). Groups are lifted from the ctx
// user when it matches the filter user; cross-user lookups apply only
// owner-or-direct-user-grant (group resolution would need a CS3 round-trip
// on this hot path).
func parseListSpacesFilter(ctx context.Context, in []*provider.ListStorageSpacesRequest_Filter) listSpacesFilter {
	f := listSpacesFilter{spaceTypes: map[string]struct{}{}}
	for _, x := range in {
		switch x.Type {
		case provider.ListStorageSpacesRequest_Filter_TYPE_SPACE_TYPE:
			st := x.GetSpaceType()
			if strings.HasPrefix(st, "+") {
				// "+grant" / "+mountpoint" are augmenting hints (include
				// additional categories), not filters. Ignore here for
				// parity with decomposedfs.
				continue
			}
			f.spaceTypes[st] = struct{}{}
		case provider.ListStorageSpacesRequest_Filter_TYPE_ID:
			if id := x.GetId(); id != nil {
				parsed, _ := storagespace.ParseID(id.OpaqueId)
				f.spaceID = parsed.SpaceId
				if f.spaceID == "" {
					f.spaceID = id.OpaqueId
				}
			}
		case provider.ListStorageSpacesRequest_Filter_TYPE_OWNER:
			if o := x.GetOwner(); o != nil {
				f.ownerID = o.OpaqueId
			}
		case provider.ListStorageSpacesRequest_Filter_TYPE_USER:
			if usr := x.GetUser(); usr != nil {
				f.userID = usr.OpaqueId
				if cu, ok := ctxpkg.ContextGetUser(ctx); ok && cu.GetId().GetOpaqueId() == usr.OpaqueId {
					f.userGroups = cu.Groups
				}
			}
		}
	}
	return f
}

// ListStorageSpaces lists spaces matching the given filters.
func (d *kvfsDriver) ListStorageSpaces(ctx context.Context, filter []*provider.ListStorageSpacesRequest_Filter, unrestricted bool) ([]*provider.StorageSpace, error) {
	u, _ := ctxpkg.ContextGetUser(ctx)
	f := parseListSpacesFilter(ctx, filter)

	spaces, err := d.store.ListSpaces(func(s *SpaceEntry) bool {
		if len(f.spaceTypes) > 0 {
			if _, ok := f.spaceTypes[s.Type]; !ok {
				return false
			}
		}
		if f.spaceID != "" && f.spaceID != s.ID {
			return false
		}
		if f.ownerID != "" && f.ownerID != s.Owner {
			return false
		}
		// TYPE_USER's grant branch needs the root node, which we don't
		// load here. Owner-match short-circuits handled post-fetch
		// alongside the existing visibility check (avoids a second
		// rootNode read).
		return true
	})
	if err != nil {
		return nil, err
	}

	var result []*provider.StorageSpace
	for _, s := range spaces {
		rootNode, _, err := d.store.GetNode(s.ID, s.RootID)
		if err != nil {
			d.log.Warn().Err(err).Str("space_id", s.ID).Msg("failed to get root node for space")
			continue
		}
		// TYPE_USER: owner short-circuits; otherwise root node must grant.
		// Matches upstream decomposedfs's "user has any access" semantic.
		if f.userID != "" && s.Owner != f.userID {
			probe := &user.User{Id: &user.UserId{OpaqueId: f.userID}, Groups: f.userGroups}
			if !d.hasGrantForUser(rootNode, probe) {
				continue
			}
		}
		// Visibility check for the *caller* (ctx user) — independent of
		// TYPE_USER which constrains the *subject* being queried.
		if !unrestricted && u != nil && s.Owner != u.Id.OpaqueId {
			if !d.hasGrantForUser(rootNode, u) {
				continue
			}
		}
		result = append(result, d.spaceToCS3(s, rootNode))
	}

	return result, nil
}

// UpdateStorageSpace updates a storage space's name, quota, or other properties.
func (d *kvfsDriver) UpdateStorageSpace(ctx context.Context, req *provider.UpdateStorageSpaceRequest) (*provider.UpdateStorageSpaceResponse, error) {
	spaceID := req.GetStorageSpace().GetId().GetOpaqueId()
	if spaceID == "" {
		return nil, errtypes.BadRequest("missing space ID")
	}
	if parsed, err := storagespace.ParseID(spaceID); err == nil && parsed.SpaceId != "" {
		spaceID = parsed.SpaceId
	}

	space, rev, err := d.store.GetSpace(spaceID)
	if err != nil {
		return nil, err
	}

	if req.StorageSpace.Name != "" {
		space.Name = req.StorageSpace.Name
	}
	if req.StorageSpace.Quota != nil {
		space.Quota = int64(req.StorageSpace.Quota.QuotaMaxBytes)
	}
	space.MTime = time.Now().UnixNano()

	if err := d.store.PutSpace(space, rev); err != nil {
		return nil, err
	}

	rootNode, _, _ := d.store.GetNode(space.ID, space.RootID)

	return &provider.UpdateStorageSpaceResponse{
		Status:       &rpc.Status{Code: rpc.Code_CODE_OK},
		StorageSpace: d.spaceToCS3(space, rootNode),
	}, nil
}

// DeleteStorageSpace deletes a storage space and all its contents.
func (d *kvfsDriver) DeleteStorageSpace(ctx context.Context, req *provider.DeleteStorageSpaceRequest) error {
	spaceID := req.GetId().GetOpaqueId()
	if spaceID == "" {
		return errtypes.BadRequest("missing space ID")
	}
	// The gateway sends composite IDs like "providerId$spaceId" — extract the raw space ID
	if parsed, err := storagespace.ParseID(spaceID); err == nil && parsed.SpaceId != "" {
		spaceID = parsed.SpaceId
	}

	space, _, err := d.store.GetSpace(spaceID)
	if err != nil {
		return err
	}

	// Detached commit context — recursive blob deletes + multipart aborts
	// in deleteSpaceContents can take time; client cancellation must not
	// interrupt the sweep. See [kvfsDriver.commitPhase].
	commitCtx, cancel := d.commitPhase(ctx)
	defer cancel()

	if err := d.deleteSpaceContents(commitCtx, spaceID, space.RootID); err != nil {
		return err
	}

	return d.store.DeleteSpace(spaceID)
}

// deleteSpaceContents removes all nodes, blobs, trash, versions, and uploads
// belonging to a space. Called before deleting the space entry itself.
// Returns an error only on a depth-bound violation (see
// recursiveDeleteNodesAndBlobs); everything else stays best-effort.
func (d *kvfsDriver) deleteSpaceContents(ctx context.Context, spaceID, rootID string) error {
	if err := d.recursiveDeleteNodesAndBlobs(ctx, spaceID, rootID, 0); err != nil {
		return err
	}
	d.store.DeleteNode(spaceID, rootID)
	d.store.DeleteChildren(spaceID, rootID)

	trashItems, err := d.store.ListTrash(spaceID)
	if err == nil {
		for _, t := range trashItems {
			if t.Node.Type == NodeTypeDir {
				// Trashed directories still have live nodes — clean them up
				if err := d.recursiveDeleteNodesAndBlobs(ctx, spaceID, t.NodeID, 0); err != nil {
					return err
				}
				d.store.DeleteNode(spaceID, t.NodeID)
				d.store.DeleteChildren(spaceID, t.NodeID)
			} else if t.Node.BlobID != "" {
				d.blob.Delete(ctx, BlobKey(spaceID, t.Node.BlobID))
			}
			d.store.DeleteTrash(spaceID, t.Key)
		}
	}

	versions, err := d.store.ListVersionsBySpace(spaceID)
	if err == nil {
		for _, v := range versions {
			if v.BlobID != "" {
				d.blob.Delete(ctx, BlobKey(spaceID, v.BlobID))
			}
			d.store.DeleteVersion(v.SpaceID, v.NodeID, v.Key)
		}
	}

	uploads, err := d.store.ListUploadsBySpace(spaceID)
	if err == nil {
		for _, u := range uploads {
			if u.S3MultipartID != "" {
				d.blob.AbortMultipartUpload(ctx, BlobKey(spaceID, u.BlobID), u.S3MultipartID)
			}
			if u.BlobID != "" {
				d.blob.Delete(ctx, BlobKey(spaceID, u.BlobID))
			}
			d.store.DeleteUpload(u.ID)
		}
	}
	return nil
}

// spaceToCS3 converts a SpaceEntry + root node to a CS3 StorageSpace.
func (d *kvfsDriver) spaceToCS3(space *SpaceEntry, rootNode *NodeEntry) *provider.StorageSpace {
	// Build the space alias for drive resolution (personal/username, project/name)
	spaceAlias := space.Type + "/" + strings.ReplaceAll(strings.ToLower(space.Name), " ", "-")

	ss := &provider.StorageSpace{
		Id:        &provider.StorageSpaceId{OpaqueId: space.ID},
		Root:      &provider.ResourceId{SpaceId: space.ID, OpaqueId: space.RootID},
		Name:      space.Name,
		SpaceType: space.Type,
		Owner: &user.User{
			Id: &user.UserId{OpaqueId: space.Owner},
		},
		Mtime: &types.Timestamp{
			Seconds: uint64(space.MTime / int64(time.Second)),
			Nanos:   uint32(space.MTime % int64(time.Second)),
		},
	}

	// Add spaceAlias to Opaque (required for Graph API driveAlias and legacy path resolution)
	ss.Opaque = utils.AppendPlainToOpaque(ss.Opaque, "spaceAlias", spaceAlias)

	if space.Quota >= 0 {
		ss.Quota = &provider.Quota{
			QuotaMaxBytes: uint64(space.Quota),
		}
	}

	if rootNode != nil {
		totalBytes := uint64(0)
		if space.Quota > 0 {
			totalBytes = uint64(space.Quota)
		}
		usedBytes := uint64(rootNode.Size)
		remainingBytes := uint64(0)
		if totalBytes > usedBytes {
			remainingBytes = totalBytes - usedBytes
		}
		ss.Quota = &provider.Quota{
			QuotaMaxBytes:  totalBytes,
			QuotaMaxFiles:  0,
			RemainingBytes: remainingBytes,
		}
		ss.Opaque = utils.AppendPlainToOpaque(ss.Opaque, "etag", rootNode.ETag)

		// Populate grants in Opaque (required for Graph API member display)
		if len(rootNode.Grants) > 0 {
			grantMap := make(map[string]*provider.ResourcePermissions, len(rootNode.Grants))
			groupMap := make(map[string]struct{})
			for key, ge := range rootNode.Grants {
				grantMap[ge.GranteeID] = uint32ToPermissions(ge.Permissions)
				if strings.HasPrefix(key, "g:") {
					groupMap[ge.GranteeID] = struct{}{}
				}
			}
			ss.Opaque = utils.AppendJSONToOpaque(ss.Opaque, "grants", grantMap)
			if len(groupMap) > 0 {
				ss.Opaque = utils.AppendJSONToOpaque(ss.Opaque, "groups", groupMap)
			}
		}
	}

	return ss
}
