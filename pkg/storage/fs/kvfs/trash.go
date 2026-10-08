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
	"path/filepath"
	"time"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	types "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"

	ctxpkg "github.com/opencloud-eu/reva/v2/pkg/ctx"
	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
	"github.com/opencloud-eu/reva/v2/pkg/events"
	"github.com/pkg/errors"
)

func (d *kvfsDriver) ListRecycle(ctx context.Context, ref *provider.Reference, key, relativePath string) ([]*provider.RecycleItem, error) {
	spaceID, err := d.checkRecyclePermission(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.ListRecycle })
	if err != nil {
		return nil, err
	}

	if key != "" {
		t, err := d.store.GetTrash(spaceID, key)
		if err != nil {
			if err == ErrNotFound {
				return nil, errtypes.NotFound(key)
			}
			return nil, err
		}
		return []*provider.RecycleItem{d.trashToRecycleItem(spaceID, t)}, nil
	}

	trashItems, err := d.store.ListTrash(spaceID)
	if err != nil {
		return nil, err
	}

	result := make([]*provider.RecycleItem, 0, len(trashItems))
	for _, t := range trashItems {
		result = append(result, d.trashToRecycleItem(spaceID, t))
	}
	return result, nil
}

func (d *kvfsDriver) trashToRecycleItem(spaceID string, t *TrashEntry) *provider.RecycleItem {
	itemType := provider.ResourceType_RESOURCE_TYPE_FILE
	if t.Node.Type == NodeTypeDir {
		itemType = provider.ResourceType_RESOURCE_TYPE_CONTAINER
	}
	// ocdav's trashbin PROPFIND derives `<oc:trashbin-original-filename>`
	// and `<oc:trashbin-original-location>` from `Ref.Path`
	// (path.Base + TrimPrefix("/")). If we leave Path empty the client
	// sees "." for the filename which (a) confuses any UI showing the
	// trash listing and (b) makes the spec-compliant "MOVE from trash"
	// restore protocol impossible because the client has no idea what
	// the file was called. Populate Path from TrashEntry.OriginalPath
	// which the Delete handler captures.
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: spaceID, OpaqueId: t.NodeID},
	}
	if t.OriginalPath != "" {
		// CS3 references use leading-slash relative paths.
		if t.OriginalPath[0] != '/' {
			ref.Path = "/" + t.OriginalPath
		} else {
			ref.Path = t.OriginalPath
		}
	} else if t.Node.Name != "" {
		ref.Path = "/" + t.Node.Name
	}
	return &provider.RecycleItem{
		Key:  t.Key,
		Ref:  ref,
		Type: itemType,
		DeletionTime: &types.Timestamp{
			Seconds: uint64(t.DeletionTime),
		},
		Size: uint64(t.Node.Size),
	}
}

func (d *kvfsDriver) RestoreRecycleItem(ctx context.Context, ref *provider.Reference, key, relativePath string, restoreRef *provider.Reference) error {
	spaceID, err := d.checkRecyclePermission(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.RestoreRecycleItem })
	if err != nil {
		return err
	}

	trashItem, err := d.store.GetTrash(spaceID, key)
	if err != nil {
		if err == ErrNotFound {
			return errtypes.NotFound(key)
		}
		return err
	}

	// Determine restore location
	restorePath := trashItem.OriginalPath
	if restoreRef != nil && restoreRef.Path != "" {
		restorePath = restoreRef.Path
	}

	parentPath := filepath.Dir(restorePath)
	parentID, err := d.resolvePathToNodeID(ctx, spaceID, parentPath)
	if err != nil {
		return errors.Wrap(err, "kvfs: restore parent not found")
	}

	// Detached commit context — three NATS bucket touches (PutNode +
	// updateChildren + DeleteTrash) must run to completion; otherwise
	// "node both restored AND in trash" leaks. See [kvfsDriver.commitPhase].
	commitCtx, cancel := d.commitPhase(ctx)
	defer cancel()

	now := time.Now().UnixNano()
	restoreName := filepath.Base(restorePath)
	// A name that maps to another node is refused before any write; the parent update re-checks it.
	if children, _, err := d.store.GetChildren(spaceID, parentID); err == nil {
		if id, ok := children[restoreName]; ok && id != trashItem.NodeID {
			return errtypes.AlreadyExists(restoreName)
		}
	}
	var restoredNodeID string
	var restoredSize int64
	var undo func() error // reverts the node write; the item stays in the trash

	// A file keeps its date and etag from the trash snapshot, so clients that kept a copy
	// download nothing; a live directory keeps its date and gets a fresh etag, so clients list
	// it again. Locks are dropped, as decomposedfs drops them on delete.
	if trashItem.Node.Type == NodeTypeDir {
		liveNode, rev, gerr := d.store.GetNode(spaceID, trashItem.NodeID)
		if gerr == nil {
			trashed := *liveNode
			undo = func() error {
				return d.putNodeWithCAS(spaceID, trashed.ID, func(n *NodeEntry) {
					n.ParentID, n.Name, n.ETag, n.Trashed = trashed.ParentID, trashed.Name, trashed.ETag, trashed.Trashed
				})
			}
			liveNode.ParentID = parentID
			liveNode.Name = restoreName
			liveNode.ETag = calculateEtag(liveNode.ID, now)
			liveNode.Lock = nil
			liveNode.Trashed = false
			if err := d.store.PutNode(liveNode, rev); err != nil {
				return errors.Wrap(err, "kvfs: failed to update restored directory node")
			}
			restoredNodeID = liveNode.ID
			restoredSize = liveNode.Size
		} else {
			// Old trash entries lost the subtree: the directory comes back empty, with a fresh
			// date and etag and no size.
			restoredNode := trashItem.Node
			restoredNode.ParentID = parentID
			restoredNode.Name = restoreName
			restoredNode.MTime = now
			restoredNode.ETag = calculateEtag(restoredNode.ID, now)
			restoredNode.Size = 0
			restoredNode.Lock = nil
			restoredNode.Trashed = false
			if err := d.store.PutNode(&restoredNode, 0); err != nil {
				return err
			}
			createdChildren := true
			if err := d.store.PutChildren(spaceID, restoredNode.ID, ChildMap{}, 0); err != nil {
				createdChildren = false
				d.log.Warn().Err(err).Str("node_id", restoredNode.ID).Msg("RestoreRecycleItem: failed to create children map for old-format directory")
			}
			undo = func() error {
				if createdChildren {
					if err := d.store.DeleteChildren(spaceID, restoredNode.ID); err != nil {
						return err
					}
				}
				return d.store.DeleteNode(spaceID, restoredNode.ID)
			}
			restoredNodeID = restoredNode.ID
			restoredSize = restoredNode.Size
		}
	} else {
		restoredNode := trashItem.Node
		restoredNode.ParentID = parentID
		restoredNode.Name = restoreName
		restoredNode.Lock = nil
		if err := d.store.PutNode(&restoredNode, 0); err != nil {
			return err
		}
		undo = func() error { return d.store.DeleteNode(spaceID, restoredNode.ID) }
		restoredNodeID = restoredNode.ID
		restoredSize = restoredNode.Size
	}

	if err := d.updateChildrenWithCASCheck(spaceID, parentID, func(children ChildMap) error {
		if id, ok := children[restoreName]; ok && id != restoredNodeID {
			return errtypes.AlreadyExists(restoreName)
		}
		children[restoreName] = restoredNodeID
		return nil
	}); err != nil {
		if uerr := undo(); uerr != nil {
			d.log.Error().Err(uerr).Str("space_id", spaceID).Str("node_id", restoredNodeID).Msg("RestoreRecycleItem: failed to revert the node write")
		}
		return err
	}

	if err := d.store.DeleteTrash(spaceID, key); err != nil {
		return err
	}

	d.propagateTreeSize(commitCtx, spaceID, parentID, restoredSize)

	executant, _ := ctxpkg.ContextGetUser(ctx)
	d.publishEvent(commitCtx, func() interface{} {
		if executant == nil {
			return nil
		}
		return events.ItemRestored{
			SpaceOwner: executant.Id,
			Executant:  executant.Id,
			ID:         spaceResourceID(spaceID, restoredNodeID),
			Ref:        spaceRef(spaceID, restoredNodeID),
			Owner:      executant.Id,
			Key:        key,
			Timestamp:  nowTimestamp(),
		}
	})

	return nil
}

func (d *kvfsDriver) PurgeRecycleItem(ctx context.Context, ref *provider.Reference, key, relativePath string) error {
	spaceID, err := d.checkRecyclePermission(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.PurgeRecycle })
	if err != nil {
		return err
	}

	trashItem, err := d.store.GetTrash(spaceID, key)
	if err != nil {
		if err == ErrNotFound {
			return errtypes.NotFound(key)
		}
		return err
	}

	// Detached commit context — partial purge (some blobs deleted in S3, some
	// nodes still in NATS) leaves an inconsistent half-state and the operator-
	// invoked re-purge is the only recovery. See [kvfsDriver.commitPhase].
	commitCtx, cancel := d.commitPhase(ctx)
	defer cancel()

	if trashItem.Node.Type == NodeTypeDir {
		// Directories: nodes and children maps are still alive in KV.
		// Recursively delete all descendants, their blobs, and versions.
		// A depth-bound violation aborts BEFORE the root node/children/
		// trash entry are touched — the item stays intact and restorable.
		if err := d.recursiveDeleteNodesAndBlobs(commitCtx, spaceID, trashItem.NodeID, 0); err != nil {
			return err
		}
		d.store.DeleteNode(spaceID, trashItem.NodeID)
		d.store.DeleteChildren(spaceID, trashItem.NodeID)
	} else if trashItem.Node.BlobID != "" {
		d.blob.Delete(commitCtx, BlobKey(spaceID, trashItem.Node.BlobID))
	}

	if err := d.store.DeleteTrash(spaceID, key); err != nil {
		return err
	}

	executant, _ := ctxpkg.ContextGetUser(ctx)
	d.publishEvent(commitCtx, func() interface{} {
		if executant == nil {
			return nil
		}
		return events.ItemPurged{
			Executant: executant.Id,
			ID:        spaceResourceID(spaceID, trashItem.NodeID),
			Ref:       spaceRef(spaceID, trashItem.NodeID),
			Owner:     executant.Id,
			Timestamp: nowTimestamp(),
		}
	})

	return nil
}

func (d *kvfsDriver) EmptyRecycle(ctx context.Context, ref *provider.Reference) error {
	spaceID, err := d.checkRecyclePermission(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.PurgeRecycle })
	if err != nil {
		return err
	}

	trashItems, err := d.store.ListTrash(spaceID)
	if err != nil {
		return err
	}

	// Detached commit context for the full sweep — partial empties leave
	// half-purged trash that requires re-running EmptyRecycle to clean up.
	// Same rationale as PurgeRecycleItem. See [kvfsDriver.commitPhase].
	commitCtx, cancel := d.commitPhase(ctx)
	defer cancel()

	for _, t := range trashItems {
		if t.Node.Type == NodeTypeDir {
			// Abort the whole empty on the first over-deep item: loud
			// and retriable (after raising the bound) beats silently
			// leaving some items behind. Already-purged items are gone;
			// this item and the rest stay in the trash untouched.
			if err := d.recursiveDeleteNodesAndBlobs(commitCtx, spaceID, t.NodeID, 0); err != nil {
				return err
			}
			d.store.DeleteNode(spaceID, t.NodeID)
			d.store.DeleteChildren(spaceID, t.NodeID)
		} else if t.Node.BlobID != "" {
			d.blob.Delete(commitCtx, BlobKey(spaceID, t.Node.BlobID))
		}
		d.store.DeleteTrash(spaceID, t.Key)
	}

	d.publishEvent(commitCtx, func() interface{} {
		u, ok := ctxpkg.ContextGetUser(ctx)
		if !ok {
			return nil
		}
		return events.TrashbinPurged{
			Executant: u.Id,
			Ref:       ref,
			Owner:     u.Id,
			Timestamp: nowTimestamp(),
		}
	})

	return nil
}

// checkRecyclePermission loads the space root node and checks a recycle-bin
// permission flag. Returns the spaceID on success.
func (d *kvfsDriver) checkRecyclePermission(ctx context.Context, ref *provider.Reference, check func(*provider.ResourcePermissions) bool) (string, error) {
	spaceID := ref.GetResourceId().GetSpaceId()
	if spaceID == "" {
		return "", errtypes.BadRequest("missing space ID")
	}

	space, _, err := d.store.GetSpace(spaceID)
	if err != nil {
		return "", err
	}
	rootNode, _, err := d.store.GetNode(spaceID, space.RootID)
	if err != nil {
		return "", err
	}
	rp := d.assemblePermissions(ctx, spaceID, rootNode)
	if !check(rp) {
		return "", errtypes.PermissionDenied(spaceID)
	}

	return spaceID, nil
}
