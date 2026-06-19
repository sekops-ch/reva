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
	"time"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	ctxpkg "github.com/opencloud-eu/reva/v2/pkg/ctx"
	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
	"github.com/opencloud-eu/reva/v2/pkg/events"
)

// Move changes the path of a resource.
func (d *kvfsDriver) Move(ctx context.Context, oldRef, newRef *provider.Reference) error {
	// Phase 1: Validate (read-only, no mutations)

	spaceID, nodeID, node, _, err := d.resolveAndAuthorize(ctx, oldRef, func(rp *provider.ResourcePermissions) bool { return rp.Move })
	if err != nil {
		return err
	}

	if node.ParentID == "" {
		return errtypes.BadRequest("cannot move space root")
	}

	if err := d.checkNodeLock(ctx, node); err != nil {
		return err
	}

	oldParentID := node.ParentID
	oldName := node.Name

	destSpaceID, newParentID, newName, err := d.resolveParentRef(ctx, newRef)
	if err != nil {
		return err
	}

	if destSpaceID != spaceID {
		return errtypes.BadRequest("cross-space move is not supported")
	}

	destParentNode, _, err := d.store.GetNode(spaceID, newParentID)
	if err != nil {
		if err == ErrNodeNotFound {
			return errtypes.NotFound("destination parent not found")
		}
		return err
	}

	drp := d.assemblePermissions(ctx, spaceID, destParentNode)
	if node.Type == NodeTypeDir {
		if !drp.CreateContainer {
			if drp.Stat {
				return errtypes.PermissionDenied(newRef.String())
			}
			return errtypes.NotFound(newRef.String())
		}
	} else {
		if !drp.InitiateFileUpload {
			if drp.Stat {
				return errtypes.PermissionDenied(newRef.String())
			}
			return errtypes.NotFound(newRef.String())
		}
	}

	// Phase 2: Mutate (crash-safe ordering: node → add-to-new → remove-from-old)
	//
	// Detached commit context so client/gateway cancellation between the
	// three CAS steps can't leave the node in both parents. See
	// [kvfsDriver.commitPhase] for the contract.
	commitCtx, cancel := d.commitPhase(ctx)
	defer cancel()

	now := time.Now().UnixNano()
	newETag := calculateEtag(nodeID, now)

	if err := d.putNodeWithCAS(spaceID, nodeID, func(n *NodeEntry) {
		n.ParentID = newParentID
		n.Name = newName
		n.MTime = now
		n.ETag = newETag
	}); err != nil {
		return err
	}

	if err := d.updateChildrenWithCASCheck(spaceID, newParentID, func(children ChildMap) error {
		if existing, ok := children[newName]; ok && existing != nodeID {
			return errtypes.AlreadyExists(newName)
		}
		children[newName] = nodeID
		return nil
	}); err != nil {
		revertErr := d.putNodeWithCAS(spaceID, nodeID, func(n *NodeEntry) {
			n.ParentID = oldParentID
			n.Name = oldName
			n.MTime = now
			n.ETag = calculateEtag(nodeID, now)
		})
		if revertErr != nil {
			d.log.Error().Err(revertErr).
				Str("space_id", spaceID).Str("node_id", nodeID).
				Msg("Move: failed to revert node metadata after children update failure")
		}
		return err
	}

	if err := d.updateChildrenWithCAS(spaceID, oldParentID, func(children ChildMap) {
		delete(children, oldName)
	}); err != nil {
		d.log.Warn().Err(err).
			Str("space_id", spaceID).Str("node_id", nodeID).
			Str("old_parent", oldParentID).Str("old_name", oldName).
			Msg("Move: failed to remove from old parent (node exists in both parents, will self-correct via GC reconciler)")
	}

	// Phase 3: Propagate
	//
	// propagateTreeSize walks ancestors updating Size + MTime + ETag at every
	// level, which is what OpenCloud sync clients need to detect deep changes.
	// For same-parent rename, delta is zero but we still propagate so the
	// rename bubbles up to the space root as an etag change.

	if newParentID == oldParentID {
		d.propagateTreeSize(commitCtx, spaceID, oldParentID, 0)
	} else {
		d.propagateTreeSize(commitCtx, spaceID, oldParentID, -node.Size)
		d.propagateTreeSize(commitCtx, spaceID, newParentID, node.Size)
	}

	executant, _ := ctxpkg.ContextGetUser(ctx)
	d.publishEvent(commitCtx, func() interface{} {
		if executant == nil {
			return nil
		}
		return events.ItemMoved{
			SpaceOwner:   executant.Id,
			Executant:    executant.Id,
			Ref:          spaceRef(spaceID, nodeID),
			Owner:        executant.Id,
			OldReference: oldRef,
			Timestamp:    nowTimestamp(),
		}
	})

	return nil
}
