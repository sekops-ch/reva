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

	"github.com/google/uuid"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	ctxpkg "github.com/opencloud-eu/reva/v2/pkg/ctx"
	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
	"github.com/opencloud-eu/reva/v2/pkg/events"
	"github.com/opencloud-eu/reva/v2/pkg/mime"
)

// TouchFile creates an empty file. An existing node is AlreadyExists, as in decomposedfs: callers
// then upload the empty content, which checks the lock and keeps the replaced content as a
// version. markprocessing only flags an existing node.
func (d *kvfsDriver) TouchFile(ctx context.Context, ref *provider.Reference, markprocessing bool, mtimeStr string) error {
	spaceID, nodeID, err := d.resolveRef(ctx, ref)
	if _, notFound := err.(errtypes.IsNotFound); notFound {
		return d.createEmptyFileAuthorized(ctx, ref, mtimeStr)
	} else if err != nil {
		return err
	}

	node, _, err := d.store.GetNode(spaceID, nodeID)
	switch {
	case err == ErrNodeNotFound:
		return d.createEmptyFileAuthorized(ctx, ref, mtimeStr)
	case err != nil:
		return err
	}

	rp := d.assemblePermissions(ctx, spaceID, node)
	if !rp.InitiateFileUpload {
		if rp.Stat {
			return errtypes.PermissionDenied(ref.String())
		}
		return errtypes.NotFound(ref.String())
	}
	if !markprocessing {
		return errtypes.AlreadyExists(ref.String())
	}
	return d.putNodeWithCAS(spaceID, nodeID, func(n *NodeEntry) { n.Processing = true })
}

func (d *kvfsDriver) createEmptyFileAuthorized(ctx context.Context, ref *provider.Reference, mtimeStr string) error {
	spaceID, parentNode, name, err := d.resolveParent(ctx, ref)
	if err != nil {
		return err
	}
	rp := d.assemblePermissions(ctx, spaceID, parentNode)
	if !rp.InitiateFileUpload {
		if rp.Stat {
			return errtypes.PermissionDenied(ref.String())
		}
		return errtypes.NotFound(ref.String())
	}
	return d.createEmptyFile(ctx, spaceID, parentNode.ID, name, mtimeStr)
}

func (d *kvfsDriver) createEmptyFile(ctx context.Context, spaceID, parentID, name, mtimeStr string) error {
	u := ctxpkg.ContextMustGetUser(ctx)
	newID := uuid.New().String()
	now := time.Now().UnixNano()
	mtime := now
	if ns, ok := parseClientMTime(mtimeStr); ok {
		mtime = ns
	}

	// Detached commit context — PutNode + updateChildrenWithCAS + touch +
	// event must complete or revert. See [kvfsDriver.commitPhase].
	commitCtx, cancel := d.commitPhase(ctx)
	defer cancel()

	node := &NodeEntry{
		ID:       newID,
		SpaceID:  spaceID,
		ParentID: parentID,
		Name:     name,
		Type:     NodeTypeFile,
		Size:     0,
		MTime:    mtime,
		ETag:     calculateEtag(newID, now),
		Owner:    u.Id.OpaqueId,
		MimeType: mime.Detect(false, name),
	}
	if err := d.store.PutNode(node, 0); err != nil {
		return err
	}

	if err := d.updateChildrenWithCASCheck(spaceID, parentID, func(children ChildMap) error {
		if _, taken := children[name]; taken {
			return errtypes.AlreadyExists(name)
		}
		children[name] = newID
		return nil
	}); err != nil {
		d.store.DeleteNode(spaceID, newID)
		return err
	}

	d.propagateTreeSize(commitCtx, spaceID, parentID, 0)

	d.publishEvent(commitCtx, func() interface{} {
		return events.FileUploaded{
			SpaceOwner: u.Id,
			Executant:  u.Id,
			Ref:        spaceRef(spaceID, newID),
			Owner:      u.Id,
			Timestamp:  nowTimestamp(),
		}
	})

	return nil
}
