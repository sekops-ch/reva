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
	"strings"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
	"github.com/pkg/errors"
)

// resolveRef resolves a CS3 Reference to a (spaceID, nodeID) pair.
func (d *kvfsDriver) resolveRef(ctx context.Context, ref *provider.Reference) (string, string, error) {
	if ref == nil {
		return "", "", errtypes.BadRequest("nil reference")
	}

	rid := ref.GetResourceId()
	if rid != nil && rid.SpaceId != "" && rid.OpaqueId != "" {
		// Direct ID reference
		spaceID := rid.SpaceId
		nodeID := rid.OpaqueId
		if ref.Path != "" && ref.Path != "." && ref.Path != "/" {
			// Relative path from the node
			resolvedID, err := d.resolveRelativePath(ctx, spaceID, nodeID, ref.Path)
			if err != nil {
				if err == ErrNodeNotFound {
					return "", "", errtypes.NotFound(ref.String())
				}
				return "", "", err
			}
			return spaceID, resolvedID, nil
		}
		return spaceID, nodeID, nil
	}

	return "", "", errtypes.BadRequest("kvfs: reference must have resource ID")
}

// resolveParent resolves ref to the folder that holds its target and the target's name. A path
// is relative to the referenced node (the space root when the reference names none), and path
// "." names the referenced node itself.
func (d *kvfsDriver) resolveParent(ctx context.Context, ref *provider.Reference) (string, *NodeEntry, string, error) {
	rid := ref.GetResourceId()
	if rid == nil || rid.SpaceId == "" {
		return "", nil, "", errtypes.BadRequest("missing space ID in reference")
	}
	spaceID := rid.SpaceId
	baseID := rid.OpaqueId
	if baseID == "" {
		space, _, err := d.store.GetSpace(spaceID)
		if err != nil {
			if err == ErrSpaceNotFound {
				return "", nil, "", errtypes.NotFound(ref.String())
			}
			return "", nil, "", err
		}
		baseID = space.RootID
	}

	if err := d.refuseTrashed(ref); err != nil {
		return "", nil, "", err
	}

	refPath := filepath.Clean("./" + ref.GetPath())
	var parentID, name string
	if refPath == "." {
		node, _, err := d.store.GetNode(spaceID, baseID)
		switch {
		case err == ErrNodeNotFound:
			return "", nil, "", errtypes.NotFound(ref.String())
		case err != nil:
			return "", nil, "", err
		case node.ParentID == "":
			return "", nil, "", errtypes.BadRequest("kvfs: the space root has no parent")
		}
		parentID, name = node.ParentID, node.Name
	} else {
		var err error
		if parentID, err = d.resolveRelativePath(ctx, spaceID, baseID, filepath.Dir(refPath)); err != nil {
			if err == ErrNodeNotFound {
				return "", nil, "", errtypes.NotFound("parent directory not found: " + filepath.Dir(refPath))
			}
			return "", nil, "", err
		}
		name = filepath.Base(refPath)
	}
	if name == "" || name == "." || name == ".." || name == "/" {
		return "", nil, "", errtypes.BadRequest("kvfs: invalid name " + name)
	}

	parent, _, err := d.store.GetNode(spaceID, parentID)
	switch {
	case err == ErrNodeNotFound:
		return "", nil, "", errtypes.NotFound(ref.String())
	case err != nil:
		return "", nil, "", err
	case parent.Type != NodeTypeDir:
		// Only who may see the file learns that it is one; anyone else gets a missing parent.
		if !d.assemblePermissions(ctx, spaceID, parent).Stat {
			return "", nil, "", errtypes.NotFound(ref.String())
		}
		return "", nil, "", errtypes.PreconditionFailed("kvfs: parent is not a folder")
	}
	return spaceID, parent, name, nil
}

// refuseTrashed answers NotFound when ref addresses a node in a trashed directory through its
// node id: a trashed subtree stays in KV only for a restore, as decomposedfs's leaves the tree. A
// path from the space root resolves through listed children only.
func (d *kvfsDriver) refuseTrashed(ref *provider.Reference) error {
	rid := ref.GetResourceId()
	if rid.GetSpaceId() == "" || rid.GetOpaqueId() == "" {
		return nil
	}
	trashed, err := d.inTrash(rid.GetSpaceId(), rid.GetOpaqueId())
	if err != nil {
		return err
	}
	if trashed {
		return errtypes.NotFound(ref.String())
	}
	return nil
}

// inTrash reports whether nodeID or one of its ancestors is a trashed directory. The mark is
// confirmed against the parent's listing, since racing renames, moves or restores can leave it on
// a folder that is back in the tree. A missing node is not in the trash; the callers' own reads
// report it.
func (d *kvfsDriver) inTrash(spaceID, nodeID string) (bool, error) {
	for depth := 0; nodeID != "" && depth <= resolveMaxDeleteDepth(d.opts); depth++ {
		node, _, err := d.store.GetNode(spaceID, nodeID)
		switch {
		case err == ErrNodeNotFound:
			return false, nil
		case err != nil:
			return false, err
		}
		if node.Trashed {
			children, _, err := d.store.GetChildren(spaceID, node.ParentID)
			if err != nil {
				return false, err
			}
			if children[node.Name] != node.ID {
				return true, nil
			}
		}
		nodeID = node.ParentID
	}
	return false, nil
}

// resolveRelativePath resolves a relative path from a starting node.
func (d *kvfsDriver) resolveRelativePath(ctx context.Context, spaceID, startNodeID, path string) (string, error) {
	path = filepath.Clean(path)
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || (len(parts) == 1 && parts[0] == "") {
		return startNodeID, nil
	}

	currentID := startNodeID
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		children, _, err := d.store.GetChildren(spaceID, currentID)
		if err != nil {
			return "", errors.Wrapf(err, "kvfs: failed to list children of %s", currentID)
		}
		childID, exists := children[part]
		if !exists {
			return "", ErrNodeNotFound
		}
		currentID = childID
	}
	return currentID, nil
}

// resolvePathToNodeID resolves an absolute path from space root to a node ID.
func (d *kvfsDriver) resolvePathToNodeID(ctx context.Context, spaceID, path string) (string, error) {
	// Space root
	space, _, err := d.store.GetSpace(spaceID)
	if err != nil {
		return "", err
	}
	return d.resolveRelativePath(ctx, spaceID, space.RootID, path)
}

// buildPath walks from a node to the space root, building the full path.
func (d *kvfsDriver) buildPath(ctx context.Context, spaceID, nodeID string) (string, error) {
	var parts []string
	currentID := nodeID

	for i := 0; i < 100; i++ { // safety limit
		node, _, err := d.store.GetNode(spaceID, currentID)
		if err != nil {
			return "", err
		}
		if node.ParentID == "" {
			// Reached the root
			break
		}
		parts = append([]string{node.Name}, parts...)
		currentID = node.ParentID
	}

	return "/" + strings.Join(parts, "/"), nil
}

// resolveAndAuthorize resolves a reference, loads the node, and checks a single
// permission flag. Returns the spaceID, nodeID, node, and assembled permissions.
// If the permission check fails, returns NotFound (to hide existence) unless the
// user has Stat access, in which case returns PermissionDenied.
func (d *kvfsDriver) resolveAndAuthorize(ctx context.Context, ref *provider.Reference, check func(*provider.ResourcePermissions) bool) (string, string, *NodeEntry, *provider.ResourcePermissions, error) {
	spaceID, nodeID, err := d.resolveRef(ctx, ref)
	if err != nil {
		return "", "", nil, nil, err
	}

	node, _, err := d.store.GetNode(spaceID, nodeID)
	if err != nil {
		if err == ErrNodeNotFound {
			return "", "", nil, nil, errtypes.NotFound(ref.String())
		}
		return "", "", nil, nil, err
	}

	rp := d.assemblePermissions(ctx, spaceID, node)
	if !check(rp) {
		if rp.Stat {
			return "", "", nil, nil, errtypes.PermissionDenied(ref.String())
		}
		return "", "", nil, nil, errtypes.NotFound(ref.String())
	}

	return spaceID, nodeID, node, rp, nil
}
