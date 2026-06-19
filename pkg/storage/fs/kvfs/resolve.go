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

// resolveParentRef resolves a reference to its parent's (spaceID, parentID, childName).
func (d *kvfsDriver) resolveParentRef(ctx context.Context, ref *provider.Reference) (string, string, string, error) {
	rid := ref.GetResourceId()
	if rid == nil || rid.SpaceId == "" {
		return "", "", "", errtypes.BadRequest("missing space ID in reference")
	}

	spaceID := rid.SpaceId
	refPath := ref.Path
	if refPath == "" {
		return "", "", "", errtypes.BadRequest("missing path in reference")
	}

	refPath = filepath.Clean(refPath)

	// When OpaqueId points directly to a file and path is "." (e.g. from
	// sharesstorageprovider for file-level shares), resolve the parent from
	// the node's own metadata instead of path-walking.
	if (refPath == "." || refPath == "/") && rid.OpaqueId != "" && rid.OpaqueId != rid.SpaceId {
		node, _, err := d.store.GetNode(spaceID, rid.OpaqueId)
		if err == nil && node.Type == NodeTypeFile && node.ParentID != "" {
			return spaceID, node.ParentID, node.Name, nil
		}
	}

	parentPath := filepath.Dir(refPath)
	name := filepath.Base(refPath)

	parentID, err := d.resolvePathToNodeID(ctx, spaceID, parentPath)
	if err != nil {
		// If parent starts from the space root, resolve from root
		parentID = rid.OpaqueId
		if parentPath != "." && parentPath != "/" {
			parentID, err = d.resolveRelativePath(ctx, spaceID, rid.OpaqueId, parentPath)
			if err != nil {
				if err == ErrNodeNotFound {
					return "", "", "", errtypes.NotFound("parent directory not found: " + parentPath)
				}
				return "", "", "", err
			}
		}
	}

	return spaceID, parentID, name, nil
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
