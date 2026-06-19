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
)

// propagateTreeSize walks ancestors starting at nodeID up to the space root,
// updating Size (by delta) plus MTime and ETag on every node along the way.
//
// Refreshing MTime + ETag on each ancestor is mandatory for OpenCloud sync
// clients to detect deep changes: clients poll the space root's etag and
// descend from there. If only the direct parent's etag changes, clients miss
// any change deeper than one level. See opencloud-eu/reva#625 for the upstream
// review that surfaced this.
//
// The size delta may be zero (e.g. same-folder rename); we still walk because
// mtime + etag still need to bubble.
//
// Best-effort: an ancestor CAS that exhausts retries bumps
// `kvfs_tree_size_drift_total` and the walk continues so the rest of the chain
// stays fresh.
func (d *kvfsDriver) propagateTreeSize(ctx context.Context, spaceID, nodeID string, delta int64) {
	currentID := nodeID
	const maxDepth = 100
	for depth := 0; depth < maxDepth; depth++ {
		nextID, ok := d.propagateOneAncestor(spaceID, currentID, delta)
		if !ok {
			// Inner loop exhausted; drift was already recorded. Walk up via
			// a non-mutating GetNode so the rest of the chain still gets a
			// chance to be propagated.
			node, _, err := d.store.GetNode(spaceID, currentID)
			if err != nil || node.ParentID == "" {
				return
			}
			currentID = node.ParentID
			continue
		}
		if nextID == "" {
			return
		}
		currentID = nextID
	}
}

// propagateOneAncestor runs a single CAS-retry loop for one ancestor in
// the propagation chain, serialised by the per-node mutex so concurrent
// commits to the same parent don't thrash each other. Returns the next
// ancestor's ID (or "" if at the space root) and true on success, or
// "" and false on CAS exhaustion / hard error (drift counter
// incremented by this function).
func (d *kvfsDriver) propagateOneAncestor(spaceID, nodeID string, delta int64) (string, bool) {
	unlock := d.lockParent(spaceID, nodeID)
	defer unlock()

	var nextID string
	var getErr bool
	err := d.casRetryLoop("tree_propagation", func() error {
		node, rev, gerr := d.store.GetNode(spaceID, nodeID)
		if gerr != nil {
			getErr = true
			return gerr
		}
		now := time.Now().UnixNano()
		if delta != 0 {
			node.Size += delta
			if node.Size < 0 {
				node.Size = 0
			}
		}
		node.MTime = now
		node.ETag = calculateEtag(node.ID, now)
		if perr := d.store.PutNode(node, rev); perr != nil {
			return perr
		}
		nextID = node.ParentID
		return nil
	})
	switch {
	case err == nil:
		return nextID, true
	case getErr:
		// GetNode failed: byte-identical with the pre-refactor path — no drift
		// counter and no exhaustion counter (CASExhausted only fires when the
		// loop exhausts on CAS conflicts, which a GetNode error short-circuits).
		return "", false
	case err == ErrCASConflict:
		// CAS retries exhausted. casRetryLoop has already incremented
		// CASExhausted{tree_propagation}; we add the drift counter + the warn
		// the pre-refactor path logged here.
		TreeSizeDrift.Inc()
		d.log.Warn().Str("node_id", nodeID).Int64("delta", delta).Msg("tree propagation CAS retries exhausted")
		return "", false
	default:
		// PutNode hard error.
		TreeSizeDrift.Inc()
		d.log.Warn().Err(err).Str("node_id", nodeID).Int64("delta", delta).Msg("tree propagation failed")
		return "", false
	}
}

// recursiveDeleteNodesAndBlobs removes all descendant nodes and their S3 blobs.
func (d *kvfsDriver) recursiveDeleteNodesAndBlobs(ctx context.Context, spaceID, nodeID string) {
	children, _, err := d.store.GetChildren(spaceID, nodeID)
	if err != nil {
		return
	}
	for _, childID := range children {
		child, _, err := d.store.GetNode(spaceID, childID)
		if err != nil {
			continue
		}
		if child.Type == NodeTypeDir {
			d.recursiveDeleteNodesAndBlobs(ctx, spaceID, childID)
		} else if child.BlobID != "" {
			d.blob.Delete(ctx, BlobKey(spaceID, child.BlobID))
		}
		d.store.DeleteNode(spaceID, childID)
		d.store.DeleteChildren(spaceID, childID)
	}
}

// recursiveDeleteNodes removes all descendant nodes of a directory.
func (d *kvfsDriver) recursiveDeleteNodes(ctx context.Context, spaceID, nodeID string) {
	children, _, err := d.store.GetChildren(spaceID, nodeID)
	if err != nil {
		return
	}
	for _, childID := range children {
		child, _, err := d.store.GetNode(spaceID, childID)
		if err != nil {
			continue
		}
		if child.Type == NodeTypeDir {
			d.recursiveDeleteNodes(ctx, spaceID, childID)
		}
		d.store.DeleteNode(spaceID, childID)
	}
	d.store.DeleteChildren(spaceID, nodeID)
}
