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

// Authoritative leader reads: the single deterministic STREAM.MSG.GET that
// confirms a direct KV read's miss against the stream leader.

package kvfs

// GC metadata reconcilers: crash-residue repair whose destructive verdicts
// are confirmed at the stream leader (see README section 9.1).

import (
	"context"
	"time"
)

// reconcileChildren walks oc-children for a single space and resolves any
// entry whose recorded child does not match the corresponding node's own
// ParentID. This catches the two interesting crash modes for Move (which is a
// 3-phase CAS sequence at kvfs.go: node, add-to-new, remove-from-old):
//
//  1. A child entry references a nodeID whose `node.ParentID` points at a
//     different directory (the "duplicate children" case the upstream review
//     on opencloud-eu/reva#625 raised).
//  2. A child entry references a nodeID that no longer exists at all (the
//     dangling-ref case).
//
// Resolution: the node's own ParentID is authoritative. Any children entry
// whose nodeID disagrees with truth is removed from that parent's children
// map. The node itself is left untouched.
//
// Best-effort interim safety net until a proper Move journal / WAL lands.
// Honors the GC dry-run flag.
func (gc *blobGC) reconcileChildren(ctx context.Context, spaceID string) int {
	if ctx.Err() != nil {
		return 0 // sweep lease lost: no destructive repair without the fence
	}
	nodes, err := gc.store.ListNodesBySpace(spaceID)
	if err != nil {
		gc.log.Warn().Err(err).Str("space_id", spaceID).Msg("gc: reconcileChildren: list nodes failed")
		return 0
	}

	// Candidate screen: nodeID -> ParentID as the listing saw it. The listing
	// may be a replica's stale view, so it only nominates candidates; every
	// removal verdict is confirmed at the leader below.
	truth := make(map[string]string, len(nodes))
	parents := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		truth[n.ID] = n.ParentID
		if n.Type == NodeTypeDir {
			parents[n.ID] = struct{}{}
		}
	}

	reconciled := 0
	for parentID := range parents {
		if ctx.Err() != nil {
			return reconciled // fenced mid-repair
		}
		children, rev, err := gc.store.GetChildren(spaceID, parentID)
		if err != nil {
			continue
		}
		dirty := false
		for name, childID := range children {
			actual, exists := truth[childID]
			if exists && actual == parentID {
				continue
			}
			// A create or move that landed after the listing is replica lag,
			// not residue: only the leader can confirm the removal premise.
			node, found, err := gc.store.GetNodeAuthoritative(spaceID, childID)
			if err != nil {
				gc.log.Warn().Err(err).
					Str("space_id", spaceID).
					Str("child_id", childID).
					Msg("gc: reconcileChildren: leader confirm failed; keeping the entry this cycle")
				GCErrors.Inc()
				continue
			}
			if found && node.ParentID == parentID {
				continue // lag caught: the entry is live
			}
			// Leader-confirmed: dangling (no node) or stale (node belongs
			// elsewhere). An absent node is genuine residue without an age
			// guard: commits write the node before its children entry, and
			// deletes unlink before removing the node.
			reason := "stale"
			actualParent := ""
			if !found {
				reason = "dangling"
			} else {
				actualParent = node.ParentID
			}
			if gc.opts.GCDryRun {
				gc.log.Info().
					Str("space_id", spaceID).
					Str("parent_id", parentID).
					Str("child_id", childID).
					Str("name", name).
					Str("reason", reason).
					Str("actual_parent", actualParent).
					Msg("gc: would remove stray children entry (dry-run)")
				reconciled++
				GCChildrenReconciled.Inc()
				continue
			}
			delete(children, name)
			dirty = true
			reconciled++
			GCChildrenReconciled.Inc()
			gc.log.Info().
				Str("space_id", spaceID).
				Str("parent_id", parentID).
				Str("child_id", childID).
				Str("name", name).
				Str("reason", reason).
				Str("actual_parent", actualParent).
				Msg("gc: removed stray children entry")
		}
		if dirty {
			if err := gc.store.PutChildren(spaceID, parentID, children, rev); err != nil {
				gc.log.Warn().Err(err).
					Str("space_id", spaceID).
					Str("parent_id", parentID).
					Msg("gc: reconcileChildren: PutChildren failed; skipping this parent for the cycle")
			}
		}
	}

	return reconciled
}

// reconcileTrash walks oc-trash for a single space and removes any trash
// entry whose referenced node is already live AND reachable in the tree.
// This catches the RestoreRecycleItem crash mode: PutNode + updateChildren
// succeeded but DeleteTrash did not, leaving the node both restored AND
// in trash.
//
// KVFS keeps directory nodes alive in oc-nodes even while trashed (for
// subtree restorability). A trashed directory has a node entry but is NOT
// listed in any parent's children map. A restored directory IS reachable.
// The reachability check distinguishes the two cases.
//
// Resolution: the node is live and reachable; the trash entry is stale.
// Honors the GC dry-run flag.
func (gc *blobGC) reconcileTrash(ctx context.Context, spaceID string, cutoff time.Time) int {
	if ctx.Err() != nil {
		return 0 // sweep lease lost: no destructive repair without the fence
	}
	nodes, err := gc.store.ListNodesBySpace(spaceID)
	if err != nil {
		gc.log.Warn().Err(err).Str("space_id", spaceID).Msg("gc: reconcileTrash: list nodes failed")
		return 0
	}

	liveNodes := make(map[string]*NodeEntry, len(nodes))
	for _, n := range nodes {
		liveNodes[n.ID] = n
	}

	// Build the set of node IDs reachable from some parent's children map.
	reachable := make(map[string]struct{})
	for _, n := range nodes {
		if n.Type != NodeTypeDir {
			continue
		}
		children, _, err := gc.store.GetChildren(spaceID, n.ID)
		if err != nil {
			continue
		}
		for _, childID := range children {
			reachable[childID] = struct{}{}
		}
	}

	trash, err := gc.store.ListTrash(spaceID)
	if err != nil {
		gc.log.Warn().Err(err).Str("space_id", spaceID).Msg("gc: reconcileTrash: list trash failed")
		return 0
	}

	reconciled := 0
	for _, t := range trash {
		if ctx.Err() != nil {
			return reconciled // fenced mid-repair
		}
		// Mid-delete window: Delete writes the trash entry before it unlinks
		// and removes the node, so a fresh entry's node is live and reachable
		// even at the leader. Entries younger than the sweep cutoff wait.
		if time.Unix(t.DeletionTime, 0).After(cutoff) {
			continue
		}
		node, isLive := liveNodes[t.NodeID]
		if !isLive {
			continue
		}
		if _, ok := reachable[t.NodeID]; !ok {
			continue
		}
		// Candidate from possibly stale views: confirm liveness and
		// reachability at the leader before removing the entry.
		leaderNode, found, err := gc.store.GetNodeAuthoritative(spaceID, t.NodeID)
		if err != nil {
			gc.log.Warn().Err(err).Str("space_id", spaceID).Str("node_id", t.NodeID).
				Msg("gc: reconcileTrash: leader confirm failed; keeping the entry this cycle")
			GCErrors.Inc()
			continue
		}
		if !found {
			continue // the delete completed: the entry is legitimate trash
		}
		childID, reach, err := gc.store.ConfirmChildAuthoritative(spaceID, leaderNode.ParentID, leaderNode.Name)
		if err != nil {
			gc.log.Warn().Err(err).Str("space_id", spaceID).Str("node_id", t.NodeID).
				Msg("gc: reconcileTrash: leader reachability confirm failed; keeping the entry this cycle")
			GCErrors.Inc()
			continue
		}
		if !reach || childID != t.NodeID {
			continue // not reachable at the leader: keep the trash entry
		}
		node = leaderNode
		// Node is live AND reachable at the leader — trash entry is stale.
		if gc.opts.GCDryRun {
			gc.log.Info().
				Str("space_id", spaceID).
				Str("trash_key", t.Key).
				Str("node_id", t.NodeID).
				Str("parent_id", node.ParentID).
				Msg("gc: would remove stale trash entry (node is live and reachable) (dry-run)")
			reconciled++
			GCTrashReconciled.Inc()
			continue
		}
		if err := gc.store.DeleteTrash(spaceID, t.Key); err != nil {
			gc.log.Warn().Err(err).
				Str("space_id", spaceID).
				Str("trash_key", t.Key).
				Msg("gc: reconcileTrash: DeleteTrash failed; skipping this entry")
			continue
		}
		reconciled++
		GCTrashReconciled.Inc()
		gc.log.Info().
			Str("space_id", spaceID).
			Str("trash_key", t.Key).
			Str("node_id", t.NodeID).
			Str("parent_id", node.ParentID).
			Msg("gc: removed stale trash entry (node already restored)")
	}

	return reconciled
}
