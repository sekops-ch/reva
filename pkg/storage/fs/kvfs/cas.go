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
	"math/rand"
	"time"

	"github.com/pkg/errors"
)

// casRetryLoop runs fn under the standard compare-and-swap retry envelope: up
// to d.maxCASRetries() attempts, full-jitter backoff between attempts, and the
// CASRetries / CASExhausted counters keyed by op. fn signals its outcome by
// return value:
//
//	nil            -> success, the loop returns nil
//	ErrCASConflict -> retry (after CASRetries{op}++ and casBackoff)
//	any other err  -> abort immediately, returned to the caller as-is
//
// On attempt exhaustion the loop increments CASExhausted{op} and returns
// ErrCASConflict. Callers that need a distinct terminal action (a wrapped
// error message, an extra counter, a drift counter) inspect the returned
// ErrCASConflict.
func (d *kvfsDriver) casRetryLoop(op string, fn func() error) error {
	for attempt := 0; attempt < d.maxCASRetries(); attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if err != ErrCASConflict {
			return err
		}
		CASRetries.WithLabelValues(op).Inc()
		casBackoff(attempt)
	}
	CASExhausted.WithLabelValues(op).Inc()
	return ErrCASConflict
}

// casBackoff sleeps with exponential full-jitter backoff. The previous
// implementation capped at 50 ms and slept `base + jitter` (so the floor
// grew with each retry); under N>>10 concurrent writers on a hot parent
// it thrashed and exhausted the retry loop in ~500 ms total. The new
// curve picks uniform [0, min(2^attempt * 10ms, 1s)] per AWS's "Full
// Jitter" guidance: the floor is always 0 so colliding writers naturally
// desynchronise, and the ceiling rises with contention so heavily
// contended writers wait proportionally longer.
//
// Backwards-compatible signature — every existing call site continues to
// pass the attempt index unchanged.
func casBackoff(attempt int) {
	const cap = time.Second
	base := time.Duration(1<<uint(attempt)) * 10 * time.Millisecond
	if base <= 0 || base > cap {
		base = cap
	}
	time.Sleep(time.Duration(rand.Int63n(int64(base) + 1)))
}

// applyChildIntent atomically applies an add and/or remove against the
// children map for (spaceID, parentID). Retries on CAS conflict up to
// d.maxCASRetries() — on each retry we re-fetch the current map and
// re-apply the intent, so concurrent writers (even from other pods) no
// longer need their own outer retry loop. Replaces the read-modify-CAS
// pattern that previously lived in commitFileNode / Delete / Move and
// re-read the entire (potentially MB-sized) children value on every
// retry without merging.
//
// Returns the post-update map (after CAS commit) for callers that need it.
func (d *kvfsDriver) applyChildIntent(spaceID, parentID string, add map[string]string, remove []string) (ChildMap, error) {
	unlock := d.lockParent(spaceID, parentID)
	defer unlock()

	var children ChildMap
	err := d.casRetryLoop("children_intent", func() error {
		current, rev, err := d.store.GetChildren(spaceID, parentID)
		if err != nil {
			return err
		}
		for name, id := range add {
			current[name] = id
		}
		for _, name := range remove {
			delete(current, name)
		}
		if err := d.store.PutChildren(spaceID, parentID, current, rev); err != nil {
			return err
		}
		children = current
		return nil
	})
	if err != nil {
		if err == ErrCASConflict {
			return nil, errors.New("kvfs: applyChildIntent failed after max CAS retries")
		}
		return nil, err
	}
	return children, nil
}

// updateChildrenWithCAS retries a get-modify-put on a parent's children map.
// modify mutates the map in place; the put is retried on CAS conflict.
func (d *kvfsDriver) updateChildrenWithCAS(spaceID, parentID string, modify func(children ChildMap)) error {
	unlock := d.lockParent(spaceID, parentID)
	defer unlock()
	return d.casRetryLoop("children", func() error {
		children, rev, err := d.store.GetChildren(spaceID, parentID)
		if err != nil {
			return err
		}
		modify(children)
		return d.store.PutChildren(spaceID, parentID, children, rev)
	})
}

// updateChildrenWithCASCheck is updateChildrenWithCAS where modify can abort
// the operation by returning a non-nil error (returned to the caller as-is).
func (d *kvfsDriver) updateChildrenWithCASCheck(spaceID, parentID string, modify func(children ChildMap) error) error {
	unlock := d.lockParent(spaceID, parentID)
	defer unlock()
	return d.casRetryLoop("children", func() error {
		children, rev, err := d.store.GetChildren(spaceID, parentID)
		if err != nil {
			return err
		}
		if err := modify(children); err != nil {
			return err
		}
		return d.store.PutChildren(spaceID, parentID, children, rev)
	})
}

// putNodeWithCAS retries get-modify-put on a node entry. Serialised by the
// per-node mutex (lockParent keyed by nodeID) so concurrent updates to the
// same node from one pod go through a queue. The mutex shares its keyspace
// with applyChildIntent / updateChildrenWithCAS — operations targeting the
// same node ID (e.g. updating a directory's mtime AND adding to its
// children) are naturally serialised.
func (d *kvfsDriver) putNodeWithCAS(spaceID, nodeID string, modify func(node *NodeEntry)) error {
	unlock := d.lockParent(spaceID, nodeID)
	defer unlock()
	return d.casRetryLoop("node", func() error {
		node, rev, err := d.store.GetNode(spaceID, nodeID)
		if err != nil {
			return err
		}
		modify(node)
		return d.store.PutNode(node, rev)
	})
}

// putNodeWithCASCheck is putNodeWithCAS where modify can abort by returning a
// non-nil error. The sentinel errNoChange short-circuits to success without a
// put (the node was already in the desired state).
func (d *kvfsDriver) putNodeWithCASCheck(spaceID, nodeID string, modify func(node *NodeEntry) error) error {
	unlock := d.lockParent(spaceID, nodeID)
	defer unlock()
	return d.casRetryLoop("node", func() error {
		node, rev, err := d.store.GetNode(spaceID, nodeID)
		if err != nil {
			return err
		}
		if err := modify(node); err != nil {
			if err == errNoChange {
				return nil
			}
			return err
		}
		return d.store.PutNode(node, rev)
	})
}
