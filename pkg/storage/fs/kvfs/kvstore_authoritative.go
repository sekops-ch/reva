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

import (
	"bytes"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack/v5"
)

// KV tombstone markers, mirrored from nats.go where they are unexported: the
// raw last message of a deleted or purged key still exists and carries this
// header; KeyValue.Get folds it into ErrKeyNotFound.
const (
	kvOperationHeader = "KV-Operation"
	kvOperationDelete = "DEL"
	kvOperationPurge  = "PURGE"
)

// getLastAuthoritative confirms a direct-read miss with ONE deterministic read
// answered by the stream leader ($JS.API.STREAM.MSG.GET — never DIRECT.GET,
// which any replica may answer from a view that trails a just-acknowledged
// write). Found -> (value, revision, true, nil); tombstone or no message ->
// ok=false; transport and leader-election failures surface as errors, never as
// a silent not-found. No retries, no sleeps — consistency handling beyond this
// single authoritative read stays with the caller.
func (s *KVStore) getLastAuthoritative(kv nats.KeyValue, key string) ([]byte, uint64, bool, error) {
	bucket := kv.Bucket()
	start := time.Now()
	// The shared JetStream context waits 30 s; a leaderless stream must fail
	// this read fast and loudly instead of pinning the request that long.
	msg, err := s.js.GetLastMsg("KV_"+bucket, "$KV."+bucket+"."+key, nats.MaxWait(s.authReadTimeout))
	KVAuthoritativeReadDuration.WithLabelValues(bucket).Observe(time.Since(start).Seconds())
	if err != nil {
		if errors.Is(err, nats.ErrMsgNotFound) {
			return nil, 0, false, nil
		}
		KVAuthoritativeReads.WithLabelValues(bucket, "error").Inc()
		return nil, 0, false, errors.Wrapf(err, "kvfs: authoritative read of %s/%s failed", bucket, key)
	}
	if op := msg.Header.Get(kvOperationHeader); op == kvOperationDelete || op == kvOperationPurge {
		return nil, 0, false, nil
	}
	return msg.Data, msg.Sequence, true, nil
}

// countAuthoritativeOutcome records what a leader read established:
// lag_caught (the replica's view missed it) or confirmed_absent. Callers
// decide found at their own level: key presence, or NAME presence for children.
func countAuthoritativeOutcome(bucket string, found bool) {
	outcome := "confirmed_absent"
	if found {
		outcome = "lag_caught"
	}
	KVAuthoritativeReads.WithLabelValues(bucket, outcome).Inc()
}

// ConfirmChildAuthoritative confirms one name of a children map at the
// stream leader: a stale-but-present map is not a KV miss, so only the leader
// can tell replica lag from true absence. The value is scanned, never materialized.
func (s *KVStore) ConfirmChildAuthoritative(spaceID, parentID, name string) (string, bool, error) {
	bucket := s.children.Bucket()
	data, _, ok, err := s.getLastAuthoritative(s.children, childrenKey(spaceID, parentID))
	if err != nil {
		return "", false, err
	}
	if !ok {
		countAuthoritativeOutcome(bucket, false)
		return "", false, nil // no entry yet: the directory has no children
	}
	id, found, err := scanChildMap(data, name)
	if err != nil {
		KVAuthoritativeReads.WithLabelValues(bucket, "error").Inc()
		return "", false, err
	}
	countAuthoritativeOutcome(bucket, found)
	return id, found, nil
}

// GetNodeAuthoritative reads one node at the stream leader. Destructive
// re-checks use it: the listing that formed a removal verdict may be a
// replica's stale view, so only the leader can confirm the premise.
func (s *KVStore) GetNodeAuthoritative(spaceID, nodeID string) (*NodeEntry, bool, error) {
	bucket := s.nodes.Bucket()
	data, _, ok, err := s.getLastAuthoritative(s.nodes, nodeKey(spaceID, nodeID))
	if err != nil {
		return nil, false, err
	}
	if !ok {
		countAuthoritativeOutcome(bucket, false)
		return nil, false, nil
	}
	var n NodeEntry
	if err := msgpack.Unmarshal(data, &n); err != nil {
		KVAuthoritativeReads.WithLabelValues(bucket, "error").Inc()
		return nil, false, errors.Wrapf(err, "kvfs: corrupt node %s/%s at the leader", spaceID, nodeID)
	}
	countAuthoritativeOutcome(bucket, true)
	return &n, true, nil
}

// GetSpaceAuthoritative reads one space entry at the stream leader; the
// ghost-space reap confirms a space's absence with it before touching the
// space's keys.
func (s *KVStore) GetSpaceAuthoritative(spaceID string) (*SpaceEntry, bool, error) {
	bucket := s.spaces.Bucket()
	data, _, ok, err := s.getLastAuthoritative(s.spaces, spaceID)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		countAuthoritativeOutcome(bucket, false)
		return nil, false, nil
	}
	var sp SpaceEntry
	if err := msgpack.Unmarshal(data, &sp); err != nil {
		KVAuthoritativeReads.WithLabelValues(bucket, "error").Inc()
		return nil, false, errors.Wrapf(err, "kvfs: corrupt space %s at the leader", spaceID)
	}
	countAuthoritativeOutcome(bucket, true)
	return &sp, true, nil
}

// scanChildMap finds one name in a msgpack-encoded ChildMap with a streaming
// decoder — the map is never materialized, so a leader confirm on a large
// directory costs one allocation-light scan instead of a full unmarshal.
func scanChildMap(data []byte, name string) (string, bool, error) {
	dec := msgpack.NewDecoder(bytes.NewReader(data))
	n, err := dec.DecodeMapLen()
	if err != nil {
		return "", false, errors.Wrap(err, "kvfs: failed to decode children map")
	}
	for i := 0; i < n; i++ {
		key, err := dec.DecodeString()
		if err != nil {
			return "", false, errors.Wrap(err, "kvfs: failed to decode children map key")
		}
		if key == name {
			id, err := dec.DecodeString()
			if err != nil {
				return "", false, errors.Wrap(err, "kvfs: failed to decode children map value")
			}
			return id, true, nil
		}
		if err := dec.Skip(); err != nil {
			return "", false, errors.Wrap(err, "kvfs: failed to skip children map value")
		}
	}
	return "", false, nil
}
