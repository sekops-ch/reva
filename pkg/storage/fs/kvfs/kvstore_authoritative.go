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
	msg, err := s.js.GetLastMsg("KV_"+bucket, "$KV."+bucket+"."+key)
	if err != nil {
		if errors.Is(err, nats.ErrMsgNotFound) {
			KVAuthoritativeReads.WithLabelValues(bucket, "absent").Inc()
			return nil, 0, false, nil
		}
		return nil, 0, false, errors.Wrapf(err, "kvfs: authoritative read of %s/%s failed", bucket, key)
	}
	if op := msg.Header.Get(kvOperationHeader); op == kvOperationDelete || op == kvOperationPurge {
		KVAuthoritativeReads.WithLabelValues(bucket, "absent").Inc()
		return nil, 0, false, nil
	}
	KVAuthoritativeReads.WithLabelValues(bucket, "hit").Inc()
	return msg.Data, msg.Sequence, true, nil
}

// GetChildrenAuthoritative re-reads a children key from the stream leader.
// resolveRelativePath uses it to confirm that a name absent from a possibly
// stale children map is truly absent before answering not-found.
func (s *KVStore) GetChildrenAuthoritative(spaceID, parentID string) (ChildMap, uint64, error) {
	data, rev, ok, err := s.getLastAuthoritative(s.children, childrenKey(spaceID, parentID))
	if err != nil {
		return nil, 0, err
	}
	if !ok {
		return ChildMap{}, 0, nil // empty directory, no entry yet
	}
	var children ChildMap
	if err := msgpack.Unmarshal(data, &children); err != nil {
		return nil, 0, errors.Wrap(err, "kvfs: failed to unmarshal children")
	}
	return children, rev, nil
}
