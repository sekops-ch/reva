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

// KV listing primitives: watcher streams that must prove completeness before
// any caller takes a verdict from them.

import (
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack/v5"
)

// watcherError drains a pending watcher error without blocking. nats.go
// queues the error BEFORE it sends the end-of-listing marker (and closes the
// channel on clean completion), so the marker alone does not prove a
// complete listing.
func watcherError(w nats.KeyWatcher) error {
	select {
	case err := <-w.Error():
		return err
	default:
		return nil
	}
}

// listAll returns every live entry in the bucket. A listing that cannot
// prove completeness fails instead of returning a prefix: destructive
// verdicts are taken from these results, so a truncated or corrupt listing
// must never read as success.
func listAll[T any](kv nats.KeyValue, bucket string) ([]*T, error) {
	watcher, err := kv.WatchAll(nats.IgnoreDeletes())
	if err != nil {
		if err == nats.ErrNoKeysFound {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "kvfs: failed to watch all %s", bucket)
	}
	defer watcher.Stop()

	var result []*T
	sawMarker := false
	for entry := range watcher.Updates() {
		if entry == nil {
			sawMarker = true
			break
		}
		var v T
		if err := msgpack.Unmarshal(entry.Value(), &v); err != nil {
			return nil, errors.Wrapf(err, "kvfs: corrupt entry %s in %s", entry.Key(), bucket)
		}
		result = append(result, &v)
	}
	if err := watcherError(watcher); err != nil {
		return nil, errors.Wrapf(err, "kvfs: listing %s ended on a watcher error", bucket)
	}
	if !sawMarker {
		return nil, errors.Errorf("kvfs: listing %s ended before the initial-values marker", bucket)
	}
	return result, nil
}

func listByPrefix[T any](kv nats.KeyValue, bucket, prefix string) ([]*T, error) {
	watcher, err := kv.Watch(prefix+">", nats.IgnoreDeletes())
	if err != nil {
		return nil, errors.Wrapf(err, "kvfs: failed to watch %s with prefix %s", bucket, prefix)
	}
	defer watcher.Stop()

	var result []*T
	sawMarker := false
	for entry := range watcher.Updates() {
		if entry == nil {
			sawMarker = true
			break
		}
		var v T
		if err := msgpack.Unmarshal(entry.Value(), &v); err != nil {
			return nil, errors.Wrapf(err, "kvfs: corrupt entry %s in %s", entry.Key(), bucket)
		}
		result = append(result, &v)
	}
	if err := watcherError(watcher); err != nil {
		return nil, errors.Wrapf(err, "kvfs: listing %s ended on a watcher error", bucket)
	}
	if !sawMarker {
		return nil, errors.Errorf("kvfs: listing %s ended before the initial-values marker", bucket)
	}
	return result, nil
}
