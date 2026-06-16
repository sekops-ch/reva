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
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack/v5"
)

// MetadataStore defines the metadata storage operations used by the kvfs driver.
type MetadataStore interface {
	Close()
	GetNode(spaceID, nodeID string) (*NodeEntry, uint64, error)
	PutNode(n *NodeEntry, expectedRev uint64) error
	DeleteNode(spaceID, nodeID string) error
	ListNodesBySpace(spaceID string) ([]*NodeEntry, error)
	GetNodes(spaceID string, nodeIDs []string) (map[string]*NodeEntry, error)
	GetChildren(spaceID, parentID string) (ChildMap, uint64, error)
	PutChildren(spaceID, parentID string, children ChildMap, expectedRev uint64) error
	DeleteChildren(spaceID, parentID string) error
	GetSpace(spaceID string) (*SpaceEntry, uint64, error)
	PutSpace(space *SpaceEntry, expectedRev uint64) error
	DeleteSpace(spaceID string) error
	ListSpaces(filter func(*SpaceEntry) bool) ([]*SpaceEntry, error)
	PutTrash(t *TrashEntry) error
	GetTrash(spaceID, key string) (*TrashEntry, error)
	DeleteTrash(spaceID, key string) error
	ListTrash(spaceID string) ([]*TrashEntry, error)
	PutVersion(v *VersionEntry) error
	ListVersions(spaceID, nodeID string) ([]*VersionEntry, error)
	ListVersionsBySpace(spaceID string) ([]*VersionEntry, error)
	GetVersion(spaceID, nodeID, key string) (*VersionEntry, error)
	DeleteVersion(spaceID, nodeID, key string) error
	PutUpload(u *UploadSession) error
	GetUpload(uploadID string) (*UploadSession, error)
	DeleteUpload(uploadID string) error
	ListUploadsBySpace(spaceID string) ([]*UploadSession, error)
	ListAllNodes() ([]*NodeEntry, error)
	ListAllVersions() ([]*VersionEntry, error)
	ListAllUploads() ([]*UploadSession, error)
	ListAllUploadEntries() ([]*UploadEntry, error)
	PurgeDeletedUploads() error
	ListAllTrash() ([]*TrashEntry, error)
	TryAcquireLock(key string, holder string, ttl time.Duration) (bool, error)
	ReleaseLock(key string, holder string) error
}

// KVStore wraps NATS JetStream KV buckets for kvfs storage.
// It provides typed CRUD operations with optimistic concurrency control
// via NATS KV revisions (Compare-And-Swap).
type KVStore struct {
	conn *nats.Conn
	js   nats.JetStreamContext // shared with upload-cache backends (see JetStream())

	nodes    nats.KeyValue // bucket: "{prefix}-nodes"    — NodeEntry per node
	children nats.KeyValue // bucket: "{prefix}-children" — child map per parent
	spaces   nats.KeyValue // bucket: "{prefix}-spaces"   — SpaceEntry per space
	trash    nats.KeyValue // bucket: "{prefix}-trash"    — TrashEntry per trashed item
	uploads  nats.KeyValue // bucket: "{prefix}-uploads"  — UploadSession per upload
	versions nats.KeyValue // bucket: "{prefix}-versions" — VersionEntry per version
	locks    nats.KeyValue // bucket: "{prefix}-locks"    — distributed locks

	// maxCASRetries bounds the create-conflict merge-retry loop in
	// PutChildren so every CAS path in the driver honours the same
	// configured retry budget. Resolved from Options at construction.
	maxCASRetries int
}

// NewKVStore connects to NATS JetStream and initializes KV buckets.
func NewKVStore(opts *Options) (*KVStore, error) {
	// Build NATS connection options with explicit reconnection tuning.
	// Defaults (MaxReconnect=60, ReconnectWait=2s) are too conservative for
	// a storage driver that must survive pod deaths without dropping requests.
	natsOpts := []nats.Option{
		nats.Name("opencloud-kvfs"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(1 * time.Second),
		nats.ReconnectJitter(500*time.Millisecond, 2*time.Second),
		nats.RetryOnFailedConnect(true),
	}
	if opts.NATSUsername != "" {
		natsOpts = append(natsOpts, nats.UserInfo(opts.NATSUsername, opts.NATSPassword))
	}

	servers := strings.Join(opts.NATSNodes, ",")

	nc, err := nats.Connect(servers, natsOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "kvfs: failed to connect to NATS")
	}

	js, err := nc.JetStream(nats.MaxWait(30 * time.Second))
	if err != nil {
		nc.Close()
		return nil, errors.Wrap(err, "kvfs: failed to get JetStream context")
	}

	store := &KVStore{
		conn:          nc,
		js:            js,
		maxCASRetries: resolveMaxCASRetries(opts),
	}

	// Create or bind to KV buckets. The children bucket may be created with a
	// raised MaxValueSize so directories with many entries do not hit the
	// default 1 MiB NATS KV value ceiling (see Options.ChildrenMaxValueSize).
	childrenBucket := opts.BucketName("children")
	buckets := map[string]*nats.KeyValue{
		opts.BucketName("nodes"):    &store.nodes,
		childrenBucket:              &store.children,
		opts.BucketName("spaces"):   &store.spaces,
		opts.BucketName("trash"):    &store.trash,
		opts.BucketName("uploads"):  &store.uploads,
		opts.BucketName("versions"): &store.versions,
		opts.BucketName("locks"):    &store.locks,
	}

	for name, target := range buckets {
		kv, err := js.KeyValue(name)
		if err != nil {
			cfg := &nats.KeyValueConfig{
				Bucket:   name,
				History:  1,
				Replicas: opts.NATSReplicas,
			}
			if name == childrenBucket && opts.ChildrenMaxValueSize > 0 {
				cfg.MaxValueSize = opts.ChildrenMaxValueSize
			}
			kv, err = js.CreateKeyValue(cfg)
			if err != nil {
				nc.Close()
				return nil, errors.Wrapf(err, "kvfs: failed to create KV bucket %s", name)
			}
		} else {
			if opts.NATSReplicas > 1 {
				ensureReplicas(js, kv, opts.NATSReplicas)
			}
			if name == childrenBucket && opts.ChildrenMaxValueSize > 0 {
				if err := ensureMaxValueSize(js, kv, opts.ChildrenMaxValueSize); err != nil {
					// NATS will refuse MaxMsgSize > server max_payload. The
					// only fix is at the NATS server level (max_payload in
					// nats.conf). Surface the rejection — silent failure
					// here would leave operators thinking the env var took
					// effect when it didn't.
					fmt.Printf("kvfs: WARNING — ensureMaxValueSize on %s failed: %v\n", name, err)
					fmt.Printf("kvfs: HINT — NATS server max_payload may be lower than requested %d bytes; bump it in nats.conf (current default 1 MiB)\n", opts.ChildrenMaxValueSize)
				}
			}
		}
		*target = kv
	}

	return store, nil
}

// Close closes the NATS connection.
func (s *KVStore) Close() {
	if s.conn != nil {
		s.conn.Close()
	}
}

// JetStream returns the underlying JetStream context. Shared with upload-cache
// backends (natsStreamUploadCache) so we don't open a second connection to the
// same NATS cluster from the same process. Callers must NOT close the returned
// context — its lifetime is bound to KVStore.Close().
func (s *KVStore) JetStream() nats.JetStreamContext {
	return s.js
}

type kvLockEntry struct {
	Holder    string `json:"holder"`
	ExpiresAt int64  `json:"expires_at"`
}

func (s *KVStore) TryAcquireLock(key string, holder string, ttl time.Duration) (bool, error) {
	now := time.Now()
	entry := kvLockEntry{
		Holder:    holder,
		ExpiresAt: now.Add(ttl).UnixNano(),
	}
	data, _ := json.Marshal(entry)

	_, err := s.locks.Create(key, data)
	if err == nil {
		return true, nil
	}

	kve, err := s.locks.Get(key)
	if err != nil {
		// Benign race: the previous holder released the lock between our Create
		// and this Get (or the bucket was freshly created). A missing key means
		// the lock is free, so retry Create once; if that loses to another
		// contender, report "not acquired" with a nil error so the caller's GC
		// cycle skips cleanly instead of logging a spurious "failed to acquire".
		if err == nats.ErrKeyNotFound {
			if _, cerr := s.locks.Create(key, data); cerr == nil {
				return true, nil
			}
			return false, nil
		}
		return false, err
	}

	var existing kvLockEntry
	if err := json.Unmarshal(kve.Value(), &existing); err != nil {
		_, err = s.locks.Update(key, data, kve.Revision())
		return err == nil, nil
	}

	if now.UnixNano() <= existing.ExpiresAt {
		return false, nil
	}

	_, err = s.locks.Update(key, data, kve.Revision())
	if err != nil {
		return false, nil
	}
	return true, nil
}

func (s *KVStore) ReleaseLock(key string, holder string) error {
	kve, err := s.locks.Get(key)
	if err != nil {
		return nil
	}

	var existing kvLockEntry
	if err := json.Unmarshal(kve.Value(), &existing); err == nil {
		if existing.Holder != holder {
			return nil
		}
	}

	return s.locks.Delete(key)
}

// streamUpdater is the subset of nats.JetStreamContext needed by ensureReplicas.
type streamUpdater interface {
	StreamInfo(stream string, opts ...nats.JSOpt) (*nats.StreamInfo, error)
	UpdateStream(cfg *nats.StreamConfig, opts ...nats.JSOpt) (*nats.StreamInfo, error)
}

// kvStatusProvider is the subset of nats.KeyValue needed by ensureReplicas.
type kvStatusProvider interface {
	Status() (nats.KeyValueStatus, error)
}

// ensureMaxValueSize widens an existing KV bucket's MaxValueSize when the
// configured ChildrenMaxValueSize is larger than the live ceiling. NATS's
// MaxValueSize maps to the underlying JetStream stream's MaxMsgSize field.
// Without this, raising STORAGE_USERS_KVFS_CHILDREN_MAX_VALUE_SIZE in a
// helm upgrade has no effect because the bucket already exists and the
// Create path is skipped.
//
// Idempotent — no-op when the live value is already ≥ desired.
func ensureMaxValueSize(js streamUpdater, kv kvStatusProvider, desired int32) error {
	if desired <= 0 {
		return nil
	}
	status, err := kv.Status()
	if err != nil {
		return errors.Wrap(err, "kvfs: failed to get bucket status")
	}
	streamName := fmt.Sprintf("KV_%s", status.Bucket())
	info, err := js.StreamInfo(streamName)
	if err != nil {
		return errors.Wrapf(err, "kvfs: failed to get stream info for %s", streamName)
	}
	if info.Config.MaxMsgSize >= desired {
		return nil
	}
	cfg := info.Config
	cfg.MaxMsgSize = desired
	if _, err := js.UpdateStream(&cfg); err != nil {
		return errors.Wrapf(err, "kvfs: failed to update MaxMsgSize from %d to %d for %s", info.Config.MaxMsgSize, desired, streamName)
	}
	return nil
}

// ensureReplicas upgrades an existing KV bucket's replica count if it is
// below the desired value. This handles the R=1 → R=3 upgrade path for
// buckets that were created before replication was configured.
func ensureReplicas(js streamUpdater, kv kvStatusProvider, desired int) error {
	if desired <= 1 {
		return nil
	}

	status, err := kv.Status()
	if err != nil {
		return errors.Wrap(err, "kvfs: failed to get bucket status")
	}

	current := status.Config().Replicas
	if current >= desired {
		return nil
	}

	streamName := fmt.Sprintf("KV_%s", status.Bucket())
	info, err := js.StreamInfo(streamName)
	if err != nil {
		return errors.Wrapf(err, "kvfs: failed to get stream info for %s", streamName)
	}

	cfg := info.Config
	cfg.Replicas = desired
	if _, err := js.UpdateStream(&cfg); err != nil {
		return errors.Wrapf(err, "kvfs: failed to update replicas from %d to %d for %s", current, desired, streamName)
	}

	return nil
}

// --- Generic KV helpers ---

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
	for entry := range watcher.Updates() {
		if entry == nil {
			break
		}
		var v T
		if err := msgpack.Unmarshal(entry.Value(), &v); err != nil {
			continue
		}
		result = append(result, &v)
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
	for entry := range watcher.Updates() {
		if entry == nil {
			break
		}
		var v T
		if err := msgpack.Unmarshal(entry.Value(), &v); err != nil {
			continue
		}
		result = append(result, &v)
	}
	return result, nil
}

// --- Node operations ---

// nodeKey builds the KV key for a node: "{spaceID}.{nodeID}".
func nodeKey(spaceID, nodeID string) string {
	return spaceID + "." + nodeID
}

// GetNode retrieves a node by space and node ID.
// Returns the node, its current KV revision, and any error.
func (s *KVStore) GetNode(spaceID, nodeID string) (*NodeEntry, uint64, error) {
	start := time.Now()
	entry, err := s.nodes.Get(nodeKey(spaceID, nodeID))
	KVOperationDuration.WithLabelValues("nodes", "get").Observe(time.Since(start).Seconds())
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return nil, 0, ErrNodeNotFound
		}
		return nil, 0, errors.Wrap(err, "kvfs: failed to get node")
	}

	var node NodeEntry
	if err := msgpack.Unmarshal(entry.Value(), &node); err != nil {
		return nil, 0, errors.Wrap(err, "kvfs: failed to unmarshal node")
	}

	return &node, entry.Revision(), nil
}

// PutNode creates or updates a node using CAS (Compare-And-Swap).
// If expectedRev is 0, the node must not exist (create).
// If expectedRev > 0, the node must be at that revision (update).
func (s *KVStore) PutNode(n *NodeEntry, expectedRev uint64) error {
	data, err := msgpack.Marshal(n)
	if err != nil {
		return errors.Wrap(err, "kvfs: failed to marshal node")
	}

	key := nodeKey(n.SpaceID, n.ID)
	op := "create"
	if expectedRev > 0 {
		op = "update"
	}
	start := time.Now()
	if expectedRev == 0 {
		_, err = s.nodes.Create(key, data)
	} else {
		_, err = s.nodes.Update(key, data, expectedRev)
	}
	KVOperationDuration.WithLabelValues("nodes", op).Observe(time.Since(start).Seconds())

	if err != nil {
		if err == nats.ErrKeyExists || isWrongLastSequence(err) {
			CASConflicts.WithLabelValues("nodes").Inc()
			return ErrCASConflict
		}
		return errors.Wrap(err, "kvfs: failed to put node")
	}
	return nil
}

// DeleteNode removes a node from the KV store.
func (s *KVStore) DeleteNode(spaceID, nodeID string) error {
	start := time.Now()
	err := s.nodes.Delete(nodeKey(spaceID, nodeID))
	KVOperationDuration.WithLabelValues("nodes", "delete").Observe(time.Since(start).Seconds())
	if err != nil && err != nats.ErrKeyNotFound {
		return errors.Wrap(err, "kvfs: failed to delete node")
	}
	return nil
}

// ListNodesBySpace lists all nodes belonging to a space.
func (s *KVStore) ListNodesBySpace(spaceID string) ([]*NodeEntry, error) {
	return listByPrefix[NodeEntry](s.nodes, "nodes", spaceID+".")
}

const kvBatchConcurrency = 50

// GetNodes retrieves multiple nodes concurrently with bounded parallelism.
// Returns a map of nodeID → *NodeEntry. Missing nodes are silently omitted.
func (s *KVStore) GetNodes(spaceID string, nodeIDs []string) (map[string]*NodeEntry, error) {
	if len(nodeIDs) == 0 {
		return nil, nil
	}

	type nodeResult struct {
		id   string
		node *NodeEntry
	}

	results := make(chan nodeResult, len(nodeIDs))
	sem := make(chan struct{}, kvBatchConcurrency)
	var wg sync.WaitGroup

	for _, id := range nodeIDs {
		wg.Add(1)
		sem <- struct{}{}
		go func(nodeID string) {
			defer wg.Done()
			defer func() { <-sem }()

			node, _, err := s.GetNode(spaceID, nodeID)
			if err != nil {
				return
			}
			results <- nodeResult{id: nodeID, node: node}
		}(id)
	}

	wg.Wait()
	close(results)

	nodes := make(map[string]*NodeEntry, len(nodeIDs))
	for r := range results {
		nodes[r.id] = r.node
	}
	return nodes, nil
}

// --- Children operations ---

// childrenKey builds the KV key for a parent's children list.
func childrenKey(spaceID, parentID string) string {
	return spaceID + "." + parentID
}

// ChildMap is a mapping from child name to child node ID.
type ChildMap map[string]string

// GetChildren retrieves the children of a parent node.
// Returns name→nodeID map, KV revision, and any error.
func (s *KVStore) GetChildren(spaceID, parentID string) (ChildMap, uint64, error) {
	start := time.Now()
	entry, err := s.children.Get(childrenKey(spaceID, parentID))
	KVOperationDuration.WithLabelValues("children", "get").Observe(time.Since(start).Seconds())
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return ChildMap{}, 0, nil // empty directory, no entry yet
		}
		return nil, 0, errors.Wrap(err, "kvfs: failed to get children")
	}

	var children ChildMap
	if err := msgpack.Unmarshal(entry.Value(), &children); err != nil {
		return nil, 0, errors.Wrap(err, "kvfs: failed to unmarshal children")
	}

	return children, entry.Revision(), nil
}

// PutChildren updates the children list for a parent using CAS.
// If expectedRev is 0, the entry must not exist (create).
// When a Create conflict occurs (another pod created children concurrently),
// we merge the new children into the existing map and retry with CAS.
func (s *KVStore) PutChildren(spaceID, parentID string, children ChildMap, expectedRev uint64) error {
	data, err := msgpack.Marshal(children)
	if err != nil {
		return errors.Wrap(err, "kvfs: failed to marshal children")
	}
	ChildrenValueBytes.Observe(float64(len(data)))

	key := childrenKey(spaceID, parentID)
	start := time.Now()
	if expectedRev == 0 {
		_, err = s.children.Create(key, data)
		KVOperationDuration.WithLabelValues("children", "create").Observe(time.Since(start).Seconds())
		if err == nats.ErrKeyExists {
			CASConflicts.WithLabelValues("children").Inc()
			for retry := 0; retry < s.maxCASRetries; retry++ {
				CASRetries.WithLabelValues("children").Inc()
				entry, rerr := s.children.Get(key)
				if rerr != nil {
					return errors.Wrap(rerr, "kvfs: failed to re-fetch children after create conflict")
				}

				var existing ChildMap
				if uerr := msgpack.Unmarshal(entry.Value(), &existing); uerr != nil {
					existing = ChildMap{}
				}
				for k, v := range children {
					existing[k] = v
				}

				merged, merr := msgpack.Marshal(existing)
				if merr != nil {
					return errors.Wrap(merr, "kvfs: failed to marshal merged children")
				}

				retryStart := time.Now()
				_, err = s.children.Update(key, merged, entry.Revision())
				KVOperationDuration.WithLabelValues("children", "update").Observe(time.Since(retryStart).Seconds())
				if err == nil {
					return nil
				}
				if err == nats.ErrKeyExists || isWrongLastSequence(err) {
					CASConflicts.WithLabelValues("children").Inc()
					casBackoff(retry)
					continue
				}
				break
			}
		}
	} else {
		_, err = s.children.Update(key, data, expectedRev)
		KVOperationDuration.WithLabelValues("children", "update").Observe(time.Since(start).Seconds())
	}

	if err != nil {
		if err == nats.ErrKeyExists || isWrongLastSequence(err) {
			CASConflicts.WithLabelValues("children").Inc()
			return ErrCASConflict
		}
		return errors.Wrap(err, "kvfs: failed to put children")
	}
	return nil
}

// isWrongLastSequence checks if the error is a NATS "wrong last sequence" CAS failure.
// NATS appends the expected sequence number (e.g. "nats: wrong last sequence: 7547")
// so we use a prefix check rather than exact match.
func isWrongLastSequence(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "nats: wrong last sequence")
}

// DeleteChildren removes the children list for a parent node.
func (s *KVStore) DeleteChildren(spaceID, parentID string) error {
	start := time.Now()
	err := s.children.Delete(childrenKey(spaceID, parentID))
	KVOperationDuration.WithLabelValues("children", "delete").Observe(time.Since(start).Seconds())
	if err != nil && err != nats.ErrKeyNotFound {
		return errors.Wrap(err, "kvfs: failed to delete children")
	}
	return nil
}

// --- Space operations ---

// GetSpace retrieves a space entry.
func (s *KVStore) GetSpace(spaceID string) (*SpaceEntry, uint64, error) {
	start := time.Now()
	entry, err := s.spaces.Get(spaceID)
	KVOperationDuration.WithLabelValues("spaces", "get").Observe(time.Since(start).Seconds())
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return nil, 0, ErrSpaceNotFound
		}
		return nil, 0, errors.Wrap(err, "kvfs: failed to get space")
	}

	var space SpaceEntry
	if err := msgpack.Unmarshal(entry.Value(), &space); err != nil {
		return nil, 0, errors.Wrap(err, "kvfs: failed to unmarshal space")
	}

	return &space, entry.Revision(), nil
}

// PutSpace creates or updates a space entry.
func (s *KVStore) PutSpace(space *SpaceEntry, expectedRev uint64) error {
	data, err := msgpack.Marshal(space)
	if err != nil {
		return errors.Wrap(err, "kvfs: failed to marshal space")
	}

	op := "create"
	if expectedRev > 0 {
		op = "update"
	}
	start := time.Now()
	if expectedRev == 0 {
		_, err = s.spaces.Create(space.ID, data)
	} else {
		_, err = s.spaces.Update(space.ID, data, expectedRev)
	}
	KVOperationDuration.WithLabelValues("spaces", op).Observe(time.Since(start).Seconds())

	if err != nil {
		if err == nats.ErrKeyExists || isWrongLastSequence(err) {
			CASConflicts.WithLabelValues("spaces").Inc()
			return ErrCASConflict
		}
		return errors.Wrap(err, "kvfs: failed to put space")
	}
	return nil
}

// DeleteSpace removes a space entry.
func (s *KVStore) DeleteSpace(spaceID string) error {
	start := time.Now()
	err := s.spaces.Delete(spaceID)
	KVOperationDuration.WithLabelValues("spaces", "delete").Observe(time.Since(start).Seconds())
	if err != nil && err != nats.ErrKeyNotFound {
		return errors.Wrap(err, "kvfs: failed to delete space")
	}
	return nil
}

// ListSpaces iterates all spaces and returns those matching the filter.
func (s *KVStore) ListSpaces(filter func(*SpaceEntry) bool) ([]*SpaceEntry, error) {
	all, err := listAll[SpaceEntry](s.spaces, "spaces")
	if err != nil {
		return nil, err
	}
	if filter == nil {
		return all, nil
	}
	var result []*SpaceEntry
	for _, space := range all {
		if filter(space) {
			result = append(result, space)
		}
	}
	return result, nil
}

// --- Trash operations ---

// trashKey builds the KV key for a trash entry.
func trashKey(spaceID, key string) string {
	return spaceID + "." + key
}

// PutTrash stores a trash entry.
func (s *KVStore) PutTrash(t *TrashEntry) error {
	data, err := msgpack.Marshal(t)
	if err != nil {
		return errors.Wrap(err, "kvfs: failed to marshal trash entry")
	}
	start := time.Now()
	_, err = s.trash.Put(trashKey(t.SpaceID, t.Key), data)
	KVOperationDuration.WithLabelValues("trash", "put").Observe(time.Since(start).Seconds())
	return errors.Wrap(err, "kvfs: failed to put trash entry")
}

// GetTrash retrieves a trash entry.
func (s *KVStore) GetTrash(spaceID, key string) (*TrashEntry, error) {
	start := time.Now()
	entry, err := s.trash.Get(trashKey(spaceID, key))
	KVOperationDuration.WithLabelValues("trash", "get").Observe(time.Since(start).Seconds())
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return nil, ErrNotFound
		}
		return nil, errors.Wrap(err, "kvfs: failed to get trash entry")
	}
	var t TrashEntry
	if err := msgpack.Unmarshal(entry.Value(), &t); err != nil {
		return nil, errors.Wrap(err, "kvfs: failed to unmarshal trash entry")
	}
	return &t, nil
}

// DeleteTrash removes a trash entry.
func (s *KVStore) DeleteTrash(spaceID, key string) error {
	start := time.Now()
	err := s.trash.Delete(trashKey(spaceID, key))
	KVOperationDuration.WithLabelValues("trash", "delete").Observe(time.Since(start).Seconds())
	if err == nats.ErrKeyNotFound {
		return ErrNotFound
	}
	return err
}

// ListTrash lists all trash entries for a space.
func (s *KVStore) ListTrash(spaceID string) ([]*TrashEntry, error) {
	return listByPrefix[TrashEntry](s.trash, "trash", spaceID+".")
}

// --- Version operations ---

// versionKey builds the KV key for a version entry.
func versionKey(spaceID, nodeID, versionKey string) string {
	return spaceID + "." + nodeID + "." + versionKey
}

// PutVersion stores a version entry.
func (s *KVStore) PutVersion(v *VersionEntry) error {
	data, err := msgpack.Marshal(v)
	if err != nil {
		return errors.Wrap(err, "kvfs: failed to marshal version")
	}
	start := time.Now()
	_, err = s.versions.Put(versionKey(v.SpaceID, v.NodeID, v.Key), data)
	KVOperationDuration.WithLabelValues("versions", "put").Observe(time.Since(start).Seconds())
	return errors.Wrap(err, "kvfs: failed to put version")
}

// ListVersions lists all versions for a node.
func (s *KVStore) ListVersions(spaceID, nodeID string) ([]*VersionEntry, error) {
	return listByPrefix[VersionEntry](s.versions, "versions", spaceID+"."+nodeID+".")
}

// GetVersion retrieves a specific version for a node.
func (s *KVStore) GetVersion(spaceID, nodeID, key string) (*VersionEntry, error) {
	start := time.Now()
	entry, err := s.versions.Get(versionKey(spaceID, nodeID, key))
	KVOperationDuration.WithLabelValues("versions", "get").Observe(time.Since(start).Seconds())
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return nil, ErrNotFound
		}
		return nil, errors.Wrap(err, "kvfs: failed to get version")
	}
	var v VersionEntry
	if err := msgpack.Unmarshal(entry.Value(), &v); err != nil {
		return nil, errors.Wrap(err, "kvfs: failed to unmarshal version")
	}
	return &v, nil
}

// DeleteVersion removes a version entry.
func (s *KVStore) DeleteVersion(spaceID, nodeID, key string) error {
	start := time.Now()
	err := s.versions.Delete(versionKey(spaceID, nodeID, key))
	KVOperationDuration.WithLabelValues("versions", "delete").Observe(time.Since(start).Seconds())
	if err != nil && err != nats.ErrKeyNotFound {
		return errors.Wrap(err, "kvfs: failed to delete version")
	}
	return nil
}

// ListVersionsBySpace lists all versions belonging to a space.
func (s *KVStore) ListVersionsBySpace(spaceID string) ([]*VersionEntry, error) {
	return listByPrefix[VersionEntry](s.versions, "versions", spaceID+".")
}

// --- Upload operations ---

// PutUpload stores an upload session.
func (s *KVStore) PutUpload(u *UploadSession) error {
	data, err := msgpack.Marshal(u)
	if err != nil {
		return errors.Wrap(err, "kvfs: failed to marshal upload session")
	}
	start := time.Now()
	_, err = s.uploads.Put(u.ID, data)
	KVOperationDuration.WithLabelValues("uploads", "put").Observe(time.Since(start).Seconds())
	return errors.Wrap(err, "kvfs: failed to put upload session")
}

// GetUpload retrieves an upload session.
func (s *KVStore) GetUpload(uploadID string) (*UploadSession, error) {
	start := time.Now()
	entry, err := s.uploads.Get(uploadID)
	KVOperationDuration.WithLabelValues("uploads", "get").Observe(time.Since(start).Seconds())
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return nil, ErrNotFound
		}
		return nil, errors.Wrap(err, "kvfs: failed to get upload session")
	}
	var u UploadSession
	if err := msgpack.Unmarshal(entry.Value(), &u); err != nil {
		return nil, errors.Wrap(err, "kvfs: failed to unmarshal upload session")
	}
	return &u, nil
}

// DeleteUpload removes an upload session.
func (s *KVStore) DeleteUpload(uploadID string) error {
	start := time.Now()
	err := s.uploads.Delete(uploadID)
	KVOperationDuration.WithLabelValues("uploads", "delete").Observe(time.Since(start).Seconds())
	if err != nil && err != nats.ErrKeyNotFound {
		return errors.Wrap(err, "kvfs: failed to delete upload")
	}
	return nil
}

// ListUploadsBySpace returns all upload sessions belonging to a space.
// Upload keys are plain UUIDs (not spaceID-prefixed), so this does a
// full scan with in-memory filter. Upload sessions are transient and
// few, so this is acceptable.
func (s *KVStore) ListUploadsBySpace(spaceID string) ([]*UploadSession, error) {
	all, err := listAll[UploadSession](s.uploads, "uploads")
	if err != nil {
		return nil, err
	}
	var result []*UploadSession
	for _, u := range all {
		if u.SpaceID == spaceID {
			result = append(result, u)
		}
	}
	return result, nil
}

// --- Bulk listing operations (used by GC) ---

// ListAllNodes returns all nodes across all spaces.
func (s *KVStore) ListAllNodes() ([]*NodeEntry, error) {
	return listAll[NodeEntry](s.nodes, "nodes")
}

// ListAllVersions returns all version entries across all spaces.
func (s *KVStore) ListAllVersions() ([]*VersionEntry, error) {
	return listAll[VersionEntry](s.versions, "versions")
}

// ListAllUploads returns all in-progress upload sessions.
func (s *KVStore) ListAllUploads() ([]*UploadSession, error) {
	return listAll[UploadSession](s.uploads, "uploads")
}

// UploadEntry is one live uploads-bucket entry as stored, with the
// server-side creation timestamp of its current revision. Session is nil
// when the stored bytes do not unmarshal — such entries are invisible to
// typed listings but still occupy the bucket, so GC needs to see them.
type UploadEntry struct {
	Key     string
	Created time.Time
	Session *UploadSession
}

// ListAllUploadEntries walks the uploads bucket once and returns every
// live entry with raw metadata. Used by GC so its reap policy can see
// entry age and corruption, both of which ListAllUploads hides.
func (s *KVStore) ListAllUploadEntries() ([]*UploadEntry, error) {
	watcher, err := s.uploads.WatchAll(nats.IgnoreDeletes())
	if err != nil {
		if err == nats.ErrNoKeysFound {
			return nil, nil
		}
		return nil, errors.Wrap(err, "kvfs: failed to watch all uploads")
	}
	defer watcher.Stop()

	var result []*UploadEntry
	for entry := range watcher.Updates() {
		if entry == nil {
			break
		}
		e := &UploadEntry{Key: entry.Key(), Created: entry.Created()}
		var u UploadSession
		if err := msgpack.Unmarshal(entry.Value(), &u); err == nil {
			e.Session = &u
		}
		result = append(result, e)
	}
	return result, nil
}

// PurgeDeletedUploads compacts accumulated delete markers (tombstones) in
// the uploads bucket. Every DeleteUpload leaves a marker that WatchAll-based
// listings must still stream and discard; sessions are by far the
// highest-churn bucket, so markers come to dominate its listing cost.
// Markers younger than the client default threshold keep their newest
// revision, which is safe for concurrent watchers.
func (s *KVStore) PurgeDeletedUploads() error {
	start := time.Now()
	err := s.uploads.PurgeDeletes()
	KVOperationDuration.WithLabelValues("uploads", "purge_deletes").Observe(time.Since(start).Seconds())
	return errors.Wrap(err, "kvfs: failed to purge upload tombstones")
}

// ListAllTrash returns all trash entries across all spaces.
func (s *KVStore) ListAllTrash() ([]*TrashEntry, error) {
	return listAll[TrashEntry](s.trash, "trash")
}

// --- Error types ---

var (
	// ErrNodeNotFound indicates the requested node does not exist in the KV store.
	ErrNodeNotFound = fmt.Errorf("kvfs: node not found")
	// ErrSpaceNotFound indicates the requested space does not exist.
	ErrSpaceNotFound = fmt.Errorf("kvfs: space not found")
	// ErrNotFound is a generic not-found error.
	ErrNotFound = fmt.Errorf("kvfs: not found")
	// ErrCASConflict indicates a Compare-And-Swap conflict (concurrent modification).
	ErrCASConflict = fmt.Errorf("kvfs: CAS conflict — concurrent modification detected")
)
