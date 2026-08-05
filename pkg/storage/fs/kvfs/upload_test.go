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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	user "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/opencloud-eu/reva/v2/pkg/storage"
	"github.com/rs/zerolog"

	ctxpkg "github.com/opencloud-eu/reva/v2/pkg/ctx"
	tusd "github.com/tus/tusd/v2/pkg/handler"
)

// --- Mock BlobStore ---

type mockBlobStore struct {
	mu             sync.Mutex
	blobs          map[string][]byte
	blobTimes      map[string]time.Time      // key -> lastModified
	multiparts     map[string]map[int][]byte // uploadID -> partNum -> data
	nextUploadID   string
	uploadPartErr  error
	completeErr    error
	initErr        error
	abortCalled    []string
	completeCalled []string
	deleteCalled   []string

	// deleteHook, if set, is invoked (outside the mutex) after each successful
	// Delete. Lease-fencing tests use it to cancel the sweep mid-delete.
	deleteHook func(key string)
}

func newMockBlobStore() *mockBlobStore {
	return &mockBlobStore{
		blobs:      make(map[string][]byte),
		multiparts: make(map[string]map[int][]byte),
	}
}

func (m *mockBlobStore) Upload(ctx context.Context, key string, reader io.Reader, size int64) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blobs[key] = data
	return nil
}

func (m *mockBlobStore) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.blobs[key]
	if !ok {
		return nil, ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *mockBlobStore) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	delete(m.blobs, key)
	m.deleteCalled = append(m.deleteCalled, key)
	hook := m.deleteHook
	m.mu.Unlock()
	if hook != nil {
		hook(key)
	}
	return nil
}

func (m *mockBlobStore) InitMultipartUpload(ctx context.Context, key string) (string, error) {
	if m.initErr != nil {
		return "", m.initErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	id := m.nextUploadID
	if id == "" {
		id = "mp-upload-1"
	}
	m.multiparts[id] = make(map[int][]byte)
	return id, nil
}

func (m *mockBlobStore) UploadPart(ctx context.Context, key, uploadID string, partNumber int, reader io.Reader, size int64) (PartInfo, error) {
	if m.uploadPartErr != nil {
		return PartInfo{}, m.uploadPartErr
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return PartInfo{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if parts, ok := m.multiparts[uploadID]; ok {
		parts[partNumber] = data
	}
	return PartInfo{
		PartNumber: partNumber,
		ETag:       fmt.Sprintf("etag-part-%d", partNumber),
		Size:       int64(len(data)),
	}, nil
}

func (m *mockBlobStore) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []PartInfo) error {
	if m.completeErr != nil {
		return m.completeErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.completeCalled = append(m.completeCalled, uploadID)
	// Assemble final blob from parts
	mpParts := m.multiparts[uploadID]
	if mpParts != nil {
		var assembled []byte
		for i := 1; i <= len(parts); i++ {
			assembled = append(assembled, mpParts[i]...)
		}
		m.blobs[key] = assembled
	}
	delete(m.multiparts, uploadID)
	return nil
}

func (m *mockBlobStore) AbortMultipartUpload(ctx context.Context, key, uploadID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.abortCalled = append(m.abortCalled, uploadID)
	delete(m.multiparts, uploadID)
	return nil
}

func (m *mockBlobStore) ListBlobs(ctx context.Context, prefix string) ([]BlobInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []BlobInfo
	for key := range m.blobs {
		if prefix == "" || strings.HasPrefix(key, prefix) {
			bi := BlobInfo{Key: key, Size: int64(len(m.blobs[key]))}
			if m.blobTimes != nil {
				bi.LastModified = m.blobTimes[key]
			}
			result = append(result, bi)
		}
	}
	return result, nil
}

// ListSpacePrefixes returns the distinct top-level "<spaceID>" segments of all
// stored blob keys, mirroring the delimiter-list semantics of the S3 backend.
func (m *mockBlobStore) ListSpacePrefixes(ctx context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := map[string]struct{}{}
	var prefixes []string
	for key := range m.blobs {
		if i := strings.IndexByte(key, '/'); i > 0 {
			id := key[:i]
			if _, ok := seen[id]; !ok {
				seen[id] = struct{}{}
				prefixes = append(prefixes, id)
			}
		}
	}
	return prefixes, nil
}

// --- Mock MetadataStore ---

type mockMetadataStore struct {
	mu                sync.Mutex
	nodes             map[string]*NodeEntry // "spaceID.nodeID" -> node
	nodeRevs          map[string]uint64
	children          map[string]ChildMap // "spaceID.parentID" -> children
	childRevs         map[string]uint64
	spaces            map[string]*SpaceEntry
	spaceRevs         map[string]uint64
	uploads           map[string]*UploadSession
	versions          []*VersionEntry
	trash             map[string]*TrashEntry
	locks             map[string]*kvLockEntry
	nextRev           uint64
	putNodeErr        error
	putChildrenErr    error
	casFailsLeft      int
	childCASFailsLeft int

	// Lease-renewal test hooks. renewLostFor makes RenewLock report the
	// lease definitively lost for a holder (simulating a concurrent steal);
	// renewErrFor makes it return a transient error (ownership unknown).
	renewLostFor map[string]bool
	renewErrFor  map[string]bool

	// uploadCreated optionally overrides the KV created-timestamp per
	// session ID for ListAllUploadEntries; unset IDs default to now so
	// existing tests never trip the age-gated reap paths.
	uploadCreated map[string]time.Time
	// corruptUploads simulates entries whose value no longer unmarshals:
	// key -> created timestamp. Visible only via ListAllUploadEntries.
	corruptUploads           map[string]time.Time
	purgeDeletedUploadsCalls int
}

func newMockMetadataStore() *mockMetadataStore {
	return &mockMetadataStore{
		nodes:     make(map[string]*NodeEntry),
		nodeRevs:  make(map[string]uint64),
		children:  make(map[string]ChildMap),
		childRevs: make(map[string]uint64),
		spaces:    make(map[string]*SpaceEntry),
		spaceRevs: make(map[string]uint64),
		uploads:   make(map[string]*UploadSession),
		trash:     make(map[string]*TrashEntry),
		locks:     make(map[string]*kvLockEntry),
		nextRev:   1,

		uploadCreated:  make(map[string]time.Time),
		corruptUploads: make(map[string]time.Time),
	}
}

func (m *mockMetadataStore) Close() {}

func (m *mockMetadataStore) allocRev() uint64 {
	m.nextRev++
	return m.nextRev
}

func (m *mockMetadataStore) GetNode(spaceID, nodeID string) (*NodeEntry, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := spaceID + "." + nodeID
	n, ok := m.nodes[key]
	if !ok {
		return nil, 0, ErrNodeNotFound
	}
	cp := *n
	return &cp, m.nodeRevs[key], nil
}

func (m *mockMetadataStore) PutNode(n *NodeEntry, expectedRev uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.casFailsLeft > 0 {
		m.casFailsLeft--
		return ErrCASConflict
	}
	if m.putNodeErr != nil {
		return m.putNodeErr
	}
	key := n.SpaceID + "." + n.ID
	if expectedRev == 0 {
		if _, exists := m.nodes[key]; exists {
			return ErrCASConflict
		}
	} else {
		if m.nodeRevs[key] != expectedRev {
			return ErrCASConflict
		}
	}
	cp := *n
	m.nodes[key] = &cp
	m.nodeRevs[key] = m.allocRev()
	return nil
}

func (m *mockMetadataStore) DeleteNode(spaceID, nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := spaceID + "." + nodeID
	delete(m.nodes, key)
	delete(m.nodeRevs, key)
	return nil
}

func (m *mockMetadataStore) GetChildren(spaceID, parentID string) (ChildMap, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := spaceID + "." + parentID
	cm, ok := m.children[key]
	if !ok {
		return ChildMap{}, 0, nil
	}
	cp := make(ChildMap)
	for k, v := range cm {
		cp[k] = v
	}
	return cp, m.childRevs[key], nil
}

func (m *mockMetadataStore) PutChildren(spaceID, parentID string, children ChildMap, expectedRev uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.childCASFailsLeft > 0 && expectedRev > 0 {
		m.childCASFailsLeft--
		return ErrCASConflict
	}
	if m.putChildrenErr != nil {
		return m.putChildrenErr
	}
	key := spaceID + "." + parentID
	if expectedRev == 0 {
		// Match real NATS KV: Create(rev=0) fails if the key already exists.
		// Required for the personal-space CAS dedup test to fail
		// realistically when two goroutines race the same deterministic ID.
		if _, exists := m.children[key]; exists {
			return ErrCASConflict
		}
	} else if m.childRevs[key] != expectedRev {
		return ErrCASConflict
	}
	cp := make(ChildMap)
	for k, v := range children {
		cp[k] = v
	}
	m.children[key] = cp
	m.childRevs[key] = m.allocRev()
	return nil
}

func (m *mockMetadataStore) DeleteChildren(spaceID, parentID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := spaceID + "." + parentID
	delete(m.children, key)
	delete(m.childRevs, key)
	return nil
}

func (m *mockMetadataStore) GetSpace(spaceID string) (*SpaceEntry, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.spaces[spaceID]
	if !ok {
		return nil, 0, ErrSpaceNotFound
	}
	cp := *s
	return &cp, m.spaceRevs[spaceID], nil
}

func (m *mockMetadataStore) PutSpace(space *SpaceEntry, expectedRev uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if expectedRev == 0 {
		// Match real NATS KV: Create(rev=0) fails if the key already exists.
		// Required for the personal-space CAS dedup test to fail
		// realistically when two goroutines race the same deterministic ID.
		if _, exists := m.spaces[space.ID]; exists {
			return ErrCASConflict
		}
	} else if m.spaceRevs[space.ID] != expectedRev {
		return ErrCASConflict
	}
	cp := *space
	m.spaces[space.ID] = &cp
	m.spaceRevs[space.ID] = m.allocRev()
	return nil
}

func (m *mockMetadataStore) DeleteSpace(spaceID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.spaces, spaceID)
	delete(m.spaceRevs, spaceID)
	return nil
}

func (m *mockMetadataStore) ListSpaces(filter func(*SpaceEntry) bool) ([]*SpaceEntry, error) {
	m.mu.Lock()
	// Copy spaces under lock, then release before calling filter
	// to avoid deadlock if the filter callback calls back into the store.
	all := make([]*SpaceEntry, 0, len(m.spaces))
	for _, s := range m.spaces {
		all = append(all, s)
	}
	m.mu.Unlock()
	var result []*SpaceEntry
	for _, s := range all {
		if filter == nil || filter(s) {
			result = append(result, s)
		}
	}
	return result, nil
}

func (m *mockMetadataStore) PutTrash(t *TrashEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *t
	m.trash[t.SpaceID+"."+t.Key] = &cp
	return nil
}

func (m *mockMetadataStore) GetTrash(spaceID, key string) (*TrashEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.trash[spaceID+"."+key]
	if !ok {
		return nil, ErrNotFound
	}
	return t, nil
}

func (m *mockMetadataStore) DeleteTrash(spaceID, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.trash, spaceID+"."+key)
	return nil
}

func (m *mockMetadataStore) ListTrash(spaceID string) ([]*TrashEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*TrashEntry
	for _, t := range m.trash {
		if t.SpaceID == spaceID {
			result = append(result, t)
		}
	}
	return result, nil
}

func (m *mockMetadataStore) PutVersion(v *VersionEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.versions = append(m.versions, v)
	return nil
}

func (m *mockMetadataStore) ListVersions(spaceID, nodeID string) ([]*VersionEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*VersionEntry
	for _, v := range m.versions {
		if v.SpaceID == spaceID && v.NodeID == nodeID {
			result = append(result, v)
		}
	}
	return result, nil
}

func (m *mockMetadataStore) GetVersion(spaceID, nodeID, key string) (*VersionEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, v := range m.versions {
		if v.SpaceID == spaceID && v.NodeID == nodeID && v.Key == key {
			return v, nil
		}
	}
	return nil, ErrNotFound
}

func (m *mockMetadataStore) DeleteVersion(spaceID, nodeID, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, v := range m.versions {
		if v.SpaceID == spaceID && v.NodeID == nodeID && v.Key == key {
			m.versions = append(m.versions[:i], m.versions[i+1:]...)
			return nil
		}
	}
	return nil
}

func (m *mockMetadataStore) PutUpload(u *UploadSession) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *u
	m.uploads[u.ID] = &cp
	return nil
}

func (m *mockMetadataStore) GetUpload(uploadID string) (*UploadSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	u, ok := m.uploads[uploadID]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *u
	return &cp, nil
}

func (m *mockMetadataStore) DeleteUpload(uploadID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.uploads, uploadID)
	delete(m.uploadCreated, uploadID)
	delete(m.corruptUploads, uploadID)
	return nil
}

func (m *mockMetadataStore) ListAllNodes() ([]*NodeEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*NodeEntry
	for _, n := range m.nodes {
		cp := *n
		result = append(result, &cp)
	}
	return result, nil
}

func (m *mockMetadataStore) ListNodesBySpace(spaceID string) ([]*NodeEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*NodeEntry
	for key, n := range m.nodes {
		if strings.HasPrefix(key, spaceID+".") {
			cp := *n
			result = append(result, &cp)
		}
	}
	return result, nil
}

func (m *mockMetadataStore) GetNodes(spaceID string, nodeIDs []string) (map[string]*NodeEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	nodes := make(map[string]*NodeEntry, len(nodeIDs))
	for _, id := range nodeIDs {
		key := spaceID + "." + id
		if n, ok := m.nodes[key]; ok {
			cp := *n
			nodes[id] = &cp
		}
	}
	return nodes, nil
}

func (m *mockMetadataStore) ListAllVersions() ([]*VersionEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*VersionEntry
	for _, v := range m.versions {
		cp := *v
		result = append(result, &cp)
	}
	return result, nil
}

func (m *mockMetadataStore) ListVersionsBySpace(spaceID string) ([]*VersionEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*VersionEntry
	for _, v := range m.versions {
		if v.SpaceID == spaceID {
			cp := *v
			result = append(result, &cp)
		}
	}
	return result, nil
}

func (m *mockMetadataStore) ListAllUploads() ([]*UploadSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*UploadSession
	for _, u := range m.uploads {
		cp := *u
		result = append(result, &cp)
	}
	return result, nil
}

func (m *mockMetadataStore) ListAllUploadEntries() ([]*UploadEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*UploadEntry
	for id, u := range m.uploads {
		cp := *u
		created, ok := m.uploadCreated[id]
		if !ok {
			created = time.Now()
		}
		result = append(result, &UploadEntry{Key: id, Created: created, Session: &cp})
	}
	for key, created := range m.corruptUploads {
		result = append(result, &UploadEntry{Key: key, Created: created})
	}
	return result, nil
}

func (m *mockMetadataStore) PurgeDeletedUploads() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.purgeDeletedUploadsCalls++
	return nil
}

func (m *mockMetadataStore) ListUploadsBySpace(spaceID string) ([]*UploadSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*UploadSession
	for _, u := range m.uploads {
		if u.SpaceID == spaceID {
			cp := *u
			result = append(result, &cp)
		}
	}
	return result, nil
}

func (m *mockMetadataStore) ListAllTrash() ([]*TrashEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*TrashEntry
	for _, t := range m.trash {
		cp := *t
		result = append(result, &cp)
	}
	return result, nil
}

// injectCASFailures makes the next n PutNode calls return ErrCASConflict.
func (m *mockMetadataStore) injectCASFailures(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.casFailsLeft = n
}

// injectChildCASFailures makes the next n PutChildren calls return ErrCASConflict.
func (m *mockMetadataStore) injectChildCASFailures(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.childCASFailsLeft = n
}

func (m *mockMetadataStore) TryAcquireLock(key string, holder string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, ok := m.locks[key]; ok {
		if time.Now().UnixNano() <= existing.ExpiresAt {
			return false, nil
		}
	}

	m.locks[key] = &kvLockEntry{
		Holder:    holder,
		ExpiresAt: time.Now().Add(ttl).UnixNano(),
	}
	return true, nil
}

func (m *mockMetadataStore) RenewLock(key string, holder string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.renewErrFor[holder] {
		return false, fmt.Errorf("mock: transient renew error for %s", holder)
	}
	existing, ok := m.locks[key]
	if !ok {
		return false, nil // lock gone — lost
	}
	if existing.Holder != holder || m.renewLostFor[holder] {
		return false, nil // stolen / not ours — lost
	}
	existing.ExpiresAt = time.Now().Add(ttl).UnixNano()
	return true, nil
}

func (m *mockMetadataStore) ReleaseLock(key string, holder string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, ok := m.locks[key]; ok && existing.Holder == holder {
		delete(m.locks, key)
	}
	return nil
}

// --- lease-test helpers ---

// setLock pre-inserts a lock entry held by holder for ttl from now.
func (m *mockMetadataStore) setLock(key, holder string, ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.locks[key] = &kvLockEntry{Holder: holder, ExpiresAt: time.Now().Add(ttl).UnixNano()}
}

// expireLock back-dates a lock's expiry so a successor can steal it, simulating
// a holder that stopped renewing (died mid-sweep).
func (m *mockMetadataStore) expireLock(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.locks[key]; ok {
		e.ExpiresAt = time.Now().Add(-time.Second).UnixNano()
	}
}

// failRenewFor makes RenewLock report the lease definitively lost for holder.
func (m *mockMetadataStore) failRenewFor(holder string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.renewLostFor == nil {
		m.renewLostFor = map[string]bool{}
	}
	m.renewLostFor[holder] = true
}

// failRenewErrFor makes RenewLock return a transient error for holder.
func (m *mockMetadataStore) failRenewErrFor(holder string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.renewErrFor == nil {
		m.renewErrFor = map[string]bool{}
	}
	m.renewErrFor[holder] = true
}

// --- Test helpers ---

func testContext() context.Context {
	return ctxpkg.ContextSetUser(context.Background(), &user.User{
		Id: &user.UserId{OpaqueId: "test-user-id"},
	})
}

func testDriver(store *mockMetadataStore, blob *mockBlobStore) *kvfsDriver {
	log := zerolog.Nop()
	// uploadCache is required for any WriteChunk / FinishUpload test.
	// Use a per-process temp dir so concurrent tests don't collide.
	tmpDir, err := os.MkdirTemp("", "kvfs-test-*")
	if err != nil {
		panic(err)
	}
	return &kvfsDriver{
		store:       store,
		blob:        blob,
		opts:        &Options{},
		log:         &log,
		uploadCache: newDiskUploadCache(tmpDir),
	}
}

func setupSpaceWithFile(store *mockMetadataStore, spaceID, parentID, nodeID, filename string) {
	store.spaces[spaceID] = &SpaceEntry{
		ID: spaceID, Type: "personal", Owner: "test-user-id",
		Name: "Test", RootID: parentID, Quota: -1,
	}
	store.spaceRevs[spaceID] = 1
	store.nodes[spaceID+"."+parentID] = &NodeEntry{
		ID: parentID, SpaceID: spaceID, Name: "root", Type: NodeTypeDir,
		Owner: "test-user-id", MimeType: "httpd/unix-directory",
	}
	store.nodeRevs[spaceID+"."+parentID] = 1
	store.children[spaceID+"."+parentID] = ChildMap{filename: nodeID}
	store.childRevs[spaceID+"."+parentID] = 1
	store.nodes[spaceID+"."+nodeID] = &NodeEntry{
		ID: nodeID, SpaceID: spaceID, ParentID: parentID,
		Name: filename, Type: NodeTypeFile, BlobID: "old-blob",
		BlobSize: 100, Size: 100, MTime: 1000, ETag: "old-etag",
		Owner: "test-user-id", MimeType: "text/plain",
	}
	store.nodeRevs[spaceID+"."+nodeID] = 1
}

func setupSpaceEmpty(store *mockMetadataStore, spaceID, parentID string) {
	store.spaces[spaceID] = &SpaceEntry{
		ID: spaceID, Type: "personal", Owner: "test-user-id",
		Name: "Test", RootID: parentID, Quota: -1,
	}
	store.spaceRevs[spaceID] = 1
	store.nodes[spaceID+"."+parentID] = &NodeEntry{
		ID: parentID, SpaceID: spaceID, Name: "root", Type: NodeTypeDir,
		Owner: "test-user-id", MimeType: "httpd/unix-directory",
	}
	store.nodeRevs[spaceID+"."+parentID] = 1
	store.children[spaceID+"."+parentID] = ChildMap{}
	store.childRevs[spaceID+"."+parentID] = 1
}

func makeSession(spaceID, parentID, filename string) *UploadSession {
	return &UploadSession{
		ID:            "sess-1",
		SpaceID:       spaceID,
		Filename:      filename,
		ParentID:      parentID,
		Size:          1024,
		Offset:        0,
		Storage:       map[string]string{"key": "val"},
		BlobID:        "blob-123",
		S3MultipartID: "mp-upload-1",
		OwnerID:       "test-user-id",
	}
}

// --- Tests ---

func TestGetInfo(t *testing.T) {
	session := makeSession("space-1", "parent-1", "test.txt")
	session.Size = 2048
	session.SizeIsDeferred = true
	session.Offset = 512
	session.Storage = map[string]string{"foo": "bar"}

	u := &kvfsUpload{session: session}
	info, err := u.GetInfo(context.Background())
	if err != nil {
		t.Fatalf("GetInfo: %v", err)
	}

	if info.ID != "sess-1" {
		t.Errorf("ID = %q, want %q", info.ID, "sess-1")
	}
	if info.Size != 2048 {
		t.Errorf("Size = %d, want 2048", info.Size)
	}
	if !info.SizeIsDeferred {
		t.Error("SizeIsDeferred = false, want true")
	}
	if info.Offset != 512 {
		t.Errorf("Offset = %d, want 512", info.Offset)
	}
	if info.MetaData["foo"] != "bar" {
		t.Errorf("MetaData[foo] = %q, want %q", info.MetaData["foo"], "bar")
	}
	if info.Storage["SpaceRoot"] != "space-1" {
		t.Errorf("Storage[SpaceRoot] = %q, want %q", info.Storage["SpaceRoot"], "space-1")
	}
	if info.Storage["BlobId"] != "blob-123" {
		t.Errorf("Storage[BlobId] = %q, want %q", info.Storage["BlobId"], "blob-123")
	}
	if info.Storage["Filename"] != "test.txt" {
		t.Errorf("Storage[Filename] = %q, want %q", info.Storage["Filename"], "test.txt")
	}
	if info.Storage["ParentId"] != "parent-1" {
		t.Errorf("Storage[ParentId] = %q, want %q", info.Storage["ParentId"], "parent-1")
	}
}

func TestWriteChunk(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	session := makeSession("space-1", "parent-1", "test.txt")
	session.Size = 1024
	session.S3MultipartID = "" // disk-staged path: no multipart pre-init
	store.uploads[session.ID] = session

	u := &kvfsUpload{session: session, driver: d}

	data := []byte("hello world chunk")
	n, err := u.WriteChunk(context.Background(), 0, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	if n != int64(len(data)) {
		t.Errorf("WriteChunk returned %d, want %d", n, len(data))
	}
	if session.Offset != int64(len(data)) {
		t.Errorf("session.Offset = %d, want %d", session.Offset, len(data))
	}
	// Bytes should land in the upload cache, NOT a multipart Parts slice.
	cacheSize, err := d.uploadCache.Size(session.ID)
	if err != nil {
		t.Fatalf("uploadCache.Size: %v", err)
	}
	if cacheSize != int64(len(data)) {
		t.Errorf("uploadCache.Size = %d, want %d", cacheSize, len(data))
	}
	// WriteChunk no longer persists session per chunk; PutUpload happens
	// only at FinishUpload (then DeleteUpload). Offset is recoverable
	// from cache.Size() in GetUpload.
}

// WriteChunk works regardless of S3MultipartID because the disk-staging
// path writes to the cache, not to S3 multipart. (The old test asserted
// an error when MultipartID was empty — that error path no longer exists.)
func TestWriteChunk_NoMultipartID(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	session := makeSession("space-1", "parent-1", "test.txt")
	session.Size = 4
	session.S3MultipartID = "" // disk-staged path: this is the normal case
	store.uploads[session.ID] = session

	u := &kvfsUpload{session: session, driver: d}
	n, err := u.WriteChunk(context.Background(), 0, strings.NewReader("data"))
	if err != nil {
		t.Fatalf("WriteChunk should succeed without MultipartID: %v", err)
	}
	if n != 4 {
		t.Errorf("WriteChunk wrote %d bytes, want 4", n)
	}
}

// Chunks accumulate in the upload cache as a single growing file, not as
// a Parts slice. Offset advances by total bytes written; the cache file
// is the size of the accumulated data.
func TestWriteChunkMultipleChunks(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	session := makeSession("space-1", "parent-1", "test.txt")
	session.Size = 100
	session.S3MultipartID = "" // disk-staged path
	store.uploads[session.ID] = session

	u := &kvfsUpload{session: session, driver: d}

	chunk := []byte("chunk-data") // 10 bytes
	for i := 0; i < 3; i++ {
		_, err := u.WriteChunk(context.Background(), session.Offset, bytes.NewReader(chunk))
		if err != nil {
			t.Fatalf("WriteChunk %d: %v", i, err)
		}
	}

	if session.Offset != 30 {
		t.Errorf("session.Offset = %d, want 30", session.Offset)
	}
	cacheSize, err := d.uploadCache.Size(session.ID)
	if err != nil {
		t.Fatalf("uploadCache.Size: %v", err)
	}
	if cacheSize != 30 {
		t.Errorf("uploadCache.Size = %d, want 30", cacheSize)
	}
}

func TestGetReaderIncomplete(t *testing.T) {
	session := makeSession("space-1", "parent-1", "test.txt")
	session.Size = 1024
	session.Offset = 512

	u := &kvfsUpload{session: session}
	_, err := u.GetReader(context.Background())
	if !errors.Is(err, tusd.ErrNotFound) {
		t.Errorf("GetReader on incomplete upload: got %v, want tusd.ErrNotFound", err)
	}
}

func TestGetReaderComplete(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	blobKey := BlobKey("space-1", "blob-123")
	blob.blobs[blobKey] = []byte("file content")

	session := makeSession("space-1", "parent-1", "test.txt")
	session.Size = 12
	session.Offset = 12

	u := &kvfsUpload{session: session, driver: d}
	reader, err := u.GetReader(context.Background())
	if err != nil {
		t.Fatalf("GetReader: %v", err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(data) != "file content" {
		t.Errorf("GetReader content = %q, want %q", string(data), "file content")
	}
}

func TestGetReaderDeferredLength(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	blobKey := BlobKey("space-1", "blob-123")
	blob.blobs[blobKey] = []byte("deferred content")

	session := makeSession("space-1", "parent-1", "test.txt")
	session.Size = 0
	session.Offset = 0
	session.SizeIsDeferred = true

	u := &kvfsUpload{session: session, driver: d}
	reader, err := u.GetReader(context.Background())
	if err != nil {
		t.Fatalf("GetReader with deferred size: %v", err)
	}
	defer reader.Close()
	data, _ := io.ReadAll(reader)
	if string(data) != "deferred content" {
		t.Errorf("content = %q, want %q", string(data), "deferred content")
	}
}

func TestDeclareLength(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	session := makeSession("space-1", "parent-1", "test.txt")
	session.SizeIsDeferred = true
	session.Size = 0
	store.uploads[session.ID] = session

	u := &kvfsUpload{session: session, driver: d}
	err := u.DeclareLength(context.Background(), 4096)
	if err != nil {
		t.Fatalf("DeclareLength: %v", err)
	}
	if session.Size != 4096 {
		t.Errorf("session.Size = %d, want 4096", session.Size)
	}
	if session.SizeIsDeferred {
		t.Error("session.SizeIsDeferred should be false after DeclareLength")
	}

	persisted, _ := store.GetUpload(session.ID)
	if persisted.Size != 4096 {
		t.Errorf("persisted.Size = %d, want 4096", persisted.Size)
	}
}

func TestTerminate(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	session := makeSession("space-1", "parent-1", "test.txt")
	store.uploads[session.ID] = session

	// Reset gauge so Dec() doesn't go negative in other tests
	UploadInFlight.WithLabelValues("tus").Add(1)

	u := &kvfsUpload{session: session, driver: d}
	err := u.Terminate(context.Background())
	if err != nil {
		t.Fatalf("Terminate: %v", err)
	}

	if len(blob.abortCalled) != 1 || blob.abortCalled[0] != "mp-upload-1" {
		t.Errorf("AbortMultipartUpload not called correctly: %v", blob.abortCalled)
	}

	_, err = store.GetUpload(session.ID)
	if err != ErrNotFound {
		t.Errorf("upload session should be deleted after Terminate, got err: %v", err)
	}
}

func TestTerminateNoMultipartID(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	session := makeSession("space-1", "parent-1", "test.txt")
	session.S3MultipartID = ""
	store.uploads[session.ID] = session
	UploadInFlight.WithLabelValues("tus").Add(1)

	u := &kvfsUpload{session: session, driver: d}
	err := u.Terminate(context.Background())
	if err != nil {
		t.Fatalf("Terminate: %v", err)
	}
	if len(blob.abortCalled) != 0 {
		t.Error("AbortMultipartUpload should not be called when S3MultipartID is empty")
	}
}

func TestFinishUploadNewFile(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	ctx := testContext()

	setupSpaceEmpty(store, "space-1", "parent-1")

	session := makeSession("space-1", "parent-1", "newfile.txt")
	session.Offset = 1024
	session.Parts = []PartInfo{{PartNumber: 1, ETag: "etag1", Size: 1024}}
	store.uploads[session.ID] = session
	UploadInFlight.WithLabelValues("tus").Add(1)

	u := &kvfsUpload{session: session, driver: d}
	err := u.FinishUpload(ctx)
	if err != nil {
		t.Fatalf("FinishUpload: %v", err)
	}

	// Verify multipart was completed
	if len(blob.completeCalled) != 1 {
		t.Error("CompleteMultipartUpload not called")
	}

	// Verify upload session was cleaned up
	_, err = store.GetUpload(session.ID)
	if err != ErrNotFound {
		t.Error("upload session should be deleted after FinishUpload")
	}

	// Verify new node was created in children
	children, _, _ := store.GetChildren("space-1", "parent-1")
	nodeID, exists := children["newfile.txt"]
	if !exists {
		t.Fatal("newfile.txt not added to parent children")
	}

	// Verify node metadata
	node, _, err := store.GetNode("space-1", nodeID)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if node.BlobID != "blob-123" {
		t.Errorf("node.BlobID = %q, want %q", node.BlobID, "blob-123")
	}
	if node.Size != 1024 {
		t.Errorf("node.Size = %d, want 1024", node.Size)
	}
	if node.Type != NodeTypeFile {
		t.Errorf("node.Type = %d, want NodeTypeFile", node.Type)
	}
	if node.Owner != "test-user-id" {
		t.Errorf("node.Owner = %q, want %q", node.Owner, "test-user-id")
	}
}

func TestFinishUploadOverwriteExisting(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	ctx := testContext()

	setupSpaceWithFile(store, "space-1", "parent-1", "existing-node", "test.txt")

	session := makeSession("space-1", "parent-1", "test.txt")
	session.Offset = 2048
	session.Parts = []PartInfo{{PartNumber: 1, ETag: "etag1", Size: 2048}}
	store.uploads[session.ID] = session
	UploadInFlight.WithLabelValues("tus").Add(1)

	u := &kvfsUpload{session: session, driver: d}
	err := u.FinishUpload(ctx)
	if err != nil {
		t.Fatalf("FinishUpload: %v", err)
	}

	// Verify existing node was updated
	node, _, _ := store.GetNode("space-1", "existing-node")
	if node.BlobID != "blob-123" {
		t.Errorf("node.BlobID = %q, want %q", node.BlobID, "blob-123")
	}
	if node.Size != 2048 {
		t.Errorf("node.Size = %d, want 2048", node.Size)
	}

	// Verify a version was created for the old content
	versions, _ := store.ListVersions("space-1", "existing-node")
	if len(versions) != 1 {
		t.Fatalf("expected 1 version, got %d", len(versions))
	}
	if versions[0].BlobID != "old-blob" {
		t.Errorf("version.BlobID = %q, want %q", versions[0].BlobID, "old-blob")
	}
	if versions[0].Size != 100 {
		t.Errorf("version.Size = %d, want 100", versions[0].Size)
	}
}

func TestFinishUploadOverwriteVersioningDisabled(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	d.opts.DisableVersioning = true
	ctx := testContext()

	setupSpaceWithFile(store, "space-1", "parent-1", "existing-node", "test.txt")

	session := makeSession("space-1", "parent-1", "test.txt")
	session.Offset = 2048
	session.Parts = []PartInfo{{PartNumber: 1, ETag: "etag1", Size: 2048}}
	store.uploads[session.ID] = session
	UploadInFlight.WithLabelValues("tus").Add(1)

	u := &kvfsUpload{session: session, driver: d}
	err := u.FinishUpload(ctx)
	if err != nil {
		t.Fatalf("FinishUpload: %v", err)
	}

	versions, _ := store.ListVersions("space-1", "existing-node")
	if len(versions) != 0 {
		t.Errorf("expected no versions with versioning disabled, got %d", len(versions))
	}
}

func TestFinishUploadIfMatchEtagMismatch(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	ctx := testContext()

	setupSpaceWithFile(store, "space-1", "parent-1", "existing-node", "test.txt")

	session := makeSession("space-1", "parent-1", "test.txt")
	session.IfMatchEtag = "wrong-etag"
	session.Offset = 1024
	session.Parts = []PartInfo{{PartNumber: 1, ETag: "etag1", Size: 1024}}
	store.uploads[session.ID] = session
	UploadInFlight.WithLabelValues("tus").Add(1)

	u := &kvfsUpload{session: session, driver: d}
	err := u.FinishUpload(ctx)
	if err == nil {
		t.Fatal("FinishUpload should fail on if-match etag mismatch")
	}
	if !strings.Contains(err.Error(), "etag mismatch") {
		t.Errorf("unexpected error: %v", err)
	}

	// Blob should be cleaned up on commit failure
	if len(blob.deleteCalled) != 1 {
		t.Errorf("expected blob Delete to be called on commit failure, got %d calls", len(blob.deleteCalled))
	}
}

// TestFinishUploadQuotaReturns507 verifies that when quota is exhausted,
// FinishUpload wraps the error as a tusd.Error with HTTP 507 (not 500).
func TestFinishUploadQuotaReturns507(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	ctx := testContext()

	// Space with quota=1 byte, root node already at size=1 → full.
	setupSpaceEmpty(store, "space-1", "parent-1")
	store.spaces["space-1"].Quota = 1
	store.nodes["space-1.parent-1"].Size = 1

	session := &UploadSession{
		ID:       "sess-quota",
		SpaceID:  "space-1",
		Filename: "bigfile.txt",
		ParentID: "parent-1",
		Size:     1024,
		Offset:   0,
		BlobID:   "blob-quota",
		OwnerID:  "test-user-id",
	}
	store.uploads[session.ID] = session
	UploadInFlight.WithLabelValues("tus").Add(1)

	u := &kvfsUpload{session: session, driver: d}

	// Stage some content (modern path — no S3MultipartID).
	data := []byte("this exceeds quota")
	n, err := u.WriteChunk(ctx, 0, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	session.Offset = n

	err = u.FinishUpload(ctx)
	if err == nil {
		t.Fatal("FinishUpload should fail when quota is exceeded")
	}

	// The error must be a tusd.Error with status 507.
	var tusErr tusd.Error
	if !errors.As(err, &tusErr) {
		t.Fatalf("expected tusd.Error, got %T: %v", err, err)
	}
	if tusErr.HTTPResponse.StatusCode != 507 {
		t.Errorf("status code = %d, want 507", tusErr.HTTPResponse.StatusCode)
	}
	if tusErr.ErrorCode != "ERR_QUOTA_EXCEEDED" {
		t.Errorf("error code = %q, want %q", tusErr.ErrorCode, "ERR_QUOTA_EXCEEDED")
	}

	// Blob should be cleaned up.
	if len(blob.deleteCalled) != 1 {
		t.Errorf("expected blob Delete on quota failure, got %d calls", len(blob.deleteCalled))
	}
}

func TestFinishUploadCompleteMultipartError(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	blob.completeErr = io.ErrUnexpectedEOF
	d := testDriver(store, blob)
	ctx := testContext()

	setupSpaceEmpty(store, "space-1", "parent-1")

	session := makeSession("space-1", "parent-1", "test.txt")
	session.Offset = 1024
	session.Parts = []PartInfo{{PartNumber: 1, ETag: "etag1", Size: 1024}}
	store.uploads[session.ID] = session
	UploadInFlight.WithLabelValues("tus").Add(1)

	u := &kvfsUpload{session: session, driver: d}
	err := u.FinishUpload(ctx)
	if err == nil {
		t.Fatal("FinishUpload should fail when CompleteMultipartUpload fails")
	}
}

func TestCommitNodeCASRetry(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	ctx := testContext()

	setupSpaceEmpty(store, "space-1", "parent-1")

	session := makeSession("space-1", "parent-1", "newfile.txt")
	session.Offset = 512
	u := &kvfsUpload{session: session, driver: d}

	// Inject 2 CAS failures; commitNode should retry and succeed on attempt 3
	store.injectCASFailures(2)

	err := u.commitNode(ctx, "")
	if err != nil {
		t.Fatalf("commitNode should succeed after CAS retries: %v", err)
	}

	children, _, _ := store.GetChildren("space-1", "parent-1")
	if _, exists := children["newfile.txt"]; !exists {
		t.Error("newfile.txt not in children after CAS retry")
	}
}

func TestCommitNodeCASExhausted(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	d.opts.MaxCASRetries = 10
	ctx := testContext()

	setupSpaceEmpty(store, "space-1", "parent-1")

	session := makeSession("space-1", "parent-1", "newfile.txt")
	session.Offset = 512
	u := &kvfsUpload{session: session, driver: d}

	// Inject more CAS failures than maxRetries (10)
	store.injectCASFailures(20)

	err := u.commitNode(ctx, "")
	if err == nil {
		t.Fatal("commitNode should fail after exhausting CAS retries")
	}
	if !strings.Contains(err.Error(), "max CAS retries") {
		t.Errorf("unexpected error: %v", err)
	}
}

// --- kvfsDriver TUS DataStore interface tests ---

func TestUseIn(t *testing.T) {
	d := testDriver(newMockMetadataStore(), newMockBlobStore())
	composer := tusd.NewStoreComposer()
	d.UseIn(composer)

	if composer.Core == nil {
		t.Error("UseIn should set Core")
	}
	if composer.Terminater == nil {
		t.Error("UseIn should set Terminater")
	}
	if composer.LengthDeferrer == nil {
		t.Error("UseIn should set LengthDeferrer")
	}
}

func TestNewUploadReturnsError(t *testing.T) {
	d := testDriver(newMockMetadataStore(), newMockBlobStore())
	_, err := d.NewUpload(context.Background(), tusd.FileInfo{})
	if err == nil {
		t.Fatal("NewUpload should always return an error")
	}
	if !strings.Contains(err.Error(), "InitiateUpload") {
		t.Errorf("error should mention InitiateUpload, got: %v", err)
	}
}

func TestGetUpload(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	session := makeSession("space-1", "parent-1", "test.txt")
	store.uploads[session.ID] = session

	upload, err := d.GetUpload(context.Background(), "sess-1")
	if err != nil {
		t.Fatalf("GetUpload: %v", err)
	}

	info, err := upload.GetInfo(context.Background())
	if err != nil {
		t.Fatalf("GetInfo: %v", err)
	}
	if info.ID != "sess-1" {
		t.Errorf("upload ID = %q, want %q", info.ID, "sess-1")
	}
}

func TestGetUploadNotFound(t *testing.T) {
	d := testDriver(newMockMetadataStore(), newMockBlobStore())
	_, err := d.GetUpload(context.Background(), "nonexistent")
	if !errors.Is(err, tusd.ErrNotFound) {
		t.Errorf("GetUpload for nonexistent: got %v, want tusd.ErrNotFound", err)
	}
}

func TestAsTerminatableUpload(t *testing.T) {
	d := testDriver(newMockMetadataStore(), newMockBlobStore())
	u := &kvfsUpload{session: makeSession("s", "p", "f"), driver: d}

	tu := d.AsTerminatableUpload(u)
	if tu == nil {
		t.Fatal("AsTerminatableUpload returned nil")
	}
	// Verify it's the same object
	if tu != u {
		t.Error("AsTerminatableUpload should return the same object")
	}
}

func TestAsLengthDeclarableUpload(t *testing.T) {
	d := testDriver(newMockMetadataStore(), newMockBlobStore())
	u := &kvfsUpload{session: makeSession("s", "p", "f"), driver: d}

	lu := d.AsLengthDeclarableUpload(u)
	if lu == nil {
		t.Fatal("AsLengthDeclarableUpload returned nil")
	}
	if lu != u {
		t.Error("AsLengthDeclarableUpload should return the same object")
	}
}

// --- createTUSSession tests ---

func TestCreateTUSSession(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	blob.nextUploadID = "s3-mp-id"
	d := testDriver(store, blob)
	ctx := testContext()

	sessionID, err := d.createTUSSession(ctx, "space-1", "parent-1", "upload.bin", 4096, false, "")
	if err != nil {
		t.Fatalf("createTUSSession: %v", err)
	}
	if sessionID == "" {
		t.Fatal("createTUSSession returned empty session ID")
	}

	session, err := store.GetUpload(sessionID)
	if err != nil {
		t.Fatalf("GetUpload: %v", err)
	}
	if session.SpaceID != "space-1" {
		t.Errorf("session.SpaceID = %q, want %q", session.SpaceID, "space-1")
	}
	if session.ParentID != "parent-1" {
		t.Errorf("session.ParentID = %q, want %q", session.ParentID, "parent-1")
	}
	if session.Filename != "upload.bin" {
		t.Errorf("session.Filename = %q, want %q", session.Filename, "upload.bin")
	}
	if session.Size != 4096 {
		t.Errorf("session.Size = %d, want 4096", session.Size)
	}
	if session.SizeIsDeferred {
		t.Error("session.SizeIsDeferred should be false")
	}
	// createTUSSession does not call InitMultipartUpload. S3MultipartID is
	// empty for new sessions; the blob upload happens in one shot at
	// FinishUpload via blob.Upload.
	if session.S3MultipartID != "" {
		t.Errorf("session.S3MultipartID = %q, want \"\" (no pre-init)", session.S3MultipartID)
	}
	if session.OwnerID != "test-user-id" {
		t.Errorf("session.OwnerID = %q, want %q", session.OwnerID, "test-user-id")
	}
	if session.BlobID == "" {
		t.Error("session.BlobID should not be empty")
	}
	if session.Expires == 0 {
		t.Error("session.Expires should be set")
	}
}

func TestCreateTUSSessionDeferred(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	ctx := testContext()

	sessionID, err := d.createTUSSession(ctx, "space-1", "parent-1", "big.bin", 0, true, "")
	if err != nil {
		t.Fatalf("createTUSSession: %v", err)
	}

	session, _ := store.GetUpload(sessionID)
	if !session.SizeIsDeferred {
		t.Error("session.SizeIsDeferred should be true")
	}
	if session.Size != 0 {
		t.Errorf("session.Size = %d, want 0", session.Size)
	}
}

func TestCreateTUSSessionWithIfMatch(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	ctx := testContext()

	sessionID, err := d.createTUSSession(ctx, "space-1", "parent-1", "file.txt", 100, false, "expected-etag")
	if err != nil {
		t.Fatalf("createTUSSession: %v", err)
	}

	session, _ := store.GetUpload(sessionID)
	if session.IfMatchEtag != "expected-etag" {
		t.Errorf("session.IfMatchEtag = %q, want %q", session.IfMatchEtag, "expected-etag")
	}
}

// createTUSSession does not call S3 at session-open time, so an S3 hiccup
// can no longer fail session creation. Inverse assertion of the previous
// "S3 init must succeed before session opens" contract.
func TestCreateTUSSession_NoS3DependencyAtSessionOpen(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	blob.initErr = io.ErrUnexpectedEOF // would have broken the pre-disk-cache path
	d := testDriver(store, blob)
	ctx := testContext()

	sid, err := d.createTUSSession(ctx, "space-1", "parent-1", "file.txt", 100, false, "")
	if err != nil {
		t.Fatalf("createTUSSession should NOT depend on S3 at open time; got: %v", err)
	}
	if sid == "" {
		t.Fatal("createTUSSession returned empty session ID")
	}
}

// --- InitiateUpload tests ---

func TestInitiateUploadReturnsBothProtocols(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	ctx := testContext()

	setupSpaceEmpty(store, "space-1", "parent-1")

	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "parent-1"},
		Path:       "./newfile.txt",
	}
	result, err := d.InitiateUpload(ctx, ref, 1024, map[string]string{})
	if err != nil {
		t.Fatalf("InitiateUpload: %v", err)
	}

	if _, ok := result["simple"]; !ok {
		t.Error("result should contain 'simple' protocol")
	}
	if _, ok := result["tus"]; !ok {
		t.Error("result should contain 'tus' protocol")
	}
}

// InitiateUpload always returns BOTH simple and tus protocols because
// session creation no longer depends on S3. The previous test asserted
// "tus is omitted when InitMultipartUpload fails" — that contract is gone.
func TestInitiateUpload_AlwaysReturnsBothProtocols(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	blob.initErr = io.ErrUnexpectedEOF // pre-disk-cache, this would have suppressed tus
	d := testDriver(store, blob)
	ctx := testContext()

	setupSpaceEmpty(store, "space-1", "parent-1")

	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "parent-1"},
		Path:       "./newfile.txt",
	}
	result, err := d.InitiateUpload(ctx, ref, 1024, map[string]string{})
	if err != nil {
		t.Fatalf("InitiateUpload: %v", err)
	}

	if _, ok := result["simple"]; !ok {
		t.Error("result should contain 'simple' protocol")
	}
	if _, ok := result["tus"]; !ok {
		t.Error("result should contain 'tus' regardless of S3 init state")
	}
}

func TestInitiateUploadWithIfMatch(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	ctx := testContext()

	setupSpaceEmpty(store, "space-1", "parent-1")

	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "parent-1"},
		Path:       "./test.txt",
	}
	result, err := d.InitiateUpload(ctx, ref, 100, map[string]string{"if-match": "some-etag"})
	if err != nil {
		t.Fatalf("InitiateUpload: %v", err)
	}

	simpleID := result["simple"]
	if !strings.Contains(simpleID, "if-match=some-etag") {
		t.Errorf("simple ID should encode if-match, got: %q", simpleID)
	}
}

func TestInitiateUploadMissingSpaceID(t *testing.T) {
	d := testDriver(newMockMetadataStore(), newMockBlobStore())
	ref := &provider.Reference{Path: "/some/path"}
	_, err := d.InitiateUpload(context.Background(), ref, 100, nil)
	if err == nil {
		t.Fatal("InitiateUpload should fail without resource ID")
	}
}

// --- Interface compliance compile-time checks ---

var (
	_ tusd.Upload                 = (*kvfsUpload)(nil)
	_ tusd.TerminatableUpload     = (*kvfsUpload)(nil)
	_ tusd.LengthDeclarableUpload = (*kvfsUpload)(nil)
)

// --- End-to-end TUS flow test ---

func TestTUSEndToEndFlow(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	ctx := testContext()

	setupSpaceEmpty(store, "space-1", "parent-1")

	// 1. Create TUS session
	sessionID, err := d.createTUSSession(ctx, "space-1", "parent-1", "e2e-file.txt", 20, false, "")
	if err != nil {
		t.Fatalf("createTUSSession: %v", err)
	}

	// 2. Get the upload
	upload, err := d.GetUpload(ctx, sessionID)
	if err != nil {
		t.Fatalf("GetUpload: %v", err)
	}

	// 3. Write two chunks
	chunk1 := []byte("0123456789")
	n1, err := upload.WriteChunk(ctx, 0, bytes.NewReader(chunk1))
	if err != nil {
		t.Fatalf("WriteChunk 1: %v", err)
	}

	chunk2 := []byte("abcdefghij")
	n2, err := upload.WriteChunk(ctx, n1, bytes.NewReader(chunk2))
	if err != nil {
		t.Fatalf("WriteChunk 2: %v", err)
	}

	if n1+n2 != 20 {
		t.Errorf("total written = %d, want 20", n1+n2)
	}

	// 4. Verify info
	info, _ := upload.GetInfo(ctx)
	if info.Offset != 20 {
		t.Errorf("info.Offset = %d, want 20", info.Offset)
	}

	// 5. Finish upload
	err = upload.FinishUpload(ctx)
	if err != nil {
		t.Fatalf("FinishUpload: %v", err)
	}

	// 6. Verify node exists
	children, _, _ := store.GetChildren("space-1", "parent-1")
	nodeID, exists := children["e2e-file.txt"]
	if !exists {
		t.Fatal("e2e-file.txt not found in parent children")
	}

	node, _, _ := store.GetNode("space-1", nodeID)
	if node.Size != 20 {
		t.Errorf("node.Size = %d, want 20", node.Size)
	}
	if node.Type != NodeTypeFile {
		t.Error("node should be a file")
	}

	// 7. Upload session should be cleaned up
	_, err = store.GetUpload(sessionID)
	if err != ErrNotFound {
		t.Error("upload session should be deleted after FinishUpload")
	}
}

func TestTUSOverwriteEndToEnd(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	ctx := testContext()

	setupSpaceWithFile(store, "space-1", "parent-1", "existing-node", "overwrite.txt")

	sessionID, err := d.createTUSSession(ctx, "space-1", "parent-1", "overwrite.txt", 5, false, "")
	if err != nil {
		t.Fatalf("createTUSSession: %v", err)
	}

	upload, err := d.GetUpload(ctx, sessionID)
	if err != nil {
		t.Fatalf("GetUpload: %v", err)
	}

	_, err = upload.WriteChunk(ctx, 0, strings.NewReader("hello"))
	if err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	err = upload.FinishUpload(ctx)
	if err != nil {
		t.Fatalf("FinishUpload: %v", err)
	}

	node, _, _ := store.GetNode("space-1", "existing-node")
	if node.Size != 5 {
		t.Errorf("node.Size = %d, want 5", node.Size)
	}

	// Old content should be versioned
	versions, _ := store.ListVersions("space-1", "existing-node")
	if len(versions) != 1 {
		t.Errorf("expected 1 version, got %d", len(versions))
	}
}

// --- Crash-safe commit regression tests: conditional cleanup + detached ctx ---
//
// The bug: on the LAST PATCH of an upload, the client/gateway could
// cancel the request context AFTER blob.Upload landed bytes in S3 but
// BEFORE commitNode created the file node in NATS. The old unconditional
// cleanup defer then wiped the session row, orphaning the blob — the
// file was permanently invisible.
//
// The fix: (1) run the commit phase on context.WithoutCancel so request
// cancellation can't interrupt it, and (2) only delete the session +
// cache when the commit fully succeeded. Failed commits preserve session
// state so the next attempt can re-commit (S3 PUT + commitFileNode are
// both idempotent).

// TestFinishUploadCommitFailurePreservesSession exercises the load-bearing
// half of the fix: when commitNode fails (synthesised here via an
// if-match etag mismatch), the session row MUST remain in the store and
// UploadInFlight MUST NOT be decremented, so a retry can re-commit.
func TestFinishUploadCommitFailurePreservesSession(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	ctx := testContext()

	setupSpaceWithFile(store, "space-1", "parent-1", "existing-node", "test.txt")

	session := makeSession("space-1", "parent-1", "test.txt")
	session.IfMatchEtag = "wrong-etag" // forces commitNode to return errtypes.Aborted
	session.Offset = 1024
	session.Parts = []PartInfo{{PartNumber: 1, ETag: "etag1", Size: 1024}}
	store.uploads[session.ID] = session

	UploadInFlight.WithLabelValues("tus") // force-init label
	beforeInFlight := getGaugeValue(t, "kvfs_upload_in_flight", map[string]string{"protocol": "tus"})
	UploadInFlight.WithLabelValues("tus").Add(1)

	u := &kvfsUpload{session: session, driver: d}
	if err := u.FinishUpload(ctx); err == nil {
		t.Fatal("FinishUpload should fail on if-match etag mismatch")
	}

	// Session row must survive — this is the regression we are guarding.
	if _, err := store.GetUpload(session.ID); err == ErrNotFound {
		t.Error("upload session was deleted on commit failure — conditional-cleanup contract broken")
	}

	// Blob was rolled back so the next retry uploads cleanly.
	if len(blob.deleteCalled) != 1 {
		t.Errorf("expected 1 rollback blob Delete on commit failure, got %d", len(blob.deleteCalled))
	}

	// UploadInFlight gauge must not be decremented for a session that's
	// still in flight. We incremented above to simulate the "in flight"
	// state at FinishUpload entry; after a failed commit it should still
	// be one above the baseline.
	if got := getGaugeValue(t, "kvfs_upload_in_flight", map[string]string{"protocol": "tus"}); got != beforeInFlight+1 {
		t.Errorf("UploadInFlight: got %v, want %v (decremented on commit failure)", got, beforeInFlight+1)
	}

	// Reset for other tests.
	UploadInFlight.WithLabelValues("tus").Add(-1)
}

func TestFinishUploadInjectedCommitFailurePreservesSession(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	d.commitFailHook = func() error { return errors.New("injected commit failure (test seam)") }
	ctx := testContext()

	setupSpaceEmpty(store, "space-1", "parent-1")

	session := makeSession("space-1", "parent-1", "injected.txt")
	session.S3MultipartID = ""
	session.Offset = 5
	store.uploads[session.ID] = session
	if _, err := d.uploadCache.Append(session.ID, 0, bytes.NewReader([]byte("hello"))); err != nil {
		t.Fatalf("uploadCache.Append: %v", err)
	}

	UploadInFlight.WithLabelValues("tus")
	beforeInFlight := getGaugeValue(t, "kvfs_upload_in_flight", map[string]string{"protocol": "tus"})
	UploadInFlight.WithLabelValues("tus").Add(1)

	u := &kvfsUpload{session: session, driver: d}
	err := u.FinishUpload(ctx)
	if err == nil {
		t.Fatal("FinishUpload should fail when the commit-fail seam is set")
	}
	if !strings.Contains(err.Error(), "injected commit failure") {
		t.Fatalf("unexpected error: %v", err)
	}

	// Blob was uploaded (injection fires AFTER blob.Upload).
	if len(blob.blobs) == 0 {
		t.Error("blob.Upload was never called — injection should fire after blob upload")
	}

	// Session row must survive (conditional-cleanup contract).
	if _, err := store.GetUpload(session.ID); err == ErrNotFound {
		t.Error("session was deleted on injected failure — conditional-cleanup contract broken")
	}

	// Gauge must not be decremented.
	if got := getGaugeValue(t, "kvfs_upload_in_flight", map[string]string{"protocol": "tus"}); got != beforeInFlight+1 {
		t.Errorf("UploadInFlight: got %v, want %v", got, beforeInFlight+1)
	}

	// Clear the knob and retry — should commit successfully.
	d.commitFailHook = nil
	if _, err := d.uploadCache.Append(session.ID, 0, bytes.NewReader([]byte("hello"))); err != nil {
		t.Fatalf("re-stage uploadCache.Append: %v", err)
	}
	store.uploads[session.ID] = session
	u2 := &kvfsUpload{session: session, driver: d}
	if err := u2.FinishUpload(ctx); err != nil {
		t.Fatalf("FinishUpload after clearing inject should succeed: %v", err)
	}

	// Session cleaned up after successful commit.
	if _, err := store.GetUpload(session.ID); err != ErrNotFound {
		t.Error("session should be cleaned up after successful commit")
	}

	UploadInFlight.WithLabelValues("tus").Add(-1)
}

// TestFinishUploadDetachedCtxSurvivesCancellation: a request ctx that is
// already canceled at FinishUpload entry must NOT prevent the commit from
// completing — the canceled ctx came from the data-gateway forwarder
// (which times out independently of the upload).
func TestFinishUploadDetachedCtxSurvivesCancellation(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)

	setupSpaceEmpty(store, "space-1", "parent-1")

	session := makeSession("space-1", "parent-1", "newfile.txt")
	session.S3MultipartID = ""
	session.Offset = 5
	store.uploads[session.ID] = session

	if _, err := d.uploadCache.Append(session.ID, 0, bytes.NewReader([]byte("hello"))); err != nil {
		t.Fatalf("uploadCache.Append: %v", err)
	}

	UploadInFlight.WithLabelValues("tus").Add(1)

	// Cancel the request ctx BEFORE FinishUpload runs. The detached
	// commitCtx (derived via context.WithoutCancel) must keep the commit
	// alive.
	ctx, cancel := context.WithCancel(testContext())
	cancel()

	u := &kvfsUpload{session: session, driver: d}
	if err := u.FinishUpload(ctx); err != nil {
		t.Fatalf("FinishUpload should succeed despite canceled request ctx: %v", err)
	}

	// Session cleaned up (commit succeeded).
	if _, err := store.GetUpload(session.ID); err != ErrNotFound {
		t.Error("upload session not cleaned up after successful commit")
	}
	// File node created.
	children, _, _ := store.GetChildren("space-1", "parent-1")
	if _, exists := children["newfile.txt"]; !exists {
		t.Error("file node not created — commit was interrupted by request ctx cancellation")
	}
	// Blob landed in S3.
	if len(blob.blobs) == 0 {
		t.Error("no blob in S3 — blob.Upload was interrupted by request ctx cancellation")
	}
}

// TestFinishUploadIdempotentReCommit exercises the A3 idempotency contract:
// when a first FinishUpload commits successfully but its caller (tusd)
// or the client triggers a second FinishUpload on the same session (the
// typical "retry on apparent failure" path), the second call must NOT
// corrupt state — it should either no-op or re-commit cleanly.
//
// In practice, the second call's session lookup uses the SAME id but
// after the first cleanup ran, the session row is gone from the store.
// tusd's GetUpload would return ErrNotFound — meaning the second
// FinishUpload never happens. This test exercises the more interesting
// case: a commit-failure-then-retry sequence, validating that the retry
// commits cleanly when fault is cleared (already exercised by
// TestFinishUploadInjectedCommitFailurePreservesSession). Here we test
// the "second commit on a still-live session" case by holding a stale
// kvfsUpload reference and calling FinishUpload twice without an
// intermediate Drop. The second call's blob.Upload should overwrite
// idempotently (S3 PUT is idempotent on key), and commitFileNode's
// existing-node CAS branch should take over.
func TestFinishUploadIdempotentReCommit(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	ctx := testContext()

	setupSpaceEmpty(store, "space-1", "parent-1")

	session := makeSession("space-1", "parent-1", "twice.txt")
	session.S3MultipartID = ""
	session.Offset = 5
	store.uploads[session.ID] = session
	if _, err := d.uploadCache.Append(session.ID, 0, bytes.NewReader([]byte("hello"))); err != nil {
		t.Fatalf("uploadCache.Append: %v", err)
	}
	UploadInFlight.WithLabelValues("tus").Add(1)

	// First FinishUpload commits cleanly.
	u1 := &kvfsUpload{session: session, driver: d}
	if err := u1.FinishUpload(ctx); err != nil {
		t.Fatalf("first FinishUpload: %v", err)
	}

	// Session row is gone, cache file dropped.
	if _, err := store.GetUpload(session.ID); err != ErrNotFound {
		t.Fatal("session row should be cleaned up after first commit")
	}
	if size, _ := d.uploadCache.Size(session.ID); size != 0 {
		t.Fatal("cache file should be dropped after first commit")
	}

	// Capture the node ID + ETag so we can compare after the second
	// commit — re-committing should leave the node intact or update
	// idempotently, not duplicate.
	children1, _, _ := store.GetChildren("space-1", "parent-1")
	nodeID1, exists := children1["twice.txt"]
	if !exists {
		t.Fatal("file node not created by first commit")
	}
	node1, _, _ := store.GetNode("space-1", nodeID1)
	etag1 := node1.ETag

	// Now simulate the "client retry" by re-staging the bytes + session
	// and calling FinishUpload again with the SAME session ID. This is
	// the scenario where tusd's GetUpload reconciles a fresh session row
	// for a resumed upload that happens to land on an already-committed
	// file (the idempotent path).
	store.uploads[session.ID] = session
	if _, err := d.uploadCache.Append(session.ID, 0, bytes.NewReader([]byte("hello"))); err != nil {
		t.Fatalf("uploadCache.Append (retry): %v", err)
	}
	UploadInFlight.WithLabelValues("tus").Add(1)

	u2 := &kvfsUpload{session: session, driver: d}
	if err := u2.FinishUpload(ctx); err != nil {
		t.Fatalf("second FinishUpload (retry): %v", err)
	}

	// Children map must still have exactly one entry under that name —
	// the existing-node CAS branch updates in place, no duplicate.
	children2, _, _ := store.GetChildren("space-1", "parent-1")
	if got := len(children2); got != 1 {
		t.Errorf("children map size = %d, want 1 (duplicate created on re-commit)", got)
	}
	nodeID2, _ := children2["twice.txt"]
	if nodeID2 != nodeID1 {
		t.Errorf("nodeID changed across re-commit: %q → %q (should reuse existing node)", nodeID1, nodeID2)
	}

	// The ETag changes because commitFileNode mints a fresh one on every
	// update; that's expected (it signals "this object was overwritten").
	// The point is the SAME node was updated, not a NEW node created.
	node2, _, _ := store.GetNode("space-1", nodeID2)
	if node2.Size != 5 {
		t.Errorf("node.Size after re-commit = %d, want 5", node2.Size)
	}
	_ = etag1 // currently unused — kept for future "etag bumped on retry" assertions if we add them
}

// TestFinishUploadConcurrentSameSession runs two FinishUpload goroutines
// against the same session ID. With Track A's idempotency contract,
// both must complete without panic; the final state must have exactly
// one file node (not two) and the session must be cleaned up by the
// successful path.
//
// This guards against the failure mode where two simultaneous retries
// race to create children entries and end up with two nodes under the
// same name.
func TestFinishUploadConcurrentSameSession(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	ctx := testContext()

	setupSpaceEmpty(store, "space-1", "parent-1")

	session := makeSession("space-1", "parent-1", "concurrent.txt")
	session.S3MultipartID = ""
	session.Offset = 5
	store.uploads[session.ID] = session
	if _, err := d.uploadCache.Append(session.ID, 0, bytes.NewReader([]byte("hello"))); err != nil {
		t.Fatalf("uploadCache.Append: %v", err)
	}
	// Two "in flight" markers — one per concurrent FinishUpload caller.
	UploadInFlight.WithLabelValues("tus").Add(2)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each goroutine builds its own kvfsUpload from the shared
			// session pointer (matches what tusd does — each PATCH
			// constructs a fresh kvfsUpload via GetUpload).
			u := &kvfsUpload{session: session, driver: d}
			errs[i] = u.FinishUpload(ctx)
		}()
	}
	wg.Wait()

	// At least ONE goroutine must have succeeded. The other may have
	// succeeded too (idempotent re-commit) or failed cleanly on a CAS
	// conflict — either outcome is acceptable, but a panic or a node
	// duplicate is not.
	successCount := 0
	for _, e := range errs {
		if e == nil {
			successCount++
		}
	}
	if successCount == 0 {
		t.Errorf("both concurrent FinishUploads failed: %v, %v", errs[0], errs[1])
	}

	children, _, _ := store.GetChildren("space-1", "parent-1")
	if got := len(children); got != 1 {
		t.Errorf("children map size = %d, want 1 (concurrent FinishUploads created duplicates)", got)
	}
	if _, exists := children["concurrent.txt"]; !exists {
		t.Error("file node not created after concurrent FinishUploads")
	}
}

// --- Simple-upload sibling-session cleanup tests ---
//
// InitiateUpload offers both protocols and persists a TUS session up
// front; a caller that commits via the simple protocol would otherwise
// strand that session until the TTL reaper runs. The simple ID therefore
// carries the sibling session's ID, and Upload reaps it on commit.

func TestInitiateUploadSimpleIDCarriesTUSSibling(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	setupSpaceEmpty(store, "s1", "root")

	result, err := d.InitiateUpload(testContext(), &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./newfile.txt",
	}, 100, map[string]string{})
	if err != nil {
		t.Fatalf("InitiateUpload: %v", err)
	}

	tusID, ok := result["tus"]
	if !ok || tusID == "" {
		t.Fatal("result should contain 'tus' protocol")
	}
	simpleID, ok := result["simple"]
	if !ok {
		t.Fatal("result should contain 'simple' protocol")
	}
	if !strings.Contains(simpleID, "tus-sibling="+tusID) {
		t.Errorf("simple ID %q does not carry tus-sibling=%s", simpleID, tusID)
	}
	if _, err := store.GetUpload(tusID); err != nil {
		t.Errorf("TUS session %s not persisted: %v", tusID, err)
	}
}

func TestUploadSimpleCommitReapsSiblingSession(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	setupSpaceEmpty(store, "s1", "root")

	ctx := testContext()
	result, err := d.InitiateUpload(ctx, &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./newfile.txt",
	}, 5, map[string]string{})
	if err != nil {
		t.Fatalf("InitiateUpload: %v", err)
	}
	tusID := result["tus"]

	// The dataprovider's simple handler passes the simple ID back as a
	// "/"-prefixed path-only reference.
	content := []byte("hello")
	ri, err := d.Upload(ctx, storage.UploadRequest{
		Ref:    &provider.Reference{Path: "/" + result["simple"]},
		Body:   io.NopCloser(bytes.NewReader(content)),
		Length: int64(len(content)),
	}, nil)
	if err != nil {
		t.Fatalf("Upload via simple ID: %v", err)
	}
	if ri == nil || ri.Name != "newfile.txt" {
		t.Fatalf("unexpected resource info: %+v", ri)
	}

	if _, err := store.GetUpload(tusID); err != ErrNotFound {
		t.Errorf("sibling TUS session %s should be reaped after simple commit, GetUpload err = %v", tusID, err)
	}
}

func TestUploadSimpleCommitWithIfMatchParsesBothParams(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	setupSpaceWithFile(store, "s1", "root", "file-1", "existing.txt")

	ctx := testContext()
	result, err := d.InitiateUpload(ctx, &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./existing.txt",
	}, 3, map[string]string{"if-match": "old-etag"})
	if err != nil {
		t.Fatalf("InitiateUpload: %v", err)
	}
	tusID := result["tus"]
	if !strings.Contains(result["simple"], "if-match=") || !strings.Contains(result["simple"], "tus-sibling=") {
		t.Fatalf("simple ID %q should carry both if-match and tus-sibling", result["simple"])
	}

	content := []byte("new")
	if _, err := d.Upload(ctx, storage.UploadRequest{
		Ref:    &provider.Reference{Path: "/" + result["simple"]},
		Body:   io.NopCloser(bytes.NewReader(content)),
		Length: int64(len(content)),
	}, nil); err != nil {
		t.Fatalf("Upload with if-match + sibling: %v", err)
	}

	if _, err := store.GetUpload(tusID); err != ErrNotFound {
		t.Errorf("sibling session should be reaped on overwrite commit, err = %v", err)
	}

	// Stale etag with DIFFERENT content must still be rejected.
	differentContent := []byte("xxx")
	result2, err := d.InitiateUpload(ctx, &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./existing.txt",
	}, int64(len(differentContent)), map[string]string{"if-match": "stale-etag"})
	if err != nil {
		t.Fatalf("second InitiateUpload: %v", err)
	}
	if _, err := d.Upload(ctx, storage.UploadRequest{
		Ref:    &provider.Reference{Path: "/" + result2["simple"]},
		Body:   io.NopCloser(bytes.NewReader(differentContent)),
		Length: int64(len(differentContent)),
	}, nil); err == nil {
		t.Error("Upload with stale if-match and different content should fail")
	}

	// Stale etag with SAME content succeeds (idempotent overwrite).
	result3, err := d.InitiateUpload(ctx, &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./existing.txt",
	}, int64(len(content)), map[string]string{"if-match": "stale-etag"})
	if err != nil {
		t.Fatalf("third InitiateUpload: %v", err)
	}
	if _, err := d.Upload(ctx, storage.UploadRequest{
		Ref:    &provider.Reference{Path: "/" + result3["simple"]},
		Body:   io.NopCloser(bytes.NewReader(content)),
		Length: int64(len(content)),
	}, nil); err != nil {
		t.Errorf("Upload with stale if-match but same content should succeed (idempotent), got: %v", err)
	}
}

func TestUploadSimpleCommitFailureKeepsSibling(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	setupSpaceEmpty(store, "s1", "root")

	ctx := testContext()
	result, err := d.InitiateUpload(ctx, &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       "./newfile.txt",
	}, 5, map[string]string{})
	if err != nil {
		t.Fatalf("InitiateUpload: %v", err)
	}
	tusID := result["tus"]

	// Force the commit to fail after the blob upload.
	store.putNodeErr = errors.New("synthetic commit failure")
	if _, err := d.Upload(ctx, storage.UploadRequest{
		Ref:    &provider.Reference{Path: "/" + result["simple"]},
		Body:   io.NopCloser(bytes.NewReader([]byte("hello"))),
		Length: 5,
	}, nil); err == nil {
		t.Fatal("Upload should fail when commit fails")
	}

	// The sibling stays put: the client may retry via TUS, and the TTL
	// reaper collects it otherwise.
	if _, err := store.GetUpload(tusID); err != nil {
		t.Errorf("sibling session should survive a failed simple commit, err = %v", err)
	}
}

// --- Idempotent overwrite tests ---

// TestCommitFileNodeIdempotentOverwrite verifies that when If-Match
// etag mismatches but the existing node already has the same checksum
// and size, commitFileNode returns success (idempotent replay).
func TestCommitFileNodeIdempotentOverwrite(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	ctx := testContext()

	setupSpaceWithFile(store, "space-1", "parent-1", "existing-node", "test.txt")
	node := store.nodes["space-1.existing-node"]
	node.Checksum = "sha1:abc123"
	node.Size = 500
	node.ETag = "new-etag-after-commit"

	nodeID, err := d.commitFileNode(ctx, commitFileParams{
		SpaceID:     "space-1",
		ParentID:    "parent-1",
		Name:        "test.txt",
		BlobID:      "different-blob-from-retry",
		Size:        500,
		Checksum:    "sha1:abc123",
		MimeType:    "text/plain",
		IfMatchEtag: "old-etag",
		OwnerID:     "test-user-id",
	})
	if err != nil {
		t.Fatalf("expected idempotent success, got error: %v", err)
	}
	if nodeID != "existing-node" {
		t.Errorf("nodeID = %q, want %q", nodeID, "existing-node")
	}

	// Node should NOT have been updated (no version, no blob change).
	afterNode := store.nodes["space-1.existing-node"]
	if afterNode.BlobID != "old-blob" {
		t.Errorf("node.BlobID changed to %q, expected unchanged %q", afterNode.BlobID, "old-blob")
	}

	versions, _ := store.ListVersions("space-1", "existing-node")
	if len(versions) != 0 {
		t.Errorf("expected 0 versions (idempotent skip), got %d", len(versions))
	}
}

// TestCommitFileNodeEtagMismatchDifferentContent verifies that a
// genuine etag mismatch (different content) still returns Aborted.
func TestCommitFileNodeEtagMismatchDifferentContent(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	ctx := testContext()

	setupSpaceWithFile(store, "space-1", "parent-1", "existing-node", "test.txt")
	node := store.nodes["space-1.existing-node"]
	node.Checksum = "sha1:abc123"
	node.Size = 500
	node.ETag = "new-etag"

	_, err := d.commitFileNode(ctx, commitFileParams{
		SpaceID:     "space-1",
		ParentID:    "parent-1",
		Name:        "test.txt",
		BlobID:      "new-blob",
		Size:        600,
		Checksum:    "sha1:different",
		MimeType:    "text/plain",
		IfMatchEtag: "old-etag",
		OwnerID:     "test-user-id",
	})
	if err == nil {
		t.Fatal("expected Aborted error for genuine etag mismatch with different content")
	}
	if !strings.Contains(err.Error(), "etag mismatch") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestCommitFileNodeIdempotentGuardNoChecksum verifies that the
// idempotency guard does NOT fire when checksum is empty (legacy
// multipart path). The etag mismatch should return Aborted as before.
func TestCommitFileNodeIdempotentGuardNoChecksum(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	ctx := testContext()

	setupSpaceWithFile(store, "space-1", "parent-1", "existing-node", "test.txt")
	node := store.nodes["space-1.existing-node"]
	node.Checksum = "sha1:abc123"
	node.Size = 500
	node.ETag = "new-etag"

	_, err := d.commitFileNode(ctx, commitFileParams{
		SpaceID:     "space-1",
		ParentID:    "parent-1",
		Name:        "test.txt",
		BlobID:      "new-blob",
		Size:        500,
		Checksum:    "",
		MimeType:    "text/plain",
		IfMatchEtag: "old-etag",
		OwnerID:     "test-user-id",
	})
	if err == nil {
		t.Fatal("expected Aborted error when checksum is empty (guard should not fire)")
	}
	if !strings.Contains(err.Error(), "etag mismatch") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestCommitFileNodeIdempotentGuardSizeMismatch verifies that matching
// checksum but different size does NOT trigger the idempotency guard.
func TestCommitFileNodeIdempotentGuardSizeMismatch(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	ctx := testContext()

	setupSpaceWithFile(store, "space-1", "parent-1", "existing-node", "test.txt")
	node := store.nodes["space-1.existing-node"]
	node.Checksum = "sha1:abc123"
	node.Size = 500
	node.ETag = "new-etag"

	_, err := d.commitFileNode(ctx, commitFileParams{
		SpaceID:     "space-1",
		ParentID:    "parent-1",
		Name:        "test.txt",
		BlobID:      "new-blob",
		Size:        999,
		Checksum:    "sha1:abc123",
		MimeType:    "text/plain",
		IfMatchEtag: "old-etag",
		OwnerID:     "test-user-id",
	})
	if err == nil {
		t.Fatal("expected Aborted error when size mismatches")
	}
	if !strings.Contains(err.Error(), "etag mismatch") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestFinishUploadIdempotentReplay is the end-to-end test: a TUS
// session whose FinishUpload succeeds but whose response is lost. The
// retry session has a different BlobID but uploads the same content.
// FinishUpload should detect the idempotent overwrite and succeed.
func TestFinishUploadIdempotentReplay(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob)
	ctx := testContext()

	// Simulate the state after the first (successful but lost) commit:
	// the existing node has the content checksum and a new etag.
	// sha1 of 1024 bytes of 'A' = 746c3f4d286c531e065e8af76e0ac0868831c6b4
	knownChecksum := "sha1:746c3f4d286c531e065e8af76e0ac0868831c6b4"
	setupSpaceWithFile(store, "space-1", "parent-1", "existing-node", "test.txt")
	node := store.nodes["space-1.existing-node"]
	node.Checksum = knownChecksum
	node.Size = 1024
	node.BlobID = "first-commit-blob"
	node.ETag = "etag-after-first-commit"

	// Create a retry session: different BlobID, stale If-Match etag.
	session := &UploadSession{
		ID:          "retry-sess",
		SpaceID:     "space-1",
		Filename:    "test.txt",
		ParentID:    "parent-1",
		Size:        1024,
		Offset:      0,
		BlobID:      "retry-blob-uuid",
		IfMatchEtag: "old-etag",
		OwnerID:     "test-user-id",
	}
	store.uploads[session.ID] = session
	UploadInFlight.WithLabelValues("tus").Add(1)

	// Stage the same content in the upload cache (modern path, no S3MultipartID).
	data := bytes.Repeat([]byte("A"), 1024)
	u := &kvfsUpload{session: session, driver: d}
	n, err := u.WriteChunk(ctx, 0, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	if n != 1024 {
		t.Fatalf("WriteChunk wrote %d, want 1024", n)
	}

	err = u.FinishUpload(ctx)
	if err != nil {
		t.Fatalf("FinishUpload should succeed on idempotent replay, got: %v", err)
	}

	// Session should be cleaned up.
	if _, err := store.GetUpload(session.ID); err != ErrNotFound {
		t.Error("upload session should be deleted after idempotent FinishUpload")
	}

	// No version should have been created.
	versions, _ := store.ListVersions("space-1", "existing-node")
	if len(versions) != 0 {
		t.Errorf("expected 0 versions on idempotent replay, got %d", len(versions))
	}

	// The existing node's BlobID should be unchanged (no re-write).
	afterNode := store.nodes["space-1.existing-node"]
	if afterNode.BlobID != "first-commit-blob" {
		t.Errorf("node.BlobID changed to %q, expected unchanged %q", afterNode.BlobID, "first-commit-blob")
	}
}
