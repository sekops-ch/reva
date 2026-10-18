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
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	user "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/rs/zerolog"
	"go-micro.dev/v4/events"

	ctxpkg "github.com/opencloud-eu/reva/v2/pkg/ctx"
	revaevents "github.com/opencloud-eu/reva/v2/pkg/events"
	"github.com/opencloud-eu/reva/v2/pkg/storage"
)

// --- Mock event stream ---

type publishedEvent struct {
	topic    string
	payload  interface{}
	metadata map[string]string
}

type mockStream struct {
	mu     sync.Mutex
	events []publishedEvent
	err    error
}

func newMockStream() *mockStream {
	return &mockStream{}
}

func (s *mockStream) Publish(topic string, msg interface{}, opts ...events.PublishOption) error {
	if s.err != nil {
		return s.err
	}
	o := events.PublishOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, publishedEvent{
		topic:    topic,
		payload:  msg,
		metadata: o.Metadata,
	})
	return nil
}

func (s *mockStream) Consume(topic string, opts ...events.ConsumeOption) (<-chan events.Event, error) {
	return nil, nil
}

func (s *mockStream) lastEvent() publishedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.events) == 0 {
		return publishedEvent{}
	}
	return s.events[len(s.events)-1]
}

func (s *mockStream) eventCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}

func (s *mockStream) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = nil
}

func (s *mockStream) eventTypes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var types []string
	for _, e := range s.events {
		types = append(types, reflect.TypeOf(e.payload).String())
	}
	return types
}

// --- Test helpers ---

func testDriverWithStream(t testing.TB, store *mockMetadataStore, blob *mockBlobStore, stream revaevents.Stream) *kvfsDriver {
	t.Helper()
	log := zerolog.Nop()
	return &kvfsDriver{
		store:       store,
		blob:        blob,
		opts:        &Options{},
		log:         &log,
		stream:      stream,
		uploadCache: newDiskUploadCache(t.TempDir()),
	}
}

func eventTestContext() context.Context {
	return ctxpkg.ContextSetUser(context.Background(), &user.User{
		Id: &user.UserId{OpaqueId: "test-user-id"},
	})
}

// --- CreateDir events ---

func TestCreateDir_DriverStaysSilent(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(t, store, blob, stream)

	setupSpaceEmpty(store, "space-1", "root-1")

	ctx := eventTestContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "root-1"},
		Path:       "./newdir",
	}

	err := d.CreateDir(ctx, ref)
	if err != nil {
		t.Fatalf("CreateDir failed: %v", err)
	}

	// The eventsmiddleware publishes ContainerCreated on the CreateContainer
	// response; a driver copy would double-count every mkdir.
	if n := stream.eventCount(); n != 0 {
		t.Fatalf("driver published %d event(s) for CreateDir, want 0 (middleware covers it): %v",
			n, stream.eventTypes())
	}
}

func TestCreateDir_NoEventWithoutStream(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(t, store, blob) // no stream

	setupSpaceEmpty(store, "space-1", "root-1")

	ctx := eventTestContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "root-1"},
		Path:       "./newdir",
	}

	err := d.CreateDir(ctx, ref)
	if err != nil {
		t.Fatalf("CreateDir failed: %v", err)
	}
	// No panic, no event — just works
}

// --- Upload (simple) events ---

func TestUpload_PublishesFileUploaded(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(t, store, blob, stream)

	setupSpaceEmpty(store, "space-1", "root-1")

	ctx := eventTestContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "root-1"},
		Path:       "./test.txt",
	}

	_, err := d.Upload(ctx, storage.UploadRequest{
		Ref:    ref,
		Body:   io.NopCloser(bytes.NewReader([]byte("hello"))),
		Length: 5,
	}, nil)
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	if stream.eventCount() != 1 {
		t.Fatalf("expected 1 event, got %d", stream.eventCount())
	}

	ev := stream.lastEvent()
	fu, ok := ev.payload.(revaevents.FileUploaded)
	if !ok {
		t.Fatalf("expected FileUploaded, got %T", ev.payload)
	}

	if fu.Executant.OpaqueId != "test-user-id" {
		t.Errorf("expected executant test-user-id, got %s", fu.Executant.OpaqueId)
	}
	if fu.Ref == nil || fu.Ref.ResourceId == nil {
		t.Fatal("expected non-nil ref")
	}
	if fu.Ref.ResourceId.SpaceId != "space-1" {
		t.Errorf("expected space-1 in ref, got %s", fu.Ref.ResourceId.SpaceId)
	}
	if fu.Timestamp == nil {
		t.Error("expected non-nil timestamp")
	}
}

func TestUpload_Overwrite_PublishesFileUploaded(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(t, store, blob, stream)

	setupSpaceWithFile(store, "space-1", "root-1", "file-1", "existing.txt")

	ctx := eventTestContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "root-1"},
		Path:       "./existing.txt",
	}

	_, err := d.Upload(ctx, storage.UploadRequest{
		Ref:    ref,
		Body:   io.NopCloser(bytes.NewReader([]byte("updated"))),
		Length: 7,
	}, nil)
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	if stream.eventCount() != 1 {
		t.Fatalf("expected 1 event, got %d", stream.eventCount())
	}

	ev := stream.lastEvent()
	_, ok := ev.payload.(revaevents.FileUploaded)
	if !ok {
		t.Fatalf("expected FileUploaded, got %T", ev.payload)
	}
}

// --- Delete events ---

func TestDelete_DriverStaysSilent(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(t, store, blob, stream)

	setupSpaceWithFile(store, "space-1", "root-1", "file-1", "doomed.txt")

	ctx := eventTestContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "file-1"},
	}

	err := d.Delete(ctx, ref)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// The eventsmiddleware publishes ItemTrashed on the Delete response.
	if n := stream.eventCount(); n != 0 {
		t.Fatalf("driver published %d event(s) for Delete, want 0 (middleware covers it): %v",
			n, stream.eventTypes())
	}
}

// --- Move events ---

func TestMove_PublishesItemMoved(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(t, store, blob, stream)

	setupSpaceWithFile(store, "space-1", "root-1", "file-1", "original.txt")

	ctx := eventTestContext()
	oldRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "file-1"},
	}
	newRef := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "root-1"},
		Path:       "./renamed.txt",
	}

	err := d.Move(ctx, oldRef, newRef)
	if err != nil {
		t.Fatalf("Move failed: %v", err)
	}

	if stream.eventCount() != 1 {
		t.Fatalf("expected 1 event, got %d", stream.eventCount())
	}

	ev := stream.lastEvent()
	im, ok := ev.payload.(revaevents.ItemMoved)
	if !ok {
		t.Fatalf("expected ItemMoved, got %T", ev.payload)
	}

	if im.Executant.OpaqueId != "test-user-id" {
		t.Errorf("expected executant test-user-id, got %s", im.Executant.OpaqueId)
	}
	if im.OldReference != oldRef {
		t.Error("expected OldReference to match original ref")
	}
	if im.Ref == nil || im.Ref.ResourceId == nil || im.Ref.ResourceId.SpaceId != "space-1" {
		t.Error("expected new Ref to be in space-1")
	}
	if im.Timestamp == nil {
		t.Error("expected non-nil timestamp")
	}
}

// --- RestoreRevision events ---

func TestRestoreRevision_DriverStaysSilent(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(t, store, blob, stream)

	setupSpaceWithFile(store, "space-1", "root-1", "file-1", "versioned.txt")

	// Add a version to restore
	ver := &VersionEntry{
		Key:      "ver-key-1",
		NodeID:   "file-1",
		SpaceID:  "space-1",
		BlobID:   "ver-blob-1",
		BlobSize: 50,
		Size:     50,
		MTime:    500,
		ETag:     "ver-etag",
	}
	store.versions = append(store.versions, ver)

	ctx := eventTestContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "file-1"},
	}

	err := d.RestoreRevision(ctx, ref, "ver-key-1")
	if err != nil {
		t.Fatalf("RestoreRevision failed: %v", err)
	}

	// The eventsmiddleware publishes FileVersionRestored on the
	// RestoreFileVersion response.
	if n := stream.eventCount(); n != 0 {
		t.Fatalf("driver published %d event(s) for RestoreRevision, want 0 (middleware covers it): %v",
			n, stream.eventTypes())
	}
}

// --- RestoreRecycleItem events ---

func TestRestoreRecycleItem_DriverStaysSilent(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(t, store, blob, stream)

	// Setup space with root
	setupSpaceEmpty(store, "space-1", "root-1")

	// Add item to trash
	trashEntry := &TrashEntry{
		Key:          "trash-key-1",
		NodeID:       "trashed-node",
		SpaceID:      "space-1",
		OriginalPath: "/restored.txt",
		DeletionTime: 999,
		Node: NodeEntry{
			ID: "trashed-node", SpaceID: "space-1", ParentID: "root-1",
			Name: "restored.txt", Type: NodeTypeFile, BlobID: "trashed-blob",
			Size: 200, Owner: "test-user-id",
		},
	}
	store.trash["space-1.trash-key-1"] = trashEntry

	ctx := eventTestContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1"},
	}

	err := d.RestoreRecycleItem(ctx, ref, "trash-key-1", "", nil)
	if err != nil {
		t.Fatalf("RestoreRecycleItem failed: %v", err)
	}

	// The eventsmiddleware publishes ItemRestored on the RestoreRecycleItem
	// response.
	if n := stream.eventCount(); n != 0 {
		t.Fatalf("driver published %d event(s) for RestoreRecycleItem, want 0 (middleware covers it): %v",
			n, stream.eventTypes())
	}
}

// --- PurgeRecycleItem events ---

func TestPurgeRecycleItem_DriverStaysSilent(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(t, store, blob, stream)

	setupSpaceEmpty(store, "space-1", "root-1")

	// Add item to trash
	trashEntry := &TrashEntry{
		Key:     "trash-key-2",
		NodeID:  "purge-node",
		SpaceID: "space-1",
		Node: NodeEntry{
			ID: "purge-node", SpaceID: "space-1",
			Type: NodeTypeFile, BlobID: "purge-blob",
			Size: 300, Owner: "test-user-id",
		},
	}
	store.trash["space-1.trash-key-2"] = trashEntry

	ctx := eventTestContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1"},
	}

	err := d.PurgeRecycleItem(ctx, ref, "trash-key-2", "")
	if err != nil {
		t.Fatalf("PurgeRecycleItem failed: %v", err)
	}

	// The eventsmiddleware publishes ItemPurged on the PurgeRecycle response.
	if n := stream.eventCount(); n != 0 {
		t.Fatalf("driver published %d event(s) for PurgeRecycleItem, want 0 (middleware covers it): %v",
			n, stream.eventTypes())
	}
}

// --- EmptyRecycle events ---

func TestEmptyRecycle_DriverStaysSilent(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(t, store, blob, stream)

	setupSpaceEmpty(store, "space-1", "root-1")

	// Add multiple items to trash
	store.trash["space-1.t1"] = &TrashEntry{
		Key: "t1", NodeID: "n1", SpaceID: "space-1",
		Node: NodeEntry{Type: NodeTypeFile, BlobID: "b1", Size: 10},
	}
	store.trash["space-1.t2"] = &TrashEntry{
		Key: "t2", NodeID: "n2", SpaceID: "space-1",
		Node: NodeEntry{Type: NodeTypeFile, BlobID: "b2", Size: 20},
	}

	ctx := eventTestContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1"},
	}

	err := d.EmptyRecycle(ctx, ref)
	if err != nil {
		t.Fatalf("EmptyRecycle failed: %v", err)
	}

	// The eventsmiddleware publishes TrashbinPurged (or ItemPurged) on the
	// PurgeRecycle response.
	if n := stream.eventCount(); n != 0 {
		t.Fatalf("driver published %d event(s) for EmptyRecycle, want 0 (middleware covers it): %v",
			n, stream.eventTypes())
	}
}

// --- TUS FinishUpload events ---

func TestFinishUpload_PublishesFileUploaded(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(t, store, blob, stream)

	setupSpaceEmpty(store, "space-1", "root-1")
	blob.nextUploadID = "mp-1"

	session := &UploadSession{
		ID:            "sess-ev-1",
		SpaceID:       "space-1",
		Filename:      "tus-file.txt",
		ParentID:      "root-1",
		Size:          5,
		Offset:        5,
		Storage:       map[string]string{},
		BlobID:        "tus-blob-1",
		S3MultipartID: "mp-1",
		Parts:         []PartInfo{{PartNumber: 1, ETag: "e1", Size: 5}},
		OwnerID:       "test-user-id",
	}
	store.uploads["sess-ev-1"] = session
	blob.blobs[BlobKey("space-1", "tus-blob-1")] = []byte("hello")
	blob.multiparts["mp-1"] = map[int][]byte{1: []byte("hello")}

	ctx := eventTestContext()
	upload := &kvfsUpload{session: session, driver: d}

	err := upload.FinishUpload(ctx)
	if err != nil {
		t.Fatalf("FinishUpload failed: %v", err)
	}

	if stream.eventCount() != 1 {
		t.Fatalf("expected 1 event, got %d", stream.eventCount())
	}

	ev := stream.lastEvent()
	fu, ok := ev.payload.(revaevents.FileUploaded)
	if !ok {
		t.Fatalf("expected FileUploaded, got %T", ev.payload)
	}

	if fu.Executant.OpaqueId != "test-user-id" {
		t.Errorf("expected executant test-user-id, got %s", fu.Executant.OpaqueId)
	}
	if fu.Ref == nil {
		t.Fatal("expected non-nil ref")
	}
	if fu.Ref.Path != "tus-file.txt" {
		t.Errorf("expected ref path tus-file.txt, got %s", fu.Ref.Path)
	}
	if fu.Timestamp == nil {
		t.Error("expected non-nil timestamp")
	}
}

// --- Stream error handling ---

func TestPublishEvent_StreamError_LogsButDoesNotFail(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	stream.err = errMockStreamFailure
	d := testDriverWithStream(t, store, blob, stream)

	setupSpaceEmpty(store, "space-1", "root-1")

	ctx := eventTestContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "root-1"},
		Path:       "./dir-with-broken-stream",
	}

	err := d.CreateDir(ctx, ref)
	if err != nil {
		t.Fatalf("CreateDir should succeed even when stream fails: %v", err)
	}
	// The event was attempted but errored — operation still succeeded
}

var errMockStreamFailure = errorString("mock stream failure")

type errorString string

func (e errorString) Error() string { return string(e) }

// --- publishEvent nil stream safety ---

func TestPublishEvent_NilStream_NoOp(t *testing.T) {
	log := zerolog.Nop()
	d := &kvfsDriver{
		log: &log,
	}
	ctx := context.Background()
	// Should not panic
	d.publishEvent(ctx, func() interface{} {
		return revaevents.FileUploaded{}
	})
}

func TestPublishEvent_NilEvent_NoPublish(t *testing.T) {
	stream := newMockStream()
	log := zerolog.Nop()
	d := &kvfsDriver{
		stream: stream,
		log:    &log,
	}
	ctx := context.Background()
	d.publishEvent(ctx, func() interface{} {
		return nil
	})
	if stream.eventCount() != 0 {
		t.Error("expected no events when factory returns nil")
	}
}

// --- Event metadata ---

func TestEvent_HasCorrectTopic(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(t, store, blob, stream)

	setupSpaceEmpty(store, "space-1", "root-1")

	ctx := eventTestContext()
	// Uploads are a driver-published event (no eventsmiddleware counterpart).
	_, err := d.Upload(ctx, storage.UploadRequest{
		Ref: &provider.Reference{
			ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "root-1"},
			Path:       "./topictest.txt",
		},
		Body:   io.NopCloser(bytes.NewReader([]byte("data"))),
		Length: 4,
	}, nil)
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	ev := stream.lastEvent()
	if ev.topic != revaevents.MainQueueName {
		t.Errorf("expected topic %q, got %q", revaevents.MainQueueName, ev.topic)
	}
	if ev.metadata[revaevents.MetadatakeyEventType] == "" {
		t.Error("expected event type in metadata")
	}
	if ev.metadata[revaevents.MetadatakeyEventID] == "" {
		t.Error("expected event ID in metadata")
	}
}

// --- Multiple operations produce correct event sequence ---

func TestMultipleOperations_EventSequence(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(t, store, blob, stream)

	setupSpaceEmpty(store, "space-1", "root-1")

	ctx := eventTestContext()

	// 1. CreateDir
	d.CreateDir(ctx, &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "root-1"},
		Path:       "./subdir",
	})

	// 2. Upload a file
	d.Upload(ctx, storage.UploadRequest{
		Ref: &provider.Reference{
			ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "root-1"},
			Path:       "./file.txt",
		},
		Body:   io.NopCloser(bytes.NewReader([]byte("data"))),
		Length: 4,
	}, nil)

	// Find the file node to delete it
	children, _, _ := store.GetChildren("space-1", "root-1")
	fileNodeID := children["file.txt"]

	// 3. Delete the file
	d.Delete(ctx, &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: fileNodeID},
	})

	// Of the three operations only the upload has no eventsmiddleware
	// counterpart, so it is the only driver publish in the sequence.
	types := stream.eventTypes()
	expected := []string{"events.FileUploaded"}
	if len(types) != len(expected) {
		t.Fatalf("expected %d event(s), got %d: %v", len(expected), len(types), types)
	}
	for i, exp := range expected {
		if types[i] != exp {
			t.Errorf("event[%d]: expected %s, got %s", i, exp, types[i])
		}
	}
}

// --- Delete directory events ---

func TestDeleteDir_DriverStaysSilent(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(t, store, blob, stream)

	// Setup space with a directory child
	setupSpaceEmpty(store, "space-1", "root-1")
	dirID := "dir-node-1"
	store.nodes["space-1."+dirID] = &NodeEntry{
		ID: dirID, SpaceID: "space-1", ParentID: "root-1",
		Name: "mydir", Type: NodeTypeDir, Owner: "test-user-id",
		MimeType: "httpd/unix-directory",
	}
	store.nodeRevs["space-1."+dirID] = 1
	store.children["space-1.root-1"] = ChildMap{"mydir": dirID}
	store.childRevs["space-1.root-1"] = store.childRevs["space-1.root-1"]
	store.children["space-1."+dirID] = ChildMap{}
	store.childRevs["space-1."+dirID] = 1

	ctx := eventTestContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: dirID},
	}

	err := d.Delete(ctx, ref)
	if err != nil {
		t.Fatalf("Delete dir failed: %v", err)
	}

	// Directory deletes are covered by the eventsmiddleware too.
	if n := stream.eventCount(); n != 0 {
		t.Fatalf("driver published %d event(s) for a directory Delete, want 0 (middleware covers it): %v",
			n, stream.eventTypes())
	}
}

// --- EmptyRecycle with no items ---

func TestEmptyRecycle_NoItems_DriverStaysSilent(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(t, store, blob, stream)

	setupSpaceEmpty(store, "space-empty", "root-empty")

	ctx := eventTestContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-empty"},
	}

	err := d.EmptyRecycle(ctx, ref)
	if err != nil {
		t.Fatalf("EmptyRecycle failed: %v", err)
	}

	if n := stream.eventCount(); n != 0 {
		t.Fatalf("driver published %d event(s) for an empty EmptyRecycle, want 0: %v",
			n, stream.eventTypes())
	}
}

// --- PurgeRecycleItem deletes blob ---

func TestPurgeRecycleItem_DeletesBlob_DriverStaysSilent(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(t, store, blob, stream)

	setupSpaceEmpty(store, "space-1", "root-1")

	blobKey := BlobKey("space-1", "purge-blob-2")
	blob.blobs[blobKey] = []byte("data")

	store.trash["space-1.tk1"] = &TrashEntry{
		Key: "tk1", NodeID: "pn1", SpaceID: "space-1",
		Node: NodeEntry{
			ID: "pn1", SpaceID: "space-1",
			Type: NodeTypeFile, BlobID: "purge-blob-2", Size: 4,
			Owner: "test-user-id",
		},
	}

	ctx := eventTestContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1"},
	}

	err := d.PurgeRecycleItem(ctx, ref, "tk1", "")
	if err != nil {
		t.Fatalf("PurgeRecycleItem failed: %v", err)
	}

	// Blob should be deleted
	if _, exists := blob.blobs[blobKey]; exists {
		t.Error("expected blob to be deleted from S3")
	}

	// The eventsmiddleware publishes ItemPurged; the driver stays silent.
	if n := stream.eventCount(); n != 0 {
		t.Fatalf("driver published %d event(s) for PurgeRecycleItem, want 0: %v",
			n, stream.eventTypes())
	}
}

// --- Restore to custom path ---

func TestRestoreRecycleItem_CustomPath_DriverStaysSilent(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(t, store, blob, stream)

	setupSpaceEmpty(store, "space-1", "root-1")

	store.trash["space-1.trash-custom"] = &TrashEntry{
		Key: "trash-custom", NodeID: "custom-node", SpaceID: "space-1",
		OriginalPath: "/old-name.txt",
		DeletionTime: 999,
		Node: NodeEntry{
			ID: "custom-node", SpaceID: "space-1", ParentID: "root-1",
			Name: "old-name.txt", Type: NodeTypeFile, BlobID: "cb1",
			Size: 100, Owner: "test-user-id",
		},
	}

	ctx := eventTestContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1"},
	}
	restoreRef := &provider.Reference{
		Path: "/new-name.txt",
	}

	err := d.RestoreRecycleItem(ctx, ref, "trash-custom", "", restoreRef)
	if err != nil {
		t.Fatalf("RestoreRecycleItem failed: %v", err)
	}

	// The eventsmiddleware publishes ItemRestored; the driver stays silent.
	if n := stream.eventCount(); n != 0 {
		t.Fatalf("driver published %d event(s) for a custom-path restore, want 0: %v",
			n, stream.eventTypes())
	}
}

// --- No user in context → permission denied ---

func TestDelete_NoUserInContext_Denied(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(t, store, blob, stream)

	setupSpaceWithFile(store, "space-1", "root-1", "file-1", "no-user.txt")

	ctx := context.Background()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "file-1"},
	}

	err := d.Delete(ctx, ref)
	if err == nil {
		t.Fatal("expected permission error for unauthenticated context")
	}

	if stream.eventCount() != 0 {
		t.Errorf("expected no events without user context, got %d", stream.eventCount())
	}
}

func TestFinishUpload_ClientContinuationWithoutUser_PublishesStoredOwner(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(t, store, blob, stream)

	setupSpaceEmpty(store, "space-1", "root-1")

	chunk1 := []byte("first chunk of a client-finished upload ")
	chunk2 := []byte("second chunk arriving on the unauthenticated data path")
	full := append(append([]byte{}, chunk1...), chunk2...)

	store.uploads["sess-cc-1"] = &UploadSession{
		ID:       "sess-cc-1",
		SpaceID:  "space-1",
		Filename: "continuation.bin",
		ParentID: "root-1",
		Size:     int64(len(full)),
		Storage:  map[string]string{},
		BlobID:   "cc-blob",
		OwnerID:  "test-user-id",
	}

	// Request 1: the authenticated creation-with-upload POST carries the user.
	u1, err := d.GetUpload(testContext(), "sess-cc-1")
	if err != nil {
		t.Fatalf("GetUpload (req 1): %v", err)
	}
	if _, err := u1.(*kvfsUpload).WriteChunk(testContext(), 0, bytes.NewReader(chunk1)); err != nil {
		t.Fatalf("WriteChunk (req 1): %v", err)
	}

	// Request 2: the client's own continuation PATCH rides the unprotected
	// data path — no user in ctx — and finishes the upload.
	noUser := context.Background()
	u2, err := d.GetUpload(noUser, "sess-cc-1")
	if err != nil {
		t.Fatalf("GetUpload (req 2): %v", err)
	}
	ku2 := u2.(*kvfsUpload)
	if _, err := ku2.WriteChunk(noUser, int64(len(chunk1)), bytes.NewReader(chunk2)); err != nil {
		t.Fatalf("WriteChunk (req 2): %v", err)
	}
	UploadInFlight.WithLabelValues("tus").Add(1)
	if err := ku2.FinishUpload(noUser); err != nil {
		t.Fatalf("FinishUpload: %v", err)
	}

	if n := stream.eventCount(); n != 1 {
		t.Fatalf("FileUploaded events = %d, want 1 (stored-owner fallback)", n)
	}
	fu, ok := stream.lastEvent().payload.(revaevents.FileUploaded)
	if !ok {
		t.Fatalf("last event is %T, want FileUploaded", stream.lastEvent().payload)
	}
	if fu.Executant.GetOpaqueId() != "test-user-id" {
		t.Errorf("Executant = %q, want the session's stored owner", fu.Executant.GetOpaqueId())
	}
	if fu.Owner.GetOpaqueId() != "test-user-id" || fu.SpaceOwner.GetOpaqueId() != "test-user-id" {
		t.Errorf("Owner/SpaceOwner = %q/%q, want the stored owner",
			fu.Owner.GetOpaqueId(), fu.SpaceOwner.GetOpaqueId())
	}
	if fu.Ref.GetPath() != "continuation.bin" {
		t.Errorf("Ref.Path = %q, want the uploaded filename", fu.Ref.GetPath())
	}
}

func TestFinishUpload_ClientContinuationWithoutUser_KeepsIdpAndInitiator(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(t, store, blob, stream)

	setupSpaceEmpty(store, "space-1", "root-1")
	body := []byte("one-chunk body finished without a user")
	store.uploads["sess-cc-2"] = &UploadSession{
		ID:          "sess-cc-2",
		SpaceID:     "space-1",
		Filename:    "idp.bin",
		ParentID:    "root-1",
		Size:        int64(len(body)),
		Storage:     map[string]string{},
		BlobID:      "cc-blob-2",
		OwnerID:     "test-user-id",
		OwnerIDP:    "https://idp.test",
		OwnerType:   1,
		InitiatorID: "init-9",
	}

	noUser := context.Background()
	u, err := d.GetUpload(noUser, "sess-cc-2")
	if err != nil {
		t.Fatalf("GetUpload: %v", err)
	}
	ku := u.(*kvfsUpload)
	if _, err := ku.WriteChunk(noUser, 0, bytes.NewReader(body)); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	UploadInFlight.WithLabelValues("tus").Add(1)
	if err := ku.FinishUpload(noUser); err != nil {
		t.Fatalf("FinishUpload: %v", err)
	}

	if n := stream.eventCount(); n != 1 {
		t.Fatalf("FileUploaded events = %d, want 1", n)
	}
	fu := stream.lastEvent().payload.(revaevents.FileUploaded)
	if fu.Executant.GetIdp() != "https://idp.test" || fu.Executant.GetType() != 1 {
		t.Errorf("Executant idp/type = %q/%d, want the stored identity",
			fu.Executant.GetIdp(), fu.Executant.GetType())
	}
	if got := stream.lastEvent().metadata[revaevents.MetadatakeyInitiatorID]; got != "init-9" {
		t.Errorf("initiator metadata = %q, want the stored initiator id", got)
	}
}

func TestDownload_DriverStaysSilent(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(t, store, blob, stream)

	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
	blob.blobs[BlobKey("s1", "old-blob")] = []byte("content")

	ctx := eventTestContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "f1"},
	}
	_, rc, err := d.Download(ctx, ref, nil)
	if err != nil {
		t.Fatalf("Download failed: %v", err)
	}
	if rc != nil {
		_, _ = io.ReadAll(rc)
		rc.Close()
	}

	// The eventsmiddleware publishes FileDownloaded on InitiateFileDownload;
	// a driver-side copy would double every download event.
	if n := stream.eventCount(); n != 0 {
		t.Fatalf("Download published %d driver events, want 0", n)
	}
}

// --- TouchFile must update parent ETag and stay silent (middleware publishes FileTouched) ---

func TestTouchFile_ExistingFile_ChangesNothing(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(t, store, blob, stream)

	setupSpaceWithFile(store, "space-1", "root-1", "file-1", "test.txt")
	origETag := store.nodes["space-1.root-1"].ETag

	ctx := eventTestContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "file-1"},
	}

	if err := d.TouchFile(ctx, ref, false, ""); !isAlreadyExists(err) {
		t.Fatalf("TouchFile on an existing file = %v, want AlreadyExists", err)
	}
	if store.nodes["space-1.root-1"].ETag != origETag {
		t.Error("parent directory ETag changed")
	}
	if stream.eventCount() != 0 {
		t.Errorf("expected no event, got %d", stream.eventCount())
	}
}

func TestTouchFile_NewFile_UpdatesParentETag(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(t, store, blob, stream)

	setupSpaceEmpty(store, "space-1", "root-1")
	parentBefore := store.nodes["space-1.root-1"]
	origETag := parentBefore.ETag

	ctx := eventTestContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "root-1"},
		Path:       "newfile.txt",
	}

	err := d.TouchFile(ctx, ref, false, "")
	if err != nil {
		t.Fatalf("TouchFile failed: %v", err)
	}

	children := store.children["space-1.root-1"]
	if _, ok := children["newfile.txt"]; !ok {
		t.Fatal("newfile.txt not found in parent's children")
	}

	parentAfter := store.nodes["space-1.root-1"]
	if parentAfter.ETag == origETag {
		t.Error("parent directory ETag did not change after TouchFile creating new file")
	}

	// The eventsmiddleware publishes FileTouched for TouchFile, as with
	// decomposedfs; a driver FileUploaded here double-counted uploads.
	if n := stream.eventCount(); n != 0 {
		t.Errorf("driver published %d event(s) for TouchFile, want 0: %v",
			n, stream.eventTypes())
	}
}

// --- Async publish path (driver wired like New(), with a live worker) ---
//
// The fixtures above (testDriverWithStream) leave eventQueue nil, so they
// exercise the synchronous fallback. The tests below build a driver WITH a
// real eventQueue + a running eventPublisher goroutine, covering the
// fire-and-forget path, queue-overflow drop, metadata fidelity, and shutdown.

// testDriverWithAsyncStream mirrors the event-worker wiring New() performs so
// the async dispatch path is under test, not just the sync fallback.
func testDriverWithAsyncStream(t testing.TB, store *mockMetadataStore, blob *mockBlobStore, stream revaevents.Stream, bufSize int) *kvfsDriver {
	t.Helper()
	log := zerolog.Nop()
	d := &kvfsDriver{
		store:       store,
		blob:        blob,
		opts:        &Options{},
		log:         &log,
		stream:      stream,
		uploadCache: newDiskUploadCache(t.TempDir()),
		eventQueue:  make(chan eventJob, bufSize),
		eventStop:   make(chan struct{}),
	}
	go d.eventPublisher()
	return d
}

// waitForEvents polls until the mock stream has recorded at least want events
// or the deadline elapses — the deterministic way to assert on async delivery.
func waitForEvents(t *testing.T, stream *mockStream, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if stream.eventCount() >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d event(s); got %d", want, stream.eventCount())
}

func TestPublishEventAsync_DrainsToStream(t *testing.T) {
	stream := newMockStream()
	d := testDriverWithAsyncStream(t, newMockMetadataStore(), newMockBlobStore(), stream, eventQueueSize)
	defer d.Shutdown(context.Background())

	d.publishEvent(eventTestContext(), func() interface{} {
		return revaevents.FileUploaded{}
	})

	waitForEvents(t, stream, 1)
}

// TestPublishEventAsync_PreservesInitiator is the regression guard for the
// metadata-fidelity fix: the worker must publish with the originating request
// ctx (detached via WithoutCancel) so events.Publish still emits initiatorID.
// A bare context.Background() in the worker would drop it, breaking downstream
// self-notification suppression.
func TestPublishEventAsync_PreservesInitiator(t *testing.T) {
	stream := newMockStream()
	d := testDriverWithAsyncStream(t, newMockMetadataStore(), newMockBlobStore(), stream, eventQueueSize)
	defer d.Shutdown(context.Background())

	ctx := ctxpkg.ContextSetInitiator(eventTestContext(), "init-123")
	d.publishEvent(ctx, func() interface{} {
		return revaevents.FileUploaded{}
	})

	waitForEvents(t, stream, 1)
	if got := stream.lastEvent().metadata[revaevents.MetadatakeyInitiatorID]; got != "init-123" {
		t.Errorf("async event lost initiatorID: got %q, want %q", got, "init-123")
	}
}

// TestPublishEventAsync_QueueFull_Drops builds a 1-slot queue with NO worker so
// nothing ever drains; the first enqueue fills the buffer and every subsequent
// one must be dropped without ever blocking the caller. Reaching the assertions
// proves enqueue is non-blocking; the buffer length proves exactly one was kept
// and the rest dropped. (EventQueueDropped's wiring is covered live by
// tests/integration/test_event_queue_metric.py — asserting the global counter
// here would need prometheus/testutil, which isn't vendored.)
func TestPublishEventAsync_QueueFull_Drops(t *testing.T) {
	stream := newMockStream()
	log := zerolog.Nop()
	d := &kvfsDriver{
		stream:     stream,
		log:        &log,
		eventQueue: make(chan eventJob, 1), // no eventPublisher started → never drains
		eventStop:  make(chan struct{}),
	}

	mk := func() interface{} { return revaevents.FileUploaded{} }
	ctx := eventTestContext()
	d.publishEvent(ctx, mk) // fills the single buffer slot
	d.publishEvent(ctx, mk) // full, dropped (must not block)
	d.publishEvent(ctx, mk) // full, dropped (must not block)

	if got := len(d.eventQueue); got != 1 {
		t.Errorf("expected exactly 1 buffered job (the rest dropped), got %d", got)
	}
	// With no worker, nothing should ever reach the stream.
	if stream.eventCount() != 0 {
		t.Errorf("no worker running, expected 0 published, got %d", stream.eventCount())
	}
}

// TestEventPublisher_ShutdownStopsWorker asserts Shutdown returns promptly and
// the worker goroutine stops draining afterwards.
func TestEventPublisher_ShutdownStopsWorker(t *testing.T) {
	stream := newMockStream()
	d := testDriverWithAsyncStream(t, newMockMetadataStore(), newMockBlobStore(), stream, eventQueueSize)

	// Prove the worker is alive.
	d.publishEvent(eventTestContext(), func() interface{} { return revaevents.FileUploaded{} })
	waitForEvents(t, stream, 1)

	done := make(chan error, 1)
	go func() { done <- d.Shutdown(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Shutdown returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return within 2s")
	}

	// After shutdown the worker is gone: a freshly enqueued event is buffered
	// but never drained.
	d.publishEvent(eventTestContext(), func() interface{} { return revaevents.FileUploaded{} })
	time.Sleep(50 * time.Millisecond)
	if stream.eventCount() != 1 {
		t.Errorf("worker still draining after Shutdown: stream count=%d, want 1", stream.eventCount())
	}
}
