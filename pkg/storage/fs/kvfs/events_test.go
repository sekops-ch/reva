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
	"os"
	"reflect"
	"sync"
	"testing"

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

func testDriverWithStream(store *mockMetadataStore, blob *mockBlobStore, stream revaevents.Stream) *kvfsDriver {
	log := zerolog.Nop()
	tmpDir, err := os.MkdirTemp("", "kvfs-test-*")
	if err != nil {
		panic(err)
	}
	return &kvfsDriver{
		store:       store,
		blob:        blob,
		opts:        &Options{},
		log:         &log,
		stream:      stream,
		uploadCache: newDiskUploadCache(tmpDir),
	}
}

func eventTestContext() context.Context {
	return ctxpkg.ContextSetUser(context.Background(), &user.User{
		Id: &user.UserId{OpaqueId: "test-user-id"},
	})
}

// --- CreateDir events ---

func TestCreateDir_PublishesContainerCreated(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(store, blob, stream)

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

	if stream.eventCount() != 1 {
		t.Fatalf("expected 1 event, got %d", stream.eventCount())
	}

	ev := stream.lastEvent()
	cc, ok := ev.payload.(revaevents.ContainerCreated)
	if !ok {
		t.Fatalf("expected ContainerCreated, got %T", ev.payload)
	}

	if cc.Executant.OpaqueId != "test-user-id" {
		t.Errorf("expected executant test-user-id, got %s", cc.Executant.OpaqueId)
	}
	if cc.SpaceOwner.OpaqueId != "test-user-id" {
		t.Errorf("expected space owner test-user-id, got %s", cc.SpaceOwner.OpaqueId)
	}
	if cc.Ref == nil || cc.Ref.ResourceId == nil {
		t.Fatal("expected non-nil ref with resource ID")
	}
	if cc.Ref.ResourceId.SpaceId != "space-1" {
		t.Errorf("expected space-1 in ref, got %s", cc.Ref.ResourceId.SpaceId)
	}
	if cc.ParentID == nil || cc.ParentID.OpaqueId != "root-1" {
		t.Error("expected ParentID to reference root-1")
	}
	if cc.Timestamp == nil {
		t.Error("expected non-nil timestamp")
	}
}

func TestCreateDir_NoEventWithoutStream(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	d := testDriver(store, blob) // no stream

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
	d := testDriverWithStream(store, blob, stream)

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
	d := testDriverWithStream(store, blob, stream)

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

func TestDelete_PublishesItemTrashed(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(store, blob, stream)

	setupSpaceWithFile(store, "space-1", "root-1", "file-1", "doomed.txt")

	ctx := eventTestContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "file-1"},
	}

	err := d.Delete(ctx, ref)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if stream.eventCount() != 1 {
		t.Fatalf("expected 1 event, got %d", stream.eventCount())
	}

	ev := stream.lastEvent()
	it, ok := ev.payload.(revaevents.ItemTrashed)
	if !ok {
		t.Fatalf("expected ItemTrashed, got %T", ev.payload)
	}

	if it.Executant.OpaqueId != "test-user-id" {
		t.Errorf("expected executant test-user-id, got %s", it.Executant.OpaqueId)
	}
	if it.ID == nil || it.ID.SpaceId != "space-1" || it.ID.OpaqueId != "file-1" {
		t.Error("expected resource ID to match deleted file")
	}
	if it.Timestamp == nil {
		t.Error("expected non-nil timestamp")
	}
}

// --- Move events ---

func TestMove_PublishesItemMoved(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(store, blob, stream)

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

func TestRestoreRevision_PublishesFileVersionRestored(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(store, blob, stream)

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

	if stream.eventCount() != 1 {
		t.Fatalf("expected 1 event, got %d", stream.eventCount())
	}

	ev := stream.lastEvent()
	fvr, ok := ev.payload.(revaevents.FileVersionRestored)
	if !ok {
		t.Fatalf("expected FileVersionRestored, got %T", ev.payload)
	}

	if fvr.Key != "ver-key-1" {
		t.Errorf("expected key ver-key-1, got %s", fvr.Key)
	}
	if fvr.Ref == nil || fvr.Ref.ResourceId.OpaqueId != "file-1" {
		t.Error("expected ref to point to file-1")
	}
	if fvr.Timestamp == nil {
		t.Error("expected non-nil timestamp")
	}
}

// --- RestoreRecycleItem events ---

func TestRestoreRecycleItem_PublishesItemRestored(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(store, blob, stream)

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

	if stream.eventCount() != 1 {
		t.Fatalf("expected 1 event, got %d", stream.eventCount())
	}

	ev := stream.lastEvent()
	ir, ok := ev.payload.(revaevents.ItemRestored)
	if !ok {
		t.Fatalf("expected ItemRestored, got %T", ev.payload)
	}

	if ir.Key != "trash-key-1" {
		t.Errorf("expected key trash-key-1, got %s", ir.Key)
	}
	if ir.ID == nil || ir.ID.OpaqueId != "trashed-node" {
		t.Error("expected ID to match trashed node")
	}
	if ir.Timestamp == nil {
		t.Error("expected non-nil timestamp")
	}
}

// --- PurgeRecycleItem events ---

func TestPurgeRecycleItem_PublishesItemPurged(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(store, blob, stream)

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

	if stream.eventCount() != 1 {
		t.Fatalf("expected 1 event, got %d", stream.eventCount())
	}

	ev := stream.lastEvent()
	ip, ok := ev.payload.(revaevents.ItemPurged)
	if !ok {
		t.Fatalf("expected ItemPurged, got %T", ev.payload)
	}

	if ip.ID == nil || ip.ID.OpaqueId != "purge-node" {
		t.Error("expected ID to match purged node")
	}
	if ip.Executant.OpaqueId != "test-user-id" {
		t.Errorf("expected executant test-user-id, got %s", ip.Executant.OpaqueId)
	}
	if ip.Timestamp == nil {
		t.Error("expected non-nil timestamp")
	}
}

// --- EmptyRecycle events ---

func TestEmptyRecycle_PublishesTrashbinPurged(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(store, blob, stream)

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

	if stream.eventCount() != 1 {
		t.Fatalf("expected 1 TrashbinPurged event, got %d", stream.eventCount())
	}

	ev := stream.lastEvent()
	tp, ok := ev.payload.(revaevents.TrashbinPurged)
	if !ok {
		t.Fatalf("expected TrashbinPurged, got %T", ev.payload)
	}

	if tp.Executant.OpaqueId != "test-user-id" {
		t.Errorf("expected executant test-user-id, got %s", tp.Executant.OpaqueId)
	}
	if tp.Ref != ref {
		t.Error("expected ref to match the passed reference")
	}
	if tp.Timestamp == nil {
		t.Error("expected non-nil timestamp")
	}
}

// --- TUS FinishUpload events ---

func TestFinishUpload_PublishesFileUploaded(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(store, blob, stream)

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
	d := testDriverWithStream(store, blob, stream)

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
	d := testDriverWithStream(store, blob, stream)

	setupSpaceEmpty(store, "space-1", "root-1")

	ctx := eventTestContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "root-1"},
		Path:       "./topictest",
	}

	d.CreateDir(ctx, ref)

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
	d := testDriverWithStream(store, blob, stream)

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

	types := stream.eventTypes()
	if len(types) != 3 {
		t.Fatalf("expected 3 events, got %d: %v", len(types), types)
	}

	expected := []string{
		"events.ContainerCreated",
		"events.FileUploaded",
		"events.ItemTrashed",
	}
	for i, exp := range expected {
		if types[i] != exp {
			t.Errorf("event[%d]: expected %s, got %s", i, exp, types[i])
		}
	}
}

// --- Delete directory events ---

func TestDeleteDir_PublishesItemTrashed(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(store, blob, stream)

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

	if stream.eventCount() != 1 {
		t.Fatalf("expected 1 event, got %d", stream.eventCount())
	}

	ev := stream.lastEvent()
	it, ok := ev.payload.(revaevents.ItemTrashed)
	if !ok {
		t.Fatalf("expected ItemTrashed for directory, got %T", ev.payload)
	}
	if it.ID.OpaqueId != dirID {
		t.Errorf("expected trashed ID %s, got %s", dirID, it.ID.OpaqueId)
	}
}

// --- EmptyRecycle with no items ---

func TestEmptyRecycle_NoItems_StillPublishes(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(store, blob, stream)

	setupSpaceEmpty(store, "space-empty", "root-empty")

	ctx := eventTestContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-empty"},
	}

	err := d.EmptyRecycle(ctx, ref)
	if err != nil {
		t.Fatalf("EmptyRecycle failed: %v", err)
	}

	if stream.eventCount() != 1 {
		t.Fatalf("expected 1 TrashbinPurged event even with no items, got %d", stream.eventCount())
	}

	_, ok := stream.lastEvent().payload.(revaevents.TrashbinPurged)
	if !ok {
		t.Fatalf("expected TrashbinPurged, got %T", stream.lastEvent().payload)
	}
}

// --- PurgeRecycleItem deletes blob ---

func TestPurgeRecycleItem_DeletesBlob_AndPublishes(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(store, blob, stream)

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

	// Event should still be published
	if stream.eventCount() != 1 {
		t.Fatalf("expected 1 event, got %d", stream.eventCount())
	}
	_, ok := stream.lastEvent().payload.(revaevents.ItemPurged)
	if !ok {
		t.Fatalf("expected ItemPurged, got %T", stream.lastEvent().payload)
	}
}

// --- Restore to custom path ---

func TestRestoreRecycleItem_CustomPath_PublishesItemRestored(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(store, blob, stream)

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

	if stream.eventCount() != 1 {
		t.Fatalf("expected 1 event, got %d", stream.eventCount())
	}

	ir, ok := stream.lastEvent().payload.(revaevents.ItemRestored)
	if !ok {
		t.Fatalf("expected ItemRestored, got %T", stream.lastEvent().payload)
	}
	if ir.Key != "trash-custom" {
		t.Errorf("expected key trash-custom, got %s", ir.Key)
	}
}

// --- No user in context → permission denied ---

func TestDelete_NoUserInContext_Denied(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(store, blob, stream)

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

// --- TouchFile must update parent ETag and publish event ---

func TestTouchFile_ExistingFile_UpdatesParentETag(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(store, blob, stream)

	setupSpaceWithFile(store, "space-1", "root-1", "file-1", "test.txt")
	parentBefore := store.nodes["space-1.root-1"]
	origETag := parentBefore.ETag

	ctx := eventTestContext()
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "space-1", OpaqueId: "file-1"},
	}

	err := d.TouchFile(ctx, ref, false, "")
	if err != nil {
		t.Fatalf("TouchFile failed: %v", err)
	}

	parentAfter := store.nodes["space-1.root-1"]
	if parentAfter.ETag == origETag {
		t.Error("parent directory ETag did not change after TouchFile on existing file")
	}

	if stream.eventCount() != 1 {
		t.Errorf("expected 1 event, got %d", stream.eventCount())
	}
}

func TestTouchFile_NewFile_UpdatesParentETag(t *testing.T) {
	store := newMockMetadataStore()
	blob := newMockBlobStore()
	stream := newMockStream()
	d := testDriverWithStream(store, blob, stream)

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

	if stream.eventCount() != 1 {
		t.Errorf("expected 1 event, got %d", stream.eventCount())
	}
}
