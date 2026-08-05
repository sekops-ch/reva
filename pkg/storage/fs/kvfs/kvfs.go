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

// Package kvfs implements a storage driver that stores all metadata and tree
// structure in NATS JetStream KV, and file blobs in S3. This enables
// horizontal scaling of storage-users pods without shared POSIX filesystems.
package kvfs

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	user "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	types "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"

	ctxpkg "github.com/opencloud-eu/reva/v2/pkg/ctx"
	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
	"github.com/opencloud-eu/reva/v2/pkg/events"
	"github.com/opencloud-eu/reva/v2/pkg/mime"
	"github.com/opencloud-eu/reva/v2/pkg/storage"
	"github.com/opencloud-eu/reva/v2/pkg/storage/fs/registry"
	"github.com/opencloud-eu/reva/v2/pkg/storagespace"
	"github.com/opencloud-eu/reva/v2/pkg/utils"
	"github.com/pkg/errors"
)

const defaultMaxCASRetries = 100

// defaultMaxDeleteDepth bounds recursive delete traversal (trash purge,
// space delete) when max_delete_depth is unset. Deep enough for any sane
// tree; shallow enough that the recursion can never threaten the stack.
const defaultMaxDeleteDepth = 100

// commitPhaseTimeout bounds every detached commit context returned by
// [kvfsDriver.commitPhase]. 5 minutes comfortably envelopes worst-case
// 1 GiB blob uploads on a slow NATS leader; prevents goroutine leaks on
// catastrophic downstream wedges. See the helper's docstring for the
// full contract.
const commitPhaseTimeout = 5 * time.Minute

// uploadSessionTTL is the lifetime of a persisted TUS session. Single source
// of truth for both the Expires stamp and the sampler's createdAt estimate
// (createdAt = Expires - uploadSessionTTL), so the two cannot drift.
const uploadSessionTTL = 24 * time.Hour

// eventQueueSize bounds the async event-publish buffer. Events are small and
// the worker drains fast under normal load; on overflow we drop and increment
// EventQueueDropped rather than block the request (see publishEventAsync).
const eventQueueSize = 1024

// uploadAgeSampleInterval is how often the upload-staleness sampler walks
// oc-uploads. A var, not a const, so tests can shrink it.
var uploadAgeSampleInterval = 5 * time.Minute

var errNoChange = fmt.Errorf("kvfs: no change needed")

type commitFileParams struct {
	SpaceID     string
	ParentID    string
	Name        string
	BlobID      string
	Size        int64
	Checksum    string
	MimeType    string
	IfMatchEtag string
	OwnerID     string
}

func init() {
	registry.Register("kvfs", New)
}

// kvfsDriver implements the storage.FS interface using NATS JetStream KV
// for metadata/tree and S3 for blob storage.
type kvfsDriver struct {
	store  MetadataStore
	blob   BlobStore
	opts   *Options
	log    *zerolog.Logger
	gc     *blobGC
	stream events.Stream

	// gcCancel cancels the GC sweep root context on Shutdown, so a graceful
	// SIGTERM aborts an in-flight sweep and its deferred ReleaseLock frees the
	// gc-sweep lock immediately instead of waiting for the lease to lapse.
	gcCancel context.CancelFunc

	// uploadCache stages TUS upload bodies between WriteChunk calls.
	// Disk-backed by default (decomposedfs-style). See upload_cache.go.
	uploadCache UploadCache

	// parentLocks serialises in-process writes against the same parent's
	// children map, converting a pod-local CAS storm into a sequential CAS
	// queue. Cross-pod contention still goes through NATS CAS, but our
	// 2-pod deployment already halves the conflict fan-out this way.
	parentLocks sync.Map // map[string]*sync.Mutex

	// eventQueue + eventStop drive the async event-publish worker. See
	// publishEvent / eventPublisher. Bounded buffer (eventQueueSize); on
	// overflow we drop and increment EventQueueDropped — events are
	// best-effort downstream.
	eventQueue chan eventJob
	eventStop  chan struct{}

	// commitFailHook is a test seam: when non-nil, FinishUpload calls it
	// after the blob upload and fails the commit phase with its error.
	// Lets crash-safety tests exercise the conditional-cleanup contract
	// (session + staged bytes must survive a failed commit) end-to-end.
	// Nil in production.
	commitFailHook func() error
}

// defaultCommitFailHook seeds kvfsDriver.commitFailHook in New. Nil unless
// a test harness sets it before driver construction.
var defaultCommitFailHook func() error

// New creates a new kvfs storage driver from the given config map.
func New(m map[string]interface{}, stream events.Stream, log *zerolog.Logger) (storage.FS, error) {
	opts, err := parseConfig(m)
	if err != nil {
		return nil, err
	}

	if log == nil {
		log = &zerolog.Logger{}
	}
	kvfsLog := log.With().Str("driver", "kvfs").Logger()
	log = &kvfsLog

	if !opts.S3ConfigComplete() {
		return nil, fmt.Errorf("kvfs: S3 configuration incomplete")
	}

	store, err := NewKVStore(opts, log)
	if err != nil {
		return nil, err
	}

	blob, err := NewS3Blobstore(opts.S3Endpoint, opts.S3Region, opts.S3Bucket, opts.S3AccessKey, opts.S3SecretKey, opts.BucketPrefix)
	if err != nil {
		store.Close()
		return nil, err
	}

	log.Info().
		Strs("nats_nodes", opts.NATSNodes).
		Str("s3_endpoint", opts.S3Endpoint).
		Str("s3_bucket", opts.S3Bucket).
		Msg("kvfs driver initialized")

	// Export the effective CAS retry bound per instance so dead config
	// (value diverging from the deployment setting) is observable.
	MaxCASRetriesGauge.WithLabelValues(opts.BucketPrefix).Set(float64(opts.MaxCASRetries))
	// Same dead-config seam for the recursive-delete depth bound.
	MaxDeleteDepthGauge.WithLabelValues(opts.BucketPrefix).Set(float64(resolveMaxDeleteDepth(opts)))

	// Initialise the upload-staging cache. Disk-backed (default) requires
	// the temp directory to exist; create it eagerly so the first
	// WriteChunk doesn't hit ENOENT under load. A NATS-backed alternative
	// is available as an opt-in for cross-pod resilience — see upload_cache.go.
	var uploadCache UploadCache
	switch opts.UploadBackend {
	case "", "disk":
		if err := os.MkdirAll(opts.UploadTmpDir, 0o700); err != nil {
			store.Close()
			return nil, errors.Wrapf(err, "kvfs: failed to create upload tmp dir %s", opts.UploadTmpDir)
		}
		uploadCache = newDiskUploadCache(opts.UploadTmpDir)
		log.Info().Str("upload_backend", "disk").Str("tmp_dir", opts.UploadTmpDir).Msg("kvfs upload cache initialized")
	case "nats":
		// Cross-pod transparent staging via JetStream. Survives pod death
		// + rolling upgrades at the cost of ~5-15 ms per chunk (vs ~1 ms
		// for disk). The stream is created at first use with R=1 file
		// storage by default.
		natsCache, err := newNATSStreamUploadCache(store.JetStream(), natsStreamUploadCacheOptions{
			StreamName:    opts.UploadNATSStream,
			SubjectPrefix: opts.UploadNATSSubjectPrefix,
			Storage:       opts.UploadNATSStorage,
			Replicas:      opts.UploadNATSReplicas,
			MaxAge:        opts.UploadNATSMaxAge,
			MaxBytes:      opts.UploadNATSMaxBytes,
			MaxChunkBytes: opts.UploadNATSMaxChunkBytes,
		}, log)
		if err != nil {
			store.Close()
			return nil, errors.Wrap(err, "kvfs: failed to initialise nats upload cache")
		}
		uploadCache = natsCache
		log.Info().
			Str("upload_backend", "nats").
			Str("stream", opts.UploadNATSStream).
			Str("subject_prefix", opts.UploadNATSSubjectPrefix).
			Str("storage", opts.UploadNATSStorage).
			Int("replicas", opts.UploadNATSReplicas).
			Dur("max_age", opts.UploadNATSMaxAge).
			Int64("max_bytes", opts.UploadNATSMaxBytes).
			Int("max_chunk_bytes", opts.UploadNATSMaxChunkBytes).
			Msg("kvfs upload cache initialized")
	default:
		return nil, fmt.Errorf("kvfs: unknown upload_backend %q (valid: disk, nats)", opts.UploadBackend)
	}

	d := &kvfsDriver{
		store:          store,
		blob:           blob,
		opts:           opts,
		log:            log,
		stream:         stream,
		uploadCache:    uploadCache,
		eventQueue:     make(chan eventJob, eventQueueSize),
		eventStop:      make(chan struct{}),
		commitFailHook: defaultCommitFailHook,
	}
	if d.commitFailHook != nil {
		log.Warn().Msg("kvfs: commit-fail test seam is active — FinishUpload will synthesise commit failures (testing only)")
	}
	go d.eventPublisher()

	// Background updater for the kvfs_temp_file_bytes_in_flight gauge.
	// Polls the disk cache's atomic counter every 10 s — cheap.
	if dc, ok := uploadCache.(*diskUploadCache); ok {
		go func() {
			t := time.NewTicker(10 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-d.eventStop:
					return
				case <-t.C:
					TempFileBytesInFlight.Set(float64(dc.BytesInFlight()))
				}
			}
		}()
	}
	// For the NATS-backed cache, the stream size is observable via
	// JetStream stream-info (`Bytes` field). Poll every 10 s and project
	// to the same gauge so dashboards work without changes.
	if nc, ok := uploadCache.(*natsStreamUploadCache); ok {
		go func() {
			t := time.NewTicker(10 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-d.eventStop:
					return
				case <-t.C:
					if info, err := nc.js.StreamInfo(nc.opts.StreamName); err == nil && info != nil {
						TempFileBytesInFlight.Set(float64(info.State.Bytes))
					}
				}
			}
		}()
	}

	// Upload-staleness sampler: refresh the oc-uploads gauges on every pod.
	// Kept separate from the GC sweep, which is lock-gated and runs once a
	// day — this read-only walk must run often and on every pod. Samples
	// once immediately so the gauges are populated at startup.
	go func() {
		sampleUploadAge := func() {
			uploads, err := d.store.ListAllUploads()
			if err != nil {
				d.log.Warn().Err(err).Msg("kvfs: upload-age sampler failed to list uploads")
				return
			}
			updateUploadAgeMetrics(opts.BucketPrefix, uploads)
		}
		sampleUploadAge()
		t := time.NewTicker(uploadAgeSampleInterval)
		defer t.Stop()
		for {
			select {
			case <-d.eventStop:
				return
			case <-t.C:
				sampleUploadAge()
			}
		}
	}()

	if opts.GCEnabled {
		d.gc = newBlobGC(store, blob, opts, log)

		// Root the sweep context on a driver-owned cancellable context so a
		// graceful Shutdown/SIGTERM aborts an in-flight sweep and releases the
		// gc-sweep lock immediately (the SIGTERM fast-path).
		gcCtx, gcCancel := context.WithCancel(context.Background())
		d.gcCancel = gcCancel
		d.gc.baseCtx = gcCtx

		// Reuse the ONE crash-safe deletion path for GC space reaping:
		// deleteSpaceContents (detached commit ctx) + DeleteSpace, identical
		// to DeleteStorageSpace. The GC never re-implements deletion.
		d.gc.reapSpace = func(ctx context.Context, spaceID, rootID string) error {
			cctx, cancel := d.commitPhase(ctx)
			defer cancel()
			// A depth-bound violation surfaces through the GC's error
			// accounting and the reap retries next cycle; the bound must
			// be raised for such a space to ever be reaped.
			if err := d.deleteSpaceContents(cctx, spaceID, rootID); err != nil {
				return err
			}
			return d.store.DeleteSpace(spaceID)
		}

		// Identity-orphan reaping needs a live-user signal. Build a resolver
		// from the service's gateway + service-account config. On any missing
		// config or construction error, degrade gracefully to a nil resolver:
		// identity reaping is disabled but the internal residue sweep still
		// runs. storage-system leaves these unset (no personal spaces).
		saIDs := collectServiceAccountIDs(opts.ServiceAccountID)
		if resolver, rerr := newCS3UserResolver(opts.GatewayAddr, opts.ServiceAccountID, opts.ServiceAccountSecret, saIDs, log); rerr != nil {
			log.Warn().Err(rerr).Msg("kvfs: GC identity reaping disabled (resolver unavailable); residue sweep still active")
		} else {
			d.gc.resolver = resolver
			log.Info().Str("gateway", opts.GatewayAddr).Int("service_accounts", len(saIDs)).Msg("kvfs: GC identity reaping enabled")
		}

		d.gc.Start()
		log.Info().
			Bool("dry_run", opts.GCDryRun).
			Str("interval", opts.GCInterval).
			Str("min_age", opts.GCMinAge).
			Bool("identity_reaping", d.gc.resolver != nil).
			Msg("blob garbage collection enabled")
	}

	return d, nil
}

// Shutdown gracefully shuts down the driver.
func (d *kvfsDriver) Shutdown(ctx context.Context) error {
	if d.eventStop != nil {
		close(d.eventStop)
	}
	if d.gc != nil {
		// Cancel the sweep context first so an in-flight sweep aborts and its
		// deferred ReleaseLock frees the gc-sweep lock now, then stop the loop.
		if d.gcCancel != nil {
			d.gcCancel()
		}
		d.gc.Stop()
	}
	d.store.Close()
	return nil
}

// maxCASRetries returns the driver's configured CAS retry bound.
func (d *kvfsDriver) maxCASRetries() int {
	return resolveMaxCASRetries(d.opts)
}

// collectServiceAccountIDs builds the deduplicated list of service account
// IDs that the GC resolver should treat as always-alive. Combines the
// storage-users service's own SA (from Options) with the cluster-wide list
// from SETTINGS_SERVICE_ACCOUNT_IDS (semicolon-separated env var).
func collectServiceAccountIDs(primaryID string) []string {
	seen := make(map[string]struct{})
	var ids []string
	add := func(id string) {
		id = strings.TrimSpace(id)
		if id == "" {
			return
		}
		if _, dup := seen[id]; dup {
			return
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	add(primaryID)
	for _, id := range strings.Split(os.Getenv("SETTINGS_SERVICE_ACCOUNT_IDS"), ";") {
		add(id)
	}
	return ids
}

// commitPhase derives a detached, timeout-bounded context for the durable
// mutation phase of a multi-step operation. Use the request ctx only for
// read-only validation (auth, quota, permissions); switch to the commit
// ctx as soon as the operation begins touching durable state (S3 blobs,
// NATS KV writes). Cancelling mid-step leaves orphaned / duplicated /
// phantom state that surfaces as PROPFIND/GET 404s. The 5-minute hard
// timeout prevents a wedged downstream from leaking the goroutine.
func (d *kvfsDriver) commitPhase(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), commitPhaseTimeout)
}

// lockParent acquires the per-parent in-process mutex. Returns the unlock
// function — callers should defer it. Cross-pod writers still race on the
// NATS CAS rev, but pod-local writers no longer thrash each other.
func (d *kvfsDriver) lockParent(spaceID, parentID string) func() {
	key := spaceID + "/" + parentID
	actual, _ := d.parentLocks.LoadOrStore(key, &sync.Mutex{})
	mu := actual.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// eventJob pairs the lazily-built event with a detached, value-preserving
// copy of the originating request context. The async worker publishes with
// that ctx so events.Publish can still derive traceParent + initiatorID
// (trace correlation + downstream self-notification suppression) — values the
// old synchronous path carried but a bare context.Background() would drop.
type eventJob struct {
	ctx context.Context
	evf func() interface{}
}

// publishEventAsync enqueues an event for the async publisher worker. If the
// queue is full (slow downstream consumer) the event is dropped and a
// counter increments — events are best-effort. The request ctx is detached
// via context.WithoutCancel so its values survive the request's lifetime
// without keeping it cancelable.
func (d *kvfsDriver) publishEventAsync(ctx context.Context, evf func() interface{}) {
	if d.stream == nil {
		return
	}
	job := eventJob{ctx: context.WithoutCancel(ctx), evf: evf}
	select {
	case d.eventQueue <- job:
		EventQueueDepth.Set(float64(len(d.eventQueue)))
	default:
		EventQueueDropped.Inc()
	}
}

// eventPublisher drains the event queue in the background. One worker is
// sufficient — events are small and the stream is fast under normal load.
func (d *kvfsDriver) eventPublisher() {
	for {
		select {
		case <-d.eventStop:
			return
		case job := <-d.eventQueue:
			EventQueueDepth.Set(float64(len(d.eventQueue)))
			d.doPublish(job.ctx, job.evf)
		}
	}
}

// doPublish builds the event lazily and publishes it, logging on error. Shared
// by the synchronous fallback (publishEvent) and the async worker so the
// build-or-skip + publish-or-log logic lives in exactly one place.
func (d *kvfsDriver) doPublish(ctx context.Context, evf func() interface{}) {
	ev := evf()
	if ev == nil {
		return
	}
	if err := events.Publish(ctx, d.stream, ev); err != nil {
		d.log.Error().Err(err).Msg("failed to publish event")
	}
}

// updateUploadAgeMetrics sets the oldest-age and session-count gauges from a
// snapshot of the instance's uploads bucket. Driven by the sampler goroutine
// in New. The prefix label keeps concurrent instances' samplers from
// overwriting each other on the shared registry.
func updateUploadAgeMetrics(prefix string, uploads []*UploadSession) {
	UploadSessionsTotal.WithLabelValues(prefix).Set(float64(len(uploads)))
	if len(uploads) == 0 {
		OldestUploadAgeSeconds.WithLabelValues(prefix).Set(0)
		return
	}
	now := time.Now().Unix()
	var oldest int64
	for _, u := range uploads {
		// createdAt is not tracked; approximate as Expires - uploadSessionTTL.
		var createdAt int64
		if u.Expires > 0 {
			createdAt = u.Expires - int64(uploadSessionTTL.Seconds())
		}
		if createdAt > 0 {
			age := now - createdAt
			if age > oldest {
				oldest = age
			}
		}
	}
	if oldest < 0 {
		oldest = 0
	}
	OldestUploadAgeSeconds.WithLabelValues(prefix).Set(float64(oldest))
}

// --- Space operations ---

// --- Read operations ---

// GetMD returns resource info for the referenced resource.
func (d *kvfsDriver) GetMD(ctx context.Context, ref *provider.Reference, mdKeys, fieldMask []string) (*provider.ResourceInfo, error) {
	spaceID, nodeID, node, rp, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.Stat })
	if err != nil {
		return nil, err
	}

	path, err := d.buildPath(ctx, spaceID, nodeID)
	if err != nil {
		path = node.Name
	}

	ri := node.ToResourceInfo(path)
	ri.PermissionSet = rp

	return ri, nil
}

// ListFolder returns resource infos for all children of the referenced resource.
func (d *kvfsDriver) ListFolder(ctx context.Context, ref *provider.Reference, mdKeys, fieldMask []string) ([]*provider.ResourceInfo, error) {
	spaceID, nodeID, _, rp, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.ListContainer })
	if err != nil {
		return nil, err
	}

	children, _, err := d.store.GetChildren(spaceID, nodeID)
	if err != nil {
		return nil, err
	}

	nodeIDs := make([]string, 0, len(children))
	for _, childID := range children {
		nodeIDs = append(nodeIDs, childID)
	}

	nodes, err := d.store.GetNodes(spaceID, nodeIDs)
	if err != nil {
		return nil, err
	}

	var result []*provider.ResourceInfo
	for name, childID := range children {
		child, ok := nodes[childID]
		if !ok {
			d.log.Warn().Str("child_id", childID).Msg("failed to get child node")
			continue
		}
		ri := child.ToResourceInfo(name)
		ri.PermissionSet = rp
		result = append(result, ri)
	}

	return result, nil
}

// Download returns a ReadCloser for the blob content of a file.
func (d *kvfsDriver) Download(ctx context.Context, ref *provider.Reference, openReaderFunc func(*provider.ResourceInfo) bool) (*provider.ResourceInfo, io.ReadCloser, error) {
	spaceID, _, node, _, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.InitiateFileDownload })
	if err != nil {
		return nil, nil, err
	}

	if node.Type != NodeTypeFile {
		return nil, nil, errtypes.BadRequest("cannot download a directory")
	}

	ri := node.ToResourceInfo(node.Name)

	if openReaderFunc != nil && !openReaderFunc(ri) {
		return ri, nil, nil
	}

	reader, err := d.blob.Download(ctx, BlobKey(spaceID, node.BlobID))
	if err != nil {
		return nil, nil, errors.Wrap(err, "kvfs: failed to download blob")
	}

	executant, _ := ctxpkg.ContextGetUser(ctx)
	d.publishEvent(ctx, func() interface{} {
		if executant == nil {
			return nil
		}
		return events.FileDownloaded{
			Executant: executant.Id,
			Ref:       spaceRef(spaceID, node.ID),
			Owner:     executant.Id,
			Timestamp: nowTimestamp(),
		}
	})

	return ri, reader, nil
}

// GetPathByID returns the path for the given resource ID relative to the space root.
func (d *kvfsDriver) GetPathByID(ctx context.Context, id *provider.ResourceId) (string, error) {
	return d.buildPath(ctx, id.SpaceId, id.OpaqueId)
}

// GetQuota returns the quota on the referenced resource.
func (d *kvfsDriver) GetQuota(ctx context.Context, ref *provider.Reference) (uint64, uint64, uint64, error) {
	spaceID := ref.GetResourceId().GetSpaceId()
	if spaceID == "" {
		return 0, 0, 0, errtypes.BadRequest("missing space ID")
	}

	space, _, err := d.store.GetSpace(spaceID)
	if err != nil {
		return 0, 0, 0, err
	}

	rootNode, _, err := d.store.GetNode(space.ID, space.RootID)
	if err != nil {
		return 0, 0, 0, err
	}

	totalBytes := uint64(space.Quota)
	if space.Quota < 0 {
		totalBytes = 0 // unlimited
	}
	usedBytes := uint64(rootNode.Size)
	remainingBytes := uint64(0)
	if totalBytes > usedBytes {
		remainingBytes = totalBytes - usedBytes
	}

	return totalBytes, usedBytes, remainingBytes, nil
}

func (d *kvfsDriver) checkQuota(spaceID string, newSize int64, isOverwrite bool, oldSize int64) error {
	space, _, err := d.store.GetSpace(spaceID)
	if err != nil {
		return err
	}
	if space.Quota <= 0 {
		return nil
	}
	rootNode, _, err := d.store.GetNode(space.ID, space.RootID)
	if err != nil {
		return err
	}

	quota := space.Quota
	used := rootNode.Size

	if isOverwrite {
		if newSize <= oldSize {
			return nil
		}
		netIncrease := newSize - oldSize
		if used+netIncrease > quota {
			return errtypes.InsufficientStorage("quota exceeded")
		}
		return nil
	}

	if used+newSize > quota || used > quota {
		return errtypes.InsufficientStorage("quota exceeded")
	}
	return nil
}

// --- Write operations ---

// CreateDir creates a new directory.
func (d *kvfsDriver) CreateDir(ctx context.Context, ref *provider.Reference) error {
	spaceID, parentID, name, err := d.resolveParentRef(ctx, ref)
	if err != nil {
		return err
	}

	parentNode, _, err := d.store.GetNode(spaceID, parentID)
	if err != nil {
		if err == ErrNodeNotFound {
			return errtypes.NotFound(ref.String())
		}
		return err
	}
	rp := d.assemblePermissions(ctx, spaceID, parentNode)
	if !rp.CreateContainer {
		if rp.Stat {
			return errtypes.PermissionDenied(ref.String())
		}
		return errtypes.NotFound(ref.String())
	}

	u := ctxpkg.ContextMustGetUser(ctx)

	// Detached commit context — three NATS bucket touches (PutNode root,
	// PutChildren root, PutChildren parent) must run to completion or
	// leave consistent retryable half-state. See [kvfsDriver.commitPhase].
	commitCtx, cancel := d.commitPhase(ctx)
	defer cancel()

	var newID string
	if err := d.casRetryLoop("mkdir", func() error {
		children, rev, err := d.store.GetChildren(spaceID, parentID)
		if err != nil {
			return err
		}

		if _, exists := children[name]; exists {
			return errtypes.AlreadyExists(name)
		}

		newID = uuid.New().String()
		now := time.Now().UnixNano()

		node := &NodeEntry{
			ID:       newID,
			SpaceID:  spaceID,
			ParentID: parentID,
			Name:     name,
			Type:     NodeTypeDir,
			MTime:    now,
			ETag:     calculateEtag(newID, now),
			Owner:    u.Id.OpaqueId,
			MimeType: "httpd/unix-directory",
		}
		if err := d.store.PutNode(node, 0); err != nil {
			return errors.Wrap(err, "kvfs: failed to create directory node")
		}

		if err := d.store.PutChildren(spaceID, newID, ChildMap{}, 0); err != nil {
			d.store.DeleteNode(spaceID, newID)
			return errors.Wrap(err, "kvfs: failed to create directory children")
		}

		children[name] = newID
		if err := d.store.PutChildren(spaceID, parentID, children, rev); err != nil {
			d.store.DeleteNode(spaceID, newID)
			d.store.DeleteChildren(spaceID, newID)
			return err
		}
		return nil
	}); err != nil {
		if err == ErrCASConflict {
			return errors.New("kvfs: CreateDir failed after max CAS retries")
		}
		return err
	}

	d.propagateTreeSize(commitCtx, spaceID, parentID, 0)

	d.publishEvent(commitCtx, func() interface{} {
		return events.ContainerCreated{
			SpaceOwner: u.Id,
			Executant:  u.Id,
			Ref:        spaceRef(spaceID, newID),
			ParentID:   spaceResourceID(spaceID, parentID),
			Owner:      u.Id,
			Timestamp:  nowTimestamp(),
		}
	})

	return nil
}

// CreateReference creates a resource of type reference.
func (d *kvfsDriver) CreateReference(ctx context.Context, path string, targetURI *url.URL) error {
	return errtypes.NotSupported("kvfs: CreateReference not supported")
}

// TouchFile sets the mtime of a resource, creating an empty file if it does not exist.
func (d *kvfsDriver) TouchFile(ctx context.Context, ref *provider.Reference, markprocessing bool, mtimeStr string) error {
	spaceID, nodeID, err := d.resolveRef(ctx, ref)
	if err != nil {
		return d.createEmptyFileAuthorized(ctx, ref)
	}

	node, rev, err := d.store.GetNode(spaceID, nodeID)
	if err != nil {
		return d.createEmptyFileAuthorized(ctx, ref)
	}

	rp := d.assemblePermissions(ctx, spaceID, node)
	if !rp.InitiateFileUpload {
		if rp.Stat {
			return errtypes.PermissionDenied(ref.String())
		}
		return errtypes.NotFound(ref.String())
	}

	// Detached commit context for the touch + propagate side-effects.
	// See [kvfsDriver.commitPhase].
	commitCtx, cancel := d.commitPhase(ctx)
	defer cancel()

	now := time.Now().UnixNano()
	node.MTime = now
	node.ETag = calculateEtag(node.ID, now)
	if markprocessing {
		node.Processing = true
	}

	if err := d.store.PutNode(node, rev); err != nil {
		return err
	}

	if node.ParentID != "" {
		d.propagateTreeSize(commitCtx, spaceID, node.ParentID, 0)
	}

	executant, _ := ctxpkg.ContextGetUser(ctx)
	d.publishEvent(commitCtx, func() interface{} {
		if executant == nil {
			return nil
		}
		return events.FileUploaded{
			SpaceOwner: executant.Id,
			Executant:  executant.Id,
			Ref:        spaceRef(spaceID, nodeID),
			Owner:      executant.Id,
			Timestamp:  nowTimestamp(),
		}
	})

	return nil
}

// Delete moves a resource to the trash.
func (d *kvfsDriver) Delete(ctx context.Context, ref *provider.Reference) error {
	spaceID, nodeID, node, _, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.Delete })
	if err != nil {
		return err
	}

	if node.ParentID == "" {
		return errtypes.BadRequest("cannot delete space root")
	}

	// Build original path before any mutations
	originalPath, _ := d.buildPath(ctx, spaceID, nodeID)

	// Detached commit context: from here on, durable state mutations must
	// run to completion regardless of client cancellation. See
	// [kvfsDriver.commitPhase].
	commitCtx, cancel := d.commitPhase(ctx)
	defer cancel()

	// Write the trash entry first (intent record). If we crash after this
	// but before removing from the parent, the node appears in both the tree
	// and the trash — harmless and self-correcting on the next delete attempt.
	trashEntry := &TrashEntry{
		Key:          uuid.New().String(),
		NodeID:       nodeID,
		SpaceID:      spaceID,
		OriginalPath: originalPath,
		DeletionTime: time.Now().Unix(),
		Node:         *node,
	}
	if err := d.store.PutTrash(trashEntry); err != nil {
		return err
	}

	// Remove from parent's children map
	if err := d.updateChildrenWithCAS(spaceID, node.ParentID, func(children ChildMap) {
		delete(children, node.Name)
	}); err != nil {
		return err
	}

	// For files: delete the node (the trash snapshot is sufficient for restore).
	// For directories: keep nodes and children maps alive so that the entire
	// subtree can be restored. The nodes are no longer reachable via the tree
	// (removed from parent's children above) but remain in KV for restore/purge.
	if node.Type != NodeTypeDir {
		d.store.DeleteNode(spaceID, nodeID)
	}

	// Propagate size decrease + MTime/ETag to all ancestors.
	d.propagateTreeSize(commitCtx, spaceID, node.ParentID, -node.Size)

	executant, _ := ctxpkg.ContextGetUser(ctx)
	d.publishEvent(commitCtx, func() interface{} {
		if executant == nil {
			return nil
		}
		return events.ItemTrashed{
			SpaceOwner: executant.Id,
			Executant:  executant.Id,
			ID:         spaceResourceID(spaceID, nodeID),
			Ref:        spaceRef(spaceID, nodeID),
			Owner:      executant.Id,
			Timestamp:  nowTimestamp(),
		}
	})

	return nil
}

// Upload creates or updates a resource with new content.
func (d *kvfsDriver) Upload(ctx context.Context, req storage.UploadRequest, uff storage.UploadFinishedFunc) (*provider.ResourceInfo, error) {
	ref := req.Ref

	var ifMatchEtag, tusSibling string
	if ref.GetResourceId() == nil || ref.GetResourceId().GetSpaceId() == "" {
		var parsedRef *provider.Reference
		var err error
		parsedRef, ifMatchEtag, tusSibling, err = d.parseUploadPath(ref.GetPath())
		if err != nil {
			return nil, err
		}
		ref = parsedRef
	}

	spaceID, parentID, name, err := d.resolveParentRef(ctx, ref)
	if err != nil {
		return nil, err
	}

	u := ctxpkg.ContextMustGetUser(ctx)
	if u == nil {
		return nil, errtypes.InternalError("kvfs: no user in context")
	}

	parentNode, _, err := d.store.GetNode(spaceID, parentID)
	if err != nil {
		if err == ErrNodeNotFound {
			return nil, errtypes.NotFound(ref.String())
		}
		return nil, err
	}
	rp := d.assemblePermissions(ctx, spaceID, parentNode)
	if !rp.InitiateFileUpload {
		// For overwrites, check the existing file's permissions (file-level grants
		// from OCS shares are on the file node, not the parent).
		allowed := false
		children, _, _ := d.store.GetChildren(spaceID, parentID)
		if existingID, ok := children[name]; ok {
			if existingNode, _, err := d.store.GetNode(spaceID, existingID); err == nil {
				fileRP := d.assemblePermissions(ctx, spaceID, existingNode)
				if fileRP.InitiateFileUpload {
					allowed = true
				} else if fileRP.Stat {
					return nil, errtypes.PermissionDenied(ref.String())
				}
			}
		}
		if !allowed {
			if rp.Stat {
				return nil, errtypes.PermissionDenied(ref.String())
			}
			return nil, errtypes.NotFound(ref.String())
		}
	}

	// Quota check before uploading blob to S3
	{
		var oldFileSize int64
		isOverwrite := false
		qChildren, _, _ := d.store.GetChildren(spaceID, parentID)
		if existingID, exists := qChildren[name]; exists {
			isOverwrite = true
			if existingNode, _, err := d.store.GetNode(spaceID, existingID); err == nil {
				oldFileSize = existingNode.Size
			}
		}
		if err := d.checkQuota(spaceID, req.Length, isOverwrite, oldFileSize); err != nil {
			return nil, err
		}
	}

	blobID := uuid.New().String()

	// Detached commit context: from blob upload through commitFileNode,
	// the request ctx must not be allowed to cancel mid-operation —
	// see [kvfsDriver.commitPhase].
	commitCtx, cancel := d.commitPhase(ctx)
	defer cancel()

	// Upload blob to S3, computing SHA-1 checksum while streaming.
	// Note: req.Body still reads from the original ctx-bound stream;
	// commitCtx only governs the outbound S3 PUT side.
	hasher := sha1.New()
	teeBody := io.TeeReader(req.Body, hasher)
	d.log.Debug().Str("blob_key", BlobKey(spaceID, blobID)).Int64("length", req.Length).Msg("Upload: uploading blob to S3")
	if err := d.blob.Upload(commitCtx, BlobKey(spaceID, blobID), teeBody, req.Length); err != nil {
		d.log.Error().Err(err).Msg("Upload: blob upload failed")
		return nil, errors.Wrap(err, "kvfs: failed to upload blob")
	}
	checksum := "sha1:" + hex.EncodeToString(hasher.Sum(nil))

	mimeType := mime.Detect(false, name)

	UploadInFlight.WithLabelValues("simple").Inc()
	defer UploadInFlight.WithLabelValues("simple").Dec()

	nodeID, err := d.commitFileNode(commitCtx, commitFileParams{
		SpaceID:     spaceID,
		ParentID:    parentID,
		Name:        name,
		BlobID:      blobID,
		Size:        req.Length,
		Checksum:    checksum,
		MimeType:    mimeType,
		IfMatchEtag: ifMatchEtag,
		OwnerID:     u.Id.OpaqueId,
	})
	if err != nil {
		// Rollback the blob so we don't leave an orphan in S3. Use
		// commitCtx so this cleanup isn't itself short-circuited by a
		// canceled request ctx.
		if delErr := d.blob.Delete(commitCtx, BlobKey(spaceID, blobID)); delErr != nil {
			d.log.Warn().Err(delErr).Str("blob_id", blobID).Msg("Upload: rollback blob delete failed — leaving for GC")
		}
		return nil, err
	}

	// The simple commit succeeded, so the TUS session minted by the same
	// InitiateUpload will never be used — reap it now instead of leaving
	// it for the TTL reaper. Best-effort: DeleteUpload tolerates a
	// missing key, and on failure the reaper still collects it.
	if tusSibling != "" {
		if delErr := d.store.DeleteUpload(tusSibling); delErr != nil {
			d.log.Warn().Err(delErr).Str("session_id", tusSibling).Msg("Upload: failed to reap sibling TUS session — leaving for the TTL reaper")
		} else {
			UploadInFlight.WithLabelValues("tus").Dec()
		}
	}

	d.publishEvent(commitCtx, func() interface{} {
		return events.FileUploaded{
			SpaceOwner: u.Id,
			Executant:  u.Id,
			Ref:        spaceRef(spaceID, nodeID),
			Owner:      u.Id,
			Timestamp:  nowTimestamp(),
		}
	})

	// NATS JetStream KV's materialized view can lag the stream commit by a
	// short window under hot-parent concurrent writes. Without this retry
	// the next line dereferences a nil node and per-request recover()
	// returns 500 with a panic stack trace — clean error is better.
	node, _, err := d.store.GetNode(spaceID, nodeID)
	if err != nil || node == nil {
		for retry := 0; retry < 5; retry++ {
			time.Sleep(time.Duration(10*(1<<retry)) * time.Millisecond)
			if n, _, e := d.store.GetNode(spaceID, nodeID); e == nil && n != nil {
				node = n
				err = nil
				break
			}
		}
	}
	if node == nil {
		return nil, errors.Wrap(err, "kvfs: node not visible after commit")
	}
	ri := node.ToResourceInfo(name)
	return ri, nil
}

func (d *kvfsDriver) commitFileNode(ctx context.Context, p commitFileParams) (string, error) {
	now := time.Now().UnixNano()

	// First decide: overwrite of an existing entry, or create of a new one?
	// We probe the children map once; the overwrite path still uses a node-CAS
	// retry loop (it must touch a specific existing node revision). The create
	// path delegates to applyChildIntent which handles the children-map CAS
	// internally with merge-on-conflict, so concurrent same-parent uploads no
	// longer thrash on a full re-read of the children map.

	children, _, err := d.store.GetChildren(p.SpaceID, p.ParentID)
	if err != nil {
		return "", err
	}

	if existingID, exists := children[p.Name]; exists {
		// Idempotency guard: when a prior FinishUpload committed but the
		// HTTP response was lost (504), the client retries with a new
		// session carrying the old If-Match etag. The node's etag moved
		// forward so the precondition check would fail. Detect by content
		// identity: same checksum + size means the desired state is
		// already achieved. Return success without re-versioning or
		// re-propagating tree size.
		if p.IfMatchEtag != "" && p.Checksum != "" {
			if node, _, err := d.store.GetNode(p.SpaceID, existingID); err == nil {
				if node.ETag != p.IfMatchEtag &&
					node.Checksum == p.Checksum &&
					node.Size == p.Size {
					IdempotentOverwriteDetected.Inc()
					d.log.Info().
						Str("node_id", existingID).
						Str("checksum", p.Checksum).
						Int64("size", p.Size).
						Str("stale_etag", p.IfMatchEtag).
						Str("current_etag", node.ETag).
						Msg("kvfs: idempotent overwrite — content already matches")
					return existingID, nil
				}
			}
		}

		// Overwrite path: CAS-update the existing node's blob/size/etag.
		// Children map is unchanged so no children CAS is needed. The version
		// snapshot is taken once (firstTry), on the first attempt only, so a CAS
		// retry never creates duplicate version entries.
		firstTry := true
		var sizeDelta int64
		if err := d.casRetryLoop("upload", func() error {
			existingNode, nodeRev, err := d.store.GetNode(p.SpaceID, existingID)
			if err != nil {
				return err
			}

			if p.IfMatchEtag != "" && existingNode.ETag != p.IfMatchEtag {
				return errtypes.Aborted("if-match etag mismatch")
			}

			if !d.opts.DisableVersioning && existingNode.BlobID != "" && firstTry {
				version := &VersionEntry{
					Key:      uuid.New().String(),
					NodeID:   existingID,
					SpaceID:  p.SpaceID,
					BlobID:   existingNode.BlobID,
					BlobSize: existingNode.BlobSize,
					MTime:    existingNode.MTime,
					ETag:     existingNode.ETag,
					Size:     existingNode.Size,
					Checksum: existingNode.Checksum,
				}
				if err := d.store.PutVersion(version); err != nil {
					d.log.Warn().Err(err).Str("node_id", existingID).Msg("failed to create version entry")
				}
				d.trimVersions(ctx, p.SpaceID, existingID)
			}
			firstTry = false

			sizeDelta = p.Size - existingNode.Size
			existingNode.BlobID = p.BlobID
			existingNode.BlobSize = p.Size
			existingNode.Size = p.Size
			existingNode.MTime = now
			existingNode.ETag = calculateEtag(existingID, now)
			existingNode.MimeType = p.MimeType
			existingNode.Checksum = p.Checksum
			existingNode.Processing = false

			return d.store.PutNode(existingNode, nodeRev)
		}); err != nil {
			if err == ErrCASConflict {
				return "", errors.New("kvfs: file overwrite failed after max CAS retries")
			}
			return "", err
		}

		// propagateTreeSize updates Size + MTime + ETag on the parent
		// and every ancestor via propagateOneAncestor.
		d.propagateTreeSize(ctx, p.SpaceID, p.ParentID, sizeDelta)
		return existingID, nil
	}

	// Create path: allocate the node first, then atomically add to parent's
	// children map via the intent helper.
	//
	// PutNode is retried with a fresh UUID on CAS conflict — UUID collisions
	// are vanishingly rare in practice but the unit suite exercises this
	// path via mock injection, so we defend it.
	var nodeID string
	if err := d.casRetryLoop("upload", func() error {
		nodeID = uuid.New().String()
		node := &NodeEntry{
			ID: nodeID, SpaceID: p.SpaceID, ParentID: p.ParentID,
			Name: p.Name, Type: NodeTypeFile,
			BlobID: p.BlobID, BlobSize: p.Size, Size: p.Size,
			MTime: now, ETag: calculateEtag(nodeID, now),
			Owner: p.OwnerID, MimeType: p.MimeType, Checksum: p.Checksum,
		}
		return d.store.PutNode(node, 0)
	}); err != nil {
		if err == ErrCASConflict {
			return "", errors.New("kvfs: file commit failed after max CAS retries")
		}
		return "", err
	}

	if _, err := d.applyChildIntent(p.SpaceID, p.ParentID, map[string]string{p.Name: nodeID}, nil); err != nil {
		d.store.DeleteNode(p.SpaceID, nodeID)
		CASExhausted.WithLabelValues("upload").Inc()
		return "", err
	}

	// propagateTreeSize covers the parent and all ancestors.
	d.propagateTreeSize(ctx, p.SpaceID, p.ParentID, p.Size)
	return nodeID, nil
}

// InitiateUpload returns protocols for initiating an upload.
// Returns both "simple" (PUT) and "tus" (resumable) protocols.
func (d *kvfsDriver) InitiateUpload(ctx context.Context, ref *provider.Reference, uploadLength int64, metadata map[string]string) (map[string]string, error) {
	rid := ref.GetResourceId()
	if rid == nil || rid.SpaceId == "" {
		return nil, errtypes.BadRequest("kvfs: InitiateUpload requires a resource ID with space ID")
	}

	formattedRef := storagespace.FormatResourceID(rid)
	relPath := ref.GetPath()
	if relPath == "" {
		relPath = "."
	}

	// Direct file reference (e.g. from sharesstorageprovider for file-level
	// shares): OpaqueId points to a file node, path is ".".  URL path
	// normalization strips "/.", so re-encode as space-root + full path.
	if (relPath == "." || relPath == "/") && rid.OpaqueId != "" && rid.OpaqueId != rid.SpaceId {
		if fullPath, err := d.buildPath(ctx, rid.SpaceId, rid.OpaqueId); err == nil && fullPath != "" {
			rootID := &provider.ResourceId{StorageId: rid.StorageId, SpaceId: rid.SpaceId, OpaqueId: rid.SpaceId}
			formattedRef = storagespace.FormatResourceID(rootID)
			relPath = fullPath
		}
	}

	var ifMatchEtag string
	if ifMatch, ok := metadata["if-match"]; ok && ifMatch != "" {
		ifMatchEtag = ifMatch
	}

	// Simple protocol: encode ref + etag in the upload ID
	simpleID := formattedRef + "/" + relPath
	if ifMatchEtag != "" {
		simpleID += "?if-match=" + url.QueryEscape(ifMatchEtag)
	}

	result := map[string]string{
		"simple": simpleID,
	}

	// TUS protocol: create a persistent upload session backed by S3 multipart
	spaceID := rid.SpaceId
	_, parentID, name, err := d.resolveParentRef(ctx, ref)
	if err != nil {
		d.log.Warn().Err(err).Msg("InitiateUpload: could not resolve parent for TUS session, TUS protocol unavailable")
		return result, nil
	}

	parentNode, _, err := d.store.GetNode(spaceID, parentID)
	if err == nil {
		rp := d.assemblePermissions(ctx, spaceID, parentNode)
		if !rp.InitiateFileUpload {
			// Parent doesn't grant write. Check if the target file has a
			// direct grant (OCS file shares grant on the file, not parent).
			allowed := false
			children, _, _ := d.store.GetChildren(spaceID, parentID)
			if existingID, ok := children[name]; ok {
				if fileNode, _, err := d.store.GetNode(spaceID, existingID); err == nil {
					fileRP := d.assemblePermissions(ctx, spaceID, fileNode)
					if fileRP.InitiateFileUpload {
						allowed = true
					} else if fileRP.Stat {
						return nil, errtypes.PermissionDenied(ref.String())
					}
				}
			}
			if !allowed {
				return nil, errtypes.PermissionDenied(ref.String())
			}
		}
	}

	// Quota check: early rejection before creating TUS session
	if uploadLength > 0 {
		var oldFileSize int64
		isOverwrite := false
		qChildren, _, _ := d.store.GetChildren(spaceID, parentID)
		if existingID, exists := qChildren[name]; exists {
			isOverwrite = true
			if existingNode, _, err := d.store.GetNode(spaceID, existingID); err == nil {
				oldFileSize = existingNode.Size
			}
		}
		if err := d.checkQuota(spaceID, uploadLength, isOverwrite, oldFileSize); err != nil {
			return nil, err
		}
	}

	tusID, err := d.createTUSSession(ctx, spaceID, parentID, name, uploadLength, uploadLength == 0, ifMatchEtag)
	if err != nil {
		d.log.Warn().Err(err).Msg("InitiateUpload: failed to create TUS session, TUS protocol unavailable")
		return result, nil
	}

	result["tus"] = tusID

	// The caller picks ONE of the offered protocols, but the TUS session
	// above is already persisted. Thread its ID through the simple ID so
	// a simple-PUT commit can reap its never-used sibling — otherwise
	// every simple upload strands one session until the TTL reaper runs.
	simpleSep := "?"
	if ifMatchEtag != "" {
		simpleSep = "&"
	}
	result["simple"] = simpleID + simpleSep + "tus-sibling=" + url.QueryEscape(tusID)

	return result, nil
}

// parseUploadPath decodes an upload path encoded by InitiateUpload.
// Format: "storageId$spaceId!opaqueId/relativePath[?if-match=etag&tus-sibling=id]"
// Returns the reference, an optional if-match etag for CAS enforcement, and
// the ID of the sibling TUS session minted alongside the simple ID (so the
// simple commit can reap it).
func (d *kvfsDriver) parseUploadPath(p string) (*provider.Reference, string, string, error) {
	// The simple handler prefixes with "/", clean it
	p = strings.TrimPrefix(p, "/")

	// Extract query parameters before parsing the path
	var ifMatchEtag, tusSibling string
	if idx := strings.Index(p, "?"); idx >= 0 {
		query, err := url.ParseQuery(p[idx+1:])
		if err == nil {
			ifMatchEtag = query.Get("if-match")
			tusSibling = query.Get("tus-sibling")
		}
		p = p[:idx]
	}

	// Split on first "/" to separate spaceRef from file path
	parts := strings.SplitN(p, "/", 2)
	if len(parts) < 2 {
		return nil, "", "", errtypes.BadRequest("kvfs: invalid upload path format: " + p)
	}

	idPart := parts[0]
	filePath := parts[1]

	rid, err := storagespace.ParseID(idPart)
	if err != nil {
		return nil, "", "", errtypes.BadRequest("kvfs: failed to parse space ID from upload path: " + err.Error())
	}

	return &provider.Reference{
		ResourceId: &rid,
		Path:       utils.MakeRelativePath(filePath),
	}, ifMatchEtag, tusSibling, nil
}

// --- Revisions ---

func (d *kvfsDriver) ListRevisions(ctx context.Context, ref *provider.Reference) ([]*provider.FileVersion, error) {
	spaceID, nodeID, _, _, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.ListFileVersions })
	if err != nil {
		return nil, err
	}

	versions, err := d.store.ListVersions(spaceID, nodeID)
	if err != nil {
		return nil, err
	}

	var result []*provider.FileVersion
	for _, v := range versions {
		result = append(result, &provider.FileVersion{
			Key:   v.Key,
			Size:  uint64(v.Size),
			Mtime: uint64(v.MTime / int64(time.Second)),
			Etag:  v.ETag,
		})
	}
	return result, nil
}

func (d *kvfsDriver) DownloadRevision(ctx context.Context, ref *provider.Reference, key string, openReaderFunc func(*provider.ResourceInfo) bool) (*provider.ResourceInfo, io.ReadCloser, error) {
	spaceID, nodeID, _, _, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool {
		return rp.ListFileVersions && rp.InitiateFileDownload
	})
	if err != nil {
		return nil, nil, err
	}

	version, err := d.store.GetVersion(spaceID, nodeID, key)
	if err != nil {
		return nil, nil, err
	}

	ri := &provider.ResourceInfo{
		Size:  uint64(version.Size),
		Etag:  version.ETag,
		Mtime: &types.Timestamp{Seconds: uint64(version.MTime / int64(time.Second))},
	}

	if openReaderFunc != nil && !openReaderFunc(ri) {
		return ri, nil, nil
	}

	reader, err := d.blob.Download(ctx, BlobKey(spaceID, version.BlobID))
	if err != nil {
		return nil, nil, err
	}

	return ri, reader, nil
}

func (d *kvfsDriver) RestoreRevision(ctx context.Context, ref *provider.Reference, key string) error {
	spaceID, nodeID, authNode, _, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.RestoreFileVersion })
	if err != nil {
		return err
	}
	if err := d.checkNodeLock(ctx, authNode); err != nil {
		return err
	}

	version, err := d.store.GetVersion(spaceID, nodeID, key)
	if err != nil {
		return err
	}

	node, rev, err := d.store.GetNode(spaceID, nodeID)
	if err != nil {
		return err
	}

	// Detached commit context — PutVersion + trimVersions + PutNode +
	// propagateTreeSize must complete atomically; partial restoration
	// leaves a node pointing at the wrong blob or a missing-version
	// dangling reference. See [kvfsDriver.commitPhase].
	commitCtx, cancel := d.commitPhase(ctx)
	defer cancel()

	// Save current as new version
	if !d.opts.DisableVersioning {
		currentVersion := &VersionEntry{
			Key:      uuid.New().String(),
			NodeID:   nodeID,
			SpaceID:  spaceID,
			BlobID:   node.BlobID,
			BlobSize: node.BlobSize,
			MTime:    node.MTime,
			ETag:     node.ETag,
			Size:     node.Size,
			Checksum: node.Checksum,
		}
		if err := d.store.PutVersion(currentVersion); err != nil {
			d.log.Warn().Err(err).Str("node_id", nodeID).Msg("failed to create version entry")
		}
		d.trimVersions(commitCtx, spaceID, nodeID)
	}

	sizeDelta := version.Size - node.Size
	node.BlobID = version.BlobID
	node.BlobSize = version.BlobSize
	node.Size = version.Size
	node.Checksum = version.Checksum
	node.MTime = time.Now().UnixNano()
	node.ETag = calculateEtag(nodeID, node.MTime)

	if err := d.store.PutNode(node, rev); err != nil {
		return err
	}

	d.propagateTreeSize(commitCtx, spaceID, node.ParentID, sizeDelta)

	executant, _ := ctxpkg.ContextGetUser(ctx)
	d.publishEvent(commitCtx, func() interface{} {
		if executant == nil {
			return nil
		}
		return events.FileVersionRestored{
			SpaceOwner: executant.Id,
			Executant:  executant.Id,
			Ref:        spaceRef(spaceID, nodeID),
			Owner:      executant.Id,
			Key:        key,
			Timestamp:  nowTimestamp(),
		}
	})

	return nil
}

func (d *kvfsDriver) trimVersions(ctx context.Context, spaceID, nodeID string) {
	if d.opts.MaxVersions <= 0 {
		return
	}

	versions, err := d.store.ListVersions(spaceID, nodeID)
	if err != nil {
		d.log.Warn().Err(err).Str("node_id", nodeID).Msg("trimVersions: failed to list versions")
		return
	}

	if len(versions) <= d.opts.MaxVersions {
		return
	}

	sort.Slice(versions, func(i, j int) bool {
		return versions[i].MTime > versions[j].MTime
	})

	for _, v := range versions[d.opts.MaxVersions:] {
		if v.BlobID != "" {
			d.blob.Delete(ctx, BlobKey(spaceID, v.BlobID))
		}
		d.store.DeleteVersion(spaceID, nodeID, v.Key)
	}
}

// --- Recycle bin ---

// --- Grants ---

// --- Arbitrary Metadata ---

func (d *kvfsDriver) SetArbitraryMetadata(ctx context.Context, ref *provider.Reference, md *provider.ArbitraryMetadata) error {
	spaceID, nodeID, _, _, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.InitiateFileUpload })
	if err != nil {
		return err
	}
	return d.putNodeWithCASCheck(spaceID, nodeID, func(node *NodeEntry) error {
		if err := d.checkNodeLock(ctx, node); err != nil {
			return err
		}
		if node.Metadata == nil {
			node.Metadata = make(map[string]string)
		}
		for k, v := range md.Metadata {
			node.Metadata[k] = v
		}
		return nil
	})
}

func (d *kvfsDriver) UnsetArbitraryMetadata(ctx context.Context, ref *provider.Reference, keys []string) error {
	spaceID, nodeID, _, _, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.InitiateFileUpload })
	if err != nil {
		return err
	}
	return d.putNodeWithCASCheck(spaceID, nodeID, func(node *NodeEntry) error {
		if err := d.checkNodeLock(ctx, node); err != nil {
			return err
		}
		for _, k := range keys {
			delete(node.Metadata, k)
		}
		return nil
	})
}

// --- Labels (favorites) ---

// Only "favorite" is supported, matching decomposedfs semantics.
const labelFavorite = "favorite"

func (d *kvfsDriver) AddLabel(ctx context.Context, ref *provider.Reference, userID *user.UserId, label string) error {
	if label != labelFavorite {
		return errtypes.BadRequest("unsupported label: " + label)
	}
	spaceID, nodeID, _, _, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.Stat })
	if err != nil {
		return err
	}
	return d.putNodeWithCASCheck(spaceID, nodeID, func(node *NodeEntry) error {
		for _, fav := range node.Favorites {
			if fav == userID.OpaqueId {
				return errNoChange
			}
		}
		node.Favorites = append(node.Favorites, userID.OpaqueId)
		return nil
	})
}

func (d *kvfsDriver) RemoveLabel(ctx context.Context, ref *provider.Reference, userID *user.UserId, label string) error {
	if label != labelFavorite {
		return errtypes.BadRequest("unsupported label: " + label)
	}
	spaceID, nodeID, _, _, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.Stat })
	if err != nil {
		return err
	}
	return d.putNodeWithCASCheck(spaceID, nodeID, func(node *NodeEntry) error {
		for i, fav := range node.Favorites {
			if fav == userID.OpaqueId {
				node.Favorites = append(node.Favorites[:i], node.Favorites[i+1:]...)
				return nil
			}
		}
		return errNoChange
	})
}

// --- Locks ---

func (d *kvfsDriver) GetLock(ctx context.Context, ref *provider.Reference) (*provider.Lock, error) {
	_, _, node, _, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.InitiateFileDownload })
	if err != nil {
		return nil, err
	}

	// Return nil for expired locks
	if node.Lock != nil && node.Lock.Expiry > 0 && time.Now().Unix() > node.Lock.Expiry {
		return nil, nil
	}

	return node.ToLock(), nil
}

func (d *kvfsDriver) SetLock(ctx context.Context, ref *provider.Reference, lock *provider.Lock) error {
	spaceID, nodeID, _, _, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.InitiateFileUpload })
	if err != nil {
		return err
	}
	return d.putNodeWithCASCheck(spaceID, nodeID, func(node *NodeEntry) error {
		if node.Lock != nil && node.Lock.LockID != "" {
			if node.Lock.Expiry > 0 && time.Now().Unix() > node.Lock.Expiry {
				node.Lock = nil
			} else {
				return errtypes.Locked("resource is already locked")
			}
		}
		node.Lock = &LockEntry{
			LockID:  lock.LockId,
			Type:    int(lock.Type),
			AppName: lock.AppName,
		}
		if lock.User != nil {
			node.Lock.UserID = lock.User.OpaqueId
		}
		if lock.Expiration != nil {
			node.Lock.Expiry = int64(lock.Expiration.Seconds)
		}
		return nil
	})
}

func (d *kvfsDriver) RefreshLock(ctx context.Context, ref *provider.Reference, lock *provider.Lock, existingLockID string) error {
	spaceID, nodeID, _, _, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.InitiateFileUpload })
	if err != nil {
		return err
	}
	return d.putNodeWithCASCheck(spaceID, nodeID, func(node *NodeEntry) error {
		if node.Lock == nil || node.Lock.LockID != existingLockID {
			return errtypes.PreconditionFailed("lock ID mismatch")
		}
		node.Lock.LockID = lock.LockId
		if lock.Expiration != nil {
			node.Lock.Expiry = int64(lock.Expiration.Seconds)
		}
		return nil
	})
}

func (d *kvfsDriver) Unlock(ctx context.Context, ref *provider.Reference, lock *provider.Lock) error {
	spaceID, nodeID, _, _, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.InitiateFileUpload })
	if err != nil {
		return err
	}
	return d.putNodeWithCASCheck(spaceID, nodeID, func(node *NodeEntry) error {
		if node.Lock == nil || node.Lock.LockID != lock.LockId {
			return errtypes.PreconditionFailed("lock ID mismatch")
		}
		node.Lock = nil
		return nil
	})
}

// --- Home (deprecated) ---

func (d *kvfsDriver) CreateHome(ctx context.Context) error {
	return nil // spaces replace homes
}

func (d *kvfsDriver) GetHome(ctx context.Context) (string, error) {
	// Return "/" so the gateway can resolve the personal space via WebdavNamespace
	// template. The actual space resolution happens via ListStorageSpaces.
	return "/", nil
}

// --- Internal helpers ---

func (d *kvfsDriver) createEmptyFileAuthorized(ctx context.Context, ref *provider.Reference) error {
	spaceID, parentID, _, err := d.resolveParentRef(ctx, ref)
	if err != nil {
		return err
	}
	parentNode, _, err := d.store.GetNode(spaceID, parentID)
	if err != nil {
		if err == ErrNodeNotFound {
			return errtypes.NotFound(ref.String())
		}
		return err
	}
	rp := d.assemblePermissions(ctx, spaceID, parentNode)
	if !rp.InitiateFileUpload {
		if rp.Stat {
			return errtypes.PermissionDenied(ref.String())
		}
		return errtypes.NotFound(ref.String())
	}
	return d.createEmptyFile(ctx, ref)
}

func (d *kvfsDriver) createEmptyFile(ctx context.Context, ref *provider.Reference) error {
	spaceID, parentID, name, err := d.resolveParentRef(ctx, ref)
	if err != nil {
		return err
	}

	u := ctxpkg.ContextMustGetUser(ctx)
	newID := uuid.New().String()
	now := time.Now().UnixNano()

	// Detached commit context — PutNode + updateChildrenWithCAS + touch +
	// event must complete or revert. See [kvfsDriver.commitPhase].
	commitCtx, cancel := d.commitPhase(ctx)
	defer cancel()

	node := &NodeEntry{
		ID:       newID,
		SpaceID:  spaceID,
		ParentID: parentID,
		Name:     name,
		Type:     NodeTypeFile,
		Size:     0,
		MTime:    now,
		ETag:     calculateEtag(newID, now),
		Owner:    u.Id.OpaqueId,
		MimeType: mime.Detect(false, name),
	}
	if err := d.store.PutNode(node, 0); err != nil {
		return err
	}

	if err := d.updateChildrenWithCAS(spaceID, parentID, func(children ChildMap) {
		children[name] = newID
	}); err != nil {
		d.store.DeleteNode(spaceID, newID)
		return err
	}

	d.propagateTreeSize(commitCtx, spaceID, parentID, 0)

	d.publishEvent(commitCtx, func() interface{} {
		return events.FileUploaded{
			SpaceOwner: u.Id,
			Executant:  u.Id,
			Ref:        spaceRef(spaceID, newID),
			Owner:      u.Id,
			Timestamp:  nowTimestamp(),
		}
	})

	return nil
}

func (d *kvfsDriver) checkNodeLock(ctx context.Context, node *NodeEntry) error {
	contextLockID, _ := ctxpkg.ContextGetLockID(ctx)
	lock := node.ToLock()
	if lock != nil {
		switch contextLockID {
		case "":
			return errtypes.Locked(lock.LockId)
		case lock.LockId:
			return nil
		default:
			return errtypes.Aborted("mismatching lock")
		}
	}
	if contextLockID != "" {
		return errtypes.Aborted("not locked")
	}
	return nil
}

// --- Helper functions ---

// calculateEtag generates an etag from a node ID and modification time.
func calculateEtag(nodeID string, mtime int64) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", nodeID, mtime)))
	return fmt.Sprintf("%x", h[:8])
}

// publishEvent enqueues an event for the async publisher worker. The
// event is constructed lazily via evf — on a no-stream driver or a
// dropped overflow, the constructor never runs.
//
// Previously this was synchronous (events.Publish ACK blocked the upload
// response). Under burst load the event stream backed up and that latency
// showed up as per-upload tail latency. The async queue decouples the two:
// uploads return as soon as the metadata commit lands, and a single
// background worker drains the event channel.
//
// Falls back to synchronous publish when the queue is nil (test fixtures
// that build kvfsDriver via struct literal without going through New()).
func (d *kvfsDriver) publishEvent(ctx context.Context, evf func() interface{}) {
	if d.stream == nil {
		return
	}
	if d.eventQueue == nil {
		// Synchronous fallback for test fixtures that build kvfsDriver via
		// struct literal without going through New().
		d.doPublish(ctx, evf)
		return
	}
	d.publishEventAsync(ctx, evf)
}

func nowTimestamp() *types.Timestamp {
	now := time.Now()
	return &types.Timestamp{
		Seconds: uint64(now.Unix()),
		Nanos:   uint32(now.Nanosecond()),
	}
}

func spaceRef(spaceID, nodeID string) *provider.Reference {
	return &provider.Reference{
		ResourceId: &provider.ResourceId{
			StorageId: spaceID,
			SpaceId:   spaceID,
			OpaqueId:  nodeID,
		},
	}
}

func spaceResourceID(spaceID, nodeID string) *provider.ResourceId {
	return &provider.ResourceId{
		StorageId: spaceID,
		SpaceId:   spaceID,
		OpaqueId:  nodeID,
	}
}
