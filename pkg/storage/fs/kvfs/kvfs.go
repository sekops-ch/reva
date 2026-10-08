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
	"fmt"
	"io"
	"net/url"
	"os"
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
	"github.com/opencloud-eu/reva/v2/pkg/storage"
	"github.com/opencloud-eu/reva/v2/pkg/storage/fs/registry"
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
	} else if opts.MaxVersions > 0 {
		log.Warn().Int("max_versions", opts.MaxVersions).Msg("kvfs: max_versions trims version entries only; enable gc_enabled to reclaim their blobs")
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
	// ocdav stats a version (HEAD/GET …/meta/<id>/v/<key>) by its revision key.
	if isRevisionRef(ref) {
		return d.statRevision(ctx, ref)
	}
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
	// The spaces dataprovider fetches a version by its revision key.
	if isRevisionRef(ref) {
		return d.downloadRevisionRef(ctx, ref, openReaderFunc)
	}
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

	reader := io.NopCloser(strings.NewReader("")) // an empty file has no blob
	if node.BlobID != "" {
		if reader, err = d.blob.Download(ctx, BlobKey(spaceID, node.BlobID)); err != nil {
			return nil, nil, errors.Wrap(err, "kvfs: failed to download blob")
		}
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
	spaceID, parentNode, name, err := d.resolveParent(ctx, ref)
	if err != nil {
		return err
	}
	parentID := parentNode.ID

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

// Delete moves a resource to the trash.
func (d *kvfsDriver) Delete(ctx context.Context, ref *provider.Reference) error {
	if err := d.refuseTrashed(ref); err != nil {
		return err
	}
	spaceID, nodeID, node, _, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.Delete })
	if err != nil {
		return err
	}

	if node.ParentID == "" {
		return errtypes.BadRequest("cannot delete space root")
	}
	if err := d.checkNodeLock(ctx, node); err != nil {
		return err
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
		if children[node.Name] == nodeID { // another node may hold the name since
			delete(children, node.Name)
		}
	}); err != nil {
		return err
	}

	// For files: delete the node (the trash snapshot is sufficient for restore).
	// For directories: keep nodes and children maps alive so that the entire
	// subtree can be restored. The nodes are no longer reachable via the tree
	// (removed from parent's children above) but remain in KV for restore/purge.
	if node.Type != NodeTypeDir {
		d.store.DeleteNode(spaceID, nodeID)
	} else if err := d.putNodeWithCAS(spaceID, nodeID, func(n *NodeEntry) {
		// A rename since the unlink took the folder back into the tree.
		if n.ParentID == node.ParentID && n.Name == node.Name {
			n.Trashed = true
		}
	}); err != nil {
		d.log.Warn().Err(err).Str("space_id", spaceID).Str("node_id", nodeID).Msg("Delete: failed to mark the trashed directory, it stays a write target by id")
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
		// PreconditionFailed as in decomposedfs: the WOPI connector then answers 200 for its own
		// lock id, which is how Collabora refreshes its lock, and 409 for another.
		if node.ToLock() != nil {
			return errtypes.PreconditionFailed("already locked")
		}
		node.Lock = newLockEntry(lock)
		return nil
	})
}

// newLockEntry stores lock as given.
func newLockEntry(lock *provider.Lock) *LockEntry {
	e := &LockEntry{
		LockID:  lock.GetLockId(),
		Type:    int(lock.GetType()),
		AppName: lock.GetAppName(),
		Meta:    lockMeta(lock.GetOpaque()),
	}
	if u := lock.GetUser(); u != nil {
		e.UserID, e.UserIdp = u.GetOpaqueId(), u.GetIdp()
	}
	if exp := lock.GetExpiration(); exp != nil {
		e.Expiry = int64(exp.GetSeconds())
	}
	return e
}

// lockModificationAllowed decides whether a request may refresh or remove held, as decomposedfs's
// isLockModificationAllowed does: a shared lock may always change; otherwise the app must match,
// and a user's lock changes only for that user, asking as that user.
func lockModificationAllowed(ctx context.Context, held, req *provider.Lock) error {
	if held.GetType() == provider.LockType_LOCK_TYPE_SHARED {
		return nil
	}
	if held.GetAppName() != req.GetAppName() {
		return errtypes.PermissionDenied("app names of the locks are mismatching")
	}
	if held.GetUser() == nil && req.GetUser() == nil {
		return nil
	}
	if !utils.UserIDEqual(held.GetUser(), req.GetUser()) {
		return errtypes.PermissionDenied("users of the locks are mismatching")
	}
	if u, _ := ctxpkg.ContextGetUser(ctx); !utils.UserIDEqual(held.GetUser(), u.GetId()) {
		return errtypes.PermissionDenied("lock holder and current user are mismatching")
	}
	return nil
}

func (d *kvfsDriver) RefreshLock(ctx context.Context, ref *provider.Reference, lock *provider.Lock, existingLockID string) error {
	spaceID, nodeID, _, _, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.InitiateFileUpload })
	if err != nil {
		return err
	}
	// As in decomposedfs, existingLockID names the lock to replace for WOPI's UnlockAndRelock; a
	// plain refresh names it by the new lock's own id. The lock is replaced as given.
	return d.putNodeWithCASCheck(spaceID, nodeID, func(node *NodeEntry) error {
		held := node.ToLock()
		if held == nil {
			return errtypes.PreconditionFailed("lock does not exist")
		}
		want := existingLockID
		if want == "" {
			want = lock.GetLockId()
		}
		if held.GetLockId() != want {
			return errtypes.Aborted("mismatching lock ID")
		}
		if err := lockModificationAllowed(ctx, held, lock); err != nil {
			return err
		}
		node.Lock = newLockEntry(lock)
		return nil
	})
}

func (d *kvfsDriver) Unlock(ctx context.Context, ref *provider.Reference, lock *provider.Lock) error {
	spaceID, nodeID, _, _, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.InitiateFileUpload })
	if err != nil {
		return err
	}
	return d.putNodeWithCASCheck(spaceID, nodeID, func(node *NodeEntry) error {
		held := node.ToLock()
		if held == nil {
			return errtypes.Aborted("lock does not exist")
		}
		if held.GetLockId() != lock.GetLockId() {
			return errtypes.Locked(held.GetLockId())
		}
		if err := lockModificationAllowed(ctx, held, lock); err != nil {
			return err
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

func (d *kvfsDriver) checkNodeLock(ctx context.Context, node *NodeEntry) error {
	contextLockID, _ := ctxpkg.ContextGetLockID(ctx)
	return checkLockID(contextLockID, node)
}

// checkLockID compares a request's lock id with node's lock, as decomposedfs does: a locked node
// needs its lock id, and a lock id on an unlocked node is refused.
func checkLockID(lockID string, node *NodeEntry) error {
	lock := node.ToLock()
	if lock != nil {
		switch lockID {
		case "":
			return errtypes.Locked(lock.LockId)
		case lock.LockId:
			return nil
		default:
			return errtypes.Aborted("mismatching lock")
		}
	}
	if lockID != "" {
		return errtypes.Aborted("not locked")
	}
	return nil
}

// --- Helper functions ---

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
