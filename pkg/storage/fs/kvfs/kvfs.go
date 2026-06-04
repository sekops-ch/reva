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
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	group "github.com/cs3org/go-cs3apis/cs3/identity/group/v1beta1"
	user "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
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

const defaultMaxCASRetries = 10

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
}

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

	store, err := NewKVStore(opts)
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
		})
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
		store:       store,
		blob:        blob,
		opts:        opts,
		log:         log,
		stream:      stream,
		uploadCache: uploadCache,
		eventQueue:  make(chan eventJob, eventQueueSize),
		eventStop:   make(chan struct{}),
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
			updateUploadAgeMetrics(uploads)
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

		// Reuse the ONE crash-safe deletion path for GC space reaping:
		// deleteSpaceContents (detached commit ctx) + DeleteSpace, identical
		// to DeleteStorageSpace. The GC never re-implements deletion.
		d.gc.reapSpace = func(ctx context.Context, spaceID, rootID string) error {
			cctx, cancel := d.commitPhase(ctx)
			defer cancel()
			d.deleteSpaceContents(cctx, spaceID, rootID)
			return d.store.DeleteSpace(spaceID)
		}

		// Identity-orphan reaping needs a live-user signal. Build a resolver
		// from the service's gateway + service-account config. On any missing
		// config or construction error, degrade gracefully to a nil resolver:
		// identity reaping is disabled but the internal residue sweep still
		// runs. storage-system leaves these unset (no personal spaces).
		if resolver, rerr := newCS3UserResolver(opts.GatewayAddr, opts.ServiceAccountID, opts.ServiceAccountSecret, log); rerr != nil {
			log.Warn().Err(rerr).Msg("kvfs: GC identity reaping disabled (resolver unavailable); residue sweep still active")
		} else {
			d.gc.resolver = resolver
			log.Info().Str("gateway", opts.GatewayAddr).Msg("kvfs: GC identity reaping enabled")
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
		d.gc.Stop()
	}
	d.store.Close()
	return nil
}

// maxCASRetries returns the driver's configured CAS retry bound.
func (d *kvfsDriver) maxCASRetries() int {
	return resolveMaxCASRetries(d.opts)
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

// applyChildIntent atomically applies an add and/or remove against the
// children map for (spaceID, parentID). Retries on CAS conflict up to
// d.opts.MaxCASRetries — on each retry we re-fetch the current map and
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

	for attempt := 0; attempt < d.maxCASRetries(); attempt++ {
		children, rev, err := d.store.GetChildren(spaceID, parentID)
		if err != nil {
			return nil, err
		}
		for name, id := range add {
			children[name] = id
		}
		for _, name := range remove {
			delete(children, name)
		}
		if err := d.store.PutChildren(spaceID, parentID, children, rev); err != nil {
			if err == ErrCASConflict {
				CASRetries.WithLabelValues("children_intent").Inc()
				casBackoff(attempt)
				continue
			}
			return nil, err
		}
		return children, nil
	}
	CASExhausted.WithLabelValues("children_intent").Inc()
	return nil, errors.New("kvfs: applyChildIntent failed after max CAS retries")
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
// snapshot of oc-uploads. Driven by the sampler goroutine in New.
func updateUploadAgeMetrics(uploads []*UploadSession) {
	UploadSessionsTotal.Set(float64(len(uploads)))
	if len(uploads) == 0 {
		OldestUploadAgeSeconds.Set(0)
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
	OldestUploadAgeSeconds.Set(float64(oldest))
}

// --- Space operations ---

// requestedSpaceIDOpaqueKeys are the opaque-map keys we accept as a
// caller-supplied space ID hint. The gateway's CreateHome sends
// "space_id" (with underscore); legacy / decomposedfs convention is
// "spaceid". Accepting both is strictly additive — at most one will be
// present in any given request.
var requestedSpaceIDOpaqueKeys = []string{"space_id", "spaceid"}

// requestedSpaceID extracts an explicit space-id hint from req.Opaque, or
// "" if the caller didn't supply one. Personal-type creates ignore the
// hint and derive the ID from the owner instead (see CreateStorageSpace).
func requestedSpaceID(req *provider.CreateStorageSpaceRequest) string {
	if req == nil || req.Opaque == nil {
		return ""
	}
	for _, k := range requestedSpaceIDOpaqueKeys {
		if entry, ok := req.Opaque.Map[k]; ok && len(entry.Value) > 0 {
			return string(entry.Value)
		}
	}
	return ""
}

// findExistingPersonal returns the (single) personal space owned by
// ownerID, or nil if none exists. Used both as the dedup fast-path and
// as the retry-after-CAS-conflict path in createPersonalSpace.
func (d *kvfsDriver) findExistingPersonal(ownerID string) *SpaceEntry {
	existing, err := d.store.ListSpaces(func(s *SpaceEntry) bool {
		return s.Type == "personal" && s.Owner == ownerID
	})
	if err != nil || len(existing) == 0 {
		return nil
	}
	return existing[0]
}

// CreateStorageSpace creates a new storage space with a root directory
// node. For type=personal the space ID is deterministically derived from
// the owner's ID — this is what makes concurrent CreateHome calls
// idempotent without a cross-pod lock: PutSpace(rev=0) acts as the
// atomic exclusion. Two pods racing into the same deterministic ID will
// see exactly one PutSpace succeed; the loser falls back through
// findExistingPersonal and returns the winner's space.
//
// For type=project / type=share / etc. the caller may supply an explicit
// ID via req.Opaque (key "space_id" or "spaceid"); otherwise a fresh
// UUID is minted.
func (d *kvfsDriver) CreateStorageSpace(ctx context.Context, req *provider.CreateStorageSpaceRequest) (*provider.CreateStorageSpaceResponse, error) {
	u := ctxpkg.ContextMustGetUser(ctx)

	spaceType := req.Type
	if spaceType == "" {
		spaceType = "personal"
	}
	ownerID := u.Id.OpaqueId

	// Personal-space dedup: fast path + CAS-retry path. Two attempts
	// suffice — the first races, the second observes the winner.
	if spaceType == "personal" {
		for attempt := 0; attempt < 2; attempt++ {
			if existing := d.findExistingPersonal(ownerID); existing != nil {
				d.log.Debug().Str("space_id", existing.ID).Str("type", spaceType).Str("owner", existing.Owner).Msg("personal space already exists, returning existing")
				rootNode, _, _ := d.store.GetNode(existing.ID, existing.RootID)
				return &provider.CreateStorageSpaceResponse{
					Status:       &rpc.Status{Code: rpc.Code_CODE_OK},
					StorageSpace: d.spaceToCS3(existing, rootNode),
				}, nil
			}
			resp, err := d.tryCreateSpace(ctx, req, spaceType, ownerID, ownerID)
			if err == nil {
				return resp, nil
			}
			if errors.Cause(err) == ErrCASConflict {
				// Lost the race; loop to surface the winner via findExistingPersonal.
				CASRetries.WithLabelValues("create_personal_space").Inc()
				continue
			}
			return nil, err
		}
		CASExhausted.WithLabelValues("create_personal_space").Inc()
		return nil, errors.New("kvfs: CreateStorageSpace(personal) failed to converge after retry")
	}

	spaceID := requestedSpaceID(req)
	if spaceID == "" {
		spaceID = uuid.New().String()
	}
	return d.tryCreateSpace(ctx, req, spaceType, ownerID, spaceID)
}

// tryCreateSpace executes the three-write sequence (PutNode +
// PutChildren + PutSpace, all at rev=0) for a fully-determined
// (spaceID, ownerID, spaceType) tuple. Returns ErrCASConflict (wrapped)
// if any of the three writes loses to a concurrent creator. Inline
// cleanup rolls back partial state so the next caller (or our own
// retry) starts from a clean slate.
//
// No commitPhase wrap: each Put is a single NATS KV write that doesn't
// take ctx; cleanup-on-error happens synchronously here.
func (d *kvfsDriver) tryCreateSpace(ctx context.Context, req *provider.CreateStorageSpaceRequest, spaceType, ownerID, spaceID string) (*provider.CreateStorageSpaceResponse, error) {
	rootID := spaceID // root node ID equals space ID
	now := time.Now().UnixNano()

	rootNode := &NodeEntry{
		ID:       rootID,
		SpaceID:  spaceID,
		ParentID: "",
		Name:     req.Name,
		Type:     NodeTypeDir,
		MTime:    now,
		ETag:     calculateEtag(rootID, now),
		Owner:    ownerID,
		MimeType: "httpd/unix-directory",
	}
	if err := d.store.PutNode(rootNode, 0); err != nil {
		return nil, errors.Wrap(err, "kvfs: failed to create root node")
	}
	if err := d.store.PutChildren(spaceID, rootID, ChildMap{}, 0); err != nil {
		d.store.DeleteNode(spaceID, rootID)
		return nil, errors.Wrap(err, "kvfs: failed to create root children")
	}

	quota := int64(-1) // unlimited
	if req.GetQuota() != nil {
		quota = int64(req.GetQuota().QuotaMaxBytes)
	}

	space := &SpaceEntry{
		ID:     spaceID,
		Type:   spaceType,
		Owner:  ownerID,
		Name:   req.Name,
		RootID: rootID,
		Quota:  quota,
		MTime:  now,
	}
	if err := d.store.PutSpace(space, 0); err != nil {
		d.store.DeleteNode(spaceID, rootID)
		d.store.DeleteChildren(spaceID, rootID)
		return nil, errors.Wrap(err, "kvfs: failed to create space")
	}

	d.log.Info().Str("space_id", spaceID).Str("type", spaceType).Str("owner", ownerID).Msg("created storage space")

	return &provider.CreateStorageSpaceResponse{
		Status:       &rpc.Status{Code: rpc.Code_CODE_OK},
		StorageSpace: d.spaceToCS3(space, rootNode),
	}, nil
}

// listSpacesFilter is the parsed representation of an inbound filter list.
// Parsing once up front keeps the per-space callback cheap and separates
// filter interpretation from filter application.
type listSpacesFilter struct {
	spaceTypes map[string]struct{} // empty = any type
	spaceID    string              // empty = any id
	ownerID    string              // empty = any owner
	userID     string              // empty = no TYPE_USER constraint
	userGroups []string            // groups for the userID, sourced from ctx when ctx user matches
}

// parseListSpacesFilter builds the internal filter representation.
// TYPE_USER matches decomposedfs semantics ("user has any access" = owner
// OR direct grantee OR group grantee). Groups are lifted from the ctx
// user when it matches the filter user; cross-user lookups apply only
// owner-or-direct-user-grant (group resolution would need a CS3 round-trip
// on this hot path).
func parseListSpacesFilter(ctx context.Context, in []*provider.ListStorageSpacesRequest_Filter) listSpacesFilter {
	f := listSpacesFilter{spaceTypes: map[string]struct{}{}}
	for _, x := range in {
		switch x.Type {
		case provider.ListStorageSpacesRequest_Filter_TYPE_SPACE_TYPE:
			st := x.GetSpaceType()
			if strings.HasPrefix(st, "+") {
				// "+grant" / "+mountpoint" are augmenting hints (include
				// additional categories), not filters. Ignore here for
				// parity with decomposedfs.
				continue
			}
			f.spaceTypes[st] = struct{}{}
		case provider.ListStorageSpacesRequest_Filter_TYPE_ID:
			if id := x.GetId(); id != nil {
				parsed, _ := storagespace.ParseID(id.OpaqueId)
				f.spaceID = parsed.SpaceId
				if f.spaceID == "" {
					f.spaceID = id.OpaqueId
				}
			}
		case provider.ListStorageSpacesRequest_Filter_TYPE_OWNER:
			if o := x.GetOwner(); o != nil {
				f.ownerID = o.OpaqueId
			}
		case provider.ListStorageSpacesRequest_Filter_TYPE_USER:
			if usr := x.GetUser(); usr != nil {
				f.userID = usr.OpaqueId
				if cu, ok := ctxpkg.ContextGetUser(ctx); ok && cu.GetId().GetOpaqueId() == usr.OpaqueId {
					f.userGroups = cu.Groups
				}
			}
		}
	}
	return f
}

// ListStorageSpaces lists spaces matching the given filters.
func (d *kvfsDriver) ListStorageSpaces(ctx context.Context, filter []*provider.ListStorageSpacesRequest_Filter, unrestricted bool) ([]*provider.StorageSpace, error) {
	u, _ := ctxpkg.ContextGetUser(ctx)
	f := parseListSpacesFilter(ctx, filter)

	spaces, err := d.store.ListSpaces(func(s *SpaceEntry) bool {
		if len(f.spaceTypes) > 0 {
			if _, ok := f.spaceTypes[s.Type]; !ok {
				return false
			}
		}
		if f.spaceID != "" && f.spaceID != s.ID {
			return false
		}
		if f.ownerID != "" && f.ownerID != s.Owner {
			return false
		}
		// TYPE_USER's grant branch needs the root node, which we don't
		// load here. Owner-match short-circuits handled post-fetch
		// alongside the existing visibility check (avoids a second
		// rootNode read).
		return true
	})
	if err != nil {
		return nil, err
	}

	var result []*provider.StorageSpace
	for _, s := range spaces {
		rootNode, _, err := d.store.GetNode(s.ID, s.RootID)
		if err != nil {
			d.log.Warn().Err(err).Str("space_id", s.ID).Msg("failed to get root node for space")
			continue
		}
		// TYPE_USER: owner short-circuits; otherwise root node must grant.
		// Matches upstream decomposedfs's "user has any access" semantic.
		if f.userID != "" && s.Owner != f.userID {
			probe := &user.User{Id: &user.UserId{OpaqueId: f.userID}, Groups: f.userGroups}
			if !d.hasGrantForUser(rootNode, probe) {
				continue
			}
		}
		// Visibility check for the *caller* (ctx user) — independent of
		// TYPE_USER which constrains the *subject* being queried.
		if !unrestricted && u != nil && s.Owner != u.Id.OpaqueId {
			if !d.hasGrantForUser(rootNode, u) {
				continue
			}
		}
		result = append(result, d.spaceToCS3(s, rootNode))
	}

	return result, nil
}

// UpdateStorageSpace updates a storage space's name, quota, or other properties.
func (d *kvfsDriver) UpdateStorageSpace(ctx context.Context, req *provider.UpdateStorageSpaceRequest) (*provider.UpdateStorageSpaceResponse, error) {
	spaceID := req.GetStorageSpace().GetId().GetOpaqueId()
	if spaceID == "" {
		return nil, errtypes.BadRequest("missing space ID")
	}
	if parsed, err := storagespace.ParseID(spaceID); err == nil && parsed.SpaceId != "" {
		spaceID = parsed.SpaceId
	}

	space, rev, err := d.store.GetSpace(spaceID)
	if err != nil {
		return nil, err
	}

	if req.StorageSpace.Name != "" {
		space.Name = req.StorageSpace.Name
	}
	if req.StorageSpace.Quota != nil {
		space.Quota = int64(req.StorageSpace.Quota.QuotaMaxBytes)
	}
	space.MTime = time.Now().UnixNano()

	if err := d.store.PutSpace(space, rev); err != nil {
		return nil, err
	}

	rootNode, _, _ := d.store.GetNode(space.ID, space.RootID)

	return &provider.UpdateStorageSpaceResponse{
		Status:       &rpc.Status{Code: rpc.Code_CODE_OK},
		StorageSpace: d.spaceToCS3(space, rootNode),
	}, nil
}

// DeleteStorageSpace deletes a storage space and all its contents.
func (d *kvfsDriver) DeleteStorageSpace(ctx context.Context, req *provider.DeleteStorageSpaceRequest) error {
	spaceID := req.GetId().GetOpaqueId()
	if spaceID == "" {
		return errtypes.BadRequest("missing space ID")
	}
	// The gateway sends composite IDs like "providerId$spaceId" — extract the raw space ID
	if parsed, err := storagespace.ParseID(spaceID); err == nil && parsed.SpaceId != "" {
		spaceID = parsed.SpaceId
	}

	space, _, err := d.store.GetSpace(spaceID)
	if err != nil {
		return err
	}

	// Detached commit context — recursive blob deletes + multipart aborts
	// in deleteSpaceContents can take time; client cancellation must not
	// interrupt the sweep. See [kvfsDriver.commitPhase].
	commitCtx, cancel := d.commitPhase(ctx)
	defer cancel()

	d.deleteSpaceContents(commitCtx, spaceID, space.RootID)

	return d.store.DeleteSpace(spaceID)
}

// deleteSpaceContents removes all nodes, blobs, trash, versions, and uploads
// belonging to a space. Called before deleting the space entry itself.
func (d *kvfsDriver) deleteSpaceContents(ctx context.Context, spaceID, rootID string) {
	d.recursiveDeleteNodesAndBlobs(ctx, spaceID, rootID)
	d.store.DeleteNode(spaceID, rootID)
	d.store.DeleteChildren(spaceID, rootID)

	trashItems, err := d.store.ListTrash(spaceID)
	if err == nil {
		for _, t := range trashItems {
			if t.Node.Type == NodeTypeDir {
				// Trashed directories still have live nodes — clean them up
				d.recursiveDeleteNodesAndBlobs(ctx, spaceID, t.NodeID)
				d.store.DeleteNode(spaceID, t.NodeID)
				d.store.DeleteChildren(spaceID, t.NodeID)
			} else if t.Node.BlobID != "" {
				d.blob.Delete(ctx, BlobKey(spaceID, t.Node.BlobID))
			}
			d.store.DeleteTrash(spaceID, t.Key)
		}
	}

	versions, err := d.store.ListVersionsBySpace(spaceID)
	if err == nil {
		for _, v := range versions {
			if v.BlobID != "" {
				d.blob.Delete(ctx, BlobKey(spaceID, v.BlobID))
			}
			d.store.DeleteVersion(v.SpaceID, v.NodeID, v.Key)
		}
	}

	uploads, err := d.store.ListUploadsBySpace(spaceID)
	if err == nil {
		for _, u := range uploads {
			if u.S3MultipartID != "" {
				d.blob.AbortMultipartUpload(ctx, BlobKey(spaceID, u.BlobID), u.S3MultipartID)
			}
			if u.BlobID != "" {
				d.blob.Delete(ctx, BlobKey(spaceID, u.BlobID))
			}
			d.store.DeleteUpload(u.ID)
		}
	}
}

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

	for attempt := 0; attempt < d.maxCASRetries(); attempt++ {
		children, rev, err := d.store.GetChildren(spaceID, parentID)
		if err != nil {
			return err
		}

		if _, exists := children[name]; exists {
			return errtypes.AlreadyExists(name)
		}

		newID := uuid.New().String()
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
			if err == ErrCASConflict {
				CASRetries.WithLabelValues("mkdir").Inc()
				casBackoff(attempt)
				continue
			}
			return err
		}

		d.touchParent(commitCtx, spaceID, parentID)

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

	CASExhausted.WithLabelValues("mkdir").Inc()
	return errors.New("kvfs: CreateDir failed after max CAS retries")
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
		d.touchParent(commitCtx, spaceID, node.ParentID)
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

	// Update parent sizes
	d.propagateTreeSize(commitCtx, spaceID, node.ParentID, -node.Size)
	d.touchParent(commitCtx, spaceID, node.ParentID)

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

// Move changes the path of a resource.
func (d *kvfsDriver) Move(ctx context.Context, oldRef, newRef *provider.Reference) error {
	// Phase 1: Validate (read-only, no mutations)

	spaceID, nodeID, node, _, err := d.resolveAndAuthorize(ctx, oldRef, func(rp *provider.ResourcePermissions) bool { return rp.Move })
	if err != nil {
		return err
	}

	if node.ParentID == "" {
		return errtypes.BadRequest("cannot move space root")
	}

	if err := d.checkNodeLock(ctx, node); err != nil {
		return err
	}

	oldParentID := node.ParentID
	oldName := node.Name

	destSpaceID, newParentID, newName, err := d.resolveParentRef(ctx, newRef)
	if err != nil {
		return err
	}

	if destSpaceID != spaceID {
		return errtypes.BadRequest("cross-space move is not supported")
	}

	destParentNode, _, err := d.store.GetNode(spaceID, newParentID)
	if err != nil {
		if err == ErrNodeNotFound {
			return errtypes.NotFound("destination parent not found")
		}
		return err
	}

	drp := d.assemblePermissions(ctx, spaceID, destParentNode)
	if node.Type == NodeTypeDir {
		if !drp.CreateContainer {
			if drp.Stat {
				return errtypes.PermissionDenied(newRef.String())
			}
			return errtypes.NotFound(newRef.String())
		}
	} else {
		if !drp.InitiateFileUpload {
			if drp.Stat {
				return errtypes.PermissionDenied(newRef.String())
			}
			return errtypes.NotFound(newRef.String())
		}
	}

	// Phase 2: Mutate (crash-safe ordering: node → add-to-new → remove-from-old)
	//
	// Detached commit context so client/gateway cancellation between the
	// three CAS steps can't leave the node in both parents. See
	// [kvfsDriver.commitPhase] for the contract.
	commitCtx, cancel := d.commitPhase(ctx)
	defer cancel()

	now := time.Now().UnixNano()
	newETag := calculateEtag(nodeID, now)

	if err := d.putNodeWithCAS(spaceID, nodeID, func(n *NodeEntry) {
		n.ParentID = newParentID
		n.Name = newName
		n.MTime = now
		n.ETag = newETag
	}); err != nil {
		return err
	}

	if err := d.updateChildrenWithCASCheck(spaceID, newParentID, func(children ChildMap) error {
		if existing, ok := children[newName]; ok && existing != nodeID {
			return errtypes.AlreadyExists(newName)
		}
		children[newName] = nodeID
		return nil
	}); err != nil {
		revertErr := d.putNodeWithCAS(spaceID, nodeID, func(n *NodeEntry) {
			n.ParentID = oldParentID
			n.Name = oldName
			n.MTime = now
			n.ETag = calculateEtag(nodeID, now)
		})
		if revertErr != nil {
			d.log.Error().Err(revertErr).
				Str("space_id", spaceID).Str("node_id", nodeID).
				Msg("Move: failed to revert node metadata after children update failure")
		}
		return err
	}

	if err := d.updateChildrenWithCAS(spaceID, oldParentID, func(children ChildMap) {
		delete(children, oldName)
	}); err != nil {
		d.log.Warn().Err(err).
			Str("space_id", spaceID).Str("node_id", nodeID).
			Str("old_parent", oldParentID).Str("old_name", oldName).
			Msg("Move: failed to remove from old parent (node exists in both parents, will self-correct via GC reconciler)")
	}

	// Phase 3: Propagate
	//
	// propagateTreeSize walks ancestors updating Size + MTime + ETag at every
	// level, which is what OpenCloud sync clients need to detect deep changes.
	// For same-parent rename, delta is zero but we still propagate so the
	// rename bubbles up to the space root as an etag change.

	if newParentID == oldParentID {
		d.propagateTreeSize(commitCtx, spaceID, oldParentID, 0)
	} else {
		d.propagateTreeSize(commitCtx, spaceID, oldParentID, -node.Size)
		d.propagateTreeSize(commitCtx, spaceID, newParentID, node.Size)
	}

	executant, _ := ctxpkg.ContextGetUser(ctx)
	d.publishEvent(commitCtx, func() interface{} {
		if executant == nil {
			return nil
		}
		return events.ItemMoved{
			SpaceOwner:   executant.Id,
			Executant:    executant.Id,
			Ref:          spaceRef(spaceID, nodeID),
			Owner:        executant.Id,
			OldReference: oldRef,
			Timestamp:    nowTimestamp(),
		}
	})

	return nil
}

// Upload creates or updates a resource with new content.
func (d *kvfsDriver) Upload(ctx context.Context, req storage.UploadRequest, uff storage.UploadFinishedFunc) (*provider.ResourceInfo, error) {
	ref := req.Ref

	var ifMatchEtag string
	if ref.GetResourceId() == nil || ref.GetResourceId().GetSpaceId() == "" {
		var parsedRef *provider.Reference
		var err error
		parsedRef, ifMatchEtag, err = d.parseUploadPath(ref.GetPath())
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
		// Overwrite path: CAS-update the existing node's blob/size/etag.
		// Children map is unchanged so no children CAS is needed.
		for attempt := 0; attempt < d.maxCASRetries(); attempt++ {
			existingNode, nodeRev, err := d.store.GetNode(p.SpaceID, existingID)
			if err != nil {
				return "", err
			}

			if p.IfMatchEtag != "" && existingNode.ETag != p.IfMatchEtag {
				return "", errtypes.Aborted("if-match etag mismatch")
			}

			if !d.opts.DisableVersioning && existingNode.BlobID != "" && attempt == 0 {
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

			sizeDelta := p.Size - existingNode.Size
			existingNode.BlobID = p.BlobID
			existingNode.BlobSize = p.Size
			existingNode.Size = p.Size
			existingNode.MTime = now
			existingNode.ETag = calculateEtag(existingID, now)
			existingNode.MimeType = p.MimeType
			existingNode.Checksum = p.Checksum
			existingNode.Processing = false

			if err := d.store.PutNode(existingNode, nodeRev); err != nil {
				if err == ErrCASConflict {
					CASRetries.WithLabelValues("upload").Inc()
					casBackoff(attempt)
					continue
				}
				return "", err
			}

			// propagateTreeSize updates the parent's Size + MTime + ETag
			// via propagateOneAncestor; an extra touchParent here would
			// just re-CAS-write the same node.
			d.propagateTreeSize(ctx, p.SpaceID, p.ParentID, sizeDelta)
			return existingID, nil
		}
		CASExhausted.WithLabelValues("upload").Inc()
		return "", errors.New("kvfs: file overwrite failed after max CAS retries")
	}

	// Create path: allocate the node first, then atomically add to parent's
	// children map via the intent helper.
	//
	// PutNode is retried with a fresh UUID on CAS conflict — UUID collisions
	// are vanishingly rare in practice but the unit suite exercises this
	// path via mock injection, so we defend it.
	var nodeID string
	var nodeCreated bool
	for attempt := 0; attempt < d.maxCASRetries(); attempt++ {
		nodeID = uuid.New().String()
		node := &NodeEntry{
			ID: nodeID, SpaceID: p.SpaceID, ParentID: p.ParentID,
			Name: p.Name, Type: NodeTypeFile,
			BlobID: p.BlobID, BlobSize: p.Size, Size: p.Size,
			MTime: now, ETag: calculateEtag(nodeID, now),
			Owner: p.OwnerID, MimeType: p.MimeType, Checksum: p.Checksum,
		}
		if err := d.store.PutNode(node, 0); err != nil {
			if err == ErrCASConflict {
				CASRetries.WithLabelValues("upload").Inc()
				casBackoff(attempt)
				continue
			}
			return "", err
		}
		nodeCreated = true
		break
	}
	if !nodeCreated {
		CASExhausted.WithLabelValues("upload").Inc()
		return "", errors.New("kvfs: file commit failed after max CAS retries")
	}

	if _, err := d.applyChildIntent(p.SpaceID, p.ParentID, map[string]string{p.Name: nodeID}, nil); err != nil {
		d.store.DeleteNode(p.SpaceID, nodeID)
		CASExhausted.WithLabelValues("upload").Inc()
		return "", err
	}

	// propagateOneAncestor (inside propagateTreeSize) covers the parent
	// touch; an extra touchParent here would be redundant.
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
	return result, nil
}

// parseUploadPath decodes an upload path encoded by InitiateUpload.
// Format: "storageId$spaceId!opaqueId/relativePath[?if-match=etag]"
// Returns the reference and an optional if-match etag for CAS enforcement.
func (d *kvfsDriver) parseUploadPath(p string) (*provider.Reference, string, error) {
	// The simple handler prefixes with "/", clean it
	p = strings.TrimPrefix(p, "/")

	// Extract query parameters (if-match) before parsing the path
	var ifMatchEtag string
	if idx := strings.Index(p, "?"); idx >= 0 {
		query, err := url.ParseQuery(p[idx+1:])
		if err == nil {
			ifMatchEtag = query.Get("if-match")
		}
		p = p[:idx]
	}

	// Split on first "/" to separate spaceRef from file path
	parts := strings.SplitN(p, "/", 2)
	if len(parts) < 2 {
		return nil, "", errtypes.BadRequest("kvfs: invalid upload path format: " + p)
	}

	idPart := parts[0]
	filePath := parts[1]

	rid, err := storagespace.ParseID(idPart)
	if err != nil {
		return nil, "", errtypes.BadRequest("kvfs: failed to parse space ID from upload path: " + err.Error())
	}

	return &provider.Reference{
		ResourceId: &rid,
		Path:       utils.MakeRelativePath(filePath),
	}, ifMatchEtag, nil
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

func (d *kvfsDriver) ListRecycle(ctx context.Context, ref *provider.Reference, key, relativePath string) ([]*provider.RecycleItem, error) {
	spaceID, err := d.checkRecyclePermission(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.ListRecycle })
	if err != nil {
		return nil, err
	}

	if key != "" {
		t, err := d.store.GetTrash(spaceID, key)
		if err != nil {
			if err == ErrNotFound {
				return nil, errtypes.NotFound(key)
			}
			return nil, err
		}
		return []*provider.RecycleItem{d.trashToRecycleItem(spaceID, t)}, nil
	}

	trashItems, err := d.store.ListTrash(spaceID)
	if err != nil {
		return nil, err
	}

	result := make([]*provider.RecycleItem, 0, len(trashItems))
	for _, t := range trashItems {
		result = append(result, d.trashToRecycleItem(spaceID, t))
	}
	return result, nil
}

func (d *kvfsDriver) trashToRecycleItem(spaceID string, t *TrashEntry) *provider.RecycleItem {
	itemType := provider.ResourceType_RESOURCE_TYPE_FILE
	if t.Node.Type == NodeTypeDir {
		itemType = provider.ResourceType_RESOURCE_TYPE_CONTAINER
	}
	// ocdav's trashbin PROPFIND derives `<oc:trashbin-original-filename>`
	// and `<oc:trashbin-original-location>` from `Ref.Path`
	// (path.Base + TrimPrefix("/")). If we leave Path empty the client
	// sees "." for the filename which (a) confuses any UI showing the
	// trash listing and (b) makes the spec-compliant "MOVE from trash"
	// restore protocol impossible because the client has no idea what
	// the file was called. Populate Path from TrashEntry.OriginalPath
	// which the Delete handler captures.
	ref := &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: spaceID, OpaqueId: t.NodeID},
	}
	if t.OriginalPath != "" {
		// CS3 references use leading-slash relative paths.
		if t.OriginalPath[0] != '/' {
			ref.Path = "/" + t.OriginalPath
		} else {
			ref.Path = t.OriginalPath
		}
	} else if t.Node.Name != "" {
		ref.Path = "/" + t.Node.Name
	}
	return &provider.RecycleItem{
		Key:  t.Key,
		Ref:  ref,
		Type: itemType,
		DeletionTime: &types.Timestamp{
			Seconds: uint64(t.DeletionTime),
		},
		Size: uint64(t.Node.Size),
	}
}

func (d *kvfsDriver) RestoreRecycleItem(ctx context.Context, ref *provider.Reference, key, relativePath string, restoreRef *provider.Reference) error {
	spaceID, err := d.checkRecyclePermission(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.RestoreRecycleItem })
	if err != nil {
		return err
	}

	trashItem, err := d.store.GetTrash(spaceID, key)
	if err != nil {
		if err == ErrNotFound {
			return errtypes.NotFound(key)
		}
		return err
	}

	// Determine restore location
	restorePath := trashItem.OriginalPath
	if restoreRef != nil && restoreRef.Path != "" {
		restorePath = restoreRef.Path
	}

	parentPath := filepath.Dir(restorePath)
	parentID, err := d.resolvePathToNodeID(ctx, spaceID, parentPath)
	if err != nil {
		return errors.Wrap(err, "kvfs: restore parent not found")
	}

	// Detached commit context — three NATS bucket touches (PutNode +
	// updateChildren + DeleteTrash) must run to completion; otherwise
	// "node both restored AND in trash" leaks. See [kvfsDriver.commitPhase].
	commitCtx, cancel := d.commitPhase(ctx)
	defer cancel()

	now := time.Now().UnixNano()
	restoreName := filepath.Base(restorePath)
	var restoredNodeID string
	var restoredSize int64

	// For directories: the node and its entire subtree are still alive in KV
	// (Delete keeps them for restorability). Update the live node in place.
	// For files (or old-format trash where the node was deleted): re-create
	// from the snapshot stored in the trash entry.
	if trashItem.Node.Type == NodeTypeDir {
		liveNode, rev, gerr := d.store.GetNode(spaceID, trashItem.NodeID)
		if gerr == nil {
			liveNode.ParentID = parentID
			liveNode.Name = restoreName
			liveNode.MTime = now
			liveNode.ETag = calculateEtag(liveNode.ID, now)
			if err := d.store.PutNode(liveNode, rev); err != nil {
				return errors.Wrap(err, "kvfs: failed to update restored directory node")
			}
			restoredNodeID = liveNode.ID
			restoredSize = liveNode.Size
		} else {
			// Fallback for old trash entries where nodes were already deleted:
			// re-create from snapshot (restores as empty directory)
			restoredNode := trashItem.Node
			restoredNode.ParentID = parentID
			restoredNode.Name = restoreName
			restoredNode.MTime = now
			restoredNode.ETag = calculateEtag(restoredNode.ID, now)
			if err := d.store.PutNode(&restoredNode, 0); err != nil {
				return err
			}
			if err := d.store.PutChildren(spaceID, restoredNode.ID, ChildMap{}, 0); err != nil {
				d.log.Warn().Err(err).Str("node_id", restoredNode.ID).Msg("RestoreRecycleItem: failed to create children map for old-format directory")
			}
			restoredNodeID = restoredNode.ID
			restoredSize = restoredNode.Size
		}
	} else {
		// File: re-create from snapshot (node was deleted at trash time)
		restoredNode := trashItem.Node
		restoredNode.ParentID = parentID
		restoredNode.Name = restoreName
		restoredNode.MTime = now
		restoredNode.ETag = calculateEtag(restoredNode.ID, now)
		if err := d.store.PutNode(&restoredNode, 0); err != nil {
			return err
		}
		restoredNodeID = restoredNode.ID
		restoredSize = restoredNode.Size
	}

	if err := d.updateChildrenWithCAS(spaceID, parentID, func(children ChildMap) {
		children[restoreName] = restoredNodeID
	}); err != nil {
		return err
	}

	if err := d.store.DeleteTrash(spaceID, key); err != nil {
		return err
	}

	d.propagateTreeSize(commitCtx, spaceID, parentID, restoredSize)

	executant, _ := ctxpkg.ContextGetUser(ctx)
	d.publishEvent(commitCtx, func() interface{} {
		if executant == nil {
			return nil
		}
		return events.ItemRestored{
			SpaceOwner: executant.Id,
			Executant:  executant.Id,
			ID:         spaceResourceID(spaceID, restoredNodeID),
			Ref:        spaceRef(spaceID, restoredNodeID),
			Owner:      executant.Id,
			Key:        key,
			Timestamp:  nowTimestamp(),
		}
	})

	return nil
}

func (d *kvfsDriver) PurgeRecycleItem(ctx context.Context, ref *provider.Reference, key, relativePath string) error {
	spaceID, err := d.checkRecyclePermission(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.PurgeRecycle })
	if err != nil {
		return err
	}

	trashItem, err := d.store.GetTrash(spaceID, key)
	if err != nil {
		if err == ErrNotFound {
			return errtypes.NotFound(key)
		}
		return err
	}

	// Detached commit context — partial purge (some blobs deleted in S3, some
	// nodes still in NATS) leaves an inconsistent half-state and the operator-
	// invoked re-purge is the only recovery. See [kvfsDriver.commitPhase].
	commitCtx, cancel := d.commitPhase(ctx)
	defer cancel()

	if trashItem.Node.Type == NodeTypeDir {
		// Directories: nodes and children maps are still alive in KV.
		// Recursively delete all descendants, their blobs, and versions.
		d.recursiveDeleteNodesAndBlobs(commitCtx, spaceID, trashItem.NodeID)
		d.store.DeleteNode(spaceID, trashItem.NodeID)
		d.store.DeleteChildren(spaceID, trashItem.NodeID)
	} else if trashItem.Node.BlobID != "" {
		d.blob.Delete(commitCtx, BlobKey(spaceID, trashItem.Node.BlobID))
	}

	if err := d.store.DeleteTrash(spaceID, key); err != nil {
		return err
	}

	executant, _ := ctxpkg.ContextGetUser(ctx)
	d.publishEvent(commitCtx, func() interface{} {
		if executant == nil {
			return nil
		}
		return events.ItemPurged{
			Executant: executant.Id,
			ID:        spaceResourceID(spaceID, trashItem.NodeID),
			Ref:       spaceRef(spaceID, trashItem.NodeID),
			Owner:     executant.Id,
			Timestamp: nowTimestamp(),
		}
	})

	return nil
}

func (d *kvfsDriver) EmptyRecycle(ctx context.Context, ref *provider.Reference) error {
	spaceID, err := d.checkRecyclePermission(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.PurgeRecycle })
	if err != nil {
		return err
	}

	trashItems, err := d.store.ListTrash(spaceID)
	if err != nil {
		return err
	}

	// Detached commit context for the full sweep — partial empties leave
	// half-purged trash that requires re-running EmptyRecycle to clean up.
	// Same rationale as PurgeRecycleItem. See [kvfsDriver.commitPhase].
	commitCtx, cancel := d.commitPhase(ctx)
	defer cancel()

	for _, t := range trashItems {
		if t.Node.Type == NodeTypeDir {
			d.recursiveDeleteNodesAndBlobs(commitCtx, spaceID, t.NodeID)
			d.store.DeleteNode(spaceID, t.NodeID)
			d.store.DeleteChildren(spaceID, t.NodeID)
		} else if t.Node.BlobID != "" {
			d.blob.Delete(commitCtx, BlobKey(spaceID, t.Node.BlobID))
		}
		d.store.DeleteTrash(spaceID, t.Key)
	}

	d.publishEvent(commitCtx, func() interface{} {
		u, ok := ctxpkg.ContextGetUser(ctx)
		if !ok {
			return nil
		}
		return events.TrashbinPurged{
			Executant: u.Id,
			Ref:       ref,
			Owner:     u.Id,
			Timestamp: nowTimestamp(),
		}
	})

	return nil
}

// --- Grants ---

func (d *kvfsDriver) AddGrant(ctx context.Context, ref *provider.Reference, g *provider.Grant) error {
	spaceID, nodeID, _, _, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.AddGrant })
	if err != nil {
		return err
	}
	return d.modifyGrant(spaceID, nodeID, g, "add")
}

func (d *kvfsDriver) DenyGrant(ctx context.Context, ref *provider.Reference, g *provider.Grantee) error {
	return errtypes.NotSupported("kvfs: DenyGrant not yet supported")
}

func (d *kvfsDriver) RemoveGrant(ctx context.Context, ref *provider.Reference, g *provider.Grant) error {
	spaceID, nodeID, _, _, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.RemoveGrant })
	if err != nil {
		return err
	}
	return d.modifyGrant(spaceID, nodeID, g, "remove")
}

func (d *kvfsDriver) UpdateGrant(ctx context.Context, ref *provider.Reference, g *provider.Grant) error {
	spaceID, nodeID, _, _, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.UpdateGrant })
	if err != nil {
		return err
	}
	return d.modifyGrant(spaceID, nodeID, g, "update")
}

func (d *kvfsDriver) ListGrants(ctx context.Context, ref *provider.Reference) ([]*provider.Grant, error) {
	_, _, node, _, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.ListGrants })
	if err != nil {
		return nil, err
	}

	var grants []*provider.Grant
	for _, g := range node.Grants {
		grants = append(grants, grantEntryToCS3(g))
	}
	return grants, nil
}

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

// resolveRef resolves a CS3 Reference to a (spaceID, nodeID) pair.
func (d *kvfsDriver) resolveRef(ctx context.Context, ref *provider.Reference) (string, string, error) {
	if ref == nil {
		return "", "", errtypes.BadRequest("nil reference")
	}

	rid := ref.GetResourceId()
	if rid != nil && rid.SpaceId != "" && rid.OpaqueId != "" {
		// Direct ID reference
		spaceID := rid.SpaceId
		nodeID := rid.OpaqueId
		if ref.Path != "" && ref.Path != "." && ref.Path != "/" {
			// Relative path from the node
			resolvedID, err := d.resolveRelativePath(ctx, spaceID, nodeID, ref.Path)
			if err != nil {
				if err == ErrNodeNotFound {
					return "", "", errtypes.NotFound(ref.String())
				}
				return "", "", err
			}
			return spaceID, resolvedID, nil
		}
		return spaceID, nodeID, nil
	}

	return "", "", errtypes.BadRequest("kvfs: reference must have resource ID")
}

// resolveParentRef resolves a reference to its parent's (spaceID, parentID, childName).
func (d *kvfsDriver) resolveParentRef(ctx context.Context, ref *provider.Reference) (string, string, string, error) {
	rid := ref.GetResourceId()
	if rid == nil || rid.SpaceId == "" {
		return "", "", "", errtypes.BadRequest("missing space ID in reference")
	}

	spaceID := rid.SpaceId
	refPath := ref.Path
	if refPath == "" {
		return "", "", "", errtypes.BadRequest("missing path in reference")
	}

	refPath = filepath.Clean(refPath)

	// When OpaqueId points directly to a file and path is "." (e.g. from
	// sharesstorageprovider for file-level shares), resolve the parent from
	// the node's own metadata instead of path-walking.
	if (refPath == "." || refPath == "/") && rid.OpaqueId != "" && rid.OpaqueId != rid.SpaceId {
		node, _, err := d.store.GetNode(spaceID, rid.OpaqueId)
		if err == nil && node.Type == NodeTypeFile && node.ParentID != "" {
			return spaceID, node.ParentID, node.Name, nil
		}
	}

	parentPath := filepath.Dir(refPath)
	name := filepath.Base(refPath)

	parentID, err := d.resolvePathToNodeID(ctx, spaceID, parentPath)
	if err != nil {
		// If parent starts from the space root, resolve from root
		parentID = rid.OpaqueId
		if parentPath != "." && parentPath != "/" {
			parentID, err = d.resolveRelativePath(ctx, spaceID, rid.OpaqueId, parentPath)
			if err != nil {
				if err == ErrNodeNotFound {
					return "", "", "", errtypes.NotFound("parent directory not found: " + parentPath)
				}
				return "", "", "", err
			}
		}
	}

	return spaceID, parentID, name, nil
}

// resolveRelativePath resolves a relative path from a starting node.
func (d *kvfsDriver) resolveRelativePath(ctx context.Context, spaceID, startNodeID, path string) (string, error) {
	path = filepath.Clean(path)
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || (len(parts) == 1 && parts[0] == "") {
		return startNodeID, nil
	}

	currentID := startNodeID
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		children, _, err := d.store.GetChildren(spaceID, currentID)
		if err != nil {
			return "", errors.Wrapf(err, "kvfs: failed to list children of %s", currentID)
		}
		childID, exists := children[part]
		if !exists {
			return "", ErrNodeNotFound
		}
		currentID = childID
	}
	return currentID, nil
}

// resolvePathToNodeID resolves an absolute path from space root to a node ID.
func (d *kvfsDriver) resolvePathToNodeID(ctx context.Context, spaceID, path string) (string, error) {
	// Space root
	space, _, err := d.store.GetSpace(spaceID)
	if err != nil {
		return "", err
	}
	return d.resolveRelativePath(ctx, spaceID, space.RootID, path)
}

// buildPath walks from a node to the space root, building the full path.
func (d *kvfsDriver) buildPath(ctx context.Context, spaceID, nodeID string) (string, error) {
	var parts []string
	currentID := nodeID

	for i := 0; i < 100; i++ { // safety limit
		node, _, err := d.store.GetNode(spaceID, currentID)
		if err != nil {
			return "", err
		}
		if node.ParentID == "" {
			// Reached the root
			break
		}
		parts = append([]string{node.Name}, parts...)
		currentID = node.ParentID
	}

	return "/" + strings.Join(parts, "/"), nil
}

// propagateTreeSize walks ancestors starting at nodeID up to the space root,
// updating Size (by delta) plus MTime and ETag on every node along the way.
//
// Refreshing MTime + ETag on each ancestor is mandatory for OpenCloud sync
// clients to detect deep changes: clients poll the space root's etag and
// descend from there. If only the direct parent's etag changes, clients miss
// any change deeper than one level. See opencloud-eu/reva#625 for the upstream
// review that surfaced this.
//
// The size delta may be zero (e.g. same-folder rename); we still walk because
// mtime + etag still need to bubble.
//
// Best-effort: an ancestor CAS that exhausts retries bumps
// `kvfs_tree_size_drift_total` and the walk continues so the rest of the chain
// stays fresh.
func (d *kvfsDriver) propagateTreeSize(ctx context.Context, spaceID, nodeID string, delta int64) {
	currentID := nodeID
	const maxDepth = 100
	for depth := 0; depth < maxDepth; depth++ {
		nextID, ok := d.propagateOneAncestor(spaceID, currentID, delta)
		if !ok {
			// Inner loop exhausted; drift was already recorded. Walk up via
			// a non-mutating GetNode so the rest of the chain still gets a
			// chance to be propagated.
			node, _, err := d.store.GetNode(spaceID, currentID)
			if err != nil || node.ParentID == "" {
				return
			}
			currentID = node.ParentID
			continue
		}
		if nextID == "" {
			return
		}
		currentID = nextID
	}
}

// propagateOneAncestor runs a single CAS-retry loop for one ancestor in
// the propagation chain, serialised by the per-node mutex so concurrent
// commits to the same parent don't thrash each other. Returns the next
// ancestor's ID (or "" if at the space root) and true on success, or
// "" and false on CAS exhaustion / hard error (drift counter
// incremented by this function).
func (d *kvfsDriver) propagateOneAncestor(spaceID, nodeID string, delta int64) (string, bool) {
	unlock := d.lockParent(spaceID, nodeID)
	defer unlock()
	for attempt := 0; attempt < d.maxCASRetries(); attempt++ {
		node, rev, err := d.store.GetNode(spaceID, nodeID)
		if err != nil {
			return "", false
		}
		now := time.Now().UnixNano()
		if delta != 0 {
			node.Size += delta
			if node.Size < 0 {
				node.Size = 0
			}
		}
		node.MTime = now
		node.ETag = calculateEtag(node.ID, now)
		if err := d.store.PutNode(node, rev); err != nil {
			if err == ErrCASConflict {
				CASRetries.WithLabelValues("tree_propagation").Inc()
				casBackoff(attempt)
				continue
			}
			TreeSizeDrift.Inc()
			d.log.Warn().Err(err).Str("node_id", nodeID).Int64("delta", delta).Msg("tree propagation failed")
			return "", false
		}
		return node.ParentID, true
	}
	TreeSizeDrift.Inc()
	d.log.Warn().Str("node_id", nodeID).Int64("delta", delta).Msg("tree propagation CAS retries exhausted")
	return "", false
}

func (d *kvfsDriver) touchParent(ctx context.Context, spaceID, nodeID string) {
	d.putNodeWithCAS(spaceID, nodeID, func(node *NodeEntry) {
		now := time.Now().UnixNano()
		node.MTime = now
		node.ETag = calculateEtag(node.ID, now)
	})
}

// recursiveDeleteNodesAndBlobs removes all descendant nodes and their S3 blobs.
func (d *kvfsDriver) recursiveDeleteNodesAndBlobs(ctx context.Context, spaceID, nodeID string) {
	children, _, err := d.store.GetChildren(spaceID, nodeID)
	if err != nil {
		return
	}
	for _, childID := range children {
		child, _, err := d.store.GetNode(spaceID, childID)
		if err != nil {
			continue
		}
		if child.Type == NodeTypeDir {
			d.recursiveDeleteNodesAndBlobs(ctx, spaceID, childID)
		} else if child.BlobID != "" {
			d.blob.Delete(ctx, BlobKey(spaceID, child.BlobID))
		}
		d.store.DeleteNode(spaceID, childID)
		d.store.DeleteChildren(spaceID, childID)
	}
}

// recursiveDeleteNodes removes all descendant nodes of a directory.
func (d *kvfsDriver) recursiveDeleteNodes(ctx context.Context, spaceID, nodeID string) {
	children, _, err := d.store.GetChildren(spaceID, nodeID)
	if err != nil {
		return
	}
	for _, childID := range children {
		child, _, err := d.store.GetNode(spaceID, childID)
		if err != nil {
			continue
		}
		if child.Type == NodeTypeDir {
			d.recursiveDeleteNodes(ctx, spaceID, childID)
		}
		d.store.DeleteNode(spaceID, childID)
	}
	d.store.DeleteChildren(spaceID, nodeID)
}

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

	d.touchParent(commitCtx, spaceID, parentID)

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

// modifyGrant adds, removes, or updates a grant on a node with CAS retry.
func (d *kvfsDriver) modifyGrant(spaceID, nodeID string, g *provider.Grant, action string) error {
	grantKey := granteeKey(g.Grantee)
	for attempt := 0; attempt < d.maxCASRetries(); attempt++ {
		node, rev, err := d.store.GetNode(spaceID, nodeID)
		if err != nil {
			return err
		}

		if node.Grants == nil {
			node.Grants = make(map[string]*GrantEntry)
		}

		switch action {
		case "add", "update":
			node.Grants[grantKey] = cs3GrantToEntry(g)
		case "remove":
			delete(node.Grants, grantKey)
		}

		err = d.store.PutNode(node, rev)
		if err == ErrCASConflict {
			CASRetries.WithLabelValues("grant").Inc()
			d.log.Warn().Int("attempt", attempt).Str("space_id", spaceID).Str("node_id", nodeID).Str("action", action).Msg("modifyGrant: CAS conflict, retrying")
			casBackoff(attempt)
			continue
		}
		if err != nil {
			return err
		}
		return nil
	}
	CASExhausted.WithLabelValues("grant").Inc()
	return ErrCASConflict
}

// resolveAndAuthorize resolves a reference, loads the node, and checks a single
// permission flag. Returns the spaceID, nodeID, node, and assembled permissions.
// If the permission check fails, returns NotFound (to hide existence) unless the
// user has Stat access, in which case returns PermissionDenied.
func (d *kvfsDriver) resolveAndAuthorize(ctx context.Context, ref *provider.Reference, check func(*provider.ResourcePermissions) bool) (string, string, *NodeEntry, *provider.ResourcePermissions, error) {
	spaceID, nodeID, err := d.resolveRef(ctx, ref)
	if err != nil {
		return "", "", nil, nil, err
	}

	node, _, err := d.store.GetNode(spaceID, nodeID)
	if err != nil {
		if err == ErrNodeNotFound {
			return "", "", nil, nil, errtypes.NotFound(ref.String())
		}
		return "", "", nil, nil, err
	}

	rp := d.assemblePermissions(ctx, spaceID, node)
	if !check(rp) {
		if rp.Stat {
			return "", "", nil, nil, errtypes.PermissionDenied(ref.String())
		}
		return "", "", nil, nil, errtypes.NotFound(ref.String())
	}

	return spaceID, nodeID, node, rp, nil
}

// checkRecyclePermission loads the space root node and checks a recycle-bin
// permission flag. Returns the spaceID on success.
func (d *kvfsDriver) checkRecyclePermission(ctx context.Context, ref *provider.Reference, check func(*provider.ResourcePermissions) bool) (string, error) {
	spaceID := ref.GetResourceId().GetSpaceId()
	if spaceID == "" {
		return "", errtypes.BadRequest("missing space ID")
	}

	space, _, err := d.store.GetSpace(spaceID)
	if err != nil {
		return "", err
	}
	rootNode, _, err := d.store.GetNode(spaceID, space.RootID)
	if err != nil {
		return "", err
	}
	rp := d.assemblePermissions(ctx, spaceID, rootNode)
	if !check(rp) {
		return "", errtypes.PermissionDenied(spaceID)
	}

	return spaceID, nil
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

// updateChildrenWithCAS retries the get-modify-put cycle on a children map
// to handle concurrent modifications. Now serialised by lockParent so
// pod-local writers go through a sequential queue instead of fighting on
// CAS — cross-pod contention still goes through NATS CAS as before.
func (d *kvfsDriver) updateChildrenWithCAS(spaceID, parentID string, modify func(children ChildMap)) error {
	unlock := d.lockParent(spaceID, parentID)
	defer unlock()
	for attempt := 0; attempt < d.maxCASRetries(); attempt++ {
		children, rev, err := d.store.GetChildren(spaceID, parentID)
		if err != nil {
			return err
		}
		modify(children)
		err = d.store.PutChildren(spaceID, parentID, children, rev)
		if err == nil {
			return nil
		}
		if err != ErrCASConflict {
			return err
		}
		CASRetries.WithLabelValues("children").Inc()
		casBackoff(attempt)
	}
	CASExhausted.WithLabelValues("children").Inc()
	return ErrCASConflict
}

func (d *kvfsDriver) updateChildrenWithCASCheck(spaceID, parentID string, modify func(children ChildMap) error) error {
	unlock := d.lockParent(spaceID, parentID)
	defer unlock()
	for attempt := 0; attempt < d.maxCASRetries(); attempt++ {
		children, rev, err := d.store.GetChildren(spaceID, parentID)
		if err != nil {
			return err
		}
		if err := modify(children); err != nil {
			return err
		}
		err = d.store.PutChildren(spaceID, parentID, children, rev)
		if err == nil {
			return nil
		}
		if err != ErrCASConflict {
			return err
		}
		CASRetries.WithLabelValues("children").Inc()
		casBackoff(attempt)
	}
	CASExhausted.WithLabelValues("children").Inc()
	return ErrCASConflict
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
	for attempt := 0; attempt < d.maxCASRetries(); attempt++ {
		node, rev, err := d.store.GetNode(spaceID, nodeID)
		if err != nil {
			return err
		}
		modify(node)
		err = d.store.PutNode(node, rev)
		if err == nil {
			return nil
		}
		if err != ErrCASConflict {
			return err
		}
		CASRetries.WithLabelValues("node").Inc()
		casBackoff(attempt)
	}
	CASExhausted.WithLabelValues("node").Inc()
	return ErrCASConflict
}

func (d *kvfsDriver) putNodeWithCASCheck(spaceID, nodeID string, modify func(node *NodeEntry) error) error {
	unlock := d.lockParent(spaceID, nodeID)
	defer unlock()
	for attempt := 0; attempt < d.maxCASRetries(); attempt++ {
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
		err = d.store.PutNode(node, rev)
		if err == nil {
			return nil
		}
		if err != ErrCASConflict {
			return err
		}
		CASRetries.WithLabelValues("node").Inc()
		casBackoff(attempt)
	}
	CASExhausted.WithLabelValues("node").Inc()
	return ErrCASConflict
}

// allPermissions returns the full owner/service-account permission set.
func allPermissions() *provider.ResourcePermissions {
	return &provider.ResourcePermissions{
		AddGrant:             true,
		CreateContainer:      true,
		Delete:               true,
		GetPath:              true,
		GetQuota:             true,
		InitiateFileDownload: true,
		InitiateFileUpload:   true,
		ListContainer:        true,
		ListFileVersions:     true,
		ListGrants:           true,
		ListRecycle:          true,
		Move:                 true,
		PurgeRecycle:         true,
		RemoveGrant:          true,
		RestoreFileVersion:   true,
		RestoreRecycleItem:   true,
		Stat:                 true,
		UpdateGrant:          true,
		DenyGrant:            true,
	}
}

func noPermissions() *provider.ResourcePermissions {
	return &provider.ResourcePermissions{}
}

func addPermissions(dst, src *provider.ResourcePermissions) {
	dst.AddGrant = dst.AddGrant || src.AddGrant
	dst.CreateContainer = dst.CreateContainer || src.CreateContainer
	dst.Delete = dst.Delete || src.Delete
	dst.GetPath = dst.GetPath || src.GetPath
	dst.GetQuota = dst.GetQuota || src.GetQuota
	dst.InitiateFileDownload = dst.InitiateFileDownload || src.InitiateFileDownload
	dst.InitiateFileUpload = dst.InitiateFileUpload || src.InitiateFileUpload
	dst.ListContainer = dst.ListContainer || src.ListContainer
	dst.ListFileVersions = dst.ListFileVersions || src.ListFileVersions
	dst.ListGrants = dst.ListGrants || src.ListGrants
	dst.ListRecycle = dst.ListRecycle || src.ListRecycle
	dst.Move = dst.Move || src.Move
	dst.PurgeRecycle = dst.PurgeRecycle || src.PurgeRecycle
	dst.RemoveGrant = dst.RemoveGrant || src.RemoveGrant
	dst.RestoreFileVersion = dst.RestoreFileVersion || src.RestoreFileVersion
	dst.RestoreRecycleItem = dst.RestoreRecycleItem || src.RestoreRecycleItem
	dst.Stat = dst.Stat || src.Stat
	dst.UpdateGrant = dst.UpdateGrant || src.UpdateGrant
	dst.DenyGrant = dst.DenyGrant || src.DenyGrant
}

// assemblePermissions determines the effective permissions for the current user
// on the given node, by checking space ownership and walking the node tree
// from the target up to the space root collecting grants.
func (d *kvfsDriver) assemblePermissions(ctx context.Context, spaceID string, node *NodeEntry) *provider.ResourcePermissions {
	u, ok := ctxpkg.ContextGetUser(ctx)
	if !ok || u == nil {
		d.log.Debug().Str("space_id", spaceID).Str("node_id", node.ID).Msg("assemblePermissions: no user in context")
		return noPermissions()
	}

	uid := u.GetId().GetOpaqueId()

	if u.GetId().GetType() == user.UserType_USER_TYPE_SERVICE {
		d.log.Debug().Str("user", uid).Str("space_id", spaceID).Msg("assemblePermissions: service account → allPermissions")
		return allPermissions()
	}

	space, _, err := d.store.GetSpace(spaceID)
	if err == nil && space.Owner == uid {
		d.log.Debug().Str("user", uid).Str("space_id", spaceID).Msg("assemblePermissions: user is space owner → allPermissions")
		return allPermissions()
	}

	ap := noPermissions()
	cur := node
	visited := make(map[string]bool)
	for cur != nil && !visited[cur.ID] {
		visited[cur.ID] = true
		d.collectGrants(cur, u, ap)
		if cur.ParentID == "" || cur.ParentID == cur.ID {
			break
		}
		parent, _, err := d.store.GetNode(spaceID, cur.ParentID)
		if err != nil {
			d.log.Warn().Err(err).Str("space_id", spaceID).Str("parent_id", cur.ParentID).Msg("assemblePermissions: parent lookup failed")
			break
		}
		cur = parent
	}

	return ap
}

// collectGrants accumulates matching user/group grants from a node.
func (d *kvfsDriver) collectGrants(node *NodeEntry, u *user.User, ap *provider.ResourcePermissions) {
	if node.Grants == nil {
		return
	}
	userKey := "u:" + u.Id.OpaqueId
	if g, ok := node.Grants[userKey]; ok {
		addPermissions(ap, uint32ToPermissions(g.Permissions))
	}
	for _, gid := range u.Groups {
		groupKey := "g:" + gid
		if g, ok := node.Grants[groupKey]; ok {
			addPermissions(ap, uint32ToPermissions(g.Permissions))
		}
	}
}

// hasGrantForUser returns true if the node has any grant matching the user
// (direct user grant or group membership grant).
func (d *kvfsDriver) hasGrantForUser(node *NodeEntry, u *user.User) bool {
	if node == nil || node.Grants == nil {
		return false
	}
	if _, ok := node.Grants["u:"+u.Id.OpaqueId]; ok {
		return true
	}
	for _, gid := range u.Groups {
		if _, ok := node.Grants["g:"+gid]; ok {
			return true
		}
	}
	return false
}

// spaceToCS3 converts a SpaceEntry + root node to a CS3 StorageSpace.
func (d *kvfsDriver) spaceToCS3(space *SpaceEntry, rootNode *NodeEntry) *provider.StorageSpace {
	// Build the space alias for drive resolution (personal/username, project/name)
	spaceAlias := space.Type + "/" + strings.ReplaceAll(strings.ToLower(space.Name), " ", "-")

	ss := &provider.StorageSpace{
		Id:        &provider.StorageSpaceId{OpaqueId: space.ID},
		Root:      &provider.ResourceId{SpaceId: space.ID, OpaqueId: space.RootID},
		Name:      space.Name,
		SpaceType: space.Type,
		Owner: &user.User{
			Id: &user.UserId{OpaqueId: space.Owner},
		},
		Mtime: &types.Timestamp{
			Seconds: uint64(space.MTime / int64(time.Second)),
			Nanos:   uint32(space.MTime % int64(time.Second)),
		},
	}

	// Add spaceAlias to Opaque (required for Graph API driveAlias and legacy path resolution)
	ss.Opaque = utils.AppendPlainToOpaque(ss.Opaque, "spaceAlias", spaceAlias)

	if space.Quota >= 0 {
		ss.Quota = &provider.Quota{
			QuotaMaxBytes: uint64(space.Quota),
		}
	}

	if rootNode != nil {
		totalBytes := uint64(0)
		if space.Quota > 0 {
			totalBytes = uint64(space.Quota)
		}
		usedBytes := uint64(rootNode.Size)
		remainingBytes := uint64(0)
		if totalBytes > usedBytes {
			remainingBytes = totalBytes - usedBytes
		}
		ss.Quota = &provider.Quota{
			QuotaMaxBytes:  totalBytes,
			QuotaMaxFiles:  0,
			RemainingBytes: remainingBytes,
		}
		ss.Opaque = utils.AppendPlainToOpaque(ss.Opaque, "etag", rootNode.ETag)

		// Populate grants in Opaque (required for Graph API member display)
		if len(rootNode.Grants) > 0 {
			grantMap := make(map[string]*provider.ResourcePermissions, len(rootNode.Grants))
			groupMap := make(map[string]struct{})
			for key, ge := range rootNode.Grants {
				grantMap[ge.GranteeID] = uint32ToPermissions(ge.Permissions)
				if strings.HasPrefix(key, "g:") {
					groupMap[ge.GranteeID] = struct{}{}
				}
			}
			ss.Opaque = utils.AppendJSONToOpaque(ss.Opaque, "grants", grantMap)
			if len(groupMap) > 0 {
				ss.Opaque = utils.AppendJSONToOpaque(ss.Opaque, "groups", groupMap)
			}
		}
	}

	return ss
}

// --- Helper functions ---

// calculateEtag generates an etag from a node ID and modification time.
func calculateEtag(nodeID string, mtime int64) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", nodeID, mtime)))
	return fmt.Sprintf("%x", h[:8])
}

// granteeKey returns a unique key for a grantee.
func granteeKey(g *provider.Grantee) string {
	if g == nil {
		return ""
	}
	switch g.Type {
	case provider.GranteeType_GRANTEE_TYPE_USER:
		return "u:" + g.GetUserId().OpaqueId
	case provider.GranteeType_GRANTEE_TYPE_GROUP:
		return "g:" + g.GetGroupId().OpaqueId
	default:
		return ""
	}
}

// cs3GrantToEntry converts a CS3 Grant to a GrantEntry for storage.
func cs3GrantToEntry(g *provider.Grant) *GrantEntry {
	entry := &GrantEntry{}
	if g.Grantee != nil {
		switch g.Grantee.Type {
		case provider.GranteeType_GRANTEE_TYPE_USER:
			entry.GranteeType = "user"
			entry.GranteeID = g.Grantee.GetUserId().OpaqueId
		case provider.GranteeType_GRANTEE_TYPE_GROUP:
			entry.GranteeType = "group"
			entry.GranteeID = g.Grantee.GetGroupId().OpaqueId
		}
	}
	if g.Permissions != nil {
		entry.Permissions = permissionsToUint32(g.Permissions)
	}
	return entry
}

// grantEntryToCS3 converts a stored GrantEntry to a CS3 Grant.
func grantEntryToCS3(entry *GrantEntry) *provider.Grant {
	grant := &provider.Grant{
		Grantee:     &provider.Grantee{},
		Permissions: uint32ToPermissions(entry.Permissions),
	}
	switch entry.GranteeType {
	case "user":
		grant.Grantee.Type = provider.GranteeType_GRANTEE_TYPE_USER
		grant.Grantee.Id = &provider.Grantee_UserId{UserId: &user.UserId{OpaqueId: entry.GranteeID}}
	case "group":
		grant.Grantee.Type = provider.GranteeType_GRANTEE_TYPE_GROUP
		grant.Grantee.Id = &provider.Grantee_GroupId{GroupId: &group.GroupId{OpaqueId: entry.GranteeID}}
	}
	return grant
}

// permissionsToUint32 encodes resource permissions as a bitmask.
func permissionsToUint32(p *provider.ResourcePermissions) uint32 {
	var bits uint32
	if p.AddGrant {
		bits |= 1 << 0
	}
	if p.CreateContainer {
		bits |= 1 << 1
	}
	if p.Delete {
		bits |= 1 << 2
	}
	if p.GetPath {
		bits |= 1 << 3
	}
	if p.GetQuota {
		bits |= 1 << 4
	}
	if p.InitiateFileDownload {
		bits |= 1 << 5
	}
	if p.InitiateFileUpload {
		bits |= 1 << 6
	}
	if p.ListContainer {
		bits |= 1 << 7
	}
	if p.ListFileVersions {
		bits |= 1 << 8
	}
	if p.ListGrants {
		bits |= 1 << 9
	}
	if p.ListRecycle {
		bits |= 1 << 10
	}
	if p.Move {
		bits |= 1 << 11
	}
	if p.PurgeRecycle {
		bits |= 1 << 12
	}
	if p.RemoveGrant {
		bits |= 1 << 13
	}
	if p.RestoreFileVersion {
		bits |= 1 << 14
	}
	if p.RestoreRecycleItem {
		bits |= 1 << 15
	}
	if p.Stat {
		bits |= 1 << 16
	}
	if p.UpdateGrant {
		bits |= 1 << 17
	}
	if p.DenyGrant {
		bits |= 1 << 18
	}
	return bits
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

// uint32ToPermissions decodes a bitmask into resource permissions.
func uint32ToPermissions(bits uint32) *provider.ResourcePermissions {
	return &provider.ResourcePermissions{
		AddGrant:             bits&(1<<0) != 0,
		CreateContainer:      bits&(1<<1) != 0,
		Delete:               bits&(1<<2) != 0,
		GetPath:              bits&(1<<3) != 0,
		GetQuota:             bits&(1<<4) != 0,
		InitiateFileDownload: bits&(1<<5) != 0,
		InitiateFileUpload:   bits&(1<<6) != 0,
		ListContainer:        bits&(1<<7) != 0,
		ListFileVersions:     bits&(1<<8) != 0,
		ListGrants:           bits&(1<<9) != 0,
		ListRecycle:          bits&(1<<10) != 0,
		Move:                 bits&(1<<11) != 0,
		PurgeRecycle:         bits&(1<<12) != 0,
		RemoveGrant:          bits&(1<<13) != 0,
		RestoreFileVersion:   bits&(1<<14) != 0,
		RestoreRecycleItem:   bits&(1<<15) != 0,
		Stat:                 bits&(1<<16) != 0,
		UpdateGrant:          bits&(1<<17) != 0,
		DenyGrant:            bits&(1<<18) != 0,
	}
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
