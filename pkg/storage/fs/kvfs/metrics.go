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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// CASRetries counts every CAS retry attempt, labeled by operation type.
	CASRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kvfs_cas_retries_total",
		Help: "Total number of CAS retry attempts in the kvfs driver",
	}, []string{"operation"})

	// CASExhausted counts CAS retry loops that exited without success.
	CASExhausted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kvfs_cas_exhausted_total",
		Help: "Total number of CAS retry loops that exhausted all attempts",
	}, []string{"operation"})

	// CASConflicts counts every ErrCASConflict or wrong-last-sequence error, labeled by bucket.
	CASConflicts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kvfs_cas_conflicts_total",
		Help: "Total number of CAS conflict errors detected",
	}, []string{"bucket"})

	// MaxCASRetriesGauge exports the effective CAS retry bound per kvfs
	// instance. The prefix label keeps the two instances distinct, since
	// both register into the same in-process registry.
	MaxCASRetriesGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kvfs_max_cas_retries",
		Help: "Effective MaxCASRetries bound per kvfs instance (labeled by bucket prefix)",
	}, []string{"prefix"})

	// MaxDeleteDepthGauge exports the effective recursive-delete depth
	// bound per kvfs instance — same dead-config seam as
	// MaxCASRetriesGauge: a scraped value diverging from the deployment
	// setting means the env var is not reaching the driver.
	MaxDeleteDepthGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kvfs_max_delete_depth",
		Help: "Effective MaxDeleteDepth bound per kvfs instance (labeled by bucket prefix)",
	}, []string{"prefix"})

	// KVOperationDuration observes the duration of NATS KV operations.
	KVOperationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kvfs_kv_operation_duration_seconds",
		Help:    "Duration of NATS KV operations in seconds",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
	}, []string{"bucket", "op"})

	// BlobOperationDuration observes the duration of S3 blob operations.
	BlobOperationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kvfs_blob_operation_duration_seconds",
		Help:    "Duration of S3 blob operations in seconds",
		Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0},
	}, []string{"op"})

	// UploadInFlight tracks the number of currently active uploads.
	// Per-pod-lifetime counter — increments on TUS session create, decrements
	// only on Finish/Terminate. Leaks on client-side aborts. Treat as a
	// per-pod debugging hint; for authoritative state use OldestUploadAge or
	// scan oc-uploads directly.
	UploadInFlight = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kvfs_upload_in_flight",
		Help: "Number of uploads currently in progress (per-pod lifetime counter; may drift — see kvfs_oldest_upload_age_seconds)",
	}, []string{"protocol"})

	// OldestUploadAgeSeconds is the age of the oldest in-flight upload
	// session, refreshed by the per-pod upload-staleness sampler. A
	// growing value means TUS sessions are accumulating without being
	// finished or reaped — usually a client-cancel leak. Labeled by
	// bucket prefix: every driver instance in the process runs its own
	// sampler against the shared registry, so an unlabeled gauge would be
	// overwritten by whichever instance ticked last, hiding the others.
	OldestUploadAgeSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kvfs_oldest_upload_age_seconds",
		Help: "Age of the oldest in-flight TUS upload session in seconds (0 if none), per bucket prefix",
	}, []string{"prefix"})

	// UploadSessionsTotal is the total number of TUS upload sessions
	// currently in the uploads bucket, refreshed by the per-pod sampler.
	// Prefix-labeled for the same reason as OldestUploadAgeSeconds.
	UploadSessionsTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kvfs_upload_sessions_total",
		Help: "Total number of TUS upload sessions in the uploads bucket, per bucket prefix",
	}, []string{"prefix"})

	// ChildrenValueBytes observes the msgpack-encoded byte size of the
	// per-parent children map on every PutChildren. Alert at
	// > 0.5 * MaxValueSize so fat directories surface before they hit the
	// NATS KV ceiling (1 MiB default, raise via STORAGE_USERS_KVFS_CHILDREN_MAX_VALUE_SIZE).
	ChildrenValueBytes = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "kvfs_children_value_bytes",
		Help:    "Serialised size of a parent's children map in bytes (per PutChildren)",
		Buckets: []float64{256, 1024, 4096, 16384, 65536, 262144, 524288, 1048576, 4194304, 16777216},
	})

	// UploadAbortedOnCancel counts uploads aborted because the client
	// cancelled mid-PATCH. Each increment ties to one S3 multipart abort.
	UploadAbortedOnCancel = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kvfs_upload_aborted_on_cancel_total",
		Help: "TUS uploads terminated because the request context was cancelled",
	})

	// IdempotentOverwriteDetected counts uploads where the If-Match etag
	// mismatched but the existing node already had the same content
	// (checksum + size). This detects 504-retry replays and prevents
	// the client from entering a permanent 412 loop.
	IdempotentOverwriteDetected = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kvfs_idempotent_overwrite_detected_total",
		Help: "Uploads detected as idempotent replays (504 retry recovery)",
	})

	// EventQueueDropped counts events dropped because the async publish
	// queue was full. Non-zero values mean we are losing observability
	// events; raise the buffer or reduce event volume.
	EventQueueDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kvfs_event_queue_dropped_total",
		Help: "Events dropped because the async publish queue was full",
	})

	// EventQueueDepth gauges the current depth of the async publish queue.
	EventQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kvfs_event_queue_depth",
		Help: "Current depth of the async event publish queue",
	})

	// TUSPhaseDuration is the per-phase latency histogram for TUS uploads.
	// Phases: initiate (createTUSSession) / write_chunk / finish /
	// blob_upload / commit_node. A `write_chunk` p99 above ~10 ms usually
	// means the cache path regressed and chunks are hitting remote I/O.
	TUSPhaseDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "kvfs_tus_phase_duration_seconds",
		Help: "TUS upload phase duration in seconds",
		// Extra resolution between 50 ms and 500 ms: the NATS-backed
		// write_chunk floor lives there, and the old 0.1→0.5 gap reported
		// any 100-500 ms p99 as a flat 500 ms.
		Buckets: []float64{0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.2, 0.3, 0.5, 1.0, 5.0, 30.0},
	}, []string{"phase"})

	UploadCachePublishDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "kvfs_upload_cache_publish_duration_seconds",
		Help:    "Latency of each NATS JetStream PublishMsg in the upload cache",
		Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1.0, 5.0},
	})

	// TempFileBytesInFlight is the current total bytes held in the upload
	// cache across all in-flight sessions on this pod. Read directly from
	// the diskUploadCache via SetTempFileBytesInFlight() on each upload event.
	// Alert at >80% of the volume sizeLimit to catch storage exhaustion
	// before the next WriteChunk fails with ENOSPC.
	TempFileBytesInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kvfs_temp_file_bytes_in_flight",
		Help: "Total bytes currently held in the upload cache (per-pod)",
	})

	// TreeSizeDrift counts ancestor CAS failures during tree-size propagation.
	TreeSizeDrift = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kvfs_tree_size_drift_total",
		Help: "Total number of ancestor CAS failures during tree-size propagation",
	})

	// GCBlobsScanned counts the total number of S3 blobs scanned by GC.
	GCBlobsScanned = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kvfs_gc_blobs_scanned_total",
		Help: "Total number of S3 blobs scanned by garbage collection",
	})

	// GCBlobsDeleted counts S3 blobs actually deleted by GC.
	GCBlobsDeleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kvfs_gc_blobs_deleted_total",
		Help: "Total number of orphaned S3 blobs deleted by garbage collection",
	})

	// GCWouldDelete counts blobs that would be deleted in dry-run mode.
	GCWouldDelete = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kvfs_gc_would_delete_total",
		Help: "Total number of orphaned S3 blobs that would be deleted (dry-run mode)",
	})

	// GCErrors counts errors encountered during GC runs.
	GCErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kvfs_gc_errors_total",
		Help: "Total number of errors during garbage collection runs",
	})

	// GCRunDuration observes the duration of each GC run.
	GCRunDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "kvfs_gc_run_duration_seconds",
		Help:    "Duration of garbage collection runs in seconds",
		Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600},
	})

	// GCOrphanPersonalSpacesDeleted counts orphaned personal spaces (owner
	// no longer a live user) reaped by GC via the standard DeleteStorageSpace
	// path. This is the identity-orphan half of the 1:1:1 guarantee.
	GCOrphanPersonalSpacesDeleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kvfs_gc_orphan_personal_spaces_deleted_total",
		Help: "Total number of orphaned personal spaces (dead owner) reaped by GC",
	})

	// GCOrphanProjectSpaces is the number of orphaned project/virtual spaces
	// (dead owner) observed on the most recent sweep. These are SURFACED for
	// manual review, never auto-deleted — deleting a shared space because its
	// owner was deprovisioned would destroy data the team still wants.
	GCOrphanProjectSpaces = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kvfs_gc_orphan_project_spaces",
		Help: "Orphaned project/virtual spaces (dead owner) needing manual review (last sweep)",
	})

	// GCIdentityReapingSkipped counts sweeps that skipped identity reaping
	// because the user resolver was unavailable or returned an empty set.
	// A non-zero rate means the safety guard fired (never delete on a
	// transient/empty live-user list) — investigate the user API, not the GC.
	GCIdentityReapingSkipped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kvfs_gc_identity_reaping_skipped_total",
		Help: "Total number of GC sweeps that skipped identity reaping (resolver error/empty)",
	})

	// GCIdentityUnknownOwners is the number of distinct space owners whose
	// liveness could NOT be determined on the most recent sweep that ran
	// identity reaping (per-owner resolver RPC errors / ambiguous statuses,
	// or a whole-lookup failure). Such owners are conservatively left un-reaped
	// ("unknown → never reap"), so a persistently non-zero value means a
	// degraded identity backend is silently suppressing orphan reaping — the
	// gap that made per-owner failures invisible before this gauge existed. Set per sweep
	// (0 on a clean sweep) by the resolver-enabled instance only; the
	// storage-system instance has a nil resolver and never writes it.
	GCIdentityUnknownOwners = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kvfs_gc_identity_unknown_owners",
		Help: "Distinct space owners whose liveness could not be determined on the last sweep (>0 => degraded identity backend, reaping suppressed)",
	})

	// GCResidueKeysDeleted counts KV entries (nodes/versions/trash/uploads)
	// reaped by the internal-consistency sweep because their owning space no
	// longer exists in oc-spaces (crash-mid-delete / best-effort residue).
	GCResidueKeysDeleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kvfs_gc_residue_keys_deleted_total",
		Help: "Total number of KV entries of deleted spaces reaped by the GC residue sweep",
	})

	// GCResidueBlobsDeleted counts S3 blobs reaped by the internal-consistency
	// sweep because their owning space no longer exists in oc-spaces.
	GCResidueBlobsDeleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kvfs_gc_residue_blobs_deleted_total",
		Help: "Total number of S3 blobs of deleted spaces reaped by the GC residue sweep",
	})

	// GCChildrenReconciled counts oc-children entries removed because the
	// child's authoritative ParentID disagrees with the parent directory
	// (stale Move residue) or the child no longer exists (dangling ref).
	GCChildrenReconciled = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kvfs_gc_children_reconciled_total",
		Help: "Total number of stale oc-children entries removed by the GC reconciler",
	})

	// GCTrashReconciled counts oc-trash entries removed because the
	// referenced node is already live and reachable in the tree
	// (RestoreRecycleItem crash residue).
	GCTrashReconciled = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kvfs_gc_trash_reconciled_total",
		Help: "Total number of stale oc-trash entries removed by the GC consistency sweep",
	})

	// GCExpiredUploadsCleaned counts upload sessions reaped by GC: past
	// their Expires stamp, or carrying no expiry and older than the
	// session TTL. rate() of this vs the TUS initiate rate answers
	// "are sessions created faster than reaped?".
	GCExpiredUploadsCleaned = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kvfs_gc_expired_uploads_cleaned_total",
		Help: "Total upload sessions reaped by GC (expired, or no expiry and older than the session TTL)",
	})

	// GCCorruptUploadsReaped counts uploads-bucket entries whose value no
	// longer unmarshals. Such entries are invisible to typed listings and
	// would otherwise live forever; GC reaps them once they exceed the
	// session TTL.
	GCCorruptUploadsReaped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kvfs_gc_corrupt_uploads_reaped_total",
		Help: "Total unparseable upload entries reaped by GC after exceeding the session TTL",
	})
)
