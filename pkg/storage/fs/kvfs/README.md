# KVFS — NATS JetStream KV + S3 storage driver for reva

> **Status: alpha.** KVFS is under active development. The on-disk format,
> driver options, metrics, and operational characteristics may still change.
> Contributed in good faith under the Apache License 2.0 with **no
> warranties or guarantees of fitness for any particular purpose** — see
> [`LICENSE`](../../../../../../../LICENSE).

KVFS is a `storage.FS` implementation that stores **metadata in NATS JetStream KV
buckets** and **blobs in any S3-compatible object store**. It is designed as a
peer to the canonical `decomposedfs` / `posix` driver, but **without any local
disk state**: every byte of file content and every piece of metadata lives in a
network-replicated backend, so OpenCloud pods can be stateless and scaled
horizontally.

## Origin and acknowledgments

KVFS was designed and authored by **Bernard Gutermann** at **Sekops Sàrl**
(<https://sekops.ch>), motivated by the architectural direction the OpenCloud
maintainers articulated in
[opencloud-eu/opencloud#1314](https://github.com/opencloud-eu/opencloud/issues/1314)
(NATS-backed upload session storage on top of S3 blobs, removing the RWX volume
requirement). KVFS generalises that idea to all metadata.

The driver was incubated against a multi-node Kubernetes deployment before
being proposed upstream. Source contributed under the Apache License 2.0;
copyright retained by Sekops Sàrl per the license terms — see per-file headers
for the full copyright notice.

## 1. Overview

| | KVFS | decomposedfs / posix |
|---|---|---|
| Metadata store | NATS JetStream KV (msgpack values) | Local filesystem (xattrs / sidecar files) |
| Blob store | S3 (minio, AWS, …) | Local filesystem or S3 |
| Pod state | Stateless (no PVCs) | Requires a shared filesystem (PVC, NFS, CephFS) |
| HA model | NATS R≥3 + S3 replication | Filesystem-dependent |
| Concurrency | Optimistic CAS on KV revisions | File locks + xattr CAS |
| Target use case | Cloud-native, horizontally scaled deployments | Single-host or shared-FS deployments |

KVFS implements the `storage.FS` interface defined in
[`reva/v2/pkg/storage/storage.go`](../../storage.go). Two instances of KVFS
typically run side-by-side in OpenCloud, one for `storage-users` and one for
`storage-system`, distinguished by `bucket_prefix` (default `oc-` and `sys-`).

## 2. Architecture

```
                ┌─────────────────────────────────────────┐
                │  OpenCloud pod (storage-users / system) │
                │                                         │
                │   ┌───────────────────────────────┐     │
                │   │   storage.FS  (kvfsDriver)    │     │
                │   └───────────┬───────────────────┘     │
                │       ┌───────┴────────┐                │
                │       ▼                ▼                │
                │   KVStore           BlobStore           │
                │  (kvstore.go)      (blobstore.go)       │
                │       │                │                │
                │       │       ┌────────┘                │
                │       │       │   blobGC (gc.go)        │
                │       │       │                         │
                └───────┼───────┼─────────────────────────┘
                        │       │
                        ▼       ▼
              ┌─────────────────────┐  ┌─────────────────────┐
              │  NATS JetStream KV  │  │  S3 (minio / AWS)   │
              │  7 buckets, R≥3     │  │  bucket: opencloud  │
              └─────────────────────┘  └─────────────────────┘
```

Components:

- **`kvfsDriver`** ([`kvfs.go`](kvfs.go)) — implements every `storage.FS`
  method; orchestrates KV + S3.
- **`KVStore`** ([`kvstore.go`](kvstore.go)) — typed wrapper around NATS KV
  with msgpack codec, CAS, and generic list helpers.
- **`BlobStore`** ([`blobstore.go`](blobstore.go)) — minio-go client; single
  PUT/GET + S3 multipart upload primitives.
- **`blobGC`** ([`gc.go`](gc.go)) — distributed garbage collector for orphaned
  blobs and expired upload sessions.
- **Metrics** ([`metrics.go`](metrics.go)) — 12 Prometheus collectors.
- **`Options`** ([`options.go`](options.go)) — config struct with `mapstructure`
  bindings.

## 3. Data model

### 3.1 NATS KV buckets

Seven buckets, each prefixed with `Options.BucketPrefix` (default `"oc"`).
All values are msgpack-encoded; KV revision numbers drive CAS.

| Bucket | Key | Value type | Purpose |
|---|---|---|---|
| `<prefix>-nodes` | `<spaceID>.<nodeID>` | [`NodeEntry`](node.go#L38) | Per-node metadata (file or directory): name, size, mtime, blob ID, mime, owner, checksum, grants, lock, favorites, custom metadata |
| `<prefix>-children` | `<spaceID>.<parentID>` | [`ChildMap`](kvstore.go) (`map[name]nodeID`) | Directory listing |
| `<prefix>-spaces` | `<spaceID>` | [`SpaceEntry`](node.go#L90) | Space root (type, owner, name, quota, root nodeID) |
| `<prefix>-trash` | `<spaceID>.<trashKey>` | [`TrashEntry`](node.go#L101) | Soft-deleted item with full node snapshot + original path |
| `<prefix>-versions` | `<spaceID>.<nodeID>.<versionKey>` | [`VersionEntry`](node.go#L113) | File revision (separate blob ID, size, etag, checksum) |
| `<prefix>-uploads` | `<uploadID>` | [`UploadSession`](node.go#L126) | TUS multipart upload state (S3 multipart ID, parts, offset, expiry, if-match) |
| `<prefix>-locks` | `<lockKey>` (e.g. `gc-sweep`) | JSON `kvLockEntry` | Distributed locks for cross-pod coordination |

**Why msgpack:** compact, schema-flexible, widely supported. msgpack tags on
each struct field are short (2–4 chars) to minimize KV value size.

### 3.2 S3 layout

Blobs live under `s3://<bucket>/<spaceID>/<pathified-blobID>`. The
"pathification" splits a flat UUID into a 2-level hex prefix to keep S3 list
operations bounded (see `BlobKey()` in [`blobstore.go`](blobstore.go)). A node
references its blob by `BlobID`; versions reference their own independent
blobs.

### 3.3 Identity model

- **Node ID = UUID**, stable for the lifetime of the node (and persists across
  rename/move).
- **Space ID = UUID**, equal to the **root node ID** for personal/project
  spaces (KVFS invariant; see `ToResourceInfo` in [`node.go:184`](node.go#L184)).
- **Path is derived**, not stored: `buildPath` walks `ParentID` chains from the
  node up to the space root.

## 4. Operations — `storage.FS` mapping

How each interface method translates to backend operations. "CAS-retried" means
the operation runs in a `defaultMaxCASRetries = 10` loop with exponential
backoff (1–50 ms + jitter); see [`kvfs.go:54`](kvfs.go#L54).

### 4.1 Namespace

| Method | KV ops | S3 ops | Notes |
|---|---|---|---|
| `CreateDir` | CAS `nodes` create + CAS `children` of parent + tree-size propagation | — | Parent ETag bumped via `touchParent` |
| `TouchFile` | CAS `nodes` create or update + CAS `children` of parent | — | Optional `processing` flag for async upload pipelines |
| `Move` | CAS update of (old parent `children`, new parent `children`, node `ParentID`) — 3-phase | — | Same-name overwrite supported; emits `ItemMoved` |
| `Delete` | Snapshot to `trash` + remove from parent `children` | — | Soft delete; emits `ItemTrashed`. Blob deletion deferred until `PurgeRecycleItem` |
| `CreateReference` | — | — | **Not supported** — symlinks are out of scope |
| `GetPathByID` | Walk `ParentID` via `nodes` reads | — | |
| `GetMD` | `nodes` get | — | Permission-gated |
| `ListFolder` | `children` get + batched `nodes` get (`GetNodes`, 50-way concurrency) | — | Permission-filtered |

### 4.2 Data plane

| Method | KV ops | S3 ops | Notes |
|---|---|---|---|
| `Upload` (simple PUT) | `commitFileNode` CAS-retry | `PUT` (single object) | Streams body → S3 while computing SHA-1; emits `FileUploaded` |
| `InitiateUpload` (TUS) | `PutUpload` (new session) | `InitiateMultipartUpload` | Returns TUS session ID in storage map |
| `WriteChunk` (TUS) | `PutUpload` (offset++) | `UploadPart` | Per-chunk; gauge `kvfs_upload_in_flight{protocol="tus"}` |
| `FinishUpload` (TUS) | `commitNode` CAS-retry + `DeleteUpload` | `CompleteMultipartUpload` | Computes SHA-1 from concatenation; emits `FileUploaded` |
| `Terminate` (TUS) | `DeleteUpload` | `AbortMultipartUpload` | |
| `Download` | `nodes` get | `GET` (stream) | |

### 4.3 Spaces

| Method | KV ops | S3 ops | Notes |
|---|---|---|---|
| `CreateStorageSpace` | `PutSpace` + create root `NodeEntry` | — | Personal & project; root node ID == space ID |
| `ListStorageSpaces` | `ListSpaces` (filtered scan) | — | Filters: owner, type, ID, `+grant:` membership |
| `UpdateStorageSpace` | CAS `PutSpace` | — | Updates name, quota |
| `DeleteStorageSpace` | Recursive `nodes` + `children` + `trash` + `versions` deletion | Cascading blob deletes | Hard delete |
| `GetQuota` | Sum of root `NodeEntry.Size` (treesize) | — | Returns (total, used, remaining) |
| `CreateHome` / `GetHome` | — | — | `CreateHome` is a no-op; `GetHome` returns `"/"` so the gateway resolves home via WebdavNamespace |

### 4.4 Trash, versions, grants, locks, metadata

| Method | KV ops | S3 ops | Notes |
|---|---|---|---|
| `ListRecycle` | `ListTrash(spaceID)` | — | |
| `RestoreRecycleItem` | `PutNode` + parent `children` update + `DeleteTrash` | — | `restoreRef` supports move-on-restore; idempotent (`NotFound` on missing key) |
| `PurgeRecycleItem` | `DeleteTrash` | `Delete` (blob) | Idempotent |
| `EmptyRecycle` | Bulk `DeleteTrash` | Bulk `Delete` | |
| `ListRevisions` | `ListVersions(spaceID, nodeID)` | — | |
| `DownloadRevision` | `GetVersion` | `GET` | |
| `RestoreRevision` | `PutVersion` (current → new version) + CAS update node | Blob copy (server-side) | Trims to `MaxVersions` |
| `AddGrant` / `RemoveGrant` / `UpdateGrant` | CAS update of `NodeEntry.Grants` | — | Inline ACL; CAS-retried |
| `DenyGrant` | — | — | **Not supported** |
| `ListGrants` | `nodes` get | — | Populates `Opaque` with role metadata |
| `GetLock` / `SetLock` / `RefreshLock` / `Unlock` | CAS update of `NodeEntry.Lock` | — | Inline lock with TTL; expired locks treated as absent |
| `SetArbitraryMetadata` / `UnsetArbitraryMetadata` | CAS update of `NodeEntry.Metadata` | — | |
| `AddLabel` / `RemoveLabel` (favorites) | CAS update of `NodeEntry.Favorites` | — | Only `"favorite"` label supported (parity with decomposedfs); other labels return `BadRequest` |

## 5. Concurrency model

KVFS uses **optimistic concurrency with CAS** via NATS JetStream's per-key
revision numbers. There are no cross-key transactions.

- **Create** uses `expectedRev = 0` (key must not exist).
- **Update** uses the revision returned by the most recent `Get` as
  `expectedRev`. Mismatch yields `ErrCASConflict`.
- **Retry**: every CAS-bound mutation runs in a loop bounded by
  `defaultMaxCASRetries = 10`, with exponential backoff + jitter capped at
  50 ms (`casBackoff` in [`kvfs.go`](kvfs.go)).
- **Conflict resolution for `PutChildren`**: on a create conflict, the helper
  re-reads, merges the new entry into the existing children map, and retries.
  This handles the common "two creates race" case without escalating an error.
- **Tree-size propagation** (`propagateTreeSize`) walks ancestors and updates
  `Size`. If an ancestor CAS fails after retries, the failure is recorded on
  the `kvfs_tree_size_drift_total` counter and propagation stops — the node
  itself is correct, only the ancestor sums may be slightly stale.

There are no global transactions, so a process crash mid-mutation can leave
orphan blobs (no metadata pointing at them) or stale upload sessions. Both
are handled by GC (§7).

## 6. Upload pipeline

KVFS supports two upload protocols, both committing through the same
`commitFileNode` CAS path so the on-disk state is identical.

### 6.1 Simple PUT (`Upload`)

```
client ── PUT body ──▶ kvfs.Upload
                          │
                          ├─▶ checkQuota
                          ├─▶ blob.Upload (S3 single PUT, hashing SHA-1 inline)
                          └─▶ commitFileNode (CAS-retried)
                                   ├─ create or update NodeEntry (with new BlobID)
                                   ├─ archive previous blob into VersionEntry (if any)
                                   ├─ update parent children map
                                   ├─ propagate tree size
                                   └─ trim to MaxVersions
```

Emits `events.FileUploaded`.

### 6.2 TUS resumable (`InitiateUpload` / `WriteChunk` / `FinishUpload`)

```
InitiateUpload  ──▶  S3 InitiateMultipartUpload
                ──▶  PutUpload (new UploadSession in `uploads` bucket)
                ──▶  return TUS session ID

WriteChunk(part_n)  ──▶  S3 UploadPart(part_n)
                    ──▶  PutUpload (offset, parts, etags)

FinishUpload   ──▶  S3 CompleteMultipartUpload
               ──▶  commitFileNode (same path as simple)
               ──▶  DeleteUpload (session)

Terminate      ──▶  S3 AbortMultipartUpload
               ──▶  DeleteUpload (session)
```

Implements `tusd.Core`, `tusd.Terminater`, and `tusd.LengthDeferrer`. The
`UploadInFlight{protocol}` gauge tracks both protocols; `FinishUpload` uses
`defer` to ensure decrement and session cleanup on every exit path.

## 7. Garbage collection

The `blobGC` loop runs on `Options.GCInterval` (default 24 h). Each sweep:

1. **Acquire distributed lock** `gc-sweep` in the `locks` bucket
   (holder = hostname, TTL = 30 min). Skip the run if the lock is held.
2. **Per-space reference set**: union of `BlobID`s referenced by `nodes`,
   `versions`, `uploads`, and `trash` (`buildSpaceReferenceSet`).
3. **List S3 blobs under the space prefix**. For each blob:
   - If referenced → keep.
   - If unreferenced **and** older than `Options.GCMinAge` (default 24 h) →
     delete (or log under `GCDryRun`). The age guard prevents deletion of
     blobs uploaded between the metadata snapshot and the S3 list.
4. **Clean expired upload sessions**: `cleanExpiredUploads` aborts the S3
   multipart and deletes the `uploads` entry for any session past its expiry.
5. **Release lock**.

`GCRunOnStart` triggers an immediate sweep at process boot. All counters
(`kvfs_gc_blobs_*_total`, `kvfs_gc_run_duration_seconds`) are exported.

## 8. Configuration

All fields are loadable from a `map[string]interface{}` via `mapstructure`
(see [`options.go`](options.go)). When wired through OpenCloud's
`storage-users` or `storage-system` services, env-var names are
`STORAGE_{USERS,SYSTEM}_KVFS_*`.

| Field | mapstructure key | Default | Purpose |
|---|---|---|---|
| `NATSNodes` | `nats_nodes` | `[nats:4222]` | NATS endpoints |
| `NATSUsername` / `NATSPassword` | `nats_username` / `nats_password` | — | NATS auth |
| `NATSReplicas` | `nats_replicas` | `1` | KV stream replication factor (use `3` for HA) |
| `S3Endpoint` | `s3.endpoint` | — | S3 endpoint URL |
| `S3Region` | `s3.region` | — | S3 region |
| `S3Bucket` | `s3.bucket` | — | Bucket for blobs |
| `S3AccessKey` / `S3SecretKey` | `s3.access_key` / `s3.secret_key` | — | S3 credentials |
| `BucketPrefix` | `bucket_prefix` | `oc` | KV bucket name prefix |
| `DisableVersioning` | `disable_versioning` | `false` | Skip version creation on overwrite |
| `MaxVersions` | `max_versions` | `0` (unlimited) | Per-file version cap |
| `GCEnabled` | `gc_enabled` | `false` | Run the GC loop |
| `GCInterval` | `gc_interval` | `24h` | Sweep frequency |
| `GCMinAge` | `gc_min_age` | `24h` | Minimum blob age before deletion |
| `GCDryRun` | `gc_dry_run` | `false` | Log-only mode |
| `GCRunOnStart` | `gc_run_on_start` | `false` | Immediate sweep at boot |

## 9. Observability

### 9.1 Metrics ([`metrics.go`](metrics.go))

| Metric | Type | Labels | Purpose |
|---|---|---|---|
| `kvfs_cas_retries_total` | counter | `operation` | CAS retry attempts |
| `kvfs_cas_exhausted_total` | counter | `operation` | CAS loops that gave up |
| `kvfs_cas_conflicts_total` | counter | `bucket` | Raw conflict count |
| `kvfs_kv_operation_duration_seconds` | histogram | `bucket`, `op` | NATS KV op latency |
| `kvfs_blob_operation_duration_seconds` | histogram | `op` | S3 op latency |
| `kvfs_upload_in_flight` | gauge | `protocol` | Active uploads (`simple` / `tus`) |
| `kvfs_tree_size_drift_total` | counter | — | Ancestor CAS failures |
| `kvfs_gc_blobs_scanned_total` | counter | — | Blobs scanned by GC |
| `kvfs_gc_blobs_deleted_total` | counter | — | Orphans deleted |
| `kvfs_gc_would_delete_total` | counter | — | Dry-run would-delete count |
| `kvfs_gc_errors_total` | counter | — | GC errors |
| `kvfs_gc_run_duration_seconds` | histogram | — | Sweep duration |

### 9.2 Events

Published to the configured event stream via `publishEvent`:

`ContainerCreated`, `FileUploaded`, `FileDownloaded`, `FileVersionRestored`,
`ItemMoved`, `ItemTrashed`, `ItemRestored`, `ItemPurged`.

These are the standard reva storage events consumed by OpenCloud's
notifications, audit, and search services.

## 10. Feature parity vs decomposedfs / posix

The reference driver in OpenCloud is `posix`, which wraps `decomposedfs`
(`vendor/.../reva/v2/pkg/storage/pkg/decomposedfs/decomposedfs.go`). The
table below covers the full `storage.FS` interface plus all auxiliary
capabilities.

Status: ✅ implemented · ⚠️ partial · ❌ missing · ➖ N/A (handled outside
the driver in both implementations).

### 10.1 `storage.FS` interface

| Method | Decomp | KVFS | Comments |
|---|:---:|:---:|---|
| `Shutdown` | ✅ | ✅ | KVFS stops GC loop + closes NATS conn |
| `ListStorageSpaces` | ✅ | ✅ | Personal + project; share spaces via `sharesstorageprovider` |
| `GetQuota` | ✅ | ✅ | Tree-size derived |
| `GetMD` | ✅ | ✅ | Permission-gated |
| `ListFolder` | ✅ | ✅ | Batched node hydration (50-way) |
| `Download` | ✅ | ✅ | Direct S3 stream |
| `GetPathByID` | ✅ | ✅ | Walks ancestor chain |
| `CreateReference` | ✅ | ❌ | Symlinks not supported |
| `CreateDir` | ✅ | ✅ | CAS-retried |
| `TouchFile` | ✅ | ✅ | mtime parsing + processing flag |
| `Delete` | ✅ | ✅ | Soft delete to trash |
| `Move` | ✅ | ✅ | 3-phase CAS |
| `InitiateUpload` | ✅ | ✅ | TUS + S3 multipart |
| `Upload` | ✅ | ✅ | Simple PUT path |
| `ListRevisions` | ✅ | ✅ | |
| `DownloadRevision` | ✅ | ✅ | |
| `RestoreRevision` | ✅ | ✅ | Trims to `MaxVersions` |
| `ListRecycle` | ✅ | ✅ | |
| `RestoreRecycleItem` | ✅ | ✅ | Idempotent; supports `restoreRef` move-on-restore |
| `PurgeRecycleItem` | ✅ | ✅ | Idempotent; deletes blob |
| `EmptyRecycle` | ✅ | ✅ | |
| `AddGrant` | ✅ | ✅ | Inline in `NodeEntry.Grants` |
| `DenyGrant` | ✅ | ❌ | Returns `NotSupported` |
| `RemoveGrant` | ✅ | ✅ | |
| `UpdateGrant` | ✅ | ✅ | |
| `ListGrants` | ✅ | ✅ | Populates `Opaque` with role metadata |
| `SetArbitraryMetadata` | ✅ | ✅ | Inline in `NodeEntry.Metadata` |
| `UnsetArbitraryMetadata` | ✅ | ✅ | |
| `AddLabel` / `RemoveLabel` | ✅ | ✅ | Only `"favorite"` accepted (matches decomposedfs); inline list in `NodeEntry.Favorites` |
| `GetLock` / `SetLock` / `RefreshLock` / `Unlock` | ✅ | ✅ | Inline `NodeEntry.Lock` with expiry; `checkNodeLock` guards mutations |
| `CreateStorageSpace` / `UpdateStorageSpace` / `DeleteStorageSpace` | ✅ | ✅ | Personal + project; cascading delete |
| `CreateHome` / `GetHome` (deprecated) | ✅ | ⚠️ | `CreateHome` is a no-op; `GetHome` returns `"/"` |

### 10.2 Auxiliary capabilities

| Capability | Decomp | KVFS | Comments |
|---|:---:|:---:|---|
| TUS resumable upload | ✅ | ✅ | `Core` + `Terminater` + `LengthDeferrer` |
| TUS concatenation | ✅ | ❌ | Not implemented |
| Tree-size propagation | ✅ (sync/async) | ⚠️ | Synchronous, best-effort; drift counter exposes failures |
| ETag computation | ✅ | ✅ | SHA-256(nodeID + mtime), first 8 bytes |
| MIME detection | ✅ | ✅ | reva `mime` package |
| Storage spaces (personal/project) | ✅ | ✅ | |
| Share spaces | ✅ | ➖ | `sharesstorageprovider` in both |
| Quota enforcement | ✅ | ✅ | Pre-upload check |
| Async postprocessing (antivirus, …) | ✅ | ❌ | Postprocessing event hooks not wired |
| Event emission | ✅ | ✅ | 8 event types (§9.2) |
| Prometheus metrics | ⚠️ minimal | ✅ | KVFS exports 12 collectors (§9.1) |
| GC of orphaned blobs | ❌ (delete-cascade only) | ✅ | Distributed-lock sweep with dry-run |
| Cleanup of expired upload sessions | ✅ | ✅ | Aborts S3 multipart + removes session |
| Metadata backend | xattrs / hybrid / msgpack | NATS KV | KVFS removes the need for filesystem xattrs |
| Blob backend | local FS or S3 | S3 only | |
| Space indexes (by-user-id, by-group-id, by-type) | ✅ filesystem indexes | ⚠️ | KVFS does filtered scans on `ListSpaces` (acceptable at current scale) |
| Thumbnails | ➖ | ➖ | External `thumbnails` service in both |
| Search | ➖ | ➖ | External `search` service in both |
| Archive download (zip/tar) | ➖ | ➖ | Gateway in both |
| WOPI / app providers | ➖ | ➖ | `collaboration` service in both |
| Public links | ➖ | ➖ | Sharing service in both |

## 11. Limitations

Known gaps relative to `decomposedfs`. None of these block typical OpenCloud
workflows on the deployment KVFS targets, but they are documented for
upstream-PR transparency.

- **`CreateReference`** (symlinks) returns `NotSupported`. Reva uses this for
  cross-storage references; KVFS expects all entities to live within KVFS.
- **`DenyGrant`** returns `NotSupported`. Deny semantics are not modelled in
  the inline grant map; positive-only grants suffice for the current
  permission system.
- **TUS concatenation** is not implemented. The `Concatable` extension is rare
  in practice and would require a server-side blob concat path.
- **Async postprocessing hooks** (antivirus, content-extraction) are not
  wired into the upload commit. The `Processing` flag exists on `NodeEntry`
  but is not consumed by any event subscriber here.
- **Tree-size propagation is best-effort.** Ancestor CAS failures bump
  `kvfs_tree_size_drift_total` rather than abort the user-visible operation.
  A periodic reconciler is not yet implemented.
- **Space indexing is by-scan, not by-index.** `ListStorageSpaces` filters
  in Go after a full bucket scan. At the spaces-per-deployment scale this is
  acceptable; at much larger scales, dedicated index buckets would be needed.
- **No migration tooling.** There is no built-in path to migrate existing
  `decomposedfs` data into KVFS or back.

## 12. Testing

15 test files (~5450 LOC, 169 tests) live alongside the driver. They run
against an in-memory KV mock (`mockStore`) and an in-memory blob mock so the
suite has no external dependencies.

| File | Coverage |
|---|---|
| [`cas_test.go`](cas_test.go) | CAS conflict handling, retries, merge logic |
| [`checksum_test.go`](checksum_test.go) | SHA-1 computation in upload paths |
| [`events_test.go`](events_test.go) | Event publishing for all 8 event types |
| [`gc_test.go`](gc_test.go) | Orphan detection, distributed lock, dry-run |
| [`kvstore_test.go`](kvstore_test.go) | KV CAS semantics, list helpers |
| [`listfolder_test.go`](listfolder_test.go) | Listing + permission filtering |
| [`metrics_test.go`](metrics_test.go) | Prometheus metric recording |
| [`move_test.go`](move_test.go) | Rename + cross-directory move |
| [`permissions_test.go`](permissions_test.go) | Grant assembly, group walks, ACL gating |
| [`quota_test.go`](quota_test.go) | Tree-size propagation, quota enforcement |
| [`recycle_test.go`](recycle_test.go) | Trash → restore / purge / empty |
| [`space_test.go`](space_test.go) | Space lifecycle |
| [`treesize_test.go`](treesize_test.go) | Ancestor sum updates, drift counter |
| [`upload_test.go`](upload_test.go) | Simple + TUS, multipart parts, deferred length |
| [`versions_test.go`](versions_test.go) | Revision creation, restore, trim |

Run from this directory:

```sh
go test ./... -count=1
```

End-to-end integration tests (Python, stdlib-only, against a live cluster)
live outside this driver in the project's `tests/` directory and are not
part of the upstream PR scope.
