Enhancement: Add kvfs storage driver (NATS JetStream KV + S3)

The new kvfs driver implements `storage.FS` with authoritative metadata
stored in NATS JetStream KV buckets and blobs stored in any S3-compatible
object store. It is a peer to `decomposed` / `decomposeds3` / `posix`,
designed for stateless deployments that cannot rely on a shared
filesystem for metadata.

Features:
 - Seven NATS KV buckets (nodes, children, spaces, trash, versions,
   uploads, locks) carrying msgpack-encoded metadata; CAS via NATS
   revisions.
 - S3 blob storage with both simple PUT and TUS resumable upload paths
   committing through a single CAS path.
 - Distributed-lock garbage collector for orphaned blobs and expired
   upload sessions, plus a reconciler for stale or dangling oc-children
   entries (interim safety net for crashed Move operations).
 - Configurable per-value cap on the children bucket (NATSChildrenMaxValueSize)
   so deployments expecting large directories can raise the default 1 MiB
   ceiling without code changes.
 - Twelve Prometheus collectors covering CAS retries, KV / blob latency,
   in-flight uploads, tree-size drift, and GC outcomes.
 - 171 unit tests across 15 files.

https://github.com/opencloud-eu/reva/pull/<N>
