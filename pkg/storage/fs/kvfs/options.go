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
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

// Options holds the configuration for the kvfs storage driver.
type Options struct {
	// NATS JetStream connection
	NATSNodes    []string `mapstructure:"nats_nodes"`
	NATSUsername string   `mapstructure:"nats_username"`
	NATSPassword string   `mapstructure:"nats_password"`
	NATSReplicas int      `mapstructure:"nats_replicas"`

	// S3 blob storage
	S3Endpoint  string `mapstructure:"s3.endpoint"`
	S3Region    string `mapstructure:"s3.region"`
	S3Bucket    string `mapstructure:"s3.bucket"`
	S3AccessKey string `mapstructure:"s3.access_key"`
	S3SecretKey string `mapstructure:"s3.secret_key"`

	// KV bucket prefix (allows multiple instances to share a NATS cluster)
	BucketPrefix string `mapstructure:"bucket_prefix"`

	// ChildrenMaxValueSize sets the per-value size cap on the children bucket
	// in bytes. A single ChildMap (msgpack-encoded map of name -> nodeID) is
	// stored per directory. With 36-char UUIDs and short filenames the default
	// 1 MiB ceiling allows roughly 12 000 to 15 000 entries per directory;
	// long filenames push that lower. Deployments expecting directories with
	// 10 000+ entries should raise this (NATS supports up to 64 MiB) or
	// adopt sharded children when that lands. Zero means use the NATS default.
	ChildrenMaxValueSize int32 `mapstructure:"children_max_value_size"`

	// MaxCASRetries bounds every CAS retry loop in the driver. Zero means
	// use the package default. Bursts on a hot parent need more retries
	// than the default 10; tune up on contended workloads.
	MaxCASRetries int `mapstructure:"max_cas_retries"`

	// UploadBackend selects the TUS body-staging implementation:
	//   - "disk" (default): buffer chunks to UploadTmpDir; matches every
	//     other reva storage driver. WriteChunk returns in microseconds.
	//     Cross-pod PATCH delivery is not supported (each pod has its
	//     own local cache).
	//   - "nats": stage to a NATS JetStream Stream (cross-pod transparent,
	//     survives pod death + rolling helm upgrades). ~5-15 ms per chunk
	//     overhead. The stream is created at first use with R=1 file
	//     storage; tune via UploadNATSStream / UploadNATSReplicas /
	//     UploadNATSStorage / UploadNATSMaxBytes / UploadNATSMaxAge.
	UploadBackend string `mapstructure:"upload_backend"`

	// UploadTmpDir is the directory the disk backend uses for per-session
	// buffer files. Defaults to "/var/lib/kvfs/uploads". The deployment
	// is expected to mount an emptyDir or PVC there sized for the peak
	// concurrent-upload byte total.
	UploadTmpDir string `mapstructure:"upload_tmp_dir"`

	// UploadNATSStream is the JetStream stream name for the NATS backend.
	// The stream is created at first use if missing. Defaults to
	// "{BucketPrefix}-uploads-stream" — different kvfs instances (e.g.
	// storage-users with prefix "oc-" vs. storage-system with "sys-") get
	// distinct names by default and don't collide.
	UploadNATSStream string `mapstructure:"upload_nats_stream"`

	// UploadNATSSubjectPrefix is the subject root for per-session subjects.
	// Each session publishes to `{UploadNATSSubjectPrefix}.{session-id}`.
	// Defaults to "{BucketPrefix}-uploads" (matches the stream's wildcard).
	UploadNATSSubjectPrefix string `mapstructure:"upload_nats_subject_prefix"`

	// UploadNATSStorage selects JetStream storage mode for the upload
	// stream: "file" (default, durable across NATS pod restart) or
	// "memory" (faster but bounded by NATS `memoryStore.maxSize`).
	UploadNATSStorage string `mapstructure:"upload_nats_storage"`

	// UploadNATSReplicas is the JetStream replica count for the upload
	// stream. Defaults to 1 — TUS resume is the correctness safety net
	// for replica loss on transient upload bodies; R=3 would 3x the write
	// amplification for no real benefit on this workload.
	UploadNATSReplicas int `mapstructure:"upload_nats_replicas"`

	// UploadNATSMaxAge caps how long an abandoned upload can keep stream
	// storage. Matches UploadSession TTL — defaults to 24h.
	UploadNATSMaxAge time.Duration `mapstructure:"upload_nats_max_age"`

	// UploadNATSMaxBytes caps total stream size across all in-flight
	// uploads. NATS rejects new publishes when full (Discard=New).
	// Defaults to 5 GiB; size it relative to the NATS JetStream storage
	// budget shared with the metadata KV buckets.
	UploadNATSMaxBytes int64 `mapstructure:"upload_nats_max_bytes"`

	// UploadNATSMaxChunkBytes is the largest single message the cache
	// publishes. Must be <= NATS server `max_payload`. Larger PATCH bodies
	// are split into multiple messages. Defaults to 8 MiB.
	UploadNATSMaxChunkBytes int `mapstructure:"upload_nats_max_chunk_bytes"`

	// SmallFileThreshold is the byte size below which the blobstore
	// uploads files via a single S3 PutObject; above this, multipart is
	// used. Default 16 MiB. Multipart adds ~80 ms overhead (Init + Part +
	// Complete vs a single Put), which is significant for the small-file
	// workloads that dominate mirall sync traffic.
	SmallFileThreshold int64 `mapstructure:"small_file_threshold"`

	// Behavior
	DisableVersioning bool `mapstructure:"disable_versioning"`
	MaxVersions       int  `mapstructure:"max_versions"`

	// Garbage collection
	GCEnabled    bool   `mapstructure:"gc_enabled"`
	GCInterval   string `mapstructure:"gc_interval"`
	GCDryRun     bool   `mapstructure:"gc_dry_run"`
	GCMinAge     string `mapstructure:"gc_min_age"`
	GCRunOnStart bool   `mapstructure:"gc_run_on_start"`
}

// parseConfig parses a raw config map into typed Options.
func parseConfig(m map[string]interface{}) (*Options, error) {
	o := &Options{}
	if err := mapstructure.Decode(m, o); err != nil {
		return nil, errors.Wrap(err, "kvfs: error decoding config")
	}
	o.init()
	return o, nil
}

// init sets defaults for missing config values.
func (o *Options) init() {
	if len(o.NATSNodes) == 0 {
		o.NATSNodes = []string{"nats:4222"}
	}
	if o.NATSReplicas == 0 {
		o.NATSReplicas = 1
	}
	if o.BucketPrefix == "" {
		o.BucketPrefix = "oc"
	}
	if o.GCInterval == "" {
		o.GCInterval = "24h"
	}
	if o.GCMinAge == "" {
		o.GCMinAge = "24h"
	}
	if o.MaxCASRetries <= 0 {
		o.MaxCASRetries = defaultMaxCASRetries
	}
	if o.UploadBackend == "" {
		o.UploadBackend = "disk"
	}
	if o.UploadTmpDir == "" {
		o.UploadTmpDir = "/var/lib/kvfs/uploads"
	}
	if o.SmallFileThreshold <= 0 {
		o.SmallFileThreshold = 16 * 1024 * 1024 // 16 MiB
	}
	// NATS-backed upload-cache defaults. Only matter when UploadBackend=nats.
	if o.UploadNATSStream == "" {
		o.UploadNATSStream = o.BucketPrefix + "-uploads-stream"
	}
	if o.UploadNATSSubjectPrefix == "" {
		o.UploadNATSSubjectPrefix = o.BucketPrefix + "-uploads"
	}
	if o.UploadNATSStorage == "" {
		o.UploadNATSStorage = "file"
	}
	if o.UploadNATSReplicas <= 0 {
		o.UploadNATSReplicas = 1
	}
	if o.UploadNATSMaxAge == 0 {
		o.UploadNATSMaxAge = 24 * time.Hour
	}
	if o.UploadNATSMaxBytes <= 0 {
		o.UploadNATSMaxBytes = 5 * 1024 * 1024 * 1024 // 5 GiB
	}
	if o.UploadNATSMaxChunkBytes <= 0 {
		o.UploadNATSMaxChunkBytes = 8 * 1024 * 1024 // 8 MiB
	}
}

func parseDurationOrDefault(s string, fallback time.Duration) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		return fallback
	}
	return d
}

// GCIntervalDuration returns the parsed GC interval duration.
func (o *Options) GCIntervalDuration() time.Duration {
	return parseDurationOrDefault(o.GCInterval, 24*time.Hour)
}

// GCMinAgeDuration returns the parsed GC minimum age duration.
func (o *Options) GCMinAgeDuration() time.Duration {
	return parseDurationOrDefault(o.GCMinAge, 24*time.Hour)
}

// S3ConfigComplete returns true if all required S3 configuration is provided.
func (o *Options) S3ConfigComplete() bool {
	return o.S3Endpoint != "" && o.S3Bucket != "" && o.S3AccessKey != "" && o.S3SecretKey != ""
}

// BucketName returns a prefixed bucket name.
func (o *Options) BucketName(name string) string {
	return o.BucketPrefix + "-" + name
}
