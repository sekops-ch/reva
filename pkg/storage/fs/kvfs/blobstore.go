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
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
)

// BlobInfo holds metadata about a blob object in S3.
type BlobInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// BlobStore defines the blob storage operations used by the kvfs driver.
type BlobStore interface {
	Upload(ctx context.Context, key string, reader io.Reader, size int64) error
	Download(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
	InitMultipartUpload(ctx context.Context, key string) (string, error)
	UploadPart(ctx context.Context, key, uploadID string, partNumber int, reader io.Reader, size int64) (PartInfo, error)
	CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []PartInfo) error
	AbortMultipartUpload(ctx context.Context, key, uploadID string) error
	ListBlobs(ctx context.Context, prefix string) ([]BlobInfo, error)
	// ListSpacePrefixes returns the set of top-level space IDs that currently
	// have at least one object in the bucket (the "<spaceID>/" prefixes). Used
	// by GC to detect S3 blobs whose owning space no longer exists in oc-spaces.
	ListSpacePrefixes(ctx context.Context) ([]string, error)
}

// S3Blobstore provides blob storage backed by S3.
// This is a standalone implementation that does not depend on decomposedfs node types.
//
// When multiple kvfs instances share a single S3 bucket (e.g. storage-users and
// storage-system both point at bucket "opencloud"), every physical object key is
// namespaced under this instance's blob prefix ("<prefix>/<spaceID>/..."). This
// keeps each instance's objects in a disjoint keyspace so that one instance's GC
// can never enumerate — and therefore never reap — another instance's blobs. The
// prefix is an internal physical-layout detail: callers always work with logical
// keys ("<spaceID>/...") produced by BlobKey, and the blobstore adds/strips the
// prefix at its boundary.
type S3Blobstore struct {
	client *minio.Client
	core   minio.Core
	bucket string
	prefix string // instance blob namespace (e.g. "oc" / "sys-"); "" = no namespacing
}

// NewS3Blobstore creates a new S3 blob store. blobPrefix namespaces this
// instance's objects within a (possibly shared) bucket; pass the kvfs
// BucketPrefix so two instances never collide. An empty blobPrefix disables
// namespacing (legacy bare "<spaceID>/" layout).
func NewS3Blobstore(endpoint, region, bucket, accessKey, secretKey, blobPrefix string) (*S3Blobstore, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, errors.Wrap(err, "kvfs: failed to parse S3 endpoint")
	}

	useSSL := u.Scheme != "http"
	client, err := minio.New(u.Host, &minio.Options{
		Region: region,
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, errors.Wrap(err, "kvfs: failed to create S3 client")
	}

	return &S3Blobstore{
		client: client,
		core:   minio.Core{Client: client},
		bucket: bucket,
		prefix: blobPrefix,
	}, nil
}

// physicalKey maps a logical blob key ("<spaceID>/...") to the physical S3
// object key, namespaced under this instance's blob prefix.
func (bs *S3Blobstore) physicalKey(logicalKey string) string {
	if bs.prefix == "" {
		return logicalKey
	}
	return bs.prefix + "/" + logicalKey
}

// logicalKey is the inverse of physicalKey: it strips this instance's blob
// prefix so callers (GC ref maps, listings) see stable logical keys.
func (bs *S3Blobstore) logicalKey(physicalKey string) string {
	if bs.prefix == "" {
		return physicalKey
	}
	return strings.TrimPrefix(physicalKey, bs.prefix+"/")
}

// parseSpacePrefix extracts a space ID from an S3 common-prefix key, scoped to
// blobPrefix. It returns ok=false for any common prefix outside this instance's
// namespace, so a residue sweep can never treat another instance's space as a
// ghost. (Pure helper, unit-tested independently of S3.)
func parseSpacePrefix(commonPrefix, blobPrefix string) (string, bool) {
	if !strings.HasSuffix(commonPrefix, "/") {
		return "", false
	}
	if blobPrefix != "" {
		want := blobPrefix + "/"
		if !strings.HasPrefix(commonPrefix, want) {
			return "", false // belongs to another instance — never reap
		}
		commonPrefix = strings.TrimPrefix(commonPrefix, want)
	}
	id := strings.TrimSuffix(commonPrefix, "/")
	if id == "" || strings.Contains(id, "/") {
		return "", false
	}
	return id, true
}

// Upload stores data from a reader into S3 under the given key. PutObject
// in minio-go switches to multipart for objects > 5 MiB automatically; we
// pass through directly so the SDK picks the strategy from `size`.
// SmallFileThreshold is honored at the kvfs upload-flow layer, not here.
func (bs *S3Blobstore) Upload(ctx context.Context, key string, reader io.Reader, size int64) error {
	start := time.Now()
	_, err := bs.client.PutObject(ctx, bs.bucket, bs.physicalKey(key), reader, size, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	BlobOperationDuration.WithLabelValues("upload").Observe(time.Since(start).Seconds())
	if err != nil {
		return errors.Wrapf(err, "kvfs: failed to upload blob %s", key)
	}
	return nil
}

// Download retrieves a blob from S3 for reading.
func (bs *S3Blobstore) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	start := time.Now()
	obj, err := bs.client.GetObject(ctx, bs.bucket, bs.physicalKey(key), minio.GetObjectOptions{})
	BlobOperationDuration.WithLabelValues("download").Observe(time.Since(start).Seconds())
	if err != nil {
		return nil, errors.Wrapf(err, "kvfs: failed to download blob %s", key)
	}
	return obj, nil
}

// Delete removes a blob from S3.
func (bs *S3Blobstore) Delete(ctx context.Context, key string) error {
	start := time.Now()
	err := bs.client.RemoveObject(ctx, bs.bucket, bs.physicalKey(key), minio.RemoveObjectOptions{})
	BlobOperationDuration.WithLabelValues("delete").Observe(time.Since(start).Seconds())
	if err != nil {
		return errors.Wrapf(err, "kvfs: failed to delete blob %s", key)
	}
	return nil
}

// InitMultipartUpload starts an S3 multipart upload and returns the upload ID.
func (bs *S3Blobstore) InitMultipartUpload(ctx context.Context, key string) (string, error) {
	start := time.Now()
	uploadID, err := bs.core.NewMultipartUpload(ctx, bs.bucket, bs.physicalKey(key), minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	BlobOperationDuration.WithLabelValues("multipart_init").Observe(time.Since(start).Seconds())
	if err != nil {
		return "", errors.Wrapf(err, "kvfs: failed to initiate multipart upload for %s", key)
	}
	return uploadID, nil
}

// UploadPart uploads a single part of a multipart upload.
func (bs *S3Blobstore) UploadPart(ctx context.Context, key, uploadID string, partNumber int, reader io.Reader, size int64) (PartInfo, error) {
	start := time.Now()
	objPart, err := bs.core.PutObjectPart(ctx, bs.bucket, bs.physicalKey(key), uploadID, partNumber,
		reader, size, minio.PutObjectPartOptions{})
	BlobOperationDuration.WithLabelValues("multipart_part").Observe(time.Since(start).Seconds())
	if err != nil {
		return PartInfo{}, errors.Wrapf(err, "kvfs: failed to upload part %d for %s", partNumber, key)
	}
	return PartInfo{
		PartNumber: objPart.PartNumber,
		ETag:       objPart.ETag,
		Size:       objPart.Size,
	}, nil
}

// CompleteMultipartUpload finalizes a multipart upload.
func (bs *S3Blobstore) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []PartInfo) error {
	completeParts := make([]minio.CompletePart, len(parts))
	for i, p := range parts {
		completeParts[i] = minio.CompletePart{
			PartNumber: p.PartNumber,
			ETag:       p.ETag,
		}
	}
	start := time.Now()
	_, err := bs.core.CompleteMultipartUpload(ctx, bs.bucket, bs.physicalKey(key), uploadID, completeParts, minio.PutObjectOptions{})
	BlobOperationDuration.WithLabelValues("multipart_complete").Observe(time.Since(start).Seconds())
	if err != nil {
		return errors.Wrapf(err, "kvfs: failed to complete multipart upload for %s", key)
	}
	return nil
}

// AbortMultipartUpload cancels an in-progress multipart upload and cleans up parts.
func (bs *S3Blobstore) AbortMultipartUpload(ctx context.Context, key, uploadID string) error {
	start := time.Now()
	err := bs.core.AbortMultipartUpload(ctx, bs.bucket, bs.physicalKey(key), uploadID)
	BlobOperationDuration.WithLabelValues("multipart_abort").Observe(time.Since(start).Seconds())
	if err != nil {
		return errors.Wrapf(err, "kvfs: failed to abort multipart upload for %s", key)
	}
	return nil
}

// ListBlobs lists all S3 objects under the given logical prefix (e.g.
// "<spaceID>/"). The physical listing is scoped to this instance's blob
// namespace; returned keys are logical (prefix stripped) so they match the
// keys produced by BlobKey.
func (bs *S3Blobstore) ListBlobs(ctx context.Context, prefix string) ([]BlobInfo, error) {
	start := time.Now()
	var blobs []BlobInfo
	for obj := range bs.client.ListObjects(ctx, bs.bucket, minio.ListObjectsOptions{
		Prefix:    bs.physicalKey(prefix),
		Recursive: true,
	}) {
		if obj.Err != nil {
			BlobOperationDuration.WithLabelValues("list").Observe(time.Since(start).Seconds())
			return nil, errors.Wrapf(obj.Err, "kvfs: error listing blobs with prefix %s", prefix)
		}
		blobs = append(blobs, BlobInfo{
			Key:          bs.logicalKey(obj.Key),
			Size:         obj.Size,
			LastModified: obj.LastModified,
		})
	}
	BlobOperationDuration.WithLabelValues("list").Observe(time.Since(start).Seconds())
	return blobs, nil
}

// ListSpacePrefixes returns the top-level "<spaceID>" prefixes present for THIS
// instance only. The listing is scoped to this instance's blob namespace
// ("<prefix>/") and common prefixes outside it are skipped, so a residue sweep
// can never enumerate — and therefore never reap — another instance's spaces
// that happen to share the bucket. Uses a delimiter list (Recursive: false) so
// the cost is one listing of the common prefixes, not a full object enumeration.
func (bs *S3Blobstore) ListSpacePrefixes(ctx context.Context) ([]string, error) {
	start := time.Now()
	listPrefix := ""
	if bs.prefix != "" {
		listPrefix = bs.prefix + "/"
	}
	var prefixes []string
	for obj := range bs.client.ListObjects(ctx, bs.bucket, minio.ListObjectsOptions{
		Prefix:    listPrefix,
		Recursive: false, // delimiter "/" → CommonPrefixes come back as keys ending in "/"
	}) {
		if obj.Err != nil {
			BlobOperationDuration.WithLabelValues("list").Observe(time.Since(start).Seconds())
			return nil, errors.Wrap(obj.Err, "kvfs: error listing space prefixes")
		}
		// CommonPrefixes are returned as ObjectInfo with a Key ending in "/".
		if id, ok := parseSpacePrefix(obj.Key, bs.prefix); ok {
			prefixes = append(prefixes, id)
		}
	}
	BlobOperationDuration.WithLabelValues("list").Observe(time.Since(start).Seconds())
	return prefixes, nil
}

// PartInfo holds the result of uploading a single multipart part.
type PartInfo struct {
	PartNumber int    `msgpack:"pn"`
	ETag       string `msgpack:"etag"`
	Size       int64  `msgpack:"sz"`
}

// BlobKey constructs an S3 object key from space ID and blob ID.
// Uses a pathified layout to distribute objects across prefixes for S3 performance.
func BlobKey(spaceID, blobID string) string {
	if len(blobID) < 4 {
		return fmt.Sprintf("%s/%s", spaceID, blobID)
	}
	// Pathify: spread across prefixes for S3 performance
	// e.g., blobID "abcdef..." → "ab/cd/ef/abcdef..."
	return fmt.Sprintf("%s/%s/%s/%s/%s", spaceID, blobID[:2], blobID[2:4], blobID[4:6], blobID)
}
