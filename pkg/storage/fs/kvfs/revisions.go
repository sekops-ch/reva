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
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	types "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"

	ctxpkg "github.com/opencloud-eu/reva/v2/pkg/ctx"
	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
	"github.com/opencloud-eu/reva/v2/pkg/events"
)

// revisionKeyDelimiter joins a node ID and a version-entry key; decomposedfs
// and posix use the same format, so ocdav can address a version as a resource.
const revisionKeyDelimiter = ".REV."

func revisionKey(nodeID, entryKey string) string { return nodeID + revisionKeyDelimiter + entryKey }

// validKeyPart reports whether s can be a KV key segment: node and entry keys
// are UUIDs, so dots, wildcards and slashes never name one of ours.
func validKeyPart(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r != '-' && r != '_' && (r < '0' || r > '9') && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
			return false
		}
	}
	return true
}

// splitRevisionKey returns the node and entry key in key. A bare key returns
// an empty nodeID; a malformed key is NotFound.
func splitRevisionKey(key string) (nodeID, entryKey string, err error) {
	nodeID, entryKey, found := strings.Cut(key, revisionKeyDelimiter)
	if !found {
		if !validKeyPart(key) {
			return "", "", errtypes.NotFound(key)
		}
		return "", key, nil
	}
	if !validKeyPart(nodeID) || !validKeyPart(entryKey) {
		return "", "", errtypes.NotFound(key)
	}
	return nodeID, entryKey, nil
}

func isRevisionRef(ref *provider.Reference) bool {
	return strings.Contains(ref.GetResourceId().GetOpaqueId(), revisionKeyDelimiter)
}

// revisionOwnerRef turns a revision-key reference into a reference to the node
// that owns the version, so authorization runs on that node.
func revisionOwnerRef(ref *provider.Reference) (*provider.Reference, string, error) {
	rid := ref.GetResourceId()
	key := rid.GetOpaqueId()
	nodeID, _, err := splitRevisionKey(key)
	if err != nil {
		return nil, "", err
	}
	if p := ref.GetPath(); p != "" && p != "." && p != "/" {
		return nil, "", errtypes.NotFound(key) // a version has no children
	}
	return &provider.Reference{ResourceId: &provider.ResourceId{
		StorageId: rid.GetStorageId(), SpaceId: rid.GetSpaceId(), OpaqueId: nodeID,
	}}, key, nil
}

// lookupVersion returns nodeID's entry for a revision or bare key. A revision
// key naming another node is NotFound: the caller authorized nodeID only.
func (d *kvfsDriver) lookupVersion(spaceID, nodeID, key string) (*VersionEntry, error) {
	keyNode, entryKey, err := splitRevisionKey(key)
	if err != nil {
		return nil, err
	}
	if keyNode != "" && keyNode != nodeID {
		return nil, errtypes.NotFound(key)
	}
	v, err := d.store.GetVersion(spaceID, nodeID, entryKey)
	switch {
	case errors.Is(err, ErrNotFound):
		return nil, errtypes.NotFound(key)
	case err != nil:
		return nil, fmt.Errorf("kvfs: get version %q of node %s: %w", entryKey, nodeID, err)
	}
	return v, nil
}

// versionResourceInfo describes version v of file node: the version's content
// metadata under the file's name, addressed by its revision key.
func versionResourceInfo(node *NodeEntry, v *VersionEntry, path string) *provider.ResourceInfo {
	ri := &provider.ResourceInfo{
		Id:       &provider.ResourceId{SpaceId: node.SpaceID, OpaqueId: revisionKey(node.ID, v.Key)},
		Type:     provider.ResourceType_RESOURCE_TYPE_FILE,
		Path:     path,
		Name:     node.Name,
		MimeType: node.MimeType,
		Etag:     v.ETag,
		Size:     uint64(v.Size),
		Mtime: &types.Timestamp{
			Seconds: uint64(v.MTime / int64(time.Second)),
			Nanos:   uint32(v.MTime % int64(time.Second)),
		},
		Checksum: parseChecksum(v.Checksum),
	}
	if node.ParentID != "" {
		ri.ParentId = &provider.ResourceId{SpaceId: node.SpaceID, OpaqueId: node.ParentID}
	}
	return ri
}

// statRevision describes the version a revision-key reference names.
func (d *kvfsDriver) statRevision(ctx context.Context, ref *provider.Reference) (*provider.ResourceInfo, error) {
	nodeRef, key, err := revisionOwnerRef(ref)
	if err != nil {
		return nil, err
	}
	spaceID, nodeID, node, rp, err := d.resolveAndAuthorize(ctx, nodeRef, func(rp *provider.ResourcePermissions) bool {
		return rp.Stat && rp.ListFileVersions
	})
	if err != nil {
		return nil, err
	}
	v, err := d.lookupVersion(spaceID, nodeID, key)
	if err != nil {
		return nil, err
	}
	path, err := d.buildPath(ctx, spaceID, nodeID)
	if err != nil {
		path = node.Name
	}
	ri := versionResourceInfo(node, v, path)
	ri.PermissionSet = rp
	return ri, nil
}

// downloadRevisionRef streams the version a revision-key reference names.
func (d *kvfsDriver) downloadRevisionRef(ctx context.Context, ref *provider.Reference, openReaderFunc func(*provider.ResourceInfo) bool) (*provider.ResourceInfo, io.ReadCloser, error) {
	nodeRef, key, err := revisionOwnerRef(ref)
	if err != nil {
		return nil, nil, err
	}
	return d.DownloadRevision(ctx, nodeRef, key, openReaderFunc)
}

func (d *kvfsDriver) ListRevisions(ctx context.Context, ref *provider.Reference) ([]*provider.FileVersion, error) {
	spaceID, nodeID, _, _, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.ListFileVersions })
	if err != nil {
		return nil, err
	}

	versions, err := d.store.ListVersions(spaceID, nodeID)
	if err != nil {
		return nil, fmt.Errorf("kvfs: list versions of node %s: %w", nodeID, err)
	}

	var result []*provider.FileVersion
	for _, v := range versions {
		result = append(result, &provider.FileVersion{
			Key:   revisionKey(nodeID, v.Key),
			Size:  uint64(v.Size),
			Mtime: uint64(v.MTime / int64(time.Second)),
			Etag:  v.ETag,
		})
	}
	return result, nil
}

func (d *kvfsDriver) DownloadRevision(ctx context.Context, ref *provider.Reference, key string, openReaderFunc func(*provider.ResourceInfo) bool) (*provider.ResourceInfo, io.ReadCloser, error) {
	spaceID, nodeID, node, _, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool {
		return rp.ListFileVersions && rp.InitiateFileDownload
	})
	if err != nil {
		return nil, nil, err
	}

	version, err := d.lookupVersion(spaceID, nodeID, key)
	if err != nil {
		return nil, nil, err
	}

	ri := versionResourceInfo(node, version, node.Name)
	if openReaderFunc != nil && !openReaderFunc(ri) {
		return ri, nil, nil
	}

	reader, err := d.blob.Download(ctx, BlobKey(spaceID, version.BlobID))
	if err != nil {
		return nil, nil, fmt.Errorf("kvfs: download version %q of node %s: %w", version.Key, nodeID, err)
	}

	return ri, reader, nil
}

// RestoreRevision makes a version the file's content again; the replaced
// content becomes a new version and the restored entry stays listed.
func (d *kvfsDriver) RestoreRevision(ctx context.Context, ref *provider.Reference, key string) error {
	spaceID, nodeID, authNode, _, err := d.resolveAndAuthorize(ctx, ref, func(rp *provider.ResourcePermissions) bool { return rp.RestoreFileVersion })
	if err != nil {
		return err
	}
	if err := d.checkNodeLock(ctx, authNode); err != nil {
		return err
	}

	version, err := d.lookupVersion(spaceID, nodeID, key)
	if err != nil {
		return err
	}

	node, rev, err := d.store.GetNode(spaceID, nodeID)
	if err != nil {
		return err
	}

	// Detached commit context so the head switch, propagation, trim and the
	// event survive a cancelled request. See [kvfsDriver.commitPhase].
	commitCtx, cancel := d.commitPhase(ctx)
	defer cancel()

	// Snapshot before the head moves, so the replaced blob always stays referenced.
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

	// Trim only after the head commit and never the restored entry, which now
	// shares the head's blob.
	if !d.opts.DisableVersioning {
		d.trimVersions(commitCtx, spaceID, nodeID, version.Key)
	}

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

// trimVersions caps a node's version entries at MaxVersions, keeping the
// entries in keep plus the newest others. It deletes entries only: a version's
// blob can also back the head or another entry, so GC reclaims blobs.
func (d *kvfsDriver) trimVersions(ctx context.Context, spaceID, nodeID string, keep ...string) {
	if d.opts.MaxVersions <= 0 {
		return
	}

	versions, err := d.store.ListVersions(spaceID, nodeID)
	if err != nil {
		d.log.Warn().Err(err).Str("space_id", spaceID).Str("node_id", nodeID).Msg("trimVersions: failed to list versions")
		return
	}
	if len(versions) <= d.opts.MaxVersions {
		return
	}

	protected := 0
	others := make([]*VersionEntry, 0, len(versions))
	for _, v := range versions {
		if slices.Contains(keep, v.Key) {
			protected++
			continue
		}
		others = append(others, v)
	}
	// Newest first; the key breaks MTime ties so the choice is deterministic.
	sort.Slice(others, func(i, j int) bool {
		if others[i].MTime != others[j].MTime {
			return others[i].MTime > others[j].MTime
		}
		return others[i].Key < others[j].Key
	})
	slots := max(d.opts.MaxVersions-protected, 0)
	if len(others) <= slots {
		return
	}

	removed := 0
	for _, v := range others[slots:] {
		if err := d.store.DeleteVersion(spaceID, nodeID, v.Key); err != nil {
			d.log.Warn().Err(err).Str("space_id", spaceID).Str("node_id", nodeID).Str("version_key", v.Key).Msg("trimVersions: failed to delete version entry")
			continue
		}
		removed++
	}
	d.log.Debug().Str("space_id", spaceID).Str("node_id", nodeID).Int("removed", removed).Int("max_versions", d.opts.MaxVersions).Msg("trimVersions: trimmed version entries")
}
