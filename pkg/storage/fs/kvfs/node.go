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
	"strings"
	"time"

	user "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	types "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"
)

// NodeType distinguishes files from directories.
type NodeType int

const (
	// NodeTypeFile represents a regular file.
	NodeTypeFile NodeType = 0
	// NodeTypeDir represents a directory.
	NodeTypeDir NodeType = 1
)

// NodeEntry represents a file or directory node stored in the KV store.
// All metadata is stored inline — no filesystem xattrs or sidecar files.
type NodeEntry struct {
	ID       string   `msgpack:"id"`  // UUID of this node
	SpaceID  string   `msgpack:"sid"` // UUID of the owning space
	ParentID string   `msgpack:"pid"` // UUID of parent node (empty for space root)
	Name     string   `msgpack:"n"`   // filename/dirname
	Type     NodeType `msgpack:"t"`   // file or directory

	// Blob storage (files only)
	BlobID   string `msgpack:"bid"` // S3 object key for the blob
	BlobSize int64  `msgpack:"bs"`  // size of the blob in bytes

	// Metadata
	Size     int64  `msgpack:"sz"`    // file size or treesize for dirs
	MTime    int64  `msgpack:"mt"`    // modification time (unix nanos)
	ETag     string `msgpack:"et"`    // entity tag
	MimeType string `msgpack:"mime"`  // MIME type
	Owner    string `msgpack:"own"`   // user ID of the owner
	Checksum string `msgpack:"cksum"` // content checksum (e.g. sha1:...)

	// Grants / ACL
	Grants map[string]*GrantEntry `msgpack:"grants,omitempty"` // grantee key -> grant

	// Arbitrary metadata (custom user properties)
	Metadata map[string]string `msgpack:"md,omitempty"`

	// File lock
	Lock *LockEntry `msgpack:"lock,omitempty"`

	// Favorites (list of user IDs who favorited this node)
	Favorites []string `msgpack:"favs,omitempty"`

	// Processing flag (set during upload)
	Processing bool `msgpack:"proc,omitempty"`
}

// GrantEntry represents an ACL grant stored inline in a node.
type GrantEntry struct {
	GranteeType string `msgpack:"gt"` // "user" or "group"
	GranteeID   string `msgpack:"gid"`
	Permissions uint32 `msgpack:"p"` // bitmask of provider.ResourcePermissions
}

// LockEntry represents a file lock stored inline in a node.
type LockEntry struct {
	LockID  string `msgpack:"lid"`
	Type    int    `msgpack:"lt"`
	UserID  string `msgpack:"uid"`
	AppName string `msgpack:"app,omitempty"`
	Expiry  int64  `msgpack:"exp,omitempty"` // unix timestamp
}

// SpaceEntry represents a storage space (personal, project, etc.).
type SpaceEntry struct {
	ID     string `msgpack:"id"`  // UUID (same as root node ID)
	Type   string `msgpack:"t"`   // "personal", "project", "virtual"
	Owner  string `msgpack:"own"` // user ID
	Name   string `msgpack:"n"`   // display name
	RootID string `msgpack:"rid"` // root node ID (== ID for root spaces)
	Quota  int64  `msgpack:"q"`   // quota in bytes (-1 for unlimited)
	MTime  int64  `msgpack:"mt"`  // last modification time (unix nanos)
}

// TrashEntry represents a trashed item.
type TrashEntry struct {
	Key          string `msgpack:"k"`   // trash key (unique ID)
	NodeID       string `msgpack:"nid"` // original node ID
	SpaceID      string `msgpack:"sid"`
	OriginalPath string `msgpack:"op"` // original path in the tree
	DeletionTime int64  `msgpack:"dt"` // unix timestamp

	// Snapshot of the node at deletion time
	Node NodeEntry `msgpack:"node"`
}

// VersionEntry represents a file revision.
type VersionEntry struct {
	Key      string `msgpack:"k"`    // version key
	NodeID   string `msgpack:"nid"`  // node this version belongs to
	SpaceID  string `msgpack:"sid"`  // space ID
	BlobID   string `msgpack:"bid"`  // S3 blob key for this version
	BlobSize int64  `msgpack:"bs"`   // blob size
	MTime    int64  `msgpack:"mt"`   // modification time
	ETag     string `msgpack:"et"`   // etag of this version
	Size     int64  `msgpack:"sz"`   // file size
	Checksum string `msgpack:"cksum"`
}

// UploadSession represents a TUS upload session stored in KV.
type UploadSession struct {
	ID       string            `msgpack:"id"`
	SpaceID  string            `msgpack:"sid"`
	NodeID   string            `msgpack:"nid"`  // target node ID (empty for new files)
	Filename string            `msgpack:"fn"`   // target filename
	ParentID string            `msgpack:"pid"`  // parent directory node ID
	Size     int64             `msgpack:"sz"`   // expected upload size
	Offset   int64             `msgpack:"off"`  // current upload offset
	Storage  map[string]string `msgpack:"meta"` // TUS metadata
	Expires  int64             `msgpack:"exp"`  // expiry unix timestamp

	// S3 multipart upload state
	BlobID          string     `msgpack:"bid"`   // target blob ID in S3
	S3MultipartID   string     `msgpack:"s3mid"` // S3 multipart upload ID
	Parts           []PartInfo `msgpack:"parts"` // completed parts
	SizeIsDeferred  bool       `msgpack:"sdef"`  // true if total size is not yet known
	IfMatchEtag     string     `msgpack:"ifm"`   // if-match etag for CAS semantics
	OwnerID         string     `msgpack:"own"`   // uploading user ID
}

// --- Conversion helpers ---

// ToResourceInfo converts a NodeEntry to a CS3 ResourceInfo.
func (n *NodeEntry) ToResourceInfo(path string) *provider.ResourceInfo {
	ri := &provider.ResourceInfo{
		Id: &provider.ResourceId{
			SpaceId:  n.SpaceID,
			OpaqueId: n.ID,
		},
		Path:     path,
		Name:     n.Name,
		MimeType: n.MimeType,
		Etag:     n.ETag,
		Size:     uint64(n.Size),
		Mtime: &types.Timestamp{
			Seconds: uint64(n.MTime / int64(time.Second)),
			Nanos:   uint32(n.MTime % int64(time.Second)),
		},
		ArbitraryMetadata: &provider.ArbitraryMetadata{
			Metadata: n.Metadata,
		},
		Checksum: parseChecksum(n.Checksum),
	}

	if n.ParentID != "" {
		ri.ParentId = &provider.ResourceId{
			SpaceId:  n.SpaceID,
			OpaqueId: n.ParentID,
		}
	}

	if n.Type == NodeTypeDir {
		ri.Type = provider.ResourceType_RESOURCE_TYPE_CONTAINER
	} else {
		ri.Type = provider.ResourceType_RESOURCE_TYPE_FILE
	}

	// In KVFS, root node ID == space ID. Populate Space.Root so that
	// roleConditionForResourceType can identify space roots as "Drive".
	ri.Space = &provider.StorageSpace{
		Root: &provider.ResourceId{
			SpaceId:  n.SpaceID,
			OpaqueId: n.SpaceID,
		},
	}

	ri.Lock = n.ToLock()

	return ri
}

// ToLock converts a LockEntry to a CS3 Lock. Returns nil for expired or absent locks.
func (n *NodeEntry) ToLock() *provider.Lock {
	if n.Lock == nil || n.Lock.LockID == "" {
		return nil
	}
	if n.Lock.Expiry > 0 && time.Now().Unix() > n.Lock.Expiry {
		return nil
	}
	lock := &provider.Lock{
		LockId:  n.Lock.LockID,
		Type:    provider.LockType(n.Lock.Type),
		AppName: n.Lock.AppName,
	}
	if n.Lock.UserID != "" {
		lock.User = &user.UserId{OpaqueId: n.Lock.UserID}
	}
	if n.Lock.Expiry > 0 {
		lock.Expiration = &types.Timestamp{
			Seconds: uint64(n.Lock.Expiry),
		}
	}
	return lock
}

// parseChecksum parses a "type:value" checksum string into a CS3 ResourceChecksum.
func parseChecksum(checksum string) *provider.ResourceChecksum {
	if checksum == "" {
		return nil
	}
	parts := strings.SplitN(checksum, ":", 2)
	if len(parts) != 2 {
		return nil
	}
	var csType provider.ResourceChecksumType
	switch parts[0] {
	case "sha1":
		csType = provider.ResourceChecksumType_RESOURCE_CHECKSUM_TYPE_SHA1
	case "md5":
		csType = provider.ResourceChecksumType_RESOURCE_CHECKSUM_TYPE_MD5
	case "adler32":
		csType = provider.ResourceChecksumType_RESOURCE_CHECKSUM_TYPE_ADLER32
	default:
		csType = provider.ResourceChecksumType_RESOURCE_CHECKSUM_TYPE_UNSET
	}
	return &provider.ResourceChecksum{Type: csType, Sum: parts[1]}
}
