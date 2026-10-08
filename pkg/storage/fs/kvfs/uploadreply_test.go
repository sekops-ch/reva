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
	"bytes"
	"io"
	"testing"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	tusd "github.com/tus/tusd/v2/pkg/handler"

	revaevents "github.com/opencloud-eu/reva/v2/pkg/events"
	"github.com/opencloud-eu/reva/v2/pkg/storage"
	"github.com/opencloud-eu/reva/v2/pkg/storagespace"
)

// tusReplyFileID mirrors the OC-FileId the datatx TUS handler derives from the upload info.
func tusReplyFileID(info tusd.FileInfo) string {
	return storagespace.FormatResourceID(&provider.ResourceId{
		StorageId: info.MetaData["providerID"],
		SpaceId:   info.Storage["SpaceRoot"],
		OpaqueId:  info.Storage["NodeId"],
	})
}

// The storage provider hands InitiateUpload its mount id and expiry as metadata; none of it may
// reach the TUS upload info, or the reply names a resource other than the uploaded file.
func TestInitiateUpload_TUSReplyNamesNoResource(t *testing.T) {
	store := newMockMetadataStore()
	d := testDriver(store, newMockBlobStore())
	setupSpaceWithFile(store, "s1", "root", "f1", "file.txt")
	ctx := testContext()

	for _, name := range []string{"./new.txt", "./file.txt"} {
		result, err := d.InitiateUpload(ctx, &provider.Reference{
			ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
			Path:       name,
		}, 5, map[string]string{"providerID": "mnt-1", "expires": "1767225600", "mtime": "1000000000"})
		if err != nil {
			t.Fatalf("InitiateUpload(%s): %v", name, err)
		}
		up, err := d.GetUpload(ctx, result["tus"])
		if err != nil {
			t.Fatalf("GetUpload(%s): %v", name, err)
		}
		info, err := up.GetInfo(ctx)
		if err != nil {
			t.Fatalf("GetInfo(%s): %v", name, err)
		}
		if id := tusReplyFileID(info); id != "" {
			t.Errorf("%s: TUS reply OC-FileId = %q, want empty", name, id)
		}
		for _, k := range []string{"providerID", "expires", "mtime"} {
			if v, ok := info.MetaData[k]; ok {
				t.Errorf("%s: MetaData[%s] = %q, want absent (HEAD echoes MetaData as Upload-Metadata)", name, k, v)
			}
		}
	}
}

func simpleUpload(t *testing.T, d *kvfsDriver, name, content string) *provider.ResourceInfo {
	t.Helper()
	ctx := testContext()
	result, err := d.InitiateUpload(ctx, &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: "root"},
		Path:       name,
	}, int64(len(content)), map[string]string{})
	if err != nil {
		t.Fatalf("InitiateUpload: %v", err)
	}
	ri, err := d.Upload(ctx, storage.UploadRequest{
		Ref:    &provider.Reference{Path: "/" + result["simple"]},
		Body:   io.NopCloser(bytes.NewReader([]byte(content))),
		Length: int64(len(content)),
	}, nil)
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	return ri
}

// The simple upload reply bypasses the storage provider, which adds the mount id to stat replies.
func TestUpload_ReplyIDCarriesMountID(t *testing.T) {
	for _, tc := range []struct{ mount, want string }{{"mnt-1", "mnt-1$s1!"}, {"", "s1!"}} {
		store := newMockMetadataStore()
		d := testDriver(store, newMockBlobStore())
		d.opts.MountID = tc.mount
		setupSpaceEmpty(store, "s1", "root")

		ri := simpleUpload(t, d, "./new.txt", "hello")
		if got, want := storagespace.FormatResourceID(ri.Id), tc.want+ri.Id.GetOpaqueId(); got != want {
			t.Errorf("mount %q: reply id = %q, want %q", tc.mount, got, want)
		}
		if ri.Id.GetOpaqueId() == "" || ri.Id.GetOpaqueId() == "s1" || ri.Id.GetOpaqueId() == "root" {
			t.Errorf("mount %q: reply names %q, not the uploaded file", tc.mount, ri.Id.GetOpaqueId())
		}
	}
}

// Event ids and stat ids keep their format: event resource ids are frozen, and the storage
// provider adds the mount id to stat replies itself.
func TestUpload_MountIDLeavesEventAndStatIDs(t *testing.T) {
	store := newMockMetadataStore()
	stream := newMockStream()
	d := testDriverWithStream(store, newMockBlobStore(), stream)
	d.opts.MountID = "mnt-1"
	setupSpaceEmpty(store, "s1", "root")

	ri := simpleUpload(t, d, "./new.txt", "hello")

	fu, ok := stream.lastEvent().payload.(revaevents.FileUploaded)
	if !ok || fu.Ref.GetResourceId().GetStorageId() != "s1" {
		t.Errorf("FileUploaded ref = %+v, want StorageId s1 (unchanged event id format)", fu.Ref)
	}
	md, err := d.GetMD(testContext(), &provider.Reference{
		ResourceId: &provider.ResourceId{SpaceId: "s1", OpaqueId: ri.Id.GetOpaqueId()},
	}, nil, nil)
	if err != nil {
		t.Fatalf("GetMD: %v", err)
	}
	if md.Id.GetStorageId() != "" {
		t.Errorf("GetMD id StorageId = %q, want empty (the storage provider adds the mount id)", md.Id.GetStorageId())
	}
}
