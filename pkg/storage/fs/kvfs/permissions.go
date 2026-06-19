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

	group "github.com/cs3org/go-cs3apis/cs3/identity/group/v1beta1"
	user "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	ctxpkg "github.com/opencloud-eu/reva/v2/pkg/ctx"
)

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
