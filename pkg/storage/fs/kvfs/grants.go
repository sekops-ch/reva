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

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	"github.com/opencloud-eu/reva/v2/pkg/errtypes"
)

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

// modifyGrant adds, removes, or updates a grant on a node with CAS retry.
func (d *kvfsDriver) modifyGrant(spaceID, nodeID string, g *provider.Grant, action string) error {
	grantKey := granteeKey(g.Grantee)
	attempt := 0
	return d.casRetryLoop("grant", func() error {
		defer func() { attempt++ }()
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

		if err := d.store.PutNode(node, rev); err != nil {
			if err == ErrCASConflict {
				d.log.Warn().Int("attempt", attempt).Str("space_id", spaceID).Str("node_id", nodeID).Str("action", action).Msg("modifyGrant: CAS conflict, retrying")
			}
			return err
		}
		return nil
	})
}
