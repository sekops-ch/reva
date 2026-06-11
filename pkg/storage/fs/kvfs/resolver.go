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

	user "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	"github.com/opencloud-eu/reva/v2/pkg/rgrpc/todo/pool"
	"github.com/opencloud-eu/reva/v2/pkg/utils"
	"github.com/rs/zerolog"
)

// UserResolver answers, for a set of space-owner IDs, which ones are still
// live users. It is the identity signal the GC needs to reap orphaned
// personal spaces (owner no longer exists) — knowledge the storage layer
// does not otherwise hold.
//
// A nil resolver disables identity reaping entirely (the internal residue
// sweep still runs). This is the correct configuration for the
// storage-system instance, which owns no personal user spaces.
//
// Safety contract: an owner whose liveness cannot be determined (transient
// error, backend unreachable, ambiguous status) MUST be OMITTED from the
// returned map. The GC treats "absent from the map" as "unknown" and never
// reaps on unknown — a space is reaped only when its owner is *definitively*
// reported as not-live. A non-nil error means the whole lookup failed (e.g.
// the resolver could not authenticate) and the GC skips identity reaping for
// the entire sweep. This is the in-process equivalent of the standalone
// cleanup tool's "refuse to run on an empty user list" guard: a degraded
// identity backend can never cause a live user's space to be deleted.
type UserResolver interface {
	// ResolveLiveness returns a map keyed by the requested owner IDs.
	// map[id]==true  → the user definitively exists (keep the space).
	// map[id]==false → the user definitively does NOT exist (reap candidate).
	// id absent       → liveness could not be determined (do NOT reap).
	ResolveLiveness(ctx context.Context, ownerIDs []string) (map[string]bool, error)
}

// cs3UserResolver resolves owner liveness through the CS3 gateway, acting as
// the configured service account. It checks each distinct owner with a single
// GetUser RPC and classifies the result strictly: only CODE_NOT_FOUND counts
// as "definitively dead". A definitive answer (vs. listing all users) avoids
// any pagination-truncation risk that could otherwise mark a live user absent.
type cs3UserResolver struct {
	gatewayAddr       string
	saID              string
	saSecret          string
	serviceAccountIDs map[string]bool
	log               *zerolog.Logger
}

// newCS3UserResolver builds a resolver from the storage service's existing
// gateway address + service-account credentials. It returns an error when any
// of them is missing so the caller can degrade gracefully to a nil resolver
// (identity reaping disabled, internal residue sweep still active) rather than
// failing driver construction.
func newCS3UserResolver(gatewayAddr, saID, saSecret string, serviceAccountIDs []string, log *zerolog.Logger) (UserResolver, error) {
	if gatewayAddr == "" || saID == "" || saSecret == "" {
		return nil, fmt.Errorf("kvfs: user resolver needs gateway_addr + service_account_id + service_account_secret")
	}
	saMap := make(map[string]bool, len(serviceAccountIDs))
	for _, id := range serviceAccountIDs {
		if id != "" {
			saMap[id] = true
		}
	}
	return &cs3UserResolver{gatewayAddr: gatewayAddr, saID: saID, saSecret: saSecret, serviceAccountIDs: saMap, log: log}, nil
}

func (r *cs3UserResolver) ResolveLiveness(ctx context.Context, ownerIDs []string) (map[string]bool, error) {
	gwc, err := pool.GetGatewayServiceClient(r.gatewayAddr)
	if err != nil {
		return nil, fmt.Errorf("kvfs: resolver could not reach gateway %q: %w", r.gatewayAddr, err)
	}
	actx, err := utils.GetServiceUserContextWithContext(ctx, gwc, r.saID, r.saSecret)
	if err != nil {
		return nil, fmt.Errorf("kvfs: resolver could not authenticate service account: %w", err)
	}

	out := make(map[string]bool, len(ownerIDs))
	for _, id := range ownerIDs {
		if id == "" {
			continue
		}
		if r.serviceAccountIDs[id] {
			out[id] = true
			continue
		}
		res, err := gwc.GetUser(actx, &user.GetUserRequest{UserId: &user.UserId{OpaqueId: id}})
		if err != nil {
			// Transient RPC failure — leave absent (unknown, never reaped).
			continue
		}
		switch res.GetStatus().GetCode() {
		case rpc.Code_CODE_OK:
			out[id] = true
		case rpc.Code_CODE_NOT_FOUND:
			out[id] = false
		default:
			// Any other status (internal, unauthenticated, …) is ambiguous:
			// leave absent so the GC does not reap on it.
		}
	}
	return out, nil
}
