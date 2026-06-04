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

import "testing"

// Regression guard for the shared-bucket data-loss incident: storage-users and
// storage-system both point at one S3 bucket with bare "<spaceID>/" keys, so the
// storage-users GC residue sweep enumerated storage-system space prefixes, found
// them absent from oc-spaces, classified them as "ghost spaces" and deleted live
// settings/role blobs. The fix namespaces every physical key under the instance's
// blob prefix; these tests pin that contract so it can never regress.

func TestBlobNamespace_PhysicalLogicalRoundTrip(t *testing.T) {
	cases := []struct {
		prefix  string
		logical string
		phys    string
	}{
		{"oc", "space-1/ab/cd/ef/blob", "oc/space-1/ab/cd/ef/blob"},
		{"sys-", "space-1/ab/cd/ef/blob", "sys-/space-1/ab/cd/ef/blob"},
		{"", "space-1/ab/cd/ef/blob", "space-1/ab/cd/ef/blob"}, // no namespacing
	}
	for _, c := range cases {
		bs := &S3Blobstore{prefix: c.prefix}
		if got := bs.physicalKey(c.logical); got != c.phys {
			t.Errorf("prefix %q: physicalKey(%q) = %q, want %q", c.prefix, c.logical, got, c.phys)
		}
		if got := bs.logicalKey(c.phys); got != c.logical {
			t.Errorf("prefix %q: logicalKey(%q) = %q, want %q (round-trip)", c.prefix, c.phys, got, c.logical)
		}
	}
}

// Two instances sharing a bucket must map the SAME logical key to DISJOINT
// physical keys — otherwise one instance's blobs can collide with or be
// enumerated by the other.
func TestBlobNamespace_InstancesAreDisjoint(t *testing.T) {
	oc := &S3Blobstore{prefix: "oc"}
	sys := &S3Blobstore{prefix: "sys-"}
	logical := "f1bdd61a-da7c-49fc-8203-0558109d1b4f/ab/cd/ef/blob"
	if oc.physicalKey(logical) == sys.physicalKey(logical) {
		t.Fatalf("storage-users and storage-system collide on physical key %q", oc.physicalKey(logical))
	}
	// The oc instance must NOT strip a sys- key (it isn't ours): logicalKey is a
	// no-op on a foreign key, so a foreign object can never be mistaken for one
	// of ours in a ref-map lookup.
	foreign := sys.physicalKey(logical)
	if oc.logicalKey(foreign) != foreign {
		t.Errorf("oc.logicalKey wrongly rewrote a foreign (sys-) key %q -> %q", foreign, oc.logicalKey(foreign))
	}
}

// parseSpacePrefix is the heart of the fix: a residue sweep must only ever see
// space IDs inside its OWN namespace. A common prefix belonging to the other
// instance (or any unexpected shape) must return ok=false so it is never reaped.
func TestParseSpacePrefix_ScopedToInstance(t *testing.T) {
	cases := []struct {
		name       string
		commonPref string
		blobPrefix string
		wantID     string
		wantOK     bool
	}{
		{"own space (oc)", "oc/space-1/", "oc", "space-1", true},
		{"own space (sys-)", "sys-/f1bdd61a/", "sys-", "f1bdd61a", true},
		// THE BUG: the oc instance must NOT treat a sys- space as a ghost.
		{"foreign space not reaped", "sys-/f1bdd61a/", "oc", "", false},
		{"foreign space not reaped (reverse)", "oc/space-1/", "sys-", "", false},
		{"not a common prefix (no trailing slash)", "oc/space-1", "oc", "", false},
		{"empty space id", "oc//", "oc", "", false},
		{"nested below space level ignored", "oc/space-1/sub/", "oc", "", false},
		{"legacy unprefixed", "space-1/", "", "space-1", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			id, ok := parseSpacePrefix(c.commonPref, c.blobPrefix)
			if ok != c.wantOK || id != c.wantID {
				t.Errorf("parseSpacePrefix(%q, %q) = (%q, %v), want (%q, %v)",
					c.commonPref, c.blobPrefix, id, ok, c.wantID, c.wantOK)
			}
		})
	}
}
