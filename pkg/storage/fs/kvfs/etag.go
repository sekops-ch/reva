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
	"crypto/sha256"
	"fmt"
)

// calculateEtag derives an etag from a node ID and the time of a change (Unix ns), so every
// commit gets a new one even when a client sends the same modification time again. Etags are
// stored bare and handed out quoted (quotedEtag).
func calculateEtag(nodeID string, changed int64) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", nodeID, changed)))
	return fmt.Sprintf("%x", h[:8])
}

// quotedEtag returns a stored etag the way clients get it, in double quotes as decomposedfs
// hands them out; "" stays "".
func quotedEtag(etag string) string {
	if etag == "" {
		return ""
	}
	return `"` + etag + `"`
}

// bareEtag strips one pair of double quotes.
func bareEtag(etag string) string {
	if len(etag) >= 2 && etag[0] == '"' && etag[len(etag)-1] == '"' {
		return etag[1 : len(etag)-1]
	}
	return etag
}

// etagMatches reports whether a client's If-Match names the stored etag, quoted or bare. "*",
// weak etags and lists are not special: they never match, as before etags were quoted.
func etagMatches(ifMatch, stored string) bool {
	return bareEtag(ifMatch) == bareEtag(stored)
}

// storedIfMatch is the If-Match an upload keeps: bare, so that pods comparing raw values still
// match it, except a quoted empty etag, which must keep failing.
func storedIfMatch(ifMatch string) string {
	if bare := bareEtag(ifMatch); bare != "" {
		return bare
	}
	return ifMatch
}
