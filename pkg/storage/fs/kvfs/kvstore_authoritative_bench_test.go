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

// Measures the per-confirm decode cost on large children maps: the streaming
// name scan against the full-map unmarshal it replaced.

import (
	"fmt"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func BenchmarkChildConfirmDecode(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 50_000} {
		m := make(ChildMap, n)
		for i := 0; i < n; i++ {
			m[fmt.Sprintf("file-%06d.txt", i)] = fmt.Sprintf("0bd2f662-%08d-4ad1-b2a4-0f3a2b7c9d%02d", i, i%100)
		}
		raw, err := msgpack.Marshal(m)
		if err != nil {
			b.Fatalf("marshal: %v", err)
		}
		b.Run(fmt.Sprintf("scan-%d", n), func(b *testing.B) {
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, found, err := scanChildMap(raw, "absent-name.txt"); err != nil || found {
					b.Fatal("scan must confirm absence")
				}
			}
		})
		b.Run(fmt.Sprintf("full-unmarshal-%d", n), func(b *testing.B) {
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var cm ChildMap
				if err := msgpack.Unmarshal(raw, &cm); err != nil {
					b.Fatal(err)
				}
				if _, found := cm["absent-name.txt"]; found {
					b.Fatal("unexpected name")
				}
			}
		})
	}
}
