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
	"testing"
)

// minimalConfig returns the smallest config map that parseConfig accepts.
func minimalConfig() map[string]interface{} {
	return map[string]interface{}{
		"nats_nodes":    []string{"nats://localhost:4222"},
		"s3.endpoint":   "https://s3.example.com",
		"s3.bucket":     "test",
		"s3.access_key": "key",
		"s3.secret_key": "secret",
	}
}

// resolveMaxCASRetries is the single source of truth for the retry bound,
// shared by the driver and the KV store. It must return the configured value
// when positive and the package default otherwise (nil opts, zero, negative).
func TestResolveMaxCASRetries(t *testing.T) {
	cases := []struct {
		name string
		opts *Options
		want int
	}{
		{"configured", &Options{MaxCASRetries: 50}, 50},
		{"zero falls back to default", &Options{MaxCASRetries: 0}, defaultMaxCASRetries},
		{"negative falls back to default", &Options{MaxCASRetries: -1}, defaultMaxCASRetries},
		{"nil opts falls back to default", nil, defaultMaxCASRetries},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveMaxCASRetries(tc.opts); got != tc.want {
				t.Errorf("resolveMaxCASRetries(%+v) = %d, want %d", tc.opts, got, tc.want)
			}
		})
	}
}

// resolveMaxDeleteDepth mirrors the resolveMaxCASRetries contract: the
// configured value when positive, the package default otherwise.
func TestResolveMaxDeleteDepth(t *testing.T) {
	cases := []struct {
		name string
		opts *Options
		want int
	}{
		{"configured", &Options{MaxDeleteDepth: 50}, 50},
		{"zero falls back to default", &Options{MaxDeleteDepth: 0}, defaultMaxDeleteDepth},
		{"negative falls back to default", &Options{MaxDeleteDepth: -1}, defaultMaxDeleteDepth},
		{"nil opts falls back to default", nil, defaultMaxDeleteDepth},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveMaxDeleteDepth(tc.opts); got != tc.want {
				t.Errorf("resolveMaxDeleteDepth(%+v) = %d, want %d", tc.opts, got, tc.want)
			}
		})
	}
}

// A configured max_delete_depth must survive decode unchanged.
func TestOptionsParseMaxDeleteDepth(t *testing.T) {
	m := minimalConfig()
	m["max_delete_depth"] = 50
	opts, err := parseConfig(m)
	if err != nil {
		t.Fatalf("parseConfig failed: %v", err)
	}
	if opts.MaxDeleteDepth != 50 {
		t.Errorf("expected MaxDeleteDepth=50, got %d", opts.MaxDeleteDepth)
	}
}

// An omitted max_delete_depth must default to defaultMaxDeleteDepth via
// init(); an explicit zero must be normalised the same way.
func TestOptionsDefaultMaxDeleteDepth(t *testing.T) {
	opts, err := parseConfig(minimalConfig())
	if err != nil {
		t.Fatalf("parseConfig failed: %v", err)
	}
	if opts.MaxDeleteDepth != defaultMaxDeleteDepth {
		t.Errorf("expected MaxDeleteDepth=%d, got %d", defaultMaxDeleteDepth, opts.MaxDeleteDepth)
	}

	m := minimalConfig()
	m["max_delete_depth"] = 0
	opts, err = parseConfig(m)
	if err != nil {
		t.Fatalf("parseConfig failed: %v", err)
	}
	if opts.MaxDeleteDepth != defaultMaxDeleteDepth {
		t.Errorf("explicit zero: expected MaxDeleteDepth=%d, got %d", defaultMaxDeleteDepth, opts.MaxDeleteDepth)
	}
}

// A configured max_cas_retries must survive decode unchanged.
func TestOptionsParseMaxCASRetries(t *testing.T) {
	m := minimalConfig()
	m["max_cas_retries"] = 50
	opts, err := parseConfig(m)
	if err != nil {
		t.Fatalf("parseConfig failed: %v", err)
	}
	if opts.MaxCASRetries != 50 {
		t.Errorf("expected MaxCASRetries=50, got %d", opts.MaxCASRetries)
	}
}

// An omitted max_cas_retries must default to defaultMaxCASRetries via init().
func TestOptionsDefaultMaxCASRetries(t *testing.T) {
	opts, err := parseConfig(minimalConfig())
	if err != nil {
		t.Fatalf("parseConfig failed: %v", err)
	}
	if opts.MaxCASRetries != defaultMaxCASRetries {
		t.Errorf("expected MaxCASRetries=%d, got %d", defaultMaxCASRetries, opts.MaxCASRetries)
	}
}

// An explicit zero must be normalised to the default (exercises the <= 0 guard
// in init()).
func TestOptionsMaxCASRetriesZeroDefaults(t *testing.T) {
	m := minimalConfig()
	m["max_cas_retries"] = 0
	opts, err := parseConfig(m)
	if err != nil {
		t.Fatalf("parseConfig failed: %v", err)
	}
	if opts.MaxCASRetries != defaultMaxCASRetries {
		t.Errorf("expected MaxCASRetries=%d, got %d", defaultMaxCASRetries, opts.MaxCASRetries)
	}
}

// The driver helper must read the configured value, and defend against a
// fixture that builds Options{} directly (or a nil opts) without init().
func TestDriverMaxCASRetriesHelper(t *testing.T) {
	cases := []struct {
		name string
		d    *kvfsDriver
		want int
	}{
		{"configured", &kvfsDriver{opts: &Options{MaxCASRetries: 7}}, 7},
		{"zero falls back", &kvfsDriver{opts: &Options{MaxCASRetries: 0}}, defaultMaxCASRetries},
		{"nil opts falls back", &kvfsDriver{}, defaultMaxCASRetries},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.d.maxCASRetries(); got != tc.want {
				t.Errorf("maxCASRetries() = %d, want %d", got, tc.want)
			}
		})
	}
}
