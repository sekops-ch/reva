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
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
)

// --- Options parsing tests ---

func TestOptionsParseNATSReplicas(t *testing.T) {
	m := map[string]interface{}{
		"nats_nodes":    []string{"nats:4222"},
		"s3.endpoint":   "http://s3:9000",
		"s3.bucket":     "test",
		"s3.access_key": "key",
		"s3.secret_key": "secret",
		"nats_replicas": 3,
	}
	opts, err := parseConfig(m)
	if err != nil {
		t.Fatalf("parseConfig failed: %v", err)
	}
	if opts.NATSReplicas != 3 {
		t.Errorf("NATSReplicas = %d, want 3", opts.NATSReplicas)
	}
}

func TestOptionsDefaultNATSReplicas(t *testing.T) {
	m := map[string]interface{}{
		"nats_nodes":    []string{"nats:4222"},
		"s3.endpoint":   "http://s3:9000",
		"s3.bucket":     "test",
		"s3.access_key": "key",
		"s3.secret_key": "secret",
	}
	opts, err := parseConfig(m)
	if err != nil {
		t.Fatalf("parseConfig failed: %v", err)
	}
	if opts.NATSReplicas != 1 {
		t.Errorf("NATSReplicas = %d, want 1 (default)", opts.NATSReplicas)
	}
}

func TestOptionsNATSReplicasZeroDefaultsToOne(t *testing.T) {
	m := map[string]interface{}{
		"nats_nodes":    []string{"nats:4222"},
		"s3.endpoint":   "http://s3:9000",
		"s3.bucket":     "test",
		"s3.access_key": "key",
		"s3.secret_key": "secret",
		"nats_replicas": 0,
	}
	opts, err := parseConfig(m)
	if err != nil {
		t.Fatalf("parseConfig failed: %v", err)
	}
	if opts.NATSReplicas != 1 {
		t.Errorf("NATSReplicas = %d, want 1 (0 should default to 1)", opts.NATSReplicas)
	}
}

// --- Mock types for ensureReplicas testing ---
// These implement the narrow streamUpdater and kvStatusProvider interfaces.

type mockKVStatus struct {
	bucket   string
	replicas int
}

func (s *mockKVStatus) Bucket() string            { return s.bucket }
func (s *mockKVStatus) Values() uint64             { return 0 }
func (s *mockKVStatus) History() int64             { return 1 }
func (s *mockKVStatus) TTL() time.Duration         { return 0 }
func (s *mockKVStatus) BackingStore() string       { return "JetStream" }
func (s *mockKVStatus) Bytes() uint64              { return 0 }
func (s *mockKVStatus) IsCompressed() bool         { return false }
func (s *mockKVStatus) Config() nats.KeyValueConfig {
	return nats.KeyValueConfig{Bucket: s.bucket, Replicas: s.replicas}
}

type mockKVStatusProvider struct {
	statusResult nats.KeyValueStatus
	statusErr    error
}

func (m *mockKVStatusProvider) Status() (nats.KeyValueStatus, error) {
	return m.statusResult, m.statusErr
}

type mockStreamUpdater struct {
	streamInfo      *nats.StreamInfo
	streamInfoErr   error
	updateStreamCfg *nats.StreamConfig
	updateStreamErr error
	updateCalled    bool
}

func (m *mockStreamUpdater) StreamInfo(stream string, opts ...nats.JSOpt) (*nats.StreamInfo, error) {
	return m.streamInfo, m.streamInfoErr
}

func (m *mockStreamUpdater) UpdateStream(cfg *nats.StreamConfig, opts ...nats.JSOpt) (*nats.StreamInfo, error) {
	m.updateCalled = true
	m.updateStreamCfg = cfg
	if m.updateStreamErr != nil {
		return nil, m.updateStreamErr
	}
	return &nats.StreamInfo{Config: *cfg}, nil
}

// --- ensureReplicas tests ---

func TestEnsureReplicas_UpgradesR1ToR3(t *testing.T) {
	kv := &mockKVStatusProvider{
		statusResult: &mockKVStatus{bucket: "oc-nodes", replicas: 1},
	}
	js := &mockStreamUpdater{
		streamInfo: &nats.StreamInfo{
			Config: nats.StreamConfig{Name: "KV_oc-nodes", Replicas: 1},
		},
	}

	err := ensureReplicas(js, kv, 3)
	if err != nil {
		t.Fatalf("ensureReplicas failed: %v", err)
	}
	if !js.updateCalled {
		t.Error("expected UpdateStream to be called")
	}
	if js.updateStreamCfg.Replicas != 3 {
		t.Errorf("UpdateStream replicas = %d, want 3", js.updateStreamCfg.Replicas)
	}
}

func TestEnsureReplicas_NoOpWhenAlreadyR3(t *testing.T) {
	kv := &mockKVStatusProvider{
		statusResult: &mockKVStatus{bucket: "oc-nodes", replicas: 3},
	}
	js := &mockStreamUpdater{}

	err := ensureReplicas(js, kv, 3)
	if err != nil {
		t.Fatalf("ensureReplicas failed: %v", err)
	}
	if js.updateCalled {
		t.Error("UpdateStream should NOT be called when already at desired replicas")
	}
}

func TestEnsureReplicas_NoOpWhenDesiredIsOne(t *testing.T) {
	kv := &mockKVStatusProvider{
		statusResult: &mockKVStatus{bucket: "oc-nodes", replicas: 1},
	}
	js := &mockStreamUpdater{}

	err := ensureReplicas(js, kv, 1)
	if err != nil {
		t.Fatalf("ensureReplicas failed: %v", err)
	}
	if js.updateCalled {
		t.Error("UpdateStream should NOT be called when desired <= 1")
	}
}

func TestEnsureReplicas_NoOpWhenDesiredIsZero(t *testing.T) {
	kv := &mockKVStatusProvider{}
	js := &mockStreamUpdater{}

	err := ensureReplicas(js, kv, 0)
	if err != nil {
		t.Fatalf("ensureReplicas failed: %v", err)
	}
	if js.updateCalled {
		t.Error("UpdateStream should NOT be called when desired is 0")
	}
}

func TestEnsureReplicas_StatusError(t *testing.T) {
	kv := &mockKVStatusProvider{statusErr: errors.New("nats unavailable")}
	js := &mockStreamUpdater{}

	err := ensureReplicas(js, kv, 3)
	if err == nil {
		t.Fatal("expected error from Status(), got nil")
	}
	if js.updateCalled {
		t.Error("UpdateStream should NOT be called on Status() error")
	}
}

func TestEnsureReplicas_StreamInfoError(t *testing.T) {
	kv := &mockKVStatusProvider{
		statusResult: &mockKVStatus{bucket: "oc-nodes", replicas: 1},
	}
	js := &mockStreamUpdater{streamInfoErr: errors.New("stream not found")}

	err := ensureReplicas(js, kv, 3)
	if err == nil {
		t.Fatal("expected error from StreamInfo(), got nil")
	}
}

func TestEnsureReplicas_UpdateStreamError(t *testing.T) {
	kv := &mockKVStatusProvider{
		statusResult: &mockKVStatus{bucket: "oc-nodes", replicas: 1},
	}
	js := &mockStreamUpdater{
		streamInfo: &nats.StreamInfo{
			Config: nats.StreamConfig{Name: "KV_oc-nodes", Replicas: 1},
		},
		updateStreamErr: errors.New("insufficient peers"),
	}

	err := ensureReplicas(js, kv, 3)
	if err == nil {
		t.Fatal("expected error from UpdateStream(), got nil")
	}
}

func TestEnsureReplicas_NeverDowngrades(t *testing.T) {
	kv := &mockKVStatusProvider{
		statusResult: &mockKVStatus{bucket: "oc-nodes", replicas: 5},
	}
	js := &mockStreamUpdater{}

	err := ensureReplicas(js, kv, 3)
	if err != nil {
		t.Fatalf("ensureReplicas failed: %v", err)
	}
	if js.updateCalled {
		t.Error("UpdateStream should NOT be called when current > desired (no downgrade)")
	}
}

// --- ensureMaxValueSize tests ---
//
// ensureMaxValueSize widens an existing KV bucket's MaxValueSize so a helm
// upgrade that raises STORAGE_USERS_KVFS_CHILDREN_MAX_VALUE_SIZE actually
// takes effect on a bucket the Create path skipped. NATS server max_payload
// is the hard upstream ceiling — if it is lower than desired the
// UpdateStream call surfaces an error, which the caller logs as WARNING+HINT.

const desiredMaxValueSize int32 = 16 * 1024 * 1024

func TestEnsureMaxValueSize_NoOpWhenDesiredIsZero(t *testing.T) {
	// statusResult is nil — if Status() were consulted the test would crash.
	kv := &mockKVStatusProvider{}
	js := &mockStreamUpdater{}

	if err := ensureMaxValueSize(js, kv, 0); err != nil {
		t.Fatalf("ensureMaxValueSize failed: %v", err)
	}
	if js.updateCalled {
		t.Error("UpdateStream should NOT be called when desired is 0")
	}
}

func TestEnsureMaxValueSize_NoOpWhenDesiredIsNegative(t *testing.T) {
	kv := &mockKVStatusProvider{}
	js := &mockStreamUpdater{}

	if err := ensureMaxValueSize(js, kv, -1); err != nil {
		t.Fatalf("ensureMaxValueSize failed: %v", err)
	}
	if js.updateCalled {
		t.Error("UpdateStream should NOT be called when desired is negative")
	}
}

func TestEnsureMaxValueSize_StatusError(t *testing.T) {
	kv := &mockKVStatusProvider{statusErr: errors.New("nats unavailable")}
	js := &mockStreamUpdater{}

	err := ensureMaxValueSize(js, kv, desiredMaxValueSize)
	if err == nil {
		t.Fatal("expected error from Status(), got nil")
	}
	if js.updateCalled {
		t.Error("UpdateStream should NOT be called on Status() error")
	}
}

func TestEnsureMaxValueSize_StreamInfoError(t *testing.T) {
	kv := &mockKVStatusProvider{
		statusResult: &mockKVStatus{bucket: "oc-children"},
	}
	js := &mockStreamUpdater{streamInfoErr: errors.New("stream not found")}

	err := ensureMaxValueSize(js, kv, desiredMaxValueSize)
	if err == nil {
		t.Fatal("expected error from StreamInfo(), got nil")
	}
	// Belt-and-braces — the wrapped message must point at the KV_<bucket>
	// stream name so an operator can find the failing stream quickly.
	if msg := err.Error(); !contains(msg, "KV_oc-children") {
		t.Errorf("error message = %q, want substring %q", msg, "KV_oc-children")
	}
	if js.updateCalled {
		t.Error("UpdateStream should NOT be called on StreamInfo() error")
	}
}

func TestEnsureMaxValueSize_NoOpWhenAlreadyAtCeiling(t *testing.T) {
	kv := &mockKVStatusProvider{
		statusResult: &mockKVStatus{bucket: "oc-children"},
	}
	js := &mockStreamUpdater{
		streamInfo: &nats.StreamInfo{
			Config: nats.StreamConfig{Name: "KV_oc-children", MaxMsgSize: desiredMaxValueSize},
		},
	}

	if err := ensureMaxValueSize(js, kv, desiredMaxValueSize); err != nil {
		t.Fatalf("ensureMaxValueSize failed: %v", err)
	}
	if js.updateCalled {
		t.Error("UpdateStream should NOT be called when already at desired ceiling")
	}
}

func TestEnsureMaxValueSize_NoOpWhenAboveCeiling(t *testing.T) {
	kv := &mockKVStatusProvider{
		statusResult: &mockKVStatus{bucket: "oc-children"},
	}
	js := &mockStreamUpdater{
		streamInfo: &nats.StreamInfo{
			Config: nats.StreamConfig{Name: "KV_oc-children", MaxMsgSize: 32 * 1024 * 1024},
		},
	}

	if err := ensureMaxValueSize(js, kv, desiredMaxValueSize); err != nil {
		t.Fatalf("ensureMaxValueSize failed: %v", err)
	}
	if js.updateCalled {
		t.Error("UpdateStream should NOT be called when current > desired (no downgrade)")
	}
}

func TestEnsureMaxValueSize_WidensWhenBelow(t *testing.T) {
	kv := &mockKVStatusProvider{
		statusResult: &mockKVStatus{bucket: "oc-children"},
	}
	js := &mockStreamUpdater{
		streamInfo: &nats.StreamInfo{
			Config: nats.StreamConfig{Name: "KV_oc-children", MaxMsgSize: 1 * 1024 * 1024},
		},
	}

	if err := ensureMaxValueSize(js, kv, desiredMaxValueSize); err != nil {
		t.Fatalf("ensureMaxValueSize failed: %v", err)
	}
	if !js.updateCalled {
		t.Fatal("expected UpdateStream to be called when current < desired")
	}
	if js.updateStreamCfg.MaxMsgSize != desiredMaxValueSize {
		t.Errorf("UpdateStream MaxMsgSize = %d, want %d", js.updateStreamCfg.MaxMsgSize, desiredMaxValueSize)
	}
	if js.updateStreamCfg.Name != "KV_oc-children" {
		t.Errorf("UpdateStream Name = %q, want %q (must carry the existing stream identity through)", js.updateStreamCfg.Name, "KV_oc-children")
	}
}

func TestEnsureMaxValueSize_WidensFromUnsetSentinel(t *testing.T) {
	// NATS treats stream-level MaxMsgSize=-1 as "unlimited" at the stream
	// layer, but the server-level max_payload still caps the publish. So
	// we must still widen to the explicit desired value — the sentinel
	// alone is not enough to make the children bucket safe.
	kv := &mockKVStatusProvider{
		statusResult: &mockKVStatus{bucket: "oc-children"},
	}
	js := &mockStreamUpdater{
		streamInfo: &nats.StreamInfo{
			Config: nats.StreamConfig{Name: "KV_oc-children", MaxMsgSize: -1},
		},
	}

	if err := ensureMaxValueSize(js, kv, desiredMaxValueSize); err != nil {
		t.Fatalf("ensureMaxValueSize failed: %v", err)
	}
	if !js.updateCalled {
		t.Fatal("expected UpdateStream to be called when current is -1 (unset sentinel) and desired > 0")
	}
	if js.updateStreamCfg.MaxMsgSize != desiredMaxValueSize {
		t.Errorf("UpdateStream MaxMsgSize = %d, want %d", js.updateStreamCfg.MaxMsgSize, desiredMaxValueSize)
	}
}

func TestEnsureMaxValueSize_UpdateStreamError(t *testing.T) {
	kv := &mockKVStatusProvider{
		statusResult: &mockKVStatus{bucket: "oc-children"},
	}
	js := &mockStreamUpdater{
		streamInfo: &nats.StreamInfo{
			Config: nats.StreamConfig{Name: "KV_oc-children", MaxMsgSize: 1 * 1024 * 1024},
		},
		updateStreamErr: errors.New("invalid stream config"),
	}

	err := ensureMaxValueSize(js, kv, desiredMaxValueSize)
	if err == nil {
		t.Fatal("expected error from UpdateStream(), got nil")
	}
	if !js.updateCalled {
		t.Error("UpdateStream should have been called before failure")
	}
	// The wrapped message must contain both the current and desired sizes
	// so the operator can correlate with the WARNING+HINT log in kvstore.go.
	msg := err.Error()
	if !contains(msg, "1048576") || !contains(msg, "16777216") || !contains(msg, "KV_oc-children") {
		t.Errorf("error message = %q, want substrings 1048576 + 16777216 + KV_oc-children", msg)
	}
}

// contains is a tiny strings.Contains stand-in to keep the test file's
// import surface narrow.
func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// ensureChildrenValueSize downgrades an ensure failure to a structured
// warning (bucket + requested size + hint fields) — the non-fatal contract
// the old WARNING+HINT prints documented. Uses the same mock seams as the
// ensureMaxValueSize tests, so the rejection is deterministic.
func TestEnsureChildrenValueSize_RejectionLogsStructuredWarning(t *testing.T) {
	kv := &mockKVStatusProvider{
		statusResult: &mockKVStatus{bucket: "oc-children"},
	}
	js := &mockStreamUpdater{
		streamInfo: &nats.StreamInfo{
			Config: nats.StreamConfig{Name: "KV_oc-children", MaxMsgSize: 1 * 1024 * 1024},
		},
		updateStreamErr: errors.New("maximum message size exceeds max payload"),
	}

	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	ensureChildrenValueSize(js, kv, "oc-children", desiredMaxValueSize, &logger)

	out := buf.String()
	if !contains(out, `"level":"warn"`) {
		t.Fatalf("expected warn level, got: %q", out)
	}
	if !contains(out, "ensureMaxValueSize failed") || !contains(out, "oc-children") {
		t.Fatalf("expected message + bucket field, got: %q", out)
	}
	if !contains(out, "hint") || !contains(out, "max_payload") {
		t.Fatalf("expected the HINT semantics as a log field, got: %q", out)
	}
	if !contains(out, "children_max_value_size") {
		t.Fatalf("expected the requested size as a log field, got: %q", out)
	}
}

// The success path must stay silent — the warning fires only on rejection.
func TestEnsureChildrenValueSize_SuccessLogsNothing(t *testing.T) {
	kv := &mockKVStatusProvider{
		statusResult: &mockKVStatus{bucket: "oc-children"},
	}
	js := &mockStreamUpdater{
		streamInfo: &nats.StreamInfo{
			Config: nats.StreamConfig{Name: "KV_oc-children", MaxMsgSize: 1 * 1024 * 1024},
		},
	}

	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	ensureChildrenValueSize(js, kv, "oc-children", desiredMaxValueSize, &logger)

	if out := buf.String(); out != "" {
		t.Fatalf("expected no log output on success, got: %q", out)
	}
	if !js.updateCalled {
		t.Fatal("expected the widen path to run")
	}
}
