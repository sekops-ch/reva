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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// gatherKVFSMetrics returns all metric families whose name starts with "kvfs_".
func gatherKVFSMetrics(t *testing.T) map[string]*dto.MetricFamily {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}
	result := make(map[string]*dto.MetricFamily)
	for _, mf := range families {
		if strings.HasPrefix(mf.GetName(), "kvfs_") {
			result[mf.GetName()] = mf
		}
	}
	return result
}

// findMetric searches a metric family for a metric matching the given label pairs.
func findMetric(mf *dto.MetricFamily, labels map[string]string) *dto.Metric {
	for _, m := range mf.GetMetric() {
		match := true
		for wantName, wantValue := range labels {
			found := false
			for _, lp := range m.GetLabel() {
				if lp.GetName() == wantName && lp.GetValue() == wantValue {
					found = true
					break
				}
			}
			if !found {
				match = false
				break
			}
		}
		if match {
			return m
		}
	}
	return nil
}

func TestMetricsRegistration(t *testing.T) {
	metrics := gatherKVFSMetrics(t)

	expected := []string{
		"kvfs_cas_retries_total",
		"kvfs_cas_exhausted_total",
		"kvfs_cas_conflicts_total",
		"kvfs_kv_operation_duration_seconds",
		"kvfs_blob_operation_duration_seconds",
		"kvfs_upload_in_flight",
		"kvfs_max_cas_retries",
		"kvfs_tree_size_drift_total",
	}

	// promauto registers metrics at init time, but counter/gauge/histogram vecs
	// only appear in Gather() after at least one label combination is observed.
	// Force-initialize all label combinations so they appear.
	CASRetries.WithLabelValues("upload")
	CASExhausted.WithLabelValues("upload")
	CASConflicts.WithLabelValues("nodes")
	KVOperationDuration.WithLabelValues("nodes", "get")
	BlobOperationDuration.WithLabelValues("upload")
	UploadInFlight.WithLabelValues("simple")
	MaxCASRetriesGauge.WithLabelValues("oc")
	TreeSizeDrift.Inc()

	metrics = gatherKVFSMetrics(t)

	for _, name := range expected {
		if _, ok := metrics[name]; !ok {
			t.Errorf("metric %q not found in gathered metrics", name)
		}
	}
}

func TestCASRetriesCounter(t *testing.T) {
	before := getCounterValue(t, "kvfs_cas_retries_total", map[string]string{"operation": "upload"})
	CASRetries.WithLabelValues("upload").Inc()
	CASRetries.WithLabelValues("upload").Inc()
	CASRetries.WithLabelValues("upload").Inc()
	after := getCounterValue(t, "kvfs_cas_retries_total", map[string]string{"operation": "upload"})

	if after-before != 3 {
		t.Errorf("CASRetries: expected delta 3, got %v", after-before)
	}
}

func TestCASRetriesLabels(t *testing.T) {
	ops := []string{"upload", "children", "node", "space"}
	for _, op := range ops {
		before := getCounterValue(t, "kvfs_cas_retries_total", map[string]string{"operation": op})
		CASRetries.WithLabelValues(op).Inc()
		after := getCounterValue(t, "kvfs_cas_retries_total", map[string]string{"operation": op})
		if after-before != 1 {
			t.Errorf("CASRetries[%s]: expected delta 1, got %v", op, after-before)
		}
	}
}

func TestCASExhaustedCounter(t *testing.T) {
	before := getCounterValue(t, "kvfs_cas_exhausted_total", map[string]string{"operation": "upload"})
	CASExhausted.WithLabelValues("upload").Inc()
	after := getCounterValue(t, "kvfs_cas_exhausted_total", map[string]string{"operation": "upload"})

	if after-before != 1 {
		t.Errorf("CASExhausted: expected delta 1, got %v", after-before)
	}
}

func TestCASConflictsCounter(t *testing.T) {
	buckets := []string{"nodes", "children", "spaces"}
	for _, bucket := range buckets {
		before := getCounterValue(t, "kvfs_cas_conflicts_total", map[string]string{"bucket": bucket})
		CASConflicts.WithLabelValues(bucket).Inc()
		after := getCounterValue(t, "kvfs_cas_conflicts_total", map[string]string{"bucket": bucket})
		if after-before != 1 {
			t.Errorf("CASConflicts[%s]: expected delta 1, got %v", bucket, after-before)
		}
	}
}

func TestKVOperationDurationHistogram(t *testing.T) {
	buckets := []string{"nodes", "children", "spaces", "trash", "versions", "uploads"}
	ops := []string{"get", "put", "create", "update", "delete"}

	for _, bucket := range buckets {
		for _, op := range ops {
			before := getHistogramCount(t, "kvfs_kv_operation_duration_seconds", map[string]string{"bucket": bucket, "op": op})
			KVOperationDuration.WithLabelValues(bucket, op).Observe(0.005)
			after := getHistogramCount(t, "kvfs_kv_operation_duration_seconds", map[string]string{"bucket": bucket, "op": op})
			if after-before != 1 {
				t.Errorf("KVOperationDuration[%s/%s]: expected count delta 1, got %v", bucket, op, after-before)
			}
		}
	}
}

func TestKVOperationDurationBuckets(t *testing.T) {
	KVOperationDuration.WithLabelValues("nodes", "get").Observe(0.002)
	KVOperationDuration.WithLabelValues("nodes", "get").Observe(0.05)
	KVOperationDuration.WithLabelValues("nodes", "get").Observe(0.5)

	metrics := gatherKVFSMetrics(t)
	mf, ok := metrics["kvfs_kv_operation_duration_seconds"]
	if !ok {
		t.Fatal("kvfs_kv_operation_duration_seconds not found")
	}

	m := findMetric(mf, map[string]string{"bucket": "nodes", "op": "get"})
	if m == nil {
		t.Fatal("metric with labels bucket=nodes, op=get not found")
	}

	h := m.GetHistogram()
	if h == nil {
		t.Fatal("histogram is nil")
	}
	if len(h.GetBucket()) == 0 {
		t.Fatal("histogram has no buckets")
	}

	expectedBounds := []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0}
	actualBounds := make([]float64, 0, len(h.GetBucket()))
	for _, b := range h.GetBucket() {
		actualBounds = append(actualBounds, b.GetUpperBound())
	}
	if len(actualBounds) != len(expectedBounds) {
		t.Errorf("KV histogram: expected %d bucket bounds, got %d", len(expectedBounds), len(actualBounds))
	}
	for i, exp := range expectedBounds {
		if i < len(actualBounds) && actualBounds[i] != exp {
			t.Errorf("KV histogram bucket[%d]: expected bound %v, got %v", i, exp, actualBounds[i])
		}
	}
}

func TestBlobOperationDurationHistogram(t *testing.T) {
	ops := []string{"upload", "download", "delete"}
	for _, op := range ops {
		before := getHistogramCount(t, "kvfs_blob_operation_duration_seconds", map[string]string{"op": op})
		BlobOperationDuration.WithLabelValues(op).Observe(0.1)
		after := getHistogramCount(t, "kvfs_blob_operation_duration_seconds", map[string]string{"op": op})
		if after-before != 1 {
			t.Errorf("BlobOperationDuration[%s]: expected count delta 1, got %v", op, after-before)
		}
	}
}

func TestBlobOperationDurationBuckets(t *testing.T) {
	BlobOperationDuration.WithLabelValues("upload").Observe(0.05)

	metrics := gatherKVFSMetrics(t)
	mf, ok := metrics["kvfs_blob_operation_duration_seconds"]
	if !ok {
		t.Fatal("kvfs_blob_operation_duration_seconds not found")
	}

	m := findMetric(mf, map[string]string{"op": "upload"})
	if m == nil {
		t.Fatal("metric with label op=upload not found")
	}

	h := m.GetHistogram()
	if h == nil {
		t.Fatal("histogram is nil")
	}

	expectedBounds := []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0}
	actualBounds := make([]float64, 0, len(h.GetBucket()))
	for _, b := range h.GetBucket() {
		actualBounds = append(actualBounds, b.GetUpperBound())
	}
	if len(actualBounds) != len(expectedBounds) {
		t.Errorf("Blob histogram: expected %d bucket bounds, got %d", len(expectedBounds), len(actualBounds))
	}
	for i, exp := range expectedBounds {
		if i < len(actualBounds) && actualBounds[i] != exp {
			t.Errorf("Blob histogram bucket[%d]: expected bound %v, got %v", i, exp, actualBounds[i])
		}
	}
}

func TestUploadInFlightGauge(t *testing.T) {
	UploadInFlight.WithLabelValues("simple").Set(0)
	before := getGaugeValue(t, "kvfs_upload_in_flight", map[string]string{"protocol": "simple"})
	if before != 0 {
		t.Fatalf("UploadInFlight: expected 0 after reset, got %v", before)
	}

	UploadInFlight.WithLabelValues("simple").Inc()
	UploadInFlight.WithLabelValues("simple").Inc()
	after := getGaugeValue(t, "kvfs_upload_in_flight", map[string]string{"protocol": "simple"})
	if after != 2 {
		t.Errorf("UploadInFlight: expected 2 after two Inc(), got %v", after)
	}

	UploadInFlight.WithLabelValues("simple").Dec()
	final := getGaugeValue(t, "kvfs_upload_in_flight", map[string]string{"protocol": "simple"})
	if final != 1 {
		t.Errorf("UploadInFlight: expected 1 after Dec(), got %v", final)
	}

	UploadInFlight.WithLabelValues("simple").Set(0)
}

func TestUploadInFlightProtocols(t *testing.T) {
	protocols := []string{"simple", "tus"}
	for _, p := range protocols {
		UploadInFlight.WithLabelValues(p).Set(0)
		UploadInFlight.WithLabelValues(p).Inc()
		val := getGaugeValue(t, "kvfs_upload_in_flight", map[string]string{"protocol": p})
		if val != 1 {
			t.Errorf("UploadInFlight[%s]: expected 1, got %v", p, val)
		}
		UploadInFlight.WithLabelValues(p).Set(0)
	}
}

func TestTreeSizeDriftCounter(t *testing.T) {
	before := getCounterValue(t, "kvfs_tree_size_drift_total", nil)
	TreeSizeDrift.Inc()
	TreeSizeDrift.Inc()
	after := getCounterValue(t, "kvfs_tree_size_drift_total", nil)

	if after-before != 2 {
		t.Errorf("TreeSizeDrift: expected delta 2, got %v", after-before)
	}
}

func TestMetricDescriptions(t *testing.T) {
	tests := []struct {
		name     string
		wantHelp string
	}{
		{"kvfs_cas_retries_total", "Total number of CAS retry attempts in the kvfs driver"},
		{"kvfs_cas_exhausted_total", "Total number of CAS retry loops that exhausted all attempts"},
		{"kvfs_cas_conflicts_total", "Total number of CAS conflict errors detected"},
		{"kvfs_kv_operation_duration_seconds", "Duration of NATS KV operations in seconds"},
		{"kvfs_blob_operation_duration_seconds", "Duration of S3 blob operations in seconds"},
		{"kvfs_upload_in_flight", "Number of uploads currently in progress (per-pod lifetime counter; may drift — see kvfs_oldest_upload_age_seconds)"},
		{"kvfs_max_cas_retries", "Effective MaxCASRetries bound per kvfs instance (labeled by bucket prefix)"},
		{"kvfs_tree_size_drift_total", "Total number of ancestor CAS failures during tree-size propagation"},
	}

	metrics := gatherKVFSMetrics(t)
	for _, tt := range tests {
		mf, ok := metrics[tt.name]
		if !ok {
			t.Errorf("metric %q not found", tt.name)
			continue
		}
		if mf.GetHelp() != tt.wantHelp {
			t.Errorf("metric %q: help = %q, want %q", tt.name, mf.GetHelp(), tt.wantHelp)
		}
	}
}

func TestMetricTypes(t *testing.T) {
	metrics := gatherKVFSMetrics(t)

	counterMetrics := []string{
		"kvfs_cas_retries_total",
		"kvfs_cas_exhausted_total",
		"kvfs_cas_conflicts_total",
		"kvfs_tree_size_drift_total",
	}
	for _, name := range counterMetrics {
		mf, ok := metrics[name]
		if !ok {
			t.Errorf("metric %q not found", name)
			continue
		}
		if mf.GetType() != dto.MetricType_COUNTER {
			t.Errorf("metric %q: type = %v, want COUNTER", name, mf.GetType())
		}
	}

	histogramMetrics := []string{
		"kvfs_kv_operation_duration_seconds",
		"kvfs_blob_operation_duration_seconds",
	}
	for _, name := range histogramMetrics {
		mf, ok := metrics[name]
		if !ok {
			t.Errorf("metric %q not found", name)
			continue
		}
		if mf.GetType() != dto.MetricType_HISTOGRAM {
			t.Errorf("metric %q: type = %v, want HISTOGRAM", name, mf.GetType())
		}
	}

	gaugeMetrics := []string{
		"kvfs_upload_in_flight",
		"kvfs_max_cas_retries",
	}
	for _, name := range gaugeMetrics {
		mf, ok := metrics[name]
		if !ok {
			t.Errorf("metric %q not found", name)
			continue
		}
		if mf.GetType() != dto.MetricType_GAUGE {
			t.Errorf("metric %q: type = %v, want GAUGE", name, mf.GetType())
		}
	}
}

// TestMaxCASRetriesGauge asserts the effective-bound gauge keeps a distinct
// value per bucket prefix — two instances must not clobber each other.
func TestMaxCASRetriesGauge(t *testing.T) {
	MaxCASRetriesGauge.WithLabelValues("oc").Set(50)
	MaxCASRetriesGauge.WithLabelValues("sys").Set(20)

	if got := getGaugeValue(t, "kvfs_max_cas_retries", map[string]string{"prefix": "oc"}); got != 50 {
		t.Errorf("kvfs_max_cas_retries{prefix=oc} = %v, want 50", got)
	}
	if got := getGaugeValue(t, "kvfs_max_cas_retries", map[string]string{"prefix": "sys"}); got != 20 {
		t.Errorf("kvfs_max_cas_retries{prefix=sys} = %v, want 20", got)
	}
}

func TestConcurrentMetricAccess(t *testing.T) {
	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 100; j++ {
				CASRetries.WithLabelValues("upload").Inc()
				CASConflicts.WithLabelValues("nodes").Inc()
				KVOperationDuration.WithLabelValues("nodes", "get").Observe(0.001)
				BlobOperationDuration.WithLabelValues("upload").Observe(0.01)
				UploadInFlight.WithLabelValues("simple").Inc()
				UploadInFlight.WithLabelValues("simple").Dec()
				TreeSizeDrift.Inc()
			}
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify gathering doesn't panic after concurrent access
	metrics := gatherKVFSMetrics(t)
	if len(metrics) == 0 {
		t.Error("no kvfs metrics found after concurrent access")
	}
}

// --- helpers ---

func getCounterValue(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	metrics := gatherKVFSMetrics(t)
	mf, ok := metrics[name]
	if !ok {
		return 0
	}
	if labels == nil {
		labels = map[string]string{}
	}
	m := findMetric(mf, labels)
	if m == nil {
		return 0
	}
	return m.GetCounter().GetValue()
}

func getGaugeValue(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	metrics := gatherKVFSMetrics(t)
	mf, ok := metrics[name]
	if !ok {
		t.Fatalf("metric %q not found", name)
	}
	m := findMetric(mf, labels)
	if m == nil {
		t.Fatalf("metric %q with labels %v not found", name, labels)
	}
	return m.GetGauge().GetValue()
}

// TestUpdateUploadAgeMetrics: count, oldest-session age, Expires==0 skip, and
// negative-age clamp.
func TestUpdateUploadAgeMetrics(t *testing.T) {
	now := time.Now().Unix()
	ttl := int64(uploadSessionTTL.Seconds())

	// Empty snapshot → both gauges 0.
	updateUploadAgeMetrics(nil)
	if v := getGaugeValue(t, "kvfs_upload_sessions_total", nil); v != 0 {
		t.Errorf("empty: sessions_total = %v, want 0", v)
	}
	if v := getGaugeValue(t, "kvfs_oldest_upload_age_seconds", nil); v != 0 {
		t.Errorf("empty: oldest_age = %v, want 0", v)
	}

	// Smaller Expires = older session; Expires==0 is untracked and ignored.
	uploads := []*UploadSession{
		{ID: "fresh", Expires: now + ttl - 3600},   // ~1h old
		{ID: "oldest", Expires: now + ttl - 10800}, // ~3h old
		{ID: "untracked", Expires: 0},              // skipped
	}
	updateUploadAgeMetrics(uploads)
	if v := getGaugeValue(t, "kvfs_upload_sessions_total", nil); v != 3 {
		t.Errorf("sessions_total = %v, want 3", v)
	}
	age := getGaugeValue(t, "kvfs_oldest_upload_age_seconds", nil)
	if age < 10700 || age > 10900 { // ~3h ± a couple seconds of clock skew
		t.Errorf("oldest_age = %v, want ~10800 (the 3h session)", age)
	}

	// Future Expires → negative raw age, must clamp to 0.
	updateUploadAgeMetrics([]*UploadSession{{ID: "future", Expires: now + 2*ttl}})
	if v := getGaugeValue(t, "kvfs_oldest_upload_age_seconds", nil); v != 0 {
		t.Errorf("future-Expires: oldest_age = %v, want 0 (clamped)", v)
	}

	// TTL round-trip: a just-created session (Expires = now + TTL) reads back
	// as age ≈ 0, pinning the two uploadSessionTTL usages against drift.
	updateUploadAgeMetrics([]*UploadSession{{ID: "just-created", Expires: now + ttl}})
	if v := getGaugeValue(t, "kvfs_oldest_upload_age_seconds", nil); v < 0 || v > 2 {
		t.Errorf("just-created: oldest_age = %v, want ≈0", v)
	}

	// Reset so other tests see a clean gauge.
	updateUploadAgeMetrics(nil)
}

func getHistogramCount(t *testing.T, name string, labels map[string]string) uint64 {
	t.Helper()
	metrics := gatherKVFSMetrics(t)
	mf, ok := metrics[name]
	if !ok {
		t.Fatalf("metric %q not found", name)
	}
	m := findMetric(mf, labels)
	if m == nil {
		// Not yet observed — count is 0
		return 0
	}
	return m.GetHistogram().GetSampleCount()
}
