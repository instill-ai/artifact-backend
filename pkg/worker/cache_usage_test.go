package worker

import "testing"

type cachedContentUsageForTest struct {
	TotalTokenCount int32 `json:"totalTokenCount,omitempty"`
}

func TestAggregateCacheUsageMetadata(t *testing.T) {
	got := aggregateCacheUsageMetadata(
		map[string]interface{}{"totalTokenCount": float64(1200)},
		&cachedContentUsageForTest{TotalTokenCount: 300},
	)

	m, ok := got.(map[string]interface{})
	if !ok {
		t.Fatalf("aggregateCacheUsageMetadata returned %T, want map[string]interface{}", got)
	}
	if total := toInt64(m["totalTokenCount"]); total != 1500 {
		t.Fatalf("totalTokenCount = %d, want 1500", total)
	}
}
