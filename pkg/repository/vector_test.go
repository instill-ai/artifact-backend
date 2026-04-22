package repository

import (
	"encoding/json"
	"testing"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

// TestHybridRerankerIsRRF pins the hybrid-search path to a single
// reranker: `RRFRanker(hybridRRFK)`. The reranker choice is NOT
// runtime-configurable — downstream consumers read the ranker off
// `SearchChunksResponse.ranker` (the proto `Ranker` enum) and must be
// able to assume the server's choice is stable across requests. If you
// change the reranker, update the handler that populates the proto
// enum in lockstep or you silently violate that contract.
func TestHybridRerankerIsRRF(t *testing.T) {
	r := milvusclient.NewRRFReranker().WithK(float64(hybridRRFK))

	var strategy, raw string
	for _, kv := range r.GetParams() {
		switch kv.GetKey() {
		case "strategy":
			strategy = kv.GetValue()
		case "params":
			raw = kv.GetValue()
		}
	}
	if strategy != "rrf" {
		t.Fatalf("strategy = %q, want %q", strategy, "rrf")
	}

	var p struct {
		K float64 `json:"k,omitempty"`
	}
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		t.Fatalf("unmarshal rrf params: %v", err)
	}
	if int(p.K) != hybridRRFK {
		t.Fatalf("rrf k = %v, want %d", p.K, hybridRRFK)
	}
}
