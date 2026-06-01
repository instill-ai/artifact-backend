package gemini

import "testing"

// TestEmbeddingBillableChars pins the usage-faithfulness contract for the
// Gemini embedding path: when the provider reports a billable character count
// (Vertex API) it is used verbatim; when it does not (the direct Gemini API
// returns no Metadata, so the count arrives as 0) the activity must fall back
// to the UTF-8 character length of the input text actually embedded. A zero
// here is what previously left RAG-indexed files with no embedding usage to
// bill against, collapsing downstream cost accounting to a flat per-file
// fallback instead of faithful per-character billing.
func TestEmbeddingBillableChars(t *testing.T) {
	cases := []struct {
		name     string
		provider int32
		text     string
		want     int32
	}{
		{"provider count wins when present", 42, "hello", 42},
		{"falls back to input length when provider omits (direct Gemini)", 0, "hello", 5},
		{"empty text yields zero", 0, "", 0},
		{"multibyte counted as characters not bytes", 0, "café—日本語", 8},
		{"provider count preferred even if smaller than text", 3, "hello world", 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := embeddingBillableChars(c.provider, c.text); got != c.want {
				t.Errorf("embeddingBillableChars(%d, %q) = %d, want %d", c.provider, c.text, got, c.want)
			}
		})
	}
}
