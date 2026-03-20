package worker

import (
	"strings"
	"testing"
)

// makeDoc builds a multi-page content string with N pages.
// Page bodies are "content of page N".
func makeDoc(n int) string {
	var sb strings.Builder
	for i := 1; i <= n; i++ {
		if i > 1 {
			sb.WriteByte('\n')
		}
		sb.WriteString("[Page: ")
		sb.WriteString(itoa(i))
		sb.WriteString("]\ncontent of page ")
		sb.WriteString(itoa(i))
	}
	return sb.String()
}

func itoa(n int) string {
	// Simple helper to avoid importing strconv in test helpers.
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

// ---- parseContentSegments -----------------------------------------------

func TestParseContentSegments_NoMarkers(t *testing.T) {
	content := "some plain text\nno page markers here"
	segs := parseContentSegments(content)
	if len(segs) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(segs))
	}
	if segs[0].pageNum != 0 {
		t.Errorf("expected pageNum 0, got %d", segs[0].pageNum)
	}
	if segs[0].body != content {
		t.Errorf("body mismatch: got %q", segs[0].body)
	}
}

func TestParseContentSegments_PageMarkers(t *testing.T) {
	doc := makeDoc(3)
	segs := parseContentSegments(doc)
	if len(segs) != 3 {
		t.Fatalf("expected 3 segments, got %d: %v", len(segs), segPageNums(segs))
	}
	for i, want := range []int{1, 2, 3} {
		if segs[i].pageNum != want {
			t.Errorf("segment %d: expected pageNum %d, got %d", i, want, segs[i].pageNum)
		}
	}
}

func TestParseContentSegments_Preamble(t *testing.T) {
	content := "preamble text\n[Page: 1]\npage one\n[Page: 2]\npage two"
	segs := parseContentSegments(content)
	if len(segs) != 3 {
		t.Fatalf("expected 3 segments (preamble + 2 pages), got %d", len(segs))
	}
	if segs[0].pageNum != 0 {
		t.Errorf("expected preamble pageNum 0, got %d", segs[0].pageNum)
	}
	if !strings.Contains(segs[0].body, "preamble") {
		t.Errorf("preamble segment missing: %q", segs[0].body)
	}
}

func TestParseContentSegments_160Pages(t *testing.T) {
	doc := makeDoc(160)
	segs := parseContentSegments(doc)
	if len(segs) != 160 {
		t.Fatalf("expected 160 segments, got %d", len(segs))
	}
	if segs[78].pageNum != 79 {
		t.Errorf("expected segment 78 to be page 79, got %d", segs[78].pageNum)
	}
}

// ---- locateTargetSegments -----------------------------------------------

func TestLocateTargetSegments_ExplicitPage(t *testing.T) {
	segs := makeSegments(160)
	indices := locateTargetSegments("In page 79, 'Jonhn Smith' should be 'John Smith'.", segs)
	if len(indices) != 1 || indices[0] != 78 {
		t.Errorf("expected [78], got %v", indices)
	}
}

func TestLocateTargetSegments_ExplicitPageRange(t *testing.T) {
	segs := makeSegments(160)
	indices := locateTargetSegments("On pages 10-12 the speaker label is wrong.", segs)
	wantSet := map[int]bool{9: true, 10: true, 11: true}
	if len(indices) != 3 {
		t.Fatalf("expected 3 indices, got %v", indices)
	}
	for _, idx := range indices {
		if !wantSet[idx] {
			t.Errorf("unexpected index %d", idx)
		}
	}
}

func TestLocateTargetSegments_KeywordFallback(t *testing.T) {
	// Create a doc where page 5 mentions "Wentworth" and no other page does.
	segs := makeSegments(20)
	segs[4].body = "[Page: 5]\nDr Wentworth presented the results"

	indices := locateTargetSegments("Replace 'Wentwroth' with 'Wentworth' throughout.", segs)
	if len(indices) != 1 || indices[0] != 4 {
		t.Errorf("expected keyword match on segment 4 (page 5), got %v", indices)
	}
}

func TestLocateTargetSegments_NoTarget(t *testing.T) {
	segs := makeSegments(20)
	// Patch mentions terms that don't appear in any segment and no page ref.
	indices := locateTargetSegments("fix the typo", segs)
	// "fix" and "typo" are both in patchStopWords; no keywords to search.
	if len(indices) != 0 {
		t.Errorf("expected no targets, got %v", indices)
	}
}

// ---- buildEditWindow ----------------------------------------------------

func TestBuildEditWindow_WithBuffer(t *testing.T) {
	segs := makeSegments(160)
	window, lo, hi := buildEditWindow(segs, []int{78}, windowBuffer)

	// target=78 (page 79), buffer=2 → lo=76 hi=80 (pages 77-81)
	wantLo := 78 - windowBuffer
	wantHi := 78 + windowBuffer
	if lo != wantLo || hi != wantHi {
		t.Errorf("expected lo=%d hi=%d, got lo=%d hi=%d", wantLo, wantHi, lo, hi)
	}
	if !strings.Contains(window, "[Page: 79]") {
		t.Error("window should contain [Page: 79]")
	}
	// page at lo (page 77 = segment 76)
	wantFirstPage := itoa(lo + 1)
	if !strings.Contains(window, "[Page: "+wantFirstPage+"]") {
		t.Errorf("window should contain [Page: %s]", wantFirstPage)
	}
	// page at hi (page 81 = segment 80)
	wantLastPage := itoa(hi + 1)
	if !strings.Contains(window, "[Page: "+wantLastPage+"]") {
		t.Errorf("window should contain [Page: %s]", wantLastPage)
	}
	// page just before lo should not be in window
	excludedPage := itoa(lo) // segment lo-1 = page lo
	if lo > 0 && strings.Contains(window, "[Page: "+excludedPage+"]") {
		t.Errorf("window should NOT contain [Page: %s]", excludedPage)
	}
}

func TestBuildEditWindow_ClampAtStart(t *testing.T) {
	segs := makeSegments(160)
	_, lo, hi := buildEditWindow(segs, []int{1}, windowBuffer)
	if lo != 0 {
		t.Errorf("expected lo=0 (clamped), got %d", lo)
	}
	if hi != 1+windowBuffer {
		t.Errorf("expected hi=%d, got %d", 1+windowBuffer, hi)
	}
}

func TestBuildEditWindow_ClampAtEnd(t *testing.T) {
	segs := makeSegments(160)
	last := len(segs) - 1
	_, lo, hi := buildEditWindow(segs, []int{last}, windowBuffer)
	if hi != last {
		t.Errorf("expected hi=%d (clamped), got %d", last, hi)
	}
	if lo != last-windowBuffer {
		t.Errorf("expected lo=%d, got %d", last-windowBuffer, lo)
	}
}

// ---- spliceContentWindow ------------------------------------------------

func TestSpliceContentWindow_RoundTrip(t *testing.T) {
	segs := makeSegments(10)
	original := joinSegments(segs)

	// Simulate a no-op merge: merged window is identical to the original window.
	window, lo, hi := buildEditWindow(segs, []int{4}, windowBuffer)
	result := spliceContentWindow(segs, lo, hi, window)

	if result != original {
		t.Errorf("round-trip splice should reproduce original document\ngot:\n%s\nwant:\n%s", result, original)
	}
}

func TestSpliceContentWindow_PatchedPagePreserved(t *testing.T) {
	segs := makeSegments(160)

	// Simulate: patch fixes page 79. Merged window has page 79 edited.
	_, lo, hi := buildEditWindow(segs, []int{78}, windowBuffer)
	// Build a fake merged window where page 79 is patched.
	patchedWindow := buildWindowWithEdit(segs, lo, hi, 78, "John Smith presented the results")

	result := spliceContentWindow(segs, lo, hi, patchedWindow)

	if !strings.Contains(result, "John Smith presented the results") {
		t.Error("patched content should appear in result")
	}
	// Pages outside the window are untouched.
	if !strings.Contains(result, "content of page 1") {
		t.Error("page 1 content should be untouched")
	}
	if !strings.Contains(result, "content of page 160") {
		t.Error("page 160 content should be untouched")
	}
	// Pages inside the window but not edited are still present.
	if !strings.Contains(result, "content of page 76") {
		t.Error("page 76 (buffer page) should be present")
	}
}

func TestSpliceContentWindow_PageCountPreserved(t *testing.T) {
	segs := makeSegments(160)
	_, lo, hi := buildEditWindow(segs, []int{78}, windowBuffer)
	window, _, _ := buildEditWindow(segs, []int{78}, windowBuffer)
	result := spliceContentWindow(segs, lo, hi, window)

	// Count [Page: N] markers in result.
	count := strings.Count(result, "[Page:")
	if count != 160 {
		t.Errorf("expected 160 page markers after splice, got %d", count)
	}
}

// ---- helpers ------------------------------------------------------------

func makeSegments(n int) []contentSegment {
	return parseContentSegments(makeDoc(n))
}

func segPageNums(segs []contentSegment) []int {
	nums := make([]int, len(segs))
	for i, s := range segs {
		nums[i] = s.pageNum
	}
	return nums
}

// buildWindowWithEdit reconstructs the window text with segIdx's body replaced.
func buildWindowWithEdit(segs []contentSegment, lo, hi, segIdx int, replacement string) string {
	var sb strings.Builder
	for i := lo; i <= hi; i++ {
		if i > lo {
			sb.WriteByte('\n')
		}
		if i == segIdx {
			sb.WriteString("[Page: ")
			sb.WriteString(itoa(segs[i].pageNum))
			sb.WriteString("]\n")
			sb.WriteString(replacement)
		} else {
			sb.WriteString(segs[i].body)
		}
	}
	return sb.String()
}
