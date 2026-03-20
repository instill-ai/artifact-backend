package worker

import (
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// contentSegment is one logical section of structured content markdown.
// For page-segmented documents each segment has a [Page: N] header.
// For unsegmented documents (audio utterances without grouping, free-form text)
// the entire content is a single segment.
type contentSegment struct {
	pageNum int    // > 0 if a [Page: N] header was parsed
	body    string // full text including the header line, no trailing newline
}

// smallDocSegmentThreshold: documents with fewer segments than this are merged
// whole (windowing overhead is not worth it below this count).
// At 5 pages a full-document merge is ~5 000–10 000 tokens — negligible.
const smallDocSegmentThreshold = 5

// windowBuffer: number of extra segments included before and after the
// identified target to give the LLM sufficient context.
// ±2 pages = 5 pages total; enough for patches that span a page boundary.
const windowBuffer = 2

var (
	// pageHeaderRe matches [Page: N] at the start of a line.
	pageHeaderRe = regexp.MustCompile(`(?m)^\[Page:\s*(\d+)\]`)

	// pageRefRe extracts explicit page references from a patch string:
	// "page 79", "pages 77-81", "on page 3", etc.
	pageRefRe = regexp.MustCompile(`(?i)(?:on\s+)?pages?\s+(\d+)(?:\s*[-–]\s*(\d+))?`)
)

// patchStopWords are excluded from keyword scoring because they appear
// in almost every patch instruction and carry no document-location signal.
var patchStopWords = map[string]bool{
	// Common English
	"the": true, "and": true, "for": true, "that": true, "this": true,
	"with": true, "from": true, "have": true, "not": true, "but": true,
	"are": true, "was": true, "were": true, "will": true, "would": true,
	// Patch-specific meta-words
	"change": true, "replace": true, "correct": true, "fix": true,
	"should": true, "instead": true, "update": true, "wrong": true,
	"typo": true, "error": true, "says": true, "reads": true,
	"spell": true, "misspelled": true, "name": true,
}

// parseContentSegments splits structured content markdown into segments.
// Segments are delimited by [Page: N] header lines. Any text before the first
// [Page:] marker is captured as a preamble segment (pageNum == 0).
// If no page markers are found the entire content is a single segment.
func parseContentSegments(content string) []contentSegment {
	locs := pageHeaderRe.FindAllStringIndex(content, -1)
	if len(locs) == 0 {
		return []contentSegment{{body: strings.TrimRight(content, "\n")}}
	}

	segments := make([]contentSegment, 0, len(locs)+1)

	// Preamble before the first [Page:] marker.
	if locs[0][0] > 0 {
		pre := strings.TrimRight(content[:locs[0][0]], "\n")
		if pre != "" {
			segments = append(segments, contentSegment{body: pre})
		}
	}

	for i, loc := range locs {
		var end int
		if i+1 < len(locs) {
			end = locs[i+1][0]
		} else {
			end = len(content)
		}
		body := strings.TrimRight(content[loc[0]:end], "\n")

		m := pageHeaderRe.FindStringSubmatch(body)
		pageNum := 0
		if len(m) > 1 {
			pageNum, _ = strconv.Atoi(m[1])
		}
		segments = append(segments, contentSegment{pageNum: pageNum, body: body})
	}
	return segments
}

// locateTargetSegments returns the segment indices most likely targeted by the
// patch. It tries strategies in order of confidence:
//  1. Explicit page reference ("page 79", "pages 77-81")
//  2. Keyword scoring (terms from the patch matched against segment text)
//
// Returns nil if no target can be located — callers should fall back to full
// document merge in this case.
func locateTargetSegments(patch string, segments []contentSegment) []int {
	if indices := findByPageReference(patch, segments); len(indices) > 0 {
		return indices
	}
	return findByKeywords(patch, segments)
}

func findByPageReference(patch string, segments []contentSegment) []int {
	matches := pageRefRe.FindAllStringSubmatch(patch, -1)
	if len(matches) == 0 {
		return nil
	}

	wanted := map[int]bool{}
	for _, m := range matches {
		start, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		wanted[start] = true
		if len(m) > 2 && m[2] != "" {
			end, err := strconv.Atoi(m[2])
			if err == nil {
				for p := start + 1; p <= end; p++ {
					wanted[p] = true
				}
			}
		}
	}

	var indices []int
	for i, seg := range segments {
		if wanted[seg.pageNum] {
			indices = append(indices, i)
		}
	}
	return indices
}

func findByKeywords(patch string, segments []contentSegment) []int {
	words := strings.FieldsFunc(patch, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})

	var keywords []string
	for _, w := range words {
		if len(w) > 3 && !patchStopWords[strings.ToLower(w)] {
			keywords = append(keywords, strings.ToLower(w))
		}
	}
	if len(keywords) == 0 {
		return nil
	}

	type scored struct {
		idx   int
		count int
	}
	all := make([]scored, len(segments))
	for i, seg := range segments {
		lower := strings.ToLower(seg.body)
		cnt := 0
		for _, kw := range keywords {
			cnt += strings.Count(lower, kw)
		}
		all[i] = scored{idx: i, count: cnt}
	}
	sort.Slice(all, func(a, b int) bool { return all[a].count > all[b].count })

	if all[0].count == 0 {
		return nil
	}
	top := all[0].count
	var indices []int
	for _, s := range all {
		if s.count < top {
			break
		}
		indices = append(indices, s.idx)
	}
	return indices
}

// buildEditWindow assembles the text window and its inclusive bounds [lo, hi]
// in the segment slice. The window covers all target segments plus buffer pages
// on each side for context.
func buildEditWindow(segments []contentSegment, targets []int, buffer int) (window string, lo, hi int) {
	if len(targets) == 0 {
		return joinSegments(segments), 0, len(segments) - 1
	}

	minIdx, maxIdx := targets[0], targets[0]
	for _, t := range targets {
		if t < minIdx {
			minIdx = t
		}
		if t > maxIdx {
			maxIdx = t
		}
	}

	lo = max(0, minIdx-buffer)
	hi = min(len(segments)-1, maxIdx+buffer)

	var sb strings.Builder
	for i := lo; i <= hi; i++ {
		if i > lo {
			sb.WriteByte('\n')
		}
		sb.WriteString(segments[i].body)
	}
	return sb.String(), lo, hi
}

// spliceContentWindow replaces the segments in [lo, hi] with mergedWindow and
// returns the reconstructed full document string.
func spliceContentWindow(segments []contentSegment, lo, hi int, mergedWindow string) string {
	var sb strings.Builder
	for i := 0; i < lo; i++ {
		sb.WriteString(segments[i].body)
		sb.WriteByte('\n')
	}
	sb.WriteString(mergedWindow)
	for i := hi + 1; i < len(segments); i++ {
		sb.WriteByte('\n')
		sb.WriteString(segments[i].body)
	}
	return sb.String()
}

func joinSegments(segments []contentSegment) string {
	parts := make([]string, len(segments))
	for i, s := range segments {
		parts[i] = s.body
	}
	return strings.Join(parts, "\n")
}
