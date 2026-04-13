package worker

import (
	"regexp"
	"strings"

	"github.com/instill-ai/artifact-backend/pkg/types"
)

// pageTagPattern matches section delimiter markers in content.
// Supported formats:
//   - [Page: 1], [Page: 2] — AI-generated page delimiters for documents
//   - [Sheet: Revenue], [Sheet: Expenses] — excelize-generated sheet delimiters for spreadsheets
//
// The function parseMarkdownPages uses FindAllStringIndex (byte positions only)
// and never reads the captured group values, so adding Sheet is fully backward-compatible.
var pageTagPattern = regexp.MustCompile(`(?m)^\[(Page|Sheet):\s*([^\]]+?)\]\s*$`)

// parseMarkdownPages parses AI-generated Markdown that contains page delimiter tags.
// Returns the markdown WITH page tags preserved, a list of pages (without tags),
// and position data for visual grounding.
//
// Important: Multi-page documents from AI ALWAYS start with [Page: 1] as the first line,
// followed by [Page: 2], [Page: 3], etc.
//
// The page tags are KEPT in the stored markdown to enable simple, bug-free page extraction.
// Position data is also calculated for visual grounding features (mapping chunks to pages).
//
// If no page tags are found, returns the entire markdown as a single page.
func parseMarkdownPages(markdown string) (markdownWithTags string, pages []string, positionData *types.PositionData) {
	if markdown == "" {
		return "", nil, nil
	}

	// Find all page delimiter tags
	pageMatches := pageTagPattern.FindAllStringIndex(markdown, -1)

	// If no page tags found, treat entire document as a single page
	// Create position data with one delimiter at the end for visual grounding consistency
	if len(pageMatches) == 0 {
		contentLength := uint32(len([]rune(markdown))) // Use rune count for Unicode safety
		singlePagePositionData := &types.PositionData{
			PageDelimiters: []uint32{contentLength}, // Single page, delimiter at end
		}
		return markdown, []string{markdown}, singlePagePositionData
	}

	// Split markdown by page tags and extract page content (without tags)
	// Also track which tag index each page corresponds to for accurate position calculation
	pages = make([]string, 0)
	pageTagIndices := make([]int, 0) // Track which tag each page came from
	lastEnd := 0

	for i, match := range pageMatches {
		matchStart := match[0]
		matchEnd := match[1]

		// Extract content before this tag (if not the first tag)
		if i > 0 || matchStart > 0 {
			pageContent := strings.TrimSpace(markdown[lastEnd:matchStart])
			if pageContent != "" {
				pages = append(pages, pageContent)
				pageTagIndices = append(pageTagIndices, i) // Page belongs to tag i
			}
		}

		// Move to end of current tag
		lastEnd = matchEnd
	}

	// Add content after the last tag
	if lastEnd < len(markdown) {
		pageContent := strings.TrimSpace(markdown[lastEnd:])
		if pageContent != "" {
			pages = append(pages, pageContent)
			pageTagIndices = append(pageTagIndices, len(pageMatches)) // Last page (after all tags)
		}
	}

	if len(pages) == 0 {
		return "", nil, nil
	}

	// Calculate position data for visual grounding
	// This maps page boundaries in the markdown WITH tags
	positionData = calculatePagePositions(markdown, pageMatches, pageTagIndices)

	// Return markdown WITH tags, pages (for validation), and position data
	return markdown, pages, positionData
}

// calculatePagePositions calculates page tag positions in markdown for visual grounding.
// For each page of actual content, stores the rune position where the NEXT page tag starts.
// This allows mapping any text position back to its page number.
//
// Example: If delimiter[0] = 386, then positions 0-385 are in page 1, and 386+ starts page 2.
func calculatePagePositions(markdown string, pageMatches [][]int, pageTagIndices []int) *types.PositionData {
	if len(pageTagIndices) == 0 || len(pageMatches) == 0 {
		return nil
	}

	pageDelimiters := make([]uint32, 0, len(pageTagIndices))

	// For each page of content, store where the NEXT page boundary is
	// pageTagIndices[i] tells us which tag comes AFTER page i's content
	for _, tagIndex := range pageTagIndices {
		var boundaryPos int

		if tagIndex < len(pageMatches) {
			// This page ends where the next tag starts
			boundaryPos = pageMatches[tagIndex][0]
		} else {
			// Last page: boundary is end of document
			boundaryPos = len(markdown)
		}

		// Convert byte position to rune position for Unicode safety
		runePos := len([]rune(markdown[:boundaryPos]))
		pageDelimiters = append(pageDelimiters, uint32(runePos))
	}

	return &types.PositionData{
		PageDelimiters: pageDelimiters,
	}
}

// deduplicatePageTags removes duplicate [Page: X] tags from assembled batch
// output. When batch conversion tells the AI to include table continuation rows
// beyond its assigned range, the AI may emit extra [Page: X] tags for pages
// that belong to the next batch. After assembly these appear as duplicates.
//
// For each page number that appears more than once, only the LAST occurrence is
// kept (the one from the batch actually assigned that page). Earlier occurrences
// have their tag line deleted; their content folds into the preceding page,
// which is correct because the table started on that page.
func deduplicatePageTags(markdown string) string {
	matches := pageTagPattern.FindAllStringSubmatchIndex(markdown, -1)
	if len(matches) == 0 {
		return markdown
	}

	// sectionKey builds a dedup key like "Page:1" or "Sheet:Revenue" from a submatch.
	sectionKey := func(m []int) string {
		return markdown[m[2]:m[3]] + ":" + markdown[m[4]:m[5]]
	}

	// Record the last occurrence index for each section key.
	lastIdx := make(map[string]int)
	for i, m := range matches {
		lastIdx[sectionKey(m)] = i
	}

	// Collect match indices that are duplicates (not the last occurrence).
	var toRemove []int
	seen := make(map[string]int)
	for i, m := range matches {
		key := sectionKey(m)
		seen[key]++
		if lastIdx[key] != i {
			toRemove = append(toRemove, i)
		}
	}

	if len(toRemove) == 0 {
		return markdown
	}

	// Build the output, skipping the tag lines of duplicate occurrences.
	var b strings.Builder
	b.Grow(len(markdown))
	lastEnd := 0
	for _, idx := range toRemove {
		m := matches[idx]
		tagStart := m[0]
		tagEnd := m[1]

		b.WriteString(markdown[lastEnd:tagStart])

		// Skip trailing newline after the tag line if present.
		if tagEnd < len(markdown) && markdown[tagEnd] == '\n' {
			tagEnd++
		}
		lastEnd = tagEnd
	}
	b.WriteString(markdown[lastEnd:])

	return b.String()
}

// ExtractPageContent extracts the content of a specific page using the [Page: X] tags.
// Page numbers are 1-indexed (page 1, page 2, etc.)
//
// The markdown must contain [Page: X] tags. This function uses regex to extract
// the content between tags, eliminating any position calculation bugs.
