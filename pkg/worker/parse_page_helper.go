package worker

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/instill-ai/artifact-backend/pkg/types"
)

// pageTagPattern matches the standard page delimiter format used by AI
// Format: [Page: 1], [Page: 2], etc.
// Multi-page documents from AI ALWAYS start with [Page: 1] as the first line and increment sequentially.
var pageTagPattern = regexp.MustCompile(`(?m)^\[Page:\s*(\d+)\]\s*$`)

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
	if len(pageMatches) == 0 {
		return markdown, []string{markdown}, nil
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

// ExtractPageContent extracts the content of a specific page using the [Page: X] tags.
// Page numbers are 1-indexed (page 1, page 2, etc.)
//
// The markdown must contain [Page: X] tags. This function uses regex to extract
// the content between tags, eliminating any position calculation bugs.
//
// Usage:
//
//	content, err := ExtractPageContent(markdownWithTags, 1) // Get page 1
//	content, err := ExtractPageContent(markdownWithTags, 2) // Get page 2
func ExtractPageContent(markdown string, pageNum int) (string, error) {
	if markdown == "" {
		return "", fmt.Errorf("markdown is empty")
	}

	if pageNum < 1 {
		return "", fmt.Errorf("invalid page number %d (must be >= 1)", pageNum)
	}

	// Pattern to match: [Page: X] followed by content until next [Page: Y] or end of string
	// (?s) enables dot to match newlines
	// (.*?) is non-greedy to stop at next tag
	pattern := fmt.Sprintf(`(?s)\[Page:\s*%d\]\s*\n(.*?)(?:\n\[Page:\s*\d+\]|\z)`, pageNum)
	re := regexp.MustCompile(pattern)

	matches := re.FindStringSubmatch(markdown)
	if len(matches) < 2 {
		// Check if markdown has NO page tags at all
		if !pageTagPattern.MatchString(markdown) {
			// Single-page document without tags
			if pageNum == 1 {
				return strings.TrimSpace(markdown), nil
			}
			return "", fmt.Errorf("page %d not found (single-page document)", pageNum)
		}
		return "", fmt.Errorf("page %d not found", pageNum)
	}

	return strings.TrimSpace(matches[1]), nil
}
