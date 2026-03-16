package worker

import (
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestParseMarkdownPages(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name                string
		input               string
		wantCleaned         string
		wantPageCount       int
		wantHasPositionData bool
	}{
		{
			name: "markdown with [Page: 1] at start (typical AI output)",
			input: `[Page: 1]

# Introduction

This is the first page.

[Page: 2]

This is the second page.

[Page: 3]

This is the third page.`,
			wantCleaned: `# Introduction

This is the first page.

This is the second page.

This is the third page.`,
			wantPageCount:       3,
			wantHasPositionData: true,
		},
		{
			name: "markdown starting with [Page: 1]",
			input: `[Page: 1]

First page content here.

[Page: 2]

Second page content here.`,
			wantCleaned: `First page content here.

Second page content here.`,
			wantPageCount:       2,
			wantHasPositionData: true,
		},
		{
			name: "markdown without page tags (single page)",
			input: `# Document

This is a single page document with no page markers.

It should be treated as one page.`,
			wantCleaned: `# Document

This is a single page document with no page markers.

It should be treated as one page.`,
			wantPageCount:       1,
			wantHasPositionData: true, // Now creates position data for single-page documents
		},
		{
			name:                "empty markdown",
			input:               "",
			wantCleaned:         "",
			wantPageCount:       0,
			wantHasPositionData: false,
		},
		{
			name: "markdown with only whitespace between tags",
			input: `[Page: 1]

Content

[Page: 2]`,
			wantCleaned:         `Content`,
			wantPageCount:       1,
			wantHasPositionData: true,
		},
		{
			name: "multi-page document starting with [Page: 1]",
			input: `[Page: 1]

# Chapter 1

This is the first page with some content.
It has multiple lines.

[Page: 2]

# Chapter 2

This is the second page.

[Page: 3]

# Chapter 3

This is the third page.`,
			wantCleaned: `# Chapter 1

This is the first page with some content.
It has multiple lines.

# Chapter 2

This is the second page.

# Chapter 3

This is the third page.`,
			wantPageCount:       3,
			wantHasPositionData: true,
		},
		{
			name: "realistic AI multi-page output",
			input: `[Page: 1]

# Document Title

Page 1 content here.
More content.

[Page: 2]

Page 2 content here.

[Page: 3]

Page 3 content here.`,
			wantCleaned: `# Document Title

Page 1 content here.
More content.

Page 2 content here.

Page 3 content here.`,
			wantPageCount:       3,
			wantHasPositionData: true,
		},
		{
			name: "sequential page numbers",
			input: `[Page: 1]

First

[Page: 2]

Second

[Page: 3]

Third

[Page: 4]

Fourth`,
			wantCleaned: `First

Second

Third

Fourth`,
			wantPageCount:       4,
			wantHasPositionData: true,
		},
		{
			name: "real AI output - presentation with location markers",
			input: `[Page: 1]
[Location: Top-Left]
[Logo: INSTILL AI]

# Instill Agent [Icon: Up-right arrow]

## Agentic Document Intelligence & Workflow

[Location: Bottom-Left]
Oct 2025

[Location: Right]
[Image: Mountains under a golden sky with a winding road]

[Page: 2]
[Location: Top-Left]
[Logo: INSTILL AI]

# Agentic Document Intelligence at Scale

Automate extraction, categorisation, and analysis and reduce upto 90% of manual processing effort

[Location: Left Column]
- [Icon: PDF] Loan_Agreement_ABC.pdf
- [Icon: XLS] Term Loan Facility.xls
- [Icon: PDF] FacilityDoc_GHI.pdf
- [Icon: PDF] FacilityDoc_GHI.pdf
- [Icon: PDF] FacilityDoc_GHI.pdf

[Location: Center Column]
1.
2. Term Loan Facility.xls
   - [Icon: Exclamation mark] Missing Covenants
   - Disqualified
3. Loan Agreement - ABC Ltd
   - [Icon: Checkmark] Compliant
   - Qualified
   - [Icon: User profile] Lisa
4. FacilityDoc_GHI
   - [Icon: Exclamation mark] EBITDA Breach
   - Qualified
5.

[Location: Top-Left of Workflow]
[Icon: User profile] John

[Location: Right Column]
Ready to take action? [Icon: Close button]

### Business outcomes
[Icon: Bell] Compliance, save hours, retain revenue

### Decisions
[Icon: Checkmark] Approval, next steps, deal qualification
- [Icon: User profile] Steve

### Research
[Icon: Search] Market, regulation, customers

### Draft contract
[Icon: Document] Reports, contracts, charts

### Ask me something
[Icon: Plus]

[Location: Bottom-Left]
Partners: OpenAI perplexity NVIDIA. Google Gartner. Innovate UK`,
			wantCleaned: `[Location: Top-Left]
[Logo: INSTILL AI]

# Instill Agent [Icon: Up-right arrow]

## Agentic Document Intelligence & Workflow

[Location: Bottom-Left]
Oct 2025

[Location: Right]
[Image: Mountains under a golden sky with a winding road]

[Location: Top-Left]
[Logo: INSTILL AI]

# Agentic Document Intelligence at Scale

Automate extraction, categorisation, and analysis and reduce upto 90% of manual processing effort

[Location: Left Column]
- [Icon: PDF] Loan_Agreement_ABC.pdf
- [Icon: XLS] Term Loan Facility.xls
- [Icon: PDF] FacilityDoc_GHI.pdf
- [Icon: PDF] FacilityDoc_GHI.pdf
- [Icon: PDF] FacilityDoc_GHI.pdf

[Location: Center Column]
1.
2. Term Loan Facility.xls
   - [Icon: Exclamation mark] Missing Covenants
   - Disqualified
3. Loan Agreement - ABC Ltd
   - [Icon: Checkmark] Compliant
   - Qualified
   - [Icon: User profile] Lisa
4. FacilityDoc_GHI
   - [Icon: Exclamation mark] EBITDA Breach
   - Qualified
5.

[Location: Top-Left of Workflow]
[Icon: User profile] John

[Location: Right Column]
Ready to take action? [Icon: Close button]

### Business outcomes
[Icon: Bell] Compliance, save hours, retain revenue

### Decisions
[Icon: Checkmark] Approval, next steps, deal qualification
- [Icon: User profile] Steve

### Research
[Icon: Search] Market, regulation, customers

### Draft contract
[Icon: Document] Reports, contracts, charts

### Ask me something
[Icon: Plus]

[Location: Bottom-Left]
Partners: OpenAI perplexity NVIDIA. Google Gartner. Innovate UK`,
			wantPageCount:       2,
			wantHasPositionData: true,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			markdownWithTags, pages, positionData := parseMarkdownPages(tt.input)

			// Check that markdown with tags contains the original content
			// For docs without page tags in input, output should equal cleaned input
			// For docs with page tags in input, output should preserve those tags
			if !strings.Contains(tt.input, "[Page:") {
				// Input has no page tags - output should match cleaned input
				c.Assert(markdownWithTags, qt.Equals, tt.wantCleaned)
			} else {
				// Input has page tags - output should preserve them
				c.Assert(strings.Contains(markdownWithTags, "[Page:"), qt.IsTrue)
			}

			// Check page count
			c.Assert(len(pages), qt.Equals, tt.wantPageCount)

			// Check position data existence
			if tt.wantHasPositionData {
				c.Assert(positionData, qt.IsNotNil)
				c.Assert(len(positionData.PageDelimiters), qt.Equals, tt.wantPageCount)
			} else {
				c.Assert(positionData, qt.IsNil)
			}
		})
	}
}

func TestParseMarkdownPages_HeavyUnicode(t *testing.T) {
	c := qt.New(t)

	// Test with heavy Unicode content including emoji, CJK characters, etc.
	input := `[Page: 1]
Hello 世界 🌍
This is page 1 with emoji 🚀 and Chinese 中文

[Page: 2]
Second page 第二页
More Unicode: ユニコード
Emoji everywhere: 😀🎉✨

[Page: 3]
Third page 第三页
Mixed content: Hello 世界 🌍
End of document`

	_, pages, positionData := parseMarkdownPages(input)

	c.Assert(len(pages), qt.Equals, 3)
	c.Assert(positionData, qt.IsNotNil)
	c.Assert(len(positionData.PageDelimiters), qt.Equals, 3)

	// NOTE: ExtractPageContent tests were removed as the function was removed during dead code cleanup.
}

func TestPageDelimiterMeaning(t *testing.T) {
	c := qt.New(t)

	// Test to demonstrate the new tag-based extraction approach
	// Page tags are KEPT in the stored markdown for simple extraction
	input := `[Page: 1]
Hello
[Page: 2]
World`

	markdownWithTags, pages, positionData := parseMarkdownPages(input)

	c.Assert(len(pages), qt.Equals, 2)
	c.Assert(pages[0], qt.Equals, "Hello")
	c.Assert(pages[1], qt.Equals, "World")

	// Markdown WITH tags is stored (tags preserved)
	c.Assert(strings.Contains(markdownWithTags, "[Page: 1]"), qt.IsTrue)
	c.Assert(strings.Contains(markdownWithTags, "[Page: 2]"), qt.IsTrue)

	// Position data is calculated for visual grounding
	c.Assert(positionData, qt.IsNotNil)
	c.Assert(len(positionData.PageDelimiters), qt.Equals, 2)

	// NOTE: ExtractPageContent tests were removed as the function was removed during dead code cleanup.
}

func TestRealAIOutputDelimiterAccuracy(t *testing.T) {
	c := qt.New(t)

	// Test the actual real-world AI example to verify delimiter accuracy
	input := `[Page: 1]
[Location: Top-Left]
[Logo: INSTILL AI]

# Instill Agent [Icon: Up-right arrow]

[Page: 2]
[Location: Top-Left]
[Logo: INSTILL AI]

# Agentic Document Intelligence at Scale`

	markdownWithTags, pages, positionData := parseMarkdownPages(input)

	c.Assert(len(pages), qt.Equals, 2)
	c.Assert(positionData, qt.IsNotNil)
	c.Assert(len(positionData.PageDelimiters), qt.Equals, 2)

	t.Logf("Markdown with tags length (runes): %d", len([]rune(markdownWithTags)))
	t.Logf("Page 1 length (runes): %d", len([]rune(pages[0])))
	t.Logf("Page 2 length (runes): %d", len([]rune(pages[1])))
	t.Logf("Position data delimiters: %v", positionData.PageDelimiters)

	// NOTE: ExtractPageContent tests were removed as the function was removed during dead code cleanup.
	// Basic assertions on parsed data still pass
	c.Assert(strings.HasPrefix(pages[0], "[Location: Top-Left]"), qt.IsTrue,
		qt.Commentf("Page 1 should start with '[Location: Top-Left]'"))
	c.Assert(strings.HasPrefix(pages[1], "[Location: Top-Left]"), qt.IsTrue,
		qt.Commentf("Page 2 should start with '[Location: Top-Left]'"))
}

// NOTE: TestExtractPageContent was removed as the ExtractPageContent function was removed during dead code cleanup.

func TestParseMarkdownPages_LargeMultiPageDocument(t *testing.T) {
	c := qt.New(t)

	// Test with a realistic large multi-page document (13 pages)
	// This verifies that delimiters remain accurate throughout the document
	// without accumulating offset errors, especially for later pages
	input := `[Page: 1]
Content for page 1 with some text.
Multiple lines here.

[Page: 2]
Content for page 2 with more text.
This page has different content.
It spans multiple lines too.

[Page: 3]
Page 3 has its own unique content.
Testing delimiter accuracy.

[Page: 4]
Page 4 content here.
Short page.

[Page: 5]
Page 5 with medium length content.
Testing position calculation.
Multiple paragraphs help verify accuracy.

[Page: 6]
Page 6 content with even more text.
We want to ensure delimiters are accurate.
Even for later pages in the document.
No accumulating offset errors should occur.

[Page: 7]
Page 7 continues the trend.
More content to parse.

[Page: 8]
Page 8 has its data.
Testing continues.

[Page: 9]
Page 9 verification.
Still accurate?

[Page: 10]
Page 10 double digits now.
Delimiters should still be precise.

[Page: 11]
Page 11 content here.
Almost done.

[Page: 12]
Page 12 penultimate page.
Final checks.

[Page: 13]
Page 13 last page content.
End of document.`

	markdownWithTags, pages, positionData := parseMarkdownPages(input)

	// Verify structure
	c.Assert(len(markdownWithTags), qt.Equals, len(input))
	c.Assert(len(pages), qt.Equals, 13)
	c.Assert(positionData, qt.IsNotNil)
	c.Assert(len(positionData.PageDelimiters), qt.Equals, 13)

	// NOTE: ExtractPageContent loop was removed as the function was removed during dead code cleanup.
	// Verify pages were parsed correctly
	for pageNum := 1; pageNum <= 13; pageNum++ {
		c.Assert(len(pages[pageNum-1]) > 0, qt.IsTrue,
			qt.Commentf("Page %d should have content", pageNum))
	}

	// Verify delimiters point to correct positions
	// Each delimiter should point to where the NEXT page tag starts
	runes := []rune(input)
	for i, delim := range positionData.PageDelimiters {
		if i < 12 { // Not the last page
			// Should point to start of next [Page: X] tag
			nextPageTag := fmt.Sprintf("[Page: %d]", i+2)
			if int(delim)+len([]rune(nextPageTag)) <= len(runes) {
				context := string(runes[delim : delim+uint32(len([]rune(nextPageTag)))])
				c.Assert(context, qt.Equals, nextPageTag,
					qt.Commentf("Delimiter[%d] should point to start of page %d tag", i, i+2))
			}
		}
	}

	t.Logf("✅ All 13 pages verified with accurate delimiters")
	t.Logf("Delimiters: %v", positionData.PageDelimiters)
}

func TestDeduplicatePageTags_NoDuplicates(t *testing.T) {
	c := qt.New(t)

	input := "[Page: 1]\nFirst page\n\n[Page: 2]\nSecond page\n\n[Page: 3]\nThird page"
	result := deduplicatePageTags(input)
	c.Assert(result, qt.Equals, input)
}

func TestDeduplicatePageTags_SingleDuplicate(t *testing.T) {
	c := qt.New(t)

	// Batch A (pages 1-2) overflows into page 3; Batch B (pages 3-4) starts at page 3.
	input := "[Page: 1]\nFirst\n\n[Page: 2]\nSecond with table\n<table><tr><td>row</td></tr>\n\n" +
		"[Page: 3]\n<tr><td>overflow row</td></tr></table>\n\n" +
		"[Page: 3]\nThird page full content\n\n[Page: 4]\nFourth page"

	result := deduplicatePageTags(input)

	// The first [Page: 3] (overflow) should be removed; its content folds into page 2 area.
	// The second [Page: 3] (correct batch) is kept.
	_, pages, _ := parseMarkdownPages(result)
	c.Assert(len(pages), qt.Equals, 4)

	// Page 2 should now include the overflow table rows
	c.Assert(strings.Contains(pages[1], "overflow row"), qt.IsTrue,
		qt.Commentf("Overflow content should fold into preceding page"))
	// Page 3 should be the correct batch's content
	c.Assert(strings.Contains(pages[2], "Third page full content"), qt.IsTrue)
}

func TestDeduplicatePageTags_MultipleDuplicates(t *testing.T) {
	c := qt.New(t)

	// Batch A (1-3) overflows into pages 4 and 5; Batch B (4-6) is normal.
	input := "[Page: 1]\nP1\n\n[Page: 2]\nP2\n\n[Page: 3]\nP3 table\n\n" +
		"[Page: 4]\noverflow-4\n\n[Page: 5]\noverflow-5\n\n" +
		"[Page: 4]\nReal P4\n\n[Page: 5]\nReal P5\n\n[Page: 6]\nP6"

	result := deduplicatePageTags(input)

	_, pages, _ := parseMarkdownPages(result)
	c.Assert(len(pages), qt.Equals, 6)

	// Page 3 should contain overflow content that was under the removed [Page: 4] and [Page: 5]
	c.Assert(strings.Contains(pages[2], "overflow-4"), qt.IsTrue)
	c.Assert(strings.Contains(pages[2], "overflow-5"), qt.IsTrue)
	// Real pages 4 and 5 are preserved
	c.Assert(strings.Contains(pages[3], "Real P4"), qt.IsTrue)
	c.Assert(strings.Contains(pages[4], "Real P5"), qt.IsTrue)
}

func TestDeduplicatePageTags_EmptyAndNoTags(t *testing.T) {
	c := qt.New(t)

	c.Assert(deduplicatePageTags(""), qt.Equals, "")
	c.Assert(deduplicatePageTags("no page tags here"), qt.Equals, "no page tags here")
}

func TestDeduplicatePageTags_RealisticBatchAssembly(t *testing.T) {
	c := qt.New(t)

	// Simulates mergeHTMLTables output for a 10-page doc with batches of 5.
	// Batch 1 (pages 1-5) has a table on page 5 that overflows to page 6.
	// Batch 2 (pages 6-10) starts normally with page 6.
	batch1 := "[Page: 1]\n# Introduction\n\n" +
		"[Page: 2]\nSome content\n\n" +
		"[Page: 3]\nMore content\n\n" +
		"[Page: 4]\nEven more\n\n" +
		"[Page: 5]\n<table>\n<tr><td>Header</td></tr>\n<tr><td>P5 row</td></tr>\n" +
		"[Page: 6]\n<tr><td>P6 overflow row</td></tr>\n</table>"

	batch2 := "[Page: 6]\n<table>\n<tr><td>Header</td></tr>\n<tr><td>P5 row</td></tr>\n<tr><td>P6 overflow row</td></tr>\n</table>\n\nRest of page 6\n\n" +
		"[Page: 7]\nPage 7\n\n" +
		"[Page: 8]\nPage 8\n\n" +
		"[Page: 9]\nPage 9\n\n" +
		"[Page: 10]\nPage 10"

	assembled := batch1 + "\n\n" + batch2
	result := deduplicatePageTags(assembled)

	_, pages, positionData := parseMarkdownPages(result)
	c.Assert(len(pages), qt.Equals, 10, qt.Commentf("Should produce exactly 10 pages"))
	c.Assert(positionData, qt.IsNotNil)
	c.Assert(len(positionData.PageDelimiters), qt.Equals, 10)

	// Page 5 should contain the overflow row (table continuation folds in)
	c.Assert(strings.Contains(pages[4], "P6 overflow row"), qt.IsTrue,
		qt.Commentf("Overflow content should be part of page 5"))

	// Page 6 should be the real batch 2 content
	c.Assert(strings.Contains(pages[5], "Rest of page 6"), qt.IsTrue,
		qt.Commentf("Page 6 should have the correct batch content"))
}
