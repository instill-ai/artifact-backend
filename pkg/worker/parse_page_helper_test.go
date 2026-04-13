package worker

import (
	"testing"
)

func TestParseMarkdownPages_SheetMarkers(t *testing.T) {
	tests := []struct {
		name              string
		input             string
		wantPageCount     int
		wantTagsPreserved bool
	}{
		{
			name:              "single sheet marker",
			input:             "[Sheet: Revenue]\n\n| Q | Amount |\n| --- | --- |\n| Q1 | $1M |\n",
			wantPageCount:     1,
			wantTagsPreserved: true,
		},
		{
			name:              "multiple sheet markers",
			input:             "[Sheet: Revenue]\n\n| Q | Amount |\n| --- | --- |\n| Q1 | $1M |\n\n[Sheet: Expenses]\n\n| Cat | Amount |\n| --- | --- |\n| Payroll | $800K |\n",
			wantPageCount:     2,
			wantTagsPreserved: true,
		},
		{
			name:              "sheet name with spaces and special chars",
			input:             "[Sheet: Q1 2025 (Draft)]\n\nSome data\n",
			wantPageCount:     1,
			wantTagsPreserved: true,
		},
		{
			name:              "mixed page and sheet markers",
			input:             "[Page: 1]\n\nPage content\n\n[Sheet: Revenue]\n\nSheet content\n",
			wantPageCount:     2,
			wantTagsPreserved: true,
		},
		{
			name:              "page markers only (backward compatibility)",
			input:             "[Page: 1]\n\nFirst page\n\n[Page: 2]\n\nSecond page\n",
			wantPageCount:     2,
			wantTagsPreserved: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			markdownWithTags, pages, positionData := parseMarkdownPages(tt.input)

			if len(pages) != tt.wantPageCount {
				t.Errorf("parseMarkdownPages() page count = %d, want %d", len(pages), tt.wantPageCount)
			}

			if tt.wantTagsPreserved && markdownWithTags != tt.input {
				t.Errorf("parseMarkdownPages() should preserve tags in output")
			}

			if positionData == nil {
				t.Fatal("parseMarkdownPages() positionData should not be nil")
			}

			if len(positionData.PageDelimiters) != tt.wantPageCount {
				t.Errorf("parseMarkdownPages() delimiter count = %d, want %d",
					len(positionData.PageDelimiters), tt.wantPageCount)
			}
		})
	}
}

func TestDeduplicatePageTags_SheetMarkers(t *testing.T) {
	input := "[Sheet: Revenue]\n\nData A\n\n[Sheet: Revenue]\n\nData B\n"
	result := deduplicatePageTags(input)

	// The first occurrence of [Sheet: Revenue] should be removed, keeping only the last
	matches := pageTagPattern.FindAllStringIndex(result, -1)
	if len(matches) != 1 {
		t.Errorf("deduplicatePageTags() should keep 1 [Sheet: Revenue], got %d", len(matches))
	}
}
