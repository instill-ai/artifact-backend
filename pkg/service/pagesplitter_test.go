package service

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/repository"
)

func TestPageSplitter_Split(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name           string
		pageDelimiters []uint32
		content        string
		expected       []Chunk
		wantErr        string
	}{
		{
			name:           "single page",
			pageDelimiters: []uint32{5},
			content:        "Hello",
			expected: []Chunk{
				{
					Start:  0,
					End:    5,
					Text:   "Hello",
					Tokens: 1, // "Hello" = 1 token
					Reference: &repository.ChunkReference{
						PageRange: [2]uint32{1, 1},
					},
				},
			},
		},
		{
			name:           "multiple pages",
			pageDelimiters: []uint32{5, 10},
			content:        "HelloWorld",
			expected: []Chunk{
				{
					Start:  0,
					End:    5,
					Text:   "Hello",
					Tokens: 1, // "Hello" = 1 token
					Reference: &repository.ChunkReference{
						PageRange: [2]uint32{1, 1},
					},
				},
				{
					Start:  5,
					End:    10,
					Text:   "World",
					Tokens: 1, // "World" = 1 token
					Reference: &repository.ChunkReference{
						PageRange: [2]uint32{2, 2},
					},
				},
			},
		},
		{
			name:           "empty content",
			pageDelimiters: []uint32{0},
			content:        "",
			expected:       []Chunk{}, // empty chunks are skipped
		},
		{
			name:           "unicode content",
			pageDelimiters: []uint32{2, 4},
			content:        "你好世界",
			expected: []Chunk{
				{
					Start:  0,
					End:    2,
					Text:   "你好",
					Tokens: 2, // "你好" = 2 tokens
					Reference: &repository.ChunkReference{
						PageRange: [2]uint32{1, 1},
					},
				},
				{
					Start:  2,
					End:    4,
					Text:   "世界",
					Tokens: 3, // "世界" = 3 tokens
					Reference: &repository.ChunkReference{
						PageRange: [2]uint32{2, 2},
					},
				},
			},
		},
		{
			name:           "content with newlines",
			pageDelimiters: []uint32{6, 12},
			content:        "Line 1\nLine 2",
			expected: []Chunk{
				{
					Start:  0,
					End:    6,
					Text:   "Line 1",
					Tokens: 3, // "Line 1" = 3 tokens
					Reference: &repository.ChunkReference{
						PageRange: [2]uint32{1, 1},
					},
				},
				{
					Start:  6,
					End:    12,
					Text:   "\nLine ",
					Tokens: 3, // "\nLine " = 3 tokens
					Reference: &repository.ChunkReference{
						PageRange: [2]uint32{2, 2},
					},
				},
			},
		},
		{
			name:           "delimiter exceeds content size",
			pageDelimiters: []uint32{5, 20},
			content:        "Hello",
			expected:       nil,
			wantErr:        "page delimiter exceeds content size",
		},
		{
			name:           "no delimiters",
			pageDelimiters: []uint32{},
			content:        "Hello",
			expected:       []Chunk{},
		},
		{
			name:           "delimiter at content boundary",
			pageDelimiters: []uint32{5},
			content:        "Hello",
			expected: []Chunk{
				{
					Start:  0,
					End:    5,
					Text:   "Hello",
					Tokens: 1,
					Reference: &repository.ChunkReference{
						PageRange: [2]uint32{1, 1},
					},
				},
			},
		},
		{
			name:           "multiple delimiters with same position",
			pageDelimiters: []uint32{5, 5, 10},
			content:        "HelloWorld",
			expected: []Chunk{
				{
					Start:  0,
					End:    5,
					Text:   "Hello",
					Tokens: 1,
					Reference: &repository.ChunkReference{
						PageRange: [2]uint32{1, 1},
					},
				},
				{
					Start:  5,
					End:    10,
					Text:   "World",
					Tokens: 1,
					Reference: &repository.ChunkReference{
						PageRange: [2]uint32{3, 3},
					},
				},
			},
		},
		{
			name:           "all empty chunks",
			pageDelimiters: []uint32{0, 0, 0},
			content:        "",
			expected:       []Chunk{},
		},
		{
			name:           "mixed empty and non-empty chunks",
			pageDelimiters: []uint32{0, 5, 5},
			content:        "Hello",
			expected: []Chunk{
				{
					Start:  0,
					End:    5,
					Text:   "Hello",
					Tokens: 1,
					Reference: &repository.ChunkReference{
						PageRange: [2]uint32{2, 2},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			splitter := &PageSplitter{
				PageDelimiters: tt.pageDelimiters,
			}

			result, err := splitter.Split(tt.content)
			if tt.wantErr != "" {
				c.Assert(err, qt.IsNotNil)
				c.Assert(err, qt.ErrorMatches, tt.wantErr)
				return
			}

			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, tt.expected)
		})
	}
}

func TestPageSplitter_Split_EdgeCases(t *testing.T) {
	c := qt.New(t)

	c.Run("special characters", func(c *qt.C) {
		content := "Hello\tWorld\nTest\r\nEnd"
		splitter := &PageSplitter{
			PageDelimiters: []uint32{5, 11, 16, 20},
		}

		result, err := splitter.Split(content)
		c.Assert(err, qt.IsNil)
		c.Assert(len(result), qt.Equals, 4)

		expectedTexts := []string{"Hello", "\tWorld", "\nTest", "\r\nEn"}
		for i, expected := range expectedTexts {
			c.Assert(result[i].Text, qt.Equals, expected)
		}
	})
}

func TestPageSplitter_Split_RealLife(t *testing.T) {
	c := qt.New(t)

	c.Run("EU Council document with page delimiters", func(c *qt.C) {
		// Read the input content from testdata
		inputPath := filepath.Join("testdata", "page-splitter-input.md")
		content, err := os.ReadFile(inputPath)
		c.Assert(err, qt.IsNil, qt.Commentf("Failed to read input file: %s", inputPath))

		splitter := &PageSplitter{
			PageDelimiters: []uint32{1232, 3853, 5974, 8058},
		}

		result, err := splitter.Split(string(content))
		c.Assert(err, qt.IsNil)
		c.Assert(len(result), qt.Equals, 4)

		// Expected values for verification
		expectedTokenCounts := []int{328, 484, 391, 365}
		expectedStartPositions := []int{0, 1232, 3853, 5974}
		expectedEndPositions := []int{1232, 3853, 5974, 8058}
		expectedOutputs := []string{
			"page-splitter-output-1.txt",
			"page-splitter-output-2.txt",
			"page-splitter-output-3.txt",
			"page-splitter-output-4.txt",
		}

		// Verify all chunk properties in a single loop
		for i, chunk := range result {
			expectedPage := uint32(i + 1)

			// Verify token count
			c.Assert(chunk.Tokens, qt.Equals, expectedTokenCounts[i], qt.Commentf("Chunk %d token count mismatch", i+1))

			// Verify page ranges are correct (1-indexed)
			c.Assert(chunk.Reference.PageRange[0], qt.Equals, expectedPage)
			c.Assert(chunk.Reference.PageRange[1], qt.Equals, expectedPage)

			// Verify that chunks are non-empty
			c.Assert(chunk.Text, qt.Not(qt.Equals), "", qt.Commentf("Chunk %d should not be empty", i+1))
			c.Assert(len(chunk.Text), qt.Not(qt.Equals), 0, qt.Commentf("Chunk %d should have content", i+1))

			// Verify start/end positions are correct
			c.Assert(chunk.Start, qt.Equals, expectedStartPositions[i], qt.Commentf("Chunk %d start position mismatch", i+1))
			c.Assert(chunk.End, qt.Equals, expectedEndPositions[i], qt.Commentf("Chunk %d end position mismatch", i+1))

			// Verify against expected output files
			outputPath := filepath.Join("testdata", expectedOutputs[i])
			expectedContent, err := os.ReadFile(outputPath)
			c.Assert(err, qt.IsNil, qt.Commentf("Failed to read expected output file: %s", outputPath))
			c.Assert(chunk.Text, qt.Equals, string(expectedContent), qt.Commentf("Chunk %d content doesn't match expected output", i+1))
		}
	})
}
