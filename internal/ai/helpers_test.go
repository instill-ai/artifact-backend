package ai

import (
	"testing"

	qt "github.com/frankban/quicktest"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

func TestMinCacheTokens(t *testing.T) {
	c := qt.New(t)

	t.Run("MinCacheTokens constant value", func(t *testing.T) {
		c.Assert(MinCacheTokens, qt.Equals, 1024)
	})
}

func TestEstimateTotalTokens(t *testing.T) {
	c := qt.New(t)

	t.Run("empty files returns zero tokens", func(t *testing.T) {
		files := []FileContent{}
		tokens, err := EstimateTotalTokens(files)
		c.Assert(err, qt.IsNil)
		c.Assert(tokens, qt.Equals, 0)
	})

	t.Run("single small text file", func(t *testing.T) {
		files := []FileContent{
			{
				Content:  []byte("Hello world!"),
				FileType: artifactpb.FileType_FILE_TYPE_TEXT,
				Filename: "test.txt",
			},
		}
		tokens, err := EstimateTotalTokens(files)
		c.Assert(err, qt.IsNil)
		// "Hello world!" is approximately 3 tokens
		c.Assert(tokens > 0 && tokens < 10, qt.IsTrue)
	})

	t.Run("multiple files accumulates tokens", func(t *testing.T) {
		// Create test content that's just under 1024 tokens
		smallContent := make([]byte, 500) // ~125 tokens (4 chars per token roughly)
		for i := range smallContent {
			smallContent[i] = 'a'
		}

		files := []FileContent{
			{
				Content:  smallContent,
				FileType: artifactpb.FileType_FILE_TYPE_TEXT,
				Filename: "file1.txt",
			},
			{
				Content:  smallContent,
				FileType: artifactpb.FileType_FILE_TYPE_TEXT,
				Filename: "file2.txt",
			},
		}
		tokens, err := EstimateTotalTokens(files)
		c.Assert(err, qt.IsNil)
		c.Assert(tokens > 0, qt.IsTrue)
		// Two files with ~125 tokens each should be ~250 tokens total
		c.Assert(tokens < 500, qt.IsTrue) // Reasonable upper bound
	})

	t.Run("large file exceeds minimum cache threshold", func(t *testing.T) {
		// Create content large enough to exceed MinCacheTokens (1024)
		// Generate realistic text content (words with spaces) to get accurate token count
		// Roughly 5.4 characters per token based on testing, so 6000 chars should exceed 1024 tokens
		var largeContent []byte
		words := []string{"hello", "world", "this", "is", "a", "test", "content", "for", "token", "counting"}
		for len(largeContent) < 6000 {
			for _, word := range words {
				largeContent = append(largeContent, []byte(word+" ")...)
				if len(largeContent) >= 6000 {
					break
				}
			}
		}

		files := []FileContent{
			{
				Content:  largeContent,
				FileType: artifactpb.FileType_FILE_TYPE_TEXT,
				Filename: "large.txt",
			},
		}
		tokens, err := EstimateTotalTokens(files)
		c.Assert(err, qt.IsNil)
		c.Assert(tokens >= MinCacheTokens, qt.IsTrue, qt.Commentf("Expected at least %d tokens, got %d", MinCacheTokens, tokens))
	})
}
