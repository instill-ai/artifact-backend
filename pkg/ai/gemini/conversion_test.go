package gemini

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	errorsx "github.com/instill-ai/x/errors"
)

func TestCleanMarkdown(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "no markers",
			input:    "# Hello\n\nWorld",
			expected: "# Hello\n\nWorld",
		},
		{
			name:     "with markdown code block markers",
			input:    "```markdown\n# Hello\n\nWorld\n```",
			expected: "# Hello\n\nWorld",
		},
		{
			name:     "with generic code block markers",
			input:    "```\n# Hello\n\nWorld\n```",
			expected: "# Hello\n\nWorld",
		},
		{
			name:     "with leading whitespace",
			input:    "   # Hello\n\nWorld   ",
			expected: "# Hello\n\nWorld",
		},
		{
			name:     "with trailing whitespace",
			input:    "# Hello\n\nWorld\n\n\n",
			expected: "# Hello\n\nWorld",
		},
		{
			name:     "with both markers and whitespace",
			input:    "  ```markdown\n# Hello\n\nWorld\n```  ",
			expected: "# Hello\n\nWorld",
		},
		{
			name:     "only closing marker",
			input:    "# Hello\n\nWorld\n```",
			expected: "# Hello\n\nWorld",
		},
		{
			name:     "only opening markdown marker",
			input:    "```markdown\n# Hello\n\nWorld",
			expected: "# Hello\n\nWorld",
		},
		{
			name:     "only opening generic marker",
			input:    "```\n# Hello\n\nWorld",
			expected: "# Hello\n\nWorld",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "only whitespace",
			input:    "   \n\n   ",
			expected: "",
		},
		{
			name:     "nested markers should not be affected",
			input:    "```markdown\n# Code Example\n\n```python\nprint('hello')\n```\n```",
			expected: "# Code Example\n\n```python\nprint('hello')\n```",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cleanMarkdown(tt.input)
			c.Assert(result, qt.Equals, tt.expected)
		})
	}
}

func TestIsVideoType(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		mimeType string
		expected bool
	}{
		{
			name:     "video mp4",
			mimeType: "video/mp4",
			expected: true,
		},
		{
			name:     "video mpeg",
			mimeType: "video/mpeg",
			expected: true,
		},
		{
			name:     "video quicktime",
			mimeType: "video/quicktime",
			expected: true,
		},
		{
			name:     "video webm",
			mimeType: "video/webm",
			expected: true,
		},
		{
			name:     "audio mp3",
			mimeType: "audio/mp3",
			expected: false,
		},
		{
			name:     "image png",
			mimeType: "image/png",
			expected: false,
		},
		{
			name:     "application pdf",
			mimeType: "application/pdf",
			expected: false,
		},
		{
			name:     "text plain",
			mimeType: "text/plain",
			expected: false,
		},
		{
			name:     "empty string",
			mimeType: "",
			expected: false,
		},
		{
			name:     "video prefix only",
			mimeType: "video",
			expected: true, // "video" is exactly 5 chars and starts with "video"
		},
		{
			name:     "vide (typo)",
			mimeType: "vide/mp4",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isVideoType(tt.mimeType)
			c.Assert(result, qt.Equals, tt.expected)
		})
	}
}

func TestExtractMarkdownFromResponse_ErrorCases(t *testing.T) {
	c := qt.New(t)

	client := &Client{
		client: nil, // Not needed for this test
	}

	t.Run("nil response", func(t *testing.T) {
		markdown, err := client.extractMarkdownFromResponse(nil)
		c.Assert(err, qt.Not(qt.IsNil))
		msg := errorsx.Message(err)
		c.Assert(msg, qt.Contains, "AI service returned an invalid response")
		c.Assert(markdown, qt.Equals, "")
	})
}

func TestConversionInputValidation(t *testing.T) {
	c := qt.New(t)

	client := &Client{
		client: nil,
	}

	t.Run("empty content", func(t *testing.T) {
		content := []byte{}
		contentType := "application/pdf"
		prompt := "convert"

		_, err := client.convertToMarkdownWithoutCache(context.Background(), content, contentType, prompt)
		c.Assert(err, qt.Not(qt.IsNil))
		msg := errorsx.Message(err)
		c.Assert(msg, qt.Contains, "file appears to be empty")
	})

	t.Run("empty content type", func(t *testing.T) {
		content := []byte("test")
		contentType := ""
		prompt := "convert"

		_, err := client.convertToMarkdownWithoutCache(context.Background(), content, contentType, prompt)
		c.Assert(err, qt.Not(qt.IsNil))
		msg := errorsx.Message(err)
		c.Assert(msg, qt.Contains, "Unsupported file type")
	})
}

func TestConvertToMarkdownWithCache_ValidationErrors(t *testing.T) {
	c := qt.New(t)

	client := &Client{
		client: nil,
	}

	t.Run("empty cache name", func(t *testing.T) {
		_, err := client.convertToMarkdownWithCache(context.Background(), "", "some prompt")
		c.Assert(err, qt.Not(qt.IsNil))
		msg := errorsx.Message(err)
		c.Assert(msg, qt.Contains, "Internal processing error")
	})
}
