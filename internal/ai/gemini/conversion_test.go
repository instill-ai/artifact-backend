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

func TestCreateContentPart(t *testing.T) {
	c := qt.New(t)

	provider := &Provider{
		client: nil, // Not needed for this test
	}

	t.Run("valid input with content type", func(t *testing.T) {
		input := &ConversionInput{
			Content:     []byte("test content"),
			ContentType: "application/pdf",
			Filename:    "test.pdf",
		}

		part, err := provider.createContentPart(input)
		c.Assert(err, qt.IsNil)
		c.Assert(part, qt.Not(qt.IsNil))
		c.Assert(part.InlineData, qt.Not(qt.IsNil))
		c.Assert(part.InlineData.MIMEType, qt.Equals, "application/pdf")
		c.Assert(part.InlineData.Data, qt.DeepEquals, []byte("test content"))
	})

	t.Run("missing content type", func(t *testing.T) {
		input := &ConversionInput{
			Content:     []byte("test content"),
			ContentType: "",
			Filename:    "test.pdf",
		}

		part, err := provider.createContentPart(input)
		c.Assert(err, qt.Not(qt.IsNil))
		msg := errorsx.Message(err)
		c.Assert(msg, qt.Contains, "Unsupported file type")
		c.Assert(part, qt.IsNil)
	})

	t.Run("image content", func(t *testing.T) {
		input := &ConversionInput{
			Content:     []byte{0x89, 0x50, 0x4E, 0x47}, // PNG header
			ContentType: "image/png",
			Filename:    "image.png",
		}

		part, err := provider.createContentPart(input)
		c.Assert(err, qt.IsNil)
		c.Assert(part, qt.Not(qt.IsNil))
		c.Assert(part.InlineData.MIMEType, qt.Equals, "image/png")
	})

	t.Run("video content", func(t *testing.T) {
		input := &ConversionInput{
			Content:     []byte("video data"),
			ContentType: "video/mp4",
			Filename:    "video.mp4",
		}

		part, err := provider.createContentPart(input)
		c.Assert(err, qt.IsNil)
		c.Assert(part, qt.Not(qt.IsNil))
		c.Assert(part.InlineData.MIMEType, qt.Equals, "video/mp4")
	})

	t.Run("audio content", func(t *testing.T) {
		input := &ConversionInput{
			Content:     []byte("audio data"),
			ContentType: "audio/mpeg",
			Filename:    "audio.mp3",
		}

		part, err := provider.createContentPart(input)
		c.Assert(err, qt.IsNil)
		c.Assert(part, qt.Not(qt.IsNil))
		c.Assert(part.InlineData.MIMEType, qt.Equals, "audio/mpeg")
	})
}

func TestExtractMarkdownFromResponse_ErrorCases(t *testing.T) {
	c := qt.New(t)

	provider := &Provider{
		client: nil, // Not needed for this test
	}

	t.Run("nil response", func(t *testing.T) {
		markdown, err := provider.extractMarkdownFromResponse(nil)
		c.Assert(err, qt.Not(qt.IsNil))
		msg := errorsx.Message(err)
		c.Assert(msg, qt.Contains, "AI service returned an invalid response")
		c.Assert(markdown, qt.Equals, "")
	})
}

func TestConversionInputValidation(t *testing.T) {
	c := qt.New(t)

	provider := &Provider{
		client: nil,
	}

	t.Run("nil input", func(t *testing.T) {
		_, err := provider.convertToMarkdownWithoutCache(context.Background(), nil)
		c.Assert(err, qt.Not(qt.IsNil))
		msg := errorsx.Message(err)
		c.Assert(msg, qt.Contains, "Invalid request")
	})

	t.Run("empty content", func(t *testing.T) {
		input := &ConversionInput{
			Content:     []byte{},
			ContentType: "application/pdf",
			Filename:    "test.pdf",
		}
		_, err := provider.convertToMarkdownWithoutCache(context.Background(), input)
		c.Assert(err, qt.Not(qt.IsNil))
		msg := errorsx.Message(err)
		c.Assert(msg, qt.Contains, "file appears to be empty")
	})
}

func TestConvertToMarkdownWithCache_ValidationErrors(t *testing.T) {
	c := qt.New(t)

	provider := &Provider{
		client: nil,
	}

	t.Run("empty cache name", func(t *testing.T) {
		_, err := provider.convertToMarkdownWithCache(context.Background(), "", "some prompt")
		c.Assert(err, qt.Not(qt.IsNil))
		msg := errorsx.Message(err)
		c.Assert(msg, qt.Contains, "Internal processing error")
	})
}
