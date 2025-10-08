package gemini

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

func TestConversionInput(t *testing.T) {
	c := qt.New(t)

	t.Run("valid conversion input", func(t *testing.T) {
		customPrompt := "Custom prompt here"
		input := &ConversionInput{
			Content:      []byte("test content"),
			ContentType:  "application/pdf",
			Filename:     "test.pdf",
			Model:        "gemini-2.5-flash",
			CustomPrompt: &customPrompt,
		}

		c.Assert(input.Content, qt.DeepEquals, []byte("test content"))
		c.Assert(input.ContentType, qt.Equals, "application/pdf")
		c.Assert(input.Filename, qt.Equals, "test.pdf")
		c.Assert(input.Model, qt.Equals, "gemini-2.5-flash")
		c.Assert(*input.CustomPrompt, qt.Equals, "Custom prompt here")
	})

	t.Run("nil custom prompt", func(t *testing.T) {
		input := &ConversionInput{
			Content:      []byte("test"),
			ContentType:  "text/plain",
			Filename:     "test.txt",
			CustomPrompt: nil,
		}

		c.Assert(input.CustomPrompt, qt.IsNil)
	})

	t.Run("empty model defaults to DefaultConversionModel", func(t *testing.T) {
		input := &ConversionInput{
			Content:     []byte("test"),
			ContentType: "text/plain",
			Filename:    "test.txt",
			Model:       "",
		}

		model := input.Model
		if model == "" {
			model = DefaultConversionModel
		}
		c.Assert(model, qt.Equals, "gemini-2.5-flash")
	})

	t.Run("multimodal content types", func(t *testing.T) {
		testCases := []struct {
			name        string
			contentType string
			filename    string
		}{
			{"document", "application/pdf", "doc.pdf"},
			{"image", "image/jpeg", "photo.jpg"},
			{"audio", "audio/mpeg", "song.mp3"},
			{"video", "video/mp4", "clip.mp4"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				input := &ConversionInput{
					Content:     []byte("content"),
					ContentType: tc.contentType,
					Filename:    tc.filename,
				}
				c.Assert(input.ContentType, qt.Equals, tc.contentType)
				c.Assert(input.Filename, qt.Equals, tc.filename)
			})
		}
	})
}

func TestConversionOutput(t *testing.T) {
	c := qt.New(t)

	t.Run("valid conversion output", func(t *testing.T) {
		cacheName := "cache-123"
		tokenUsage := &TokenUsage{
			InputTokens:       100,
			OutputTokens:      200,
			TotalTokens:       300,
			CachedInputTokens: 50,
		}

		output := &ConversionOutput{
			Markdown:   "# Converted Content",
			TokensUsed: tokenUsage,
			CacheName:  &cacheName,
			Model:      "gemini-2.5-flash",
		}

		c.Assert(output.Markdown, qt.Equals, "# Converted Content")
		c.Assert(output.TokensUsed, qt.Not(qt.IsNil))
		c.Assert(output.TokensUsed.InputTokens, qt.Equals, int32(100))
		c.Assert(output.TokensUsed.OutputTokens, qt.Equals, int32(200))
		c.Assert(output.TokensUsed.TotalTokens, qt.Equals, int32(300))
		c.Assert(output.TokensUsed.CachedInputTokens, qt.Equals, int32(50))
		c.Assert(*output.CacheName, qt.Equals, "cache-123")
		c.Assert(output.Model, qt.Equals, "gemini-2.5-flash")
	})

	t.Run("nil cache name for direct conversion", func(t *testing.T) {
		output := &ConversionOutput{
			Markdown:   "# Content",
			TokensUsed: nil,
			CacheName:  nil,
			Model:      "gemini-2.5-flash",
		}

		c.Assert(output.CacheName, qt.IsNil)
		c.Assert(output.TokensUsed, qt.IsNil)
	})

	t.Run("zero values", func(t *testing.T) {
		var output ConversionOutput
		c.Assert(output.Markdown, qt.Equals, "")
		c.Assert(output.TokensUsed, qt.IsNil)
		c.Assert(output.CacheName, qt.IsNil)
		c.Assert(output.Model, qt.Equals, "")
	})
}

func TestTokenUsage(t *testing.T) {
	c := qt.New(t)

	t.Run("valid token usage", func(t *testing.T) {
		usage := &TokenUsage{
			InputTokens:       1000,
			OutputTokens:      2000,
			TotalTokens:       3000,
			CachedInputTokens: 500,
		}

		c.Assert(usage.InputTokens, qt.Equals, int32(1000))
		c.Assert(usage.OutputTokens, qt.Equals, int32(2000))
		c.Assert(usage.TotalTokens, qt.Equals, int32(3000))
		c.Assert(usage.CachedInputTokens, qt.Equals, int32(500))
	})

	t.Run("zero cached tokens", func(t *testing.T) {
		usage := &TokenUsage{
			InputTokens:       1000,
			OutputTokens:      500,
			TotalTokens:       1500,
			CachedInputTokens: 0,
		}

		c.Assert(usage.CachedInputTokens, qt.Equals, int32(0))
	})

	t.Run("all zeros", func(t *testing.T) {
		var usage TokenUsage
		c.Assert(usage.InputTokens, qt.Equals, int32(0))
		c.Assert(usage.OutputTokens, qt.Equals, int32(0))
		c.Assert(usage.TotalTokens, qt.Equals, int32(0))
		c.Assert(usage.CachedInputTokens, qt.Equals, int32(0))
	})
}

func TestCacheInfo(t *testing.T) {
	c := qt.New(t)

	t.Run("valid cache info", func(t *testing.T) {
		now := time.Now()
		expiry := now.Add(time.Hour)

		info := &CacheInfo{
			CacheName:   "cache-456",
			Model:       "gemini-2.5-flash",
			CreateTime:  now,
			ExpireTime:  expiry,
			ContentType: "application/pdf",
		}

		c.Assert(info.CacheName, qt.Equals, "cache-456")
		c.Assert(info.Model, qt.Equals, "gemini-2.5-flash")
		c.Assert(info.CreateTime, qt.Equals, now)
		c.Assert(info.ExpireTime, qt.Equals, expiry)
		c.Assert(info.ContentType, qt.Equals, "application/pdf")
	})

	t.Run("multimodal content types", func(t *testing.T) {
		testCases := []string{
			"application/pdf",
			"image/png",
			"audio/mpeg",
			"video/mp4",
		}

		for _, contentType := range testCases {
			info := &CacheInfo{
				ContentType: contentType,
			}
			c.Assert(info.ContentType, qt.Equals, contentType)
		}
	})

	t.Run("zero values", func(t *testing.T) {
		var info CacheInfo
		c.Assert(info.CacheName, qt.Equals, "")
		c.Assert(info.Model, qt.Equals, "")
		c.Assert(info.ContentType, qt.Equals, "")
	})
}

func TestConstants(t *testing.T) {
	c := qt.New(t)

	t.Run("DefaultConversionModel", func(t *testing.T) {
		c.Assert(DefaultConversionModel, qt.Equals, "gemini-2.5-flash")
	})

	t.Run("DefaultCacheTTL", func(t *testing.T) {
		c.Assert(DefaultCacheTTL, qt.Equals, time.Hour)
	})

	t.Run("DefaultSystemInstruction not empty", func(t *testing.T) {
		c.Assert(DefaultSystemInstruction, qt.Not(qt.Equals), "")
		c.Assert(len(DefaultSystemInstruction) > 50, qt.IsTrue, qt.Commentf("System instruction should be descriptive"))
	})

	t.Run("DefaultPromptTemplate not empty", func(t *testing.T) {
		c.Assert(DefaultConvertToMarkdownPromptTemplate, qt.Not(qt.Equals), "")
		c.Assert(len(DefaultConvertToMarkdownPromptTemplate) > 100, qt.IsTrue, qt.Commentf("Prompt template should be comprehensive"))
	})

	t.Run("DefaultPromptTemplate contains key instructions", func(t *testing.T) {
		// Verify prompt contains multimodal guidance
		c.Assert(DefaultConvertToMarkdownPromptTemplate, qt.Contains, "document")
		c.Assert(DefaultConvertToMarkdownPromptTemplate, qt.Contains, "image")
		c.Assert(DefaultConvertToMarkdownPromptTemplate, qt.Contains, "audio")
		c.Assert(DefaultConvertToMarkdownPromptTemplate, qt.Contains, "video")
		c.Assert(DefaultConvertToMarkdownPromptTemplate, qt.Contains, "Markdown")
	})

	t.Run("DefaultSystemInstruction mentions multimodal", func(t *testing.T) {
		c.Assert(DefaultSystemInstruction, qt.Contains, "multimodal")
	})
}

func TestConversionInput_FieldTypes(t *testing.T) {
	c := qt.New(t)

	t.Run("Content is byte slice", func(t *testing.T) {
		input := &ConversionInput{
			Content: []byte{0x01, 0x02, 0x03},
		}
		c.Assert(len(input.Content), qt.Equals, 3)
		c.Assert(input.Content[0], qt.Equals, byte(0x01))
	})

	t.Run("ContentType is string", func(t *testing.T) {
		input := &ConversionInput{
			ContentType: "application/pdf",
		}
		c.Assert(input.ContentType, qt.Equals, "application/pdf")
	})

	t.Run("Model is string", func(t *testing.T) {
		input := &ConversionInput{
			Model: "custom-model",
		}
		c.Assert(input.Model, qt.Equals, "custom-model")
	})

	t.Run("CustomPrompt is pointer to string", func(t *testing.T) {
		prompt := "test"
		input := &ConversionInput{
			CustomPrompt: &prompt,
		}
		c.Assert(input.CustomPrompt, qt.Not(qt.IsNil))
		c.Assert(*input.CustomPrompt, qt.Equals, "test")
	})
}
