package gemini

import (
	"testing"
	"time"

	"google.golang.org/genai"

	qt "github.com/frankban/quicktest"
)

// Note: TestConversionInput removed as conversionInput struct was eliminated.
// Direct parameter passing is now used for better symmetry between cached and non-cached conversion.

func TestConversionOutput(t *testing.T) {
	c := qt.New(t)

	t.Run("valid conversion output", func(t *testing.T) {
		cacheName := "cache-123"
		usageMetadata := &genai.GenerateContentResponseUsageMetadata{
			PromptTokenCount:        100,
			CandidatesTokenCount:    200,
			TotalTokenCount:         300,
			CachedContentTokenCount: 50,
		}

		output := &conversionOutput{
			Markdown:      "# Converted Content",
			UsageMetadata: usageMetadata,
			CacheName:     &cacheName,
			Model:         "gemini-2.5-flash",
		}

		c.Assert(output.Markdown, qt.Equals, "# Converted Content")
		c.Assert(output.UsageMetadata, qt.Not(qt.IsNil))
		c.Assert(output.UsageMetadata.PromptTokenCount, qt.Equals, int32(100))
		c.Assert(output.UsageMetadata.CandidatesTokenCount, qt.Equals, int32(200))
		c.Assert(output.UsageMetadata.TotalTokenCount, qt.Equals, int32(300))
		c.Assert(output.UsageMetadata.CachedContentTokenCount, qt.Equals, int32(50))
		c.Assert(*output.CacheName, qt.Equals, "cache-123")
		c.Assert(output.Model, qt.Equals, "gemini-2.5-flash")
	})

	t.Run("nil cache name for direct conversion", func(t *testing.T) {
		output := &conversionOutput{
			Markdown:      "# Content",
			UsageMetadata: nil,
			CacheName:     nil,
			Model:         "gemini-2.5-flash",
		}

		c.Assert(output.CacheName, qt.IsNil)
		c.Assert(output.UsageMetadata, qt.IsNil)
	})

	t.Run("zero values", func(t *testing.T) {
		var output conversionOutput
		c.Assert(output.Markdown, qt.Equals, "")
		c.Assert(output.UsageMetadata, qt.IsNil)
		c.Assert(output.CacheName, qt.IsNil)
		c.Assert(output.Model, qt.Equals, "")
	})
}

// Note: TestCacheInfo removed as CacheInfo struct was eliminated (unused in codebase).

func TestConstants(t *testing.T) {
	c := qt.New(t)

	t.Run("DefaultConversionModel", func(t *testing.T) {
		c.Assert(DefaultConversionModel, qt.Equals, "gemini-2.5-flash")
	})

	t.Run("DefaultCacheTTL", func(t *testing.T) {
		c.Assert(DefaultCacheTTL, qt.Equals, time.Hour)
	})

	t.Run("DefaultSystemInstruction not empty", func(t *testing.T) {
		c.Assert(DefaultRAGSystemInstruction, qt.Not(qt.Equals), "")
		c.Assert(len(DefaultRAGSystemInstruction) > 50, qt.IsTrue, qt.Commentf("System instruction should be descriptive"))
	})

	t.Run("DefaultPromptTemplate not empty", func(t *testing.T) {
		c.Assert(DefaultGenerateContentPrompt, qt.Not(qt.Equals), "")
		c.Assert(len(DefaultGenerateContentPrompt) > 100, qt.IsTrue, qt.Commentf("Prompt template should be comprehensive"))
	})

	t.Run("DefaultPromptTemplate contains key instructions", func(t *testing.T) {
		// Verify prompt contains multimodal guidance
		c.Assert(DefaultGenerateContentPrompt, qt.Contains, "document")
		c.Assert(DefaultGenerateContentPrompt, qt.Contains, "image")
		c.Assert(DefaultGenerateContentPrompt, qt.Contains, "audio")
		c.Assert(DefaultGenerateContentPrompt, qt.Contains, "video")
		c.Assert(DefaultGenerateContentPrompt, qt.Contains, "Markdown")
	})

	t.Run("DefaultSystemInstruction mentions multimodal", func(t *testing.T) {
		c.Assert(DefaultRAGSystemInstruction, qt.Contains, "multimodal")
	})
}

// Note: conversionInput struct tests removed as the struct was eliminated.
// Direct parameter passing is now used for better symmetry between cached and non-cached conversion.
