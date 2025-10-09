package gemini

import (
	"context"
	"testing"
	"time"

	"google.golang.org/genai"

	qt "github.com/frankban/quicktest"

	errorsx "github.com/instill-ai/x/errors"
)

func TestCacheInputValidation(t *testing.T) {
	c := qt.New(t)

	provider := &Provider{
		client: nil,
	}

	t.Run("nil input", func(t *testing.T) {
		_, err := provider.createCache(context.Background(), nil)
		c.Assert(err, qt.Not(qt.IsNil))
		msg := errorsx.Message(err)
		c.Assert(msg, qt.Contains, "Invalid request")
	})

	t.Run("empty content", func(t *testing.T) {
		input := &CacheInput{
			Content:     []byte{},
			ContentType: "application/pdf",
			Filename:    "test.pdf",
		}
		_, err := provider.createCache(context.Background(), input)
		c.Assert(err, qt.Not(qt.IsNil))
		msg := errorsx.Message(err)
		c.Assert(msg, qt.Contains, "file appears to be empty")
	})

	t.Run("valid input structure", func(t *testing.T) {
		ttl := 5 * time.Minute
		displayName := "test-cache"
		sysInstruction := "test instruction"

		input := &CacheInput{
			Model:             DefaultConversionModel,
			Content:           []byte("test content"),
			ContentType:       "application/pdf",
			Filename:          "test.pdf",
			TTL:               &ttl,
			DisplayName:       &displayName,
			SystemInstruction: &sysInstruction,
		}

		c.Assert(input.Model, qt.Equals, DefaultConversionModel)
		c.Assert(len(input.Content), qt.Equals, 12)
		c.Assert(input.ContentType, qt.Equals, "application/pdf")
		c.Assert(input.Filename, qt.Equals, "test.pdf")
		c.Assert(*input.TTL, qt.Equals, 5*time.Minute)
		c.Assert(*input.DisplayName, qt.Equals, "test-cache")
		c.Assert(*input.SystemInstruction, qt.Equals, "test instruction")
	})
}

func TestDeleteCacheValidation(t *testing.T) {
	c := qt.New(t)

	provider := &Provider{
		client: nil,
	}

	t.Run("empty cache name returns error", func(t *testing.T) {
		err := provider.deleteCache(context.Background(), "")
		c.Assert(err, qt.Not(qt.IsNil))
		msg := errorsx.Message(err)
		c.Assert(msg, qt.Contains, "Internal error during cache cleanup")
	})
}

func TestCacheConstants(t *testing.T) {
	c := qt.New(t)

	t.Run("MaxInlineSize is 20MB", func(t *testing.T) {
		expected := 20 * 1024 * 1024
		c.Assert(MaxInlineSize, qt.Equals, expected)
	})

	t.Run("FileUploadTimeout is 5 minutes", func(t *testing.T) {
		expected := 5 * time.Minute
		c.Assert(FileUploadTimeout, qt.Equals, expected)
	})
}

func TestCacheOutput(t *testing.T) {
	c := qt.New(t)

	t.Run("valid cache output structure", func(t *testing.T) {
		now := time.Now()
		expiry := now.Add(time.Hour)
		usageMetadata := &genai.CachedContentUsageMetadata{
			TotalTokenCount: 1024,
		}

		output := &CacheOutput{
			CacheName:     "test-cache-123",
			Model:         "gemini-2.5-flash",
			CreateTime:    now,
			ExpireTime:    expiry,
			UsageMetadata: usageMetadata,
		}

		c.Assert(output.CacheName, qt.Equals, "test-cache-123")
		c.Assert(output.Model, qt.Equals, "gemini-2.5-flash")
		c.Assert(output.CreateTime, qt.Equals, now)
		c.Assert(output.ExpireTime, qt.Equals, expiry)
		c.Assert(output.UsageMetadata, qt.Not(qt.IsNil))
		c.Assert(output.UsageMetadata.TotalTokenCount, qt.Equals, int32(1024))
	})

	t.Run("zero values", func(t *testing.T) {
		var output CacheOutput
		c.Assert(output.CacheName, qt.Equals, "")
		c.Assert(output.Model, qt.Equals, "")
		c.Assert(output.UsageMetadata, qt.IsNil)
	})
}

func TestCacheInput_DefaultValues(t *testing.T) {
	c := qt.New(t)

	t.Run("nil TTL should use default", func(t *testing.T) {
		input := &CacheInput{
			Content:     []byte("test"),
			ContentType: "text/plain",
			Filename:    "test.txt",
			// TTL is nil - should use DefaultCacheTTL
		}
		c.Assert(input.TTL, qt.IsNil)
	})

	t.Run("nil DisplayName is allowed", func(t *testing.T) {
		input := &CacheInput{
			Content:     []byte("test"),
			ContentType: "text/plain",
			Filename:    "test.txt",
			// DisplayName is nil
		}
		c.Assert(input.DisplayName, qt.IsNil)
	})

	t.Run("nil SystemInstruction is allowed", func(t *testing.T) {
		input := &CacheInput{
			Content:     []byte("test"),
			ContentType: "text/plain",
			Filename:    "test.txt",
			// SystemInstruction is nil
		}
		c.Assert(input.SystemInstruction, qt.IsNil)
	})
}

func TestCacheInputFileSizeScenarios(t *testing.T) {
	c := qt.New(t)

	t.Run("small file under 20MB", func(t *testing.T) {
		content := make([]byte, 1024*1024) // 1MB
		input := &CacheInput{
			Content:     content,
			ContentType: "application/pdf",
			Filename:    "small.pdf",
		}
		c.Assert(len(input.Content), qt.Equals, 1024*1024)
		c.Assert(len(input.Content) < MaxInlineSize, qt.IsTrue)
	})

	t.Run("large file over 20MB", func(t *testing.T) {
		content := make([]byte, 25*1024*1024) // 25MB
		input := &CacheInput{
			Content:     content,
			ContentType: "video/mp4",
			Filename:    "large.mp4",
		}
		c.Assert(len(input.Content), qt.Equals, 25*1024*1024)
		c.Assert(len(input.Content) > MaxInlineSize, qt.IsTrue)
	})

	t.Run("exactly 20MB", func(t *testing.T) {
		content := make([]byte, MaxInlineSize)
		input := &CacheInput{
			Content:     content,
			ContentType: "application/pdf",
			Filename:    "exact20mb.pdf",
		}
		c.Assert(len(input.Content), qt.Equals, MaxInlineSize)
	})
}

func TestVideoTypeDetection(t *testing.T) {
	c := qt.New(t)

	t.Run("video requires file API for caching", func(t *testing.T) {
		// Small video file still requires File API
		content := make([]byte, 100*1024) // 100KB - under 20MB
		useFileAPI := len(content) > MaxInlineSize || isVideoType("video/mp4")
		c.Assert(useFileAPI, qt.IsTrue, qt.Commentf("Videos should always use File API for caching"))
	})

	t.Run("large non-video requires file API", func(t *testing.T) {
		content := make([]byte, 25*1024*1024) // 25MB
		useFileAPI := len(content) > MaxInlineSize || isVideoType("application/pdf")
		c.Assert(useFileAPI, qt.IsTrue, qt.Commentf("Large files should use File API"))
	})

	t.Run("small non-video uses inline", func(t *testing.T) {
		content := make([]byte, 1*1024*1024) // 1MB
		useFileAPI := len(content) > MaxInlineSize || isVideoType("application/pdf")
		c.Assert(useFileAPI, qt.IsFalse, qt.Commentf("Small non-video files can use inline"))
	})
}
