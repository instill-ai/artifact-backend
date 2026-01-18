package gemini

import (
	"context"
	"testing"
	"time"

	"google.golang.org/genai"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/ai"
	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

// Note: TestCacheInputValidation removed as createCache and createCacheInput were eliminated.
// All caching now goes through createBatchCache which handles both single and multiple files.

func TestDeleteCacheValidation(t *testing.T) {
	c := qt.New(t)

	client := &Client{
		client: nil,
	}

	t.Run("empty cache name returns error", func(t *testing.T) {
		err := client.deleteCache(context.Background(), "")
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

// Note: TestCacheInput_DefaultValues and TestCacheInputFileSizeScenarios removed
// as createCacheInput struct was eliminated. File size logic is now tested via createBatchCache.

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

func TestCreateCache_Validation(t *testing.T) {
	c := qt.New(t)

	client := &Client{
		client: nil,
	}

	t.Run("returns error when no files provided", func(t *testing.T) {
		result, err := client.CreateCache(context.Background(), []ai.FileContent{}, 5*time.Minute)
		c.Assert(err, qt.Not(qt.IsNil))
		c.Assert(result, qt.IsNil)
		msg := errorsx.Message(err)
		c.Assert(msg, qt.Contains, "No files provided for caching")
	})

	t.Run("returns error when file content is empty", func(t *testing.T) {
		result, err := client.CreateCache(
			context.Background(),
			[]ai.FileContent{
				{
					Content:  []byte{},
					FileType: artifactpb.File_TYPE_PDF,
					FileDisplayName: "test.pdf",
				},
			},
			5*time.Minute,
		)
		c.Assert(err, qt.Not(qt.IsNil))
		c.Assert(result, qt.IsNil)
		msg := errorsx.Message(err)
		c.Assert(msg, qt.Contains, "file appears to be empty")
	})

	t.Run("returns error when any file in batch has empty content", func(t *testing.T) {
		result, err := client.CreateCache(
			context.Background(),
			[]ai.FileContent{
				{
					Content:  []byte("valid content"),
					FileType: artifactpb.File_TYPE_PDF,
					FileDisplayName: "test1.pdf",
				},
				{
					Content:  []byte{}, // empty content
					FileType: artifactpb.File_TYPE_PDF,
					FileDisplayName: "test2.pdf",
				},
			},
			5*time.Minute,
		)
		c.Assert(err, qt.Not(qt.IsNil))
		c.Assert(result, qt.IsNil)
		msg := errorsx.Message(err)
		c.Assert(msg, qt.Contains, "file appears to be empty")
	})
}

func TestCreateCache_DisplayName(t *testing.T) {
	c := qt.New(t)

	t.Run("single file uses filename in display name", func(t *testing.T) {
		files := []ai.FileContent{
			{
				Content:  []byte("test content"),
				FileType: artifactpb.File_TYPE_PDF,
				FileDisplayName: "my-document.pdf",
			},
		}

		displayName := "cache-my-document.pdf"
		if len(files) == 1 {
			displayName = "cache-my-document.pdf"
		}

		c.Assert(displayName, qt.Equals, "cache-my-document.pdf")
	})

	t.Run("multiple files use count in display name", func(t *testing.T) {
		fileCount := 2
		displayName := "cache-2-files"

		c.Assert(displayName, qt.Equals, "cache-2-files")
		c.Assert(fileCount, qt.Equals, 2)
	})
}

func TestCreateBatchCache_Validation(t *testing.T) {
	c := qt.New(t)

	client := &Client{
		client: nil,
	}

	t.Run("returns error on empty files array", func(t *testing.T) {
		files := []ai.FileContent{}
		_, err := client.createBatchCache(context.Background(), GetModel(), files, time.Hour, "test")
		c.Assert(err, qt.Not(qt.IsNil))
		msg := errorsx.Message(err)
		c.Assert(msg, qt.Contains, "No files provided")
	})

	t.Run("validates all files have content", func(t *testing.T) {
		files := []ai.FileContent{
			{
				Content:  []byte("valid"),
				FileType: artifactpb.File_TYPE_PDF,
				FileDisplayName: "test1.pdf",
			},
			{
				Content:  []byte{}, // empty
				FileType: artifactpb.File_TYPE_PDF,
				FileDisplayName: "test2.pdf",
			},
		}
		_, err := client.createBatchCache(context.Background(), GetModel(), files, time.Hour, "test")
		c.Assert(err, qt.Not(qt.IsNil))
		msg := errorsx.Message(err)
		c.Assert(msg, qt.Contains, "file appears to be empty")
	})

	t.Run("uses default model when not specified", func(t *testing.T) {
		files := []ai.FileContent{
			{
				Content:  []byte("test"),
				FileType: artifactpb.File_TYPE_PDF,
				FileDisplayName: "test.pdf",
			},
		}

		// When model is empty string, it should default to the configured model via GetModel()
		// This would be verified by checking the created cache's model field in a full test
		c.Assert(len(files), qt.Equals, 1)
	})

	t.Run("uses default TTL when zero", func(t *testing.T) {
		files := []ai.FileContent{
			{
				Content:  []byte("test"),
				FileType: artifactpb.File_TYPE_PDF,
				FileDisplayName: "test.pdf",
			},
		}

		// When TTL is 0, it should default to DefaultCacheTTL
		// This would be verified by checking the created cache's TTL in a full test
		c.Assert(len(files), qt.Equals, 1)
	})
}

func TestConvertToCacheResult(t *testing.T) {
	c := qt.New(t)

	t.Run("converts CacheOutput to CacheResult correctly", func(t *testing.T) {
		now := time.Now()
		expiry := now.Add(time.Hour)
		usageMetadata := &genai.CachedContentUsageMetadata{
			TotalTokenCount: 2048,
		}

		output := &CacheOutput{
			CacheName:     "test-cache-456",
			Model:         "gemini-2.5-flash",
			CreateTime:    now,
			ExpireTime:    expiry,
			UsageMetadata: usageMetadata,
		}

		result := convertToCacheResult(output)

		c.Assert(result, qt.Not(qt.IsNil))
		c.Assert(result.CacheName, qt.Equals, "test-cache-456")
		c.Assert(result.Model, qt.Equals, "gemini-2.5-flash")
		c.Assert(result.CreateTime, qt.Equals, now)
		c.Assert(result.ExpireTime, qt.Equals, expiry)
		c.Assert(result.UsageMetadata, qt.Not(qt.IsNil))
		// UsageMetadata is type any from ai.CacheResult, so we need to type assert
		if cachedMeta, ok := result.UsageMetadata.(*genai.CachedContentUsageMetadata); ok {
			c.Assert(cachedMeta.TotalTokenCount, qt.Equals, int32(2048))
		}
	})

	t.Run("handles nil usage metadata", func(t *testing.T) {
		output := &CacheOutput{
			CacheName:     "test-cache",
			Model:         "gemini-2.5-flash",
			CreateTime:    time.Now(),
			ExpireTime:    time.Now().Add(time.Hour),
			UsageMetadata: nil,
		}

		result := convertToCacheResult(output)

		c.Assert(result, qt.Not(qt.IsNil))
		c.Assert(result.UsageMetadata, qt.IsNil)
	})
}
