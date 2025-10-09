package gemini

import (
	"time"

	"google.golang.org/genai"
)

// CacheInput defines the input for creating a cached context
type CacheInput struct {
	// Model to use for caching
	Model string
	// Content is the raw file content
	Content []byte
	// ContentType is the MIME type
	ContentType string
	// Filename for better identification
	Filename string
	// TTL for the cache (default: 1 hour)
	TTL *time.Duration
	// DisplayName for easier identification
	DisplayName *string
	// SystemInstruction for the cache
	SystemInstruction *string
}

// CacheOutput defines the output of cache operations
type CacheOutput struct {
	// CacheName is the identifier for the cached content
	CacheName string
	// Model used for caching
	Model string
	// CreateTime when the cache was created
	CreateTime time.Time
	// ExpireTime when the cache will expire
	ExpireTime time.Time
	// UsageMetadata contains token usage information for the cached content
	UsageMetadata *genai.CachedContentUsageMetadata
}

// ConversionInput defines the input for content-to-markdown conversion (supports documents, images, audio, video)
// Note: This is for direct conversion only. For cached conversion, use ConvertToMarkdownWithCache() separately.
type ConversionInput struct {
	// Content is the file content (raw bytes for any supported type)
	Content []byte
	// ContentType is the MIME type of the file (e.g., image/jpeg, video/mp4, application/pdf)
	ContentType string
	// Filename is the original filename (helps with format detection)
	Filename string
	// Model specifies the Gemini model to use (default: gemini-2.5-flash)
	Model string
	// CustomPrompt allows overriding the default conversion prompt
	CustomPrompt *string
}

// ConversionOutput defines the output of content-to-markdown conversion
type ConversionOutput struct {
	// Markdown is the converted/extracted markdown content
	Markdown string
	// UsageMetadata tracks the number of tokens consumed
	UsageMetadata *genai.GenerateContentResponseUsageMetadata
	// CacheName is the name of the cached content (if caching was enabled)
	CacheName *string
	// Model is the model that was used for conversion
	Model string
}

// CacheInfo contains information about a cached content conversion
type CacheInfo struct {
	CacheName  string
	Model      string
	CreateTime time.Time
	ExpireTime time.Time
	// ContentType of the cached content (document, image, audio, or video)
	ContentType string
}
