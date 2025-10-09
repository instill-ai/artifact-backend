package ai

import (
	"context"
	"time"

	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// ConversionResult represents the result of understanding unstructured data content and extracting it to Markdown
type ConversionResult struct {
	Markdown     string
	PositionData *types.PositionData
	Length       []uint32
	Provider     string // "gemini", "openai", "anthropic"
	// UsageMetadata contains token usage information from the AI provider
	// The actual type depends on the provider
	UsageMetadata any
}

// CacheResult represents the result of creating a cache for unstructured data content
type CacheResult struct {
	CacheName  string
	Model      string
	CreateTime time.Time
	ExpireTime time.Time
	// UsageMetadata contains token usage information from the AI provider
	// The actual type depends on the provider
	UsageMetadata any
}

// Provider defines the interface for AI providers that understand unstructured data
// (documents, images, audio, video) and extract content to Markdown
type Provider interface {
	// Name returns the provider name (e.g., "gemini", "openai")
	Name() string

	// ConvertToMarkdown understands unstructured data content and extracts it to Markdown
	// The prompt parameter specifies the task-specific instruction (e.g., conversion prompt or summary prompt)
	ConvertToMarkdown(ctx context.Context, content []byte, fileType artifactpb.FileType, filename string, prompt string) (*ConversionResult, error)

	// ConvertToMarkdownWithCache uses a pre-existing cached context for content understanding
	ConvertToMarkdownWithCache(ctx context.Context, cacheName, prompt string) (*ConversionResult, error)

	// CreateCache creates a cached context for efficient content understanding of unstructured data
	CreateCache(ctx context.Context, content []byte, fileType artifactpb.FileType, filename string, ttl time.Duration) (*CacheResult, error)

	// DeleteCache deletes a cached context
	DeleteCache(ctx context.Context, cacheName string) error

	// SupportsFileType returns true if this provider can understand and extract content from this file type
	SupportsFileType(fileType artifactpb.FileType) bool

	// Close releases provider resources
	Close() error
}
