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

// CacheListResult represents a page of cached contents
type CacheListResult struct {
	Caches        []CacheResult
	NextPageToken string
}

// CacheListOptions represents options for listing caches
type CacheListOptions struct {
	PageSize  int32
	PageToken string
}

// CacheUpdateOptions represents options for updating a cache
type CacheUpdateOptions struct {
	TTL        *time.Duration
	ExpireTime *time.Time
}

// FileContent represents a single file's content for batch caching
type FileContent struct {
	FileUID  types.FileUIDType
	Content  []byte
	FileType artifactpb.FileType
	Filename string
}

// ChatResult represents the AI's chat response using cached context
type ChatResult struct {
	Answer        string // The AI-generated answer
	Model         string // Model used (e.g., "gemini-1.5-pro-002")
	UsageMetadata any    // Token usage metadata from the AI provider
}

// Provider defines the interface for AI providers that understand unstructured data
// (documents, images, audio, video) and extract content to Markdown
type Provider interface {
	// Name returns the provider name (e.g., "gemini", "openai")
	Name() string

	// ConvertToMarkdownWithoutCache understands unstructured data content and extracts it to Markdown
	// This does direct conversion WITHOUT using a cached context
	// The prompt parameter specifies the task-specific instruction (e.g., conversion prompt or summary prompt)
	ConvertToMarkdownWithoutCache(ctx context.Context, content []byte, fileType artifactpb.FileType, filename string, prompt string) (*ConversionResult, error)

	// ConvertToMarkdownWithCache uses a pre-existing cached context for content understanding
	// This is more efficient when the same content is being processed multiple times
	ConvertToMarkdownWithCache(ctx context.Context, cacheName, prompt string) (*ConversionResult, error)

	// CreateCache creates a cached context for efficient content understanding of unstructured data
	// Supports both single file and batch operations (pass multiple files for batch caching)
	// systemInstruction specifies the AI's behavior (e.g., RAG instruction for conversion, Chat instruction for chat)
	CreateCache(ctx context.Context, files []FileContent, ttl time.Duration, systemInstruction string) (*CacheResult, error)

	// ListCaches lists all cached contexts with pagination support
	ListCaches(ctx context.Context, options *CacheListOptions) (*CacheListResult, error)

	// GetCache retrieves details of a specific cached context
	GetCache(ctx context.Context, cacheName string) (*CacheResult, error)

	// UpdateCache updates the expiration of a cached context
	UpdateCache(ctx context.Context, cacheName string, options *CacheUpdateOptions) (*CacheResult, error)

	// DeleteCache deletes a cached context
	DeleteCache(ctx context.Context, cacheName string) error

	// ChatWithCache responds to a prompt using a pre-cached context
	// This enables instant chat without needing embeddings/retrieval for files still being processed
	// The cached context contains the full file content(s) for the AI to reference
	ChatWithCache(ctx context.Context, cacheName, prompt string) (*ChatResult, error)

	// ChatWithFiles sends files + prompt directly to AI without caching
	// Used for small files that couldn't be cached by Gemini (< 1024 tokens minimum)
	// Enables chat during processing phase without needing embeddings or cache
	ChatWithFiles(ctx context.Context, files []FileContent, prompt string) (*ChatResult, error)

	// SupportsFileType returns true if this provider can understand and extract content from this file type
	SupportsFileType(fileType artifactpb.FileType) bool

	// Close releases provider resources
	Close() error
}
