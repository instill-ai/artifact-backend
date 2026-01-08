package ai

import (
	"context"
	"time"

	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// Model family constants
const (
	// Model family identifiers for routing requests
	// Note: VertexAI and Gemini both use ModelFamilyGemini
	// VertexAI is the preferred implementation when configured (GCS-based)
	// Gemini API is the fallback (direct API-based)
	ModelFamilyGemini = "gemini"
	ModelFamilyOpenAI = "openai"

	// DefaultModelFamily is determined by precedence in NewCompositeClient
	// Precedence: Gemini (VertexAI preferred) > OpenAI
	DefaultModelFamily = "gemini"
)

// ConversionResult represents the result of understanding unstructured data content and extracting it to Markdown
type ConversionResult struct {
	Markdown     string
	PositionData *types.PositionData
	Length       []uint32
	Client       string // "gemini", "openai", "anthropic"
	// UsageMetadata contains token usage information from the AI client
	// The actual type depends on the client
	UsageMetadata any
}

// CacheResult represents the result of creating a cache for unstructured data content
type CacheResult struct {
	CacheName  string
	Model      string
	CreateTime time.Time
	ExpireTime time.Time
	// UsageMetadata contains token usage information from the AI client
	// The actual type depends on the client
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
	Content         []byte
	FileType        artifactpb.File_Type
	FileDisplayName string
}

// ChatResult represents the AI's chat response using cached context
type ChatResult struct {
	Answer        string // The AI-generated answer
	Model         string // Model used (e.g., "gemini-1.5-pro-002")
	UsageMetadata any    // Token usage metadata from the AI client
}

// EmbedResult represents the result of an embedding operation
type EmbedResult struct {
	Vectors        [][]float32 // The embedding vectors
	Model          string      // Model used (e.g., "gemini-embedding-001")
	Dimensionality int32       // Vector dimensionality (e.g., 3072)
}

// Client defines the interface for AI vendor API clients that understand unstructured data
// (documents, images, audio, video) and extract content to Markdown
// The client interface also includes routing capabilities to select the appropriate
// implementation based on model family (for composite/multi-client scenarios)
type Client interface {
	// Name returns the client name (e.g., "gemini", "openai", "composite")
	Name() string

	// ConvertToMarkdownWithoutCache understands unstructured data content and extracts it to Markdown
	// This does direct conversion WITHOUT using a cached context
	// The prompt parameter specifies the task-specific instruction (e.g., conversion prompt or summary prompt)
	ConvertToMarkdownWithoutCache(ctx context.Context, content []byte, fileType artifactpb.File_Type, filename string, prompt string) (*ConversionResult, error)

	// ConvertToMarkdownWithCache uses a pre-existing cached context for content understanding
	// This is more efficient when the same content is being processed multiple times
	ConvertToMarkdownWithCache(ctx context.Context, cacheName, prompt string) (*ConversionResult, error)

	// CreateCache creates a cached context for efficient content understanding of unstructured data
	// Supports both single file and batch operations (pass multiple files for batch caching)
	CreateCache(ctx context.Context, files []FileContent, ttl time.Duration) (*CacheResult, error)

	// ListCaches lists all cached contexts with pagination support
	ListCaches(ctx context.Context, options *CacheListOptions) (*CacheListResult, error)

	// GetCache retrieves details of a specific cached context
	GetCache(ctx context.Context, cacheName string) (*CacheResult, error)

	// UpdateCache updates the expiration of a cached context
	UpdateCache(ctx context.Context, cacheName string, options *CacheUpdateOptions) (*CacheResult, error)

	// DeleteCache deletes a cached context
	DeleteCache(ctx context.Context, cacheName string) error

	// EmbedTexts generates embeddings with a specific task type optimization
	// taskType specifies the optimization (e.g., "RETRIEVAL_DOCUMENT", "RETRIEVAL_QUERY", "QUESTION_ANSWERING")
	// dimensionality specifies the desired embedding vector size (e.g., 1536, 3072)
	//   - For OpenAI: always returns 1536 regardless of this parameter
	//   - For Gemini: can dynamically output 768, 1536, or 3072 based on this parameter
	EmbedTexts(ctx context.Context, texts []string, taskType string, dimensionality int32) (*EmbedResult, error)

	// GetEmbeddingDimensionality returns the embedding vector dimensionality for this client
	// For OpenAI: always returns 1536
	// For Gemini: always returns 3072 (full dimensionality)
	GetEmbeddingDimensionality() int32

	// CountTokens counts the total tokens for the given content without actually processing it
	// This is useful for determining whether to use caching or direct processing
	// Returns the total token count and any usage metadata from the AI client
	CountTokens(ctx context.Context, content []byte, fileType artifactpb.File_Type, filename string) (int, any, error)

	// GetModelFamily returns the appropriate client for a specific model family
	// This is used by composite clients to route requests to the correct implementation
	// For single-client implementations, this returns self
	// modelFamily examples: "gemini", "openai"
	GetModelFamily(modelFamily string) (Client, error)

	// Close releases client resources
	Close() error
}
