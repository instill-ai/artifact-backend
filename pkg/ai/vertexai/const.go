package vertexai

import (
	"time"
)

// Constants for VertexAI client
const (
	// Model family identifier (uses Gemini models via VertexAI)
	ModelFamily = "gemini"

	// DefaultModel is the default VertexAI model for multimodal content conversion
	DefaultModel = "gemini-2.5-flash"

	// DefaultCacheTTL is the default time-to-live for cached content (1 minute)
	// Matches CE's default
	DefaultCacheTTL = time.Minute

	// MinCacheTokens is the minimum token count for VertexAI cache creation
	// VertexAI requires 2048 tokens minimum (different from Gemini API's 1024)
	// Reference: https://cloud.google.com/vertex-ai/generative-ai/docs/context-cache/context-cache-overview
	MinCacheTokens = 2048

	// MaxInlineSize is the 20MB threshold for GCS upload
	// Files larger than this should be uploaded to GCS instead of inline
	MaxInlineSize = 20 * 1024 * 1024

	// FileUploadTimeout is the timeout for file upload to GCS
	FileUploadTimeout = 5 * time.Minute

	// GCSSignedURLExpiration is the default expiration for GCS signed URLs
	GCSSignedURLExpiration = 15 * time.Minute
)

// Embedding constants for VertexAI
const (
	// DefaultEmbeddingModel is the default embedding model for VertexAI
	DefaultEmbeddingModel = "gemini-embedding-001"

	// DefaultEmbeddingDimension is the default embedding dimension
	// VertexAI text-embedding-005 supports dimensions up to 768
	DefaultEmbeddingDimension = 3072
)

// GetModel returns the default VertexAI model
func GetModel() string {
	return DefaultModel
}

// GetCacheTTL returns the default cache TTL
func GetCacheTTL() time.Duration {
	return DefaultCacheTTL
}

// GetEmbeddingModel returns the default embedding model
func GetEmbeddingModel() string {
	return DefaultEmbeddingModel
}

// GetEmbeddingDimension returns the default embedding dimension
func GetEmbeddingDimension() int32 {
	return DefaultEmbeddingDimension
}
