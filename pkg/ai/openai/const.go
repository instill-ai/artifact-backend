package openai

// Constants for OpenAI AI client
const (
	// Model family identifier
	ModelFamily = "openai"

	// Embedding dimensions (constant, not configurable)
	// OpenAI text-embedding-3-small produces 1536-dimensional embeddings
	DefaultEmbeddingDimension = 1536

	// Default embedding model for OpenAI
	DefaultEmbeddingModel = "text-embedding-3-small"
)
