package gemini

import (
	"embed"
	"log"
	"sync"
	"time"
)

//go:embed prompt/*.md
var promptFS embed.FS

var (
	promptCache     map[string]string
	promptCacheMu   sync.RWMutex
	promptCacheOnce sync.Once
)

// Constants for Gemini AI client
const (
	// Model family identifier
	ModelFamily = "gemini"

	// DefaultModel is the default Gemini model for multimodal content conversion
	DefaultModel = "gemini-2.5-flash"

	// DefaultCacheTTL is the default time-to-live for cached content (1 minute)
	// Both Gemini cache and Redis cache metadata use this TTL
	// Every call to VIEW_CACHE renews the TTL for both caches
	DefaultCacheTTL = time.Minute

	// MinCacheTokens is the minimum token count for Gemini cache creation
	// Gemini API requires 1024 tokens minimum
	MinCacheTokens = 1024

	// Embedding dimensions (configurable via Matryoshka Representation Learning)
	// - 768: Recommended by Google for optimal balance of storage efficiency and quality
	// - 1536: Compatible with OpenAI for migration scenarios
	// - 3072: Maximum quality (full-size embeddings)
	DefaultEmbeddingDimension = 3072

	// Default embedding model for Gemini
	DefaultEmbeddingModel = "gemini-embedding-001"

	// Task types for embeddings
	// TaskTypeRetrievalDocument is used for embedding document chunks that will be stored in the vector database
	// This optimizes embeddings for being retrieved by search queries
	TaskTypeRetrievalDocument = "RETRIEVAL_DOCUMENT"

	// TaskTypeRetrievalQuery is used for embedding search queries to find similar document chunks
	// This is used in similarity search operations to find relevant chunks
	TaskTypeRetrievalQuery = "RETRIEVAL_QUERY"

	// TaskTypeQuestionAnswering is used for embedding questions in a Q&A system
	// This optimizes for finding documents that answer the question
	TaskTypeQuestionAnswering = "QUESTION_ANSWERING"
)

// loadPrompts loads all prompt templates from embedded files
func loadPrompts() {
	promptCacheOnce.Do(func() {
		promptCache = make(map[string]string)

		prompts := map[string]string{
			"rag_system_instruction": "prompt/rag_system_instruction.md",
			"rag_generate_content":   "prompt/rag_generate_content.md",
			"rag_generate_summary":   "prompt/rag_generate_summary.md",
		}

		for key, path := range prompts {
			content, err := promptFS.ReadFile(path)
			if err != nil {
				log.Fatalf("Failed to load prompt %s: %v", path, err)
			}
			promptCache[key] = string(content)
		}
	})
}

// getPrompt retrieves a prompt from cache
func getPrompt(key string) string {
	promptCacheMu.RLock()
	defer promptCacheMu.RUnlock()

	if prompt, ok := promptCache[key]; ok {
		return prompt
	}
	return ""
}

// GetSystemInstruction returns the RAG system instruction from embedded template
func GetSystemInstruction() string {
	loadPrompts()
	return getPrompt("rag_system_instruction")
}

// GetGenerateContentPrompt returns the content generation prompt from embedded template
func GetGenerateContentPrompt() string {
	loadPrompts()
	return getPrompt("rag_generate_content")
}

// GetGenerateSummaryPrompt returns the summary generation prompt from embedded template
func GetGenerateSummaryPrompt() string {
	loadPrompts()
	return getPrompt("rag_generate_summary")
}

// GetModel returns the default Gemini model
func GetModel() string {
	return DefaultModel
}

// GetCacheTTL returns the default cache TTL
func GetCacheTTL() time.Duration {
	return DefaultCacheTTL
}
