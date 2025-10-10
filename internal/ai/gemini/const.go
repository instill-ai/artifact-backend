package gemini

import (
	"embed"
	"log"
	"sync"
	"time"

	"github.com/instill-ai/artifact-backend/config"
)

//go:embed prompt/*.md
var promptFS embed.FS

var (
	promptCache     map[string]string
	promptCacheMu   sync.RWMutex
	promptCacheOnce sync.Once
)

// Constants for Gemini AI provider
const (
	// DefaultModel is the default Gemini model for multimodal content conversion
	DefaultModel = "gemini-2.5-flash"

	// DefaultCacheTTL is the default time-to-live for cached content (1 hour)
	DefaultCacheTTL = time.Hour
)

// loadPrompts loads all prompt templates from embedded files
func loadPrompts() {
	promptCacheOnce.Do(func() {
		promptCache = make(map[string]string)

		prompts := map[string]string{
			"chat_system_instruction": "prompt/chat_system_instruction.md",
			"rag_system_instruction":  "prompt/rag_system_instruction.md",
			"rag_generate_content":    "prompt/rag_generate_content.md",
			"rag_generate_summary":    "prompt/rag_generate_summary.md",
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

// GetChatSystemInstruction returns the chat system instruction from embedded template
func GetChatSystemInstruction() string {
	loadPrompts()
	return getPrompt("chat_system_instruction")
}

// GetRAGSystemInstruction returns the RAG system instruction from embedded template
func GetRAGSystemInstruction() string {
	loadPrompts()
	return getPrompt("rag_system_instruction")
}

// GetRAGGenerateContentPrompt returns the content generation prompt from embedded template
func GetRAGGenerateContentPrompt() string {
	loadPrompts()
	return getPrompt("rag_generate_content")
}

// GetRAGGenerateSummaryPrompt returns the summary generation prompt from embedded template
func GetRAGGenerateSummaryPrompt() string {
	loadPrompts()
	return getPrompt("rag_generate_summary")
}

// GetModel returns the default Gemini model
func GetModel() string {
	return DefaultModel
}

// GetCacheTTL returns the configured cache TTL or default
func GetCacheTTL() time.Duration {
	if config.Config.RAG.Model.Gemini.CacheTTLMinutes > 0 {
		return time.Duration(config.Config.RAG.Model.Gemini.CacheTTLMinutes) * time.Minute
	}
	return DefaultCacheTTL
}
