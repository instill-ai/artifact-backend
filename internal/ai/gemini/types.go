package gemini

import (
	"time"

	"google.golang.org/genai"
)

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
