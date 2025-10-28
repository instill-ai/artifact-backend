package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/instill-ai/artifact-backend/pkg/types"
)

// FileContentRef stores a small file's content directly in Redis
// Used for small files that couldn't be cached by AI client (< 1024 tokens ≈ 3-4KB)
// Since these files are small, storing content in Redis is more efficient than
// fetching from MinIO again during cache retrieval operations
type FileContentRef struct {
	FileUID  types.FileUIDType `json:"file_uid"`  // File unique identifier
	Content  []byte            `json:"content"`   // Actual file content (small, typically < 4KB)
	FileType string            `json:"file_type"` // File type (e.g., "FILE_TYPE_PDF")
	Filename string            `json:"filename"`  // Original filename
}

// CacheMetadata represents the metadata stored in Redis for cache operations
// This is ephemeral data with TTL matching the AI cached context expiration
// Supports two modes:
// 1. Cached mode: CachedContextEnabled=true, CacheName set, FileContents empty (large files, cached context exists)
// 2. Uncached mode: CachedContextEnabled=false, CacheName empty, FileContents set (small files, cached context does not exist)
type CacheMetadata struct {
	CacheName            string              `json:"cache_name,omitempty"`    // AI cache name (empty if cache creation failed)
	Model                string              `json:"model"`                   // Model used (e.g., "gemini-1.5-pro-002")
	FileUIDs             []types.FileUIDType `json:"file_uids"`               // File UIDs included in this cache
	FileCount            int                 `json:"file_count"`              // Number of files (denormalized for convenience)
	CreateTime           time.Time           `json:"create_time"`             // When cache was created
	ExpireTime           time.Time           `json:"expire_time"`             // When cache will expire
	CachedContextEnabled bool                `json:"cached_context_enabled"`  // Whether AI cached context exists
	FileContents         []FileContentRef    `json:"file_contents,omitempty"` // File references for uncached small files
}

// Cache interface defines operations for ephemeral cache metadata
type Cache interface {
	// SetCacheMetadata stores cache metadata in Redis with TTL
	SetCacheMetadata(ctx context.Context, kbUID types.KBUIDType, fileUIDs []types.FileUIDType, metadata *CacheMetadata, ttl time.Duration) error

	// GetCacheMetadata retrieves cache metadata from Redis
	// Returns nil if cache not found or expired
	GetCacheMetadata(ctx context.Context, kbUID types.KBUIDType, fileUIDs []types.FileUIDType) (*CacheMetadata, error)

	// DeleteCacheMetadata removes cache metadata from Redis
	// Used for manual cleanup (e.g., when cache is explicitly invalidated)
	DeleteCacheMetadata(ctx context.Context, kbUID types.KBUIDType, fileUIDs []types.FileUIDType) error
}

// cacheRepository implements Cache interface using Redis
type cacheRepository struct {
	redisClient *redis.Client
}

// NewCacheRepository creates a new cache repository
func NewCacheRepository(redisClient *redis.Client) Cache {
	return &cacheRepository{
		redisClient: redisClient,
	}
}

// SetCacheMetadata stores cache metadata in Redis with TTL
func (r *cacheRepository) SetCacheMetadata(
	ctx context.Context,
	kbUID types.KBUIDType,
	fileUIDs []types.FileUIDType,
	metadata *CacheMetadata,
	ttl time.Duration,
) error {
	if metadata == nil {
		return fmt.Errorf("metadata cannot be nil")
	}

	if ttl <= 0 {
		return fmt.Errorf("TTL must be positive, got: %v", ttl)
	}

	key := CacheKey(kbUID, fileUIDs)

	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal cache metadata: %w", err)
	}

	if err := r.redisClient.Set(ctx, key, data, ttl).Err(); err != nil {
		return fmt.Errorf("failed to store cache metadata in Redis: %w", err)
	}

	return nil
}

// GetCacheMetadata retrieves cache metadata from Redis
func (r *cacheRepository) GetCacheMetadata(
	ctx context.Context,
	kbUID types.KBUIDType,
	fileUIDs []types.FileUIDType,
) (*CacheMetadata, error) {
	key := CacheKey(kbUID, fileUIDs)

	data, err := r.redisClient.Get(ctx, key).Result()
	if err == redis.Nil {
		// Cache not found or expired (Redis automatically removed it)
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get cache metadata from Redis: %w", err)
	}

	var metadata CacheMetadata
	if err := json.Unmarshal([]byte(data), &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cache metadata: %w", err)
	}

	// Double-check expiration (redundant but safe)
	// Redis TTL should have already handled this, but we verify for safety
	if time.Now().After(metadata.ExpireTime) {
		// Metadata is expired, delete it and return nil
		_ = r.redisClient.Del(ctx, key).Err()
		return nil, nil
	}

	return &metadata, nil
}

// DeleteCacheMetadata removes cache metadata from Redis
func (r *cacheRepository) DeleteCacheMetadata(
	ctx context.Context,
	kbUID types.KBUIDType,
	fileUIDs []types.FileUIDType,
) error {
	key := CacheKey(kbUID, fileUIDs)

	if err := r.redisClient.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("failed to delete cache metadata from Redis: %w", err)
	}

	return nil
}

// CacheKey generates a deterministic Redis key for a set of file UIDs
// Format: artifact:cache:kb:{kb_uid}:files:{hash}
//
// The "artifact:" prefix ensures proper namespacing in shared Redis instances
// The hash is generated from sorted file UIDs for deterministic key generation
func CacheKey(kbUID types.KBUIDType, fileUIDs []types.FileUIDType) string {
	// Sort file UIDs for deterministic key generation
	// This ensures that the same set of files always produces the same key,
	// regardless of the order they're provided in
	sortedUIDs := make([]string, len(fileUIDs))
	for i, uid := range fileUIDs {
		sortedUIDs[i] = uid.String()
	}
	sort.Strings(sortedUIDs)

	// Create hash of sorted UIDs
	combined := strings.Join(sortedUIDs, "|")
	hash := sha256.Sum256([]byte(combined))
	hashStr := hex.EncodeToString(hash[:])[:16] // Use first 16 chars for readability

	return fmt.Sprintf("artifact:cache:kb:%s:files:%s", kbUID.String(), hashStr)
}

// CacheKeyPattern returns a pattern for matching all cache keys for a KB
// Useful for debugging or bulk operations
func CacheKeyPattern(kbUID types.KBUIDType) string {
	return fmt.Sprintf("artifact:cache:kb:%s:files:*", kbUID.String())
}

// AllCacheKeysPattern returns a pattern for matching all cache keys
// Useful for global cache statistics or cleanup operations
func AllCacheKeysPattern() string {
	return "artifact:cache:*"
}
