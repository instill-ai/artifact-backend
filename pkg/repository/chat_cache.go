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
// Used for small files that couldn't be cached by AI provider (< 1024 tokens ≈ 3-4KB)
// Since these files are small, storing content in Redis is more efficient than
// fetching from MinIO again during chat operations
type FileContentRef struct {
	FileUID  types.FileUIDType `json:"file_uid"`  // File unique identifier
	Content  []byte            `json:"content"`   // Actual file content (small, typically < 4KB)
	FileType string            `json:"file_type"` // File type (e.g., "FILE_TYPE_PDF")
	Filename string            `json:"filename"`  // Original filename
}

// ChatCacheMetadata represents the metadata stored in Redis for chat caches
// This is ephemeral data with TTL matching the AI cached context expiration
// Supports two modes:
// 1. Cached mode: CachedContextEnabled=true, CacheName set, FileContents empty (large files, cached context exists)
// 2. Uncached mode: CachedContextEnabled=false, CacheName empty, FileContents set (small files, cached context does not exist)
type ChatCacheMetadata struct {
	CacheName            string              `json:"cache_name,omitempty"`    // AI cache name (empty if cache creation failed)
	Model                string              `json:"model"`                   // Model used (e.g., "gemini-1.5-pro-002")
	FileUIDs             []types.FileUIDType `json:"file_uids"`               // File UIDs included in this cache
	FileCount            int                 `json:"file_count"`              // Number of files (denormalized for convenience)
	CreateTime           time.Time           `json:"create_time"`             // When cache was created
	ExpireTime           time.Time           `json:"expire_time"`             // When cache will expire
	CachedContextEnabled bool                `json:"cached_context_enabled"`  // Whether AI cached context exists
	FileContents         []FileContentRef    `json:"file_contents,omitempty"` // File references for uncached small files
}

// ChatCache interface defines operations for ephemeral chat cache metadata
type ChatCache interface {
	// SetChatCacheMetadata stores chat cache metadata in Redis with TTL
	SetChatCacheMetadata(ctx context.Context, kbUID types.KBUIDType, fileUIDs []types.FileUIDType, metadata *ChatCacheMetadata, ttl time.Duration) error

	// GetChatCacheMetadata retrieves chat cache metadata from Redis
	// Returns nil if cache not found or expired
	GetChatCacheMetadata(ctx context.Context, kbUID types.KBUIDType, fileUIDs []types.FileUIDType) (*ChatCacheMetadata, error)

	// DeleteChatCacheMetadata removes chat cache metadata from Redis
	// Used for manual cleanup (e.g., when cache is explicitly invalidated)
	DeleteChatCacheMetadata(ctx context.Context, kbUID types.KBUIDType, fileUIDs []types.FileUIDType) error
}

// chatCacheRepository implements ChatCache interface using Redis
type chatCacheRepository struct {
	redisClient *redis.Client
}

// NewChatCacheRepository creates a new chat cache repository
func NewChatCacheRepository(redisClient *redis.Client) ChatCache {
	return &chatCacheRepository{
		redisClient: redisClient,
	}
}

// SetChatCacheMetadata stores chat cache metadata in Redis with TTL
func (r *chatCacheRepository) SetChatCacheMetadata(
	ctx context.Context,
	kbUID types.KBUIDType,
	fileUIDs []types.FileUIDType,
	metadata *ChatCacheMetadata,
	ttl time.Duration,
) error {
	if metadata == nil {
		return fmt.Errorf("metadata cannot be nil")
	}

	if ttl <= 0 {
		return fmt.Errorf("TTL must be positive, got: %v", ttl)
	}

	key := ChatCacheKey(kbUID, fileUIDs)

	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal chat cache metadata: %w", err)
	}

	if err := r.redisClient.Set(ctx, key, data, ttl).Err(); err != nil {
		return fmt.Errorf("failed to store chat cache metadata in Redis: %w", err)
	}

	return nil
}

// GetChatCacheMetadata retrieves chat cache metadata from Redis
func (r *chatCacheRepository) GetChatCacheMetadata(
	ctx context.Context,
	kbUID types.KBUIDType,
	fileUIDs []types.FileUIDType,
) (*ChatCacheMetadata, error) {
	key := ChatCacheKey(kbUID, fileUIDs)

	data, err := r.redisClient.Get(ctx, key).Result()
	if err == redis.Nil {
		// Cache not found or expired (Redis automatically removed it)
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get chat cache metadata from Redis: %w", err)
	}

	var metadata ChatCacheMetadata
	if err := json.Unmarshal([]byte(data), &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal chat cache metadata: %w", err)
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

// DeleteChatCacheMetadata removes chat cache metadata from Redis
func (r *chatCacheRepository) DeleteChatCacheMetadata(
	ctx context.Context,
	kbUID types.KBUIDType,
	fileUIDs []types.FileUIDType,
) error {
	key := ChatCacheKey(kbUID, fileUIDs)

	if err := r.redisClient.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("failed to delete chat cache metadata from Redis: %w", err)
	}

	return nil
}

// ChatCacheKey generates a deterministic Redis key for a set of file UIDs
// Format: artifact:chat-cache:kb:{kb_uid}:files:{hash}
//
// The "artifact:" prefix ensures proper namespacing in shared Redis instances
// The hash is generated from sorted file UIDs for deterministic key generation
func ChatCacheKey(kbUID types.KBUIDType, fileUIDs []types.FileUIDType) string {
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

	return fmt.Sprintf("artifact:chat-cache:kb:%s:files:%s", kbUID.String(), hashStr)
}

// ChatCacheKeyPattern returns a pattern for matching all chat cache keys for a KB
// Useful for debugging or bulk operations
func ChatCacheKeyPattern(kbUID types.KBUIDType) string {
	return fmt.Sprintf("artifact:chat-cache:kb:%s:files:*", kbUID.String())
}

// AllChatCacheKeysPattern returns a pattern for matching all chat cache keys
// Useful for global cache statistics or cleanup operations
func AllChatCacheKeysPattern() string {
	return "artifact:chat-cache:*"
}
