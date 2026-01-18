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
	FileType string            `json:"file_type"` // File type (e.g., "TYPE_PDF")
	Filename string            `json:"filename"`  // Original filename
}

// CacheMetadata represents the metadata stored in Redis for AI context cache operations
// This is ephemeral data with TTL matching the AI cached context expiration
// Supports two modes:
// 1. Cached mode: CachedContextEnabled=true, CacheName set, FileContents empty (large files, cached context exists)
// 2. Uncached mode: CachedContextEnabled=false, CacheName empty, FileContents set (small files, cached context does not exist)
type CacheMetadata struct {
	CacheName            string              `json:"cache_name,omitempty"`   // AI cache name (empty if cache creation failed)
	Model                string              `json:"model"`                  // Model used (e.g., "gemini-1.5-pro-002")
	FileUIDs             []types.FileUIDType `json:"file_uids"`              // File UIDs included in this cache
	FileCount            int                 `json:"file_count"`             // Number of files (denormalized for convenience)
	CreateTime           time.Time           `json:"create_time"`            // When cache was created
	ExpireTime           time.Time           `json:"expire_time"`            // When cache will expire
	CachedContextEnabled bool                `json:"cached_context_enabled"` // Whether AI cached context exists
}

// GCSFileInfo stores GCS file metadata for TTL-based cleanup
type GCSFileInfo struct {
	KBUIDStr      string    `json:"kb_uid"`      // Knowledge base UID as string
	FileUIDStr    string    `json:"file_uid"`    // File UID as string
	View          string    `json:"view"`        // View type (e.g., "VIEW_CONTENT")
	GCSBucket     string    `json:"gcs_bucket"`  // GCS bucket name
	GCSObjectPath string    `json:"gcs_path"`    // GCS object path
	UploadTime    time.Time `json:"upload_time"` // When file was uploaded to GCS
	ExpiresAt     time.Time `json:"expires_at"`  // When file should be deleted from GCS
}

// Cache interface defines operations for ephemeral cache metadata
type Cache interface {
	// SetCacheMetadata stores cache metadata in Redis with TTL
	SetCacheMetadata(ctx context.Context, kbUID types.KBUIDType, fileUIDs []types.FileUIDType, metadata *CacheMetadata, ttl time.Duration) error

	// GetCacheMetadata retrieves cache metadata from Redis
	// Returns nil if cache not found or expired
	GetCacheMetadata(ctx context.Context, kbUID types.KBUIDType, fileUIDs []types.FileUIDType) (*CacheMetadata, error)

	// RenewCacheMetadataTTL updates the TTL and expire time for existing cache metadata
	// Used when GetFile?view=VIEW_CACHE is called to extend the cache lifetime
	RenewCacheMetadataTTL(ctx context.Context, kbUID types.KBUIDType, fileUIDs []types.FileUIDType, newTTL time.Duration, newExpireTime time.Time) error

	// DeleteCacheMetadata removes cache metadata from Redis
	// Used for manual cleanup (e.g., when cache is explicitly invalidated)
	DeleteCacheMetadata(ctx context.Context, kbUID types.KBUIDType, fileUIDs []types.FileUIDType) error

	// CheckGCSFileExists checks if a file exists in GCS (cached check)
	// Returns true if file exists in GCS cache, false otherwise
	CheckGCSFileExists(ctx context.Context, kbUID types.KBUIDType, fileUID types.FileUIDType, view string) (bool, error)

	// SetGCSFileInfo stores GCS file metadata in Redis with TTL for cleanup
	// ttl should be 10 minutes for on-demand GCS files
	SetGCSFileInfo(ctx context.Context, kbUID types.KBUIDType, fileUID types.FileUIDType, view string, gcsInfo *GCSFileInfo, ttl time.Duration) error

	// GetGCSFileInfo retrieves GCS file metadata from Redis
	// Returns nil if not found or expired
	GetGCSFileInfo(ctx context.Context, kbUID types.KBUIDType, fileUID types.FileUIDType, view string) (*GCSFileInfo, error)

	// DeleteGCSFileCache removes GCS file metadata from Redis
	DeleteGCSFileCache(ctx context.Context, kbUID types.KBUIDType, fileUID types.FileUIDType, view string) error

	// ScanGCSFilesForCleanup scans Redis for GCS files that need cleanup
	// Returns list of GCS files whose TTL has expired or is about to expire
	ScanGCSFilesForCleanup(ctx context.Context, maxCount int64) ([]GCSFileInfo, error)
}

// cache implements Cache interface using Redis
type cache struct {
	redisClient *redis.Client
}

// NewCache implements Cache interface using Redis
func NewCache(redisClient *redis.Client) Cache {
	return &cache{
		redisClient: redisClient,
	}
}

// SetCacheMetadata stores cache metadata in Redis with TTL
func (r *cache) SetCacheMetadata(
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
func (r *cache) GetCacheMetadata(
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

// RenewCacheMetadataTTL updates the TTL and expire time for existing cache metadata
// This is more efficient than re-marshaling and setting the entire metadata
func (r *cache) RenewCacheMetadataTTL(
	ctx context.Context,
	kbUID types.KBUIDType,
	fileUIDs []types.FileUIDType,
	newTTL time.Duration,
	newExpireTime time.Time,
) error {
	if newTTL <= 0 {
		return fmt.Errorf("TTL must be positive, got: %v", newTTL)
	}

	key := CacheKey(kbUID, fileUIDs)

	// Get existing metadata
	metadata, err := r.GetCacheMetadata(ctx, kbUID, fileUIDs)
	if err != nil {
		return fmt.Errorf("failed to get cache metadata for renewal: %w", err)
	}
	if metadata == nil {
		return fmt.Errorf("cache metadata not found for renewal")
	}

	// Update expire time
	metadata.ExpireTime = newExpireTime

	// Re-serialize and store with new TTL
	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal cache metadata for renewal: %w", err)
	}

	if err := r.redisClient.Set(ctx, key, data, newTTL).Err(); err != nil {
		return fmt.Errorf("failed to renew cache metadata TTL in Redis: %w", err)
	}

	return nil
}

// DeleteCacheMetadata removes cache metadata from Redis
func (r *cache) DeleteCacheMetadata(
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

// ============================================================================
// GCS File Cache Methods (for TTL-based cleanup and existence checking)
// ============================================================================

// GCSFileKey generates a Redis key for GCS file metadata
// Format: artifact:gcs:kb:{kb_uid}:file:{file_uid}:view:{view}
func GCSFileKey(kbUID types.KBUIDType, fileUID types.FileUIDType, view string) string {
	return fmt.Sprintf("artifact:gcs:kb:%s:file:%s:view:%s", kbUID.String(), fileUID.String(), view)
}

// GCSFileKeyPattern returns a pattern for matching all GCS file keys
func GCSFileKeyPattern() string {
	return "artifact:gcs:*"
}

// CheckGCSFileExists checks if a file exists in GCS (cached check)
func (r *cache) CheckGCSFileExists(
	ctx context.Context,
	kbUID types.KBUIDType,
	fileUID types.FileUIDType,
	view string,
) (bool, error) {
	key := GCSFileKey(kbUID, fileUID, view)

	exists, err := r.redisClient.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check GCS file existence in Redis: %w", err)
	}

	return exists > 0, nil
}

// SetGCSFileInfo stores GCS file metadata in Redis with TTL
func (r *cache) SetGCSFileInfo(
	ctx context.Context,
	kbUID types.KBUIDType,
	fileUID types.FileUIDType,
	view string,
	gcsInfo *GCSFileInfo,
	ttl time.Duration,
) error {
	if gcsInfo == nil {
		return fmt.Errorf("gcsInfo cannot be nil")
	}

	if ttl <= 0 {
		return fmt.Errorf("TTL must be positive, got: %v", ttl)
	}

	key := GCSFileKey(kbUID, fileUID, view)

	data, err := json.Marshal(gcsInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal GCS file info: %w", err)
	}

	if err := r.redisClient.Set(ctx, key, data, ttl).Err(); err != nil {
		return fmt.Errorf("failed to store GCS file info in Redis: %w", err)
	}

	return nil
}

// GetGCSFileInfo retrieves GCS file metadata from Redis
func (r *cache) GetGCSFileInfo(
	ctx context.Context,
	kbUID types.KBUIDType,
	fileUID types.FileUIDType,
	view string,
) (*GCSFileInfo, error) {
	key := GCSFileKey(kbUID, fileUID, view)

	data, err := r.redisClient.Get(ctx, key).Result()
	if err == redis.Nil {
		// Not found or expired
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get GCS file info from Redis: %w", err)
	}

	var gcsInfo GCSFileInfo
	if err := json.Unmarshal([]byte(data), &gcsInfo); err != nil {
		return nil, fmt.Errorf("failed to unmarshal GCS file info: %w", err)
	}

	return &gcsInfo, nil
}

// DeleteGCSFileCache removes GCS file metadata from Redis
func (r *cache) DeleteGCSFileCache(
	ctx context.Context,
	kbUID types.KBUIDType,
	fileUID types.FileUIDType,
	view string,
) error {
	key := GCSFileKey(kbUID, fileUID, view)

	if err := r.redisClient.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("failed to delete GCS file cache from Redis: %w", err)
	}

	return nil
}

// ScanGCSFilesForCleanup scans Redis for GCS files that need cleanup
// Returns list of GCS files whose TTL is expiring soon (for background cleanup)
func (r *cache) ScanGCSFilesForCleanup(ctx context.Context, maxCount int64) ([]GCSFileInfo, error) {
	pattern := GCSFileKeyPattern()

	var allKeys []string
	var cursor uint64

	// Scan all GCS file keys
	for {
		keys, nextCursor, err := r.redisClient.Scan(ctx, cursor, pattern, maxCount).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to scan GCS file keys: %w", err)
		}

		allKeys = append(allKeys, keys...)
		cursor = nextCursor

		if cursor == 0 {
			break
		}
	}

	// Retrieve metadata for each key
	var gcsFiles []GCSFileInfo
	for _, key := range allKeys {
		data, err := r.redisClient.Get(ctx, key).Result()
		if err == redis.Nil {
			// Key expired between scan and get
			continue
		}
		if err != nil {
			// Log error but continue with other keys
			continue
		}

		var gcsInfo GCSFileInfo
		if err := json.Unmarshal([]byte(data), &gcsInfo); err != nil {
			// Log error but continue
			continue
		}

		// Check TTL to see if file is expiring soon (within 1 minute)
		ttl, err := r.redisClient.TTL(ctx, key).Result()
		if err != nil {
			continue
		}

		// Include files that are expiring within 1 minute or already expired
		if ttl <= 1*time.Minute {
			gcsFiles = append(gcsFiles, gcsInfo)
		}
	}

	return gcsFiles, nil
}
