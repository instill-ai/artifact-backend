package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	"github.com/gofrs/uuid"
)

const (
	CreateEvent string = "Create"
	UpdateEvent string = "Update"
	DeleteEvent string = "Delete"
)

// ResourcePrefix represents the prefix for different resource types in AIP-compliant IDs
type ResourcePrefix string

const (
	PrefixKnowledgeBase ResourcePrefix = "kb"
	PrefixFile          ResourcePrefix = "file"
	PrefixChunk         ResourcePrefix = "chk"
	PrefixObject        ResourcePrefix = "obj"
	PrefixSystem        ResourcePrefix = "sys"
)

// base62Chars contains the characters used for base62 encoding (URL-safe without special chars)
const base62Chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// encodeBase62 encodes a byte slice to a base62 string
func encodeBase62(data []byte) string {
	if len(data) == 0 {
		return ""
	}

	// Convert bytes to a big integer-like representation
	// Process in chunks to avoid overflow
	var result strings.Builder
	for _, b := range data {
		// For each byte, we'll append its base62 representation
		// This gives us roughly 1.3 chars per byte (log(256)/log(62))
		if b == 0 {
			result.WriteByte(base62Chars[0])
		} else {
			for b > 0 {
				result.WriteByte(base62Chars[b%62])
				b /= 62
			}
		}
	}
	return result.String()
}

// GeneratePrefixedResourceID creates an AIP-compliant prefixed resource ID from a UUID.
// The format is: {prefix}-{base62(sha256(uid)[:10])}
// This provides 80 bits of entropy in a URL-safe format.
// Example: uuid "550e8400-e29b-41d4-a716-446655440000" with prefix "kb"
//
//	-> "kb-7F3a2B1cXy5"
func GeneratePrefixedResourceID(prefix ResourcePrefix, uid uuid.UUID) string {
	hash := sha256.Sum256([]byte(uid.String()))
	// Take first 10 bytes (80 bits) for sufficient entropy
	encoded := encodeBase62(hash[:10])
	return fmt.Sprintf("%s-%s", prefix, encoded)
}

// GenerateSlug converts a display name to a URL-safe slug.
// Example: "My Research Collection" -> "my-research-collection"
// Rules:
// - Convert to lowercase
// - Replace spaces with dashes
// - Remove non-alphanumeric characters (except dashes)
// - Collapse multiple dashes into one
// - Trim leading/trailing dashes
func GenerateSlug(displayName string) string {
	// Convert to lowercase
	slug := strings.ToLower(displayName)

	// Replace spaces and underscores with dashes
	slug = strings.ReplaceAll(slug, " ", "-")
	slug = strings.ReplaceAll(slug, "_", "-")

	// Remove non-alphanumeric characters (except dashes)
	re := regexp.MustCompile(`[^a-z0-9-]`)
	slug = re.ReplaceAllString(slug, "")

	// Collapse multiple dashes into one
	multiDashRegex := regexp.MustCompile(`-+`)
	slug = multiDashRegex.ReplaceAllString(slug, "-")

	// Trim leading/trailing dashes
	slug = strings.Trim(slug, "-")

	return slug
}

// GenerateResourceID creates a hash-based resource ID from a display name and UUID.
// The format is: {slug}-{sha256(uid)[:8]}
// Example: "My Research Collection" with uid "550e8400-e29b-41d4-a716-446655440000"
//
//	-> "my-research-collection-7f3a2b1c"
//
// If the slug is empty, only the hash is returned.
// This ensures globally unique IDs while maintaining human-readable URLs.
//
// Deprecated: Use GeneratePrefixedResourceID instead. The new standard uses
// prefixed IDs like "kb-{hash}", "file-{hash}" instead of slug-based IDs.
// This function is kept for backward compatibility but should not be used
// for new code. See data-model.md for the canonical ID format specification.
func GenerateResourceID(displayName string, uid uuid.UUID) string {
	slug := GenerateSlug(displayName)
	hash := sha256.Sum256([]byte(uid.String()))
	shortHash := hex.EncodeToString(hash[:])[:8]
	if slug == "" {
		return shortHash
	}
	return fmt.Sprintf("%s-%s", slug, shortHash)
}
