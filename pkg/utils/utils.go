package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/gofrs/uuid"
	"github.com/instill-ai/x/resource"
)

const (
	CreateEvent string = "Create"
	UpdateEvent string = "Update"
	DeleteEvent string = "Delete"
)

// Resource prefix constants for artifact-backend AIP-compliant IDs
const (
	PrefixKnowledgeBase = "kb"
	PrefixFile          = "file"
	PrefixChunk         = "chk"
	PrefixObject        = "obj"
	PrefixSystem        = "sys"
)

// GeneratePrefixedResourceID creates an AIP-compliant prefixed resource ID from a UUID.
// Delegates to resource.GeneratePrefixedID from github.com/instill-ai/x/resource
func GeneratePrefixedResourceID(prefix string, uid uuid.UUID) string {
	return resource.GeneratePrefixedID(prefix, uid)
}

// GenerateSlug converts a display name to a URL-safe slug.
// Delegates to resource.GenerateSlug from github.com/instill-ai/x/resource
func GenerateSlug(displayName string) string {
	return resource.GenerateSlug(displayName, 0)
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
	slug := resource.GenerateSlug(displayName, 0)
	hash := sha256.Sum256([]byte(uid.String()))
	shortHash := hex.EncodeToString(hash[:])[:8]
	if slug == "" {
		return shortHash
	}
	return fmt.Sprintf("%s-%s", slug, shortHash)
}
