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
func GenerateResourceID(displayName string, uid uuid.UUID) string {
	slug := GenerateSlug(displayName)
	hash := sha256.Sum256([]byte(uid.String()))
	shortHash := hex.EncodeToString(hash[:])[:8]
	if slug == "" {
		return shortHash
	}
	return fmt.Sprintf("%s-%s", slug, shortHash)
}

// GoRecover take a function as input, if the function panics, the panic will be recovered and the error will be returned
func GoRecover(f func(), name string) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("\x1b[31m", "panic occurred at: \n", name, "\npanic: \n", r, "\x1b[0m")
		}
	}()
	f()
}
