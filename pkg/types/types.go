package types

import (
	"time"

	"github.com/gofrs/uuid"
)

// FileType represents the type of file being processed
type FileType string

const (
	// DocumentFileType represents the type of document file
	DocumentFileType FileType = "document"
	// VideoFileType represents the type of video file
	VideoFileType FileType = "video"
	// ImageFileType represents the type of image file
	ImageFileType FileType = "image"
	// AudioFileType represents the type of audio file
	AudioFileType FileType = "audio"
)

// CatalogType represents the type of knowledge base catalog
type CatalogType string

const (
	// CatalogTypeUnspecified is the default/unspecified catalog type
	CatalogTypeUnspecified CatalogType = ""
	// CatalogTypePersistent represents a persistent catalog
	CatalogTypePersistent CatalogType = "persistent"
	// CatalogTypeEphemeral represents an ephemeral/temporary catalog
	CatalogTypeEphemeral CatalogType = "ephemeral"
)

// Tag represents a repository tag (domain model)
type Tag struct {
	Name       string    // The name of the tag (e.g. "repositories/{repo}/tags/{id}")
	ID         string    // The tag identifier
	Digest     string    // Unique identifier from the manifest
	UpdateTime time.Time // Tag update time
}

// ContentType represents the type of content in a chunk
type ContentType string

const (
	// ChunkContentType represents the type of chunk content
	ChunkContentType ContentType = "chunk"
	// SummaryContentType represents the type of summary content
	SummaryContentType ContentType = "summary"
	// AugmentedContentType represents the type of augmented content
	AugmentedContentType ContentType = "augmented"
)

type (
	// Knowledge Base unique identifier
	KBUIDType = uuid.UUID
	// File unique identifier
	FileUIDType = uuid.UUID
	// Text chunk unique identifier
	TextChunkUIDType = uuid.UUID
	// Converted file unique identifier
	ConvertedFileUIDType = uuid.UUID // Converted file unique identifier

	// User and Permission UIDs
	UserUIDType      = uuid.UUID // User unique identifier
	RequesterUIDType = uuid.UUID // Request initiator unique identifier
	CreatorUIDType   = uuid.UUID // Creator unique identifier
	OwnerUIDType     = uuid.UUID // Owner unique identifier

	// Source tracking (used in chunk metadata)
	SourceUIDType   = uuid.UUID // Source entity unique identifier
	SourceTableType = string    // Source table name
)
