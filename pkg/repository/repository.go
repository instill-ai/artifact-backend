package repository

import (
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	"github.com/minio/minio-go/v7"

	"github.com/instill-ai/artifact-backend/pkg/repository/object"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
)

// Type aliases to help minimock detect imports from embedded interfaces
var _ artifactpb.KnowledgeBaseType
var _ *minio.ObjectInfo

// Repository interface defines the methods for the repository.
type Repository interface {
	// Tag handles repository tag operations for versioning and metadata tracking
	Tag
	// KnowledgeBase manages knowledge base CRUD operations and listings
	KnowledgeBase
	// KnowledgeBaseFile manages files within knowledge bases including upload and processing
	File
	// ConvertedFile handles converted file operations (e.g., PDF to markdown conversions)
	ConvertedFile
	// TextChunk manages text chunks extracted from files for RAG and search
	Chunk
	// Embedding manages vector embeddings for semantic search and similarity matching
	Embedding
	// Object manages object metadata and CRUD operations
	Object
	// VectorDatabase provides vector database operations for similarity search (e.g., Milvus, Qdrant)
	VectorDatabase
	// ChatCache manages file cache metadata in Redis for instant GetFile?view=VIEW_CACHE responses
	Cache
	// System manages system-wide Knowledge Base configuration
	System
	// GetMinIOStorage returns the MinIO storage instance (primary/default storage for all uploads)
	GetMinIOStorage() object.Storage
	// GetGCSStorage returns the GCS storage instance for on-demand use (returns nil if not configured)
	GetGCSStorage() object.Storage
	// GetDB returns the underlying database connection for transaction management
	GetDB() *gorm.DB
}

// repository implements Artifact storage functions in PostgreSQL, vector database, object storage, and Redis.
type repository struct {
	db *gorm.DB
	VectorDatabase
	minioStorage object.Storage // Primary storage (always MinIO for uploads)
	gcsStorage   object.Storage // On-demand GCS storage (optional)
	Cache
}

// NewRepository returns an initialized repository.
// minioStorage is always MinIO for uploads and persistence.
// gcsStorage is optional and used only for on-demand GetFile requests with storage_provider=STORAGE_PROVIDER_GCS.
func NewRepository(db *gorm.DB, vectorDatabase VectorDatabase, minioStorage object.Storage, redisClient *redis.Client, gcsStorage object.Storage) Repository {
	return &repository{
		db:             db,
		VectorDatabase: vectorDatabase,
		minioStorage:   minioStorage,
		gcsStorage:     gcsStorage,
		Cache:          NewCache(redisClient),
	}
}

// NewDBOnlyRepository returns a minimal repository with only database access.
// This is used for initialization processes like seeding default systems.
// Note: Methods requiring vectorDB, MinIO, or Redis will panic if called.
func NewDBOnlyRepository(db *gorm.DB) Repository {
	return &repository{
		db: db,
	}
}

// GetMinIOStorage returns the MinIO storage instance for primary storage operations.
// This is always available and used for all file uploads.
func (r *repository) GetMinIOStorage() object.Storage {
	return r.minioStorage
}

// GetGCSStorage returns the GCS storage instance for on-demand use.
// Returns nil if GCS is not configured.
func (r *repository) GetGCSStorage() object.Storage {
	return r.gcsStorage
}

// GetDB returns the underlying database connection for transaction management
func (r *repository) GetDB() *gorm.DB {
	return r.db
}
