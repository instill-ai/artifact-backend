package repository

import (
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	"github.com/minio/minio-go/v7"
)

// Type aliases to help minimock detect imports from embedded interfaces
var _ artifactpb.CatalogType
var _ *minio.ObjectInfo

// Repository interface defines the methods for the repository.
type Repository interface {
	// Tag handles repository tag operations for versioning and metadata tracking
	Tag
	// KnowledgeBase manages knowledge base CRUD operations and listings
	KnowledgeBase
	// KnowledgeBaseFile manages files within knowledge bases including upload and processing
	KnowledgeBaseFile
	// ConvertedFile handles converted file operations (e.g., PDF to markdown conversions)
	ConvertedFile
	// TextChunk manages text chunks extracted from files for RAG and search
	TextChunk
	// Embedding manages vector embeddings for semantic search and similarity matching
	Embedding
	// Object manages object metadata and CRUD operations
	Object
	// ObjectURL manages object URL generation and access control
	ObjectURL
	// VectorDatabase provides vector database operations for similarity search (e.g., Milvus, Qdrant)
	VectorDatabase
	// ObjectStorage handles object storage operations for file persistence (e.g., MinIO, S3)
	ObjectStorage
	// ChatCache manages chat conversation caching in Redis for performance optimization
	ChatCache
	// System manages system-wide Knowledge Base configuration
	System
}

// repository implements Artifact storage functions in PostgreSQL, vector database, object storage, and Redis.
type repository struct {
	db *gorm.DB
	VectorDatabase
	ObjectStorage
	ChatCache
}

// NewRepository returns an initialized repository.
func NewRepository(db *gorm.DB, vectorDatabase VectorDatabase, objectStorage ObjectStorage, redisClient *redis.Client) Repository {
	return &repository{
		db:             db,
		VectorDatabase: vectorDatabase,
		ObjectStorage:  objectStorage,
		ChatCache:      NewChatCacheRepository(redisClient),
	}
}
