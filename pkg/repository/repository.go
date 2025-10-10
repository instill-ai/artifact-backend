package repository

import (
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

type Repository interface {
	Tag
	KnowledgeBase
	KnowledgeBaseFile
	ConvertedFile
	TextChunk
	Embedding
	Object
	ObjectURL
	VectorDatabase
	ObjectStorage
	ChatCache
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
