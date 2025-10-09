package repository

import (
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
}

// repository implements Artifact storage functions in PostgreSQL, vector database, and object storage.
type repository struct {
	db *gorm.DB
	VectorDatabase
	ObjectStorage
}

// NewRepository returns an initialized repository.
func NewRepository(db *gorm.DB, vectorDatabase VectorDatabase, objectStorage ObjectStorage) Repository {
	return &repository{
		db:             db,
		VectorDatabase: vectorDatabase,
		ObjectStorage:  objectStorage,
	}
}
