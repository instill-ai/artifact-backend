package repository

import (
	"gorm.io/gorm"
)

type RepositoryI interface {
	TagI
	KnowledgeBaseI
	KnowledgeBaseFileI
}

// Repository implements Artifact storage functions in PostgreSQL.
type Repository struct {
	db *gorm.DB
}

// NewRepository returns an initialized repository.
func NewRepository(db *gorm.DB) RepositoryI {
	return &Repository{
		db: db,
	}
}
