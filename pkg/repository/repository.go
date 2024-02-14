package repository

import (
	"gorm.io/gorm"
)

// DefaultPageSize is the default pagination page size when page size is not assigned
const DefaultPageSize = 10

// MaxPageSize is the maximum pagination page size if the assigned value is over this number
const MaxPageSize = 100

// Repository interface
type Repository interface {
}

type repository struct {
	db *gorm.DB
}

func NewRepository(db *gorm.DB) Repository {
	return &repository{
		db: db,
	}
}
