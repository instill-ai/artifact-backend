package repository

import (
	"context"

	"github.com/gofrs/uuid"
	"go.einride.tech/aip/filtering"
	"go.einride.tech/aip/ordering"
)

type CatalogRunI interface {
	CreateCatalogRun(ctx context.Context, run *CatalogRun) (*CatalogRun, error)
	UpdateCatalogRun(ctx context.Context, runUID uuid.UUID, run *CatalogRun) error
	ListCatalogRuns(ctx context.Context, pageSize, page int64, filter filtering.Filter, order ordering.OrderBy, catalogUID, requesterUID string, isOwner bool) (modelTriggers []*CatalogRun, totalSize int64, err error)
}

func (r *Repository) CreateCatalogRun(ctx context.Context, run *CatalogRun) (*CatalogRun, error) {
	err := r.db.Create(run).Error
	if err != nil {
		return nil, err
	}
	return run, nil
}

func (r *Repository) UpdateCatalogRun(ctx context.Context, runUID uuid.UUID, run *CatalogRun) error {
	return r.db.Model(&CatalogRun{}).Where(&CatalogRun{UID: runUID}).Updates(&run).Error
}

func (r *Repository) ListCatalogRuns(ctx context.Context, pageSize, page int64, filter filtering.Filter, order ordering.OrderBy, catalogUID, requesterUID string, isOwner bool) (modelTriggers []*CatalogRun, totalSize int64, err error) {
	// TODO implement me
	panic("implement me")
}
