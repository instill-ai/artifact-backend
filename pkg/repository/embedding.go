package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type EmbeddingI interface {
	UpsertEmbeddings(ctx context.Context, embeddings []Embedding, externalServiceCall func(embUIDs []string) error) ([]Embedding, error)
	DeleteEmbeddingsBySource(ctx context.Context, sourceTable string, sourceUID uuid.UUID) error
	DeleteEmbeddingsByUIDs(ctx context.Context, embUIDs []uuid.UUID) error
}
type Embedding struct {
	UID         uuid.UUID  `gorm:"column:uid;type:uuid;default:gen_random_uuid();primaryKey" json:"uid"`
	SourceUID   uuid.UUID  `gorm:"column:source_uid;type:uuid;not null" json:"source_uid"`
	SourceTable string     `gorm:"column:source_table;size:255;not null" json:"source_table"`
	Vector      []float32  `gorm:"column:vector;type:jsonb;not null" json:"vector"`
	CreateTime  *time.Time `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime  *time.Time `gorm:"column:update_time;not null;default:CURRENT_TIMESTAMP" json:"update_time"`
}

type EmbeddingColumns struct {
	UID         string
	SourceUID   string
	SourceTable string
	Vector      string
	CreateTime  string
	UpdateTime  string
}

var EmbeddingColumn = EmbeddingColumns{
	UID:         "uid",
	SourceUID:   "source_uid",
	SourceTable: "source_table",
	Vector:      "vector",
	CreateTime:  "create_time",
	UpdateTime:  "update_time",
}

// TableName returns the table name of the Embedding
func (Embedding) TableName() string {
	return "embedding"
}

// UpsertEmbeddings upserts embeddings into the database. If externalServiceCall
// is not nil, it will be called with the list of embedding UIDs.
func (r *Repository) UpsertEmbeddings(
	ctx context.Context,
	embeddings []Embedding,
	externalServiceCall func(embUIDs []string) error,
) ([]Embedding, error) {
	// get logger
	logger, _ := logger.GetZapLogger(ctx)
	// Start a transaction
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Upsert the embeddings
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: EmbeddingColumn.SourceTable}, {Name: EmbeddingColumn.SourceUID}}, // Unique column that triggers the upsert
			DoUpdates: clause.AssignmentColumns([]string{EmbeddingColumn.Vector}),                              // Fields to update on conflict
		}).Create(&embeddings).Error; err != nil {
			logger.Error("failed to upsert embeddings", zap.Error(err))
			return fmt.Errorf("failed to upsert embeddings: %w", err)
		}

		embUIDs := make([]string, len(embeddings))
		for i, emb := range embeddings {
			embUIDs[i] = emb.UID.String()
		}

		if externalServiceCall != nil {
			if err := externalServiceCall(embUIDs); err != nil {
				logger.Error("external service call failed in upsertEmbeddings", zap.Error(err))
				return fmt.Errorf("external service call failed: %w", err)
			}
		}

		return nil
	})

	if err != nil {
		logger.Error("upsertEmbeddings transaction failed", zap.Error(err))
		return nil, fmt.Errorf("transaction failed: %w", err)
	}

	return embeddings, nil
}

// DeleteEmbeddingsBySource deletes all the embeddings associated with a certain source table and sourceUID.
func (r *Repository) DeleteEmbeddingsBySource(ctx context.Context, sourceTable string, sourceUID uuid.UUID) error {
	where := fmt.Sprintf("%s = ? AND %s = ?", EmbeddingColumn.SourceTable, EmbeddingColumn.SourceUID)
	return r.db.WithContext(ctx).Where(where, sourceTable, sourceUID).Delete(&Embedding{}).Error
}

// DeleteEmbeddingsByUIDs deletes all the embeddings associated with a certain source table and sourceUID.
func (r *Repository) DeleteEmbeddingsByUIDs(ctx context.Context, embUIDs []uuid.UUID) error {
	where := fmt.Sprintf("%s IN (?)", EmbeddingColumn.UID)
	return r.db.WithContext(ctx).Where(where, embUIDs).Delete(&Embedding{}).Error
}
