package repository

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type EmbeddingI interface {
	UpsertEmbeddings(ctx context.Context, embeddings []Embedding, externalServiceCall func(embUIDs []string) error) ([]Embedding, error)
	DeleteEmbeddingsBySource(ctx context.Context, sourceTable string, sourceUID uuid.UUID) error
	DeleteEmbeddingsByUIDs(ctx context.Context, embUIDs []uuid.UUID) error
	HardDeleteEmbeddingsByKbUID(ctx context.Context, kbUID uuid.UUID) error
	HardDeleteEmbeddingsByKbFileUID(ctx context.Context, kbFileUID uuid.UUID) error
	// GetEmbeddingByUIDs fetches embeddings by their UIDs.
	GetEmbeddingByUIDs(ctx context.Context, embUIDs []uuid.UUID) ([]Embedding, error)
	ListEmbeddingsByKbFileUID(ctx context.Context, kbFileUID uuid.UUID) ([]Embedding, error)
}
type Embedding struct {
	UID uuid.UUID `gorm:"column:uid;type:uuid;default:gen_random_uuid();primaryKey" json:"uid"`
	// SourceUID is the UID of the source entity that the embedding is associated with. i.e. the UID of the chunk, file, etc.
	// And SourceTable is the table name of the source entity.
	SourceTable string     `gorm:"column:source_table;size:255;not null" json:"source_table"`
	SourceUID   uuid.UUID  `gorm:"column:source_uid;type:uuid;not null" json:"source_uid"`
	Vector      Vector     `gorm:"column:vector;type:jsonb;not null" json:"vector"`
	Collection  string     `gorm:"column:collection;size:255;not null" json:"collection"`
	CreateTime  *time.Time `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime  *time.Time `gorm:"column:update_time;not null;default:CURRENT_TIMESTAMP" json:"update_time"`
	KbUID       uuid.UUID  `gorm:"column:kb_uid;type:uuid;not null" json:"kb_uid"`
	KbFileUID   uuid.UUID  `gorm:"column:kb_file_uid;type:uuid;not null" json:"kb_file_uid"`
}

type Vector []float32

func (v Vector) Value() (driver.Value, error) {
	if v == nil {
		return nil, nil
	}
	r, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return string(r), nil
}

func (v *Vector) Scan(value interface{}) error {
	if value == nil {
		*v = nil
		return nil
	}

	b, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("type assertion to []byte failed")
	}

	return json.Unmarshal(b, v)
}

// MarshalJSON implements the json.Marshaler interface
func (v Vector) MarshalJSON() ([]byte, error) {
	if v == nil {
		return []byte("null"), nil
	}
	return json.Marshal([]float32(v))
}

// UnmarshalJSON implements the json.Unmarshaler interface
func (v *Vector) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*v = nil
		return nil
	}
	var slice []float32
	if err := json.Unmarshal(data, &slice); err != nil {
		return err
	}
	*v = Vector(slice)
	return nil
}

type EmbeddingColumns struct {
	UID         string
	SourceUID   string
	SourceTable string
	Vector      string
	Collection  string
	CreateTime  string
	UpdateTime  string
	KbUID       string
	KbFileUID   string
}

var EmbeddingColumn = EmbeddingColumns{
	UID:         "uid",
	SourceUID:   "source_uid",
	SourceTable: "source_table",
	Vector:      "vector",
	Collection:  "collection",
	CreateTime:  "create_time",
	UpdateTime:  "update_time",
	KbUID:       "kb_uid",
	KbFileUID:   "kb_file_uid",
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

// GetEmbeddingByUIDs fetches embeddings by their UIDs.
func (r *Repository) GetEmbeddingByUIDs(ctx context.Context, embUIDs []uuid.UUID) ([]Embedding, error) {
	var embeddings []Embedding
	where := fmt.Sprintf("%s IN (?)", EmbeddingColumn.UID)
	if err := r.db.WithContext(ctx).Where(where, embUIDs).Find(&embeddings).Error; err != nil {
		return nil, err
	}
	return embeddings, nil
}

// HardDeleteEmbeddingsByKbUID deletes all the embeddings associated with a certain kbUID.
func (r *Repository) HardDeleteEmbeddingsByKbUID(ctx context.Context, kbUID uuid.UUID) error {
	where := fmt.Sprintf("%s = ?", EmbeddingColumn.KbUID)
	return r.db.WithContext(ctx).Where(where, kbUID).Unscoped().Delete(&Embedding{}).Error
}

// HardDeleteEmbeddingsByKbFileUID deletes all the embeddings associated with a certain kbFileUID.
func (r *Repository) HardDeleteEmbeddingsByKbFileUID(ctx context.Context, kbFileUID uuid.UUID) error {
	where := fmt.Sprintf("%s = ?", EmbeddingColumn.KbFileUID)
	return r.db.WithContext(ctx).Where(where, kbFileUID).Unscoped().Delete(&Embedding{}).Error
}

// ListEmbeddingsByKbFileUID fetches embeddings by their kbFileUID.
func (r *Repository) ListEmbeddingsByKbFileUID(ctx context.Context, kbFileUID uuid.UUID) ([]Embedding, error) {
	var embeddings []Embedding
	where := fmt.Sprintf("%s = ?", EmbeddingColumn.KbFileUID)
	if err := r.db.WithContext(ctx).Where(where, kbFileUID).Find(&embeddings).Error; err != nil {
		return nil, err
	}
	return embeddings, nil
}
