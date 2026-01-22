package repository

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/instill-ai/artifact-backend/pkg/types"
	"gorm.io/gorm"

	logx "github.com/instill-ai/x/log"
)

const (
	// EmbeddingTableName is the table name for embeddings
	EmbeddingTableName = "embedding"
)

// Embedding is the interface for the embedding repository
type Embedding interface {
	DeleteAndCreateEmbeddings(_ context.Context, fileUID types.FileUIDType, embeddings []EmbeddingModel, externalServiceCall func([]EmbeddingModel) error) ([]EmbeddingModel, error)
	CreateEmbeddings(_ context.Context, embeddings []EmbeddingModel, externalServiceCall func([]EmbeddingModel) error) ([]EmbeddingModel, error)
	DeleteEmbeddingsByKBFileUID(_ context.Context, kbFileUID types.FileUIDType) error
	HardDeleteEmbeddingsByKBUID(_ context.Context, kbUID types.KBUIDType) error
	HardDeleteEmbeddingsByKBFileUID(_ context.Context, kbFileUID types.FileUIDType) error
	ListEmbeddingsByKBFileUID(_ context.Context, kbFileUID types.FileUIDType) ([]EmbeddingModel, error)
	GetEmbeddingCountByKBUID(ctx context.Context, kbUID types.KBUIDType) (int64, error)
	UpdateEmbeddingTagsForFile(ctx context.Context, collectionID string, fileUID types.FileUIDType, tags []string) error
}

// EmbeddingModel is the model for the embedding table
type EmbeddingModel struct {
	UID types.EmbeddingUIDType `gorm:"column:uid;type:uuid;default:gen_random_uuid();primaryKey" json:"uid"`
	// SourceUID is the UID of the source entity that the embedding is associated with. i.e. the UID of the chunk, file, etc.
	// And SourceTable is the table name of the source entity.
	SourceTable      string                     `gorm:"column:source_table;size:255;not null" json:"source_table"`
	SourceUID        types.SourceUIDType        `gorm:"column:source_uid;type:uuid;not null" json:"source_uid"`
	Vector           Vector                     `gorm:"column:vector;type:jsonb;not null" json:"vector"`
	CreateTime       *time.Time                 `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime       *time.Time                 `gorm:"column:update_time;not null;default:CURRENT_TIMESTAMP" json:"update_time"`
	KnowledgeBaseUID types.KnowledgeBaseUIDType `gorm:"column:kb_uid;type:uuid;not null" json:"kb_uid"`
	FileUID          types.FileUIDType          `gorm:"column:file_uid;type:uuid;not null" json:"file_uid"`
	// ContentType stores the MIME type (e.g., "text/markdown", "application/pdf")
	ContentType string `gorm:"column:content_type;size:255;not null" json:"content_type"`
	// ChunkType stores the chunk classification ("content", "summary", "augmented")
	ChunkType string `gorm:"column:chunk_type;size:255;not null" json:"chunk_type"`
	// Tags associated with the embedding, inherited from the source file.
	// Note: The tags are not stored in the database, they will only be used for
	// filtering on the vector DB. However, the worker converts the repository
	// embedding entity to the domain one, not vice versa, so this field is
	// needed to be passed along to the vector DB.
	// TODO: after the worker layer is rewritten as Temporal workflows and
	// activities, rewrite how the embedding entities are converted to domain ->
	// repository.
	Tags []string `json:"tags" gorm:"-"`
}

// Vector is the type for the vector column
type Vector []float32

// Value implements the driver.Valuer interface
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

// Scan implements the sql.Scanner interface
func (v *Vector) Scan(value any) error {
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

// EmbeddingColumns is the columns for the embedding table
type EmbeddingColumns struct {
	UID         string
	SourceUID   string
	SourceTable string
	Vector      string
	CreateTime  string
	UpdateTime  string
	KBUID       string
	FileUID     string
	ContentType string
	ChunkType   string
}

// EmbeddingColumn is the column for the embedding table
var EmbeddingColumn = EmbeddingColumns{
	UID:         "uid",
	SourceUID:   "source_uid",
	SourceTable: "source_table",
	Vector:      "vector",
	CreateTime:  "create_time",
	UpdateTime:  "update_time",
	KBUID:       "kb_uid",
	FileUID:     "file_uid",
	ContentType: "content_type",
	ChunkType:   "chunk_type",
}

// TableName returns the table name of the Embedding
func (EmbeddingModel) TableName() string {
	return EmbeddingTableName
}

// DeleteAndCreateEmbeddings inserts a set of new embeddings extracted from a
// file into the database. Previous embeddings might exist, which indicates that
// the file is being reprocessed. In that case, the existing embeddings are
// deleted.  A function is passed as an argument as a way to call external
// services (i.e., the vector database) within the upsert transaction.
func (r *repository) DeleteAndCreateEmbeddings(
	ctx context.Context,
	fileUID types.FileUIDType,
	embeddings []EmbeddingModel,
	externalServiceCall func([]EmbeddingModel) error,
) ([]EmbeddingModel, error) {

	logger, _ := logx.GetZapLogger(ctx)

	// Start a transaction
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Delete existing embeddings
		if err := tx.Where("kb_file_uid = ?", fileUID).Delete(&EmbeddingModel{}).Error; err != nil {
			return fmt.Errorf("deleting existing embeddings: %w", err)
		}

		if len(embeddings) == 0 {
			logger.Warn("no embeddings to upsert")
			return nil // return nil to commit the transaction (DELETE was successful)
		}

		// Insert new embeddings. This will update the UIDs in the embedding
		// slice.
		if err := tx.Create(&embeddings).Error; err != nil {
			return fmt.Errorf("creating embeddings: %w", err)
		}

		if externalServiceCall != nil {
			if err := externalServiceCall(embeddings); err != nil {
				return fmt.Errorf("calling external service: %w", err)
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("transaction failed: %w", err)
	}

	return embeddings, nil
}

// CreateEmbeddings inserts a batch of embeddings into the database without
// deleting existing ones. This is useful for batch processing where deletion
// has already been performed separately.
func (r *repository) CreateEmbeddings(
	ctx context.Context,
	embeddings []EmbeddingModel,
	externalServiceCall func([]EmbeddingModel) error,
) ([]EmbeddingModel, error) {

	logger, _ := logx.GetZapLogger(ctx)

	if len(embeddings) == 0 {
		logger.Warn("no embeddings to create")
		return embeddings, nil
	}

	// Start a transaction
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Insert new embeddings. This will update the UIDs in the embedding
		// slice.
		if err := tx.Create(&embeddings).Error; err != nil {
			return fmt.Errorf("creating embeddings: %w", err)
		}

		if externalServiceCall != nil {
			if err := externalServiceCall(embeddings); err != nil {
				return fmt.Errorf("calling external service: %w", err)
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("transaction failed: %w", err)
	}

	return embeddings, nil
}

// DeleteEmbeddingsByKBFileUID deletes all embeddings associated with a file
// (soft delete, respects the model's DeletedAt field if present).
func (r *repository) DeleteEmbeddingsByKBFileUID(ctx context.Context, kbFileUID types.FileUIDType) error {
	where := fmt.Sprintf("%s = ?", EmbeddingColumn.FileUID)
	return r.db.WithContext(ctx).Where(where, kbFileUID).Delete(&EmbeddingModel{}).Error
}

// HardDeleteEmbeddingsByKBUID deletes all the embeddings associated with a certain kbUID.
func (r *repository) HardDeleteEmbeddingsByKBUID(ctx context.Context, kbUID types.KBUIDType) error {
	where := fmt.Sprintf("%s = ?", EmbeddingColumn.KBUID)
	return r.db.WithContext(ctx).Where(where, kbUID).Unscoped().Delete(&EmbeddingModel{}).Error
}

// HardDeleteEmbeddingsByKBFileUID deletes all the embeddings associated with a certain kbFileUID.
func (r *repository) HardDeleteEmbeddingsByKBFileUID(ctx context.Context, kbFileUID types.FileUIDType) error {
	where := fmt.Sprintf("%s = ?", EmbeddingColumn.FileUID)
	return r.db.WithContext(ctx).Where(where, kbFileUID).Unscoped().Delete(&EmbeddingModel{}).Error
}

// ListEmbeddingsByKBFileUID fetches embeddings by their kbFileUID.
func (r *repository) ListEmbeddingsByKBFileUID(ctx context.Context, kbFileUID types.FileUIDType) ([]EmbeddingModel, error) {
	var embeddings []EmbeddingModel
	where := fmt.Sprintf("%s = ?", EmbeddingColumn.FileUID)
	if err := r.db.WithContext(ctx).Where(where, kbFileUID).Find(&embeddings).Error; err != nil {
		return nil, err
	}
	return embeddings, nil
}

// GetEmbeddingCountByKBUID returns the count of embeddings for a knowledge base
func (r *repository) GetEmbeddingCountByKBUID(ctx context.Context, kbUID types.KBUIDType) (int64, error) {
	var count int64
	err := r.db.WithContext(ctx).
		Table(EmbeddingTableName+" AS e").
		Joins("INNER JOIN "+FileTableName+" AS f ON e.file_uid = f.uid").
		Where("e.kb_uid = ?", kbUID).
		Where("f.delete_time IS NULL"). // Exclude soft-deleted files
		Count(&count).
		Error
	if err != nil {
		return 0, fmt.Errorf("counting embeddings for KB %s: %w", kbUID, err)
	}
	return count, nil
}

// UpdateEmbeddingTagsForFile updates tags in Milvus for all embeddings of a file
// This is used when file tags are updated to keep vector database in sync with file metadata
func (r *repository) UpdateEmbeddingTagsForFile(ctx context.Context, collectionID string, fileUID types.FileUIDType, tags []string) error {
	return r.UpdateEmbeddingTags(ctx, collectionID, fileUID, tags)
}
