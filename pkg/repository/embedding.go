package repository

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"gorm.io/gorm"

	logx "github.com/instill-ai/x/log"
)

type EmbeddingI interface {
	DeleteAndCreateEmbeddings(_ context.Context, fileUID uuid.UUID, embeddings []Embedding, externalServiceCall func([]Embedding) error) ([]Embedding, error)
	HardDeleteEmbeddingsByKbUID(_ context.Context, kbUID uuid.UUID) error
	HardDeleteEmbeddingsByKbFileUID(_ context.Context, kbFileUID uuid.UUID) error
	ListEmbeddingsByKbFileUID(_ context.Context, kbFileUID uuid.UUID) ([]Embedding, error)
}
type Embedding struct {
	UID uuid.UUID `gorm:"column:uid;type:uuid;default:gen_random_uuid();primaryKey" json:"uid"`
	// SourceUID is the UID of the source entity that the embedding is associated with. i.e. the UID of the chunk, file, etc.
	// And SourceTable is the table name of the source entity.
	SourceTable string     `gorm:"column:source_table;size:255;not null" json:"source_table"`
	SourceUID   uuid.UUID  `gorm:"column:source_uid;type:uuid;not null" json:"source_uid"`
	Vector      Vector     `gorm:"column:vector;type:jsonb;not null" json:"vector"`
	CreateTime  *time.Time `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime  *time.Time `gorm:"column:update_time;not null;default:CURRENT_TIMESTAMP" json:"update_time"`
	KbUID       uuid.UUID  `gorm:"column:kb_uid;type:uuid;not null" json:"kb_uid"`
	KbFileUID   uuid.UUID  `gorm:"column:kb_file_uid;type:uuid;not null" json:"kb_file_uid"`
	FileType    string     `gorm:"column:file_type;size:255;not null" json:"file_type"`
	ContentType string     `gorm:"column:content_type;size:255;not null" json:"content_type"`
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

type EmbeddingColumns struct {
	UID         string
	SourceUID   string
	SourceTable string
	Vector      string
	CreateTime  string
	UpdateTime  string
	KbUID       string
	KbFileUID   string
	FileType    string
	ContentType string
}

var EmbeddingColumn = EmbeddingColumns{
	UID:         "uid",
	SourceUID:   "source_uid",
	SourceTable: "source_table",
	Vector:      "vector",
	CreateTime:  "create_time",
	UpdateTime:  "update_time",
	KbUID:       "kb_uid",
	KbFileUID:   "kb_file_uid",
	FileType:    "file_type",
	ContentType: "content_type",
}

// TableName returns the table name of the Embedding
func (Embedding) TableName() string {
	return "embedding"
}

// DeleteAndCreateEmbeddings inserts a set of new embeddings extracted from a
// file into the database. Previous embeddings might exist, which indicates that
// the file is being reprocessed. In that case, the existing embeddings are
// deleted.  A function is passed as an argument as a way to call external
// services (i.e., the vector database) within the upsert transaction.
func (r *Repository) DeleteAndCreateEmbeddings(
	ctx context.Context,
	fileUID uuid.UUID,
	embeddings []Embedding,
	externalServiceCall func([]Embedding) error,
) ([]Embedding, error) {

	logger, _ := logx.GetZapLogger(ctx)

	// Start a transaction
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Delete existing embeddings
		if err := tx.Where("kb_file_uid = ?", fileUID).Delete(&Embedding{}).Error; err != nil {
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
