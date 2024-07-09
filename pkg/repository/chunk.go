package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type TextChunkI interface {
	DeleteAndCreateChunks(ctx context.Context, sourceTable string, sourceUID uuid.UUID, chunks []TextChunk, externalServiceCall func(chunkUIDs []string) error) ([]TextChunk, error)
	DeleteChunksBySource(ctx context.Context, sourceTable string, sourceUID uuid.UUID) error
	DeleteChunksByUIDs(ctx context.Context, chunkUIDs []uuid.UUID) error
	GetTextChunksBySource(ctx context.Context, sourceTable string, sourceUID uuid.UUID) ([]TextChunk, error)
}

// currently, we use minio to store the chunk but in the future, we may just get the content from the source
// and segment it using start and end on the fly which is more storage efficient.
type TextChunk struct {
	UID         uuid.UUID `gorm:"column:uid;type:uuid;default:gen_random_uuid();primaryKey" json:"uid"`
	SourceUID   uuid.UUID `gorm:"column:source_uid;type:uuid;not null" json:"source_uid"`
	SourceTable string    `gorm:"column:source_table;size:255;not null" json:"source_table"`
	Start       int       `gorm:"column:start;not null" json:"start"`
	End         int       `gorm:"column:end;not null" json:"end"`
	// ContentDest is the destination path in minio
	ContentDest string     `gorm:"column:content_dest;size:255;not null" json:"content_dest"`
	Tokens      int        `gorm:"column:tokens;not null" json:"tokens"`
	Retrievable bool       `gorm:"column:retrievable;not null;default:true" json:"retrievable"`
	Order       int        `gorm:"column:order;not null" json:"order"`
	CreateTime  *time.Time `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime  *time.Time `gorm:"column:update_time;not null;default:CURRENT_TIMESTAMP" json:"update_time"`
}

type TextChunkColumns struct {
	UID         string
	SourceUID   string
	SourceTable string
	Start       string
	End         string
	ContentDest string
	Tokens      string
	Retrievable string
	Order       string
	CreateTime  string
	UpdateTime  string
}

var TextChunkColumn = TextChunkColumns{
	UID:         "uid",
	SourceUID:   "source_uid",
	SourceTable: "source_table",
	Start:       "start",
	End:         "end",
	ContentDest: "content_dest",
	Tokens:      "tokens",
	Retrievable: "retrievable",
	Order:       "order",
	CreateTime:  "create_time",
	UpdateTime:  "update_time",
}

// TableName returns the table name of the TextChunk
func (TextChunk) TableName() string {
	return "text_chunk"
}

// DeleteAndCreateChunks deletes all the chunks associated with
// a certain source table and sourceUID, then batch inserts the new chunks
// within a transaction.
func (r *Repository) DeleteAndCreateChunks(ctx context.Context, sourceTable string, sourceUID uuid.UUID, chunks []TextChunk, externalServiceCall func(chunkUIDs []string) error) ([]TextChunk, error) {
	// Start a transaction
	err := r.db.Transaction(func(tx *gorm.DB) error {
		// Delete existing chunks
		where := fmt.Sprintf("%s = ? AND %s = ?", TextChunkColumn.SourceTable, TextChunkColumn.SourceUID)
		if err := tx.WithContext(ctx).Where(where, sourceTable, sourceUID).Delete(&TextChunk{}).Error; err != nil {
			return err
		}

		// Batch insert new chunks
		if err := tx.WithContext(ctx).Create(&chunks).Error; err != nil {
			return err
		}

		// Call external service function
		var chunkUIDs []string
		for _, chunk := range chunks {
			chunkUIDs = append(chunkUIDs, chunk.UID.String())
		}
		if externalServiceCall != nil {
			if err := externalServiceCall(chunkUIDs); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return chunks, nil
}

// DeleteChunksBySource deletes all the chunks associated with a certain source table and sourceUID.
func (r *Repository) DeleteChunksBySource(ctx context.Context, sourceTable string, sourceUID uuid.UUID) error {
	where := fmt.Sprintf("%s = ? AND %s = ?", TextChunkColumn.SourceTable, TextChunkColumn.SourceUID)
	return r.db.WithContext(ctx).Where(where, sourceTable, sourceUID).Delete(&TextChunk{}).Error
}

// DeleteChunksByUIDs deletes all the chunks associated with a certain source table and sourceUID.
func (r *Repository) DeleteChunksByUIDs(ctx context.Context, chunkUIDs []uuid.UUID) error {
	where := fmt.Sprintf("%s IN (?)", TextChunkColumn.UID)
	return r.db.WithContext(ctx).Where(where, chunkUIDs).Delete(&TextChunk{}).Error
}

// GetTextChunksBySource returns the text chunks by source table and source UID
func (r *Repository) GetTextChunksBySource(ctx context.Context, sourceTable string, sourceUID uuid.UUID) ([]TextChunk, error) {
	var chunks []TextChunk
	where := fmt.Sprintf("%s = ? AND %s = ?", TextChunkColumn.SourceTable, TextChunkColumn.SourceUID)
	if err := r.db.WithContext(ctx).Where(where, sourceTable, sourceUID).Find(&chunks).Error; err != nil {
		return nil, err
	}
	return chunks, nil
}
