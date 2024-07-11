package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type TextChunkI interface {
	TextChunkTableName() string
	DeleteAndCreateChunks(ctx context.Context, sourceTable string, sourceUID uuid.UUID, chunks []*TextChunk, externalServiceCall func(chunkUIDs []string) (map[string]any, error)) ([]*TextChunk, error)
	DeleteChunksBySource(ctx context.Context, sourceTable string, sourceUID uuid.UUID) error
	DeleteChunksByUIDs(ctx context.Context, chunkUIDs []uuid.UUID) error
	GetTextChunksBySource(ctx context.Context, sourceTable string, sourceUID uuid.UUID) ([]TextChunk, error)
	GetTotalTokensByListKBUIDs(ctx context.Context, kbUIDs []uuid.UUID) (map[uuid.UUID]int, error)
}

// currently, we use minio to store the chunk but in the future, we may just get the content from the source
// and segment it using start and end on the fly which is more storage efficient.
type TextChunk struct {
	UID         uuid.UUID `gorm:"column:uid;type:uuid;default:gen_random_uuid();primaryKey" json:"uid"`
	SourceUID   uuid.UUID `gorm:"column:source_uid;type:uuid;not null" json:"source_uid"`
	SourceTable string    `gorm:"column:source_table;size:255;not null" json:"source_table"`
	StartPos    int       `gorm:"column:start_pos;not null" json:"start"`
	EndPos      int       `gorm:"column:end_pos;not null" json:"end"`
	// ContentDest is the destination path in minio
	ContentDest string     `gorm:"column:content_dest;size:255;not null" json:"content_dest"`
	Tokens      int        `gorm:"column:tokens;not null" json:"tokens"`
	Retrievable bool       `gorm:"column:retrievable;not null;default:true" json:"retrievable"`
	InOrder     int        `gorm:"column:in_order;not null" json:"order"`
	CreateTime  *time.Time `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime  *time.Time `gorm:"column:update_time;not null;default:CURRENT_TIMESTAMP" json:"update_time"`
	// KbUID is the knowledge base UID
	KbUID uuid.UUID `gorm:"column:kb_uid;type:uuid" json:"kb_uid"`
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
	Start:       "start_pos",
	End:         "end_pos",
	ContentDest: "content_dest",
	Tokens:      "tokens",
	Retrievable: "retrievable",
	Order:       "in_order",
	CreateTime:  "create_time",
	UpdateTime:  "update_time",
}

// TableName returns the table name of the TextChunk
func (r *Repository) TextChunkTableName() string {
	return "text_chunk"
}

// DeleteAndCreateChunks deletes all the chunks associated with
// a certain source table and sourceUID, then batch inserts the new chunks
// within a transaction.
func (r *Repository) DeleteAndCreateChunks(ctx context.Context, sourceTable string, sourceUID uuid.UUID, chunks []*TextChunk, externalServiceCall func(chunkUIDs []string) (map[string]any, error)) ([]*TextChunk, error) {
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
			if chunkDestMap, err := externalServiceCall(chunkUIDs); err != nil {
				return err
			} else {
				// update the content dest of each chunk
				for _, chunk := range chunks {
					if dest, ok := chunkDestMap[chunk.UID.String()]; ok {
						if data, ok := dest.(string); ok {
							chunk.ContentDest = data
						}
					}
				}
			}
		}

		// Update the content dest of each chunk
		if err := BatchUpdateContentDest(ctx, tx, chunks); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return chunks, nil
}

// Batch update function
func BatchUpdateContentDest(ctx context.Context, tx *gorm.DB, chunks []*TextChunk) error {
	if len(chunks) == 0 {
		return nil
	}

	return tx.WithContext(ctx).Model(&TextChunk{}).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: TextChunkColumn.UID}},
			DoUpdates: clause.AssignmentColumns([]string{
				TextChunkColumn.SourceUID, TextChunkColumn.SourceTable, TextChunkColumn.Start, TextChunkColumn.End, TextChunkColumn.ContentDest,
				TextChunkColumn.Tokens, TextChunkColumn.Retrievable, TextChunkColumn.Order,
			}),
		}).
		Create(chunks).Error
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

// GetTotalTokensByListKBUIDs returns the total tokens of the chunks by list of KBUIDs
func (r *Repository) GetTotalTokensByListKBUIDs(ctx context.Context, kbUIDs []uuid.UUID) (map[uuid.UUID]int, error) {
	var totalTokens []struct {
		KbUID  uuid.UUID `gorm:"kb_uid"`
		Tokens int       `gorm:"tokens"`
	}
	if err := r.db.WithContext(ctx).Model(&TextChunk{}).
		Select("kb_uid, SUM(tokens) as tokens").
		Where("kb_uid IN (?)", kbUIDs).
		Group("kb_uid").
		Scan(&totalTokens).Error; err != nil {
		return nil, err
	}

	totalTokensMap := make(map[uuid.UUID]int)
	for _, tt := range totalTokens {
		totalTokensMap[tt.KbUID] = tt.Tokens
	}
	return totalTokensMap, nil
}
