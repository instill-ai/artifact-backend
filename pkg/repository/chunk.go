package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type TextChunkI interface {
	TextChunkTableName() string
	DeleteAndCreateChunks(ctx context.Context, sourceTable string, sourceUID uuid.UUID, chunks []*TextChunk, externalServiceCall func(chunkUIDs []string) (map[string]any, error)) ([]*TextChunk, error)
	DeleteChunksBySource(ctx context.Context, sourceTable string, sourceUID uuid.UUID) error
	DeleteChunksByUIDs(ctx context.Context, chunkUIDs []uuid.UUID) error
	// HardDeleteChunksByKbUID deletes all the chunks associated with a certain kbUID.
	HardDeleteChunksByKbUID(ctx context.Context, kbUID uuid.UUID) error
	// HardDeleteChunksByKbFileUID deletes all the chunks associated with a certain kbFileUID.
	HardDeleteChunksByKbFileUID(ctx context.Context, kbFileUID uuid.UUID) error
	GetTextChunksBySource(ctx context.Context, sourceTable string, sourceUID uuid.UUID) ([]TextChunk, error)
	GetChunksByUIDs(ctx context.Context, chunkUIDs []uuid.UUID) ([]TextChunk, error)
	GetTotalTokensByListKBUIDs(ctx context.Context, kbUIDs []uuid.UUID) (map[uuid.UUID]int, error)
	ListChunksByKbFileUID(ctx context.Context, kbFileUID uuid.UUID) ([]TextChunk, error)
	GetFilesTotalTokens(ctx context.Context, sources map[FileUID]struct {
		SourceTable SourceTable
		SourceUID   SourceUID
	}) (map[FileUID]int, error)
	// GetTotalChunksBySources
	GetTotalChunksBySources(ctx context.Context, sources map[FileUID]struct {
		SourceTable SourceTable
		SourceUID   SourceUID
	}) (map[FileUID]int, error)
	UpdateChunk(ctx context.Context, chunkUID string, updates map[string]interface{}) (*TextChunk, error)
}

// currently, we use minio to store the chunk but in the future, we may just get the content from the source
// and segment it using start and end on the fly which is more storage efficient.
type TextChunk struct {
	UID uuid.UUID `gorm:"column:uid;type:uuid;default:gen_random_uuid();primaryKey" json:"uid"`
	// SourceUID is the UID of the source entity that the chunk is associated with. i.e. the UID of file or converted file etc.
	// And SourceTable is the table name of the source entity.
	SourceTable string    `gorm:"column:source_table;size:255;not null" json:"source_table"`
	SourceUID   uuid.UUID `gorm:"column:source_uid;type:uuid;not null" json:"source_uid"`
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
	KbUID     uuid.UUID `gorm:"column:kb_uid;type:uuid" json:"kb_uid"`
	KbFileUID uuid.UUID `gorm:"column:kb_file_uid;type:uuid" json:"kb_file_uid"`
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
	KbUID       string
	KbFileUID   string
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
	KbUID:       "kb_uid",
	KbFileUID:   "kb_file_uid",
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

type FileUID = uuid.UUID
type SourceTable = string
type SourceUID = uuid.UUID

// GetFilesTotalTokens returns the total tokens of the chunks by list of source table and source UID
func (r *Repository) GetFilesTotalTokens(ctx context.Context, sources map[FileUID]struct {
	SourceTable SourceTable
	SourceUID   SourceUID
}) (map[FileUID]int, error) {
	result := make(map[FileUID]int)

	// Prepare the conditions for the query
	var conditions []string
	var values []interface{}

	for _, source := range sources {
		conditions = append(conditions, "(source_table = ? AND source_uid = ?)")
		values = append(values, source.SourceTable, source.SourceUID)
	}

	// Combine all conditions
	whereClause := strings.Join(conditions, " OR ")

	// Query to get total tokens grouped by source_table and source_uid
	var tokenSums []struct {
		SourceTable string    `gorm:"column:source_table"`
		SourceUID   uuid.UUID `gorm:"column:source_uid"`
		TotalTokens int       `gorm:"column:total_tokens"`
	}

	err := r.db.WithContext(ctx).Model(&TextChunk{}).
		Select("source_table, source_uid, COALESCE(SUM(tokens), 0) as total_tokens").
		Where(whereClause, values...).
		Group("source_table, source_uid").
		Find(&tokenSums).Error

	if err != nil {
		return nil, err
	}

	// Populate the result map
	for _, sum := range tokenSums {
		for fileUID, source := range sources {
			if source.SourceTable == sum.SourceTable && source.SourceUID == sum.SourceUID {
				result[fileUID] = sum.TotalTokens
				break
			}
		}
	}

	return result, nil
}

// GetTotalChunksBySources returns the count of the chunks by source table and source UID
func (r *Repository) GetTotalChunksBySources(ctx context.Context, sources map[FileUID]struct {
	SourceTable SourceTable
	SourceUID   SourceUID
}) (map[FileUID]int, error) {
	result := make(map[FileUID]int)

	// Prepare the conditions for the query
	var conditions []string
	var values []interface{}

	for _, source := range sources {
		conditions = append(conditions, "(source_table = ? AND source_uid = ?)")
		values = append(values, source.SourceTable, source.SourceUID)
	}

	// Combine all conditions
	whereClause := strings.Join(conditions, " OR ")

	// Query to get total tokens grouped by source_table and source_uid
	var tokenSums []struct {
		SourceTable SourceTable `gorm:"column:source_table"`
		SourceUID   SourceUID   `gorm:"column:source_uid"`
		TotalTokens int         `gorm:"column:total_tokens"`
	}

	err := r.db.WithContext(ctx).Model(&TextChunk{}).
		Select("source_table, source_uid, COUNT(*) as total_tokens").
		Where(whereClause, values...).
		Group("source_table, source_uid").
		Find(&tokenSums).Error

	if err != nil {
		return nil, err
	}

	// Populate the result map
	for _, sum := range tokenSums {
		for fileUID, source := range sources {
			if source.SourceTable == sum.SourceTable && source.SourceUID == sum.SourceUID {
				result[fileUID] = sum.TotalTokens
				break
			}
		}
	}

	return result, nil
}

// UpdateChunk updates a specific chunk identified by chunkUID with the provided updates map.
func (r *Repository) UpdateChunk(ctx context.Context, chunkUID string, updates map[string]interface{}) (*TextChunk, error) {
	// Fetch the existing chunk to ensure it exists
	var existingChunk TextChunk
	where := fmt.Sprintf("%s = ?", TextChunkColumn.UID)
	if err := r.db.WithContext(ctx).Where(where, chunkUID).First(&existingChunk).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("chunk UID not found: %v. err: %w", chunkUID, gorm.ErrRecordNotFound)
		}
		return nil, err
	}

	// Update the specific fields of the chunk
	if err := r.db.WithContext(ctx).Model(&existingChunk).Updates(updates).Error; err != nil {
		return nil, err
	}

	// Fetch the updated chunk
	var updatedChunk TextChunk
	if err := r.db.WithContext(ctx).Where(where, chunkUID).Take(&updatedChunk).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("chunk UID not found after update: %v. err: %w", chunkUID, gorm.ErrRecordNotFound)
		}
		return nil, err
	}

	return &updatedChunk, nil
}

func (r *Repository) GetChunksByUIDs(ctx context.Context, chunkUIDs []uuid.UUID) ([]TextChunk, error) {
	var chunks []TextChunk
	where := fmt.Sprintf("%s IN (?)", TextChunkColumn.UID)
	if err := r.db.WithContext(ctx).Where(where, chunkUIDs).Find(&chunks).Error; err != nil {
		return nil, err
	}
	return chunks, nil
}

// HardDeleteChunksByKbUID deletes all the chunks associated with a certain kbUID.
func (r *Repository) HardDeleteChunksByKbUID(ctx context.Context, kbUID uuid.UUID) error {
	where := fmt.Sprintf("%s = ?", TextChunkColumn.KbUID)
	return r.db.WithContext(ctx).Where(where, kbUID).Unscoped().Delete(&TextChunk{}).Error
}

// ListChunksByKbFileUID returns the list of chunks by kbFileUID
func (r *Repository) ListChunksByKbFileUID(ctx context.Context, kbFileUID uuid.UUID) ([]TextChunk, error) {
	var chunks []TextChunk
	where := fmt.Sprintf("%s = ?", TextChunkColumn.KbFileUID)
	if err := r.db.WithContext(ctx).Where(where, kbFileUID).Find(&chunks).Error; err != nil {
		return nil, err
	}
	return chunks, nil
}

// HardDeleteChunksByKbFileUID deletes all the chunks associated with a certain kbFileUID.
func (r *Repository) HardDeleteChunksByKbFileUID(ctx context.Context, kbFileUID uuid.UUID) error {
	where := fmt.Sprintf("%s = ?", TextChunkColumn.KbFileUID)
	return r.db.WithContext(ctx).Where(where, kbFileUID).Unscoped().Delete(&TextChunk{}).Error
}
