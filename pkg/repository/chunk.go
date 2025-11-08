package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/instill-ai/artifact-backend/pkg/types"

	logx "github.com/instill-ai/x/log"
)

const (
	// TextChunkTableName is the table name for text chunks
	TextChunkTableName = "chunk"
)

// TextChunk is the interface for the text chunk repository
type TextChunk interface {
	DeleteAndCreateTextChunks(
		_ context.Context,
		fileUID types.FileUIDType,
		textChunks []*TextChunkModel,
		externalServiceCall func(chunkUIDs []string) (destinations map[string]string, _ error),
	) ([]*TextChunkModel, error)

	// CreateTextChunks creates new text chunks without deletion
	// Note: Assumes old chunks have been deleted separately via DeleteOldTextChunksActivity
	CreateTextChunks(_ context.Context, textChunks []*TextChunkModel) error

	// HardDeleteTextChunksByKBUID deletes all the chunks associated with a certain kbUID.
	HardDeleteTextChunksByKBUID(_ context.Context, kbUID types.KBUIDType) error
	// HardDeleteTextChunksByKBFileUID deletes all the chunks associated with a certain kbFileUID.
	HardDeleteTextChunksByKBFileUID(_ context.Context, kbFileUID types.FileUIDType) error
	GetTextChunksBySource(_ context.Context, sourceTable string, sourceUID types.SourceUIDType) ([]TextChunkModel, error)
	GetTextChunksByUIDs(_ context.Context, chunkUIDs []types.TextChunkUIDType) ([]TextChunkModel, error)
	GetTotalTokensByListKBUIDs(_ context.Context, kbUIDs []types.KBUIDType) (map[types.KBUIDType]int, error)
	ListTextChunksByKBFileUID(_ context.Context, kbFileUID types.FileUIDType) ([]TextChunkModel, error)
	GetFilesTotalTokens(_ context.Context, sources map[types.FileUIDType]struct {
		SourceTable types.SourceTableType
		SourceUID   types.SourceUIDType
	}) (map[types.FileUIDType]int, error)
	// GetTotalTextChunksBySources
	GetTotalTextChunksBySources(_ context.Context, sources map[types.FileUIDType]struct {
		SourceTable types.SourceTableType
		SourceUID   types.SourceUIDType
	}) (map[types.FileUIDType]int, error)
	UpdateTextChunk(_ context.Context, chunkUID string, updates map[string]any) (*TextChunkModel, error)
	UpdateTextChunkDestinations(_ context.Context, destinations map[string]string) error
	GetChunkCountByKBUID(ctx context.Context, kbUID types.KBUIDType) (int64, error)
}

// TextChunkModel is the model for the text chunk table.
// Currently, we use minio to store the text chunks but in the future, we may just
// get the content from the source and segment it using start and end on the
// fly which is more storage efficient.
type TextChunkModel struct {
	UID types.TextChunkUIDType `gorm:"column:uid;type:uuid;default:gen_random_uuid();primaryKey" json:"uid"`
	// SourceUID is the UID of the source entity that the text chunk is associated
	// with. i.e. the UID of file or converted file etc. And SourceTable is the
	// table name of the source entity.
	SourceTable types.SourceTableType `gorm:"column:source_table;size:255;not null" json:"source_table"`
	SourceUID   types.SourceUIDType   `gorm:"column:source_uid;type:uuid;not null" json:"source_uid"`
	StartPos    int                   `gorm:"column:start_pos;not null" json:"start"`
	EndPos      int                   `gorm:"column:end_pos;not null" json:"end"`

	ReferenceJSON datatypes.JSON            `gorm:"column:reference;type:jsonb" json:"reference_json"`
	Reference     *types.TextChunkReference `gorm:"-" json:"chunk_reference"`

	// ContentDest is the destination path in minio
	ContentDest string     `gorm:"column:content_dest;size:255;not null" json:"content_dest"`
	Tokens      int        `gorm:"column:tokens;not null" json:"tokens"`
	Retrievable bool       `gorm:"column:retrievable;not null;default:true" json:"retrievable"`
	InOrder     int        `gorm:"column:in_order;not null" json:"order"`
	CreateTime  *time.Time `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime  *time.Time `gorm:"column:update_time;not null;default:CURRENT_TIMESTAMP" json:"update_time"`
	// KBUID is the knowledge base UID
	KBUID   types.KBUIDType   `gorm:"column:kb_uid;type:uuid" json:"kb_uid"`
	FileUID types.FileUIDType `gorm:"column:file_uid;type:uuid" json:"file_uid"`
	// ContentType stores the MIME type (always "text/markdown" for chunks)
	ContentType string `gorm:"column:content_type;size:255;not null" json:"content_type"`
	// ChunkType stores the chunk classification ("content", "summary", "augmented")
	ChunkType string `gorm:"column:chunk_type;size:255;not null" json:"chunk_type"`
}

// TableName overrides the default table name for GORM
func (TextChunkModel) TableName() string {
	return TextChunkTableName
}

// TextChunkColumns is the columns for the text chunk table
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
	KBUID       string
	FileUID     string
	ContentType string
	ChunkType   string
}

// TextChunkColumn is the column for the text chunk table
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
	KBUID:       "kb_uid",
	FileUID:     "file_uid",
	ContentType: "content_type",
	ChunkType:   "chunk_type",
}

// DeleteAndCreateTextChunks deletes all the text chunks associated with
// a specific source_uid (converted_file), then batch inserts the new text chunks
// within a transaction. This allows content and summary chunks to coexist since they
// reference different converted_file records (different source_uid values).
func (r *repository) DeleteAndCreateTextChunks(
	ctx context.Context,
	fileUID types.FileUIDType,
	textChunks []*TextChunkModel,
	externalServiceCall func(chunkUIDs []string) (destinations map[string]string, _ error),
) ([]*TextChunkModel, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Start a transaction
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Delete existing text chunks by source_uid (not file_uid)
		// This ensures we only delete chunks for the specific converted_file being updated
		// (e.g., content chunks or summary chunks), allowing them to coexist
		if len(textChunks) > 0 {
			sourceUID := textChunks[0].SourceUID
			sourceTable := textChunks[0].SourceTable
			err := tx.Where("source_uid = ? AND source_table = ?", sourceUID, sourceTable).Delete(&TextChunkModel{}).Error
			if err != nil {
				return fmt.Errorf("deleting existing text chunks for source_uid %s: %w", sourceUID, err)
			}
			logger.Info("Deleted existing text chunks",
				zap.String("source_uid", sourceUID.String()),
				zap.String("source_table", string(sourceTable)))
		}

		if len(textChunks) == 0 {
			logger.Warn("no text chunks to create")
			return nil // return nil to commit the transaction (DELETE was successful)
		}

		// Batch insert new text chunks
		if err := tx.Create(&textChunks).Error; err != nil {
			return fmt.Errorf("creating chunks: %w", err)
		}

		// Call external service function
		var chunkUIDs []string
		for _, textChunk := range textChunks {
			chunkUIDs = append(chunkUIDs, textChunk.UID.String())
		}
		if externalServiceCall != nil {
			chunkDestMap, err := externalServiceCall(chunkUIDs)
			if err != nil {
				return err
			}
			// update the content dest of each text chunk
			for _, textChunk := range textChunks {
				if dest, ok := chunkDestMap[textChunk.UID.String()]; ok {
					textChunk.ContentDest = dest
				}
			}
		}

		// Update the content dest of each text chunk
		if err := BatchUpdateContentDest(ctx, tx, textChunks); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return textChunks, nil
}

// CreateTextChunks creates new text chunks in the database without deletion
// This is used when old chunks have been deleted separately at the workflow level
func (r *repository) CreateTextChunks(ctx context.Context, textChunks []*TextChunkModel) error {
	if len(textChunks) == 0 {
		return nil
	}

	// Batch insert new text chunks
	if err := r.db.WithContext(ctx).Create(&textChunks).Error; err != nil {
		return fmt.Errorf("creating text chunks: %w", err)
	}

	return nil
}

// BatchUpdateContentDest updates the content dest of the text chunks
func BatchUpdateContentDest(ctx context.Context, tx *gorm.DB, textChunks []*TextChunkModel) error {
	if len(textChunks) == 0 {
		return nil
	}

	return tx.WithContext(ctx).Model(&TextChunkModel{}).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: TextChunkColumn.UID}},
			DoUpdates: clause.AssignmentColumns([]string{
				TextChunkColumn.SourceUID, TextChunkColumn.SourceTable, TextChunkColumn.Start, TextChunkColumn.End, TextChunkColumn.ContentDest,
				TextChunkColumn.Tokens, TextChunkColumn.Retrievable, TextChunkColumn.Order,
			}),
		}).
		Create(textChunks).Error
}

// GetTextChunksBySource returns the text chunks by source table and source UID
func (r *repository) GetTextChunksBySource(ctx context.Context, sourceTable string, sourceUID types.SourceUIDType) ([]TextChunkModel, error) {
	var textChunks []TextChunkModel
	where := fmt.Sprintf("%s = ? AND %s = ?", TextChunkColumn.SourceTable, TextChunkColumn.SourceUID)
	if err := r.db.WithContext(ctx).Where(where, sourceTable, sourceUID).Find(&textChunks).Error; err != nil {
		return nil, err
	}
	return textChunks, nil
}

// GetTotalTokensByListKBUIDs returns the total tokens of the text chunks by list of KBUIDs
func (r *repository) GetTotalTokensByListKBUIDs(ctx context.Context, kbUIDs []types.KBUIDType) (map[types.KBUIDType]int, error) {
	var totalTokens []struct {
		KBUID  types.KBUIDType `gorm:"kb_uid"`
		Tokens int             `gorm:"tokens"`
	}
	if err := r.db.WithContext(ctx).Model(&TextChunkModel{}).
		Select("kb_uid, SUM(tokens) as tokens").
		Where("kb_uid IN (?)", kbUIDs).
		Group("kb_uid").
		Scan(&totalTokens).Error; err != nil {
		return nil, err
	}

	totalTokensMap := make(map[types.KBUIDType]int)
	for _, tt := range totalTokens {
		totalTokensMap[tt.KBUID] = tt.Tokens
	}
	return totalTokensMap, nil
}

// GetFilesTotalTokens returns the total tokens from the usage_metadata field in the file table
// According to Gemini API docs: https://ai.google.dev/gemini-api/docs/tokens?lang=python
// total_tokens = content.total_token_count + summary.total_token_count
// This replaces the old implementation that summed chunk tokens (which were estimated, not actual)
func (r *repository) GetFilesTotalTokens(ctx context.Context, sources map[types.FileUIDType]struct {
	SourceTable types.SourceTableType
	SourceUID   types.SourceUIDType
}) (map[types.FileUIDType]int, error) {
	result := make(map[types.FileUIDType]int)
	if len(sources) == 0 {
		return result, nil
	}

	// Extract file UIDs from sources
	fileUIDs := make([]types.FileUIDType, 0, len(sources))
	for fileUID := range sources {
		fileUIDs = append(fileUIDs, fileUID)
	}

	// Query files to get usage_metadata
	var files []KnowledgeBaseFileModel
	err := r.db.WithContext(ctx).
		Select("uid, usage_metadata").
		Where("uid IN ?", fileUIDs).
		Find(&files).Error

	if err != nil {
		return nil, err
	}

	// Parse usage_metadata and calculate total tokens for each file
	for _, file := range files {
		// Unmarshal usage_metadata
		if err := file.UsageMetadataUnmarshalFunc(); err != nil {
			// If unmarshal fails, skip this file (no usage metadata available yet)
			continue
		}

		if file.UsageMetadataUnmarshal == nil {
			// No usage metadata, skip
			continue
		}

		// Extract total_token_count from content and summary
		// According to Gemini API docs: https://ai.google.dev/gemini-api/docs/tokens?lang=python
		// The usage metadata is stored directly (not nested) in the JSONB field
		contentTokens := 0
		summaryTokens := 0

		// Get content total tokens (stored as content.totalTokenCount)
		if totalTokenCount, ok := file.UsageMetadataUnmarshal.Content["totalTokenCount"].(float64); ok {
			contentTokens = int(totalTokenCount)
		} else if totalTokenCount, ok := file.UsageMetadataUnmarshal.Content["totalTokenCount"].(int); ok {
			contentTokens = totalTokenCount
		}

		// Get summary total tokens (stored as summary.totalTokenCount)
		if totalTokenCount, ok := file.UsageMetadataUnmarshal.Summary["totalTokenCount"].(float64); ok {
			summaryTokens = int(totalTokenCount)
		} else if totalTokenCount, ok := file.UsageMetadataUnmarshal.Summary["totalTokenCount"].(int); ok {
			summaryTokens = totalTokenCount
		}

		// Calculate total tokens (content + summary)
		totalTokens := contentTokens + summaryTokens

		// Store in result map
		result[file.UID] = totalTokens
	}

	return result, nil
}

// GetTotalTextChunksBySources returns the count of the text chunks by source table and source UID
func (r *repository) GetTotalTextChunksBySources(ctx context.Context, sources map[types.FileUIDType]struct {
	SourceTable types.SourceTableType
	SourceUID   types.SourceUIDType
}) (map[types.FileUIDType]int, error) {
	result := make(map[types.FileUIDType]int)
	if len(sources) == 0 {
		return result, nil
	}

	// Prepare the conditions for the query
	var conditions []string
	var values []any

	for _, source := range sources {
		conditions = append(conditions, "(source_table = ? AND source_uid = ?)")
		values = append(values, source.SourceTable, source.SourceUID)
	}

	// Combine all conditions
	whereClause := strings.Join(conditions, " OR ")

	// Query to get total tokens grouped by source_table and source_uid
	var tokenSums []struct {
		SourceTable types.SourceTableType `gorm:"column:source_table"`
		SourceUID   types.SourceUIDType   `gorm:"column:source_uid"`
		TotalTokens int                   `gorm:"column:total_tokens"`
	}

	err := r.db.WithContext(ctx).Model(&TextChunkModel{}).
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

// UpdateTextChunk updates a specific text chunk identified by textChunkUID with the provided updates map.
func (r *repository) UpdateTextChunk(ctx context.Context, textChunkUID string, updates map[string]any) (*TextChunkModel, error) {
	// Fetch the existing text chunk to ensure it exists
	var existingTextChunk TextChunkModel
	where := fmt.Sprintf("%s = ?", TextChunkColumn.UID)
	if err := r.db.WithContext(ctx).Where(where, textChunkUID).First(&existingTextChunk).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("text chunk UID not found: %v. err: %w", textChunkUID, gorm.ErrRecordNotFound)
		}
		return nil, err
	}

	// Update the specific fields of the text chunk
	if err := r.db.WithContext(ctx).Model(&existingTextChunk).Updates(updates).Error; err != nil {
		return nil, err
	}

	// Fetch the updated text chunk
	var updatedTextChunk TextChunkModel
	if err := r.db.WithContext(ctx).Where(where, textChunkUID).Take(&updatedTextChunk).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("text chunk UID not found after update: %v. err: %w", textChunkUID, gorm.ErrRecordNotFound)
		}
		return nil, err
	}

	return &updatedTextChunk, nil
}

func (r *repository) GetTextChunksByUIDs(ctx context.Context, textChunkUIDs []types.TextChunkUIDType) ([]TextChunkModel, error) {
	var textChunks []TextChunkModel
	where := fmt.Sprintf("%s IN (?)", TextChunkColumn.UID)
	if err := r.db.WithContext(ctx).Where(where, textChunkUIDs).Find(&textChunks).Error; err != nil {
		return nil, err
	}
	return textChunks, nil
}

// HardDeleteTextChunksByKBUID deletes all the text chunks associated with a certain kbUID.
func (r *repository) HardDeleteTextChunksByKBUID(ctx context.Context, kbUID types.KBUIDType) error {
	where := fmt.Sprintf("%s = ?", TextChunkColumn.KBUID)
	return r.db.WithContext(ctx).Where(where, kbUID).Unscoped().Delete(&TextChunkModel{}).Error
}

// ListTextChunksByKBFileUID returns the list of text chunks by kbFileUID
func (r *repository) ListTextChunksByKBFileUID(ctx context.Context, kbFileUID types.FileUIDType) ([]TextChunkModel, error) {
	var textChunks []TextChunkModel
	where := fmt.Sprintf("%s = ?", TextChunkColumn.FileUID)
	if err := r.db.WithContext(ctx).Where(where, kbFileUID).Find(&textChunks).Error; err != nil {
		return nil, err
	}
	return textChunks, nil
}

// HardDeleteTextChunksByKBFileUID deletes all the text chunks associated with a certain kbFileUID.
func (r *repository) HardDeleteTextChunksByKBFileUID(ctx context.Context, kbFileUID types.FileUIDType) error {
	where := fmt.Sprintf("%s = ?", TextChunkColumn.FileUID)
	return r.db.WithContext(ctx).Where(where, kbFileUID).Unscoped().Delete(&TextChunkModel{}).Error
}

// UpdateTextChunkDestinations updates the content_dest field for multiple text chunks
func (r *repository) UpdateTextChunkDestinations(ctx context.Context, destinations map[string]string) error {
	if len(destinations) == 0 {
		return nil
	}

	// Extract text chunk UIDs for validation
	textChunkUIDs := make([]string, 0, len(destinations))
	for uid := range destinations {
		textChunkUIDs = append(textChunkUIDs, uid)
	}

	// Use a transaction to update all destinations atomically
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Validate that all text chunk UIDs exist
		var existingTextChunks []TextChunkModel
		if err := tx.Select("uid").Where("uid IN ?", textChunkUIDs).Find(&existingTextChunks).Error; err != nil {
			return fmt.Errorf("failed to query existing text chunks: %w", err)
		}

		// Check if all provided text chunk UIDs exist
		if len(existingTextChunks) != len(destinations) {
			existingUIDs := make(map[string]bool)
			for _, textChunk := range existingTextChunks {
				existingUIDs[textChunk.UID.String()] = true
			}

			var missingUIDs []string
			for uid := range destinations {
				if !existingUIDs[uid] {
					missingUIDs = append(missingUIDs, uid)
				}
			}
			return fmt.Errorf("text chunk UIDs not found: %v", missingUIDs)
		}

		// Build a CASE WHEN statement for bulk update
		// UPDATE chunk SET content_dest = CASE
		//   WHEN uid = 'uid1' THEN 'dest1'
		//   WHEN uid = 'uid2' THEN 'dest2'
		//   ...
		// END WHERE uid IN ('uid1', 'uid2', ...)

		var caseClauses []string
		var args []any
		for uid, dest := range destinations {
			caseClauses = append(caseClauses, "WHEN uid = ? THEN ?")
			args = append(args, uid, dest)
		}

		caseSQL := fmt.Sprintf("CASE %s END", strings.Join(caseClauses, " "))

		if err := tx.Model(&TextChunkModel{}).
			Where("uid IN ?", textChunkUIDs).
			Update("content_dest", gorm.Expr(caseSQL, args...)).Error; err != nil {
			return fmt.Errorf("failed to update text chunk destinations: %w", err)
		}

		return nil
	})
}

// GORM hooks
func (tc *TextChunkModel) fillReferenceJSON() (err error) {
	if tc.Reference == nil {
		return nil
	}

	tc.ReferenceJSON, err = json.Marshal(tc.Reference)
	return err
}

// BeforeCreate is a GORM hook that runs before creating a text chunk record
func (tc *TextChunkModel) BeforeCreate(tx *gorm.DB) error {
	return tc.fillReferenceJSON()
}

// BeforeSave is a GORM hook that runs before saving a text chunk record
func (tc *TextChunkModel) BeforeSave(tx *gorm.DB) error {
	return tc.fillReferenceJSON()
}

// BeforeUpdate is a GORM hook that runs before updating a text chunk record
func (tc *TextChunkModel) BeforeUpdate(tx *gorm.DB) error {
	return tc.fillReferenceJSON()
}

// AfterFind is a GORM hook that runs after finding a text chunk record
func (tc *TextChunkModel) AfterFind(tx *gorm.DB) error {
	if tc.ReferenceJSON == nil {
		return nil
	}

	if tc.Reference == nil {
		tc.Reference = new(types.TextChunkReference)
	}

	return json.Unmarshal(tc.ReferenceJSON, tc.Reference)
}

// GetChunkCountByKBUID returns the count of text chunks for a knowledge base
func (r *repository) GetChunkCountByKBUID(ctx context.Context, kbUID types.KBUIDType) (int64, error) {
	var count int64
	err := r.db.WithContext(ctx).
		Table(TextChunkTableName+" AS tc").
		Joins("INNER JOIN "+KnowledgeBaseFileTableName+" AS f ON tc.file_uid = f.uid").
		Where("tc.kb_uid = ?", kbUID).
		Where("f.delete_time IS NULL"). // Exclude soft-deleted files
		Count(&count).
		Error
	if err != nil {
		return 0, fmt.Errorf("counting text chunks for KB %s: %w", kbUID, err)
	}
	return count, nil
}
