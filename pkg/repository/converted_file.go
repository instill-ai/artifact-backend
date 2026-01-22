package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"

	"github.com/instill-ai/artifact-backend/pkg/types"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

const (
	// ConvertedFileTableName is the table name for converted files
	ConvertedFileTableName = "converted_file"
)

// ConvertedFile is the interface for the converted file repository
type ConvertedFile interface {
	CreateConvertedFileWithDestination(ctx context.Context, cf ConvertedFileModel) (*ConvertedFileModel, error)
	UpdateConvertedFile(ctx context.Context, uid types.ConvertedFileUIDType, update map[string]any) error
	DeleteConvertedFile(ctx context.Context, uid types.ConvertedFileUIDType) error
	DeleteAllConvertedFilesInKb(ctx context.Context, kbUID types.KBUIDType) error
	HardDeleteConvertedFileByFileUID(ctx context.Context, fileUID types.FileUIDType) error
	GetConvertedFileByFileUID(ctx context.Context, fileUID types.FileUIDType) (*ConvertedFileModel, error)
	GetConvertedFileByFileUIDAndType(ctx context.Context, fileUID types.FileUIDType, convertedType artifactpb.ConvertedFileType) (*ConvertedFileModel, error)
	GetAllConvertedFilesByFileUID(ctx context.Context, fileUID types.FileUIDType) ([]ConvertedFileModel, error)
	GetConvertedFileCountByKBUID(ctx context.Context, kbUID types.KBUIDType) (int64, error)
}

// ConvertedFileModel is the model for the converted file
type ConvertedFileModel struct {
	UID              types.ConvertedFileUIDType `gorm:"column:uid;type:uuid;default:gen_random_uuid();primaryKey" json:"uid"`
	KnowledgeBaseUID types.KnowledgeBaseUIDType `gorm:"column:kb_uid;type:uuid;not null" json:"kb_uid"`
	// FileUID is the original file UID in knowledge base file table
	FileUID types.FileUIDType `gorm:"column:file_uid;type:uuid;not null" json:"file_uid"`
	// ContentType stores the MIME type (always "text/markdown" for converted markdown files)
	ContentType string `gorm:"column:content_type;size:100;not null" json:"content_type"`
	// ConvertedType specifies the purpose of this converted file (content or summary)
	ConvertedType string `gorm:"column:converted_type;size:50;not null;default:content" json:"converted_type"`
	// StoragePath is the path in MinIO bucket
	StoragePath string `gorm:"column:storage_path;size:255;not null" json:"storage_path"`

	PositionDataJSON datatypes.JSON      `gorm:"column:position_data;type:jsonb" json:"position_data_json"`
	PositionData     *types.PositionData `gorm:"-" json:"position_data"`

	CreateTime *time.Time `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime *time.Time `gorm:"column:update_time;not null;default:CURRENT_TIMESTAMP;autoUpdateTime" json:"update_time"`
}

// TableName overrides the default table name for GORM
func (ConvertedFileModel) TableName() string {
	return ConvertedFileTableName
}

// ConvertedFileColumns is the columns for the converted file model
type ConvertedFileColumns struct {
	UID           string
	KBUID         string
	FileUID       string
	ContentType   string
	ConvertedType string
	StoragePath   string
	CreateTime    string
	UpdateTime    string
}

// ConvertedFileColumn is the column for the converted file model
var ConvertedFileColumn = ConvertedFileColumns{
	UID:           "uid",
	KBUID:         "kb_uid",
	FileUID:       "file_uid",
	ContentType:   "content_type",
	ConvertedType: "converted_type",
	StoragePath:   "storage_path",
	CreateTime:    "create_time",
	UpdateTime:    "update_time",
}

// CreateConvertedFileWithDestination creates a converted file record with a known destination.
// This method properly decouples database operations from external storage operations.
// Note: Old converted files should be cleaned up by DeleteOldConvertedFilesActivity before calling this.
func (r *repository) CreateConvertedFileWithDestination(ctx context.Context, cf ConvertedFileModel) (*ConvertedFileModel, error) {
	// Validate required fields before attempting to persist
	if cf.FileUID.IsNil() {
		return nil, fmt.Errorf("file_uid is required")
	}
	if cf.KnowledgeBaseUID.IsNil() {
		return nil, fmt.Errorf("knowledge_base_uid is required")
	}
	if cf.StoragePath == "" {
		return nil, fmt.Errorf("storage_path is required and cannot be empty")
	}
	if cf.ContentType == "" {
		return nil, fmt.Errorf("content_type is required and cannot be empty")
	}
	if cf.ConvertedType == "" {
		return nil, fmt.Errorf("converted_type is required and cannot be empty")
	}

	err := r.db.Transaction(func(tx *gorm.DB) error {
		// Note: Cleanup of old converted files is handled by DeleteOldConvertedFilesActivity in the workflow
		// This ensures all old files (content + summary) are deleted before new ones are created,
		// preventing race conditions and allowing content and summary files to coexist

		// Create the new ConvertedFileModel with the provided destination
		if err := tx.Create(&cf).Error; err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	return &cf, nil
}

// GetConvertedFileByFileUID returns the converted file by file UID
// Note: If multiple converted files exist for a file (e.g., content + summary),
// this returns the first one found. Use GetConvertedFileByFileUIDAndType for explicit queries.
func (r *repository) GetConvertedFileByFileUID(ctx context.Context, fileUID types.FileUIDType) (*ConvertedFileModel, error) {
	var cf ConvertedFileModel
	where := fmt.Sprintf("%s = ?", ConvertedFileColumn.FileUID)
	if err := r.db.WithContext(ctx).Where(where, fileUID).First(&cf).Error; err != nil {
		return nil, err
	}
	return &cf, nil
}

// GetConvertedFileByFileUIDAndType returns the converted file by file UID and type
// This is the preferred method for retrieving specific converted files (content vs summary)
func (r *repository) GetConvertedFileByFileUIDAndType(ctx context.Context, fileUID types.FileUIDType, convertedType artifactpb.ConvertedFileType) (*ConvertedFileModel, error) {
	var cf ConvertedFileModel
	where := fmt.Sprintf("%s = ? AND %s = ?", ConvertedFileColumn.FileUID, ConvertedFileColumn.ConvertedType)
	if err := r.db.WithContext(ctx).Where(where, fileUID, convertedType.String()).First(&cf).Error; err != nil {
		return nil, err
	}
	return &cf, nil
}

// GetAllConvertedFilesByFileUID returns all converted files for a given file UID
// This is useful for files that have multiple converted versions (e.g., content + summary)
func (r *repository) GetAllConvertedFilesByFileUID(ctx context.Context, fileUID types.FileUIDType) ([]ConvertedFileModel, error) {
	var files []ConvertedFileModel
	where := fmt.Sprintf("%s = ?", ConvertedFileColumn.FileUID)
	if err := r.db.WithContext(ctx).Where(where, fileUID).Find(&files).Error; err != nil {
		return nil, err
	}
	return files, nil
}

// DeleteConvertedFile deletes the record by UID
func (r *repository) DeleteConvertedFile(ctx context.Context, uid types.ConvertedFileUIDType) error {
	err := r.db.Transaction(func(tx *gorm.DB) error {
		// Specify the condition to find the record by its UID
		where := fmt.Sprintf("%s = ?", ConvertedFileColumn.UID)
		if err := tx.Where(where, uid).Delete(&ConvertedFileModel{}).Error; err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// DeleteAllConvertedFilesInKb deletes all the records in the knowledge base
func (r *repository) DeleteAllConvertedFilesInKb(ctx context.Context, kbUID types.KBUIDType) error {
	err := r.db.Transaction(func(tx *gorm.DB) error {
		// Specify the condition to find the record by its UID
		where := fmt.Sprintf("%s = ?", ConvertedFileColumn.KBUID)
		if err := tx.Where(where, kbUID).Delete(&ConvertedFileModel{}).Error; err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// UpdateConvertedFile updates the record by UID using update map.
func (r *repository) UpdateConvertedFile(ctx context.Context, uid types.ConvertedFileUIDType, update map[string]any) error {
	// Specify the condition to find the record by its UID
	where := fmt.Sprintf("%s = ?", ConvertedFileColumn.UID)
	if err := r.db.WithContext(ctx).Model(&ConvertedFileModel{}).Where(where, uid).Updates(update).Error; err != nil {
		return err
	}
	return nil
}

// HardDeleteConvertedFileByFileUID deletes the record by file UID
func (r *repository) HardDeleteConvertedFileByFileUID(ctx context.Context, fileUID types.FileUIDType) error {
	// Specify the condition to find the record by its UID
	where := fmt.Sprintf("%s = ?", ConvertedFileColumn.FileUID)
	if err := r.db.WithContext(ctx).Where(where, fileUID).Unscoped().Delete(&ConvertedFileModel{}).Error; err != nil {
		return err
	}
	return nil
}

// GORM hooks
func (cf *ConvertedFileModel) fillPositionDataJSON() (err error) {
	if cf.PositionData == nil {
		return nil
	}

	cf.PositionDataJSON, err = json.Marshal(cf.PositionData)
	return err
}

func (cf *ConvertedFileModel) BeforeCreate(tx *gorm.DB) error {
	return cf.fillPositionDataJSON()
}

func (cf *ConvertedFileModel) BeforeSave(tx *gorm.DB) error {
	return cf.fillPositionDataJSON()
}

func (cf *ConvertedFileModel) BeforeUpdate(tx *gorm.DB) error {
	return cf.fillPositionDataJSON()
}

func (cf *ConvertedFileModel) AfterFind(tx *gorm.DB) error {
	if cf.PositionDataJSON == nil {
		return nil
	}

	if cf.PositionData == nil {
		cf.PositionData = new(types.PositionData)
	}

	return json.Unmarshal(cf.PositionDataJSON, cf.PositionData)
}

// GetConvertedFileCountByKBUID returns the count of converted files for a knowledge base
// IMPORTANT: Only counts converted files for active (non-deleted) files
// This is critical for validation during updates to avoid counting converted files
// belonging to soft-deleted files that are awaiting cleanup
func (r *repository) GetConvertedFileCountByKBUID(ctx context.Context, kbUID types.KBUIDType) (int64, error) {
	var count int64
	err := r.db.WithContext(ctx).
		Table(ConvertedFileTableName+" AS cf").
		Joins("INNER JOIN "+FileTableName+" AS f ON cf.file_uid = f.uid").
		Where("cf.kb_uid = ?", kbUID).
		Where("f.delete_time IS NULL"). // CRITICAL: Exclude converted files for deleted files
		Count(&count).
		Error
	if err != nil {
		return 0, fmt.Errorf("counting converted files for KB %s: %w", kbUID, err)
	}
	return count, nil
}
