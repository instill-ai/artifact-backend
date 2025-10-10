package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/instill-ai/artifact-backend/pkg/types"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

const (
	// ConvertedFileTableName is the table name for converted files
	ConvertedFileTableName = "converted_file"
)

type ConvertedFile interface {
	CreateConvertedFileWithDestination(ctx context.Context, cf ConvertedFileModel) (*ConvertedFileModel, error)
	UpdateConvertedFile(ctx context.Context, uid types.ConvertedFileUIDType, update map[string]any) error
	DeleteConvertedFile(ctx context.Context, uid types.ConvertedFileUIDType) error
	DeleteAllConvertedFilesInKb(ctx context.Context, kbUID types.KBUIDType) error
	HardDeleteConvertedFileByFileUID(ctx context.Context, fileUID types.FileUIDType) error
	GetConvertedFileByFileUID(ctx context.Context, fileUID types.FileUIDType) (*ConvertedFileModel, error)
}

type ConvertedFileModel struct {
	UID   types.ConvertedFileUIDType `gorm:"column:uid;type:uuid;default:gen_random_uuid();primaryKey" json:"uid"`
	KBUID types.KBUIDType            `gorm:"column:kb_uid;type:uuid;not null" json:"kb_uid"`
	// FileUID is the original file UID in knowledge base file table
	FileUID types.FileUIDType `gorm:"column:file_uid;type:uuid;not null" json:"file_uid"`
	Name    string            `gorm:"column:name;size:255;not null" json:"name"`
	// MIME type
	Type string `gorm:"column:type;size:100;not null" json:"type"`
	// destination path in minio
	Destination string `gorm:"column:destination;size:255;not null" json:"destination"`

	PositionDataJSON datatypes.JSON      `gorm:"column:position_data;type:jsonb" json:"position_data_json"`
	PositionData     *types.PositionData `gorm:"-" json:"position_data"`

	CreateTime *time.Time `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime *time.Time `gorm:"column:update_time;not null;default:CURRENT_TIMESTAMP;autoUpdateTime" json:"update_time"`
}

// TableName overrides the default table name for GORM
func (ConvertedFileModel) TableName() string {
	return ConvertedFileTableName
}

type ConvertedFileColumns struct {
	UID         string
	KBUID       string
	FileUID     string
	Name        string
	Type        string
	Destination string
	CreateTime  string
	UpdateTime  string
}

var ConvertedFileColumn = ConvertedFileColumns{
	UID:         "uid",
	KBUID:       "kb_uid",
	FileUID:     "file_uid",
	Name:        "name",
	Type:        "type",
	Destination: "destination",
	CreateTime:  "create_time",
	UpdateTime:  "update_time",
}

// CreateConvertedFileWithDestination creates a converted file record with a known destination.
// This method properly decouples database operations from external storage operations.
// It handles deletion of any existing converted file for the same file_uid (for reprocessing).
func (r *repository) CreateConvertedFileWithDestination(ctx context.Context, cf ConvertedFileModel) (*ConvertedFileModel, error) {
	// Validate required fields before attempting to persist
	if cf.FileUID.IsNil() {
		return nil, fmt.Errorf("file_uid is required")
	}
	if cf.KBUID.IsNil() {
		return nil, fmt.Errorf("kb_uid is required")
	}
	if cf.Destination == "" {
		return nil, fmt.Errorf("destination is required and cannot be empty")
	}
	if cf.Name == "" {
		return nil, fmt.Errorf("name is required and cannot be empty")
	}
	if cf.Type == "" {
		return nil, fmt.Errorf("type is required and cannot be empty")
	}

	err := r.db.Transaction(func(tx *gorm.DB) error {
		// There may already be a converted file (if we're reprocessing a file).
		if err := tx.Where("file_uid = ?", cf.FileUID).Delete(&ConvertedFileModel{}).Error; err != nil {
			return err
		}

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
func (r *repository) GetConvertedFileByFileUID(ctx context.Context, fileUID types.FileUIDType) (*ConvertedFileModel, error) {
	var cf ConvertedFileModel
	where := fmt.Sprintf("%s = ?", ConvertedFileColumn.FileUID)
	if err := r.db.WithContext(ctx).Where(where, fileUID).First(&cf).Error; err != nil {
		return nil, err
	}
	return &cf, nil
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
