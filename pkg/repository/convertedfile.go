package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type ConvertedFileI interface {
	ConvertedFileTableName() string

	CreateConvertedFileWithDestination(ctx context.Context, cf ConvertedFile) (*ConvertedFile, error)
	UpdateConvertedFile(ctx context.Context, uid uuid.UUID, update map[string]any) error
	DeleteConvertedFile(ctx context.Context, uid uuid.UUID) error
	DeleteAllConvertedFilesInKb(ctx context.Context, kbUID uuid.UUID) error
	HardDeleteConvertedFileByFileUID(ctx context.Context, fileUID uuid.UUID) error
	GetConvertedFileByFileUID(ctx context.Context, fileUID uuid.UUID) (*ConvertedFile, error)
}

// PositionData contains metadata from the conversion step that will be used to
// position the chunks extracted from the Markdown string within the original
// file.
//
// When more filetypes are supported, this entity might evolve to accommodate
// other kinds of positions (e.g. time markers mapping a character to a
// duration).
type PositionData struct {
	// Page delimiters contains the byte length from the beginning of the
	// document until the last character in a given page. The cumulative length
	// of the pages is kept in order ease the positioning of the chunks within
	// the original document.
	// E.g., we can access the contents of a page as
	// []rune(markdownStr)[pageDelimiters[i]:pageDelimiters[i+1]]
	PageDelimiters []uint32 `json:"page_delimiters"`
}

type ConvertedFile struct {
	UID   uuid.UUID `gorm:"column:uid;type:uuid;default:gen_random_uuid();primaryKey" json:"uid"`
	KbUID uuid.UUID `gorm:"column:kb_uid;type:uuid;not null" json:"kb_uid"`
	// FileUID is the original file UID in knowledge base file table
	FileUID uuid.UUID `gorm:"column:file_uid;type:uuid;not null" json:"file_uid"`
	Name    string    `gorm:"column:name;size:255;not null" json:"name"`
	// MIME type
	Type string `gorm:"column:type;size:100;not null" json:"type"`
	// destination path in minio
	Destination string `gorm:"column:destination;size:255;not null" json:"destination"`

	PositionDataJSON datatypes.JSON `gorm:"column:position_data;type:jsonb" json:"position_data_json"`
	PositionData     *PositionData  `gorm:"-" json:"position_data"`

	CreateTime *time.Time `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime *time.Time `gorm:"column:update_time;not null;default:CURRENT_TIMESTAMP;autoUpdateTime" json:"update_time"`
}

type ConvertedFileColumns struct {
	UID         string
	KbUID       string
	FileUID     string
	Name        string
	Type        string
	Destination string
	CreateTime  string
	UpdateTime  string
}

var ConvertedFileColumn = ConvertedFileColumns{
	UID:         "uid",
	KbUID:       "kb_uid",
	FileUID:     "file_uid",
	Name:        "name",
	Type:        "type",
	Destination: "destination",
	CreateTime:  "create_time",
	UpdateTime:  "update_time",
}

// ConvertedFileTableName returns the table name of the ConvertedFile
func (r *Repository) ConvertedFileTableName() string {
	return "converted_file"
}

// CreateConvertedFileWithDestination creates a converted file record with a known destination.
// This method properly decouples database operations from external storage operations.
// It handles deletion of any existing converted file for the same file_uid (for reprocessing).
func (r *Repository) CreateConvertedFileWithDestination(ctx context.Context, cf ConvertedFile) (*ConvertedFile, error) {
	err := r.db.Transaction(func(tx *gorm.DB) error {
		// There may already be a converted file (if we're reprocessing a file).
		if err := tx.Where("file_uid = ?", cf.FileUID).Delete(&ConvertedFile{}).Error; err != nil {
			return err
		}

		// Create the new ConvertedFile with the provided destination
		if err := tx.Create(&cf).Error; err != nil {
			return err
		}

		// Check if UID was generated after create
		if cf.UID == uuid.Nil {
			return fmt.Errorf("did not get UID after create")
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	return &cf, nil
}

// GetConvertedFileByFileUID returns the converted file by file UID
func (r *Repository) GetConvertedFileByFileUID(ctx context.Context, fileUID uuid.UUID) (*ConvertedFile, error) {
	var cf ConvertedFile
	where := fmt.Sprintf("%s = ?", ConvertedFileColumn.FileUID)
	if err := r.db.WithContext(ctx).Where(where, fileUID).First(&cf).Error; err != nil {
		return nil, err
	}
	return &cf, nil
}

// DeleteConvertedFile deletes the record by UID
func (r *Repository) DeleteConvertedFile(ctx context.Context, uid uuid.UUID) error {
	err := r.db.Transaction(func(tx *gorm.DB) error {
		// Specify the condition to find the record by its UID
		where := fmt.Sprintf("%s = ?", ConvertedFileColumn.UID)
		if err := tx.Where(where, uid).Delete(&ConvertedFile{}).Error; err != nil {
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
func (r *Repository) DeleteAllConvertedFilesInKb(ctx context.Context, kbUID uuid.UUID) error {
	err := r.db.Transaction(func(tx *gorm.DB) error {
		// Specify the condition to find the record by its UID
		where := fmt.Sprintf("%s = ?", ConvertedFileColumn.KbUID)
		if err := tx.Where(where, kbUID).Delete(&ConvertedFile{}).Error; err != nil {
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
func (r *Repository) UpdateConvertedFile(ctx context.Context, uid uuid.UUID, update map[string]any) error {
	// Specify the condition to find the record by its UID
	where := fmt.Sprintf("%s = ?", ConvertedFileColumn.UID)
	if err := r.db.WithContext(ctx).Model(&ConvertedFile{}).Where(where, uid).Updates(update).Error; err != nil {
		return err
	}
	return nil
}

// HardDeleteConvertedFileByFileUID deletes the record by file UID
func (r *Repository) HardDeleteConvertedFileByFileUID(ctx context.Context, fileUID uuid.UUID) error {
	// Specify the condition to find the record by its UID
	where := fmt.Sprintf("%s = ?", ConvertedFileColumn.FileUID)
	if err := r.db.WithContext(ctx).Where(where, fileUID).Unscoped().Delete(&ConvertedFile{}).Error; err != nil {
		return err
	}
	return nil
}

// GORM hooks
func (cf *ConvertedFile) fillPositionDataJSON() (err error) {
	if cf.PositionData == nil {
		return nil
	}

	cf.PositionDataJSON, err = json.Marshal(cf.PositionData)
	return err
}

func (cf *ConvertedFile) BeforeCreate(tx *gorm.DB) error {
	return cf.fillPositionDataJSON()
}

func (cf *ConvertedFile) BeforeSave(tx *gorm.DB) error {
	return cf.fillPositionDataJSON()
}

func (cf *ConvertedFile) BeforeUpdate(tx *gorm.DB) error {
	return cf.fillPositionDataJSON()
}

func (cf *ConvertedFile) AfterFind(tx *gorm.DB) error {
	if cf.PositionDataJSON == nil {
		return nil
	}

	if cf.PositionData == nil {
		cf.PositionData = new(PositionData)
	}

	return json.Unmarshal(cf.PositionDataJSON, cf.PositionData)
}
