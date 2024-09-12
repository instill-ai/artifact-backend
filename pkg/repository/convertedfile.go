package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"gorm.io/gorm"
)

type ConvertedFileI interface {
	ConvertedFileTableName() string
	CreateConvertedFile(ctx context.Context, cf ConvertedFile, callExternalService func(convertedFileUID uuid.UUID) (map[string]any, error)) (*ConvertedFile, error)
	DeleteConvertedFile(ctx context.Context, uid uuid.UUID) error
	DeleteAllConvertedFilesInKb(ctx context.Context, kbUID uuid.UUID) error
	HardDeleteConvertedFileByFileUID(ctx context.Context, fileUID uuid.UUID) error
	GetConvertedFileByFileUID(ctx context.Context, fileUID uuid.UUID) (*ConvertedFile, error)
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
	Destination string     `gorm:"column:destination;size:255;not null" json:"destination"`
	CreateTime  *time.Time `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime  *time.Time `gorm:"column:update_time;not null;default:CURRENT_TIMESTAMP;autoUpdateTime" json:"update_time"`
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

func (r *Repository) CreateConvertedFile(ctx context.Context, cf ConvertedFile, callExternalService func(convertedFileUID uuid.UUID) (map[string]any, error)) (*ConvertedFile, error) {
	err := r.db.Transaction(func(tx *gorm.DB) error {
		// Check if file_uid exists
		var existingFile ConvertedFile
		where := fmt.Sprintf("%s = ?", ConvertedFileColumn.FileUID)
		if err := tx.Where(where, cf.FileUID).First(&existingFile).Error; err != nil {
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
		}

		// If file_uid exists, delete it
		if existingFile.UID != uuid.Nil {
			if err := tx.Delete(&existingFile).Error; err != nil {
				return err
			}
		}

		// Create the new ConvertedFile
		if err := tx.Create(&cf).Error; err != nil {
			return err
		}

		// Check if UID was generated after create
		if cf.UID == uuid.Nil {
			return fmt.Errorf("did not get UID after create")
		}

		if callExternalService != nil {
			// Call the external service using the created record's UID
			if output, err := callExternalService(cf.UID); err != nil {
				// If the external service returns an error, return the error to trigger a rollback
				return err
			} else {
				// get dest from output and update the record
				if dest, ok := output[ConvertedFileColumn.Destination].(string); ok {
					update := map[string]any{ConvertedFileColumn.Destination: dest}
					if err := tx.Model(&cf).Updates(update).Error; err != nil {
						return err
					}
				}
			}
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
