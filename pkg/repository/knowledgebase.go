package repository

import (
	"context"
	"time"

	"gorm.io/gorm"
)

type KnowledgeBaseI interface {
	CreateKnowledgeBase(ctx context.Context, kb KnowledgeBase) (*KnowledgeBase, error)
	GetKnowledgeBase(ctx context.Context) ([]KnowledgeBase, error)
	UpdateKnowledgeBase(ctx context.Context, kb KnowledgeBase) (*KnowledgeBase, error)
	DeleteKnowledgeBase(ctx context.Context, kb KnowledgeBase) error
}

type KnowledgeBase struct {
	ID          uint       `gorm:"column:id;primaryKey" json:"id"`
	KbID        string     `gorm:"column:kb_id;size:255;not null;unique" json:"kb_id"`
	Name        string     `gorm:"column:name;size:255;not null" json:"name"`
	Description string     `gorm:"column:description;size:1023" json:"description"`
	Tags        TagsArray  `gorm:"column:tags;type:VARCHAR(255)[]" json:"tags"`
	Owner       string     `gorm:"column:owner;size:255;not null" json:"owner"`
	CreateTime  time.Time  `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime  time.Time  `gorm:"column:update_time;not null;default:CURRENT_TIMESTAMP" json:"update_time"`
	DeleteTime  *time.Time `gorm:"column:delete_time" json:"delete_time"`
}

// table columns map
type KnowledgeBaseColumns struct {
	ID          string
	KbID        string
	Name        string
	Description string
	Tags        string
	Owner       string
	CreateTime  string
	UpdateTime  string
	DeleteTime  string
}

var KnowledgeBaseColumn = KnowledgeBaseColumns{
	ID:          "id",
	KbID:        "kb_id",
	Name:        "name",
	Description: "description",
	Tags:        "tags",
	Owner:       "owner",
	CreateTime:  "create_time",
	UpdateTime:  "update_time",
	DeleteTime:  "delete_time",
}

type TagsArray []string

// CreateKnowledgeBase inserts a new KnowledgeBase record into the database.
func (r *Repository) CreateKnowledgeBase(ctx context.Context, kb KnowledgeBase) (*KnowledgeBase, error) {
	// Set the creation and update times to the current time if they are not already set
	if kb.CreateTime.IsZero() {
		kb.CreateTime = time.Now().UTC()
	}
	if kb.UpdateTime.IsZero() {
		kb.UpdateTime = time.Now().UTC()
	}

	if err := r.db.WithContext(ctx).Create(&kb).Error; err != nil {
		return nil, err
	}

	return &kb, nil
}

// GetKnowledgeBase fetches all KnowledgeBase records from the database, excluding soft-deleted ones.
func (r *Repository) GetKnowledgeBase(ctx context.Context) ([]KnowledgeBase, error) {
	var knowledgeBases []KnowledgeBase

	// Exclude records where DeleteTime is not null
	if err := r.db.WithContext(ctx).Where("delete_time IS NULL").Find(&knowledgeBases).Error; err != nil {
		return nil, err
	}

	return knowledgeBases, nil
}

// UpdateKnowledgeBase updates a KnowledgeBase record in the database except for CreateTime and DeleteTime.
func (r *Repository) UpdateKnowledgeBase(ctx context.Context, kb KnowledgeBase) (*KnowledgeBase, error) {
	// Fetch the existing record to ensure it exists and to get the CreateTime and DeleteTime fields
	var existingKB KnowledgeBase
	if err := r.db.WithContext(ctx).First(&existingKB, kb.ID).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, err
		}
		return nil, err
	}

	// Preserve CreateTime and DeleteTime
	kb.CreateTime = existingKB.CreateTime
	kb.DeleteTime = existingKB.DeleteTime

	// Manually set the update time to the current time
	kb.UpdateTime = time.Now().UTC()

	// Update the record with the new values, except for CreateTime and DeleteTime
	if err := r.db.WithContext(ctx).Model(&existingKB).Updates(KnowledgeBase{
		KbID:        kb.KbID,
		Name:        kb.Name,
		Description: kb.Description,
		Tags:        kb.Tags,
		Owner:       kb.Owner,
		UpdateTime:  kb.UpdateTime, // Manually set update time
	}).Error; err != nil {
		return nil, err
	}
	return &kb, nil
}

// DeleteKnowledgeBase sets the DeleteTime to the current time to perform a soft delete.
func (r *Repository) DeleteKnowledgeBase(ctx context.Context, kb KnowledgeBase) error {
	// Fetch the existing record to ensure it exists
	var existingKB KnowledgeBase
	if err := r.db.WithContext(ctx).First(&existingKB, kb.ID).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return err
		}
		return err
	}

	// Set the DeleteTime to the current time
	*existingKB.DeleteTime = time.Now().UTC()
	// set update time to current time
	existingKB.UpdateTime = time.Now().UTC()

	// Save the changes to mark the record as soft deleted
	if err := r.db.WithContext(ctx).Save(&existingKB).Error; err != nil {
		return err
	}

	return nil
}
