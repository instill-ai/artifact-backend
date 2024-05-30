package repository

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type KnowledgeBaseI interface {
	CreateKnowledgeBase(ctx context.Context, kb KnowledgeBase) (*KnowledgeBase, error)
	ListKnowledgeBases(ctx context.Context, uid string) ([]KnowledgeBase, error)
	UpdateKnowledgeBase(ctx context.Context, uid string, kb KnowledgeBase) (*KnowledgeBase, error)
	DeleteKnowledgeBase(ctx context.Context, uid, kb_id string) error
}

type KnowledgeBase struct {
	ID          uuid.UUID  `gorm:"type:uuid;default:uuid_generate_v4();primaryKey" json:"id"`
	KbID        string     `gorm:"column:kb_id;size:255;not null;unique" json:"kb_id"`
	Name        string     `gorm:"column:name;size:255;not null" json:"name"`
	Description string     `gorm:"column:description;size:1023" json:"description"`
	Tags        TagsArray  `gorm:"column:tags;type:VARCHAR(255)[]" json:"tags"`
	Owner       string     `gorm:"column:owner;size:255;not null" json:"owner"`
	CreateTime  *time.Time `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime  *time.Time `gorm:"column:update_time;not null;autoUpdateTime" json:"update_time"` // Use autoUpdateTime
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

// TagsArray is a custom type to handle PostgreSQL VARCHAR(255)[] arrays.
type TagsArray []string

// Scan implements the Scanner interface for TagsArray.
func (tags *TagsArray) Scan(value interface{}) error {
	if value == nil {
		*tags = []string{}
		return nil
	}

	// Convert the value to string and parse it as a PostgreSQL array.
	*tags = parsePostgresArray(value.(string))
	return nil
}

// Value implements the driver Valuer interface for TagsArray.
func (tags TagsArray) Value() (driver.Value, error) {
	// Convert the TagsArray to a PostgreSQL array string.
	return formatPostgresArray(tags), nil
}

// Helper functions to parse and format PostgreSQL arrays.
func parsePostgresArray(s string) []string {
	trimmed := strings.Trim(s, "{}")
	if trimmed == "" {
		return []string{}
	}
	// Remove quotes around array elements if present
	elements := strings.Split(trimmed, ",")
	for i := range elements {
		elements[i] = strings.Trim(elements[i], "\"")
	}
	return elements
}

func formatPostgresArray(tags []string) string {
	// Join array elements with commas without adding extra quotes
	return "{" + strings.Join(tags, ",") + "}"
}

// CreateKnowledgeBase inserts a new KnowledgeBase record into the database.
func (r *Repository) CreateKnowledgeBase(ctx context.Context, kb KnowledgeBase) (*KnowledgeBase, error) {
	// check if the kb_id is unique, if yes try to add suffix to make it unique
	for try := 0; try < 5; try++ {
		// Check if the kb_id already exists
		var existingKB KnowledgeBase
		if err := r.db.WithContext(ctx).Where(KnowledgeBaseColumn.KbID+" = ?", kb.KbID).First(&existingKB).Error; err != nil {
			if err != gorm.ErrRecordNotFound {
				return nil, err
			}
		} else {
			// kb_id already exists, generate a new one
			kb.KbID = fmt.Sprintf("%s_%s", kb.KbID, uuid.New().String()[0:5]) // Add suffix to make it unique
			continue
		}
	}

	// check if the name is unique in the owner's knowledge bases
	nameExists, err := r.checkIfNameUnique(ctx, kb.Owner, kb.Name)
	if err != nil {
		return nil, err
	}
	if nameExists {
		return nil, fmt.Errorf("knowledge base name already exists")
	}

	// Create a new KnowledgeBase record
	if err := r.db.WithContext(ctx).Create(&kb).Error; err != nil {
		return nil, err
	}

	return &kb, nil
}

// GetKnowledgeBase fetches all KnowledgeBase records from the database, excluding soft-deleted ones.
func (r *Repository) ListKnowledgeBases(ctx context.Context, owner string) ([]KnowledgeBase, error) {
	var knowledgeBases []KnowledgeBase
	where_string := fmt.Sprintf("%v IS NULL AND %v = ?", KnowledgeBaseColumn.DeleteTime, KnowledgeBaseColumn.Owner)
	// Exclude records where DeleteTime is not null and filter by owner
	if err := r.db.WithContext(ctx).Where(where_string, owner).Find(&knowledgeBases).Error; err != nil {
		return nil, err
	}

	return knowledgeBases, nil
}

// UpdateKnowledgeBase updates a KnowledgeBase record in the database except for CreateTime and DeleteTime.
func (r *Repository) UpdateKnowledgeBase(ctx context.Context, uid string, kb KnowledgeBase) (*KnowledgeBase, error) {
	// Fetch the existing record to ensure it exists and to get the CreateTime and DeleteTime fields
	var existingKB KnowledgeBase

	// Find the KnowledgeBase record by ID
	conds := fmt.Sprintf("%s = ?", KnowledgeBaseColumn.KbID)
	// Find the KnowledgeBase record by ID
	if err := r.db.WithContext(ctx).Where(conds, kb.KbID).First(&existingKB).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
		return nil, err
	}

	// Check if the user is the owner of the record
	if existingKB.Owner != uid {
		return nil, fmt.Errorf("user is not the owner of the knowledge base")
	}

	// Update the specific fields of the record
	if err := r.db.WithContext(ctx).Model(&existingKB).Updates(map[string]interface{}{
		// "kb_id":        kb.KbID,
		// KnowledgeBaseColumn.Owner:       kb.Owner,
		KnowledgeBaseColumn.Name:        kb.Name,
		KnowledgeBaseColumn.Description: kb.Description,
		KnowledgeBaseColumn.Tags:        kb.Tags,
	}).Error; err != nil {
		return nil, err
	}
	// Fetch the updated record
	var updatedKB KnowledgeBase
	if err := r.db.WithContext(ctx).Where(conds, kb.KbID).First(&updatedKB).Error; err != nil {
		return nil, err
	}
	// Return the updated record
	kb = updatedKB
	return &kb, nil
}

// DeleteKnowledgeBase sets the DeleteTime to the current time to perform a soft delete.
func (r *Repository) DeleteKnowledgeBase(ctx context.Context, uid string, kb_id string) error {
	// Fetch the existing record to ensure it exists
	var existingKB KnowledgeBase
	conds := fmt.Sprintf("%v = ? AND %v IS NULL", KnowledgeBaseColumn.KbID, KnowledgeBaseColumn.DeleteTime)
	if err := r.db.WithContext(ctx).First(&existingKB, conds, kb_id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return err
		}
		return err
	}

	// Check if the user is the owner of the record
	if existingKB.Owner != uid {
		return fmt.Errorf("user is not the owner of the knowledge base")
	}

	// Set the DeleteTime to the current time
	deleteTime := time.Now().UTC()
	existingKB.DeleteTime = &deleteTime

	// Save the changes to mark the record as soft deleted
	if err := r.db.WithContext(ctx).Save(&existingKB).Error; err != nil {
		return err
	}

	return nil
}

func (r *Repository) checkIfNameUnique(ctx context.Context, owner string, name string) (bool, error) {
	var existingKB KnowledgeBase
	where_string := fmt.Sprintf("%v = ? AND %v = ?", KnowledgeBaseColumn.Owner, KnowledgeBaseColumn.Name)
	if err := r.db.WithContext(ctx).Where(where_string, owner, name).First(&existingKB).Error; err != nil {
		if err != gorm.ErrRecordNotFound {
			return false, err
		}
	} else {
		return true, nil
	}
	return false, nil
}
