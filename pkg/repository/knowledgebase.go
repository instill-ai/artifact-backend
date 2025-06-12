package repository

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"github.com/lib/pq"
	"gorm.io/gorm"

	"github.com/instill-ai/artifact-backend/pkg/customerror"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

type KnowledgeBaseI interface {
	CreateKnowledgeBase(ctx context.Context, kb KnowledgeBase, externalService func(kbUID string) error) (*KnowledgeBase, error)
	ListKnowledgeBases(ctx context.Context, ownerUID string) ([]KnowledgeBase, error)
	ListKnowledgeBasesByCatalogType(ctx context.Context, ownerUID string, catalogType artifactpb.CatalogType) ([]KnowledgeBase, error)
	UpdateKnowledgeBase(ctx context.Context, ownerUID string, kb KnowledgeBase) (*KnowledgeBase, error)
	DeleteKnowledgeBase(ctx context.Context, ownerUID, kbID string) (*KnowledgeBase, error)
	GetKnowledgeBaseByOwnerAndKbID(ctx context.Context, ownerUID uuid.UUID, kbID string) (*KnowledgeBase, error)
	GetKnowledgeBaseCountByOwner(ctx context.Context, ownerUID string, catalogType artifactpb.CatalogType) (int64, error)
	IncreaseKnowledgeBaseUsage(ctx context.Context, tx *gorm.DB, kbUID string, amount int) error
	GetKnowledgeBasesByUIDs(ctx context.Context, kbUIDs []uuid.UUID) ([]KnowledgeBase, error)
	GetKnowledgeBaseByUID(context.Context, uuid.UUID) (*KnowledgeBase, error)
}

type KnowledgeBase struct {
	UID  uuid.UUID `gorm:"column:uid;type:uuid;default:uuid_generate_v4();primaryKey" json:"uid"`
	KbID string    `gorm:"column:id;size:255;not null" json:"kb_id"`
	// current name is the kb_id
	Name        string     `gorm:"column:name;size:255;not null" json:"name"`
	Description string     `gorm:"column:description;size:1023" json:"description"`
	Tags        TagsArray  `gorm:"column:tags;type:VARCHAR(255)[]" json:"tags"`
	Owner       string     `gorm:"column:owner;type:uuid;not null" json:"owner"`
	CreateTime  *time.Time `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime  *time.Time `gorm:"column:update_time;not null;autoUpdateTime" json:"update_time"` // Use autoUpdateTime
	DeleteTime  *time.Time `gorm:"column:delete_time" json:"delete_time"`
	// creator
	CreatorUID uuid.UUID `gorm:"column:creator_uid;type:uuid;not null" json:"creator_uid"`
	Usage      int64     `gorm:"column:usage;not null;default:0" json:"usage"`
	// this type is defined in artifact/artifact/v1alpha/catalog.proto
	CatalogType string `gorm:"column:catalog_type;size:255" json:"catalog_type"`

	ConvertingPipelines pq.StringArray `gorm:"column:converting_pipelines;type:varchar(255)[]" json:"converting_pipelines"`
}

// table columns map
type KnowledgeBaseColumns struct {
	UID         string
	KbID        string
	Name        string
	Description string
	Tags        string
	Owner       string
	CreateTime  string
	UpdateTime  string
	DeleteTime  string
	Usage       string
	CatalogType string
}

var KnowledgeBaseColumn = KnowledgeBaseColumns{
	UID:         "uid",
	KbID:        "id",
	Name:        "name",
	Description: "description",
	Tags:        "tags",
	Owner:       "owner",
	CreateTime:  "create_time",
	UpdateTime:  "update_time",
	DeleteTime:  "delete_time",
	Usage:       "usage",
	CatalogType: "catalog_type",
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
func (r *Repository) CreateKnowledgeBase(ctx context.Context, kb KnowledgeBase, externalService func(kbUID string) error) (*KnowledgeBase, error) {
	// Start a database transaction
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// check if the name is unique in the owner's knowledge bases
		KbIDExists, err := r.checkIfKbIDUnique(ctx, kb.Owner, kb.KbID)
		if err != nil {
			return err
		}
		if KbIDExists {
			return fmt.Errorf("knowledge base name already exists. err: %w", customerror.ErrInvalidArgument)
		}

		// Create a new KnowledgeBase record
		if err := tx.Create(&kb).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("knowledge base ID not found: %v, err:%w", kb.KbID, gorm.ErrRecordNotFound)
			}
			return err
		}

		// Call the external service
		if externalService != nil {
			if err := externalService(kb.UID.String()); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &kb, nil
}

// GetKnowledgeBase fetches all KnowledgeBase records from the database, excluding soft-deleted ones.
func (r *Repository) ListKnowledgeBases(ctx context.Context, owner string) ([]KnowledgeBase, error) {
	var knowledgeBases []KnowledgeBase
	// Exclude records where DeleteTime is not null and filter by owner
	whereString := fmt.Sprintf("%v IS NULL AND %v = ?", KnowledgeBaseColumn.DeleteTime, KnowledgeBaseColumn.Owner)
	if err := r.db.WithContext(ctx).Where(whereString, owner).Find(&knowledgeBases).Error; err != nil {
		return nil, err
	}

	return knowledgeBases, nil
}

// ListKnowledgeBasesByCatalogType fetches all KnowledgeBase records from the database, excluding soft-deleted ones.
func (r *Repository) ListKnowledgeBasesByCatalogType(ctx context.Context, owner string, catalogType artifactpb.CatalogType) ([]KnowledgeBase, error) {
	var knowledgeBases []KnowledgeBase
	// Exclude records where DeleteTime is not null and filter by owner
	whereString := fmt.Sprintf("%v IS NULL AND %v = ? AND %v = ?", KnowledgeBaseColumn.DeleteTime, KnowledgeBaseColumn.Owner, KnowledgeBaseColumn.CatalogType)
	if err := r.db.WithContext(ctx).Where(whereString, owner, catalogType.String()).Find(&knowledgeBases).Error; err != nil {
		return nil, err
	}

	return knowledgeBases, nil
}

// UpdateKnowledgeBase updates a KnowledgeBase record in the database except for CreateTime and DeleteTime.
func (r *Repository) UpdateKnowledgeBase(ctx context.Context, owner string, kb KnowledgeBase) (*KnowledgeBase, error) {
	// Fetch the existing record to ensure it exists and to get the CreateTime and DeleteTime fields
	var existingKB KnowledgeBase

	// Find the KnowledgeBase record by ID
	conds := fmt.Sprintf("%s = ? AND %s = ? AND %s is NULL", KnowledgeBaseColumn.KbID, KnowledgeBaseFileColumn.Owner, KnowledgeBaseColumn.DeleteTime)
	// Find the KnowledgeBase record by ID
	if err := r.db.WithContext(ctx).Where(conds, kb.KbID, kb.Owner).First(&existingKB).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("knowledge base ID not found. kb_id: %v, owner_uid: %v", kb.KbID, owner)
		}
		return nil, err
	}

	// Update the specific fields of the record
	if err := r.db.WithContext(ctx).Model(&existingKB).Updates(map[string]interface{}{
		// "kb_id":        kb.KbID,
		// KnowledgeBaseColumn.Owner:       kb.Owner,
		// KnowledgeBaseColumn.Name:        kb.Name,
		KnowledgeBaseColumn.Description: kb.Description,
		KnowledgeBaseColumn.Tags:        kb.Tags,
	}).Error; err != nil {
		return nil, err
	}
	// Fetch the updated record
	var updatedKB KnowledgeBase
	if err := r.db.WithContext(ctx).Where(conds, kb.KbID, kb.Owner).First(&updatedKB).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("knowledge base name id not found: %v. err: %w", kb.KbID, gorm.ErrRecordNotFound)
		}
		return nil, err
	}
	return &updatedKB, nil
}

// DeleteKnowledgeBase sets the DeleteTime to the current time to perform a soft delete.
func (r *Repository) DeleteKnowledgeBase(ctx context.Context, ownerUID string, kbID string) (*KnowledgeBase, error) {
	// Fetch the existing record to ensure it exists
	var existingKB KnowledgeBase
	conds := fmt.Sprintf("%v = ? AND %v = ? AND %v IS NULL", KnowledgeBaseColumn.Owner, KnowledgeBaseColumn.KbID, KnowledgeBaseColumn.DeleteTime)
	if err := r.db.WithContext(ctx).First(&existingKB, conds, ownerUID, kbID).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, fmt.Errorf("knowledge base ID not found: %v. err: %w", kbID, gorm.ErrRecordNotFound)
		}
		return nil, err
	}

	// Set the DeleteTime to the current time
	deleteTime := time.Now().UTC()
	existingKB.DeleteTime = &deleteTime

	// Save the changes to mark the record as soft deleted
	if err := r.db.WithContext(ctx).Save(&existingKB).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("knowledge base ID not found: %v. err: %w", existingKB.KbID, gorm.ErrRecordNotFound)
		}
		return nil, err
	}

	return &existingKB, nil
}

func (r *Repository) checkIfKbIDUnique(ctx context.Context, owner string, kbID string) (bool, error) {
	var existingKB KnowledgeBase
	whereString := fmt.Sprintf("%v = ? AND %v = ? AND %s is NULL", KnowledgeBaseColumn.Owner, KnowledgeBaseColumn.KbID, KnowledgeBaseColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereString, owner, kbID).First(&existingKB).Error; err != nil {
		if err != gorm.ErrRecordNotFound {
			return false, err
		}
	} else {
		return true, nil
	}
	return false, nil
}

// check if knowledge base exists by kb_uid
func (r *Repository) checkIfKnowledgeBaseExists(ctx context.Context, kbUID string) (bool, error) {
	var existingKB KnowledgeBase
	whereString := fmt.Sprintf("%v = ? AND %s is NULL", KnowledgeBaseColumn.UID, KnowledgeBaseColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereString, kbUID).First(&existingKB).Error; err != nil {
		if err != gorm.ErrRecordNotFound {
			return false, err
		}
	} else {
		return true, nil
	}
	return false, nil
}

// get the knowledge base by (owner, kb_id)
func (r *Repository) GetKnowledgeBaseByOwnerAndKbID(ctx context.Context, owner uuid.UUID, kbID string) (*KnowledgeBase, error) {
	var existingKB KnowledgeBase
	whereString := fmt.Sprintf("%v = ? AND %v = ? AND %v is NULL", KnowledgeBaseColumn.Owner, KnowledgeBaseColumn.KbID, KnowledgeBaseColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereString, owner, kbID).First(&existingKB).Error; err != nil {
		return nil, err
	}
	return &existingKB, nil
}

// get the count of knowledge bases by owner
func (r *Repository) GetKnowledgeBaseCountByOwner(ctx context.Context, owner string, catalogType artifactpb.CatalogType) (int64, error) {
	var count int64
	whereString := fmt.Sprintf("%v = ? AND %v is NULL AND %v = ?", KnowledgeBaseColumn.Owner, KnowledgeBaseColumn.DeleteTime, KnowledgeBaseColumn.CatalogType)
	if err := r.db.WithContext(ctx).Model(&KnowledgeBase{}).Where(whereString, owner, catalogType.String()).Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

// IncreaseKnowledgeBaseUsage increments the usage count of a KnowledgeBase record by a specified amount.
func (r *Repository) IncreaseKnowledgeBaseUsage(ctx context.Context, tx *gorm.DB, kbUID string, amount int) error {
	if tx == nil {
		tx = r.db.WithContext(ctx)
	}
	// Increment the usage count of the KnowledgeBase record by the specified amount
	where := fmt.Sprintf("%v = ?", KnowledgeBaseColumn.UID)
	expr := fmt.Sprintf("%v + ?", KnowledgeBaseColumn.Usage)
	if err := tx.WithContext(ctx).Model(&KnowledgeBase{}).Where(where, kbUID).Update(KnowledgeBaseColumn.Usage, gorm.Expr(expr, amount)).Error; err != nil {
		return err
	}
	return nil
}

// GetKnowledgeBasesByUIDs fetches a slice of knowledge bases by UID.
func (r *Repository) GetKnowledgeBasesByUIDs(ctx context.Context, kbUIDs []uuid.UUID) ([]KnowledgeBase, error) {
	var knowledgeBases []KnowledgeBase
	whereString := fmt.Sprintf("%v IN (?) AND %v IS NULL", KnowledgeBaseColumn.UID, KnowledgeBaseColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereString, kbUIDs).Find(&knowledgeBases).Error; err != nil {
		return nil, err
	}
	return knowledgeBases, nil
}

// GetKnowledgeBaseByUID fetches a knowledge base by its primary key.
func (r *Repository) GetKnowledgeBaseByUID(ctx context.Context, uid uuid.UUID) (*KnowledgeBase, error) {
	kb := new(KnowledgeBase)
	err := r.db.WithContext(ctx).Where("uid = ?", uid).First(kb).Error
	return kb, err
}
