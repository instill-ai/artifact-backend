package repository

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/instill-ai/artifact-backend/pkg/types"
	"github.com/lib/pq"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	errorsx "github.com/instill-ai/x/errors"
)

type KnowledgeBase interface {
	CreateKnowledgeBase(ctx context.Context, kb KnowledgeBaseModel, externalService func(kbUID types.KBUIDType) error) (*KnowledgeBaseModel, error)
	ListKnowledgeBases(ctx context.Context, ownerUID string) ([]KnowledgeBaseModel, error)
	ListKnowledgeBasesByCatalogType(ctx context.Context, ownerUID string, catalogType types.CatalogType) ([]KnowledgeBaseModel, error)
	UpdateKnowledgeBase(ctx context.Context, id, ownerUID string, kb KnowledgeBaseModel) (*KnowledgeBaseModel, error)
	DeleteKnowledgeBase(ctx context.Context, ownerUID, kbID string) (*KnowledgeBaseModel, error)
	GetKnowledgeBaseByOwnerAndKbID(ctx context.Context, ownerUID types.OwnerUIDType, kbID string) (*KnowledgeBaseModel, error)
	GetKnowledgeBaseCountByOwner(ctx context.Context, ownerUID string, catalogType types.CatalogType) (int64, error)
	IncreaseKnowledgeBaseUsage(ctx context.Context, tx *gorm.DB, kbUID string, amount int) error
	GetKnowledgeBasesByUIDs(ctx context.Context, kbUIDs []types.KBUIDType) ([]KnowledgeBaseModel, error)
	GetKnowledgeBaseByUID(context.Context, types.KBUIDType) (*KnowledgeBaseModel, error)
}

type KnowledgeBaseModel struct {
	UID  types.KBUIDType `gorm:"column:uid;type:uuid;default:uuid_generate_v4();primaryKey" json:"uid"`
	KbID string          `gorm:"column:id;size:255;not null" json:"kb_id"`
	// current name is the kb_id
	Name        string     `gorm:"column:name;size:255;not null" json:"name"`
	Description string     `gorm:"column:description;size:1023" json:"description"`
	Tags        TagsArray  `gorm:"column:tags;type:VARCHAR(255)[]" json:"tags"`
	Owner       string     `gorm:"column:owner;type:uuid;not null" json:"owner"`
	CreateTime  *time.Time `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime  *time.Time `gorm:"column:update_time;not null;autoUpdateTime" json:"update_time"` // Use autoUpdateTime
	DeleteTime  *time.Time `gorm:"column:delete_time" json:"delete_time"`
	// creator
	CreatorUID types.CreatorUIDType `gorm:"column:creator_uid;type:uuid;not null" json:"creator_uid"`
	Usage      int64                `gorm:"column:usage;not null;default:0" json:"usage"`
	// this type is defined in artifact/artifact/v1alpha/catalog.proto
	CatalogType string `gorm:"column:catalog_type;size:255" json:"catalog_type"`

	ConvertingPipelines pq.StringArray `gorm:"column:converting_pipelines;type:varchar(255)[]" json:"converting_pipelines"`
}

// TableName overrides the default table name for GORM
func (KnowledgeBaseModel) TableName() string {
	return "knowledge_base"
}

// KnowledgeBaseColumns is the columns for the knowledge base table
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
func (tags *TagsArray) Scan(value any) error {
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

// CreateKnowledgeBase inserts a new KnowledgeBaseModel record into the database.
func (r *repository) CreateKnowledgeBase(ctx context.Context, kb KnowledgeBaseModel, externalService func(kbUID types.KBUIDType) error) (*KnowledgeBaseModel, error) {
	// Start a database transaction
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// check if the name is unique in the owner's knowledge bases
		KbIDExists, err := r.checkIfKbIDUnique(ctx, kb.Owner, kb.KbID)
		if err != nil {
			return err
		}
		if KbIDExists {
			return fmt.Errorf("knowledge base name %q already exists: %w", kb.KbID, errorsx.ErrAlreadyExists)
		}

		// Create a new KnowledgeBaseModel record
		if err := tx.Create(&kb).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("knowledge base ID not found: %v, err:%w", kb.KbID, gorm.ErrRecordNotFound)
			}
			return err
		}

		// Call the external service
		if externalService != nil {
			if err := externalService(kb.UID); err != nil {
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

// GetKnowledgeBase fetches all KnowledgeBaseModel records from the database, excluding soft-deleted ones.
func (r *repository) ListKnowledgeBases(ctx context.Context, owner string) ([]KnowledgeBaseModel, error) {
	var knowledgeBases []KnowledgeBaseModel
	// Exclude records where DeleteTime is not null and filter by owner
	whereString := fmt.Sprintf("%v IS NULL AND %v = ?", KnowledgeBaseColumn.DeleteTime, KnowledgeBaseColumn.Owner)
	if err := r.db.WithContext(ctx).Where(whereString, owner).Find(&knowledgeBases).Error; err != nil {
		return nil, err
	}

	return knowledgeBases, nil
}

// ListKnowledgeBasesByCatalogType fetches all KnowledgeBaseModel records from the database, excluding soft-deleted ones.
func (r *repository) ListKnowledgeBasesByCatalogType(ctx context.Context, owner string, catalogType types.CatalogType) ([]KnowledgeBaseModel, error) {
	var knowledgeBases []KnowledgeBaseModel
	// Exclude records where DeleteTime is not null and filter by owner
	whereString := fmt.Sprintf("%v IS NULL AND %v = ? AND %v = ?", KnowledgeBaseColumn.DeleteTime, KnowledgeBaseColumn.Owner, KnowledgeBaseColumn.CatalogType)
	if err := r.db.WithContext(ctx).Where(whereString, owner, string(catalogType)).Find(&knowledgeBases).Error; err != nil {
		return nil, err
	}

	return knowledgeBases, nil
}

// UpdateKnowledgeBase updates a KnowledgeBaseModel record in the database. The
// modifiable fields are description, tags and conversion pipelines.
func (r *repository) UpdateKnowledgeBase(ctx context.Context, id, owner string, kb KnowledgeBaseModel) (*KnowledgeBaseModel, error) {
	where := fmt.Sprintf("%s = ? AND %s = ? AND %s is NULL", KnowledgeBaseColumn.KbID, KnowledgeBaseFileColumn.Owner, KnowledgeBaseColumn.DeleteTime)

	// Update the specific fields of the record. Empty fields will be ignored.
	updatedKB := new(KnowledgeBaseModel)
	err := r.db.WithContext(ctx).
		Clauses(clause.Returning{}).
		Model(&updatedKB).
		Where(where, id, owner).
		Updates(KnowledgeBaseModel{
			Description:         kb.Description,
			Tags:                kb.Tags,
			ConvertingPipelines: kb.ConvertingPipelines,
		}).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("knowledge base %s/%s not found", id, owner)
		}
		return nil, fmt.Errorf("updating record: %w", err)
	}

	return updatedKB, nil
}

// DeleteKnowledgeBase sets the DeleteTime to the current time to perform a soft delete.
func (r *repository) DeleteKnowledgeBase(ctx context.Context, ownerUID string, kbID string) (*KnowledgeBaseModel, error) {
	// Fetch the existing record to ensure it exists
	var existingKB KnowledgeBaseModel
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

func (r *repository) checkIfKbIDUnique(ctx context.Context, owner string, kbID string) (bool, error) {
	var existingKB KnowledgeBaseModel
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
func (r *repository) checkIfKnowledgeBaseExists(ctx context.Context, kbUID types.KBUIDType) (bool, error) {
	whereString := fmt.Sprintf("%v = ? AND %s is NULL", KnowledgeBaseColumn.UID, KnowledgeBaseColumn.DeleteTime)
	err := r.db.WithContext(ctx).Where(whereString, kbUID).First(&KnowledgeBaseModel{}).Error
	if err != nil {
		if err != gorm.ErrRecordNotFound {
			return false, err
		}

		return false, nil
	}

	return true, nil
}

// get the knowledge base by (owner, kb_id)
func (r *repository) GetKnowledgeBaseByOwnerAndKbID(ctx context.Context, owner types.OwnerUIDType, kbID string) (*KnowledgeBaseModel, error) {
	var existingKB KnowledgeBaseModel
	whereString := fmt.Sprintf("%v = ? AND %v = ? AND %v is NULL", KnowledgeBaseColumn.Owner, KnowledgeBaseColumn.KbID, KnowledgeBaseColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereString, owner, kbID).First(&existingKB).Error; err != nil {
		return nil, err
	}
	return &existingKB, nil
}

// get the count of knowledge bases by owner
func (r *repository) GetKnowledgeBaseCountByOwner(ctx context.Context, owner string, catalogType types.CatalogType) (int64, error) {
	var count int64
	whereString := fmt.Sprintf("%v = ? AND %v is NULL AND %v = ?", KnowledgeBaseColumn.Owner, KnowledgeBaseColumn.DeleteTime, KnowledgeBaseColumn.CatalogType)
	if err := r.db.WithContext(ctx).Model(&KnowledgeBaseModel{}).Where(whereString, owner, string(catalogType)).Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

// IncreaseKnowledgeBaseUsage increments the usage count of a KnowledgeBaseModel record by a specified amount.
func (r *repository) IncreaseKnowledgeBaseUsage(ctx context.Context, tx *gorm.DB, kbUID string, amount int) error {
	if tx == nil {
		tx = r.db.WithContext(ctx)
	}
	// Increment the usage count of the KnowledgeBaseModel record by the specified amount
	where := fmt.Sprintf("%v = ?", KnowledgeBaseColumn.UID)
	expr := fmt.Sprintf("%v + ?", KnowledgeBaseColumn.Usage)
	if err := tx.WithContext(ctx).Model(&KnowledgeBaseModel{}).Where(where, kbUID).Update(KnowledgeBaseColumn.Usage, gorm.Expr(expr, amount)).Error; err != nil {
		return err
	}
	return nil
}

// GetKnowledgeBasesByUIDs fetches a slice of knowledge bases by UID.
func (r *repository) GetKnowledgeBasesByUIDs(ctx context.Context, kbUIDs []types.KBUIDType) ([]KnowledgeBaseModel, error) {
	var knowledgeBases []KnowledgeBaseModel
	whereString := fmt.Sprintf("%v IN (?) AND %v IS NULL", KnowledgeBaseColumn.UID, KnowledgeBaseColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereString, kbUIDs).Find(&knowledgeBases).Error; err != nil {
		return nil, err
	}
	return knowledgeBases, nil
}

// GetKnowledgeBaseByUID fetches a knowledge base by its primary key.
func (r *repository) GetKnowledgeBaseByUID(ctx context.Context, uid types.KBUIDType) (*KnowledgeBaseModel, error) {
	kb := new(KnowledgeBaseModel)
	err := r.db.WithContext(ctx).Where("uid = ?", uid).First(kb).Error
	return kb, err
}
