package repository

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

// IsUpdateInProgress returns true if the KB is currently being updated
// This includes all active phases from reprocessing through metadata sync
func IsUpdateInProgress(status string) bool {
	return status == artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_UPDATING.String() ||
		status == artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_SWAPPING.String() ||
		status == artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_VALIDATING.String() ||
		status == artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_SYNCING.String()
}

// IsUpdateComplete returns true if the update has finished (success, failure, rollback, or aborted)
// Used to determine if a new update can be started
func IsUpdateComplete(status string) bool {
	return status == "" ||
		status == artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED.String() ||
		status == artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_FAILED.String() ||
		status == artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_ROLLED_BACK.String() ||
		status == artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_ABORTED.String()
}

// IsDualProcessingNeeded returns true if files uploaded to this KB require dual processing
// Dual processing is needed during active updates (all phases) and during retention period
func IsDualProcessingNeeded(status string) bool {
	// During update phases: dual process with staging KB
	if IsUpdateInProgress(status) {
		return true
	}
	// During retention period: dual process with rollback KB
	if status == artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED.String() ||
		status == artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_ROLLED_BACK.String() {
		return true
	}
	return false
}

// KnowledgeBase interface defines the methods for the knowledge base repository
type KnowledgeBase interface {
	CreateKnowledgeBase(ctx context.Context, kb KnowledgeBaseModel, externalService func(kbUID types.KBUIDType) error) (*KnowledgeBaseModel, error)
	ListKnowledgeBases(ctx context.Context, ownerUID string) ([]KnowledgeBaseModel, error)
	ListKnowledgeBasesByCatalogType(ctx context.Context, ownerUID string, catalogType artifactpb.CatalogType) ([]KnowledgeBaseModel, error)
	UpdateKnowledgeBase(ctx context.Context, id, ownerUID string, kb KnowledgeBaseModel) (*KnowledgeBaseModel, error)
	DeleteKnowledgeBase(ctx context.Context, ownerUID, kbID string) (*KnowledgeBaseModel, error)
	GetKnowledgeBaseByOwnerAndKbID(ctx context.Context, ownerUID types.OwnerUIDType, kbID string) (*KnowledgeBaseModel, error)
	GetKnowledgeBaseByID(ctx context.Context, kbID string) (*KnowledgeBaseModel, error)
	GetKnowledgeBaseCountByOwner(ctx context.Context, ownerUID string, catalogType artifactpb.CatalogType) (int64, error)
	IncreaseKnowledgeBaseUsage(ctx context.Context, tx *gorm.DB, kbUID string, amount int) error
	GetKnowledgeBasesByUIDs(ctx context.Context, kbUIDs []types.KBUIDType) ([]KnowledgeBaseModel, error)
	GetKnowledgeBaseByUID(context.Context, types.KBUIDType) (*KnowledgeBaseModel, error)
	// GetActiveCollectionUID retrieves the active collection UID for a KB
	// This supports collection versioning for dimension changes
	GetActiveCollectionUID(ctx context.Context, kbUID types.KBUIDType) (*types.KBUIDType, error)
	// IsCollectionInUse checks if a collection is still referenced by any KB
	IsCollectionInUse(ctx context.Context, collectionUID types.CollectionUIDType) (bool, error)
	// IsKBUpdating checks if a KB is currently in updating state
	IsKBUpdating(ctx context.Context, kbUID types.KBUIDType) (bool, error)
	// GetStagingKBForProduction finds the staging KB associated with a production catalog
	// Returns nil if no staging KB exists (no update in progress)
	GetStagingKBForProduction(ctx context.Context, ownerUID types.OwnerUIDType, productionCatalogID string) (*KnowledgeBaseModel, error)
	// GetRollbackKBForProduction finds the rollback KB for a production catalog during retention period
	// Returns nil if no rollback KB exists (no retention period active)
	GetRollbackKBForProduction(ctx context.Context, ownerUID types.OwnerUIDType, productionCatalogID string) (*KnowledgeBaseModel, error)
	// GetDualProcessingTarget determines if dual processing is needed and returns the target KB
	// Returns a DualProcessingTarget with IsNeeded=false if no dual processing is needed
	GetDualProcessingTarget(ctx context.Context, productionKB *KnowledgeBaseModel) (*DualProcessingTarget, error)
	// RAG update methods
	CreateStagingKnowledgeBase(ctx context.Context, original *KnowledgeBaseModel, newEmbeddingConfig *EmbeddingConfigJSON, externalService func(kbUID types.KBUIDType) error) (*KnowledgeBaseModel, error)
	// ListKnowledgeBasesForUpdate finds KBs ready for the next update cycle
	ListKnowledgeBasesForUpdate(ctx context.Context, tagFilters []string, catalogIDs []string) ([]KnowledgeBaseModel, error)
	// ListKnowledgeBasesByUpdateStatus lists all KBs with a specific update_status
	ListKnowledgeBasesByUpdateStatus(ctx context.Context, updateStatus string) ([]KnowledgeBaseModel, error)
	// UpdateKnowledgeBaseUpdateStatus updates the update status of a KB
	UpdateKnowledgeBaseUpdateStatus(ctx context.Context, kbUID types.KBUIDType, status string, workflowID string) error
	// UpdateKnowledgeBaseAborted sets the KB status to ABORTED and explicitly clears the workflow ID to NULL
	UpdateKnowledgeBaseAborted(ctx context.Context, kbUID types.KBUIDType) error
	// UpdateKnowledgeBaseWithMap updates a KB using a map to allow zero values like false
	UpdateKnowledgeBaseWithMap(ctx context.Context, id, owner string, updates map[string]any) error

	// UpdateKnowledgeBaseResources updates kb_uid references in all resource tables
	// This is critical for atomic swap to ensure resources follow their knowledge bases
	UpdateKnowledgeBaseResources(ctx context.Context, fromKBUID, toKBUID types.KBUIDType) error
}

// KnowledgeBaseModel defines the structure of a knowledge base
type KnowledgeBaseModel struct {
	UID  types.KBUIDType `gorm:"column:uid;type:uuid;default:uuid_generate_v4();primaryKey" json:"uid"`
	KBID string          `gorm:"column:id;size:255;not null" json:"kb_id"`
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

	// Embedding configuration stored as JSONB
	// Format: {"model_family": "gemini", "dimensionality": 3072}
	EmbeddingConfig EmbeddingConfigJSON `gorm:"column:embedding_config;type:jsonb" json:"embedding_config"`

	// ActiveCollectionUID points to the Milvus collection currently used by this KB
	// This allows collection versioning when embedding dimensions change
	// During updates with dimension changes:
	// - New collection is created with new dimensionality
	// - active_collection_uid is swapped to point to new collection
	// - Old collection is preserved in rollback KB for potential rollback
	// Note: Use uuid.Nil (all zeros) to represent NULL/unset values
	ActiveCollectionUID types.KBUIDType `gorm:"column:active_collection_uid;type:uuid" json:"active_collection_uid"`

	// Staging flag for KB update management
	// staging=false: Production KB (actively used for queries)
	// staging=true: Staging/rollback KB (held for potential rollback)
	Staging                bool       `gorm:"column:staging;not null;default:false" json:"staging"`
	UpdateStatus           string     `gorm:"column:update_status;size:50" json:"update_status"`
	UpdateWorkflowID       string     `gorm:"column:update_workflow_id;size:255" json:"update_workflow_id"`
	UpdateStartedAt        *time.Time `gorm:"column:update_started_at" json:"update_started_at"`
	UpdateCompletedAt      *time.Time `gorm:"column:update_completed_at" json:"update_completed_at"`
	RollbackRetentionUntil *time.Time `gorm:"column:rollback_retention_until" json:"rollback_retention_until"`
}

// EmbeddingConfigJSON represents the embedding configuration in the database
type EmbeddingConfigJSON struct {
	ModelFamily    string `json:"model_family"`
	Dimensionality uint32 `json:"dimensionality"`
}

// Scan implements the Scanner interface for EmbeddingConfigJSON
func (e *EmbeddingConfigJSON) Scan(value any) error {
	if value == nil {
		return nil
	}

	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("failed to unmarshal JSONB value: %v", value)
	}

	return json.Unmarshal(bytes, e)
}

// Value implements the driver Valuer interface for EmbeddingConfigJSON
func (e EmbeddingConfigJSON) Value() (driver.Value, error) {
	if e.ModelFamily == "" {
		return nil, nil
	}
	jsonBytes, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	return string(jsonBytes), nil
}

// TableName overrides the default table name for GORM
func (KnowledgeBaseModel) TableName() string {
	return "knowledge_base"
}

// KnowledgeBaseColumns is the columns for the knowledge base table
type KnowledgeBaseColumns struct {
	UID                    string
	KBID                   string
	Name                   string
	Description            string
	Tags                   string
	Owner                  string
	CreateTime             string
	UpdateTime             string
	DeleteTime             string
	Usage                  string
	CatalogType            string
	ActiveCollectionUID    string
	Staging                string
	UpdateStatus           string
	UpdateWorkflowID       string
	UpdateStartedAt        string
	UpdateCompletedAt      string
	RollbackRetentionUntil string
}

// KnowledgeBaseColumn is the columns for the knowledge base table
var KnowledgeBaseColumn = KnowledgeBaseColumns{
	UID:                    "uid",
	KBID:                   "id",
	Name:                   "name",
	Description:            "description",
	Tags:                   "tags",
	Owner:                  "owner",
	CreateTime:             "create_time",
	UpdateTime:             "update_time",
	DeleteTime:             "delete_time",
	Usage:                  "usage",
	CatalogType:            "catalog_type",
	ActiveCollectionUID:    "active_collection_uid",
	Staging:                "staging",
	UpdateStatus:           "update_status",
	UpdateWorkflowID:       "update_workflow_id",
	UpdateStartedAt:        "update_started_at",
	UpdateCompletedAt:      "update_completed_at",
	RollbackRetentionUntil: "rollback_retention_until",
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
		KBIDExists, err := r.checkIfKBIDUnique(ctx, kb.Owner, kb.KBID)
		if err != nil {
			return err
		}
		if KBIDExists {
			return fmt.Errorf("knowledge base name %q already exists: %w", kb.KBID, errorsx.ErrAlreadyExists)
		}

		// Create a new KnowledgeBaseModel record
		if err := tx.Create(&kb).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("knowledge base ID not found: %v, err:%w", kb.KBID, gorm.ErrRecordNotFound)
			}
			return err
		}

		// After Create(), kb.UID is now set by the database
		// If active_collection_uid is not set (uuid.Nil), default it to the KB's own UID (legacy behavior)
		// This maintains backward compatibility for existing code paths
		if kb.ActiveCollectionUID == uuid.Nil {
			kb.ActiveCollectionUID = kb.UID
			if err := tx.Model(&KnowledgeBaseModel{}).Where("uid = ?", kb.UID).Update("active_collection_uid", kb.UID).Error; err != nil {
				return fmt.Errorf("setting active_collection_uid: %w", err)
			}
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
func (r *repository) ListKnowledgeBasesByCatalogType(ctx context.Context, owner string, catalogType artifactpb.CatalogType) ([]KnowledgeBaseModel, error) {
	var knowledgeBases []KnowledgeBaseModel
	// Exclude records where DeleteTime is not null and filter by owner
	whereString := fmt.Sprintf("%v IS NULL AND %v = ? AND %v = ?", KnowledgeBaseColumn.DeleteTime, KnowledgeBaseColumn.Owner, KnowledgeBaseColumn.CatalogType)
	if err := r.db.WithContext(ctx).Where(whereString, owner, catalogType.String()).Find(&knowledgeBases).Error; err != nil {
		return nil, err
	}

	return knowledgeBases, nil
}

// UpdateKnowledgeBase updates a KnowledgeBaseModel record in the database.
// For the atomic swap, use this method with all necessary fields (Name, KBID, Staging, etc.)
func (r *repository) UpdateKnowledgeBase(ctx context.Context, id, owner string, kb KnowledgeBaseModel) (*KnowledgeBaseModel, error) {
	where := fmt.Sprintf("%s = ? AND %s = ? AND %s is NULL", KnowledgeBaseColumn.KBID, KnowledgeBaseColumn.Owner, KnowledgeBaseColumn.DeleteTime)

	// Update all non-zero fields of the record using struct (GORM ignores zero values)
	// This allows atomic swap to update Name, KBID, Staging, UpdateStatus, etc.
	updatedKB := new(KnowledgeBaseModel)
	err := r.db.WithContext(ctx).
		Clauses(clause.Returning{}).
		Model(&updatedKB).
		Where(where, id, owner).
		Updates(kb). // Pass entire struct - GORM updates all non-zero fields
		Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("knowledge base %s/%s not found", id, owner)
		}
		return nil, fmt.Errorf("updating record: %w", err)
	}

	return updatedKB, nil
}

// UpdateKnowledgeBaseWithMap updates a knowledge base using a map, allowing zero values like false
func (r *repository) UpdateKnowledgeBaseWithMap(ctx context.Context, id, owner string, updates map[string]interface{}) error {
	where := fmt.Sprintf("%s = ? AND %s = ? AND %s is NULL", KnowledgeBaseColumn.KBID, KnowledgeBaseColumn.Owner, KnowledgeBaseColumn.DeleteTime)

	// Convert tags if present to TagsArray type for proper PostgreSQL array handling
	if tags, ok := updates["tags"]; ok {
		if tagSlice, ok := tags.([]string); ok {
			updates["tags"] = TagsArray(tagSlice)
		}
	}

	err := r.db.WithContext(ctx).
		Model(&KnowledgeBaseModel{}).
		Where(where, id, owner).
		Updates(updates).
		Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("knowledge base %s/%s not found", id, owner)
		}
		return fmt.Errorf("updating record: %w", err)
	}

	return nil
}

// UpdateKBUIDInResources updates kb_uid in all resource tables (files, chunks, embeddings, converted_files)
// This is CRITICAL for atomic swap: when KBs are swapped, all resources must follow their KBs.
// Without this, queries will fail because resources point to old KB UIDs.
func (r *repository) UpdateKnowledgeBaseResources(ctx context.Context, fromKBUID, toKBUID types.KBUIDType) error {
	tables := []string{
		"knowledge_base_file",
		"text_chunk",
		"embedding",
		"converted_file",
	}

	// Use a transaction to ensure all updates succeed or none do
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, table := range tables {
			query := fmt.Sprintf("UPDATE %s SET kb_uid = ? WHERE kb_uid = ?", table)
			result := tx.Exec(query, toKBUID, fromKBUID)
			if result.Error != nil {
				return fmt.Errorf("updating kb_uid in %s: %w", table, result.Error)
			}
		}
		return nil
	})
}

// DeleteKnowledgeBase sets the DeleteTime to the current time to perform a soft delete.
func (r *repository) DeleteKnowledgeBase(ctx context.Context, ownerUID string, kbID string) (*KnowledgeBaseModel, error) {
	// Fetch the existing record to ensure it exists
	var existingKB KnowledgeBaseModel
	conds := fmt.Sprintf("%v = ? AND %v = ? AND %v IS NULL", KnowledgeBaseColumn.Owner, KnowledgeBaseColumn.KBID, KnowledgeBaseColumn.DeleteTime)
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
			return nil, fmt.Errorf("knowledge base ID not found: %v. err: %w", existingKB.KBID, gorm.ErrRecordNotFound)
		}
		return nil, err
	}

	return &existingKB, nil
}

func (r *repository) checkIfKBIDUnique(ctx context.Context, owner string, kbID string) (bool, error) {
	var existingKB KnowledgeBaseModel
	whereString := fmt.Sprintf("%v = ? AND %v = ? AND %s is NULL", KnowledgeBaseColumn.Owner, KnowledgeBaseColumn.KBID, KnowledgeBaseColumn.DeleteTime)
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
	whereString := fmt.Sprintf("%v = ? AND %v = ? AND %v is NULL", KnowledgeBaseColumn.Owner, KnowledgeBaseColumn.KBID, KnowledgeBaseColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereString, owner, kbID).First(&existingKB).Error; err != nil {
		return nil, err
	}
	return &existingKB, nil
}

// GetKnowledgeBaseByID gets a knowledge base by catalog ID (without owner filtering)
func (r *repository) GetKnowledgeBaseByID(ctx context.Context, kbID string) (*KnowledgeBaseModel, error) {
	var kb KnowledgeBaseModel
	whereString := fmt.Sprintf("%v = ? AND %v is NULL", KnowledgeBaseColumn.KBID, KnowledgeBaseColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereString, kbID).First(&kb).Error; err != nil {
		return nil, err
	}
	return &kb, nil
}

// ListKnowledgeBasesByUpdateStatus lists all knowledge bases with a specific update status
func (r *repository) ListKnowledgeBasesByUpdateStatus(ctx context.Context, updateStatus string) ([]KnowledgeBaseModel, error) {
	var kbs []KnowledgeBaseModel
	whereString := fmt.Sprintf("%v = ? AND %v is NULL", KnowledgeBaseColumn.UpdateStatus, KnowledgeBaseColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereString, updateStatus).Find(&kbs).Error; err != nil {
		return nil, err
	}
	return kbs, nil
}

// get the count of knowledge bases by owner
func (r *repository) GetKnowledgeBaseCountByOwner(ctx context.Context, owner string, catalogType artifactpb.CatalogType) (int64, error) {
	var count int64
	whereString := fmt.Sprintf("%v = ? AND %v is NULL AND %v = ?", KnowledgeBaseColumn.Owner, KnowledgeBaseColumn.DeleteTime, KnowledgeBaseColumn.CatalogType)
	if err := r.db.WithContext(ctx).Model(&KnowledgeBaseModel{}).Where(whereString, owner, catalogType.String()).Count(&count).Error; err != nil {
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

// GetActiveCollectionUID retrieves the active collection UID for a KB
// This enables collection versioning to support embedding dimension changes
func (r *repository) GetActiveCollectionUID(ctx context.Context, kbUID types.KBUIDType) (*types.KBUIDType, error) {
	var kb KnowledgeBaseModel
	err := r.db.WithContext(ctx).
		Select("active_collection_uid").
		Where("uid = ?", kbUID).
		First(&kb).
		Error
	if err != nil {
		return nil, fmt.Errorf("getting active collection UID for KB %s: %w", kbUID, err)
	}
	if kb.ActiveCollectionUID == uuid.Nil {
		// Fallback: if active_collection_uid is not set, use the KB's own UID (legacy behavior)
		activeCollectionUID := kbUID
		return &activeCollectionUID, nil
	}
	return &kb.ActiveCollectionUID, nil
}

// IsCollectionInUse checks if a collection is still referenced by any KB
// This prevents premature deletion during cleanup when collections are shared across KB versions
func (r *repository) IsCollectionInUse(ctx context.Context, collectionUID types.KBUIDType) (bool, error) {
	var count int64
	err := r.db.WithContext(ctx).
		Model(&KnowledgeBaseModel{}).
		Where("active_collection_uid = ? AND delete_time IS NULL", collectionUID).
		Count(&count).
		Error
	if err != nil {
		return false, fmt.Errorf("checking if collection %s is in use: %w", collectionUID, err)
	}
	return count > 0, nil
}

// IsKBUpdating checks if a KB is currently in updating state
// Used to detect when files should be dual-processed (production + staging)
func (r *repository) IsKBUpdating(ctx context.Context, kbUID types.KBUIDType) (bool, error) {
	var updateStatus string
	err := r.db.WithContext(ctx).
		Model(&KnowledgeBaseModel{}).
		Select("update_status").
		Where("uid = ? AND delete_time IS NULL", kbUID).
		Scan(&updateStatus).
		Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return false, nil
		}
		return false, fmt.Errorf("checking KB update status: %w", err)
	}
	return updateStatus == artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_UPDATING.String(), nil
}

// GetStagingKBForProduction finds the staging KB for a production catalog during an update
// Returns nil if no staging KB exists (no update in progress)
func (r *repository) GetStagingKBForProduction(ctx context.Context, ownerUID types.OwnerUIDType, productionCatalogID string) (*KnowledgeBaseModel, error) {
	// Staging KB naming convention: {production-catalog-id}-staging
	stagingCatalogID := fmt.Sprintf("%s-staging", productionCatalogID)

	var stagingKB KnowledgeBaseModel
	err := r.db.WithContext(ctx).
		Where(fmt.Sprintf("%v = ? AND %v = ? AND %v = ? AND %v IS NULL",
			KnowledgeBaseColumn.Owner,
			KnowledgeBaseColumn.KBID,
			KnowledgeBaseColumn.Staging,
			KnowledgeBaseColumn.DeleteTime),
			ownerUID, stagingCatalogID, true).
		First(&stagingKB).
		Error

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			// No staging KB found - not an error, just means no update in progress
			return nil, nil
		}
		return nil, fmt.Errorf("finding staging KB for %s: %w", productionCatalogID, err)
	}

	return &stagingKB, nil
}

// GetRollbackKBForProduction finds the rollback KB for a production catalog during retention period
// Returns nil if no rollback KB exists (no retention period active)
func (r *repository) GetRollbackKBForProduction(ctx context.Context, ownerUID types.OwnerUIDType, productionCatalogID string) (*KnowledgeBaseModel, error) {
	// Rollback KB naming convention: {production-catalog-id}-rollback
	rollbackCatalogID := fmt.Sprintf("%s-rollback", productionCatalogID)

	var rollbackKB KnowledgeBaseModel
	err := r.db.WithContext(ctx).
		Where(fmt.Sprintf("%v = ? AND %v = ? AND %v = ? AND %v IS NULL",
			KnowledgeBaseColumn.Owner,
			KnowledgeBaseColumn.KBID,
			KnowledgeBaseColumn.Staging,
			KnowledgeBaseColumn.DeleteTime),
			ownerUID, rollbackCatalogID, true).
		First(&rollbackKB).
		Error

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			// No rollback KB found - not an error, just means no retention period active
			return nil, nil
		}
		return nil, fmt.Errorf("finding rollback KB for %s: %w", productionCatalogID, err)
	}

	return &rollbackKB, nil
}

// DualProcessingTarget represents the target KB for dual processing
type DualProcessingTarget struct {
	IsNeeded    bool
	TargetKB    *KnowledgeBaseModel
	Phase       string // "updating", "swapping", or "retention"
	Description string // Human-readable description for logging
}

// GetDualProcessingTarget determines if dual processing is needed and returns the target KB
// Returns a DualProcessingTarget with IsNeeded=false if no dual processing is needed
//
// Dual processing is needed in four scenarios:
// 1. Phase 2 (updating): Staging KB exists - full dual processing workflow
// 2. Phase 3 (swapping): Staging KB exists - file synchronization (minimal processing)
// 3. Phase 6 (retention after update): Rollback KB exists - full dual processing with old RAG config
// 4. Phase 6 (retention after rollback): Rollback KB exists - full dual processing (continue retention)
func (r *repository) GetDualProcessingTarget(ctx context.Context, productionKB *KnowledgeBaseModel) (*DualProcessingTarget, error) {
	result := &DualProcessingTarget{
		IsNeeded: false,
	}

	// Check if production KB is in a state that requires dual processing
	status := productionKB.UpdateStatus

	// Scenario 1 & 2: Update in progress (Phase 2-5: updating, swapping, validating, syncing)
	// Files uploaded during ANY active update phase should be dual-processed to both production and staging KBs
	//
	// CRITICAL FIX: During "swapping" status, after the actual swap happens (Phase 5), the staging KB is deleted
	// and rollback KB is created. So we need to check BOTH: first try staging, then fallback to rollback.
	if IsUpdateInProgress(status) {
		ownerUID := types.OwnerUIDType(uuid.FromStringOrNil(productionKB.Owner))

		// First, try to find staging KB (exists before swap)
		stagingKB, err := r.GetStagingKBForProduction(ctx, ownerUID, productionKB.KBID)
		if err != nil {
			return nil, fmt.Errorf("checking for staging KB: %w", err)
		}
		if stagingKB != nil {
			result.IsNeeded = true
			result.TargetKB = stagingKB
			result.Phase = status
			switch status {
			case artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_UPDATING.String():
				result.Description = "Update in progress (Phase 2: Reprocess) - full dual processing with staging KB"
			case artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_SWAPPING.String():
				result.Description = "Update in progress (Phase 3-5: Pre-Swap) - file synchronization with staging KB"
			case artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_VALIDATING.String():
				result.Description = "Update in progress (Phase 4: Validation) - file synchronization with staging KB"
			case artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_SYNCING.String():
				result.Description = "Update in progress (Phase 5: Pre-Swap Sync) - file synchronization with staging KB"
			default:
				result.Description = fmt.Sprintf("Update in progress (Phase: %s) - file synchronization with staging KB", status)
			}
			return result, nil
		}

		// If staging KB doesn't exist during update (swap already happened), check for rollback KB
		// This happens when we're in "swapping" status but SwapKnowledgeBasesActivity has already run
		rollbackKB, err := r.GetRollbackKBForProduction(ctx, ownerUID, productionKB.KBID)
		if err != nil {
			return nil, fmt.Errorf("checking for rollback KB during post-swap: %w", err)
		}
		if rollbackKB != nil {
			result.IsNeeded = true
			result.TargetKB = rollbackKB
			result.Phase = status
			result.Description = fmt.Sprintf("Update in progress (Phase: %s, Post-Swap) - file synchronization with rollback KB", status)
			return result, nil
		}
	}

	// Scenario 3 & 4: Retention period active (Phase 6)
	// - After successful update: status = KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED
	// - After rollback: status = KNOWLEDGE_BASE_UPDATE_STATUS_ROLLED_BACK
	// In both cases, if rollback KB exists, continue dual processing
	if status == artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED.String() ||
		status == artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_ROLLED_BACK.String() {
		ownerUID := types.OwnerUIDType(uuid.FromStringOrNil(productionKB.Owner))
		rollbackKB, err := r.GetRollbackKBForProduction(ctx, ownerUID, productionKB.KBID)
		if err != nil {
			return nil, fmt.Errorf("checking for rollback KB: %w", err)
		}
		if rollbackKB != nil {
			result.IsNeeded = true
			result.TargetKB = rollbackKB
			result.Phase = "retention"
			if status == artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED.String() {
				result.Description = "Retention period active after update (Phase 6: Cleanup) - full dual processing with rollback KB (old RAG config)"
			} else {
				result.Description = "Retention period active after rollback (Phase 6: Cleanup) - full dual processing with rollback KB (rolled-back RAG config)"
			}
			return result, nil
		}
	}

	// No dual processing needed
	return result, nil
}

// CreateStagingKnowledgeBase creates a staging KB for update with a new UID
// If newEmbeddingConfig is provided, uses it; otherwise uses original's config
func (r *repository) CreateStagingKnowledgeBase(ctx context.Context, original *KnowledgeBaseModel, newEmbeddingConfig *EmbeddingConfigJSON, externalService func(kbUID types.KBUIDType) error) (*KnowledgeBaseModel, error) {
	now := time.Now()

	// Use new config if provided, otherwise use original's config
	embeddingConfig := original.EmbeddingConfig
	if newEmbeddingConfig != nil {
		embeddingConfig = *newEmbeddingConfig
	}

	stagingKB := KnowledgeBaseModel{
		// New UID is generated automatically by GORM
		// Shadow KB naming: {original}-staging (simpler than version-based naming)
		Name:            fmt.Sprintf("%s-staging", original.Name),
		KBID:            fmt.Sprintf("%s-staging", original.KBID),
		Description:     original.Description,
		Tags:            append(original.Tags, "staging"),
		Owner:           original.Owner,
		CreatorUID:      original.CreatorUID,
		CatalogType:     original.CatalogType,
		EmbeddingConfig: embeddingConfig,
		Staging:         true, // Mark as staging for staging KB
		UpdateStatus:    artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_UPDATING.String(),
		CreateTime:      &now,
		UpdateTime:      &now,
		// ActiveCollectionUID will be set to staging KB's own UID by CreateKnowledgeBase
		// This allows the staging KB to have its own collection with potentially different dimensionality
	}

	return r.CreateKnowledgeBase(ctx, stagingKB, externalService)
}

// ListKnowledgeBasesForUpdate finds KBs ready for the next update cycle
// It filters for production KBs that either have never been updated or have completed updates
func (r *repository) ListKnowledgeBasesForUpdate(ctx context.Context, tagFilters []string, catalogIDs []string) ([]KnowledgeBaseModel, error) {
	var kbs []KnowledgeBaseModel

	// Filter: Not deleted
	query := r.db.WithContext(ctx).Where(fmt.Sprintf("%v IS NULL", KnowledgeBaseColumn.DeleteTime))

	// Filter: Only production KBs (not staging/rollback)
	query = query.Where(fmt.Sprintf("%v = ?", KnowledgeBaseColumn.Staging), false)

	// Filter: Only KBs that have never been updated OR have completed updates
	query = query.Where(
		fmt.Sprintf("(%v IS NULL OR %v = ?)", KnowledgeBaseColumn.UpdateStatus, KnowledgeBaseColumn.UpdateStatus),
		artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED.String(),
	)

	// Filter by tags if specified (use OR logic - match any tag)
	if len(tagFilters) > 0 {
		for i, tag := range tagFilters {
			if i == 0 {
				query = query.Where("? = ANY(tags)", tag)
			} else {
				query = query.Or("? = ANY(tags)", tag)
			}
		}
	}

	// Filter by specific catalog IDs if provided
	if len(catalogIDs) > 0 {
		query = query.Where(fmt.Sprintf("%v IN ?", KnowledgeBaseColumn.KBID), catalogIDs)
	}

	if err := query.Find(&kbs).Error; err != nil {
		return nil, err
	}

	return kbs, nil
}

// UpdateKnowledgeBaseUpdateStatus updates the update status and workflow ID
func (r *repository) UpdateKnowledgeBaseUpdateStatus(ctx context.Context, kbUID types.KBUIDType, status string, workflowID string) error {
	updates := map[string]any{
		"update_status": status,
	}

	// Only set workflow ID if it's being started (UPDATING)
	// For all terminal states (COMPLETED, FAILED, ABORTED), explicitly clear it to NULL
	switch status {
	case artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_UPDATING.String():
		now := time.Now()
		updates["update_started_at"] = &now
		updates["update_workflow_id"] = workflowID
	case artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED.String(),
		artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_FAILED.String(),
		artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_ABORTED.String(),
		artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_ROLLED_BACK.String():
		now := time.Now()
		updates["update_completed_at"] = &now
		updates["update_workflow_id"] = nil // Explicitly clear to NULL for all terminal states
	default:
		// For intermediate states (SYNCING, VALIDATING, SWAPPING), keep workflow ID if provided
		if workflowID != "" {
			updates["update_workflow_id"] = workflowID
		}
	}

	return r.db.WithContext(ctx).Model(&KnowledgeBaseModel{}).
		Where("uid = ?", kbUID).
		Updates(updates).Error
}

// UpdateKnowledgeBaseAborted sets the KB status to ABORTED and explicitly clears the workflow ID to NULL
// This is necessary because GORM's Updates() with empty string "" doesn't set it to NULL in the database
// and the DeleteCatalog safeguards check for != "" which would still block deletion
func (r *repository) UpdateKnowledgeBaseAborted(ctx context.Context, kbUID types.KBUIDType) error {
	now := time.Now()
	return r.db.WithContext(ctx).Model(&KnowledgeBaseModel{}).
		Where("uid = ?", kbUID).
		Updates(map[string]any{
			"update_status":       artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_ABORTED.String(),
			"update_workflow_id":  nil, // Explicitly set to NULL
			"update_completed_at": &now,
		}).Error
}
