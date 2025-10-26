package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/instill-ai/artifact-backend/internal/ai"
	"github.com/instill-ai/artifact-backend/pkg/types"
)

// System interface defines methods for system-wide configurations
type System interface {
	// GetSystem retrieves a complete system configuration by ID
	GetSystem(ctx context.Context, id string) (*SystemModel, error)
	// GetSystemByUID retrieves a complete system configuration by UID
	GetSystemByUID(ctx context.Context, uid types.SystemUIDType) (*SystemModel, error)

	// CreateSystem creates a new system configuration (fails if ID already exists)
	CreateSystem(ctx context.Context, id string, config map[string]any, description string) error

	// UpdateSystem updates an existing system configuration (fails if ID doesn't exist)
	UpdateSystem(ctx context.Context, id string, config map[string]any, description string) error

	// ListSystems lists all system configurations
	ListSystems(ctx context.Context) ([]SystemModel, error)

	// DeleteSystem deletes a system configuration
	DeleteSystem(ctx context.Context, id string) error

	// RenameSystemByID changes the ID of a system configuration
	RenameSystemByID(ctx context.Context, id string, newID string) error

	// GetConfigByID retrieves the system configuration for a given system ID
	// If the ID doesn't exist, it falls back to "default"
	// If "default" doesn't exist, it returns hardcoded new standard (Gemini/3072)
	GetConfigByID(ctx context.Context, id string) (*SystemConfigJSON, error)

	// UpdateConfigByID updates the system configuration for a system ID
	UpdateConfigByID(ctx context.Context, id string, config SystemConfigJSON) error

	// GetDefaultSystem retrieves the current default system configuration
	GetDefaultSystem(ctx context.Context) (*SystemModel, error)

	// SetDefaultSystem sets a system as the default, unsetting any previous default
	SetDefaultSystem(ctx context.Context, id string) error
}

// SystemModel represents the system table structure
// Follows standard resource table pattern with uid, id, and timestamps
// Systems are global resources (no owner field, resource name is computed as systems/{id})
type SystemModel struct {
	UID         types.SystemUIDType `gorm:"column:uid;type:uuid;default:uuid_generate_v4();primaryKey" json:"uid"`
	ID          string              `gorm:"column:id;type:varchar(255);not null;unique" json:"id"` // User-facing ID (e.g., "openai", "gemini")
	Config      map[string]any      `gorm:"column:config;type:jsonb;not null;serializer:json" json:"config"`
	Description string              `gorm:"column:description;type:text" json:"description"`
	IsDefault   bool                `gorm:"column:is_default;type:boolean;not null;default:false" json:"is_default"`
	CreateTime  *time.Time          `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime  *time.Time          `gorm:"column:update_time;not null;autoUpdateTime" json:"update_time"`
	DeleteTime  gorm.DeletedAt      `gorm:"column:delete_time;index" json:"delete_time"`
}

// TableName overrides the default table name for GORM
func (SystemModel) TableName() string {
	return "system"
}

// GetConfigJSON converts the Config map to SystemConfigJSON
func (s *SystemModel) GetConfigJSON() (*SystemConfigJSON, error) {
	// The Config is stored as map[string]any by GORM's JSONB deserializer
	// We need to convert it to our typed struct

	// Marshal to JSON bytes then unmarshal to SystemConfigJSON
	jsonBytes, err := json.Marshal(s.Config)
	if err != nil {
		return nil, fmt.Errorf("marshaling config to JSON: %w", err)
	}

	var config SystemConfigJSON
	if err := json.Unmarshal(jsonBytes, &config); err != nil {
		return nil, fmt.Errorf("unmarshaling config to SystemConfigJSON (raw: %s): %w", string(jsonBytes), err)
	}

	// Validate the config has required fields
	if config.RAG.Embedding.ModelFamily == "" {
		return nil, fmt.Errorf("invalid config: missing model_family (raw: %s)", string(jsonBytes))
	}
	if config.RAG.Embedding.Dimensionality == 0 {
		return nil, fmt.Errorf("invalid config: dimensionality is 0 (raw: %s)", string(jsonBytes))
	}

	return &config, nil
}

// GetSystem retrieves a complete system configuration by ID
// Filters out soft-deleted records (automatically handled by gorm.DeletedAt)
func (r *repository) GetSystem(ctx context.Context, id string) (*SystemModel, error) {
	var system SystemModel

	err := r.db.WithContext(ctx).
		Where("id = ?", id).
		First(&system).Error

	if err != nil {
		return nil, err
	}

	return &system, nil
}

// GetSystemByUID retrieves a complete system configuration by UID
// Filters out soft-deleted records (automatically handled by gorm.DeletedAt)
func (r *repository) GetSystemByUID(ctx context.Context, uid types.SystemUIDType) (*SystemModel, error) {
	var system SystemModel

	err := r.db.WithContext(ctx).
		Where("uid = ?", uid).
		First(&system).Error

	if err != nil {
		return nil, err
	}

	return &system, nil
}

// CreateSystem creates a new system configuration
// Returns error if a system with the same ID already exists
func (r *repository) CreateSystem(ctx context.Context, id string, config map[string]any, description string) error {
	now := time.Now()

	system := SystemModel{
		ID:          id,
		Config:      config,
		Description: description,
		IsDefault:   false, // New systems are not default by default
		CreateTime:  &now,
		UpdateTime:  &now,
	}

	// Create will fail if ID already exists (unique constraint)
	return r.db.WithContext(ctx).Create(&system).Error
}

// UpdateSystem updates an existing system configuration
// Returns error if the system doesn't exist
func (r *repository) UpdateSystem(ctx context.Context, id string, config map[string]any, description string) error {
	// Verify the system exists first
	var existing SystemModel
	if err := r.db.WithContext(ctx).Where("id = ?", id).First(&existing).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("system with id %q not found", id)
		}
		return err
	}

	// Update only the specified fields
	updates := map[string]interface{}{
		"config":      config,
		"description": description,
		"update_time": time.Now(),
	}

	return r.db.WithContext(ctx).
		Model(&SystemModel{}).
		Where("id = ?", id).
		Updates(updates).Error
}

// ListSystems lists all system configurations
// Filters out soft-deleted records
func (r *repository) ListSystems(ctx context.Context) ([]SystemModel, error) {
	var systems []SystemModel

	err := r.db.WithContext(ctx).
		Order("id").
		Find(&systems).Error

	if err != nil {
		return nil, err
	}

	return systems, nil
}

// DeleteSystem soft-deletes a system configuration
func (r *repository) DeleteSystem(ctx context.Context, id string) error {
	// Prevent deletion of the openai system (default for existing KBs)
	if id == "openai" {
		return errors.New("cannot delete openai system configuration")
	}

	return r.db.WithContext(ctx).
		Where("id = ?", id).
		Delete(&SystemModel{}).Error
}

// RenameSystemByID changes the ID of a system configuration
func (r *repository) RenameSystemByID(ctx context.Context, id string, newID string) error {
	// Prevent renaming the openai system
	if id == "openai" {
		return errors.New("cannot rename openai system configuration")
	}

	result := r.db.WithContext(ctx).
		Model(&SystemModel{}).
		Where("id = ?", id).
		Update("id", newID)

	if result.Error != nil {
		return result.Error
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf("system with id %q not found", id)
	}

	return nil
}

// GetConfigByID retrieves the system configuration for a given system ID
func (r *repository) GetConfigByID(ctx context.Context, id string) (*SystemConfigJSON, error) {
	system, err := r.GetSystem(ctx, id)

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Fallback to 'openai' if specific ID not found (default for existing KBs)
			if id != "openai" {
				return r.GetConfigByID(ctx, "openai")
			}
			// Final fallback to hardcoded OpenAI standard if 'openai' doesn't exist
			return &SystemConfigJSON{
				RAG: RAGConfig{
					Embedding: EmbeddingConfig{
						ModelFamily:    ai.ModelFamilyOpenAI,
						Dimensionality: uint32(ai.OpenAIEmbeddingDim),
					},
				},
			}, nil
		}
		return nil, err
	}

	// Navigate the nested config: config.rag.embedding
	ragConfig, ok := system.Config["rag"].(map[string]any)
	if !ok {
		// No rag config, fallback to openai or hardcoded
		if id != "openai" {
			return r.GetConfigByID(ctx, "openai")
		}
		return &SystemConfigJSON{
			RAG: RAGConfig{
				Embedding: EmbeddingConfig{
					ModelFamily:    ai.ModelFamilyOpenAI,
					Dimensionality: uint32(ai.OpenAIEmbeddingDim),
				},
			},
		}, nil
	}

	embeddingConfig, ok := ragConfig["embedding"].(map[string]any)
	if !ok {
		// No embedding config, fallback
		if id != "openai" {
			return r.GetConfigByID(ctx, "openai")
		}
		return &SystemConfigJSON{
			RAG: RAGConfig{
				Embedding: EmbeddingConfig{
					ModelFamily:    ai.ModelFamilyOpenAI,
					Dimensionality: uint32(ai.OpenAIEmbeddingDim),
				},
			},
		}, nil
	}

	// Extract model_family and dimensionality
	modelFamily, _ := embeddingConfig["model_family"].(string)

	var dimensionality uint32
	switch v := embeddingConfig["dimensionality"].(type) {
	case float64:
		dimensionality = uint32(v)
	case int:
		dimensionality = uint32(v)
	case int64:
		dimensionality = uint32(v)
	}

	return &SystemConfigJSON{
		RAG: RAGConfig{
			Embedding: EmbeddingConfig{
				ModelFamily:    modelFamily,
				Dimensionality: dimensionality,
			},
		},
	}, nil
}

// UpdateConfigByID updates the system configuration for a system ID
func (r *repository) UpdateConfigByID(ctx context.Context, id string, config SystemConfigJSON) error {
	// Verify system exists
	_, err := r.GetSystem(ctx, id)
	if err != nil {
		return err
	}

	// Set the entire config from the struct
	// The SystemConfigJSON will be marshaled to proper JSON structure
	configJSON, err := json.Marshal(config)
	if err != nil {
		return err
	}

	var configMap map[string]any
	if err := json.Unmarshal(configJSON, &configMap); err != nil {
		return err
	}

	// Save the updated system
	return r.UpdateSystem(ctx, id, configMap, "System configuration for knowledge bases")
}

// GetDefaultSystem retrieves the current default system configuration
func (r *repository) GetDefaultSystem(ctx context.Context) (*SystemModel, error) {
	var system SystemModel
	if err := r.db.WithContext(ctx).Where("is_default = ?", true).First(&system).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// No default system set, fallback to "openai"
			return r.GetSystem(ctx, "openai")
		}
		return nil, err
	}
	return &system, nil
}

// SetDefaultSystem sets a system as the default, unsetting any previous default
func (r *repository) SetDefaultSystem(ctx context.Context, id string) error {
	// Verify the system exists and is not soft-deleted
	system, err := r.GetSystem(ctx, id)
	if err != nil {
		return err
	}
	if system == nil {
		return fmt.Errorf("system with id %s not found", id)
	}

	// Use a transaction to ensure atomic update:
	// 1. Unset any current default
	// 2. Set the new default
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Unset all defaults (should only be one, but to be safe)
		if err := tx.Model(&SystemModel{}).Where("is_default = ?", true).Update("is_default", false).Error; err != nil {
			return err
		}

		// Set new default
		if err := tx.Model(&SystemModel{}).Where("id = ?", id).Update("is_default", true).Error; err != nil {
			return err
		}

		return nil
	})
}
