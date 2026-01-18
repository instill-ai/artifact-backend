package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"gorm.io/gorm"

	"github.com/instill-ai/artifact-backend/pkg/ai"
	"github.com/instill-ai/artifact-backend/pkg/ai/openai"
	"github.com/instill-ai/artifact-backend/pkg/types"
	"github.com/instill-ai/artifact-backend/pkg/utils"
)

// System interface defines methods for system-wide configurations
type System interface {
	// GetSystem retrieves a complete system configuration by ID (hash-based canonical ID)
	GetSystem(ctx context.Context, id string) (*SystemModel, error)
	// GetSystemBySlug retrieves a complete system configuration by slug
	GetSystemBySlug(ctx context.Context, slug string) (*SystemModel, error)
	// GetSystemByIDOrSlug retrieves a system by ID (sys-xxx) or slug (openai, gemini)
	// This provides backward compatibility for clients that may use either format.
	GetSystemByIDOrSlug(ctx context.Context, idOrSlug string) (*SystemModel, error)
	// GetSystemByUID retrieves a complete system configuration by UID
	GetSystemByUID(ctx context.Context, uid types.SystemUIDType) (*SystemModel, error)

	// CreateSystem creates a new system configuration
	// The ID is auto-generated as "sys-{hash}" from the UID via BeforeCreate hook
	// displayName is used to generate the slug for human-readable identification
	CreateSystem(ctx context.Context, displayName string, config map[string]any, description string) (*SystemModel, error)

	// UpdateSystem updates an existing system configuration (fails if ID doesn't exist)
	UpdateSystem(ctx context.Context, id string, config map[string]any, description string) error

	// UpdateSystemByUpdateMap updates a system using a selective update map (fails if ID doesn't exist)
	UpdateSystemByUpdateMap(ctx context.Context, id string, updateFields map[string]interface{}) error

	// ListSystems lists all system configurations
	ListSystems(ctx context.Context) ([]SystemModel, error)

	// DeleteSystem deletes a system configuration
	DeleteSystem(ctx context.Context, id string) error

	// RenameSystemByID updates the display name and slug of a system configuration
	// The canonical ID (sys-xxx) is immutable. Only display_name and slug change.
	RenameSystemByID(ctx context.Context, systemID string, newDisplayName string) error

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

	// SeedDefaultSystems ensures the default system configurations exist
	// This is idempotent - systems are only created if they don't already exist (by slug)
	SeedDefaultSystems(ctx context.Context, presets []PresetSystem) error
}

// SystemModel represents the system table structure
// Follows AIP standard resource table pattern with uid, id, display_name, slug, and timestamps
// Systems are global resources (no owner field, resource name is computed as systems/{id})
type SystemModel struct {
	UID         types.SystemUIDType `gorm:"column:uid;type:uuid;default:uuid_generate_v4();primaryKey" json:"uid"`
	ID          string              `gorm:"column:id;type:varchar(255);not null;unique" json:"id"` // Hash-based ID (e.g., "sys-8f3a2k9e7c1")
	DisplayName string              `gorm:"column:display_name;type:varchar(255);not null" json:"display_name"`
	Slug        string              `gorm:"column:slug;type:varchar(255)" json:"slug"` // URL-friendly slug (e.g., "openai", "gemini")
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

// BeforeCreate is a GORM hook that auto-generates UID and ID before inserting
// ID format: "sys-{base62_hash}" following AIP resource ID convention
func (s *SystemModel) BeforeCreate(tx *gorm.DB) error {
	// Generate UID if not set (compare with uuid.Nil for UUID type)
	if uuid.UUID(s.UID) == uuid.Nil {
		s.UID = types.SystemUIDType(uuid.Must(uuid.NewV4()))
		tx.Statement.SetColumn("UID", s.UID)
	}

	// Generate hash-based ID from UID if not set
	if s.ID == "" {
		s.ID = utils.GeneratePrefixedResourceID(utils.PrefixSystem, uuid.UUID(s.UID))
		tx.Statement.SetColumn("ID", s.ID)
	}

	// Generate slug from display_name if not set
	if s.Slug == "" && s.DisplayName != "" {
		s.Slug = generateSlug(s.DisplayName)
		tx.Statement.SetColumn("Slug", s.Slug)
	}

	return nil
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
// ID is the hash-based canonical ID like "sys-a1b2c3d4e5f6g7h8"
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

// GetSystemBySlug retrieves a complete system configuration by slug
// Slug is the URL-friendly identifier like "openai", "gemini"
// Filters out soft-deleted records (automatically handled by gorm.DeletedAt)
func (r *repository) GetSystemBySlug(ctx context.Context, slug string) (*SystemModel, error) {
	var system SystemModel

	err := r.db.WithContext(ctx).
		Where("slug = ?", slug).
		First(&system).Error

	if err != nil {
		return nil, err
	}

	return &system, nil
}

// GetSystemByIDOrSlug retrieves a system by ID (sys-xxx) or slug (openai, gemini)
// This provides backward compatibility for clients that may use either format.
// First tries to find by ID, then falls back to slug lookup.
func (r *repository) GetSystemByIDOrSlug(ctx context.Context, idOrSlug string) (*SystemModel, error) {
	// First try by ID (canonical hash-based IDs start with "sys-")
	if strings.HasPrefix(idOrSlug, "sys-") {
		system, err := r.GetSystem(ctx, idOrSlug)
		if err == nil {
			return system, nil
		}
		// If not found by ID, don't fall back - it was explicitly an ID
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
		return nil, err
	}

	// Try by slug (for backward compatibility with "openai", "gemini", etc.)
	return r.GetSystemBySlug(ctx, idOrSlug)
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
// The ID is auto-generated as "sys-{hash}" from the UID via BeforeCreate hook
// displayName is used to generate the slug for human-readable identification
func (r *repository) CreateSystem(ctx context.Context, displayName string, config map[string]any, description string) (*SystemModel, error) {
	now := time.Now()

	system := SystemModel{
		// ID is auto-generated by BeforeCreate hook
		DisplayName: displayName,
		// Slug is auto-generated by BeforeCreate hook from DisplayName
		Config:      config,
		Description: description,
		IsDefault:   false, // New systems are not default by default
		CreateTime:  &now,
		UpdateTime:  &now,
	}

	// Create will auto-generate UID and ID via BeforeCreate hook
	if err := r.db.WithContext(ctx).Create(&system).Error; err != nil {
		return nil, err
	}

	return &system, nil
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

// UpdateSystemByUpdateMap updates a system by ID using a selective update map
// Only fields present in the updateFields map will be updated
// Returns error if the system doesn't exist
func (r *repository) UpdateSystemByUpdateMap(ctx context.Context, id string, updateFields map[string]interface{}) error {
	// Verify the system exists first
	var existing SystemModel
	if err := r.db.WithContext(ctx).Where("id = ?", id).First(&existing).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("system with id %q not found", id)
		}
		return err
	}

	// Handle config field specially - marshal to JSON string for JSONB column
	if configVal, ok := updateFields["config"]; ok {
		configBytes, err := json.Marshal(configVal)
		if err != nil {
			return fmt.Errorf("failed to marshal config: %w", err)
		}
		// PostgreSQL JSONB expects a string, not bytes
		updateFields["config"] = string(configBytes)
	}

	// Always update the timestamp
	updateFields["update_time"] = time.Now()

	return r.db.WithContext(ctx).
		Model(&SystemModel{}).
		Where("id = ?", id).
		Updates(updateFields).Error
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
	// First retrieve the system to check its slug
	system, err := r.GetSystem(ctx, id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("system with id %q not found", id)
		}
		return err
	}

	// Prevent deletion of protected systems (openai, gemini)
	protectedSlugs := []string{"openai", "gemini"}
	for _, slug := range protectedSlugs {
		if system.Slug == slug {
			return fmt.Errorf("cannot delete protected system %q (slug: %s)", id, slug)
		}
	}

	return r.db.WithContext(ctx).
		Where("id = ?", id).
		Delete(&SystemModel{}).Error
}

// RenameSystemByID updates the display name and slug of a system configuration
// The canonical ID (sys-xxx) is immutable. Only display_name and slug change.
func (r *repository) RenameSystemByID(ctx context.Context, systemID string, newDisplayName string) error {
	// First retrieve the system to check if it's protected
	system, err := r.GetSystem(ctx, systemID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("system with id %q not found", systemID)
		}
		return err
	}

	// Prevent renaming of protected systems (openai, gemini)
	protectedSlugs := []string{"openai", "gemini"}
	for _, slug := range protectedSlugs {
		if system.Slug == slug {
			return fmt.Errorf("cannot rename protected system %q (slug: %s)", systemID, slug)
		}
	}

	// Generate new slug from display name (lowercase, replace spaces with hyphens)
	newSlug := generateSlug(newDisplayName)

	// Update the system by ID (not slug)
	result := r.db.WithContext(ctx).
		Model(&SystemModel{}).
		Where("id = ?", systemID).
		Updates(map[string]interface{}{
			"display_name": newDisplayName,
			"slug":         newSlug,
		})

	if result.Error != nil {
		return result.Error
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf("system with id %q not found", systemID)
	}

	return nil
}

// generateSlug creates a URL-friendly slug from a display name
func generateSlug(displayName string) string {
	// Convert to lowercase and replace spaces with hyphens
	slug := strings.ToLower(displayName)
	slug = strings.ReplaceAll(slug, " ", "-")
	// Remove any characters that aren't alphanumeric or hyphens
	var result strings.Builder
	for _, r := range slug {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// GetConfigByID retrieves the system configuration for a given system ID
func (r *repository) GetConfigByID(ctx context.Context, id string) (*SystemConfigJSON, error) {
	system, err := r.GetSystem(ctx, id)

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Fallback to 'openai' system by slug if specific ID not found (default for existing KBs)
			return r.getConfigBySlugWithFallback(ctx, "openai")
		}
		return nil, err
	}

	return r.extractConfigFromSystem(ctx, system)
}

// getConfigBySlugWithFallback retrieves system config by slug with hardcoded fallback
func (r *repository) getConfigBySlugWithFallback(ctx context.Context, slug string) (*SystemConfigJSON, error) {
	system, err := r.GetSystemBySlug(ctx, slug)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Final fallback to hardcoded OpenAI standard if system doesn't exist
			return &SystemConfigJSON{
				RAG: RAGConfig{
					Embedding: EmbeddingConfig{
						ModelFamily:    ai.ModelFamilyOpenAI,
						Dimensionality: uint32(openai.DefaultEmbeddingDimension),
					},
				},
			}, nil
		}
		return nil, err
	}
	return r.extractConfigFromSystem(ctx, system)
}

// extractConfigFromSystem extracts SystemConfigJSON from a SystemModel
// If the system's config is incomplete, returns hardcoded OpenAI defaults to avoid infinite recursion
func (r *repository) extractConfigFromSystem(_ context.Context, system *SystemModel) (*SystemConfigJSON, error) {
	// Navigate the nested config: config.rag.embedding
	ragConfig, ok := system.Config["rag"].(map[string]any)
	if !ok {
		// No rag config, return hardcoded default to avoid infinite recursion
		return &SystemConfigJSON{
			RAG: RAGConfig{
				Embedding: EmbeddingConfig{
					ModelFamily:    ai.ModelFamilyOpenAI,
					Dimensionality: uint32(openai.DefaultEmbeddingDimension),
				},
			},
		}, nil
	}

	embeddingConfig, ok := ragConfig["embedding"].(map[string]any)
	if !ok {
		// No embedding config, return hardcoded default to avoid infinite recursion
		return &SystemConfigJSON{
			RAG: RAGConfig{
				Embedding: EmbeddingConfig{
					ModelFamily:    ai.ModelFamilyOpenAI,
					Dimensionality: uint32(openai.DefaultEmbeddingDimension),
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
			// No default system set, fallback to "openai" by slug
			return r.GetSystemBySlug(ctx, "openai")
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

// PresetSystem defines a preset system configuration for seeding
type PresetSystem struct {
	DisplayName    string
	Slug           string
	ModelFamily    string
	Dimensionality uint32
	Description    string
	IsDefault      bool
}

// SeedDefaultSystems ensures the default system configurations exist
// This is idempotent - systems are only created if they don't already exist (by slug)
func (r *repository) SeedDefaultSystems(ctx context.Context, presets []PresetSystem) error {
	for _, preset := range presets {
		// Check if system with this slug already exists
		_, err := r.GetSystemBySlug(ctx, preset.Slug)
		if err == nil {
			// System already exists, skip
			continue
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			// Unexpected error
			return fmt.Errorf("checking existing system %q: %w", preset.Slug, err)
		}

		// System doesn't exist, create it
		now := time.Now()
		system := SystemModel{
			// UID and ID are auto-generated by BeforeCreate hook
			DisplayName: preset.DisplayName,
			Slug:        preset.Slug, // Explicitly set slug to match preset
			Config: map[string]any{
				"rag": map[string]any{
					"embedding": map[string]any{
						"model_family":   preset.ModelFamily,
						"dimensionality": preset.Dimensionality,
					},
				},
			},
			Description: preset.Description,
			IsDefault:   preset.IsDefault,
			CreateTime:  &now,
			UpdateTime:  &now,
		}

		if err := r.db.WithContext(ctx).Create(&system).Error; err != nil {
			return fmt.Errorf("creating system %q: %w", preset.Slug, err)
		}
	}

	return nil
}
