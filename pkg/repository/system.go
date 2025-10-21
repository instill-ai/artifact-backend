package repository

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/instill-ai/artifact-backend/internal/ai"
)

// SystemProfile interface defines methods for system-wide configuration profiles
type SystemProfile interface {
	// GetSystemProfile retrieves a complete system configuration profile
	GetSystemProfile(ctx context.Context, profile string) (*SystemProfileModel, error)

	// UpdateSystemProfile creates or updates a system configuration profile
	UpdateSystemProfile(ctx context.Context, profile string, config map[string]any, description string) error

	// ListSystemProfiles lists all system configuration profiles
	ListSystemProfiles(ctx context.Context) ([]SystemProfileModel, error)

	// DeleteSystemProfile deletes a system configuration profile
	DeleteSystemProfile(ctx context.Context, profile string) error

	// GetDefaultEmbeddingConfig retrieves the default embedding configuration for a given profile
	// If the profile doesn't exist, it falls back to "default" profile
	// If "default" doesn't exist, it returns hardcoded new standard (Gemini/3072)
	GetDefaultEmbeddingConfig(ctx context.Context, profile string) (*EmbeddingConfigJSON, error)

	// UpdateDefaultEmbeddingConfig updates the default embedding configuration for a profile
	UpdateDefaultEmbeddingConfig(ctx context.Context, profile string, config EmbeddingConfigJSON) error
}

// SystemProfileModel represents the system table structure
type SystemProfileModel struct {
	Profile     string                 `gorm:"column:profile;type:varchar(255);primaryKey;default:default" json:"profile"`
	Config      map[string]any `gorm:"column:config;type:jsonb;not null;serializer:json" json:"config"`
	Description string                 `gorm:"column:description;type:text" json:"description"`
	UpdatedAt   time.Time              `gorm:"column:updated_at;not null;default:CURRENT_TIMESTAMP" json:"updated_at"`
}

// TableName overrides the default table name for GORM
func (SystemProfileModel) TableName() string {
	return "system"
}

// GetSystemProfile retrieves a complete system configuration profile
func (r *repository) GetSystemProfile(ctx context.Context, profile string) (*SystemProfileModel, error) {
	var systemProfile SystemProfileModel

	err := r.db.WithContext(ctx).
		Where("profile = ?", profile).
		First(&systemProfile).Error

	if err != nil {
		return nil, err
	}

	return &systemProfile, nil
}

// UpdateSystemProfile creates or updates a system configuration profile
func (r *repository) UpdateSystemProfile(ctx context.Context, profile string, config map[string]any, description string) error {
	now := time.Now()

	systemProfile := SystemProfileModel{
		Profile:     profile,
		Config:      config,
		Description: description,
		UpdatedAt:   now,
	}

	return r.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "profile"}},
			DoUpdates: clause.AssignmentColumns([]string{"config", "description", "updated_at"}),
		}).
		Create(&systemProfile).Error
}

// ListSystemProfiles lists all system configuration profiles
func (r *repository) ListSystemProfiles(ctx context.Context) ([]SystemProfileModel, error) {
	var profiles []SystemProfileModel

	err := r.db.WithContext(ctx).
		Order("profile").
		Find(&profiles).Error

	if err != nil {
		return nil, err
	}

	return profiles, nil
}

// DeleteSystemProfile deletes a system configuration profile
func (r *repository) DeleteSystemProfile(ctx context.Context, profile string) error {
	// Prevent deletion of the default profile
	if profile == "default" {
		return errors.New("cannot delete default system profile")
	}

	return r.db.WithContext(ctx).
		Where("profile = ?", profile).
		Delete(&SystemProfileModel{}).Error
}

// GetDefaultEmbeddingConfig retrieves the default embedding configuration for a profile
func (r *repository) GetDefaultEmbeddingConfig(ctx context.Context, profile string) (*EmbeddingConfigJSON, error) {
	systemProfile, err := r.GetSystemProfile(ctx, profile)

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Fallback to 'default' profile if specific profile not found
			if profile != "default" {
				return r.GetDefaultEmbeddingConfig(ctx, "default")
			}
			// Final fallback to hardcoded new standard if 'default' profile doesn't exist
			return &EmbeddingConfigJSON{
				ModelFamily:    ai.ModelFamilyGemini,
				Dimensionality: uint32(ai.GeminiEmbeddingDimDefault),
			}, nil
		}
		return nil, err
	}

	// Navigate the nested config: config.rag.default_embedding_config
	ragConfig, ok := systemProfile.Config["rag"].(map[string]any)
	if !ok {
		// No rag config, fallback to default profile or hardcoded
		if profile != "default" {
			return r.GetDefaultEmbeddingConfig(ctx, "default")
		}
		return &EmbeddingConfigJSON{
			ModelFamily:    ai.ModelFamilyGemini,
			Dimensionality: uint32(ai.GeminiEmbeddingDimDefault),
		}, nil
	}

	embeddingConfig, ok := ragConfig["default_embedding_config"].(map[string]any)
	if !ok {
		// No default_embedding_config, fallback
		if profile != "default" {
			return r.GetDefaultEmbeddingConfig(ctx, "default")
		}
		return &EmbeddingConfigJSON{
			ModelFamily:    ai.ModelFamilyGemini,
			Dimensionality: uint32(ai.GeminiEmbeddingDimDefault),
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

	return &EmbeddingConfigJSON{
		ModelFamily:    modelFamily,
		Dimensionality: dimensionality,
	}, nil
}

// UpdateDefaultEmbeddingConfig updates the default embedding configuration for a profile
func (r *repository) UpdateDefaultEmbeddingConfig(ctx context.Context, profile string, config EmbeddingConfigJSON) error {
	// Get existing profile or create new one
	systemProfile, err := r.GetSystemProfile(ctx, profile)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}

	// Initialize config if profile doesn't exist
	if systemProfile == nil {
		systemProfile = &SystemProfileModel{
			Profile: profile,
			Config:  make(map[string]any),
		}
	}

	// Ensure rag category exists
	ragConfig, ok := systemProfile.Config["rag"].(map[string]any)
	if !ok {
		ragConfig = make(map[string]any)
	}

	// Set default_embedding_config
	ragConfig["default_embedding_config"] = map[string]any{
		"model_family":   string(config.ModelFamily),
		"dimensionality": config.Dimensionality,
	}
	systemProfile.Config["rag"] = ragConfig

	// Save the updated profile
	return r.UpdateSystemProfile(ctx, profile, systemProfile.Config, "Default embedding configuration for newly created knowledge bases")
}
