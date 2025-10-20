package service

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"gorm.io/gorm"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// GetSystemProfileAdmin retrieves a system configuration profile by name
func (s *service) GetSystemProfileAdmin(ctx context.Context, profile string) (*artifactpb.GetSystemProfileAdminResponse, error) {
	// Default to "default" profile if not specified
	if profile == "" {
		profile = "default"
	}

	// Get profile from repository
	systemProfile, err := s.repository.GetSystemProfile(ctx, profile)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, fmt.Errorf("system profile %q not found", profile)
		}
		return nil, fmt.Errorf("failed to get system profile: %w", err)
	}

	// Convert map[string]interface{} to structpb.Struct
	configStruct, err := structpb.NewStruct(systemProfile.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to convert config to protobuf struct: %w", err)
	}

	// Build response
	resp := &artifactpb.GetSystemProfileAdminResponse{
		SystemProfile: &artifactpb.SystemProfile{
			Profile:     systemProfile.Profile,
			Config:      configStruct,
			Description: systemProfile.Description,
			UpdatedAt:   timestamppb.New(systemProfile.UpdatedAt),
		},
	}

	return resp, nil
}

// UpdateSystemProfileAdmin creates or updates a system configuration profile
func (s *service) UpdateSystemProfileAdmin(ctx context.Context, req *artifactpb.UpdateSystemProfileAdminRequest) (*artifactpb.UpdateSystemProfileAdminResponse, error) {
	if req.SystemProfile == nil {
		return nil, fmt.Errorf("system_profile is required")
	}

	if req.SystemProfile.Profile == "" {
		return nil, fmt.Errorf("profile name is required")
	}

	// Convert structpb.Struct to map[string]interface{}
	config := req.SystemProfile.Config.AsMap()

	// Update profile in repository (upsert: create if doesn't exist, update if exists)
	err := s.repository.UpdateSystemProfile(ctx, req.SystemProfile.Profile, config, req.SystemProfile.Description)
	if err != nil {
		return nil, fmt.Errorf("failed to update system profile: %w", err)
	}

	// Retrieve the saved profile to return with updated timestamp
	systemProfile, err := s.repository.GetSystemProfile(ctx, req.SystemProfile.Profile)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve updated system profile: %w", err)
	}

	// Convert map[string]interface{} to structpb.Struct
	configStruct, err := structpb.NewStruct(systemProfile.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to convert config to protobuf struct: %w", err)
	}

	// Build response
	resp := &artifactpb.UpdateSystemProfileAdminResponse{
		SystemProfile: &artifactpb.SystemProfile{
			Profile:     systemProfile.Profile,
			Config:      configStruct,
			Description: systemProfile.Description,
			UpdatedAt:   timestamppb.New(systemProfile.UpdatedAt),
		},
	}

	return resp, nil
}

// ListSystemProfilesAdmin lists all system configuration profiles
func (s *service) ListSystemProfilesAdmin(ctx context.Context) (*artifactpb.ListSystemProfilesAdminResponse, error) {
	// Get all profiles from repository
	profiles, err := s.repository.ListSystemProfiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list system profiles: %w", err)
	}

	// Convert to protobuf messages
	pbProfiles := make([]*artifactpb.SystemProfile, 0, len(profiles))
	for _, profile := range profiles {
		// Convert map[string]interface{} to structpb.Struct
		configStruct, err := structpb.NewStruct(profile.Config)
		if err != nil {
			return nil, fmt.Errorf("failed to convert config for profile %q: %w", profile.Profile, err)
		}

		pbProfiles = append(pbProfiles, &artifactpb.SystemProfile{
			Profile:     profile.Profile,
			Config:      configStruct,
			Description: profile.Description,
			UpdatedAt:   timestamppb.New(profile.UpdatedAt),
		})
	}

	resp := &artifactpb.ListSystemProfilesAdminResponse{
		SystemProfiles: pbProfiles,
	}

	return resp, nil
}

// DeleteSystemProfileAdmin deletes a system configuration profile
func (s *service) DeleteSystemProfileAdmin(ctx context.Context, profile string) (*artifactpb.DeleteSystemProfileAdminResponse, error) {
	if profile == "" {
		return nil, fmt.Errorf("profile name is required")
	}

	// Delete profile from repository
	err := s.repository.DeleteSystemProfile(ctx, profile)
	if err != nil {
		return &artifactpb.DeleteSystemProfileAdminResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to delete system profile: %v", err),
		}, nil
	}

	resp := &artifactpb.DeleteSystemProfileAdminResponse{
		Success: true,
		Message: fmt.Sprintf("System profile %q deleted successfully", profile),
	}

	return resp, nil
}
