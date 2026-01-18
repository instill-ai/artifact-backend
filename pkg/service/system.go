package service

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"gorm.io/gorm"

	"github.com/instill-ai/artifact-backend/pkg/repository"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
)

// GetSystemAdmin retrieves a system configuration by ID or slug
func (s *service) GetSystemAdmin(ctx context.Context, id string) (*artifactpb.GetSystemAdminResponse, error) {
	// Default to "default" if not specified
	if id == "" {
		id = "default"
	}

	// Get system from repository - accepts both ID (sys-xxx) and slug (openai, gemini)
	system, err := s.repository.GetSystemByIDOrSlug(ctx, id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, fmt.Errorf("system %q not found", id)
		}
		return nil, fmt.Errorf("failed to get system: %w", err)
	}

	// Convert map[string]interface{} to structpb.Struct
	configStruct, err := structpb.NewStruct(system.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to convert config to protobuf struct: %w", err)
	}

	// Build response
	resp := &artifactpb.GetSystemAdminResponse{
		System: systemModelToProto(system, configStruct),
	}

	return resp, nil
}

// systemModelToProto converts a SystemModel to an artifactpb.System
func systemModelToProto(system *repository.SystemModel, configStruct *structpb.Struct) *artifactpb.System {
	pbSystem := &artifactpb.System{
		Name:        fmt.Sprintf("systems/%s", system.ID),
		Id:          system.ID,
		DisplayName: system.DisplayName,
		Slug:        system.Slug,
		Config:      configStruct,
		Description: &system.Description,
		IsDefault:   system.IsDefault,
		CreateTime:  timestamppb.New(*system.CreateTime),
		UpdateTime:  timestamppb.New(*system.UpdateTime),
	}
	if system.DeleteTime.Valid {
		pbSystem.DeleteTime = timestamppb.New(system.DeleteTime.Time)
	}
	return pbSystem
}

// CreateSystemAdmin creates a new system configuration
// The ID is auto-generated as "sys-{hash}" following AIP resource ID convention
// displayName is required and used to generate the human-readable slug
func (s *service) CreateSystemAdmin(ctx context.Context, req *artifactpb.CreateSystemAdminRequest) (*artifactpb.CreateSystemAdminResponse, error) {
	if req.System == nil {
		return nil, fmt.Errorf("system is required")
	}

	// display_name is required for creating a system
	// ID is auto-generated, so we use display_name for identification
	displayName := req.System.DisplayName
	if displayName == "" {
		return nil, fmt.Errorf("system display_name is required")
	}

	// Convert structpb.Struct to map[string]interface{}
	config := req.System.Config.AsMap()

	// Get description (default to empty string if not provided)
	description := ""
	if req.System.Description != nil {
		description = *req.System.Description
	}

	// Create system in repository - ID is auto-generated
	system, err := s.repository.CreateSystem(ctx, displayName, config, description)
	if err != nil {
		return nil, fmt.Errorf("failed to create system: %w", err)
	}

	// Convert map[string]interface{} to structpb.Struct
	configStruct, err := structpb.NewStruct(system.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to convert config to protobuf struct: %w", err)
	}

	// Build response
	resp := &artifactpb.CreateSystemAdminResponse{
		System: systemModelToProto(system, configStruct),
	}

	return resp, nil
}

// UpdateSystemAdmin updates an existing system configuration
// The handler has already applied field mask, so this just persists the changes
func (s *service) UpdateSystemAdmin(ctx context.Context, req *artifactpb.UpdateSystemAdminRequest) (*artifactpb.UpdateSystemAdminResponse, error) {
	if req.System == nil {
		return nil, fmt.Errorf("system is required")
	}

	if req.System.Id == "" {
		return nil, fmt.Errorf("system id is required")
	}

	// Build update map based on field mask
	updateFields := make(map[string]interface{})

	for _, path := range req.UpdateMask.GetPaths() {
		switch path {
		case "config":
			// Convert structpb.Struct to map[string]interface{}
			updateFields["config"] = req.System.Config.AsMap()
		case "description":
			// Get description (default to empty string if not provided)
			description := ""
			if req.System.Description != nil {
				description = *req.System.Description
			}
			updateFields["description"] = description
		}
	}

	// Update system in repository using selective update
	err := s.repository.UpdateSystemByUpdateMap(ctx, req.System.Id, updateFields)
	if err != nil {
		return nil, fmt.Errorf("failed to update system: %w", err)
	}

	// Retrieve the saved system to return with updated timestamp
	system, err := s.repository.GetSystemByIDOrSlug(ctx, req.System.Id)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve updated system: %w", err)
	}

	// Convert map[string]interface{} to structpb.Struct
	configStruct, err := structpb.NewStruct(system.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to convert config to protobuf struct: %w", err)
	}

	// Build response
	resp := &artifactpb.UpdateSystemAdminResponse{
		System: systemModelToProto(system, configStruct),
	}

	return resp, nil
}

// ListSystemsAdmin lists all system configurations
func (s *service) ListSystemsAdmin(ctx context.Context) (*artifactpb.ListSystemsAdminResponse, error) {
	// Get all systems from repository
	systems, err := s.repository.ListSystems(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list systems: %w", err)
	}

	// Convert to protobuf messages
	pbSystems := make([]*artifactpb.System, 0, len(systems))
	for i := range systems {
		system := &systems[i]
		// Convert map[string]interface{} to structpb.Struct
		configStruct, err := structpb.NewStruct(system.Config)
		if err != nil {
			return nil, fmt.Errorf("failed to convert config for system %q: %w", system.ID, err)
		}

		pbSystems = append(pbSystems, systemModelToProto(system, configStruct))
	}

	resp := &artifactpb.ListSystemsAdminResponse{
		Systems: pbSystems,
	}

	return resp, nil
}

// DeleteSystemAdmin deletes a system configuration
func (s *service) DeleteSystemAdmin(ctx context.Context, id string) (*artifactpb.DeleteSystemAdminResponse, error) {
	if id == "" {
		return nil, fmt.Errorf("system id is required")
	}

	// Delete system from repository
	err := s.repository.DeleteSystem(ctx, id)
	if err != nil {
		// Return error instead of Success: false to properly propagate protection errors
		return nil, fmt.Errorf("failed to delete system: %w", err)
	}

	resp := &artifactpb.DeleteSystemAdminResponse{
		Success: true,
		Message: fmt.Sprintf("System %q deleted successfully", id),
	}

	return resp, nil
}

// RenameSystemAdmin renames a system configuration (updates display_name and slug)
// Note: The canonical ID (sys-xxx) is immutable. Only display_name and slug change.
func (s *service) RenameSystemAdmin(ctx context.Context, req *artifactpb.RenameSystemAdminRequest) (*artifactpb.RenameSystemAdminResponse, error) {
	if req.GetSystemId() == "" {
		return nil, fmt.Errorf("system id is required")
	}

	if req.GetNewDisplayName() == "" {
		return nil, fmt.Errorf("new display name is required")
	}

	// Rename system in repository (updates display_name and regenerates slug)
	err := s.repository.RenameSystemByID(ctx, req.GetSystemId(), req.GetNewDisplayName())
	if err != nil {
		return nil, fmt.Errorf("failed to rename system: %w", err)
	}

	// Retrieve the renamed system
	system, err := s.repository.GetSystem(ctx, req.GetSystemId())
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve renamed system: %w", err)
	}

	// Convert map[string]interface{} to structpb.Struct
	configStruct, err := structpb.NewStruct(system.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to convert config to protobuf struct: %w", err)
	}

	// Build response
	resp := &artifactpb.RenameSystemAdminResponse{
		System: systemModelToProto(system, configStruct),
	}

	return resp, nil
}

// SetDefaultSystemAdmin sets a system as the default
func (s *service) SetDefaultSystemAdmin(ctx context.Context, req *artifactpb.SetDefaultSystemAdminRequest) (*artifactpb.SetDefaultSystemAdminResponse, error) {
	if req.GetSystemId() == "" {
		return nil, fmt.Errorf("system id is required")
	}

	// Set as default in repository
	err := s.repository.SetDefaultSystem(ctx, req.GetSystemId())
	if err != nil {
		return nil, fmt.Errorf("failed to set default system: %w", err)
	}

	// Retrieve the updated system to return
	system, err := s.repository.GetSystem(ctx, req.GetSystemId())
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve updated system: %w", err)
	}

	// Convert map[string]interface{} to structpb.Struct
	configStruct, err := structpb.NewStruct(system.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to convert config to protobuf struct: %w", err)
	}

	// Build response
	resp := &artifactpb.SetDefaultSystemAdminResponse{
		System: systemModelToProto(system, configStruct),
	}

	return resp, nil
}

// GetDefaultSystemAdmin retrieves the current default system
func (s *service) GetDefaultSystemAdmin(ctx context.Context, req *artifactpb.GetDefaultSystemAdminRequest) (*artifactpb.GetDefaultSystemAdminResponse, error) {
	// Get default system from repository
	system, err := s.repository.GetDefaultSystem(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get default system: %w", err)
	}

	// Convert map[string]interface{} to structpb.Struct
	configStruct, err := structpb.NewStruct(system.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to convert config to protobuf struct: %w", err)
	}

	// Build response
	resp := &artifactpb.GetDefaultSystemAdminResponse{
		System: systemModelToProto(system, configStruct),
	}

	return resp, nil
}
