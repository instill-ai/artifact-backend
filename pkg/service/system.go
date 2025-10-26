package service

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"gorm.io/gorm"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	"github.com/instill-ai/x/checkfield"
)

// GetSystemAdmin retrieves a system configuration by ID
func (s *service) GetSystemAdmin(ctx context.Context, id string) (*artifactpb.GetSystemAdminResponse, error) {
	// Default to "default" if not specified
	if id == "" {
		id = "default"
	}

	// Get system from repository
	system, err := s.repository.GetSystem(ctx, id)
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
		System: &artifactpb.System{
			Name:        fmt.Sprintf("systems/%s", system.ID),
			Uid:         system.UID.String(),
			Id:          system.ID,
			Config:      configStruct,
			Description: &system.Description,
			IsDefault:   system.IsDefault,
			CreateTime:  timestamppb.New(*system.CreateTime),
			UpdateTime:  timestamppb.New(*system.UpdateTime),
			DeleteTime:  nil,
		},
	}

	return resp, nil
}

// CreateSystemAdmin creates a new system configuration
func (s *service) CreateSystemAdmin(ctx context.Context, req *artifactpb.CreateSystemAdminRequest) (*artifactpb.CreateSystemAdminResponse, error) {
	if req.System == nil {
		return nil, fmt.Errorf("system is required")
	}

	if req.System.Id == "" {
		return nil, fmt.Errorf("system id is required")
	}

	// Convert structpb.Struct to map[string]interface{}
	config := req.System.Config.AsMap()

	// Get description (default to empty string if not provided)
	description := ""
	if req.System.Description != nil {
		description = *req.System.Description
	}

	// Create system in repository
	err := s.repository.CreateSystem(ctx, req.System.Id, config, description)
	if err != nil {
		return nil, fmt.Errorf("failed to create system: %w", err)
	}

	// Retrieve the created system to return with timestamp and UID
	system, err := s.repository.GetSystem(ctx, req.System.Id)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve created system: %w", err)
	}

	// Convert map[string]interface{} to structpb.Struct
	configStruct, err := structpb.NewStruct(system.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to convert config to protobuf struct: %w", err)
	}

	// Build response
	resp := &artifactpb.CreateSystemAdminResponse{
		System: &artifactpb.System{
			Name:        fmt.Sprintf("systems/%s", system.ID),
			Uid:         system.UID.String(),
			Id:          system.ID,
			Config:      configStruct,
			Description: &system.Description,
			IsDefault:   system.IsDefault,
			CreateTime:  timestamppb.New(*system.CreateTime),
			UpdateTime:  timestamppb.New(*system.UpdateTime),
			DeleteTime:  nil,
		},
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

	// Convert structpb.Struct to map[string]interface{}
	config := req.System.Config.AsMap()

	// Get description (default to empty string if not provided)
	description := ""
	if req.System.Description != nil {
		description = *req.System.Description
	}

	// Update system in repository
	err := s.repository.UpdateSystem(ctx, req.System.Id, config, description)
	if err != nil {
		return nil, fmt.Errorf("failed to update system: %w", err)
	}

	// Retrieve the saved system to return with updated timestamp
	system, err := s.repository.GetSystem(ctx, req.System.Id)
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
		System: &artifactpb.System{
			Name:        fmt.Sprintf("systems/%s", system.ID),
			Uid:         system.UID.String(),
			Id:          system.ID,
			Config:      configStruct,
			Description: &system.Description,
			IsDefault:   system.IsDefault,
			CreateTime:  timestamppb.New(*system.CreateTime),
			UpdateTime:  timestamppb.New(*system.UpdateTime),
			DeleteTime:  nil,
		},
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
	for _, system := range systems {
		// Convert map[string]interface{} to structpb.Struct
		configStruct, err := structpb.NewStruct(system.Config)
		if err != nil {
			return nil, fmt.Errorf("failed to convert config for system %q: %w", system.ID, err)
		}

		pbSystems = append(pbSystems, &artifactpb.System{
			Name:        fmt.Sprintf("systems/%s", system.ID),
			Uid:         system.UID.String(),
			Id:          system.ID,
			Config:      configStruct,
			Description: &system.Description,
			IsDefault:   system.IsDefault,
			CreateTime:  timestamppb.New(*system.CreateTime),
			UpdateTime:  timestamppb.New(*system.UpdateTime),
			DeleteTime:  nil, // Not exposed in API for active systems
		})
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
		return &artifactpb.DeleteSystemAdminResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to delete system: %v", err),
		}, nil
	}

	resp := &artifactpb.DeleteSystemAdminResponse{
		Success: true,
		Message: fmt.Sprintf("System %q deleted successfully", id),
	}

	return resp, nil
}

// RenameSystemAdmin renames a system configuration (changes its ID)
func (s *service) RenameSystemAdmin(ctx context.Context, req *artifactpb.RenameSystemAdminRequest) (*artifactpb.RenameSystemAdminResponse, error) {
	if req.SystemId == "" {
		return nil, fmt.Errorf("system id is required")
	}

	if req.NewSystemId == "" {
		return nil, fmt.Errorf("new system id is required")
	}

	// Validate new ID format (RFC-1034)
	if err := checkfield.CheckResourceID(req.NewSystemId); err != nil {
		return nil, fmt.Errorf("invalid new system id: %w", err)
	}

	// Rename system in repository
	err := s.repository.RenameSystemByID(ctx, req.SystemId, req.NewSystemId)
	if err != nil {
		return nil, fmt.Errorf("failed to rename system: %w", err)
	}

	// Retrieve the renamed system
	system, err := s.repository.GetSystem(ctx, req.NewSystemId)
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
		System: &artifactpb.System{
			Name:        fmt.Sprintf("systems/%s", system.ID),
			Uid:         system.UID.String(),
			Id:          system.ID,
			Config:      configStruct,
			Description: &system.Description,
			IsDefault:   system.IsDefault,
			CreateTime:  timestamppb.New(*system.CreateTime),
			UpdateTime:  timestamppb.New(*system.UpdateTime),
			DeleteTime:  nil,
		},
	}

	return resp, nil
}

// SetDefaultSystemAdmin sets a system as the default
func (s *service) SetDefaultSystemAdmin(ctx context.Context, req *artifactpb.SetDefaultSystemAdminRequest) (*artifactpb.SetDefaultSystemAdminResponse, error) {
	if req.Id == "" {
		return nil, fmt.Errorf("system id is required")
	}

	// Set as default in repository
	err := s.repository.SetDefaultSystem(ctx, req.Id)
	if err != nil {
		return nil, fmt.Errorf("failed to set default system: %w", err)
	}

	// Retrieve the updated system to return
	system, err := s.repository.GetSystem(ctx, req.Id)
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
		System: &artifactpb.System{
			Name:        fmt.Sprintf("systems/%s", system.ID),
			Uid:         system.UID.String(),
			Id:          system.ID,
			Config:      configStruct,
			Description: &system.Description,
			IsDefault:   system.IsDefault,
			CreateTime:  timestamppb.New(*system.CreateTime),
			UpdateTime:  timestamppb.New(*system.UpdateTime),
			DeleteTime:  nil,
		},
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
		System: &artifactpb.System{
			Name:        fmt.Sprintf("systems/%s", system.ID),
			Uid:         system.UID.String(),
			Id:          system.ID,
			Config:      configStruct,
			Description: &system.Description,
			IsDefault:   system.IsDefault,
			CreateTime:  timestamppb.New(*system.CreateTime),
			UpdateTime:  timestamppb.New(*system.UpdateTime),
			DeleteTime:  nil,
		},
	}

	return resp, nil
}
