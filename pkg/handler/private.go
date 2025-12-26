package handler

import (
	"context"
	"fmt"
	"strings"

	"github.com/gofrs/uuid"
	"github.com/iancoleman/strcase"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	fieldmask_utils "github.com/mennanov/fieldmask-utils"

	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"
	"github.com/instill-ai/x/checkfield"

	artifact "github.com/instill-ai/artifact-backend/pkg/service"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	mgmtpb "github.com/instill-ai/protogen-go/core/mgmt/v1beta"
	constantx "github.com/instill-ai/x/constant"
	logx "github.com/instill-ai/x/log"
)

// System field definitions for field mask validation
var (
	// outputOnlySystemFields are fields that cannot be specified in create/update requests
	outputOnlySystemFields = []string{"name", "uid", "is_default", "create_time", "update_time", "delete_time"}

	// immutableSystemFields are fields that cannot be changed via Update (use Rename endpoint to change ID)
	immutableSystemFields = []string{"id"}
)

// PrivateHandler handles the private Artifact endpoints.
type PrivateHandler struct {
	artifactpb.UnimplementedArtifactPrivateServiceServer
	service artifact.Service
	logger  *zap.Logger
}

// NewPrivateHandler returns an initialized private handler.
func NewPrivateHandler(s artifact.Service, log *zap.Logger) *PrivateHandler {
	return &PrivateHandler{
		service: s,
		logger:  log,
	}
}

// CreateKnowledgeBaseAdmin creates a system-level knowledge base without a creator.
// This is used by internal services (e.g., agent-backend) to create shared knowledge bases
// like "instill-agent" that are not owned by any specific user.
func (h *PrivateHandler) CreateKnowledgeBaseAdmin(ctx context.Context, req *artifactpb.CreateKnowledgeBaseAdminRequest) (*artifactpb.CreateKnowledgeBaseAdminResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	logger.Info("CreateKnowledgeBaseAdmin called",
		zap.String("namespace_id", req.GetNamespaceId()),
		zap.String("id", req.GetId()))

	// Validate required fields
	if req.GetNamespaceId() == "" {
		return nil, status.Error(codes.InvalidArgument, "namespace_id is required")
	}
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	// Get namespace
	ns, err := h.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		logger.Error("failed to get namespace", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "namespace not found: %v", err)
	}

	// Get default system
	system, err := h.service.Repository().GetDefaultSystem(ctx)
	if err != nil {
		logger.Error("failed to get default system", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to get default system: %v", err)
	}

	systemConfig, err := system.GetConfigJSON()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to parse system config: %v", err)
	}

	// Determine KB type
	kbType := req.GetType()
	if kbType == artifactpb.KnowledgeBaseType_KNOWLEDGE_BASE_TYPE_UNSPECIFIED {
		kbType = artifactpb.KnowledgeBaseType_KNOWLEDGE_BASE_TYPE_PERSISTENT
	}

	// External service callback for vector DB collection and ACL
	callExternalService := func(kbUID types.KBUIDType, collectionUID types.KBUIDType) error {
		err := h.service.Repository().CreateCollection(ctx, constant.KBCollectionName(collectionUID), systemConfig.RAG.Embedding.Dimensionality)
		if err != nil {
			return fmt.Errorf("creating vector database collection: %w", err)
		}
		err = h.service.ACLClient().SetOwner(ctx, "knowledgebase", kbUID, string(ns.NsType), ns.NsUID)
		if err != nil {
			return fmt.Errorf("setting knowledge base owner: %w", err)
		}
		return nil
	}

	// Create knowledge base WITHOUT creator (CreatorUID = nil)
	dbData, err := h.service.Repository().CreateKnowledgeBase(
		ctx,
		repository.KnowledgeBaseModel{
			KBID:              req.GetId(),
			Description:       req.GetDescription(),
			Tags:              req.GetTags(),
			NamespaceUID:      ns.NsUID.String(),
			CreatorUID:        nil, // No creator for system KBs
			KnowledgeBaseType: kbType.String(),
			SystemUID:         system.UID,
		},
		callExternalService,
	)
	if err != nil {
		logger.Error("failed to create knowledge base", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to create knowledge base: %v", err)
	}

	logger.Info("Created system knowledge base",
		zap.String("uid", dbData.UID.String()),
		zap.String("id", dbData.KBID))

	return &artifactpb.CreateKnowledgeBaseAdminResponse{
		KnowledgeBase: &artifactpb.KnowledgeBase{
			Name:        fmt.Sprintf("namespaces/%s/knowledge-bases/%s", req.GetNamespaceId(), dbData.KBID),
			Uid:         dbData.UID.String(),
			Id:          dbData.KBID,
			Description: dbData.Description,
			Tags:        dbData.Tags,
			OwnerName:   ns.Name(),
			OwnerUid:    ns.NsUID.String(),
			CreateTime:  timestamppb.New(*dbData.CreateTime),
			UpdateTime:  timestamppb.New(*dbData.UpdateTime),
		},
	}, nil
}

// UpdateKnowledgeBaseAdmin updates a knowledge base with system-reserved tags (admin only).
// Unlike the public UpdateKnowledgeBase, this endpoint:
// - Does NOT validate reserved tag prefixes (allows "instill-", "agent:" etc.)
// - Does NOT require ACL checks (admin-only access)
func (h *PrivateHandler) UpdateKnowledgeBaseAdmin(ctx context.Context, req *artifactpb.UpdateKnowledgeBaseAdminRequest) (*artifactpb.UpdateKnowledgeBaseAdminResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	logger.Info("UpdateKnowledgeBaseAdmin called",
		zap.String("namespace_id", req.GetNamespaceId()),
		zap.String("knowledge_base_id", req.GetKnowledgeBaseId()))

	// Validate required fields
	if req.GetNamespaceId() == "" {
		return nil, status.Error(codes.InvalidArgument, "namespace_id is required")
	}
	if req.GetKnowledgeBaseId() == "" {
		return nil, status.Error(codes.InvalidArgument, "knowledge_base_id is required")
	}

	// Get namespace
	ns, err := h.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		logger.Error("failed to get namespace", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "namespace not found: %v", err)
	}

	// Get existing knowledge base
	kb, err := h.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, req.GetKnowledgeBaseId())
	if err != nil {
		logger.Error("failed to get knowledge base", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "knowledge base not found: %v", err)
	}

	// Build update model - admin can set any tags including reserved ones
	updateModel := repository.KnowledgeBaseModel{}
	if req.UpdateMask != nil {
		for _, path := range req.UpdateMask.Paths {
			switch path {
			case "description":
				updateModel.Description = req.GetKnowledgeBase().GetDescription()
			case "tags":
				// Admin endpoint: NO validation of reserved tags
				updateModel.Tags = req.GetKnowledgeBase().GetTags()
			}
		}
	}

	// Update knowledge base
	updatedKB, err := h.service.Repository().UpdateKnowledgeBase(
		ctx,
		req.GetKnowledgeBaseId(),
		ns.NsUID.String(),
		updateModel,
	)
	if err != nil {
		logger.Error("failed to update knowledge base", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to update knowledge base: %v", err)
	}

	// Fetch owner for response
	owner, _ := h.service.FetchOwnerByNamespace(ctx, ns)

	logger.Info("UpdateKnowledgeBaseAdmin completed",
		zap.String("uid", kb.UID.String()),
		zap.String("id", kb.KBID))

	return &artifactpb.UpdateKnowledgeBaseAdminResponse{
		KnowledgeBase: convertKBToCatalogPB(updatedKB, ns, owner, nil),
	}, nil
}

// UpdateFileAdmin updates a file with system-reserved tags (admin only).
// Unlike the public UpdateFile, this endpoint:
// - Does NOT validate reserved tag prefixes (allows "agent:collection:{uid}" etc.)
// - Does NOT require ACL checks (admin-only access)
func (h *PrivateHandler) UpdateFileAdmin(ctx context.Context, req *artifactpb.UpdateFileAdminRequest) (*artifactpb.UpdateFileAdminResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	logger.Info("UpdateFileAdmin called",
		zap.String("namespace_id", req.GetNamespaceId()),
		zap.String("knowledge_base_id", req.GetKnowledgeBaseId()),
		zap.String("file_id", req.GetFileId()))

	// Validate required fields
	if req.GetNamespaceId() == "" {
		return nil, status.Error(codes.InvalidArgument, "namespace_id is required")
	}
	if req.GetKnowledgeBaseId() == "" {
		return nil, status.Error(codes.InvalidArgument, "knowledge_base_id is required")
	}
	if req.GetFileId() == "" {
		return nil, status.Error(codes.InvalidArgument, "file_id is required")
	}

	// Get namespace
	ns, err := h.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		logger.Error("failed to get namespace", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "namespace not found: %v", err)
	}

	// Get knowledge base
	kb, err := h.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, req.GetKnowledgeBaseId())
	if err != nil {
		logger.Error("failed to get knowledge base", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "knowledge base not found: %v", err)
	}

	// Get file
	fileUID := uuid.FromStringOrNil(req.GetFileId())
	kbFiles, err := h.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []types.FileUIDType{types.FileUIDType(fileUID)})
	if err != nil || len(kbFiles) == 0 {
		logger.Error("failed to get file", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "file not found")
	}
	kbFile := kbFiles[0]

	// Build update map based on field mask - admin can set any tags including reserved ones
	updates := make(map[string]any)
	if req.UpdateMask != nil {
		for _, path := range req.UpdateMask.Paths {
			switch path {
			case "tags":
				// Admin endpoint: NO validation of reserved tags
				updates[repository.KnowledgeBaseFileColumn.Tags] = req.GetFile().GetTags()
			case "external_metadata":
				updates[repository.KnowledgeBaseFileColumn.ExternalMetadata] = req.GetFile().GetExternalMetadata()
			}
		}
	}

	if len(updates) == 0 {
		logger.Warn("no fields to update")
		return nil, status.Error(codes.InvalidArgument, "no fields to update")
	}

	// Update file
	updatedFile, err := h.service.Repository().UpdateKnowledgeBaseFile(ctx, kbFile.UID.String(), updates)
	if err != nil {
		logger.Error("failed to update file", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to update file: %v", err)
	}

	// Fetch owner for response
	owner, _ := h.service.FetchOwnerByNamespace(ctx, ns)

	// Fetch creator if available
	var creator *mgmtpb.User
	if updatedFile.CreatorUID != uuid.Nil {
		creator, _ = h.service.FetchUserByUID(ctx, updatedFile.CreatorUID.String())
	}

	logger.Info("UpdateFileAdmin completed",
		zap.String("file_uid", updatedFile.UID.String()))

	return &artifactpb.UpdateFileAdminResponse{
		File: convertKBFileToPB(updatedFile, ns, kb, owner, creator),
	}, nil
}

// GetObjectAdmin retrieves the information of an object (admin only).
func (h *PrivateHandler) GetObjectAdmin(ctx context.Context, req *artifactpb.GetObjectAdminRequest) (*artifactpb.GetObjectAdminResponse, error) {
	objectUID, err := uuid.FromString(req.GetUid())
	if err != nil {
		h.logger.Error("GetObjectAdmin", zap.Error(err))
		return nil, err
	}

	obj, err := h.service.Repository().GetObjectByUID(ctx, objectUID)
	if err != nil {
		h.logger.Error("GetObjectAdmin", zap.Error(err))
		return nil, err
	}

	if obj == nil {
		return nil, fmt.Errorf("object not found")
	}

	return &artifactpb.GetObjectAdminResponse{
		Object: repository.TurnObjectInDBToObjectInProto(obj),
	}, nil
}

// UpdateObjectAdmin updates the information of an object (admin only).
func (h *PrivateHandler) UpdateObjectAdmin(ctx context.Context, req *artifactpb.UpdateObjectAdminRequest) (*artifactpb.UpdateObjectAdminResponse, error) {
	objectUID, err := uuid.FromString(req.GetUid())
	if err != nil {
		h.logger.Error("UpdateObjectAdmin", zap.Error(err))
		return nil, fmt.Errorf("invalid object UID: %w", err)
	}

	updateMap := make(map[string]any)

	if req.Size != nil {
		updateMap[repository.ObjectColumn.Size] = *req.Size
	}
	if req.Type != nil {
		updateMap[repository.ObjectColumn.ContentType] = *req.Type
	}
	if req.IsUploaded != nil {
		updateMap[repository.ObjectColumn.IsUploaded] = *req.IsUploaded
	}
	if req.LastModifiedTime != nil {
		updateMap[repository.ObjectColumn.LastModifiedTime] = req.LastModifiedTime.AsTime()
	}

	updatedObject, err := h.service.Repository().UpdateObjectByUpdateMap(ctx, objectUID, updateMap)
	if err != nil {
		h.logger.Error("UpdateObjectAdmin", zap.Error(err))
		return nil, fmt.Errorf("failed to update object: %w", err)
	}

	return &artifactpb.UpdateObjectAdminResponse{
		Object: repository.TurnObjectInDBToObjectInProto(updatedObject),
	}, nil
}

// GetFileAsMarkdownAdmin and GetChatFileAdmin have been removed.
// Use GetFile with VIEW_CONTENT instead to get the converted markdown via pre-signed URL.

// DeleteFileAdmin deletes a file from a knowledge base (admin only).
// This is a private gRPC-only method for internal operations like integration tests.
func (h *PrivateHandler) DeleteFileAdmin(ctx context.Context, req *artifactpb.DeleteFileAdminRequest) (*artifactpb.DeleteFileAdminResponse, error) {
	h.logger.Info("DeleteFileAdmin CALLED",
		zap.String("file_id_from_request", req.GetFileId()))

	// For the admin endpoint, we only receive file_id, so we need to look up the namespace and knowledge base
	// from the file's KB to construct the full request for the public handler
	fileUID := uuid.FromStringOrNil(req.GetFileId())

	files, err := h.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []types.FileUIDType{types.FileUIDType(fileUID)})
	if err != nil || len(files) == 0 {
		h.logger.Error("DeleteFileAdmin: failed to get file", zap.Error(err))
		return nil, fmt.Errorf("file not found: %w", err)
	}

	file := files[0]
	kb, err := h.service.Repository().GetKnowledgeBaseByUID(ctx, file.KBUID)
	if err != nil {
		h.logger.Error("DeleteFileAdmin: failed to get KB", zap.Error(err))
		return nil, fmt.Errorf("knowledge base not found: %w", err)
	}

	// Create namespace ID from owner UID for the public API
	namespaceID := fmt.Sprintf("users/%s", kb.NamespaceUID)

	// For admin endpoints, inject owner UID into gRPC metadata for authentication
	// Get existing metadata and append to it
	md, _ := metadata.FromIncomingContext(ctx)
	if md == nil {
		md = metadata.MD{}
	}
	// Set the auth type and user UID headers (make a copy to avoid modifying the original)
	md = md.Copy()
	md.Set(strings.ToLower(constantx.HeaderAuthTypeKey), "user")
	md.Set(strings.ToLower(constantx.HeaderUserUIDKey), kb.NamespaceUID)
	ctx = metadata.NewIncomingContext(ctx, md)

	h.logger.Info("DeleteFileAdmin: Injected metadata",
		zap.String("auth_type", "user"),
		zap.String("user_uid", kb.NamespaceUID))

	// Create a public handler to reuse the existing delete logic
	publicHandler := &PublicHandler{
		service: h.service,
	}

	// Delegate to the public handler's DeleteFile implementation which includes:
	// - ACL checks (with staging/rollback KB bypass)
	// - Soft-deletion of file
	// - Dual deletion to staging/rollback KB if applicable
	// - Cleanup workflow triggering
	publicReq := &artifactpb.DeleteFileRequest{
		NamespaceId:     namespaceID,
		KnowledgeBaseId: kb.KBID,
		FileId:          req.FileId,
	}

	resp, err := publicHandler.DeleteFile(ctx, publicReq)
	if err != nil {
		h.logger.Error("DeleteFileAdmin", zap.Error(err))
		return nil, err
	}

	h.logger.Info("DeleteFileAdmin: file deleted successfully",
		zap.String("file_id", req.FileId))

	return &artifactpb.DeleteFileAdminResponse{
		FileId: resp.FileId,
	}, nil
}

// RollbackAdmin rolls back a knowledge base to its previous version (admin only)
func (h *PrivateHandler) RollbackAdmin(ctx context.Context, req *artifactpb.RollbackAdminRequest) (*artifactpb.RollbackAdminResponse, error) {
	// Parse resource name: users/{user}/knowledge-bases/{knowledge_base}
	parts := strings.Split(req.Name, "/")
	if len(parts) != 4 || parts[0] != "users" || parts[2] != "knowledge-bases" {
		return nil, status.Errorf(codes.InvalidArgument, "invalid resource name format: %s", req.Name)
	}

	namespaceID := parts[1]
	knowledgeBaseID := parts[3]

	// Parse owner UID
	ownerUID, err := parseOwnerUID(namespaceID)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid user ID: %v", err)
	}

	// Call service with namespace ID for proper resource name construction
	resp, err := h.service.RollbackAdmin(ctx, ownerUID, namespaceID, knowledgeBaseID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to rollback: %v", err)
	}

	return resp, nil
}

// PurgeRollbackAdmin manually purges the rollback knowledge base immediately (admin only)
func (h *PrivateHandler) PurgeRollbackAdmin(ctx context.Context, req *artifactpb.PurgeRollbackAdminRequest) (*artifactpb.PurgeRollbackAdminResponse, error) {
	// Parse resource name
	parts := strings.Split(req.Name, "/")
	if len(parts) != 4 || parts[0] != "users" || parts[2] != "knowledge-bases" {
		return nil, status.Errorf(codes.InvalidArgument, "invalid resource name format: %s", req.Name)
	}

	userID := parts[1]
	knowledgeBaseID := parts[3]

	// Parse owner UID
	ownerUID, err := parseOwnerUID(userID)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid user ID: %v", err)
	}

	// Call service
	resp, err := h.service.PurgeRollbackAdmin(ctx, ownerUID, knowledgeBaseID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to purge rollback: %v", err)
	}

	return resp, nil
}

// SetRollbackRetentionAdmin sets the rollback retention period for a knowledge base with flexible time units (admin only)
func (h *PrivateHandler) SetRollbackRetentionAdmin(ctx context.Context, req *artifactpb.SetRollbackRetentionAdminRequest) (*artifactpb.SetRollbackRetentionAdminResponse, error) {
	// Parse resource name
	parts := strings.Split(req.Name, "/")
	if len(parts) != 4 || parts[0] != "users" || parts[2] != "knowledge-bases" {
		return nil, status.Errorf(codes.InvalidArgument, "invalid resource name format: %s", req.Name)
	}

	userID := parts[1]
	knowledgeBaseID := parts[3]

	// Parse owner UID
	ownerUID, err := parseOwnerUID(userID)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid user ID: %v", err)
	}

	// Validate time unit
	if req.TimeUnit == artifactpb.SetRollbackRetentionAdminRequest_TIME_UNIT_UNSPECIFIED {
		return nil, status.Errorf(codes.InvalidArgument, "time unit must be specified")
	}

	// Call service
	resp, err := h.service.SetRollbackRetentionAdmin(ctx, ownerUID, knowledgeBaseID, req.Duration, req.TimeUnit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to set rollback retention: %v", err)
	}

	return resp, nil
}

// ExecuteKnowledgeBaseUpdateAdmin executes the prepared knowledge base update (admin only)
func (h *PrivateHandler) ExecuteKnowledgeBaseUpdateAdmin(ctx context.Context, req *artifactpb.ExecuteKnowledgeBaseUpdateAdminRequest) (*artifactpb.ExecuteKnowledgeBaseUpdateAdminResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	logger.Info("ExecuteKnowledgeBaseUpdateAdmin handler called", zap.Int("knowledgeBaseCount", len(req.KnowledgeBaseIds)), zap.Strings("knowledgeBaseIds", req.KnowledgeBaseIds))

	// Call service - pass Admin request directly
	resp, err := h.service.ExecuteKnowledgeBaseUpdateAdmin(ctx, req)
	if err != nil {
		logger.Error("ExecuteKnowledgeBaseUpdateAdmin service error", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to execute update: %v", err)
	}

	logger.Info("ExecuteKnowledgeBaseUpdateAdmin handler response", zap.Bool("started", resp.Started), zap.String("message", resp.Message))
	return resp, nil
}

// AbortKnowledgeBaseUpdateAdmin aborts ongoing KB update workflows (admin only)
func (h *PrivateHandler) AbortKnowledgeBaseUpdateAdmin(ctx context.Context, req *artifactpb.AbortKnowledgeBaseUpdateAdminRequest) (*artifactpb.AbortKnowledgeBaseUpdateAdminResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	logger.Info("AbortKnowledgeBaseUpdateAdmin handler called", zap.Int("knowledgeBaseCount", len(req.KnowledgeBaseIds)), zap.Strings("knowledgeBaseIds", req.KnowledgeBaseIds))

	// Call service
	resp, err := h.service.AbortKnowledgeBaseUpdateAdmin(ctx, req)
	if err != nil {
		logger.Error("AbortKnowledgeBaseUpdateAdmin service error", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to abort update: %v", err)
	}

	logger.Info("AbortKnowledgeBaseUpdateAdmin handler response", zap.Bool("success", resp.Success), zap.String("message", resp.Message))
	return resp, nil
}

// GetKnowledgeBaseUpdateStatusAdmin returns the current status of system update (admin only)
func (h *PrivateHandler) GetKnowledgeBaseUpdateStatusAdmin(ctx context.Context, req *artifactpb.GetKnowledgeBaseUpdateStatusAdminRequest) (*artifactpb.GetKnowledgeBaseUpdateStatusAdminResponse, error) {
	// Call service
	resp, err := h.service.GetKnowledgeBaseUpdateStatusAdmin(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get update status: %v", err)
	}

	return resp, nil
}

// GetSystemAdmin retrieves a system configuration (admin only)
func (h *PrivateHandler) GetSystemAdmin(ctx context.Context, req *artifactpb.GetSystemAdminRequest) (*artifactpb.GetSystemAdminResponse, error) {
	// Call service
	resp, err := h.service.GetSystemAdmin(ctx, req.Id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get system: %v", err)
	}

	h.logger.Info("GetSystemAdmin", zap.String("id", req.Id))
	return resp, nil
}

// CreateSystemAdmin creates a new system configuration (admin only)
func (h *PrivateHandler) CreateSystemAdmin(ctx context.Context, req *artifactpb.CreateSystemAdminRequest) (*artifactpb.CreateSystemAdminResponse, error) {
	// Call service
	resp, err := h.service.CreateSystemAdmin(ctx, req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create system: %v", err)
	}

	h.logger.Info("CreateSystemAdmin", zap.String("id", req.System.Id))
	return resp, nil
}

// UpdateSystemAdmin updates an existing system configuration (admin only)
func (h *PrivateHandler) UpdateSystemAdmin(ctx context.Context, req *artifactpb.UpdateSystemAdminRequest) (*artifactpb.UpdateSystemAdminResponse, error) {
	pbSystemReq := req.GetSystem()
	if pbSystemReq == nil {
		return nil, status.Errorf(codes.InvalidArgument, "system is required")
	}
	if pbSystemReq.Id == "" {
		return nil, status.Errorf(codes.InvalidArgument, "system id is required")
	}

	pbUpdateMask := req.GetUpdateMask()

	// Config field is type google.protobuf.Struct, which needs to be updated as a whole
	for idx, path := range pbUpdateMask.Paths {
		if strings.Contains(path, "config") {
			pbUpdateMask.Paths[idx] = "config"
		}
	}

	// Validate the field mask
	if !pbUpdateMask.IsValid(pbSystemReq) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid field mask")
	}

	// Get existing system
	getResp, err := h.service.GetSystemAdmin(ctx, req.System.Id)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "failed to get system: %v", err)
	}

	// Check and remove output-only fields from mask
	pbUpdateMask, err = checkfield.CheckUpdateOutputOnlyFields(pbUpdateMask, outputOnlySystemFields)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid output-only fields: %v", err)
	}

	// Convert to fieldmask-utils mask
	mask, err := fieldmask_utils.MaskFromProtoFieldMask(pbUpdateMask, strcase.ToCamel)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid field mask: %v", err)
	}

	// Empty mask means no updates
	if mask.IsEmpty() {
		return &artifactpb.UpdateSystemAdminResponse{System: getResp.System}, nil
	}

	pbSystemToUpdate := getResp.GetSystem()

	// Return error if IMMUTABLE fields are intentionally changed
	if err := checkfield.CheckUpdateImmutableFields(pbSystemReq, pbSystemToUpdate, immutableSystemFields); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "immutable field changed: %v", err)
	}

	// Only the fields mentioned in the field mask will be copied to pbSystemToUpdate
	if err := fieldmask_utils.StructToStruct(mask, pbSystemReq, pbSystemToUpdate); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to apply field mask: %v", err)
	}

	// Call service to update
	resp, err := h.service.UpdateSystemAdmin(ctx, &artifactpb.UpdateSystemAdminRequest{
		System:     pbSystemToUpdate,
		UpdateMask: pbUpdateMask,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update system: %v", err)
	}

	h.logger.Info("UpdateSystemAdmin", zap.String("id", req.System.Id), zap.Strings("fields", pbUpdateMask.Paths))
	return resp, nil
}

// ListSystemsAdmin lists all system configurations (admin only)
func (h *PrivateHandler) ListSystemsAdmin(ctx context.Context, req *artifactpb.ListSystemsAdminRequest) (*artifactpb.ListSystemsAdminResponse, error) {
	// Call service
	resp, err := h.service.ListSystemsAdmin(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list systems: %v", err)
	}

	h.logger.Info("ListSystemsAdmin", zap.Int("count", len(resp.Systems)))
	return resp, nil
}

// DeleteSystemAdmin deletes a system configuration (admin only)
func (h *PrivateHandler) DeleteSystemAdmin(ctx context.Context, req *artifactpb.DeleteSystemAdminRequest) (*artifactpb.DeleteSystemAdminResponse, error) {
	// Call service
	resp, err := h.service.DeleteSystemAdmin(ctx, req.Id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete system: %v", err)
	}

	h.logger.Info("DeleteSystemAdmin", zap.String("id", req.Id), zap.Bool("success", resp.Success))
	return resp, nil
}

// RenameSystemAdmin renames a system configuration (admin only)
func (h *PrivateHandler) RenameSystemAdmin(ctx context.Context, req *artifactpb.RenameSystemAdminRequest) (*artifactpb.RenameSystemAdminResponse, error) {
	// Call service
	resp, err := h.service.RenameSystemAdmin(ctx, req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to rename system: %v", err)
	}

	h.logger.Info("RenameSystemAdmin", zap.String("old_id", req.SystemId), zap.String("new_id", req.NewSystemId))
	return resp, nil
}

// SetDefaultSystemAdmin sets a system as the default (admin only)
func (h *PrivateHandler) SetDefaultSystemAdmin(ctx context.Context, req *artifactpb.SetDefaultSystemAdminRequest) (*artifactpb.SetDefaultSystemAdminResponse, error) {
	// Call service
	resp, err := h.service.SetDefaultSystemAdmin(ctx, req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to set default system: %v", err)
	}

	h.logger.Info("SetDefaultSystemAdmin", zap.String("id", req.Id), zap.Bool("is_default", resp.System.IsDefault))
	return resp, nil
}

// GetDefaultSystemAdmin retrieves the current default system (admin only)
func (h *PrivateHandler) GetDefaultSystemAdmin(ctx context.Context, req *artifactpb.GetDefaultSystemAdminRequest) (*artifactpb.GetDefaultSystemAdminResponse, error) {
	// Call service
	resp, err := h.service.GetDefaultSystemAdmin(ctx, req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get default system: %v", err)
	}

	h.logger.Info("GetDefaultSystemAdmin", zap.String("id", resp.System.Id), zap.Bool("is_default", resp.System.IsDefault))
	return resp, nil
}

// Helper functions

func parseOwnerUID(userID string) (types.OwnerUIDType, error) {
	uid, err := uuid.FromString(userID)
	if err != nil {
		return types.OwnerUIDType{}, err
	}
	return types.OwnerUIDType(uid), nil
}
