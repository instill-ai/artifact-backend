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
	"github.com/instill-ai/x/resource"

	artifact "github.com/instill-ai/artifact-backend/pkg/service"
	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
	mgmtpb "github.com/instill-ai/protogen-go/mgmt/v1beta"
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

	// Parse namespace ID from parent (format: namespaces/{namespace})
	namespaceID, err := parseNamespaceFromParent(req.GetParent())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid parent format: %v", err)
	}

	kb := req.GetKnowledgeBase()
	if kb == nil {
		return nil, status.Error(codes.InvalidArgument, "knowledge_base is required")
	}

	// For admin requests, id is optional (auto-generated as kb-{hash} if empty)
	kbID := kb.GetId()

	logger.Info("CreateKnowledgeBaseAdmin called",
		zap.String("namespace_id", namespaceID),
		zap.String("id", kbID))

	// Get namespace
	ns, err := h.service.GetNamespaceByNsID(ctx, namespaceID)
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
	kbType := kb.GetType()
	if kbType == artifactpb.KnowledgeBaseType_KNOWLEDGE_BASE_TYPE_UNSPECIFIED {
		kbType = artifactpb.KnowledgeBaseType_KNOWLEDGE_BASE_TYPE_PERSISTENT
	}

	// Use display_name from request, fallback to id if not set
	displayName := kb.GetDisplayName()
	if displayName == "" {
		displayName = kbID
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
	// Pass the slug from request if provided, otherwise repository will auto-generate
	dbData, err := h.service.Repository().CreateKnowledgeBase(
		ctx,
		repository.KnowledgeBaseModel{
			ID:                kbID,
			DisplayName:       displayName,
			Slug:              kb.GetSlug(), // Pass slug from request (e.g., "instill-agent" for default KBs)
			Description:       kb.GetDescription(),
			Tags:              kb.GetTags(),
			NamespaceUID:      ns.NsUID.String(),
			CreatorUID:        nil, // No creator for system KBs
			KnowledgeBaseType: kbType.String(),
			SystemUID:         system.UID,
		},
		callExternalService,
	)
	if err != nil {
		// Handle "already exists" gracefully - it's expected in race conditions
		if strings.Contains(err.Error(), "already exists") {
			logger.Debug("knowledge base already exists (expected for system KBs)", zap.String("id", kbID))
			return nil, status.Errorf(codes.AlreadyExists, "knowledge base already exists: %s", kbID)
		}
		logger.Error("failed to create knowledge base", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to create knowledge base: %v", err)
	}

	logger.Info("Created system knowledge base",
		zap.String("uid", dbData.UID.String()),
		zap.String("id", dbData.ID))

	return &artifactpb.CreateKnowledgeBaseAdminResponse{
		KnowledgeBase: &artifactpb.KnowledgeBase{
			Name:        fmt.Sprintf("namespaces/%s/knowledge-bases/%s", namespaceID, dbData.ID),
			Id:          dbData.ID,
			DisplayName: displayName,
			Description: dbData.Description,
			Tags:        dbData.Tags,
			OwnerName:   ns.Name(),
			CreateTime:  timestamppb.New(*dbData.CreateTime),
			UpdateTime:  timestamppb.New(*dbData.UpdateTime),
		},
	}, nil
}

// ListKnowledgeBasesAdmin lists all knowledge bases in a namespace without ACL filtering.
// Unlike the public ListKnowledgeBases, this endpoint:
// - Does NOT filter by ACL permissions
// - Returns ALL knowledge bases including system-created ones (no creator)
// - Used by internal services (e.g., agent-backend) to find existing system KBs
func (h *PrivateHandler) ListKnowledgeBasesAdmin(ctx context.Context, req *artifactpb.ListKnowledgeBasesAdminRequest) (*artifactpb.ListKnowledgeBasesAdminResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Parse namespace ID from parent (format: namespaces/{namespace})
	namespaceID, err := parseNamespaceFromParent(req.GetParent())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid parent format: %v", err)
	}

	logger.Info("ListKnowledgeBasesAdmin called", zap.String("namespace_id", namespaceID))

	// Get namespace
	ns, err := h.service.GetNamespaceByNsID(ctx, namespaceID)
	if err != nil {
		logger.Error("failed to get namespace", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "namespace not found: %v", err)
	}

	// List knowledge bases WITHOUT ACL filtering
	// The repository ListKnowledgeBases directly queries by owner UID without ACL checks
	kbs, err := h.service.Repository().ListKnowledgeBases(ctx, ns.NsUID.String())
	if err != nil {
		logger.Error("failed to list knowledge bases", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to list knowledge bases: %v", err)
	}

	// Convert to proto format
	pbKBs := make([]*artifactpb.KnowledgeBase, 0, len(kbs))
	for _, kb := range kbs {
		pbKBs = append(pbKBs, &artifactpb.KnowledgeBase{
			Name:        fmt.Sprintf("namespaces/%s/knowledge-bases/%s", namespaceID, kb.ID),
			Id:          kb.ID,
			DisplayName: kb.DisplayName,
			Description: kb.Description,
			Tags:        kb.Tags,
			OwnerName:   ns.Name(),
			CreateTime:  timestamppb.New(*kb.CreateTime),
			UpdateTime:  timestamppb.New(*kb.UpdateTime),
		})
	}

	logger.Info("ListKnowledgeBasesAdmin completed", zap.Int("count", len(pbKBs)))

	return &artifactpb.ListKnowledgeBasesAdminResponse{
		KnowledgeBases: pbKBs,
		TotalSize:      int32(len(pbKBs)),
	}, nil
}

// UpdateKnowledgeBaseAdmin updates a knowledge base with system-reserved tags (admin only).
// Unlike the public UpdateKnowledgeBase, this endpoint:
// - Does NOT validate reserved tag prefixes (allows "instill-", "agent:" etc.)
// - Does NOT require ACL checks (admin-only access)
func (h *PrivateHandler) UpdateKnowledgeBaseAdmin(ctx context.Context, req *artifactpb.UpdateKnowledgeBaseAdminRequest) (*artifactpb.UpdateKnowledgeBaseAdminResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Parse namespace and KB ID from knowledge_base.name (AIP-134)
	namespaceID, knowledgeBaseID, err := parseKnowledgeBaseFromName(req.GetKnowledgeBase().GetName())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid knowledge_base.name format: %v", err)
	}

	logger.Info("UpdateKnowledgeBaseAdmin called",
		zap.String("namespace_id", namespaceID),
		zap.String("knowledge_base_id", knowledgeBaseID))

	// Get namespace
	ns, err := h.service.GetNamespaceByNsID(ctx, namespaceID)
	if err != nil {
		logger.Error("failed to get namespace", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "namespace not found: %v", err)
	}

	// Get existing knowledge base
	kb, err := h.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, knowledgeBaseID)
	if err != nil {
		logger.Error("failed to get knowledge base", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "knowledge base not found: %v", err)
	}

	// Build update model - admin can set any tags including reserved ones
	updateModel := repository.KnowledgeBaseModel{}
	if req.GetUpdateMask() != nil {
		for _, path := range req.GetUpdateMask().GetPaths() {
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
		knowledgeBaseID,
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
		zap.String("id", kb.ID))

	return &artifactpb.UpdateKnowledgeBaseAdminResponse{
		KnowledgeBase: convertKBToCatalogPB(updatedKB, ns, owner, nil),
	}, nil
}

// UpdateFileAdmin updates a file with system-reserved tags (admin only).
// Unlike the public UpdateFile, this endpoint:
// - Does NOT validate reserved tag prefixes (allows "agent:collection:{id}" where {id} is hash-based like col-xxx)
// - Does NOT require ACL checks (admin-only access)
func (h *PrivateHandler) UpdateFileAdmin(ctx context.Context, req *artifactpb.UpdateFileAdminRequest) (*artifactpb.UpdateFileAdminResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Parse namespace, KB, and file ID from file.name (AIP-134)
	// Format: namespaces/{namespace}/knowledge-bases/{kb}/files/{file}
	namespaceID, kbID, fileID, err := parseFileFromName(req.GetFile().GetName())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid file.name format: %v", err)
	}

	logger.Info("UpdateFileAdmin called",
		zap.String("namespace_id", namespaceID),
		zap.String("kb_id", kbID),
		zap.String("file_id", fileID))

	// Get namespace
	ns, err := h.service.GetNamespaceByNsID(ctx, namespaceID)
	if err != nil {
		logger.Error("failed to get namespace", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "namespace not found: %v", err)
	}

	// Get file by hash-based ID
	kbFiles, err := h.service.Repository().GetFilesByFileIDs(ctx, []string{fileID})
	if err != nil || len(kbFiles) == 0 {
		logger.Error("failed to get file", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "file not found")
	}
	kbFile := kbFiles[0]

	// Get KB UIDs from junction table
	kbUIDs, err := h.service.Repository().GetKnowledgeBaseUIDsForFile(ctx, kbFile.UID)
	if err != nil || len(kbUIDs) == 0 {
		logger.Error("failed to get KB associations for file", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "file not associated with any knowledge base")
	}

	// Get knowledge base from file (use first associated KB)
	kb, err := h.service.Repository().GetKnowledgeBaseByUID(ctx, kbUIDs[0])
	if err != nil {
		logger.Error("failed to get knowledge base", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "knowledge base not found: %v", err)
	}

	// Build update map based on field mask - admin can set any tags including reserved ones
	updates := make(map[string]any)
	if req.GetUpdateMask() != nil {
		for _, path := range req.GetUpdateMask().GetPaths() {
			switch path {
			case "tags":
				// Admin endpoint: NO validation of reserved tags
				updates[repository.FileColumn.Tags] = req.GetFile().GetTags()
			case "external_metadata":
				updates[repository.FileColumn.ExternalMetadata] = req.GetFile().GetExternalMetadata()
			}
		}
	}

	if len(updates) == 0 {
		logger.Warn("no fields to update")
		return nil, status.Error(codes.InvalidArgument, "no fields to update")
	}

	// Update file
	updatedFile, err := h.service.Repository().UpdateFile(ctx, kbFile.UID.String(), updates)
	if err != nil {
		logger.Error("failed to update file", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to update file: %v", err)
	}

	// If tags were updated, sync them to Milvus embeddings
	if _, tagsUpdated := updates[repository.FileColumn.Tags]; tagsUpdated {
		// Get the active collection UID for this KB
		collectionID := constant.KBCollectionName(kb.ActiveCollectionUID)

		// Update tags in Milvus for all embeddings of this file
		if err := h.service.Repository().UpdateEmbeddingTagsForFile(ctx, collectionID, types.FileUIDType(kbFile.UID), updatedFile.Tags); err != nil {
			logger.Warn("Failed to update embedding tags in Milvus (file tags in DB were updated)",
				zap.String("fileUID", kbFile.UID.String()),
				zap.Error(err))
			// Don't fail the request - DB tags were updated successfully
			// Milvus tags will be resynced if file is reprocessed
		}
	}

	// Fetch owner for response
	owner, _ := h.service.FetchOwnerByNamespace(ctx, ns)

	// Fetch creator if available
	var creator *mgmtpb.User
	if updatedFile.CreatorUID != uuid.Nil {
		creator, _ = h.service.FetchUserByUID(ctx, updatedFile.CreatorUID.String())
	}

	// Get object ID if file has an associated object (for AIP-122 compliant resource reference)
	objectID := ""
	if updatedFile.ObjectUID != nil {
		obj, err := h.service.Repository().GetObjectByUID(ctx, *updatedFile.ObjectUID)
		if err == nil && obj != nil {
			objectID = string(obj.ID)
		}
	}

	logger.Info("UpdateFileAdmin completed",
		zap.String("file_uid", updatedFile.UID.String()))

	return &artifactpb.UpdateFileAdminResponse{
		File: convertKBFileToPB(updatedFile, ns, kb, owner, creator, objectID),
	}, nil
}

// GetObjectAdmin retrieves the information of an object (admin only).
func (h *PrivateHandler) GetObjectAdmin(ctx context.Context, req *artifactpb.GetObjectAdminRequest) (*artifactpb.GetObjectAdminResponse, error) {
	// Parse object UID from full resource name: objects/{object}
	name := req.GetName()
	parts := strings.Split(name, "/")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid object resource name: %s", name)
	}
	objectUIDStr := parts[len(parts)-1]

	objectUID, err := uuid.FromString(objectUIDStr)
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
	// Parse object UID from full resource name: objects/{object}
	name := req.GetName()
	parts := strings.Split(name, "/")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid object resource name: %s", name)
	}
	objectUIDStr := parts[len(parts)-1]

	objectUID, err := uuid.FromString(objectUIDStr)
	if err != nil {
		h.logger.Error("UpdateObjectAdmin", zap.Error(err))
		return nil, fmt.Errorf("invalid object UID: %w", err)
	}

	updateMap := make(map[string]any)

	if req.GetSize() != 0 {
		updateMap[repository.ObjectColumn.Size] = req.GetSize()
	}
	if req.GetContentType() != "" {
		updateMap[repository.ObjectColumn.ContentType] = req.GetContentType()
	}
	if req.GetIsUploaded() {
		updateMap[repository.ObjectColumn.IsUploaded] = req.GetIsUploaded()
	}
	if req.GetLastModifiedTime() != nil {
		updateMap[repository.ObjectColumn.LastModifiedTime] = req.GetLastModifiedTime().AsTime()
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
	// Extract file ID from resource name: namespaces/{namespace}/files/{file}
	fileID := resource.ExtractResourceID(req.GetName())
	h.logger.Info("DeleteFileAdmin CALLED",
		zap.String("name", req.GetName()),
		zap.String("file_id", fileID))

	// For the admin endpoint, we receive the resource name, so we need to look up the namespace and knowledge base
	// from the file's KB to construct the full request for the public handler
	files, err := h.service.Repository().GetFilesByFileIDs(ctx, []string{fileID})
	if err != nil || len(files) == 0 {
		h.logger.Error("DeleteFileAdmin: failed to get file", zap.Error(err))
		return nil, fmt.Errorf("file not found: %w", err)
	}

	file := files[0]

	// Get KB UIDs from junction table
	kbUIDs, err := h.service.Repository().GetKnowledgeBaseUIDsForFile(ctx, file.UID)
	if err != nil || len(kbUIDs) == 0 {
		h.logger.Error("DeleteFileAdmin: failed to get KB associations for file", zap.Error(err))
		return nil, fmt.Errorf("file not associated with any knowledge base: %w", err)
	}

	kb, err := h.service.Repository().GetKnowledgeBaseByUID(ctx, kbUIDs[0])
	if err != nil {
		h.logger.Error("DeleteFileAdmin: failed to get KB", zap.Error(err))
		return nil, fmt.Errorf("knowledge base not found: %w", err)
	}

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
	// Use file.ID for the name pattern (namespace is extracted from the ID in DeleteFile)
	publicReq := &artifactpb.DeleteFileRequest{
		Name: fmt.Sprintf("namespaces/%s/files/%s", file.NamespaceUID.String(), file.ID),
	}

	resp, err := publicHandler.DeleteFile(ctx, publicReq)
	if err != nil {
		h.logger.Error("DeleteFileAdmin", zap.Error(err))
		return nil, err
	}

	h.logger.Info("DeleteFileAdmin: file deleted successfully",
		zap.String("name", req.GetName()))

	return &artifactpb.DeleteFileAdminResponse{
		Name: resp.Name,
	}, nil
}

// ReprocessFileAdmin triggers file reprocessing without ACL checks (admin only).
func (h *PrivateHandler) ReprocessFileAdmin(ctx context.Context, req *artifactpb.ReprocessFileAdminRequest) (*artifactpb.ReprocessFileAdminResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	fileUIDStr := req.GetFileUid()
	if fileUIDStr == "" {
		return nil, status.Error(codes.InvalidArgument, "file_uid is required")
	}

	logger.Info("ReprocessFileAdmin called", zap.String("file_uid", fileUIDStr))

	// Parse file UID
	fileUID, err := uuid.FromString(fileUIDStr)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid file_uid format: %v", err)
	}

	// Get file by UID
	files, err := h.service.Repository().GetFilesByFileUIDs(ctx, []types.FileUIDType{types.FileUIDType(fileUID)})
	if err != nil {
		logger.Error("failed to get file", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to get file: %v", err)
	}
	if len(files) == 0 {
		return nil, status.Error(codes.NotFound, "file not found")
	}
	file := files[0]

	// Get KB UIDs from junction table
	kbUIDs, err := h.service.Repository().GetKnowledgeBaseUIDsForFile(ctx, file.UID)
	if err != nil || len(kbUIDs) == 0 {
		logger.Error("failed to get KB associations for file", zap.Error(err))
		return nil, status.Error(codes.NotFound, "file not associated with any knowledge base")
	}

	// Get knowledge base
	kb, err := h.service.Repository().GetKnowledgeBaseByUID(ctx, kbUIDs[0])
	if err != nil {
		logger.Error("failed to get knowledge base", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to get knowledge base: %v", err)
	}

	// NOTE: No ACL check - this is an admin endpoint

	// Block reprocessing during validation phase (same as public endpoint)
	if kb.UpdateStatus == artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_VALIDATING.String() {
		return nil, status.Error(codes.ResourceExhausted, "knowledge base is in critical update phase, please wait")
	}

	logger.Info("Starting admin file reprocessing",
		zap.String("fileUID", file.UID.String()),
		zap.String("filename", file.DisplayName),
		zap.String("currentStatus", file.ProcessStatus),
		zap.String("kbUID", kb.UID.String()))

	// Use KB owner as both owner and requester for admin operations
	ownerUID := types.UserUIDType(uuid.FromStringOrNil(kb.NamespaceUID))
	requesterUID := types.RequesterUIDType(uuid.FromStringOrNil(kb.NamespaceUID))

	// Update file status to PROCESSING before triggering the workflow
	updatedFiles, err := h.service.Repository().ProcessFiles(ctx, []string{file.UID.String()}, requesterUID)
	if err != nil {
		logger.Error("Failed to update file status to PROCESSING", zap.Error(err), zap.String("fileUID", file.UID.String()))
		return nil, status.Errorf(codes.Internal, "failed to update file status: %v", err)
	}

	if len(updatedFiles) == 0 {
		return nil, status.Error(codes.NotFound, "file not found after status update")
	}

	// Trigger file processing workflow
	err = h.service.ProcessFile(ctx, kb.UID, []types.FileUIDType{file.UID}, ownerUID, requesterUID)
	if err != nil {
		logger.Error("Failed to trigger file reprocessing",
			zap.Error(err),
			zap.String("fileUID", file.UID.String()),
			zap.String("filename", file.DisplayName))
		return nil, status.Errorf(codes.Internal, "failed to start reprocessing: %v", err)
	}

	logger.Info("Admin file reprocessing started successfully",
		zap.String("fileUID", file.UID.String()),
		zap.String("filename", file.DisplayName),
		zap.String("kbUID", kb.UID.String()))

	// Convert to protobuf response
	updatedFile := updatedFiles[0]
	pbFile := &artifactpb.File{
		Name:          fmt.Sprintf("namespaces/%s/knowledge-bases/%s/files/%s", kb.NamespaceUID, kb.ID, updatedFile.ID),
		Id:            updatedFile.ID,
		DisplayName:   updatedFile.DisplayName,
		Type:          artifactpb.File_Type(artifactpb.File_Type_value[updatedFile.FileType]),
		ProcessStatus: artifactpb.FileProcessStatus(artifactpb.FileProcessStatus_value[updatedFile.ProcessStatus]),
		Size:          updatedFile.Size,
	}
	if updatedFile.CreateTime != nil {
		pbFile.CreateTime = timestamppb.New(*updatedFile.CreateTime)
	}
	if updatedFile.UpdateTime != nil {
		pbFile.UpdateTime = timestamppb.New(*updatedFile.UpdateTime)
	}

	return &artifactpb.ReprocessFileAdminResponse{
		File: pbFile,
	}, nil
}

// RollbackAdmin rolls back a knowledge base to its previous version (admin only)
func (h *PrivateHandler) RollbackAdmin(ctx context.Context, req *artifactpb.RollbackAdminRequest) (*artifactpb.RollbackAdminResponse, error) {
	// Parse resource name: users/{user}/knowledge-bases/{knowledge_base} or namespaces/{namespace}/knowledge-bases/{knowledge_base}
	parts := strings.Split(req.Name, "/")
	if len(parts) != 4 || (parts[0] != "users" && parts[0] != "namespaces") || parts[2] != "knowledge-bases" {
		return nil, status.Errorf(codes.InvalidArgument, "invalid resource name format: %s", req.Name)
	}

	namespaceID := parts[1]
	knowledgeBaseID := parts[3]

	// Resolve owner UID (supports both UUID and namespace ID)
	ownerUID, err := h.resolveOwnerUID(ctx, namespaceID)
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
	// Parse resource name: users/{user}/knowledge-bases/{knowledge_base} or namespaces/{namespace}/knowledge-bases/{knowledge_base}
	parts := strings.Split(req.Name, "/")
	if len(parts) != 4 || (parts[0] != "users" && parts[0] != "namespaces") || parts[2] != "knowledge-bases" {
		return nil, status.Errorf(codes.InvalidArgument, "invalid resource name format: %s", req.Name)
	}

	userID := parts[1]
	knowledgeBaseID := parts[3]

	// Resolve owner UID (supports both UUID and namespace ID)
	ownerUID, err := h.resolveOwnerUID(ctx, userID)
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
	// Parse resource name: users/{user}/knowledge-bases/{knowledge_base} or namespaces/{namespace}/knowledge-bases/{knowledge_base}
	parts := strings.Split(req.Name, "/")
	if len(parts) != 4 || (parts[0] != "users" && parts[0] != "namespaces") || parts[2] != "knowledge-bases" {
		return nil, status.Errorf(codes.InvalidArgument, "invalid resource name format: %s", req.Name)
	}

	userID := parts[1]
	knowledgeBaseID := parts[3]

	// Resolve owner UID (supports both UUID and namespace ID)
	ownerUID, err := h.resolveOwnerUID(ctx, userID)
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
	logger.Info("ExecuteKnowledgeBaseUpdateAdmin handler called", zap.Int("knowledgeBaseCount", len(req.KnowledgeBases)), zap.Strings("knowledgeBaseIds", req.KnowledgeBases))

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
	logger.Info("AbortKnowledgeBaseUpdateAdmin handler called", zap.Int("knowledgeBaseCount", len(req.KnowledgeBases)), zap.Strings("knowledgeBaseIds", req.KnowledgeBases))

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
	// Extract system ID from resource name: systems/{system}
	systemID := resource.ExtractResourceID(req.GetName())
	// Call service
	resp, err := h.service.GetSystemAdmin(ctx, systemID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get system: %v", err)
	}

	h.logger.Info("GetSystemAdmin", zap.String("name", req.GetName()))
	return resp, nil
}

// CreateSystemAdmin creates a new system configuration (admin only)
// The ID is auto-generated as "sys-{hash}" following AIP resource ID convention
func (h *PrivateHandler) CreateSystemAdmin(ctx context.Context, req *artifactpb.CreateSystemAdminRequest) (*artifactpb.CreateSystemAdminResponse, error) {
	// Call service
	resp, err := h.service.CreateSystemAdmin(ctx, req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create system: %v", err)
	}

	h.logger.Info("CreateSystemAdmin",
		zap.String("id", resp.System.Id),
		zap.String("display_name", resp.System.DisplayName),
		zap.String("slug", resp.System.Slug))
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
	// Extract system ID from resource name: systems/{system}
	systemID := resource.ExtractResourceID(req.GetName())
	// Call service
	resp, err := h.service.DeleteSystemAdmin(ctx, systemID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete system: %v", err)
	}

	h.logger.Info("DeleteSystemAdmin", zap.String("name", req.GetName()), zap.Bool("success", resp.Success))
	return resp, nil
}

// RenameSystemAdmin renames a system configuration (admin only)
func (h *PrivateHandler) RenameSystemAdmin(ctx context.Context, req *artifactpb.RenameSystemAdminRequest) (*artifactpb.RenameSystemAdminResponse, error) {
	// Call service
	resp, err := h.service.RenameSystemAdmin(ctx, req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to rename system: %v", err)
	}

	h.logger.Info("RenameSystemAdmin", zap.String("name", req.GetName()), zap.String("new_display_name", req.GetNewDisplayName()))
	return resp, nil
}

// SetDefaultSystemAdmin sets a system as the default (admin only)
func (h *PrivateHandler) SetDefaultSystemAdmin(ctx context.Context, req *artifactpb.SetDefaultSystemAdminRequest) (*artifactpb.SetDefaultSystemAdminResponse, error) {
	// Call service
	resp, err := h.service.SetDefaultSystemAdmin(ctx, req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to set default system: %v", err)
	}

	h.logger.Info("SetDefaultSystemAdmin", zap.String("name", req.GetName()), zap.Bool("is_default", resp.System.IsDefault))
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

// resolveOwnerUID resolves a namespace ID to an owner UID.
// It first tries to parse as UUID (for backward compatibility), then looks up via service.
func (h *PrivateHandler) resolveOwnerUID(ctx context.Context, namespaceID string) (types.OwnerUIDType, error) {
	// First try to parse as UUID (backward compatibility)
	if uid, err := uuid.FromString(namespaceID); err == nil {
		return types.OwnerUIDType(uid), nil
	}

	// If not a UUID, look up the namespace via service
	ns, err := h.service.GetNamespaceByNsID(ctx, namespaceID)
	if err != nil {
		return types.OwnerUIDType{}, fmt.Errorf("failed to look up namespace %q: %w", namespaceID, err)
	}

	// Use the namespace's UID directly (NsUID is the owner UID)
	return types.OwnerUIDType(ns.NsUID), nil
}

// ResetKnowledgeBaseEmbeddingsAdmin resets all embeddings for a knowledge base (admin only).
func (h *PrivateHandler) ResetKnowledgeBaseEmbeddingsAdmin(ctx context.Context, req *artifactpb.ResetKnowledgeBaseEmbeddingsAdminRequest) (*artifactpb.ResetKnowledgeBaseEmbeddingsAdminResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Parse namespace and KB ID from name (format: namespaces/{namespace}/knowledge-bases/{kb})
	namespaceID, knowledgeBaseID, err := parseKnowledgeBaseFromName(req.GetName())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid name format: %v", err)
	}

	logger.Info("ResetKnowledgeBaseEmbeddingsAdmin called",
		zap.String("namespace_id", namespaceID),
		zap.String("knowledge_base_id", knowledgeBaseID))

	// Get namespace
	ns, err := h.service.GetNamespaceByNsID(ctx, namespaceID)
	if err != nil {
		logger.Error("failed to get namespace", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "namespace not found: %v", err)
	}

	// Get knowledge base
	kb, err := h.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, knowledgeBaseID)
	if err != nil {
		logger.Error("failed to get knowledge base", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "knowledge base not found: %v", err)
	}

	kbUID := types.KBUIDType(kb.UID)
	collectionName := constant.KBCollectionName(kb.ActiveCollectionUID)

	logger.Info("Resetting KB embeddings for BM25 support",
		zap.String("kbUID", kb.UID.String()),
		zap.String("collectionName", collectionName))

	// Step 1: Drop Milvus collection (removes old schema without BM25 fields)
	if err := h.service.Repository().DropCollection(ctx, collectionName); err != nil {
		// Log but continue - collection might not exist yet
		logger.Warn("Failed to drop Milvus collection (may not exist)", zap.Error(err))
	} else {
		logger.Info("Dropped Milvus collection", zap.String("collection", collectionName))
	}

	// Step 2: Delete embeddings from PostgreSQL
	if err := h.service.Repository().HardDeleteEmbeddingsByKBUID(ctx, kbUID); err != nil {
		logger.Error("Failed to delete embeddings", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to delete embeddings: %v", err)
	}

	// Step 3: Delete chunks from PostgreSQL
	if err := h.service.Repository().HardDeleteTextChunksByKBUID(ctx, kbUID); err != nil {
		logger.Error("Failed to delete chunks", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to delete chunks: %v", err)
	}

	// Step 4: Delete converted files from PostgreSQL
	if err := h.service.Repository().DeleteAllConvertedFilesInKb(ctx, kbUID); err != nil {
		logger.Error("Failed to delete converted files", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to delete converted files: %v", err)
	}

	// Step 5: Reset all file statuses to NOTSTARTED
	fileCount, err := h.service.Repository().ResetFileStatusesByKBUID(ctx, kbUID)
	if err != nil {
		logger.Error("Failed to reset file statuses", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to reset file statuses: %v", err)
	}

	logger.Info("KB embeddings reset completed",
		zap.String("kbUID", kb.UID.String()),
		zap.Int64("filesToReembed", fileCount))

	// Fetch owner for response
	owner, _ := h.service.FetchOwnerByNamespace(ctx, ns)

	return &artifactpb.ResetKnowledgeBaseEmbeddingsAdminResponse{
		KnowledgeBase:  convertKBToCatalogPB(kb, ns, owner, nil),
		FilesToReembed: int32(fileCount),
	}, nil
}

// AddFilesToKnowledgeBaseAdmin adds file associations to a target KB by file resource names (admin only).
// Files can belong to multiple KBs (many-to-many relationship).
func (h *PrivateHandler) AddFilesToKnowledgeBaseAdmin(ctx context.Context, req *artifactpb.AddFilesToKnowledgeBaseAdminRequest) (*artifactpb.AddFilesToKnowledgeBaseAdminResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Parse target KB from name (format: namespaces/{namespace}/knowledge-bases/{kb})
	targetNamespaceID, targetKBID, err := parseKnowledgeBaseFromName(req.GetTargetKnowledgeBase())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid target_knowledge_base format: %v", err)
	}

	logger.Info("AddFilesToKnowledgeBaseAdmin called",
		zap.String("target_namespace_id", targetNamespaceID),
		zap.String("target_kb_id", targetKBID),
		zap.Int("file_count", len(req.GetFiles())))

	// Get target namespace
	targetNs, err := h.service.GetNamespaceByNsID(ctx, targetNamespaceID)
	if err != nil {
		logger.Error("failed to get target namespace", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "target namespace not found: %v", err)
	}

	// Get target knowledge base
	targetKB, err := h.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, targetNs.NsUID, targetKBID)
	if err != nil {
		logger.Error("failed to get target knowledge base", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "target knowledge base not found: %v", err)
	}

	// Parse file resource names to extract file IDs
	// Format: namespaces/{namespace}/files/{file}
	fileIDs := make([]string, 0, len(req.GetFiles()))
	for _, fileName := range req.GetFiles() {
		_, _, fileID, err := parseFileFromName(fileName)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid file resource name format: %s", fileName)
		}
		fileIDs = append(fileIDs, fileID)
	}

	// Add file associations
	filesAdded, err := h.service.Repository().AddFilesToKnowledgeBase(ctx, types.KnowledgeBaseUIDType(targetKB.UID), fileIDs)
	if err != nil {
		logger.Error("failed to add files to knowledge base", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to add files: %v", err)
	}

	logger.Info("AddFilesToKnowledgeBaseAdmin completed",
		zap.String("target_kb_uid", targetKB.UID.String()),
		zap.Int64("files_added", filesAdded))

	return &artifactpb.AddFilesToKnowledgeBaseAdminResponse{
		FilesAdded: int32(filesAdded),
	}, nil
}

// DeleteKnowledgeBaseAdmin force-deletes a knowledge base (admin only).
// CASCADE removes file-KB associations from junction table. Files remain orphaned.
// Also cleans up the Milvus collection.
func (h *PrivateHandler) DeleteKnowledgeBaseAdmin(ctx context.Context, req *artifactpb.DeleteKnowledgeBaseAdminRequest) (*artifactpb.DeleteKnowledgeBaseAdminResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Parse namespace and KB ID from name (format: namespaces/{namespace}/knowledge-bases/{kb})
	namespaceID, knowledgeBaseID, err := parseKnowledgeBaseFromName(req.GetName())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid name format: %v", err)
	}

	logger.Info("DeleteKnowledgeBaseAdmin called",
		zap.String("namespace_id", namespaceID),
		zap.String("knowledge_base_id", knowledgeBaseID))

	// Get namespace
	ns, err := h.service.GetNamespaceByNsID(ctx, namespaceID)
	if err != nil {
		logger.Error("failed to get namespace", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "namespace not found: %v", err)
	}

	// Get knowledge base
	kb, err := h.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, knowledgeBaseID)
	if err != nil {
		logger.Error("failed to get knowledge base", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "knowledge base not found: %v", err)
	}

	collectionName := constant.KBCollectionName(kb.ActiveCollectionUID)

	// Step 1: Drop Milvus collection if it exists
	if err := h.service.Repository().DropCollection(ctx, collectionName); err != nil {
		// Log but continue - collection might not exist
		logger.Warn("Failed to drop Milvus collection (may not exist)", zap.Error(err), zap.String("collection", collectionName))
	} else {
		logger.Info("Dropped Milvus collection", zap.String("collection", collectionName))
	}

	// Step 2: Remove ACL entry
	if err := h.service.ACLClient().Purge(ctx, "knowledgebase", kb.UID); err != nil {
		// Log but continue - ACL entry might not exist
		logger.Warn("Failed to purge ACL entry", zap.Error(err))
	}

	// Step 3: Hard delete the knowledge base (CASCADE will remove file_knowledge_base associations)
	if err := h.service.Repository().HardDeleteKnowledgeBase(ctx, kb.UID.String()); err != nil {
		logger.Error("failed to delete knowledge base", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to delete knowledge base: %v", err)
	}

	logger.Info("DeleteKnowledgeBaseAdmin completed",
		zap.String("kb_uid", kb.UID.String()),
		zap.String("kb_id", kb.ID))

	return &artifactpb.DeleteKnowledgeBaseAdminResponse{}, nil
}

// ListFilesAdmin lists files in a knowledge base without ACL checks (admin only).
// Used by internal services during migrations and administrative operations.
func (h *PrivateHandler) ListFilesAdmin(ctx context.Context, req *artifactpb.ListFilesAdminRequest) (*artifactpb.ListFilesAdminResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Parse namespace and KB ID from parent (format: namespaces/{namespace}/knowledge-bases/{kb})
	namespaceID, knowledgeBaseID, err := parseKnowledgeBaseFromName(req.GetParent())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid parent format: %v", err)
	}

	logger.Info("ListFilesAdmin called",
		zap.String("namespace_id", namespaceID),
		zap.String("knowledge_base_id", knowledgeBaseID),
		zap.Int32("page_size", req.GetPageSize()))

	// Get namespace
	ns, err := h.service.GetNamespaceByNsID(ctx, namespaceID)
	if err != nil {
		logger.Error("failed to get namespace", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "namespace not found: %v", err)
	}

	// Get knowledge base
	kb, err := h.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, knowledgeBaseID)
	if err != nil {
		logger.Error("failed to get knowledge base", zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "knowledge base not found: %v", err)
	}

	// Set default page size
	pageSize := req.GetPageSize()
	if pageSize <= 0 {
		pageSize = 100
	}

	// List files from repository without ACL checks
	files, nextPageToken, totalSize, err := h.service.Repository().ListKnowledgeBaseFilesAdmin(
		ctx,
		types.KnowledgeBaseUIDType(kb.UID),
		pageSize,
		req.GetPageToken(),
	)
	if err != nil {
		logger.Error("failed to list files", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to list files: %v", err)
	}

	// Convert to proto files with minimal information (admin context - no owner/creator)
	protoFiles := make([]*artifactpb.File, len(files))
	for i, f := range files {
		protoFiles[i] = convertKBFileToPB(&f, ns, kb, nil, nil, "")
	}

	logger.Info("ListFilesAdmin completed",
		zap.Int("files_returned", len(protoFiles)),
		zap.Int32("total_size", totalSize))

	return &artifactpb.ListFilesAdminResponse{
		Files:         protoFiles,
		NextPageToken: nextPageToken,
		TotalSize:     totalSize,
	}, nil
}
