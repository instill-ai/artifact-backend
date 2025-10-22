package handler

import (
	"context"
	"fmt"
	"strings"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifact "github.com/instill-ai/artifact-backend/pkg/service"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	constantx "github.com/instill-ai/x/constant"
	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
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

// ListRepositoryTagsAdmin returns the versions of a distribution registry
// repository (admin only).
func (h *PrivateHandler) ListRepositoryTagsAdmin(ctx context.Context, req *artifactpb.ListRepositoryTagsAdminRequest) (*artifactpb.ListRepositoryTagsAdminResponse, error) {

	resp, err := h.service.ListRepositoryTags(ctx, (*artifactpb.ListRepositoryTagsRequest)(req))
	if err != nil {
		return nil, err
	}

	h.logger.Info("ListRepositoryTagsAdmin")
	return (*artifactpb.ListRepositoryTagsAdminResponse)(resp), nil
}

// CreateRepositoryTagAdmin registers the information of a repository tag after it
// has been pushed to the registry (admin only).
func (h *PrivateHandler) CreateRepositoryTagAdmin(ctx context.Context, req *artifactpb.CreateRepositoryTagAdminRequest) (*artifactpb.CreateRepositoryTagAdminResponse, error) {
	resp, err := h.service.CreateRepositoryTag(ctx, (*artifactpb.CreateRepositoryTagRequest)(req))
	if err != nil {
		return nil, err
	}

	h.logger.Info("CreateRepositoryTagAdmin")
	return (*artifactpb.CreateRepositoryTagAdminResponse)(resp), nil
}

// GetRepositoryTagAdmin retrieve the information of a repository tag (admin only).
func (h *PrivateHandler) GetRepositoryTagAdmin(ctx context.Context, req *artifactpb.GetRepositoryTagAdminRequest) (*artifactpb.GetRepositoryTagAdminResponse, error) {
	resp, err := h.service.GetRepositoryTag(ctx, (*artifactpb.GetRepositoryTagRequest)(req))
	if err != nil {
		return nil, err
	}

	h.logger.Info("GetRepositoryTagAdmin")
	return (*artifactpb.GetRepositoryTagAdminResponse)(resp), nil
}

// DeleteRepositoryTagAdmin deletes the information of a repository tag in registry (admin only).
func (h *PrivateHandler) DeleteRepositoryTagAdmin(ctx context.Context, req *artifactpb.DeleteRepositoryTagAdminRequest) (*artifactpb.DeleteRepositoryTagAdminResponse, error) {
	resp, err := h.service.DeleteRepositoryTag(ctx, (*artifactpb.DeleteRepositoryTagRequest)(req))
	if err != nil {
		return nil, err
	}

	h.logger.Info("DeleteRepositoryTagAdmin")
	return (*artifactpb.DeleteRepositoryTagAdminResponse)(resp), nil
}

// GetObjectURLAdmin retrieves the information of an object URL (admin only).
func (h *PrivateHandler) GetObjectURLAdmin(ctx context.Context, req *artifactpb.GetObjectURLAdminRequest) (*artifactpb.GetObjectURLAdminResponse, error) {
	// check if both UID and EncodedURLPath, one of them is provided
	if req.GetUid() != "" && req.GetEncodedUrlPath() != "" {
		return nil, fmt.Errorf("one of UID or EncodedURLPath must be provided")
	}

	var resp *repository.ObjectURLModel
	var err error
	objectURLUID := uuid.FromStringOrNil(req.GetUid())
	if objectURLUID != uuid.Nil {
		resp, err = h.service.Repository().GetObjectURLByUID(ctx, objectURLUID)
		if err != nil {
			h.logger.Error("GetObjectURLAdmin", zap.Error(err))
			return nil, fmt.Errorf("cannot get object URL by UID: %w", err)
		}
	} else if req.GetEncodedUrlPath() != "" {
		resp, err = h.service.Repository().GetObjectURLByEncodedURLPath(ctx, req.GetEncodedUrlPath())
		if err != nil {
			h.logger.Error("GetObjectURLAdmin", zap.Error(err))
			return nil, fmt.Errorf("cannot get object URL by encoded URL path: %w", err)
		}
	}

	return (*artifactpb.GetObjectURLAdminResponse)(repository.TurnObjectURLToResponse(resp)), nil
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

// GetFileAsMarkdownAdmin returns the Markdown representation of a file (admin only).
func (h *PrivateHandler) GetFileAsMarkdownAdmin(ctx context.Context, req *artifactpb.GetFileAsMarkdownAdminRequest) (*artifactpb.GetFileAsMarkdownAdminResponse, error) {
	fileUID := uuid.FromStringOrNil(req.GetFileUid())
	source, err := h.service.Repository().GetSourceByFileUID(ctx, fileUID)
	if err != nil {
		return nil, fmt.Errorf("fetching truth source: %w", err)
	}

	// get the source file sourceContent from minIO using dest of source
	sourceContent, err := h.service.Repository().GetFile(ctx, config.Config.Minio.BucketName, source.Dest)
	if err != nil {
		return nil, fmt.Errorf("getting source file from blob storage: %w", err)
	}

	return &artifactpb.GetFileAsMarkdownAdminResponse{Markdown: string(sourceContent)}, nil
}

// GetChatFileAdmin returns the Markdown representation of a file (admin only, deprecated).
// This method is deprecated and GetFileAsMarkdownAdmin should be used instead.
// TODO: As soon as clients update to GetFileAsMarkdownAdmin, this endpoint should
// be be removed.
func (h *PrivateHandler) GetChatFileAdmin(ctx context.Context, req *artifactpb.GetChatFileAdminRequest) (*artifactpb.GetChatFileAdminResponse, error) {
	// use catalog id and file id to get kbFile
	fileID := req.FileId
	if fileID == "" {
		h.logger.Error("file id is empty", zap.String("file_id", fileID))
		return nil, fmt.Errorf("need either file uid or file id is")
	}
	ns, err := h.service.GetNamespaceByNsID(ctx, req.NamespaceId)
	if err != nil {
		h.logger.Error("failed to get namespace by ns id", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get namespace by ns id: %w", err),
			"Unable to access the specified namespace. Please check the namespace ID and try again.",
		)
	}
	kb, err := h.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, req.CatalogId)
	if err != nil {
		h.logger.Error("failed to get knowledge base by owner and kb id", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get catalog by namespace and catalog id: %w", err),
			"Unable to access the specified catalog. Please check the catalog ID and try again.",
		)
	}

	kbFile, err := h.service.Repository().GetKnowledgebaseFileByKBUIDAndFileID(ctx, kb.UID, fileID)
	if err != nil {
		h.logger.Error("failed to get file by file id", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get file by file id: %w", err),
			"File not found. Please check the file ID and try again.",
		)
	}

	// get source file
	source, err := h.service.Repository().GetSourceByFileUID(ctx, kbFile.UID)
	if err != nil {
		h.logger.Error("failed to get truth source by file uid", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get truth source by file uid: %w", err),
			"Unable to retrieve file source. Please try again.",
		)
	}

	// get the source file sourceContent from minIO using dest of source
	sourceContent, err := h.service.Repository().GetFile(ctx, config.Config.Minio.BucketName, source.Dest)
	if err != nil {
		h.logger.Error("failed to get file from minio", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get file from minio: %w", err),
			"Unable to retrieve file content. Please try again.",
		)
	}

	// Add the originalData field to the response
	return &artifactpb.GetChatFileAdminResponse{
		Markdown: sourceContent,
	}, nil
}

// DeleteCatalogFileAdmin deletes a file from a catalog (admin only).
// This is a private gRPC-only method for internal operations like integration tests.
func (h *PrivateHandler) DeleteCatalogFileAdmin(ctx context.Context, req *artifactpb.DeleteCatalogFileAdminRequest) (*artifactpb.DeleteCatalogFileAdminResponse, error) {
	h.logger.Info("DeleteCatalogFileAdmin CALLED",
		zap.String("file_uid_from_request", req.GetFileUid()))

	// For the admin endpoint, we only receive file_uid, so we need to look up the namespace and catalog
	// from the file's KB to construct the full request for the public handler
	fileUID := uuid.FromStringOrNil(req.GetFileUid())

	files, err := h.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []types.FileUIDType{types.FileUIDType(fileUID)})
	if err != nil || len(files) == 0 {
		h.logger.Error("DeleteCatalogFileAdmin: failed to get file", zap.Error(err))
		return nil, fmt.Errorf("file not found: %w", err)
	}

	file := files[0]
	kb, err := h.service.Repository().GetKnowledgeBaseByUID(ctx, file.KBUID)
	if err != nil {
		h.logger.Error("DeleteCatalogFileAdmin: failed to get KB", zap.Error(err))
		return nil, fmt.Errorf("catalog not found: %w", err)
	}

	// Create namespace ID from owner UID for the public API
	namespaceID := fmt.Sprintf("users/%s", kb.Owner)

	// For admin endpoints, inject owner UID into gRPC metadata for authentication
	// Get existing metadata and append to it
	md, _ := metadata.FromIncomingContext(ctx)
	if md == nil {
		md = metadata.MD{}
	}
	// Set the auth type and user UID headers (make a copy to avoid modifying the original)
	md = md.Copy()
	md.Set(strings.ToLower(constantx.HeaderAuthTypeKey), "user")
	md.Set(strings.ToLower(constantx.HeaderUserUIDKey), kb.Owner)
	ctx = metadata.NewIncomingContext(ctx, md)

	h.logger.Info("DeleteCatalogFileAdmin: Injected metadata",
		zap.String("auth_type", "user"),
		zap.String("user_uid", kb.Owner))

	// Create a public handler to reuse the existing delete logic
	publicHandler := &PublicHandler{
		service: h.service,
	}

	// Delegate to the public handler's implementation which includes:
	// - ACL checks (with staging/rollback KB bypass)
	// - Soft-deletion of file
	// - Dual deletion to staging/rollback KB if applicable
	// - Cleanup workflow triggering
	publicReq := &artifactpb.DeleteCatalogFileRequest{
		NamespaceId: namespaceID,
		CatalogId:   kb.KBID,
		FileUid:     req.FileUid,
	}

	resp, err := publicHandler.DeleteCatalogFile(ctx, publicReq)
	if err != nil {
		h.logger.Error("DeleteCatalogFileAdmin", zap.Error(err))
		return nil, err
	}

	h.logger.Info("DeleteCatalogFileAdmin: file deleted successfully",
		zap.String("file_uid", req.FileUid))

	return &artifactpb.DeleteCatalogFileAdminResponse{
		FileUid: resp.FileUid,
	}, nil
}

// RollbackAdmin rolls back a knowledge base to its previous version (admin only)
func (h *PrivateHandler) RollbackAdmin(ctx context.Context, req *artifactpb.RollbackAdminRequest) (*artifactpb.RollbackAdminResponse, error) {
	// Parse resource name: users/{user}/catalogs/{catalog}
	parts := strings.Split(req.Name, "/")
	if len(parts) != 4 || parts[0] != "users" || parts[2] != "catalogs" {
		return nil, status.Errorf(codes.InvalidArgument, "invalid resource name format: %s", req.Name)
	}

	userID := parts[1]
	catalogID := parts[3]

	// Parse owner UID
	ownerUID, err := parseOwnerUID(userID)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid user ID: %v", err)
	}

	// Call service
	resp, err := h.service.RollbackAdmin(ctx, ownerUID, catalogID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to rollback: %v", err)
	}

	return resp, nil
}

// PurgeRollbackAdmin manually purges the rollback knowledge base immediately (admin only)
func (h *PrivateHandler) PurgeRollbackAdmin(ctx context.Context, req *artifactpb.PurgeRollbackAdminRequest) (*artifactpb.PurgeRollbackAdminResponse, error) {
	// Parse resource name
	parts := strings.Split(req.Name, "/")
	if len(parts) != 4 || parts[0] != "users" || parts[2] != "catalogs" {
		return nil, status.Errorf(codes.InvalidArgument, "invalid resource name format: %s", req.Name)
	}

	userID := parts[1]
	catalogID := parts[3]

	// Parse owner UID
	ownerUID, err := parseOwnerUID(userID)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid user ID: %v", err)
	}

	// Call service
	resp, err := h.service.PurgeRollbackAdmin(ctx, ownerUID, catalogID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to purge rollback: %v", err)
	}

	return resp, nil
}

// SetRollbackRetentionAdmin sets the rollback retention period for a knowledge base with flexible time units (admin only)
func (h *PrivateHandler) SetRollbackRetentionAdmin(ctx context.Context, req *artifactpb.SetRollbackRetentionAdminRequest) (*artifactpb.SetRollbackRetentionAdminResponse, error) {
	// Parse resource name
	parts := strings.Split(req.Name, "/")
	if len(parts) != 4 || parts[0] != "users" || parts[2] != "catalogs" {
		return nil, status.Errorf(codes.InvalidArgument, "invalid resource name format: %s", req.Name)
	}

	userID := parts[1]
	catalogID := parts[3]

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
	resp, err := h.service.SetRollbackRetentionAdmin(ctx, ownerUID, catalogID, req.Duration, req.TimeUnit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to set rollback retention: %v", err)
	}

	return resp, nil
}

// ExecuteKnowledgeBaseUpdateAdmin executes the prepared knowledge base update (admin only)
func (h *PrivateHandler) ExecuteKnowledgeBaseUpdateAdmin(ctx context.Context, req *artifactpb.ExecuteKnowledgeBaseUpdateAdminRequest) (*artifactpb.ExecuteKnowledgeBaseUpdateAdminResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	logger.Info("ExecuteKnowledgeBaseUpdateAdmin handler called", zap.Int("catalogCount", len(req.CatalogIds)), zap.Strings("catalogIds", req.CatalogIds))

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
	logger.Info("AbortKnowledgeBaseUpdateAdmin handler called", zap.Int("catalogCount", len(req.CatalogIds)), zap.Strings("catalogIds", req.CatalogIds))

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

// GetSystemProfileAdmin retrieves a system configuration profile (admin only)
func (h *PrivateHandler) GetSystemProfileAdmin(ctx context.Context, req *artifactpb.GetSystemProfileAdminRequest) (*artifactpb.GetSystemProfileAdminResponse, error) {
	// Call service
	resp, err := h.service.GetSystemProfileAdmin(ctx, req.Profile)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get system profile: %v", err)
	}

	h.logger.Info("GetSystemProfileAdmin", zap.String("profile", req.Profile))
	return resp, nil
}

// UpdateSystemProfileAdmin creates or updates a system configuration profile (admin only)
func (h *PrivateHandler) UpdateSystemProfileAdmin(ctx context.Context, req *artifactpb.UpdateSystemProfileAdminRequest) (*artifactpb.UpdateSystemProfileAdminResponse, error) {
	// Call service
	resp, err := h.service.UpdateSystemProfileAdmin(ctx, req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update system profile: %v", err)
	}

	h.logger.Info("UpdateSystemProfileAdmin", zap.String("profile", req.SystemProfile.Profile))
	return resp, nil
}

// ListSystemProfilesAdmin lists all system configuration profiles (admin only)
func (h *PrivateHandler) ListSystemProfilesAdmin(ctx context.Context, req *artifactpb.ListSystemProfilesAdminRequest) (*artifactpb.ListSystemProfilesAdminResponse, error) {
	// Call service
	resp, err := h.service.ListSystemProfilesAdmin(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list system profiles: %v", err)
	}

	h.logger.Info("ListSystemProfilesAdmin", zap.Int("count", len(resp.SystemProfiles)))
	return resp, nil
}

// DeleteSystemProfileAdmin deletes a system configuration profile (admin only)
func (h *PrivateHandler) DeleteSystemProfileAdmin(ctx context.Context, req *artifactpb.DeleteSystemProfileAdminRequest) (*artifactpb.DeleteSystemProfileAdminResponse, error) {
	// Call service
	resp, err := h.service.DeleteSystemProfileAdmin(ctx, req.Profile)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete system profile: %v", err)
	}

	h.logger.Info("DeleteSystemProfileAdmin", zap.String("profile", req.Profile), zap.Bool("success", resp.Success))
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
