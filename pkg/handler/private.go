package handler

import (
	"context"
	"fmt"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/repository"

	artifact "github.com/instill-ai/artifact-backend/pkg/service"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
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

// ListRepositoryTags returns the versions of a distribution registry
// repository.
func (h *PrivateHandler) ListRepositoryTags(ctx context.Context, req *artifactpb.ListRepositoryTagsRequest) (*artifactpb.ListRepositoryTagsResponse, error) {

	resp, err := h.service.ListRepositoryTags(ctx, req)
	if err != nil {
		return nil, err
	}

	h.logger.Info("ListRepositoryTags")
	return resp, nil
}

// CreateRepositoryTag registers the information of a repository tag after it
// has been pushed to the registry.
func (h *PrivateHandler) CreateRepositoryTag(ctx context.Context, req *artifactpb.CreateRepositoryTagRequest) (*artifactpb.CreateRepositoryTagResponse, error) {
	resp, err := h.service.CreateRepositoryTag(ctx, req)
	if err != nil {
		return nil, err
	}

	h.logger.Info("CreateRepositoryTag")
	return resp, nil
}

// GetRepositoryTag retrieve the information of a repository tag.
func (h *PrivateHandler) GetRepositoryTag(ctx context.Context, req *artifactpb.GetRepositoryTagRequest) (*artifactpb.GetRepositoryTagResponse, error) {
	resp, err := h.service.GetRepositoryTag(ctx, req)
	if err != nil {
		return nil, err
	}

	h.logger.Info("GetRepositoryTag")
	return resp, nil
}

// DeleteRepositoryTag deletes the information of a repository tag in registry.
func (h *PrivateHandler) DeleteRepositoryTag(ctx context.Context, req *artifactpb.DeleteRepositoryTagRequest) (*artifactpb.DeleteRepositoryTagResponse, error) {
	resp, err := h.service.DeleteRepositoryTag(ctx, req)
	if err != nil {
		return nil, err
	}

	h.logger.Info("DeleteRepositoryTag")
	return resp, nil
}

// GetObjectURL retrieves the information of an object URL.
func (h *PrivateHandler) GetObjectURL(ctx context.Context, req *artifactpb.GetObjectURLRequest) (*artifactpb.GetObjectURLResponse, error) {
	// check if both UID and EncodedURLPath, one of them is provided
	if req.GetUid() != "" && req.GetEncodedUrlPath() != "" {
		return nil, fmt.Errorf("one of UID or EncodedURLPath must be provided")
	}

	var resp *repository.ObjectURL
	var err error
	objectURLUID := uuid.FromStringOrNil(req.GetUid())
	if objectURLUID != uuid.Nil {
		resp, err = h.service.Repository().GetObjectURLByUID(ctx, objectURLUID)
		if err != nil {
			h.logger.Error("GetObjectURL", zap.Error(err))
			return nil, fmt.Errorf("cannot get object URL by UID: %w", err)
		}
	} else if req.GetEncodedUrlPath() != "" {
		resp, err = h.service.Repository().GetObjectURLByEncodedURLPath(ctx, req.GetEncodedUrlPath())
		if err != nil {
			h.logger.Error("GetObjectURL", zap.Error(err))
			return nil, fmt.Errorf("cannot get object URL by encoded URL path: %w", err)
		}
	}

	return repository.TurnObjectURLToResponse(resp), nil
}

func (h *PrivateHandler) GetObject(ctx context.Context, req *artifactpb.GetObjectRequest) (*artifactpb.GetObjectResponse, error) {
	objectUID, err := uuid.FromString(req.GetUid())
	if err != nil {
		h.logger.Error("GetObject", zap.Error(err))
		return nil, err
	}

	obj, err := h.service.Repository().GetObjectByUID(ctx, objectUID)
	if err != nil {
		h.logger.Error("GetObject", zap.Error(err))
		return nil, err
	}

	if obj == nil {
		return nil, fmt.Errorf("object not found")
	}

	return &artifactpb.GetObjectResponse{
		Object: repository.TurnObjectInDBToObjectInProto(obj),
	}, nil
}

// UpdateObject updates the information of an object
func (h *PrivateHandler) UpdateObject(ctx context.Context, req *artifactpb.UpdateObjectRequest) (*artifactpb.UpdateObjectResponse, error) {
	objectUID, err := uuid.FromString(req.GetUid())
	if err != nil {
		h.logger.Error("UpdateObject", zap.Error(err))
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
		h.logger.Error("UpdateObject", zap.Error(err))
		return nil, fmt.Errorf("failed to update object: %w", err)
	}

	return &artifactpb.UpdateObjectResponse{
		Object: repository.TurnObjectInDBToObjectInProto(updatedObject),
	}, nil
}

// GetFileAsMarkdown returns the Markdown representation of a file.
func (h *PrivateHandler) GetFileAsMarkdown(ctx context.Context, req *artifactpb.GetFileAsMarkdownRequest) (*artifactpb.GetFileAsMarkdownResponse, error) {
	fileUID := uuid.FromStringOrNil(req.GetFileUid())
	source, err := h.service.Repository().GetTruthSourceByFileUID(ctx, fileUID)
	if err != nil {
		return nil, fmt.Errorf("fetching truth source: %w", err)
	}

	// get the source file sourceContent from minIO using dest of source
	sourceContent, err := h.service.MinIO().GetFile(ctx, config.Config.Minio.BucketName, source.Dest)
	if err != nil {
		return nil, fmt.Errorf("getting source file from blob storage: %w", err)
	}

	return &artifactpb.GetFileAsMarkdownResponse{Markdown: string(sourceContent)}, nil
}

// GetChatFile returns the Markdown representation of a file.
// This method is deprecated and GetFileAsMarkdown should be used instead.
// TODO: As soon as clients update to GetFileAsMarkdown, this endpoint should
// be be removed.
func (h *PrivateHandler) GetChatFile(ctx context.Context, req *artifactpb.GetChatFileRequest) (*artifactpb.GetChatFileResponse, error) {
	// use catalog id and file id to get kbFile
	fileID := req.FileId
	if fileID == "" {
		h.logger.Error("file id is empty", zap.String("file_id", fileID))
		return nil, fmt.Errorf("need either file uid or file id is")
	}
	ns, err := h.service.GetNamespaceByNsID(ctx, req.NamespaceId)
	if err != nil {
		h.logger.Error("failed to get namespace by ns id", zap.Error(err))
		return nil, fmt.Errorf("failed to get namespace by ns id. err: %w", err)
	}
	kb, err := h.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, req.CatalogId)
	if err != nil {
		h.logger.Error("failed to get knowledge base by owner and kb id", zap.Error(err))
		return nil, fmt.Errorf("failed to get catalog by namespace and catalog id. err: %w", err)
	}

	kbFile, err := h.service.Repository().GetKnowledgebaseFileByKbUIDAndFileID(ctx, kb.UID, fileID)
	if err != nil {
		h.logger.Error("failed to get file by file id", zap.Error(err))
		return nil, fmt.Errorf("failed to get file by file id. err: %w", err)
	}

	// get source file
	source, err := h.service.Repository().GetTruthSourceByFileUID(ctx, kbFile.UID)
	if err != nil {
		h.logger.Error("failed to get truth source by file uid", zap.Error(err))
		return nil, fmt.Errorf("failed to get truth source by file uid. err: %w", err)
	}

	// get the source file sourceContent from minIO using dest of source
	sourceContent, err := h.service.MinIO().GetFile(ctx, config.Config.Minio.BucketName, source.Dest)
	if err != nil {
		h.logger.Error("failed to get file from minio", zap.Error(err))
		return nil, fmt.Errorf("failed to get file from minio. err: %w", err)
	}

	// Add the originalData field to the response
	return &artifactpb.GetChatFileResponse{
		Markdown: sourceContent,
	}, nil
}
