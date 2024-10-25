package handler

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/gofrs/uuid"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/repository"

	artifact "github.com/instill-ai/artifact-backend/pkg/service"
	pb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

var tracer = otel.Tracer("artifact-backend.private-handler.tracer")

// PrivateHandler handles the private Artifact endpoints.
type PrivateHandler struct {
	pb.UnimplementedArtifactPrivateServiceServer
	service *artifact.Service
}

// NewPrivateHandler returns an initialized private handler.
func NewPrivateHandler(_ context.Context, s *artifact.Service) pb.ArtifactPrivateServiceServer {
	return &PrivateHandler{
		service: s,
	}
}

// ListRepositoryTags returns the versions of a distribution registry
// repository.
func (h *PrivateHandler) ListRepositoryTags(ctx context.Context, req *pb.ListRepositoryTagsRequest) (*pb.ListRepositoryTagsResponse, error) {
	ctx, span := tracer.Start(ctx, "ListRepositoryTags", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	logger, _ := logger.GetZapLogger(ctx)

	resp, err := h.service.ListRepositoryTags(ctx, req)
	if err != nil {
		span.SetStatus(1, err.Error())
		return nil, err
	}

	logger.Info("ListRepositoryTags")
	return resp, nil
}

// CreateRepositoryTag registers the information of a repository tag after it
// has been pushed to the registry.
func (h *PrivateHandler) CreateRepositoryTag(ctx context.Context, req *pb.CreateRepositoryTagRequest) (*pb.CreateRepositoryTagResponse, error) {
	ctx, span := tracer.Start(ctx, "CreateRepositoryTag", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	logger, _ := logger.GetZapLogger(ctx)

	resp, err := h.service.CreateRepositoryTag(ctx, req)
	if err != nil {
		span.SetStatus(1, err.Error())
		return nil, err
	}

	logger.Info("CreateRepositoryTag")
	return resp, nil
}

// GetRepositoryTag retrieve the information of a repository tag.
func (h *PrivateHandler) GetRepositoryTag(ctx context.Context, req *pb.GetRepositoryTagRequest) (*pb.GetRepositoryTagResponse, error) {
	ctx, span := tracer.Start(ctx, "GetRepositoryTag", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	logger, _ := logger.GetZapLogger(ctx)

	resp, err := h.service.GetRepositoryTag(ctx, req)
	if err != nil {
		span.SetStatus(1, err.Error())
		return nil, err
	}

	logger.Info("GeteRepositoryTag")
	return resp, nil
}

// DeleteRepositoryTag deletes the information of a repository tag in registry.
func (h *PrivateHandler) DeleteRepositoryTag(ctx context.Context, req *pb.DeleteRepositoryTagRequest) (*pb.DeleteRepositoryTagResponse, error) {
	ctx, span := tracer.Start(ctx, "DeleteRepositoryTag", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	logger, _ := logger.GetZapLogger(ctx)

	resp, err := h.service.DeleteRepositoryTag(ctx, req)
	if err != nil {
		span.SetStatus(1, err.Error())
		return nil, err
	}

	logger.Info("DeleteRepositoryTag")
	return resp, nil
}

// GetObjectURL retrieves the information of an object URL.
func (h *PrivateHandler) GetObjectURL(ctx context.Context, req *pb.GetObjectURLRequest) (*pb.GetObjectURLResponse, error) {
	ctx, span := tracer.Start(ctx, "GetObjectURL", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	logger, _ := logger.GetZapLogger(ctx)

	// check if both UID and EncodedURLPath, one of them is provided
	if req.GetUid() != "" && req.GetEncodedUrlPath() != "" {
		return nil, fmt.Errorf("one of UID or EncodedURLPath must be provided")
	}

	var resp *repository.ObjectURL
	var err error
	objectURLUID := uuid.FromStringOrNil(req.GetUid())
	if objectURLUID != uuid.Nil {
		resp, err = h.service.Repository.GetObjectURLByUID(ctx, objectURLUID)
		if err != nil {
			span.SetStatus(1, err.Error())
			logger.Error("GetObjectURL", zap.Error(err))
			return nil, fmt.Errorf("cannot get object URL by UID: %w", err)
		}
	} else if req.GetEncodedUrlPath() != "" {
		resp, err = h.service.Repository.GetObjectURLByEncodedURLPath(ctx, req.GetEncodedUrlPath())
		if err != nil {
			span.SetStatus(1, err.Error())
			logger.Error("GetObjectURL", zap.Error(err))
			return nil, fmt.Errorf("cannot get object URL by encoded URL path: %w", err)
		}
	}

	return repository.TurnObjectURLToResponse(resp), nil
}

func (h *PrivateHandler) GetObject(ctx context.Context, req *pb.GetObjectRequest) (*pb.GetObjectResponse, error) {
	ctx, span := tracer.Start(ctx, "GetObject", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	logger, _ := logger.GetZapLogger(ctx)

	objectUID, err := uuid.FromString(req.GetUid())
	if err != nil {
		span.SetStatus(1, err.Error())
		logger.Error("GetObject", zap.Error(err))
		return nil, err
	}

	obj, err := h.service.Repository.GetObjectByUID(ctx, objectUID)
	if err != nil {
		span.SetStatus(1, err.Error())
		logger.Error("GetObject", zap.Error(err))
		return nil, err
	}

	if obj == nil {
		return nil, fmt.Errorf("object not found")
	}

	return &pb.GetObjectResponse{
		Object: repository.TurnObjectInDBToObjectInProto(obj),
	}, nil
}

// UpdateObject updates the information of an object
func (h *PrivateHandler) UpdateObject(ctx context.Context, req *pb.UpdateObjectRequest) (*pb.UpdateObjectResponse, error) {
	ctx, span := tracer.Start(ctx, "UpdateObject", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	logger, _ := logger.GetZapLogger(ctx)

	objectUID, err := uuid.FromString(req.GetUid())
	if err != nil {
		span.SetStatus(1, err.Error())
		logger.Error("UpdateObject", zap.Error(err))
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

	updatedObject, err := h.service.Repository.UpdateObjectByUpdateMap(ctx, objectUID, updateMap)
	if err != nil {
		span.SetStatus(1, err.Error())
		logger.Error("UpdateObject", zap.Error(err))
		return nil, fmt.Errorf("failed to update object: %w", err)
	}

	return &pb.UpdateObjectResponse{
		Object: repository.TurnObjectInDBToObjectInProto(updatedObject),
	}, nil
}
