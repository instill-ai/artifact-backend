package handler

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/instill-ai/artifact-backend/pkg/logger"
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
