package handler

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	artifactPB "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	healthcheckPB "github.com/instill-ai/protogen-go/common/healthcheck/v1beta"

	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/service"
)

// TODO: in the public_handler, we should convert all id to uuid when calling service

var tracer = otel.Tracer("artifact-backend.public-handler.tracer")

// PublicHandler handles public API
type PublicHandler struct {
	artifactPB.UnimplementedArtifactPublicServiceServer
	service service.Service
}

// NewPublicHandler initiates a handler instance
func NewPublicHandler(_ context.Context, s service.Service) artifactPB.ArtifactPublicServiceServer {
	return &PublicHandler{
		service: s,
	}
}

// PrivateHandler handles the private Artifact endpoints.
type PrivateHandler struct {
	artifactPB.UnimplementedArtifactPrivateServiceServer
	service service.Service
}

// NewPrivateHandler returns an initialized private handler.
func NewPrivateHandler(_ context.Context, s service.Service) artifactPB.ArtifactPrivateServiceServer {
	return &PrivateHandler{
		service: s,
	}
}

// GetService returns the service
func (h *PublicHandler) GetService() service.Service {
	return h.service
}

// SetService sets the service
func (h *PublicHandler) SetService(s service.Service) {
	h.service = s
}

func (h *PublicHandler) Liveness(_ context.Context, req *artifactPB.LivenessRequest) (*artifactPB.LivenessResponse, error) {
	return &artifactPB.LivenessResponse{
		HealthCheckResponse: &healthcheckPB.HealthCheckResponse{
			Status: healthcheckPB.HealthCheckResponse_SERVING_STATUS_SERVING,
		},
	}, nil
}

func (h *PublicHandler) Readiness(_ context.Context, req *artifactPB.ReadinessRequest) (*artifactPB.ReadinessResponse, error) {
	return &artifactPB.ReadinessResponse{
		HealthCheckResponse: &healthcheckPB.HealthCheckResponse{
			Status: healthcheckPB.HealthCheckResponse_SERVING_STATUS_SERVING,
		},
	}, nil
}

// ListRepositoryTags returns the versions of a distribution registry
// repository.
func (h *PrivateHandler) ListRepositoryTags(ctx context.Context, req *artifactPB.ListRepositoryTagsRequest) (*artifactPB.ListRepositoryTagsResponse, error) {
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
