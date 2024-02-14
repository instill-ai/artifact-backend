package handler

import (
	"context"

	"github.com/instill-ai/artifact-backend/pkg/service"

	artifactPB "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	healthcheckPB "github.com/instill-ai/protogen-go/common/healthcheck/v1beta"
)

// TODO: in the public_handler, we should convert all id to uuid when calling service

// var tracer = otel.Tracer("artifact-backend.public-handler.tracer")

// PublicHandler handles public API
type PublicHandler struct {
	artifactPB.UnimplementedArtifactPublicServiceServer
	service service.Service
}

// NewPublicHandler initiates a handler instance
func NewPublicHandler(ctx context.Context, s service.Service) artifactPB.ArtifactPublicServiceServer {
	return &PublicHandler{
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

func (h *PublicHandler) Liveness(ctx context.Context, req *artifactPB.LivenessRequest) (*artifactPB.LivenessResponse, error) {
	return &artifactPB.LivenessResponse{
		HealthCheckResponse: &healthcheckPB.HealthCheckResponse{
			Status: healthcheckPB.HealthCheckResponse_SERVING_STATUS_SERVING,
		},
	}, nil
}

func (h *PublicHandler) Readiness(ctx context.Context, req *artifactPB.ReadinessRequest) (*artifactPB.ReadinessResponse, error) {
	return &artifactPB.ReadinessResponse{
		HealthCheckResponse: &healthcheckPB.HealthCheckResponse{
			Status: healthcheckPB.HealthCheckResponse_SERVING_STATUS_SERVING,
		},
	}, nil
}
