package handler

import (
	"context"

	"go.uber.org/zap"

	artifact "github.com/instill-ai/artifact-backend/pkg/service"
	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
	healthcheckpb "github.com/instill-ai/protogen-go/common/healthcheck/v1beta"
)

// PublicHandler handles public API
type PublicHandler struct {
	artifactpb.UnimplementedArtifactPublicServiceServer
	service artifact.Service
}

// NewPublicHandler initiates a handler instance
func NewPublicHandler(service artifact.Service, log *zap.Logger) *PublicHandler {
	return &PublicHandler{
		service: service,
	}
}

// Liveness returns the health of the service.
func (ph *PublicHandler) Liveness(_ context.Context, _ *artifactpb.LivenessRequest) (*artifactpb.LivenessResponse, error) {
	return &artifactpb.LivenessResponse{
		HealthCheckResponse: &healthcheckpb.HealthCheckResponse{
			Status: healthcheckpb.HealthCheckResponse_SERVING_STATUS_SERVING,
		},
	}, nil
}

// Readiness returns the state of the service.
func (ph *PublicHandler) Readiness(_ context.Context, _ *artifactpb.ReadinessRequest) (*artifactpb.ReadinessResponse, error) {
	return &artifactpb.ReadinessResponse{
		HealthCheckResponse: &healthcheckpb.HealthCheckResponse{
			Status: healthcheckpb.HealthCheckResponse_SERVING_STATUS_SERVING,
		},
	}, nil
}
