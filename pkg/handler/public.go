package handler

import (
	"context"
	"fmt"

	artifact "github.com/instill-ai/artifact-backend/pkg/service"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	healthcheckPB "github.com/instill-ai/protogen-go/common/healthcheck/v1beta"
)

// PublicHandler handles public API
type PublicHandler struct {
	artifactpb.UnimplementedArtifactPublicServiceServer
	ctx     context.Context
	service *artifact.Service
}

// NewPublicHandler initiates a handler instance
func NewPublicHandler(ctx context.Context, service *artifact.Service) artifactpb.ArtifactPublicServiceServer {
	return &PublicHandler{
		ctx:     ctx,
		service: service}
}

// Liveness returns the health of the service.
func (h *PublicHandler) Liveness(_ context.Context, _ *artifactpb.LivenessRequest) (*artifactpb.LivenessResponse, error) {
	fmt.Println("Liveness")
	return &artifactpb.LivenessResponse{
		HealthCheckResponse: &healthcheckPB.HealthCheckResponse{
			Status: healthcheckPB.HealthCheckResponse_SERVING_STATUS_SERVING,
		},
	}, nil
}

// Readiness returns the state of the service.
func (h *PublicHandler) Readiness(_ context.Context, _ *artifactpb.ReadinessRequest) (*artifactpb.ReadinessResponse, error) {
	return &artifactpb.ReadinessResponse{
		HealthCheckResponse: &healthcheckPB.HealthCheckResponse{
			Status: healthcheckPB.HealthCheckResponse_SERVING_STATUS_SERVING,
		},
	}, nil
}
