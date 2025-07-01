package main

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/service"
	"github.com/instill-ai/x/log"

	grpcclient "github.com/instill-ai/artifact-backend/pkg/client/grpc"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
)

func main() {
	ctx := context.Background()

	log.Debug = config.Config.Server.Debug
	logger, _ := log.GetZapLogger(ctx)

	if err := config.Init(); err != nil {
		logger.Fatal("Failed to initialize config", zap.Error(err))
	}

	pipelinePublicGrpcConn, err := grpcclient.NewGRPCConn(
		fmt.Sprintf("%v:%v", config.Config.PipelineBackend.Host,
			config.Config.PipelineBackend.PublicPort),
		config.Config.PipelineBackend.HTTPS.Cert,
		config.Config.PipelineBackend.HTTPS.Key)
	if err != nil {
		logger.Fatal("Failed to create pipeline client", zap.Error(err))
	}
	defer func() {
		if err := pipelinePublicGrpcConn.Close(); err != nil {
			logger.Error("Failed to close pipeline client", zap.Error(err))
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "Instill-Service", "instill")

	upserter := &service.PipelineReleaseUpserter{
		FS:                          service.PresetPipelinesFS,
		PipelinePublicServiceClient: pipelinepb.NewPipelinePublicServiceClient(pipelinePublicGrpcConn),
	}

	for _, pr := range service.PresetPipelinesList {
		logger := logger.With(zap.String("id", pr.ID), zap.String("version", pr.Version))
		if err := upserter.Upsert(ctx, pr); err != nil {
			logger.Error("Failed to add pipeline", zap.Error(err))
			continue
		}

		logger.Info("Processed pipeline")
	}
}
