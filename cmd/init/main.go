package main

import (
	"context"
	"log"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"

	grpczap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/pipeline"

	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
	clientx "github.com/instill-ai/x/client"
	clientgrpcx "github.com/instill-ai/x/client/grpc"
	logx "github.com/instill-ai/x/log"
)

func main() {
	ctx := context.Background()

	if err := config.Init(config.ParseConfigFlag()); err != nil {
		log.Fatal(err.Error())
	}

	logx.Debug = config.Config.Server.Debug
	logger, _ := logx.GetZapLogger(context.Background())
	defer func() {
		// can't handle the error due to https://github.com/uber-go/zap/issues/880
		_ = logger.Sync()
	}()

	// Set gRPC logging based on debug mode
	if config.Config.Server.Debug {
		grpczap.ReplaceGrpcLoggerV2WithVerbosity(logger, 0) // All logs
	} else {
		grpczap.ReplaceGrpcLoggerV2WithVerbosity(logger, 3) // verbosity 3 will avoid [transport] from emitting
	}

	pipelinePublicServiceClient, pipelinePublicClose, err := clientgrpcx.NewClient[pipelinepb.PipelinePublicServiceClient](
		clientgrpcx.WithServiceConfig(clientx.ServiceConfig{
			Host:       config.Config.PipelineBackend.Host,
			PublicPort: config.Config.PipelineBackend.PublicPort,
		}),
		clientgrpcx.WithSetOTELClientHandler(config.Config.OTELCollector.Enable),
	)
	if err != nil {
		logger.Fatal("failed to create pipeline public service client", zap.Error(err))
	}
	defer func() {
		if err := pipelinePublicClose(); err != nil {
			logger.Error("failed to close pipeline public service client", zap.Error(err))
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "Instill-Service", "instill")

	upserter := &pipeline.PipelineReleaseUpserter{
		FS:                          pipeline.PresetPipelinesFS,
		PipelinePublicServiceClient: pipelinePublicServiceClient,
	}

	for _, pr := range pipeline.PresetPipelinesList {
		logger := logger.With(zap.String("id", pr.ID), zap.String("version", pr.Version))
		if err := upserter.Upsert(ctx, pr); err != nil {
			logger.Error("Failed to add pipeline", zap.Error(err))
			continue
		}

		logger.Info("Processed pipeline")
	}
}
