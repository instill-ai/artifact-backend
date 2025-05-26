package grpcclient

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/middleware"

	pb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
)

// NewPipelinePublicClient returns an initialized gRPC client for the Pipeline public
// API.
func NewPipelinePublicClient(ctx context.Context) (pb.PipelinePublicServiceClient, *grpc.ClientConn) {
	logger, _ := logger.GetZapLogger(ctx)

	credDialOpt := grpc.WithTransportCredentials(insecure.NewCredentials())
	if config.Config.PipelineBackend.HTTPS.Cert != "" && config.Config.PipelineBackend.HTTPS.Key != "" {
		creds, err := credentials.NewServerTLSFromFile(config.Config.PipelineBackend.HTTPS.Cert, config.Config.PipelineBackend.HTTPS.Key)
		if err != nil {
			logger.Fatal(err.Error())
		}
		credDialOpt = grpc.WithTransportCredentials(creds)
	}

	clientConn, err := grpc.NewClient(
		fmt.Sprintf("%v:%v", config.Config.PipelineBackend.Host, config.Config.PipelineBackend.PublicPort),
		credDialOpt,
		grpc.WithUnaryInterceptor(middleware.MetadataPropagatorInterceptor),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(constant.MaxPayloadSize),
			grpc.MaxCallSendMsgSize(constant.MaxPayloadSize)),
	)
	if err != nil {
		logger.Error(err.Error())
		return nil, nil
	}

	return pb.NewPipelinePublicServiceClient(clientConn), clientConn
}
