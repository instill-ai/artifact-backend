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

	pb "github.com/instill-ai/protogen-go/model/model/v1alpha"
)

// NewModelPublicClient returns an initialized gRPC client for the Model public
// API.
func NewModelPublicClient(ctx context.Context) (pb.ModelPublicServiceClient, *grpc.ClientConn) {
	logger, _ := logger.GetZapLogger(ctx)

	credDialOpt := grpc.WithTransportCredentials(insecure.NewCredentials())
	if config.Config.ModelBackend.HTTPS.Cert != "" && config.Config.ModelBackend.HTTPS.Key != "" {
		creds, err := credentials.NewServerTLSFromFile(config.Config.ModelBackend.HTTPS.Cert, config.Config.ModelBackend.HTTPS.Key)
		if err != nil {
			logger.Fatal(err.Error())
		}
		credDialOpt = grpc.WithTransportCredentials(creds)
	}

	clientConn, err := grpc.NewClient(
		fmt.Sprintf("%v:%v", config.Config.ModelBackend.Host, config.Config.ModelBackend.PublicPort),
		credDialOpt,
		grpc.WithUnaryInterceptor(middleware.MetadataPropagatorInterceptor),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(constant.MaxPayloadSize),
			grpc.MaxCallSendMsgSize(constant.MaxPayloadSize),
		),
	)
	if err != nil {
		logger.Error(err.Error())
		return nil, nil
	}

	return pb.NewModelPublicServiceClient(clientConn), clientConn
}
