package grpcclient

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/middleware"
	"github.com/instill-ai/x/client"

	pb "github.com/instill-ai/protogen-go/core/mgmt/v1beta"
)

// NewMGMTPrivateClient returns an initialized gRPC client for the MGMT private
// API.
func NewMGMTPrivateClient(ctx context.Context) (pb.MgmtPrivateServiceClient, *grpc.ClientConn) {
	logger, _ := logger.GetZapLogger(ctx)

	credDialOpt := grpc.WithTransportCredentials(insecure.NewCredentials())
	if config.Config.MgmtBackend.HTTPS.Cert != "" && config.Config.MgmtBackend.HTTPS.Key != "" {
		creds, err := credentials.NewServerTLSFromFile(config.Config.MgmtBackend.HTTPS.Cert, config.Config.MgmtBackend.HTTPS.Key)
		if err != nil {
			logger.Fatal(err.Error())
		}
		credDialOpt = grpc.WithTransportCredentials(creds)
	}

	clientConn, err := grpc.NewClient(
		fmt.Sprintf("%v:%v", config.Config.MgmtBackend.Host, config.Config.MgmtBackend.PrivatePort),
		credDialOpt,
		grpc.WithUnaryInterceptor(middleware.MetadataPropagatorInterceptor),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(client.MaxPayloadSize),
			grpc.MaxCallSendMsgSize(client.MaxPayloadSize),
		),
	)
	if err != nil {
		logger.Error(err.Error())
		return nil, nil
	}

	return pb.NewMgmtPrivateServiceClient(clientConn), clientConn
}
