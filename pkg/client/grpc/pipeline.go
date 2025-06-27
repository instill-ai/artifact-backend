package grpcclient

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/middleware"
	"github.com/instill-ai/x/client"

	pb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
)

// NewPipelinePublicClient returns an initialized gRPC client for the Pipeline public
// API.
func NewPipelinePublicClient(
	ctx context.Context,
	svc client.ServiceConfig,
) (pb.PipelinePublicServiceClient, *grpc.ClientConn) {
	logger, _ := logger.GetZapLogger(ctx)

	credDialOpt := grpc.WithTransportCredentials(insecure.NewCredentials())
	if svc.HTTPS.Cert != "" && svc.HTTPS.Key != "" {
		creds, err := credentials.NewServerTLSFromFile(svc.HTTPS.Cert, svc.HTTPS.Key)
		if err != nil {
			logger.Fatal(err.Error())
		}
		credDialOpt = grpc.WithTransportCredentials(creds)
	}

	clientConn, err := grpc.NewClient(
		fmt.Sprintf("%v:%v", svc.Host, svc.PublicPort),
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
