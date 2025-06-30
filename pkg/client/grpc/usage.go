package grpcclient

import (
	"context"
	"crypto/tls"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/x/client"
	"github.com/instill-ai/x/log"

	pb "github.com/instill-ai/protogen-go/core/usage/v1beta"
)

// NewUsageClient returns an intialized gRPC client for the Usage API.
func NewUsageClient(ctx context.Context) (pb.UsageServiceClient, *grpc.ClientConn) {
	logger, _ := log.GetZapLogger(ctx)

	var clientDialOpts grpc.DialOption
	var err error
	if config.Config.Server.Usage.TLSEnabled {
		tlsConfig := &tls.Config{}
		clientDialOpts = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	} else {
		clientDialOpts = grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	clientConn, err := grpc.NewClient(fmt.Sprintf("%v:%v", config.Config.Server.Usage.Host, config.Config.Server.Usage.Port), clientDialOpts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(client.MaxPayloadSize), grpc.MaxCallSendMsgSize(client.MaxPayloadSize)))
	if err != nil {
		logger.Error(err.Error())
		return nil, nil
	}

	return pb.NewUsageServiceClient(clientConn), clientConn
}
