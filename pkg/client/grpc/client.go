package grpcclient

import (
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func NewGRPCConn(server, cert, key string) (*grpc.ClientConn, error) {
	var creds credentials.TransportCredentials
	if cert == "" || key == "" {
		creds = insecure.NewCredentials()
	} else {
		var err error
		creds, err = credentials.NewServerTLSFromFile(cert, key)
		if err != nil {
			return nil, err
		}
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(constant.MaxPayloadSize),
			grpc.MaxCallSendMsgSize(constant.MaxPayloadSize),
		),
	}

	return grpc.NewClient(server, opts...)
}
