package grpcclient

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/instill-ai/x/client"
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
			grpc.MaxCallRecvMsgSize(client.MaxPayloadSize),
			grpc.MaxCallSendMsgSize(client.MaxPayloadSize),
		),
	}

	return grpc.NewClient(server, opts...)
}
