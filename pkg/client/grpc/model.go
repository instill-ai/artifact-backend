package grpcclient

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/logger"

	pb "github.com/instill-ai/protogen-go/model/model/v1alpha"
)

// ModelClientConfig contains configuration for model client
type ModelClientConfig struct {
	MaxRetries           int
	InitBackoff          time.Duration
	MaxBackoff           time.Duration
	BackoffMultiplier    float64
	RetryableStatusCodes []string
}

// DefaultModelClientConfig provides default retry configuration
func DefaultModelClientConfig() *ModelClientConfig {
	return &ModelClientConfig{
		MaxRetries:        5,
		InitBackoff:       5 * time.Second,
		MaxBackoff:        15 * time.Second,
		BackoffMultiplier: 1.5,
		RetryableStatusCodes: []string{
			"UNAVAILABLE",
			"RESOURCE_EXHAUSTED",
			"INTERNAL",
		},
	}
}

// NewModelPublicClient returns an initialized gRPC client for the Model public
// API.
func NewModelPublicClient(ctx context.Context) (pb.ModelPublicServiceClient, *grpc.ClientConn) {
	logger, _ := logger.GetZapLogger(ctx)

	var clientDialOpts grpc.DialOption
	if config.Config.ModelBackend.HTTPS.Cert != "" && config.Config.ModelBackend.HTTPS.Key != "" {
		creds, err := credentials.NewServerTLSFromFile(config.Config.ModelBackend.HTTPS.Cert, config.Config.ModelBackend.HTTPS.Key)
		if err != nil {
			logger.Fatal(err.Error())
		}
		clientDialOpts = grpc.WithTransportCredentials(creds)
	} else {
		clientDialOpts = grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	// Configure retry policy
	retryConfig := DefaultModelClientConfig()

	// Configure connection backoff parameters
	connParams := grpc.ConnectParams{
		Backoff: backoff.Config{
			BaseDelay:  retryConfig.InitBackoff,
			Multiplier: retryConfig.BackoffMultiplier,
			Jitter:     0.2,
			MaxDelay:   retryConfig.MaxBackoff,
		},
		MinConnectTimeout: 5 * time.Second,
	}

	clientConn, err := grpc.NewClient(
		fmt.Sprintf("%v:%v", config.Config.ModelBackend.Host, config.Config.ModelBackend.PublicPort),
		grpc.WithConnectParams(connParams),
		clientDialOpts,
		grpc.WithDefaultServiceConfig(`{
			"methodConfig": [{
				"name": [{"service": "model.model.v1alpha.ModelPublicService"}],
				"waitForReady": true,
				"retryPolicy": {
					"MaxAttempts": 4,
					"InitialBackoff": "5s",
					"MaxBackoff": "15s",
					"BackoffMultiplier": 1.5,
					"RetryableStatusCodes": ["UNAVAILABLE", "RESOURCE_EXHAUSTED", "INTERNAL"]
				}
			}]
		}`),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(constant.MaxPayloadSize), grpc.MaxCallSendMsgSize(constant.MaxPayloadSize)),
	)
	if err != nil {
		logger.Error(err.Error())
		return nil, nil
	}

	return pb.NewModelPublicServiceClient(clientConn), clientConn
}
