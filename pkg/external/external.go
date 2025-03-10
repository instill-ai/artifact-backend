package external

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/logger"

	mgmtpb "github.com/instill-ai/protogen-go/core/mgmt/v1beta"
	usagepb "github.com/instill-ai/protogen-go/core/usage/v1beta"
)

// InitMgmtPrivateServiceClient initialises a MgmtPrivateServiceClient instance
func InitMgmtPrivateServiceClient(ctx context.Context) (mgmtpb.MgmtPrivateServiceClient, *grpc.ClientConn) {
	logger, _ := logger.GetZapLogger(ctx)

	var clientDialOpts grpc.DialOption
	if config.Config.MgmtBackend.HTTPS.Cert != "" && config.Config.MgmtBackend.HTTPS.Key != "" {
		creds, err := credentials.NewServerTLSFromFile(config.Config.MgmtBackend.HTTPS.Cert, config.Config.MgmtBackend.HTTPS.Key)
		if err != nil {
			logger.Fatal(err.Error())
		}
		clientDialOpts = grpc.WithTransportCredentials(creds)
	} else {
		clientDialOpts = grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	clientConn, err := grpc.NewClient(fmt.Sprintf("%v:%v", config.Config.MgmtBackend.Host, config.Config.MgmtBackend.PrivatePort), clientDialOpts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(constant.MaxPayloadSize), grpc.MaxCallSendMsgSize(constant.MaxPayloadSize)))
	if err != nil {
		logger.Error(err.Error())
		return nil, nil
	}

	return mgmtpb.NewMgmtPrivateServiceClient(clientConn), clientConn
}

func InitMgmtPublicServiceClient(ctx context.Context) (mgmtpb.MgmtPublicServiceClient, *grpc.ClientConn) {
	logger, _ := logger.GetZapLogger(ctx)

	var clientDialOpts grpc.DialOption
	if config.Config.MgmtBackend.HTTPS.Cert != "" && config.Config.MgmtBackend.HTTPS.Key != "" {
		creds, err := credentials.NewServerTLSFromFile(config.Config.MgmtBackend.HTTPS.Cert, config.Config.MgmtBackend.HTTPS.Key)
		if err != nil {
			logger.Fatal(err.Error())
		}
		clientDialOpts = grpc.WithTransportCredentials(creds)
	} else {
		clientDialOpts = grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	clientConn, err := grpc.NewClient(fmt.Sprintf("%v:%v", config.Config.MgmtBackend.Host, config.Config.MgmtBackend.PublicPort), clientDialOpts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(constant.MaxPayloadSize), grpc.MaxCallSendMsgSize(constant.MaxPayloadSize)))
	if err != nil {
		logger.Error(err.Error())
		return nil, nil
	}

	return mgmtpb.NewMgmtPublicServiceClient(clientConn), clientConn
}

// InitUsageServiceClient initialises a UsageServiceClient instance (no mTLS)
func InitUsageServiceClient(ctx context.Context) (usagepb.UsageServiceClient, *grpc.ClientConn) {
	logger, _ := logger.GetZapLogger(ctx)

	var clientDialOpts grpc.DialOption
	var err error
	if config.Config.Server.Usage.TLSEnabled {
		tlsConfig := &tls.Config{}
		clientDialOpts = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	} else {
		clientDialOpts = grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	clientConn, err := grpc.NewClient(fmt.Sprintf("%v:%v", config.Config.Server.Usage.Host, config.Config.Server.Usage.Port), clientDialOpts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(constant.MaxPayloadSize), grpc.MaxCallSendMsgSize(constant.MaxPayloadSize)))
	if err != nil {
		logger.Error(err.Error())
		return nil, nil
	}

	return usagepb.NewUsageServiceClient(clientConn), clientConn
}

// InitInfluxDBServiceClient initialises a InfluxDBServiceClient instance
func InitInfluxDBServiceClient(ctx context.Context) (influxdb2.Client, api.WriteAPI) {

	logger, _ := logger.GetZapLogger(ctx)

	var creds credentials.TransportCredentials
	var err error

	influxOptions := influxdb2.DefaultOptions()
	if config.Config.Server.Debug {
		influxOptions = influxOptions.SetLogLevel(log.DebugLevel)
	}
	influxOptions = influxOptions.SetFlushInterval(uint(time.Duration(config.Config.InfluxDB.FlushInterval * int(time.Second)).Milliseconds()))

	if config.Config.InfluxDB.HTTPS.Cert != "" && config.Config.InfluxDB.HTTPS.Key != "" {
		// TODO: support TLS
		creds, err = credentials.NewServerTLSFromFile(config.Config.InfluxDB.HTTPS.Cert, config.Config.InfluxDB.HTTPS.Key)
		if err != nil {
			logger.Fatal(err.Error())
		}
		logger.Info(creds.Info().ServerName)
	}

	client := influxdb2.NewClientWithOptions(
		config.Config.InfluxDB.URL,
		config.Config.InfluxDB.Token,
		influxOptions,
	)

	if _, err := client.Ping(ctx); err != nil {
		logger.Warn(err.Error())
	}

	writeAPI := client.WriteAPI(config.Config.InfluxDB.Org, config.Config.InfluxDB.Bucket)

	return client, writeAPI
}
