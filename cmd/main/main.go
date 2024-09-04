package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	openfga "github.com/openfga/api/proto/openfga/v1"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/propagators/b3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/encoding/protojson"
	"gorm.io/gorm"

	grpcMiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcZap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpcRecovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/acl"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/handler"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/middleware"
	"github.com/instill-ai/artifact-backend/pkg/milvus"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	servicePkg "github.com/instill-ai/artifact-backend/pkg/service"
	"github.com/instill-ai/artifact-backend/pkg/usage"
	"github.com/instill-ai/artifact-backend/pkg/utils"
	"github.com/instill-ai/artifact-backend/pkg/worker"

	grpcClient "github.com/instill-ai/artifact-backend/pkg/client/grpc"
	httpClient "github.com/instill-ai/artifact-backend/pkg/client/http"
	database "github.com/instill-ai/artifact-backend/pkg/db"
	customOtel "github.com/instill-ai/artifact-backend/pkg/logger/otel"
	minio "github.com/instill-ai/artifact-backend/pkg/minio"
	artifactPB "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	mgmtPB "github.com/instill-ai/protogen-go/core/mgmt/v1beta"
	pipelinePB "github.com/instill-ai/protogen-go/vdp/pipeline/v1beta"
)

var propagator propagation.TextMapPropagator

// grpcHandlerFunc handles incoming HTTP requests and routes them to either the gRPC server or the gateway handler.
// It wraps the handler function with h2c.NewHandler to support HTTP/2 requests.
// The function extracts the B3 context from the incoming request headers and sets it in the request context.
// If the request is a gRPC request, it calls the gRPC server's ServeHTTP method.
// Otherwise, it calls the gateway handler's ServeHTTP method.
func grpcHandlerFunc(grpcServer *grpc.Server, gwHandler http.Handler) http.Handler {
	return h2c.NewHandler(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			propagator = b3.New(b3.WithInjectEncoding(b3.B3MultipleHeader))
			ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
			r = r.WithContext(ctx)

			if r.ProtoMajor == 2 && strings.Contains(r.Header.Get("Content-Type"), "application/grpc") {
				grpcServer.ServeHTTP(w, r)
			} else {
				gwHandler.ServeHTTP(w, r)
			}
		}),
		&http2.Server{},
	)
}

func main() {
	// gorm's autoUpdate will use local timezone by default, so we need to set it to UTC
	time.Local = time.UTC

	// Initialize config
	if err := config.Init(); err != nil {
		log.Fatal(err.Error())
	}

	// setup tracing and metrics
	ctx, cancel := context.WithCancel(context.Background())

	tp, err := customOtel.SetupTracing(ctx, "artifact-backend")
	if err != nil {
		panic(err)
	}

	defer func() {
		err = tp.Shutdown(ctx)
	}()

	ctx, span := otel.Tracer("main-tracer").Start(ctx,
		"main",
	)
	defer cancel()

	logger, _ := logger.GetZapLogger(ctx)
	defer func() {
		// can't handle the error due to https://github.com/uber-go/zap/issues/880
		_ = logger.Sync()
	}()

	// verbosity 3 will avoid [transport] from emitting
	grpcZap.ReplaceGrpcLoggerV2WithVerbosity(logger, 3)

	// Initialize clients needed for service
	pipelinePublicServiceClient, pipelinePublicGrpcConn, _, mgmtPublicServiceClientConn, mgmtPrivateServiceClient, mgmtPrivateServiceGrpcConn,
		redisClient, influxDBClient, db, minioClient, milvusClient, aclClient, fgaClientConn, fgaReplicaClientConn := newClients(ctx, logger)
	if pipelinePublicGrpcConn != nil {
		defer pipelinePublicGrpcConn.Close()
	}
	if mgmtPublicServiceClientConn != nil {
		defer mgmtPublicServiceClientConn.Close()
	}
	if mgmtPrivateServiceGrpcConn != nil {
		defer mgmtPrivateServiceGrpcConn.Close()
	}
	defer redisClient.Close()
	defer influxDBClient.Close()
	defer database.Close(db)
	if fgaClientConn != nil {
		defer fgaClientConn.Close()
	}
	if fgaReplicaClientConn != nil {
		defer fgaReplicaClientConn.Close()
	}

	// Initialize service
	service := servicePkg.NewService(
		repository.NewRepository(db),
		minioClient,
		mgmtPrivateServiceClient,
		pipelinePublicServiceClient,
		httpClient.NewRegistryClient(ctx),
		redisClient,
		milvusClient,
		aclClient)

	grpcServerOpts, creds := newGrpcOptionAndCreds(logger)

	publicGrpcS := grpc.NewServer(grpcServerOpts...)
	reflection.Register(publicGrpcS)
	artifactPB.RegisterArtifactPublicServiceServer(
		publicGrpcS,
		handler.NewPublicHandler(ctx, service),
	)

	privateGrpcS := grpc.NewServer(grpcServerOpts...)
	reflection.Register(privateGrpcS)
	artifactPB.RegisterArtifactPrivateServiceServer(
		privateGrpcS,
		handler.NewPrivateHandler(ctx, service),
	)

	publicServeMux := runtime.NewServeMux(
		runtime.WithForwardResponseOption(middleware.HTTPResponseModifier),
		runtime.WithErrorHandler(middleware.ErrorHandler),
		runtime.WithIncomingHeaderMatcher(middleware.CustomMatcher),
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
			MarshalOptions: protojson.MarshalOptions{
				EmitUnpopulated: true,
				UseEnumNumbers:  false,
			},
			UnmarshalOptions: protojson.UnmarshalOptions{
				DiscardUnknown: true,
			},
		}),
	)

	// activate file-to-embeddings worker pool
	wp := worker.NewFileToEmbWorkerPool(ctx, service, config.Config.FileToEmbeddingWorker.NumberOfWorkers)
	wp.Start()

	// Start usage reporter
	var usg usage.Usage
	if config.Config.Server.Usage.Enabled {
		usageServiceClient, usageServiceClientConn := grpcClient.NewUsageClient(ctx)
		if usageServiceClientConn != nil {
			defer usageServiceClientConn.Close()
			logger.Info("try to start usage reporter")
			go utils.GoRecover(func() {
				for {
					usg = usage.NewUsage(ctx, mgmtPrivateServiceClient, redisClient, usageServiceClient)
					if usg != nil {
						usg.StartReporter(ctx)
						logger.Info("usage reporter started")
						break
					}
					logger.Warn("retry to start usage reporter after 5 minutes")
					time.Sleep(5 * time.Minute)
				}
			}, "Usage Reporter")
		}
	}

	// Start gRPC server
	var dialOpts []grpc.DialOption
	if config.Config.Server.HTTPS.Cert != "" && config.Config.Server.HTTPS.Key != "" {
		dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(creds), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(constant.MaxPayloadSize), grpc.MaxCallSendMsgSize(constant.MaxPayloadSize))}
	} else {
		dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(constant.MaxPayloadSize), grpc.MaxCallSendMsgSize(constant.MaxPayloadSize))}
	}

	if err := artifactPB.RegisterArtifactPublicServiceHandlerFromEndpoint(ctx, publicServeMux, fmt.Sprintf(":%v", config.Config.Server.PublicPort), dialOpts); err != nil {
		logger.Fatal(err.Error())
	}
	var tlsConfig *tls.Config
	if config.Config.Server.HTTPS.Cert != "" && config.Config.Server.HTTPS.Key != "" {
		tlsConfig = &tls.Config{
			ClientAuth: tls.RequireAndVerifyClientCert,
		}
	}

	publicHTTPServer := &http.Server{
		Addr:      fmt.Sprintf(":%v", config.Config.Server.PublicPort),
		Handler:   grpcHandlerFunc(publicGrpcS, publicServeMux),
		TLSConfig: tlsConfig,
	}

	privatePort := fmt.Sprintf(":%d", config.Config.Server.PrivatePort)
	// Wait for interrupt signal to gracefully shutdown the server with a timeout of 5 seconds.
	errSig := make(chan error)

	go func() {
		privateListener, err := net.Listen("tcp", privatePort)
		if err != nil {
			errSig <- fmt.Errorf("failed to listen: %w", err)
		}
		if err := privateGrpcS.Serve(privateListener); err != nil {
			errSig <- fmt.Errorf("failed to serve: %w", err)
		}
	}()

	go func() {
		var err error
		switch {
		case config.Config.Server.HTTPS.Cert != "" && config.Config.Server.HTTPS.Key != "":
			err = publicHTTPServer.ListenAndServeTLS(config.Config.Server.HTTPS.Cert, config.Config.Server.HTTPS.Key)
		default:
			err = publicHTTPServer.ListenAndServe()
		}
		if err != nil {
			errSig <- err
		}
	}()

	span.End()
	logger.Info("gRPC server is running.")

	// kill (no param) default send syscall.SIGTERM
	// kill -2 is syscall.SIGINT
	// kill -9 is syscall.SIGKILL but can't be catch, so don't need add it
	quitSig := make(chan os.Signal, 1)
	signal.Notify(quitSig, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errSig:
		logger.Error(fmt.Sprintf("Fatal error: %v\n", err))
	case <-quitSig:
		// if config.Config.Server.Usage.Enabled && usg != nil {
		// 	usg.TriggerSingleReporter(ctx)
		// }
		logger.Info("Shutting down server...")
		publicGrpcS.GracefulStop()
		wp.GraceFulStop()
		logger.Info("server shutdown 1")
	}
	fmt.Println("server shutdown 2")
}

func newClients(ctx context.Context, logger *zap.Logger) (
	pipelinePB.PipelinePublicServiceClient,
	*grpc.ClientConn,
	mgmtPB.MgmtPublicServiceClient,
	*grpc.ClientConn,
	mgmtPB.MgmtPrivateServiceClient,
	*grpc.ClientConn,
	*redis.Client,
	influxdb2.Client,
	*gorm.DB,
	*minio.Minio,
	milvus.MilvusClientI,
	acl.ACLClient,
	*grpc.ClientConn,
	*grpc.ClientConn,
) {

	// init pipeline grpc client
	pipelinePublicGrpcConn, err := grpcClient.NewGRPCConn(
		fmt.Sprintf("%v:%v", config.Config.PipelineBackend.Host,
			config.Config.PipelineBackend.PublicPort),
		config.Config.PipelineBackend.HTTPS.Cert,
		config.Config.PipelineBackend.HTTPS.Key)
	if err != nil {
		logger.Fatal(fmt.Sprintf("failed to create pipeline public grpc client: %v", err))
	}
	pipelinePublicServiceClient := pipelinePB.NewPipelinePublicServiceClient(pipelinePublicGrpcConn)

	// initialize mgmt clients
	mgmtPrivateServiceClient, mgmtPrivateServiceClientConn := grpcClient.NewMGMTPrivateClient(ctx)
	mgmtPublicServiceClient, mgmtPublicServiceClientConn := grpcClient.NewMGMTPublicClient(ctx)

	// Initialize redis client
	redisClient := redis.NewClient(&config.Config.Cache.Redis.RedisOptions)

	// Initialize InfluxDB client
	influxDBClient, influxDBWriteClient := httpClient.NewInfluxDBClient(ctx)

	influxErrCh := influxDBWriteClient.Errors()
	go utils.GoRecover(func() {
		for err := range influxErrCh {
			logger.Error(fmt.Sprintf("write to bucket %s error: %s\n", config.Config.InfluxDB.Bucket, err.Error()))
		}
	}, "InfluxDB")

	// Initialize repository
	db := database.GetSharedConnection()

	// Initialize Minio client
	minioClient, err := minio.NewMinioClientAndInitBucket(config.Config.Minio)
	if err != nil {
		logger.Fatal(fmt.Sprintf("failed to create minio client: %v", err))
	}

	// Initialize milvus client
	milvusClient, err := milvus.NewMilvusClient(ctx, config.Config.Milvus.Host, config.Config.Milvus.Port)
	if err != nil {
		logger.Fatal(fmt.Sprintf("failed to create milvus client: %v", err))
	}

	// Init ACL client
	fgaClient, fgaClientConn := acl.InitOpenFGAClient(ctx, config.Config.OpenFGA.Host, config.Config.OpenFGA.Port)

	var fgaReplicaClient openfga.OpenFGAServiceClient
	var fgaReplicaClientConn *grpc.ClientConn
	if config.Config.OpenFGA.Replica.Host != "" {

		fgaReplicaClient, fgaReplicaClientConn = acl.InitOpenFGAClient(ctx, config.Config.OpenFGA.Replica.Host, config.Config.OpenFGA.Replica.Port)

	}
	aclClient := acl.NewACLClient(fgaClient, fgaReplicaClient, redisClient)
	return pipelinePublicServiceClient, pipelinePublicGrpcConn, mgmtPublicServiceClient, mgmtPrivateServiceClientConn, mgmtPrivateServiceClient, mgmtPublicServiceClientConn, redisClient, influxDBClient, db, minioClient, milvusClient, aclClient, fgaClientConn, fgaReplicaClientConn
}

func newGrpcOptionAndCreds(logger *zap.Logger) ([]grpc.ServerOption, credentials.TransportCredentials) {
	// Shared options for the logger, with a custom gRPC code to log level function.
	opts := []grpcZap.Option{
		grpcZap.WithDecider(func(fullMethodName string, err error) bool {
			// will not log gRPC calls if it was a call to liveness or readiness and no error was raised
			if err == nil {
				if match, _ := regexp.MatchString("artifact.artifact.v1alpha.ArtifactPublicService/.*ness$", fullMethodName); match {
					return false
				}
				// stop logging successful private function calls
				if match, _ := regexp.MatchString("artifact.artifact.v1alpha.ArtifactPrivateService/.*$", fullMethodName); match {
					return false
				}
			}
			// by default everything will be logged
			return true
		}),
	}
	grpcServerOpts := []grpc.ServerOption{
		grpc.StreamInterceptor(grpcMiddleware.ChainStreamServer(
			middleware.StreamAppendMetadataInterceptor,
			grpcZap.StreamServerInterceptor(logger, opts...),
			grpcRecovery.StreamServerInterceptor(middleware.RecoveryInterceptorOpt()),
		)),
		grpc.UnaryInterceptor(grpcMiddleware.ChainUnaryServer(
			middleware.UnaryAppendMetadataAndErrorCodeInterceptor,
			grpcZap.UnaryServerInterceptor(logger, opts...),
			grpcRecovery.UnaryServerInterceptor(middleware.RecoveryInterceptorOpt()),
		)),
	}

	// Create tls based credential.
	var creds credentials.TransportCredentials
	var err error
	if config.Config.Server.HTTPS.Cert != "" && config.Config.Server.HTTPS.Key != "" {
		creds, err = credentials.NewServerTLSFromFile(config.Config.Server.HTTPS.Cert, config.Config.Server.HTTPS.Key)
		if err != nil {
			logger.Fatal(fmt.Sprintf("failed to create credentials: %v", err))
		}
		grpcServerOpts = append(grpcServerOpts, grpc.Creds(creds))
	}

	grpcServerOpts = append(grpcServerOpts, grpc.MaxRecvMsgSize(constant.MaxPayloadSize))
	grpcServerOpts = append(grpcServerOpts, grpc.MaxSendMsgSize(constant.MaxPayloadSize))
	return grpcServerOpts, creds
}
