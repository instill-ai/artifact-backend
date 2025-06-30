package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
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

	grpcmiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpczap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpcrecovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	openfga "github.com/openfga/api/proto/openfga/v1"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/acl"
	"github.com/instill-ai/artifact-backend/pkg/handler"
	"github.com/instill-ai/artifact-backend/pkg/middleware"
	"github.com/instill-ai/artifact-backend/pkg/milvus"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/usage"
	"github.com/instill-ai/artifact-backend/pkg/utils"
	"github.com/instill-ai/artifact-backend/pkg/worker"
	"github.com/instill-ai/x/client"

	grpcclient "github.com/instill-ai/artifact-backend/pkg/client/grpc"
	httpclient "github.com/instill-ai/artifact-backend/pkg/client/http"
	database "github.com/instill-ai/artifact-backend/pkg/db"
	customotel "github.com/instill-ai/artifact-backend/pkg/logger/otel"
	minio "github.com/instill-ai/artifact-backend/pkg/minio"
	servicepkg "github.com/instill-ai/artifact-backend/pkg/service"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	mgmtpb "github.com/instill-ai/protogen-go/core/mgmt/v1beta"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
	grpcclientx "github.com/instill-ai/x/client/grpc"
	logx "github.com/instill-ai/x/log"
	miniox "github.com/instill-ai/x/minio"
)

var (
	// These variables might be overridden at buildtime.
	serviceVersion = "dev"
	serviceName    = "artifact-backend"

	propagator propagation.TextMapPropagator
)

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

	tp, err := customotel.SetupTracing(ctx, "artifact-backend")
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

	logx.Debug = config.Config.Server.Debug
	logger, _ := logx.GetZapLogger(ctx)
	defer func() {
		// can't handle the error due to https://github.com/uber-go/zap/issues/880
		_ = logger.Sync()
	}()

	// verbosity 3 will avoid [transport] from emitting
	grpczap.ReplaceGrpcLoggerV2WithVerbosity(logger, 3)

	// Initialize clients needed for service
	pipelinePublicServiceClient, mgmtPrivateServiceClient,
		redisClient, db, minioClient, milvusClient, aclClient, closer := newClients(ctx, logger)
	defer closer()

	// Initialize service
	service := servicepkg.NewService(
		repository.NewRepository(db),
		minioClient,
		mgmtPrivateServiceClient,
		pipelinePublicServiceClient,
		httpclient.NewRegistryClient(ctx),
		redisClient,
		milvusClient,
		aclClient)

	grpcServerOpts, creds := newGrpcOptionAndCreds(logger)

	publicGrpcS := grpc.NewServer(grpcServerOpts...)
	reflection.Register(publicGrpcS)
	publicHandler := handler.NewPublicHandler(service, logger)
	artifactpb.RegisterArtifactPublicServiceServer(publicGrpcS, publicHandler)

	privateGrpcS := grpc.NewServer(grpcServerOpts...)
	reflection.Register(privateGrpcS)

	privateHandler := handler.NewPrivateHandler(service, logger)
	artifactpb.RegisterArtifactPrivateServiceServer(privateGrpcS, privateHandler)

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

	privateServeMux := runtime.NewServeMux(
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

	// activate persistent catalog file-to-embeddings worker pool
	wp := worker.NewPersistentCatalogFileToEmbWorkerPool(ctx, service, config.Config.FileToEmbeddingWorker.NumberOfWorkers, artifactpb.CatalogType_CATALOG_TYPE_PERSISTENT)
	wp.Start()

	// Start usage reporter
	var usg usage.Usage
	if config.Config.Server.Usage.Enabled {
		usageServiceClient, usageServiceClientConn := grpcclient.NewUsageClient(ctx)
		if usageServiceClientConn != nil {
			defer usageServiceClientConn.Close()
			logger.Info("try to start usage reporter")
			go utils.GoRecover(func() {
				for {
					usg = usage.NewUsage(ctx, mgmtPrivateServiceClient, redisClient, usageServiceClient, serviceVersion)
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
		dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(creds), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(client.MaxPayloadSize), grpc.MaxCallSendMsgSize(client.MaxPayloadSize))}
	} else {
		dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(client.MaxPayloadSize), grpc.MaxCallSendMsgSize(client.MaxPayloadSize))}
	}

	if err := artifactpb.RegisterArtifactPublicServiceHandlerFromEndpoint(ctx, publicServeMux, fmt.Sprintf(":%v", config.Config.Server.PublicPort), dialOpts); err != nil {
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

	if err := artifactpb.RegisterArtifactPrivateServiceHandlerFromEndpoint(ctx, privateServeMux, fmt.Sprintf(":%v", config.Config.Server.PrivatePort), dialOpts); err != nil {
		logger.Fatal(err.Error())
	}

	if err := privateServeMux.HandlePath("POST", "/v1alpha/minio-audit", privateHandler.IngestMinIOAuditLogs); err != nil {
		logger.Fatal("Failed to set up MinIO audit endpoint", zap.Error(err))
	}

	privateHTTPServer := &http.Server{
		Addr:      fmt.Sprintf(":%v", config.Config.Server.PrivatePort),
		Handler:   grpcHandlerFunc(privateGrpcS, privateServeMux),
		TLSConfig: tlsConfig,
	}

	// Wait for interrupt signal to gracefully shutdown the server with a timeout of 5 seconds.
	errSig := make(chan error)
	defer close(errSig)

	if config.Config.Server.HTTPS.Cert != "" && config.Config.Server.HTTPS.Key != "" {
		go func() {
			if err := privateHTTPServer.ListenAndServeTLS(config.Config.Server.HTTPS.Cert, config.Config.Server.HTTPS.Key); err != nil {
				errSig <- err
			}
		}()
		go func() {
			if err := publicHTTPServer.ListenAndServeTLS(config.Config.Server.HTTPS.Cert, config.Config.Server.HTTPS.Key); err != nil {
				errSig <- err
			}
		}()
	} else {
		go func() {
			if err := privateHTTPServer.ListenAndServe(); err != nil {
				errSig <- err
			}
		}()
		go func() {
			if err := publicHTTPServer.ListenAndServe(); err != nil {
				errSig <- err
			}
		}()
	}

	span.End()
	logger.Info("gRPC server is running.")

	// kill (no param) default send syscall.SIGTERM
	// kill -2 is syscall.SIGINT
	// kill -9 is syscall.SIGKILL but can't be catch, so don't need add it
	quitSig := make(chan os.Signal, 1)
	defer close(quitSig)
	signal.Notify(quitSig, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errSig:
		logger.Error(fmt.Sprintf("Fatal error: %v\n", err))
		os.Exit(1)
	case <-quitSig:
		// if config.Config.Server.Usage.Enabled && usg != nil {
		// 	usg.TriggerSingleReporter(ctx)
		// }
		logger.Info("Shutting down server...")
		publicGrpcS.GracefulStop()
		wp.GraceFulStop()
		logger.Info("server shutdown due to signal")
		os.Exit(0)
	}
}

func newClients(ctx context.Context, logger *zap.Logger) (
	pipelinepb.PipelinePublicServiceClient,
	mgmtpb.MgmtPrivateServiceClient,
	*redis.Client,
	*gorm.DB,
	*minio.Minio,
	milvus.MilvusClientI,
	acl.ACLClient,
	func(),
) {
	closeFuncs := map[string]func() error{}

	// init pipeline grpc client
	pipelinePublicServiceClient, pclose, err := grpcclientx.NewPipelinePublicClient(config.Config.PipelineBackend)
	if err != nil {
		logger.Fatal("Failed to initialize pipeline client", zap.Error(err))
	}
	closeFuncs["pipeline"] = pclose

	// initialize mgmt clients
	mgmtPrivateServiceClient, mclose, err := grpcclientx.NewMGMTPrivateClient(config.Config.MgmtBackend)
	if err != nil {
		logger.Fatal("Failed to initialize mgmt client", zap.Error(err))
	}
	closeFuncs["mgmt"] = mclose

	// Initialize redis client
	redisClient := redis.NewClient(&config.Config.Cache.Redis.RedisOptions)
	closeFuncs["redis"] = redisClient.Close

	// Initialize repository
	db := database.GetSharedConnection()
	closeFuncs["database"] = func() error {
		database.Close(db)
		return nil
	}

	// Initialize Minio client
	minioClient, err := minio.NewMinioClientAndInitBucket(ctx, miniox.ClientParams{
		Config: config.Config.Minio,
		Logger: logger,
		AppInfo: miniox.AppInfo{
			Name:    serviceName,
			Version: serviceVersion,
		},
	})
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
	closeFuncs["fga"] = fgaClientConn.Close

	var fgaReplicaClient openfga.OpenFGAServiceClient
	if config.Config.OpenFGA.Replica.Host != "" {
		var fgaReplicaClientConn *grpc.ClientConn
		fgaReplicaClient, fgaReplicaClientConn = acl.InitOpenFGAClient(ctx, config.Config.OpenFGA.Replica.Host, config.Config.OpenFGA.Replica.Port)
		closeFuncs["fgaReplica"] = fgaReplicaClientConn.Close
	}

	aclClient := acl.NewACLClient(fgaClient, fgaReplicaClient, redisClient)

	closer := func() {
		for conn, fn := range closeFuncs {
			if err := fn(); err != nil {
				logger.Error("Failed to close conn", zap.Error(err), zap.String("conn", conn))
			}
		}
	}

	return pipelinePublicServiceClient, mgmtPrivateServiceClient, redisClient, db, minioClient, milvusClient, aclClient, closer
}

func newGrpcOptionAndCreds(logger *zap.Logger) ([]grpc.ServerOption, credentials.TransportCredentials) {
	// Shared options for the logger, with a custom gRPC code to log level function.
	opts := []grpczap.Option{
		grpczap.WithDecider(func(fullMethodName string, err error) bool {
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
		grpc.StreamInterceptor(grpcmiddleware.ChainStreamServer(
			middleware.StreamAppendMetadataInterceptor,
			grpczap.StreamServerInterceptor(logger, opts...),
			grpcrecovery.StreamServerInterceptor(middleware.RecoveryInterceptorOpt()),
		)),
		grpc.UnaryInterceptor(grpcmiddleware.ChainUnaryServer(
			middleware.UnaryAppendMetadataAndErrorCodeInterceptor,
			grpczap.UnaryServerInterceptor(logger, opts...),
			grpcrecovery.UnaryServerInterceptor(middleware.RecoveryInterceptorOpt()),
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

	grpcServerOpts = append(grpcServerOpts, grpc.MaxRecvMsgSize(client.MaxPayloadSize))
	grpcServerOpts = append(grpcServerOpts, grpc.MaxSendMsgSize(client.MaxPayloadSize))
	return grpcServerOpts, creds
}
