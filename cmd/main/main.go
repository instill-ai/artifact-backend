package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/encoding/protojson"
	"gorm.io/gorm"

	grpczap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	openfga "github.com/openfga/api/proto/openfga/v1"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/acl"
	"github.com/instill-ai/artifact-backend/pkg/handler"
	"github.com/instill-ai/artifact-backend/pkg/milvus"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"
	"github.com/instill-ai/artifact-backend/pkg/usage"
	"github.com/instill-ai/artifact-backend/pkg/utils"
	"github.com/instill-ai/artifact-backend/pkg/worker"

	httpclient "github.com/instill-ai/artifact-backend/pkg/client/http"
	database "github.com/instill-ai/artifact-backend/pkg/db"
	minio "github.com/instill-ai/artifact-backend/pkg/minio"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	mgmtpb "github.com/instill-ai/protogen-go/core/mgmt/v1beta"
	usagepb "github.com/instill-ai/protogen-go/core/usage/v1beta"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
	clientx "github.com/instill-ai/x/client"
	clientgrpcx "github.com/instill-ai/x/client/grpc"
	logx "github.com/instill-ai/x/log"
	miniox "github.com/instill-ai/x/minio"
	otelx "github.com/instill-ai/x/otel"
	servergrpcx "github.com/instill-ai/x/server/grpc"
	gatewayx "github.com/instill-ai/x/server/grpc/gateway"
)

var (
	// These variables might be overridden at buildtime.
	serviceName    = "artifact-backend"
	serviceVersion = "dev"
)

// grpcHandlerFunc handles incoming HTTP requests and routes them to either the gRPC server or the gateway handler.
// It wraps the handler function with h2c.NewHandler to support HTTP/2 requests.
// The function extracts the B3 context from the incoming request headers and sets it in the request context.
// If the request is a gRPC request, it calls the gRPC server's ServeHTTP method.
// Otherwise, it calls the gateway handler's ServeHTTP method.
func grpcHandlerFunc(grpcServer *grpc.Server, gwHandler http.Handler) http.Handler {
	return h2c.NewHandler(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	if err := config.Init(config.ParseConfigFlag()); err != nil {
		log.Fatal(err.Error())
	}

	// setup tracing and metrics
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cleanup := otelx.SetupWithCleanup(ctx,
		otelx.WithServiceName(serviceName),
		otelx.WithServiceVersion(serviceVersion),
		otelx.WithHost(config.Config.OTELCollector.Host),
		otelx.WithPort(config.Config.OTELCollector.Port),
		otelx.WithCollectorEnable(config.Config.OTELCollector.Enable),
	)
	defer cleanup()

	logx.Debug = config.Config.Server.Debug
	logger, _ := logx.GetZapLogger(ctx)
	defer func() {
		// can't handle the error due to https://github.com/uber-go/zap/issues/880
		_ = logger.Sync()
	}()

	// Set gRPC logging based on debug mode
	if config.Config.Server.Debug {
		grpczap.ReplaceGrpcLoggerV2WithVerbosity(logger, 0) // All logs
	} else {
		grpczap.ReplaceGrpcLoggerV2WithVerbosity(logger, 3) // verbosity 3 will avoid [transport] from emitting
	}

	// Get gRPC server options and credentials
	grpcServerOpts, err := servergrpcx.NewServerOptionsAndCreds(
		servergrpcx.WithServiceName(serviceName),
		servergrpcx.WithServiceVersion(serviceVersion),
		servergrpcx.WithServiceConfig(clientx.HTTPSConfig{
			Cert: config.Config.Server.HTTPS.Cert,
			Key:  config.Config.Server.HTTPS.Key,
		}),
		servergrpcx.WithSetOTELServerHandler(config.Config.OTELCollector.Enable),
	)
	if err != nil {
		logger.Fatal("failed to create gRPC server options and credentials", zap.Error(err))
	}

	privateGrpcS := grpc.NewServer(grpcServerOpts...)
	reflection.Register(privateGrpcS)

	publicGrpcS := grpc.NewServer(grpcServerOpts...)
	reflection.Register(publicGrpcS)

	// Initialize clients needed for service
	pipelinePublicServiceClient, mgmtPrivateServiceClient,
		redisClient, db, minioClient, vectorDB, aclClient, closer := newClients(ctx, logger)
	defer closer()

	// Initialize service
	svc := service.NewService(
		repository.NewRepository(db),
		minioClient,
		mgmtPrivateServiceClient,
		pipelinePublicServiceClient,
		httpclient.NewRegistryClient(ctx),
		redisClient,
		vectorDB,
		aclClient,
	)

	privateHandler := handler.NewPrivateHandler(svc, logger)
	artifactpb.RegisterArtifactPrivateServiceServer(
		privateGrpcS,
		privateHandler)

	artifactpb.RegisterArtifactPublicServiceServer(
		publicGrpcS,
		handler.NewPublicHandler(svc, logger))

	privateServeMux := runtime.NewServeMux(
		runtime.WithForwardResponseOption(gatewayx.HTTPResponseModifier),
		runtime.WithErrorHandler(gatewayx.ErrorHandler),
		runtime.WithIncomingHeaderMatcher(gatewayx.CustomHeaderMatcher),
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

	publicServeMux := runtime.NewServeMux(
		runtime.WithForwardResponseOption(gatewayx.HTTPResponseModifier),
		runtime.WithErrorHandler(gatewayx.ErrorHandler),
		runtime.WithIncomingHeaderMatcher(gatewayx.CustomHeaderMatcher),
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
	wp := worker.NewPersistentCatalogFileToEmbWorkerPool(ctx, svc, config.Config.FileToEmbeddingWorker.NumberOfWorkers, artifactpb.CatalogType_CATALOG_TYPE_PERSISTENT)
	wp.Start()

	// Start usage reporter
	var usg usage.Usage
	if config.Config.Server.Usage.Enabled {
		usageServiceClient, usageServiceClientClose, err := clientgrpcx.NewClient[usagepb.UsageServiceClient](
			clientgrpcx.WithServiceConfig(clientx.ServiceConfig{
				Host:       config.Config.Server.Usage.Host,
				PublicPort: config.Config.Server.Usage.Port,
			}),
			clientgrpcx.WithSetOTELClientHandler(config.Config.OTELCollector.Enable),
		)
		if err != nil {
			logger.Error("failed to create usage service client", zap.Error(err))
		}
		defer func() {
			if err := usageServiceClientClose(); err != nil {
				logger.Error("failed to close usage service client", zap.Error(err))
			}
		}()
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

	dialOpts, err := clientgrpcx.NewClientOptionsAndCreds(
		clientgrpcx.WithServiceConfig(clientx.ServiceConfig{
			HTTPS: clientx.HTTPSConfig{
				Cert: config.Config.Server.HTTPS.Cert,
				Key:  config.Config.Server.HTTPS.Key,
			},
		}),
		clientgrpcx.WithSetOTELClientHandler(false),
	)
	if err != nil {
		logger.Fatal("failed to create client options and credentials", zap.Error(err))
	}

	if err := artifactpb.RegisterArtifactPrivateServiceHandlerFromEndpoint(ctx, privateServeMux, fmt.Sprintf(":%v", config.Config.Server.PrivatePort), dialOpts); err != nil {
		logger.Fatal(err.Error())
	}

	if err := artifactpb.RegisterArtifactPublicServiceHandlerFromEndpoint(ctx, publicServeMux, fmt.Sprintf(":%v", config.Config.Server.PublicPort), dialOpts); err != nil {
		logger.Fatal(err.Error())
	}

	privateHTTPServer := &http.Server{
		Addr:    fmt.Sprintf(":%v", config.Config.Server.PrivatePort),
		Handler: grpcHandlerFunc(privateGrpcS, privateServeMux),
	}

	publicHTTPServer := &http.Server{
		Addr:    fmt.Sprintf(":%v", config.Config.Server.PublicPort),
		Handler: grpcHandlerFunc(publicGrpcS, publicServeMux),
	}

	if err := privateServeMux.HandlePath("POST", "/v1alpha/minio-audit", privateHandler.IngestMinIOAuditLogs); err != nil {
		logger.Fatal("Failed to set up MinIO audit endpoint", zap.Error(err))
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
		if config.Config.Server.Usage.Enabled && usg != nil {
			usg.TriggerSingleReporter(ctx)
		}
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
	service.VectorDatabase,
	*acl.ACLClient,
	func(),
) {
	closeFuncs := map[string]func() error{}

	// init pipeline grpc client
	// Initialize mgmt private service client
	pipelinePublicServiceClient, pipelinePublicClose, err := clientgrpcx.NewClient[pipelinepb.PipelinePublicServiceClient](
		clientgrpcx.WithServiceConfig(clientx.ServiceConfig{
			Host:       config.Config.PipelineBackend.Host,
			PublicPort: config.Config.PipelineBackend.PublicPort,
		}),
		clientgrpcx.WithSetOTELClientHandler(config.Config.OTELCollector.Enable),
	)
	if err != nil {
		logger.Fatal("failed to create pipeline public service client", zap.Error(err))
	}
	closeFuncs["pipelinePublic"] = pipelinePublicClose

	// initialize mgmt clients
	mgmtPrivateServiceClient, mclose, err := clientgrpcx.NewClient[mgmtpb.MgmtPrivateServiceClient](
		clientgrpcx.WithServiceConfig(config.Config.MgmtBackend),
		clientgrpcx.WithSetOTELClientHandler(config.Config.OTELCollector.Enable),
	)
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
	vectorDB, vclose, err := milvus.NewVectorDatabase(ctx, config.Config.Milvus.Host, config.Config.Milvus.Port)
	if err != nil {
		logger.Fatal(fmt.Sprintf("failed to create milvus client: %v", err))
	}
	closeFuncs["milvus"] = vclose

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

	return pipelinePublicServiceClient, mgmtPrivateServiceClient, redisClient, db, minioClient, vectorDB, aclClient, closer
}
