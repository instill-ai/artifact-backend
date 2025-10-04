package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"gorm.io/gorm"

	grpczap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	openfga "github.com/openfga/api/proto/openfga/v1"
	temporalclient "go.temporal.io/sdk/client"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/acl"
	"github.com/instill-ai/artifact-backend/pkg/milvus"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"
	"github.com/instill-ai/x/client"
	"github.com/instill-ai/x/temporal"

	database "github.com/instill-ai/artifact-backend/pkg/db"
	artifactminio "github.com/instill-ai/artifact-backend/pkg/minio"
	artifactworker "github.com/instill-ai/artifact-backend/pkg/worker"
	mgmtpb "github.com/instill-ai/protogen-go/core/mgmt/v1beta"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
	clientgrpcx "github.com/instill-ai/x/client/grpc"
	logx "github.com/instill-ai/x/log"
	miniox "github.com/instill-ai/x/minio"
	otelx "github.com/instill-ai/x/otel"
)

const gracefulShutdownWaitPeriod = 15 * time.Second
const gracefulShutdownTimeout = 60 * time.Minute

var (
	// These variables might be overridden at buildtime.
	serviceName    = "artifact-backend-worker"
	serviceVersion = "dev"
)

func main() {
	if err := config.Init(config.ParseConfigFlag()); err != nil {
		log.Fatal(err.Error())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup all OpenTelemetry components
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

	// Initialize all clients
	pipelinePublicServiceClient, mgmtPrivateServiceClient,
		redisClient, db, minioClient, vectorDB, aclClient, temporalClient, closeClients := newClients(ctx, logger)
	defer closeClients()

	// Create worker instance first (without service dependencies for now)
	// We'll set up the service layer after creating workflow wrappers
	cw, err := artifactworker.New(
		artifactworker.Config{
			Service: nil, // Will be set after service is created
		},
		logger,
	)
	if err != nil {
		logger.Fatal("Unable to create worker", zap.Error(err))
	}

	// Initialize workflow implementations with the worker instance
	processFileWf := artifactworker.NewProcessFileWorkflow(temporalClient, cw)
	cleanupFileWf := artifactworker.NewCleanupFileWorkflow(temporalClient, cw)
	cleanupKBWf := artifactworker.NewCleanupKnowledgeBaseWorkflow(temporalClient, cw)
	embedTextsWf := artifactworker.NewEmbedTextsWorkflow(temporalClient, cw)
	deleteFilesWf := artifactworker.NewDeleteFilesWorkflow(temporalClient, cw)
	getFilesWf := artifactworker.NewGetFilesWorkflow(temporalClient, cw)

	// Initialize repository
	repo := repository.NewRepository(db)

	// Create service with workflow implementations
	svc := service.NewService(
		repo,
		minioClient,
		mgmtPrivateServiceClient,
		pipelinePublicServiceClient,
		nil, // registry client - not needed for worker
		redisClient,
		vectorDB,
		aclClient,
		processFileWf,
		cleanupFileWf,
		cleanupKBWf,
		embedTextsWf,
		deleteFilesWf,
		getFilesWf,
	)

	// Update the worker instance with the service
	// (the workflow wrappers already have a reference to this worker)
	cw.SetService(svc)

	w := worker.New(temporalClient, artifactworker.TaskQueue, worker.Options{
		EnableSessionWorker:                    true,
		WorkflowPanicPolicy:                    worker.BlockWorkflow,
		WorkerStopTimeout:                      gracefulShutdownTimeout,
		MaxConcurrentWorkflowTaskExecutionSize: 100,
		Interceptors: func() []interceptor.WorkerInterceptor {
			if !config.Config.OTELCollector.Enable {
				return nil
			}
			workerInterceptor, err := opentelemetry.NewTracingInterceptor(opentelemetry.TracerOptions{
				Tracer:            otel.Tracer(serviceName),
				TextMapPropagator: otel.GetTextMapPropagator(),
			})
			if err != nil {
				logger.Fatal("Unable to create worker tracing interceptor", zap.Error(err))
			}
			return []interceptor.WorkerInterceptor{workerInterceptor}
		}(),
	})

	// Register workflows
	w.RegisterWorkflow(cw.ProcessFileWorkflow)
	w.RegisterWorkflow(cw.CleanupFileWorkflow)
	w.RegisterWorkflow(cw.CleanupKnowledgeBaseWorkflow)
	w.RegisterWorkflow(cw.EmbedTextsWorkflow)
	w.RegisterWorkflow(cw.SaveChunksWorkflow)
	w.RegisterWorkflow(cw.DeleteFilesWorkflow)
	w.RegisterWorkflow(cw.GetFilesWorkflow)

	// Register batch embedding activity
	w.RegisterActivity(cw.EmbedTextsActivity)

	// Register MinIO activities
	w.RegisterActivity(cw.SaveChunkActivity)
	w.RegisterActivity(cw.DeleteFileActivity)
	w.RegisterActivity(cw.GetFileActivity)
	w.RegisterActivity(cw.UpdateChunkDestinationsActivity)

	// Register utility activities
	w.RegisterActivity(cw.GetFileStatusActivity)
	w.RegisterActivity(cw.UpdateFileStatusActivity)

	// Register cleanup activities - File cleanup
	w.RegisterActivity(cw.DeleteOriginalFileActivity)
	w.RegisterActivity(cw.DeleteConvertedFileActivity)
	w.RegisterActivity(cw.DeleteChunksFromMinIOActivity)
	w.RegisterActivity(cw.DeleteEmbeddingsFromVectorDBActivity)

	// Register cleanup activities - Knowledge base cleanup
	w.RegisterActivity(cw.DeleteKBFilesFromMinIOActivity)
	w.RegisterActivity(cw.DropVectorDBCollectionActivity)
	w.RegisterActivity(cw.DeleteKBFileRecordsActivity)
	w.RegisterActivity(cw.DeleteKBConvertedFileRecordsActivity)
	w.RegisterActivity(cw.DeleteKBChunkRecordsActivity)
	w.RegisterActivity(cw.DeleteKBEmbeddingRecordsActivity)
	w.RegisterActivity(cw.PurgeKBACLActivity)

	// Register process file activities
	// Conversion Phase
	w.RegisterActivity(cw.GetFileMetadataActivity)
	w.RegisterActivity(cw.GetFileContentActivity)
	w.RegisterActivity(cw.ConvertToMarkdownActivity)
	w.RegisterActivity(cw.CleanupOldConvertedFileActivity)
	w.RegisterActivity(cw.CreateConvertedFileRecordActivity)
	w.RegisterActivity(cw.UploadConvertedFileToMinIOActivity)
	w.RegisterActivity(cw.UpdateConvertedFileDestinationActivity)
	w.RegisterActivity(cw.DeleteConvertedFileRecordActivity)
	w.RegisterActivity(cw.DeleteConvertedFileFromMinIOActivity)
	w.RegisterActivity(cw.UpdateConversionMetadataActivity)
	// Chunking Phase
	w.RegisterActivity(cw.GetConvertedFileForChunkingActivity)
	w.RegisterActivity(cw.ChunkContentActivity)
	w.RegisterActivity(cw.SaveChunksToDBActivity)
	w.RegisterActivity(cw.UpdateChunkingMetadataActivity)
	// Embedding Phase
	w.RegisterActivity(cw.GetChunksForEmbeddingActivity)
	w.RegisterActivity(cw.GenerateEmbeddingsActivity)
	w.RegisterActivity(cw.SaveEmbeddingsToVectorDBActivity)
	w.RegisterActivity(cw.UpdateEmbeddingMetadataActivity)
	// Summary Phase
	w.RegisterActivity(cw.GetFileContentForSummaryActivity)
	w.RegisterActivity(cw.GenerateSummaryFromPipelineActivity)
	w.RegisterActivity(cw.SaveSummaryActivity)

	if err := w.Start(); err != nil {
		logger.Fatal(fmt.Sprintf("Unable to start worker: %s", err))
	}

	logger.Info("worker is running.")

	// Note: File processing workflows are triggered directly by the API handler
	// when ProcessCatalogFiles is called, so we don't need a dispatcher polling
	// for files. The old dispatcher pattern is not needed with Temporal.

	// kill (no param) default send syscall.SIGTERM
	// kill -2 is syscall.SIGINT
	// kill -9 is syscall.SIGKILL but can't be catch, so don't need add it
	quitSig := make(chan os.Signal, 1)
	signal.Notify(quitSig, syscall.SIGINT, syscall.SIGTERM)

	// When the server receives a SIGTERM, we'll try to finish ongoing
	// workflows. This is because file processing activities might use shared
	// resources that prevent workflows from recovering from interruptions.
	<-quitSig

	time.Sleep(gracefulShutdownWaitPeriod)

	logger.Info("Shutting down worker...")
	w.Stop()
}

func newClients(ctx context.Context, logger *zap.Logger) (
	pipelinepb.PipelinePublicServiceClient,
	mgmtpb.MgmtPrivateServiceClient,
	*redis.Client,
	*gorm.DB,
	*artifactminio.Minio,
	service.VectorDatabase,
	*acl.ACLClient,
	temporalclient.Client,
	func(),
) {
	closeFuncs := map[string]func() error{}

	// Initialize mgmt private service client
	mgmtPrivateServiceClient, mgmtPrivateClose, err := clientgrpcx.NewClient[mgmtpb.MgmtPrivateServiceClient](
		clientgrpcx.WithServiceConfig(config.Config.MgmtBackend),
		clientgrpcx.WithSetOTELClientHandler(config.Config.OTELCollector.Enable),
	)
	if err != nil {
		logger.Fatal("failed to create mgmt private service client", zap.Error(err))
	}
	closeFuncs["mgmtPrivate"] = mgmtPrivateClose

	// Initialize pipeline public service client
	pipelinePublicServiceClient, pipelinePublicClose, err := clientgrpcx.NewClient[pipelinepb.PipelinePublicServiceClient](
		clientgrpcx.WithServiceConfig(client.ServiceConfig{
			Host:       config.Config.PipelineBackend.Host,
			PublicPort: config.Config.PipelineBackend.PublicPort,
		}),
		clientgrpcx.WithSetOTELClientHandler(config.Config.OTELCollector.Enable),
	)
	if err != nil {
		logger.Fatal("failed to create pipeline public service client", zap.Error(err))
	}
	closeFuncs["pipelinePublic"] = pipelinePublicClose

	// Initialize database
	db := database.GetSharedConnection()
	closeFuncs["database"] = func() error {
		database.Close(db)
		return nil
	}

	// Initialize redis client
	redisClient := redis.NewClient(&config.Config.Cache.Redis.RedisOptions)
	closeFuncs["redis"] = redisClient.Close

	// Initialize Temporal client
	temporalClientOptions, err := temporal.ClientOptions(config.Config.Temporal, logger)
	if err != nil {
		logger.Fatal("Unable to build Temporal client options", zap.Error(err))
	}

	// Only add interceptor if tracing is enabled
	if config.Config.OTELCollector.Enable {
		temporalTracingInterceptor, err := opentelemetry.NewTracingInterceptor(opentelemetry.TracerOptions{
			Tracer:            otel.Tracer(serviceName),
			TextMapPropagator: otel.GetTextMapPropagator(),
		})
		if err != nil {
			logger.Fatal("Unable to create temporal tracing interceptor", zap.Error(err))
		}
		temporalClientOptions.Interceptors = []interceptor.ClientInterceptor{temporalTracingInterceptor}
	}

	temporalClient, err := temporalclient.Dial(temporalClientOptions)
	if err != nil {
		logger.Fatal("Unable to create Temporal client", zap.Error(err))
	}
	closeFuncs["temporal"] = func() error {
		temporalClient.Close()
		return nil
	}

	// Initialize MinIO client (artifact-backend's wrapper)
	minioClient, err := artifactminio.NewMinioClientAndInitBucket(ctx, miniox.ClientParams{
		Config: config.Config.Minio,
		Logger: logger,
		AppInfo: miniox.AppInfo{
			Name:    serviceName,
			Version: serviceVersion,
		},
	})
	if err != nil {
		logger.Fatal("failed to create MinIO client", zap.Error(err))
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

	return pipelinePublicServiceClient, mgmtPrivateServiceClient,
		redisClient, db, minioClient, vectorDB, aclClient, temporalClient, closer
}
