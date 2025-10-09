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
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/x/client"
	"github.com/instill-ai/x/temporal"

	database "github.com/instill-ai/artifact-backend/pkg/db"
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
	pipelinePublicServiceClient, _, // mgmtPrivateServiceClient not needed for worker
		redisClient, db, minioClient, vectorDB, aclClient, temporalClient, closeClients := newClients(ctx, logger)
	defer closeClients()

	// Initialize repository with vector database
	repo := repository.NewRepository(db, vectorDB, minioClient)

	// Create worker with all primitive dependencies (no circular dependency)
	cw, err := artifactworker.New(
		temporalClient,
		repo,
		pipelinePublicServiceClient,
		aclClient,
		redisClient,
		logger,
	)
	if err != nil {
		logger.Fatal("Unable to create worker", zap.Error(err))
	}

	// Register workflows and activities with Temporal worker
	// Note: Service layer is not needed here - it's only for the API handlers in cmd/main
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

	// ===== Workflow Registrations =====

	// Main workflows
	w.RegisterWorkflow(cw.ProcessFileWorkflow)          // Main file processing orchestration workflow
	w.RegisterWorkflow(cw.CleanupFileWorkflow)          // Single file cleanup and deletion workflow
	w.RegisterWorkflow(cw.CleanupKnowledgeBaseWorkflow) // Knowledge base cleanup and deletion workflow
	w.RegisterWorkflow(cw.DeleteFilesWorkflow)          // Batch file deletion workflow
	w.RegisterWorkflow(cw.GetFilesWorkflow)             // Batch file retrieval workflow

	// Child workflows (called by main workflows)
	w.RegisterWorkflow(cw.ProcessContentWorkflow)           // Parallel: Convert file to markdown
	w.RegisterWorkflow(cw.ProcessSummaryWorkflow)           // Parallel: Generate file summary
	w.RegisterWorkflow(cw.EmbedTextsWorkflow)               // Batch text embedding generation
	w.RegisterWorkflow(cw.SaveEmbeddingsToVectorDBWorkflow) // Vector embedding storage

	// ===== Shared Activities (Used by Multiple Workflows) =====

	// Embedding generation
	w.RegisterActivity(cw.EmbedTextsActivity) // Generate vector embeddings for text batches

	// MinIO operations
	w.RegisterActivity(cw.SaveTextChunkBatchActivity)          // Save text chunk batch to MinIO
	w.RegisterActivity(cw.DeleteFileActivity)                  // Delete single file from MinIO
	w.RegisterActivity(cw.GetFileActivity)                     // Retrieve single file from MinIO
	w.RegisterActivity(cw.UpdateTextChunkDestinationsActivity) // Update text chunk MinIO destinations in DB

	// File status management
	w.RegisterActivity(cw.GetFileStatusActivity)    // Retrieve current file processing status
	w.RegisterActivity(cw.UpdateFileStatusActivity) // Update file processing status

	// ===== CleanupFileWorkflow Activities =====
	// Activities for cleaning up individual file resources
	w.RegisterActivity(cw.DeleteOriginalFileActivity)           // Delete original uploaded file from MinIO
	w.RegisterActivity(cw.DeleteConvertedFileActivity)          // Delete converted markdown file from MinIO
	w.RegisterActivity(cw.DeleteTextChunksFromMinIOActivity)    // Delete all file chunks from MinIO
	w.RegisterActivity(cw.DeleteEmbeddingsFromVectorDBActivity) // Delete file embeddings from Milvus
	w.RegisterActivity(cw.DeleteEmbeddingRecordsActivity)       // Delete embedding records from database

	// ===== CleanupKnowledgeBaseWorkflow Activities =====
	// Activities for cleaning up entire knowledge base resources
	w.RegisterActivity(cw.DeleteKBFilesFromMinIOActivity)       // Delete all KB files from MinIO
	w.RegisterActivity(cw.DropVectorDBCollectionActivity)       // Drop Milvus collection for KB
	w.RegisterActivity(cw.DeleteKBFileRecordsActivity)          // Delete all file records from database
	w.RegisterActivity(cw.DeleteKBConvertedFileRecordsActivity) // Delete all converted file records
	w.RegisterActivity(cw.DeleteKBTextChunkRecordsActivity)     // Delete all chunk records
	w.RegisterActivity(cw.DeleteKBEmbeddingRecordsActivity)     // Delete all embedding records
	w.RegisterActivity(cw.PurgeKBACLActivity)                   // Remove all ACL permissions for KB

	// ===== ProcessFileWorkflow Activities (Main Workflow) =====
	// Main workflow orchestrating the entire file processing pipeline
	// Note: Actual conversion and summarization are delegated to child workflows

	// File Metadata and Setup Phase
	w.RegisterActivity(cw.GetFileMetadataActivity)                // Fetch file metadata from database
	w.RegisterActivity(cw.GetFileContentActivity)                 // Retrieve file binary content from MinIO
	w.RegisterActivity(cw.CleanupOldConvertedFileActivity)        // Remove previous conversion artifacts
	w.RegisterActivity(cw.CreateConvertedFileRecordActivity)      // Create DB record for converted file
	w.RegisterActivity(cw.UploadConvertedFileToMinIOActivity)     // Upload converted markdown to MinIO
	w.RegisterActivity(cw.UpdateConvertedFileDestinationActivity) // Update MinIO destination in DB
	w.RegisterActivity(cw.DeleteConvertedFileRecordActivity)      // Delete conversion record on failure
	w.RegisterActivity(cw.DeleteConvertedFileFromMinIOActivity)   // Delete converted file from MinIO on failure
	w.RegisterActivity(cw.UpdateConversionMetadataActivity)       // Update file status and conversion metadata

	// Chunking Phase - Combined content and summary chunking (sequential after parallel AI operations)
	w.RegisterActivity(cw.GetConvertedFileForChunkingActivity) // Retrieve converted markdown for chunking
	w.RegisterActivity(cw.UpdateChunkingMetadataActivity)      // Update file status and chunking metadata

	// Embedding Phase - Vector embedding generation and storage
	w.RegisterActivity(cw.GetChunksForEmbeddingActivity)   // Retrieve text chunks for embedding
	w.RegisterActivity(cw.UpdateEmbeddingMetadataActivity) // Update file status and embedding metadata

	// ===== ProcessContentWorkflow Activities (Child Workflow) =====
	// Child workflow for parallel markdown conversion with shared AI cache
	w.RegisterActivity(cw.ConvertFileTypeActivity)              // Convert non-AI-native formats (GIF→PNG, MKV→MP4, DOC→PDF)
	w.RegisterActivity(cw.CacheContextActivity)                 // Create AI cache for efficient processing
	w.RegisterActivity(cw.ConvertToFileActivity)                // Convert file to markdown using AI or pipelines
	w.RegisterActivity(cw.DeleteCacheActivity)                  // Clean up AI cache after processing
	w.RegisterActivity(cw.DeleteTemporaryConvertedFileActivity) // Clean up temporary converted file from MinIO (core-blob/tmp/*)

	// ===== ProcessSummaryWorkflow Activities (Child Workflow) =====
	// Child workflow for parallel summary generation with shared AI cache
	w.RegisterActivity(cw.GenerateSummaryActivity) // Generate summary using AI with cache
	w.RegisterActivity(cw.SaveSummaryActivity)     // Save summary to PostgreSQL database

	// ===== Chunking and Embedding Activities =====
	// Used in main ProcessFileWorkflow for content chunking and persistence
	w.RegisterActivity(cw.ChunkContentActivity)       // Split markdown content into semantic chunks
	w.RegisterActivity(cw.SaveTextChunksToDBActivity) // Persist chunks to database with metadata

	// ===== SaveEmbeddingsToVectorDBWorkflow Activities (Child Workflow) =====
	// Child workflow handling vector embedding storage
	w.RegisterActivity(cw.DeleteOldEmbeddingsFromVectorDBActivity) // Remove old embeddings from Milvus
	w.RegisterActivity(cw.DeleteOldEmbeddingsFromDBActivity)       // Remove old embedding records from DB
	w.RegisterActivity(cw.SaveEmbeddingBatchActivity)              // Save embedding batch to DB and vector store
	w.RegisterActivity(cw.FlushCollectionActivity)                 // Flush Milvus collection to persist data

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
	repository.ObjectStorage,
	repository.VectorDatabase,
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
	minioClient, err := repository.NewMinioObjectStorage(ctx, miniox.ClientParams{
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
	vectorDB, vclose, err := repository.NewVectorDatabase(ctx, config.Config.Milvus.Host, config.Config.Milvus.Port)
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
