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
	"github.com/instill-ai/artifact-backend/internal/ai"
	"github.com/instill-ai/artifact-backend/internal/ai/gemini"
	"github.com/instill-ai/artifact-backend/internal/ai/openai"
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

const gracefulShutdownWaitPeriod = 15 * time.Second // Wait period before stopping worker
const gracefulShutdownTimeout = 60 * time.Minute    // Maximum time for in-flight workflows to complete

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
		grpczap.ReplaceGrpcLoggerV2WithVerbosity(logger, 0) // All logs including transport layer
	} else {
		grpczap.ReplaceGrpcLoggerV2WithVerbosity(logger, 3) // Suppress transport layer logs (verbosity 3+)
	}

	// Initialize all clients (mgmtPrivateServiceClient not needed for worker - only used in cmd/main)
	pipelinePublicServiceClient, _,
		redisClient, db, minioClient, vectorDB, aclClient, temporalClient, closeClients := newClients(ctx, logger)
	defer closeClients()

	// Initialize repository with vector database and Redis
	repo := repository.NewRepository(db, vectorDB, minioClient, redisClient)

	// Initialize AI client
	aiClient, err := newAIClient(ctx, logger)
	if err != nil {
		logger.Fatal("Failed to initialize AI client", zap.Error(err))
	}

	// Create worker with direct dependencies (primitives only - no service layer to avoid circular deps)
	cw, err := artifactworker.New(
		temporalClient,
		repo,
		pipelinePublicServiceClient,
		aclClient,
		redisClient,
		logger,
		aiClient,
	)
	if err != nil {
		logger.Fatal("Unable to create worker", zap.Error(err))
	}

	// Register workflows and activities with Temporal worker
	// Note: Service layer not used here - only needed in cmd/main for API handler business logic
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
	w.RegisterWorkflow(cw.ProcessFileWorkflow)          // Main file processing orchestration workflow (batch)
	w.RegisterWorkflow(cw.CleanupFileWorkflow)          // Single file cleanup and deletion workflow
	w.RegisterWorkflow(cw.CleanupKnowledgeBaseWorkflow) // Knowledge base cleanup and deletion workflow

	// RAG update workflows
	w.RegisterWorkflow(cw.UpdateKnowledgeBaseWorkflow) // Single KB update workflow (6-phase)

	// Child workflows (called by main workflows)
	w.RegisterWorkflow(cw.SaveEmbeddingsWorkflow) // Vector embedding storage

	// ===== Shared Activities (Used by Multiple Workflows) =====

	// Embedding generation
	w.RegisterActivity(cw.EmbedTextsActivity) // Generate vector embeddings for text batches

	// MinIO operations
	w.RegisterActivity(cw.DeleteFilesBatchActivity) // Delete multiple files from MinIO (parallel)
	w.RegisterActivity(cw.GetFilesBatchActivity)    // Retrieve multiple files from MinIO (parallel)

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
	w.RegisterActivity(cw.SoftDeleteKBRecordActivity)           // Soft-delete the KB record itself
	w.RegisterActivity(cw.PurgeKBACLActivity)                   // Remove all ACL permissions for KB

	// ===== RAG update activities =====
	// Activities for knowledge base update workflow (6-phase)
	w.RegisterActivity(cw.ListKnowledgeBasesForUpdateActivity)     // Find KBs needing update
	w.RegisterActivity(cw.ValidateUpdateEligibilityActivity)       // Check if KB can be updated
	w.RegisterActivity(cw.CreateStagingKnowledgeBaseActivity)      // Create staging KB for update
	w.RegisterActivity(cw.ListFilesForReprocessingActivity)        // List files for reprocessing
	w.RegisterActivity(cw.CloneFileToStagingKBActivity)            // Clone file to staging KB
	w.RegisterActivity(cw.SynchronizeKBActivity)                   // Lock KB and wait for dual-processed files
	w.RegisterActivity(cw.ValidateUpdatedKBActivity)               // Validate data integrity after sync
	w.RegisterActivity(cw.SwapKnowledgeBasesActivity)              // Atomic pointer swap of collections
	w.RegisterActivity(cw.UpdateKnowledgeBaseUpdateStatusActivity) // Update KB update status
	w.RegisterActivity(cw.CleanupOldKnowledgeBaseActivity)         // Cleanup rollback KB after retention

	// ===== ProcessFileWorkflow Activities (Main Workflow) =====
	// Main workflow orchestrating the entire file processing pipeline
	// Content generation and summarization are handled by composite activities (ProcessContentActivity, ProcessSummaryActivity)

	// File Metadata and Setup Phase
	w.RegisterActivity(cw.GetFileMetadataActivity)                // Fetch file metadata from database
	w.RegisterActivity(cw.GetFileContentActivity)                 // Retrieve file binary content from MinIO
	w.RegisterActivity(cw.DeleteOldConvertedFilesActivity)        // Remove previous conversion artifacts
	w.RegisterActivity(cw.CreateConvertedFileRecordActivity)      // Create DB record for converted file
	w.RegisterActivity(cw.UploadConvertedFileToMinIOActivity)     // Upload converted markdown to MinIO
	w.RegisterActivity(cw.UpdateConvertedFileDestinationActivity) // Update MinIO destination in DB
	w.RegisterActivity(cw.DeleteConvertedFileRecordActivity)      // Delete conversion record on failure
	w.RegisterActivity(cw.DeleteConvertedFileFromMinIOActivity)   // Delete converted file from MinIO on failure
	w.RegisterActivity(cw.UpdateConversionMetadataActivity)       // Update file status and conversion metadata

	// Format Standardization Phase - Executed before caching
	w.RegisterActivity(cw.StandardizeFileTypeActivity)          // Convert non-AI-native formats (DOCX→PDF, GIF→PNG, MKV→MP4)
	w.RegisterActivity(cw.DeleteTemporaryConvertedFileActivity) // Clean up temporary converted file from MinIO (core-blob/tmp/*)

	// AI Caching Phase - Create caches for efficient processing
	w.RegisterActivity(cw.CacheFileContextActivity)       // Create AI cache for individual file processing
	w.RegisterActivity(cw.CacheChatContextActivity)       // Create AI cache for multi-file chat (Chat API)
	w.RegisterActivity(cw.StoreChatCacheMetadataActivity) // Store chat cache metadata in Redis for Chat API
	w.RegisterActivity(cw.DeleteCacheActivity)            // Clean up AI cache after processing

	// Content and Summary Processing Phase - Composite activities (flattened from child workflows)
	w.RegisterActivity(cw.ProcessContentActivity) // Complete content processing: markdown conversion → save to DB
	w.RegisterActivity(cw.ProcessSummaryActivity) // Complete summary processing: generate → save to DB

	// Chunking Phase - Combined content and summary chunking (sequential after parallel AI operations)
	w.RegisterActivity(cw.DeleteOldTextChunksActivity) // Delete old text chunk records before creating new ones
	w.RegisterActivity(cw.ChunkContentActivity)        // Split markdown content into semantic chunks
	w.RegisterActivity(cw.SaveTextChunksActivity)      // Persist chunks to database and MinIO storage

	// Embedding Phase - Vector embedding generation and storage
	w.RegisterActivity(cw.GetChunksForEmbeddingActivity)   // Retrieve text chunks for embedding
	w.RegisterActivity(cw.UpdateEmbeddingMetadataActivity) // Update file status and embedding metadata

	// ===== SaveEmbeddingsWorkflow Activities (Child Workflow) =====
	// Child workflow handling vector embedding storage
	w.RegisterActivity(cw.DeleteOldEmbeddingsActivity) // Remove old embeddings from both Milvus and DB
	w.RegisterActivity(cw.SaveEmbeddingBatchActivity)  // Save embedding batch to DB and vector store
	w.RegisterActivity(cw.FlushCollectionActivity)     // Flush Milvus collection to persist data

	if err := w.Start(); err != nil {
		logger.Fatal(fmt.Sprintf("Unable to start worker: %s", err))
	}

	logger.Info("Temporal worker started successfully and is polling for tasks")

	// Workflows are triggered by API handlers (e.g., ProcessCatalogFiles in cmd/main)
	// No dispatcher needed - Temporal handles task distribution and retries automatically

	// Setup graceful shutdown on SIGTERM (kill) and SIGINT (Ctrl+C)
	// Note: SIGKILL (kill -9) cannot be caught and will force immediate termination
	quitSig := make(chan os.Signal, 1)
	signal.Notify(quitSig, syscall.SIGINT, syscall.SIGTERM)

	// Block until shutdown signal received
	<-quitSig

	// Allow in-flight workflows to complete gracefully (shared resources like MinIO/DB need proper cleanup)
	logger.Info("Shutdown signal received, waiting for in-flight workflows to complete...")
	time.Sleep(gracefulShutdownWaitPeriod)

	logger.Info("Shutting down worker...")
	w.Stop()
}

// newClients initializes all external service clients and returns a cleanup function
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

	// Initialize mgmt-backend client (for user/org management)
	mgmtPrivateServiceClient, mgmtPrivateClose, err := clientgrpcx.NewClient[mgmtpb.MgmtPrivateServiceClient](
		clientgrpcx.WithServiceConfig(config.Config.MgmtBackend),
		clientgrpcx.WithSetOTELClientHandler(config.Config.OTELCollector.Enable),
	)
	if err != nil {
		logger.Fatal("failed to create mgmt private service client", zap.Error(err))
	}
	closeFuncs["mgmtPrivate"] = mgmtPrivateClose

	// Initialize pipeline-backend client (for AI pipelines and conversions)
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

	// Initialize PostgreSQL database connection (for metadata and file records)
	db := database.GetSharedConnection()
	closeFuncs["database"] = func() error {
		database.Close(db)
		return nil
	}

	// Initialize Redis client (for caching and chat cache metadata)
	redisClient := redis.NewClient(&config.Config.Cache.Redis.RedisOptions)
	closeFuncs["redis"] = redisClient.Close

	// Initialize Temporal client (for workflow orchestration)
	temporalClientOptions, err := temporal.ClientOptions(config.Config.Temporal, logger)
	if err != nil {
		logger.Fatal("Unable to build Temporal client options", zap.Error(err))
	}

	// Add OpenTelemetry tracing interceptor if enabled
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

	// Initialize MinIO client (for object storage - files, chunks, embeddings)
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

	// Initialize Milvus client (for vector database - embedding storage and similarity search)
	vectorDB, vclose, err := repository.NewVectorDatabase(ctx, config.Config.Milvus.Host, config.Config.Milvus.Port)
	if err != nil {
		logger.Fatal(fmt.Sprintf("failed to create milvus client: %v", err))
	}
	closeFuncs["milvus"] = vclose

	// Initialize OpenFGA client (for access control and permissions)
	fgaClient, fgaClientConn := acl.InitOpenFGAClient(ctx, config.Config.OpenFGA.Host, config.Config.OpenFGA.Port)
	closeFuncs["fga"] = fgaClientConn.Close

	// Initialize OpenFGA replica client if configured (for read scaling)
	var fgaReplicaClient openfga.OpenFGAServiceClient
	if config.Config.OpenFGA.Replica.Host != "" {
		var fgaReplicaClientConn *grpc.ClientConn
		fgaReplicaClient, fgaReplicaClientConn = acl.InitOpenFGAClient(ctx, config.Config.OpenFGA.Replica.Host, config.Config.OpenFGA.Replica.Port)
		closeFuncs["fgaReplica"] = fgaReplicaClientConn.Close
	}

	// Create ACL client with primary and optional replica
	aclClient := acl.NewACLClient(fgaClient, fgaReplicaClient, redisClient)

	// Return all clients and a cleanup function that closes all connections
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

// newAIClient creates an AI client based on the configured API keys
func newAIClient(ctx context.Context, logger *zap.Logger) (ai.Client, error) {
	cfg := config.Config
	aiClients := make(map[string]ai.Client)

	// Initialize Gemini client if API key is provided
	if cfg.RAG.Model.Gemini.APIKey != "" {
		geminiClient, err := gemini.NewClient(ctx, cfg.RAG.Model.Gemini.APIKey)
		if err != nil {
			logger.Error("Failed to initialize Gemini client", zap.Error(err))
		} else {
			aiClients[ai.ModelFamilyGemini] = geminiClient
			logger.Info("Gemini client initialized",
				zap.String("client", geminiClient.Name()),
				zap.Int32("embedding_dimension", geminiClient.GetEmbeddingDimensionality()))
		}
	} else {
		logger.Warn("Gemini API key not configured. Content conversion and summarization will use pipeline fallback.")
	}

	// Initialize OpenAI client if API key is provided
	if cfg.RAG.Model.OpenAI.APIKey != "" {
		openaiClient, err := openai.NewClient(ctx, cfg.RAG.Model.OpenAI.APIKey)
		if err != nil {
			logger.Warn("Failed to initialize OpenAI client for legacy embeddings", zap.Error(err))
		} else {
			aiClients[ai.ModelFamilyOpenAI] = openaiClient
			logger.Info("OpenAI client initialized for legacy embeddings",
				zap.Int32("embedding_dimension", openaiClient.GetEmbeddingDimensionality()))
		}
	}

	// Create composite client if we have clients
	if len(aiClients) == 0 {
		return nil, nil // No clients configured - not a fatal error
	}

	aiClient, err := ai.NewCompositeClient(aiClients, ai.DefaultModelFamily)
	if err != nil {
		return nil, fmt.Errorf("failed to create composite client: %w", err)
	}

	logger.Info("AI client initialized successfully",
		zap.String("client", aiClient.Name()),
		zap.Int("available_clients", len(aiClients)))

	return aiClient, nil
}
