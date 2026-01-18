package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.temporal.io/api/enums/v1"
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
	"github.com/instill-ai/artifact-backend/pkg/ai"
	"github.com/instill-ai/artifact-backend/pkg/ai/gemini"
	"github.com/instill-ai/artifact-backend/pkg/ai/openai"
	"github.com/instill-ai/artifact-backend/pkg/ai/vertexai"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/repository/object"
	"github.com/instill-ai/x/client"
	"github.com/instill-ai/x/temporal"

	database "github.com/instill-ai/artifact-backend/pkg/db"
	artifactworker "github.com/instill-ai/artifact-backend/pkg/worker"
	mgmtpb "github.com/instill-ai/protogen-go/mgmt/v1beta"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/v1beta"
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

	// Initialize object storage
	// MinIO is ALWAYS the primary/default storage for file uploads and persistence
	// GCS is initialized separately for on-demand use when explicitly requested via storage_provider parameter
	objectStorage := minioClient
	logger.Info("MinIO object storage initialized as primary storage")

	// Initialize GCS storage for on-demand use (if configured)
	// GCS is only used when GetFile is called with storage_provider=STORAGE_PROVIDER_GCS
	var gcsStorage object.Storage
	var err error
	if config.Config.GCS.ProjectID != "" && config.Config.GCS.Region != "" && config.Config.GCS.Bucket != "" && config.Config.GCS.SAKey != "" {
		// Trim whitespace from service account key to handle YAML multiline formatting
		saKey := strings.TrimSpace(config.Config.GCS.SAKey)

		gcsStorage, err = object.NewGCSStorage(ctx, object.GCSConfig{
			ProjectID:         config.Config.GCS.ProjectID,
			Region:            config.Config.GCS.Region,
			Bucket:            config.Config.GCS.Bucket,
			ServiceAccountKey: saKey,
		})
		if err != nil {
			logger.Warn("GCS object storage initialization failed (will not be available for on-demand use)", zap.Error(err))
			gcsStorage = nil
		} else {
			logger.Info("GCS object storage initialized for on-demand use",
				zap.String("project", config.Config.GCS.ProjectID),
				zap.String("region", config.Config.GCS.Region),
				zap.String("bucket", config.Config.GCS.Bucket))
		}
	} else {
		logger.Info("GCS not configured, will not be available for on-demand use")
	}

	repo := repository.NewRepository(db, vectorDB, objectStorage, redisClient, gcsStorage)

	// Initialize AI client
	// VertexAI client requires GCS storage, while Gemini/OpenAI don't need storage
	// Pass GCS storage if configured, otherwise pass nil (Gemini File API will be used)
	var aiStorage object.Storage
	if gcsStorage != nil {
		aiStorage = gcsStorage // VertexAI needs GCS for file operations
	}
	aiClient, err := newAIClient(ctx, logger, aiStorage)
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

	// GCS cleanup workflow
	w.RegisterWorkflow(cw.GCSCleanupContinuousWorkflow) // Continuous GCS cleanup (runs every 2 minutes)

	// ===== Shared Activities (Used by Multiple Workflows) =====

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
	w.RegisterActivity(cw.GetInProgressFileCountActivity)        // Check for in-progress files before cleanup
	w.RegisterActivity(cw.DeleteKBFilesFromMinIOActivity)        // Delete all KB files from MinIO
	w.RegisterActivity(cw.DropVectorDBCollectionActivity)        // Drop Milvus collection for KB
	w.RegisterActivity(cw.DeleteKBFileRecordsActivity)           // Delete all file records from database
	w.RegisterActivity(cw.DeleteKBConvertedFileRecordsActivity)  // Delete all converted file records
	w.RegisterActivity(cw.DeleteKBTextChunkRecordsActivity)      // Delete all chunk records
	w.RegisterActivity(cw.DeleteKBEmbeddingRecordsActivity)      // Delete all embedding records
	w.RegisterActivity(cw.SoftDeleteKBRecordActivity)            // Soft-delete the KB record itself
	w.RegisterActivity(cw.PurgeKBACLActivity)                    // Remove all ACL permissions for KB
	w.RegisterActivity(cw.ClearProductionKBRetentionActivity)    // Clear retention field on production KB after rollback cleanup
	w.RegisterActivity(cw.CheckRollbackRetentionExpiredActivity) // Check if rollback KB retention period has expired

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
	w.RegisterActivity(cw.VerifyKBCleanupActivity)                 // Verify KB cleanup was successful
	w.RegisterActivity(cw.GetKBCollectionUIDActivity)              // Get KB collection UID for verification

	// ===== ProcessFileWorkflow Activities (Main Workflow) =====
	// Main workflow orchestrating the entire file processing pipeline
	// Content generation and summarization are handled by composite activities (ProcessContentActivity, ProcessSummaryActivity)

	// File Metadata and Setup Phase
	w.RegisterActivity(cw.GetFileMetadataActivity)                // Fetch file metadata from database
	w.RegisterActivity(cw.FindTargetFileByNameActivity)           // Find target file for dual-processing coordination
	w.RegisterActivity(cw.GetFileContentActivity)                 // Retrieve file binary content from MinIO
	w.RegisterActivity(cw.DeleteOldConvertedFilesActivity)        // Remove previous conversion artifacts
	w.RegisterActivity(cw.CreateConvertedFileRecordActivity)      // Create DB record for converted file
	w.RegisterActivity(cw.UploadConvertedFileToMinIOActivity)     // Upload converted markdown to MinIO
	w.RegisterActivity(cw.UpdateConvertedFileDestinationActivity) // Update MinIO destination in DB
	w.RegisterActivity(cw.DeleteConvertedFileRecordActivity)      // Delete conversion record on failure
	w.RegisterActivity(cw.DeleteConvertedFileFromMinIOActivity)   // Delete converted file from MinIO on failure
	w.RegisterActivity(cw.UpdateConversionMetadataActivity)       // Update file status and conversion metadata
	w.RegisterActivity(cw.UpdateUsageMetadataActivity)            // Update usage metadata (AI token counts) from content and summary processing

	// Format Standardization Phase - Executed before caching
	w.RegisterActivity(cw.StandardizeFileTypeActivity) // Convert non-AI-native formats and save all standard files (PDF, PNG, OGG, MP4) to converted-file folder

	// AI Caching Phase - Create caches for efficient processing
	w.RegisterActivity(cw.CacheFileContextActivity) // Create AI cache for individual file processing
	w.RegisterActivity(cw.DeleteCacheActivity)      // Clean up AI cache after processing

	// Content and Summary Processing Phase - Composite activities (flattened from child workflows)
	w.RegisterActivity(cw.ProcessContentActivity) // Complete content processing: markdown conversion → save to DB
	w.RegisterActivity(cw.ProcessSummaryActivity) // Complete summary processing: generate → save to DB

	// Chunking Phase - Combined content and summary chunking (sequential after parallel AI operations)
	w.RegisterActivity(cw.DeleteOldTextChunksActivity) // Delete old text chunk records before creating new ones
	w.RegisterActivity(cw.ChunkContentActivity)        // Split markdown content into semantic chunks
	w.RegisterActivity(cw.SaveChunksActivity)          // Persist chunks to database and MinIO storage

	// Embedding Phase - Vector embedding generation and storage
	w.RegisterActivity(cw.EmbedAndSaveChunksActivity)      // Combined: query chunks, generate embeddings, save to DB/Milvus
	w.RegisterActivity(cw.UpdateEmbeddingMetadataActivity) // Update file status and embedding metadata

	// ===== GCS Cleanup Activities =====
	// Activities for cleaning up expired GCS files
	w.RegisterActivity(cw.CleanupExpiredGCSFilesActivity) // Scan and delete expired GCS files from bucket and Redis

	if err := w.Start(); err != nil {
		logger.Fatal(fmt.Sprintf("Unable to start worker: %s", err))
	}

	logger.Info("Temporal worker started successfully and is polling for tasks")

	// Start the GCS cleanup continuous workflow as a singleton
	// This workflow runs indefinitely, cleaning up expired GCS files every 2 minutes
	// Using a fixed WorkflowID ensures only one instance runs at a time
	go func() {
		workflowOptions := temporalclient.StartWorkflowOptions{
			ID:                       "artifact-backend-gcs-cleanup-continuous-singleton",
			TaskQueue:                artifactworker.TaskQueue,
			WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
			WorkflowExecutionTimeout: 0, // No timeout - runs indefinitely
		}

		logger.Info("Starting GCS cleanup continuous workflow (singleton)")
		_, err := temporalClient.ExecuteWorkflow(context.Background(), workflowOptions, cw.GCSCleanupContinuousWorkflow)
		if err != nil {
			logger.Error("Failed to start GCS cleanup continuous workflow", zap.Error(err))
		} else {
			logger.Info("GCS cleanup continuous workflow started successfully")
		}
	}()

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
	object.Storage,
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
	logger.Info("Initializing MinIO client", zap.String("bucket", config.Config.Minio.BucketName), zap.String("host", config.Config.Minio.Host))
	minioClient, err := object.NewMinIOStorage(ctx, miniox.ClientParams{
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
	logger.Info("MinIO client initialized successfully", zap.String("bucket", config.Config.Minio.BucketName))

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
func newAIClient(ctx context.Context, logger *zap.Logger, storage object.Storage) (ai.Client, error) {
	cfg := config.Config
	aiClients := make(map[string]ai.Client)

	// Initialize VertexAI client if configured (takes precedence over Gemini for "gemini" model family)
	// VertexAI requires GCS object storage backend
	if cfg.RAG.Model.VertexAI.ProjectID != "" && cfg.RAG.Model.VertexAI.Region != "" && cfg.RAG.Model.VertexAI.SAKey != "" && storage != nil {
		vertexaiClient, err := vertexai.NewClient(ctx, vertexai.Config{
			ProjectID: cfg.RAG.Model.VertexAI.ProjectID,
			Region:    cfg.RAG.Model.VertexAI.Region,
			SAKey:     cfg.RAG.Model.VertexAI.SAKey,
		}, storage)
		if err != nil {
			logger.Error("Failed to initialize VertexAI client", zap.Error(err))
		} else {
			// VertexAI handles "gemini" model family requests
			aiClients[ai.ModelFamilyGemini] = vertexaiClient
			logger.Info("VertexAI client initialized",
				zap.String("client", vertexaiClient.Name()),
				zap.String("project", cfg.RAG.Model.VertexAI.ProjectID),
				zap.String("region", cfg.RAG.Model.VertexAI.Region),
				zap.Int32("embedding_dimension", vertexaiClient.GetEmbeddingDimensionality()))
		}
	} else if cfg.RAG.Model.Gemini.APIKey != "" {
		// Fallback to Gemini API client if VertexAI not configured
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
		logger.Warn("Neither VertexAI nor Gemini API configured. Content conversion and summarization will use pipeline fallback.")
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
