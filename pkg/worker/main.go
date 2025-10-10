package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	"github.com/redis/go-redis/v9"
	"go.temporal.io/sdk/client"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/internal/ai"
	"github.com/instill-ai/artifact-backend/internal/ai/gemini"
	"github.com/instill-ai/artifact-backend/pkg/acl"
	"github.com/instill-ai/artifact-backend/pkg/pipeline"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
)

// TaskQueue is the Temporal task queue name for all workflows and activities.
const TaskQueue = "artifact-backend"

// TextChunkSize controls tokens per text chunk. Smaller = more embeddings = slower. Larger = faster but less precise.
// TextChunkOverlap controls overlap between text chunks (20% = good context continuity).
const (
	TextChunkSize    = 1000 // Balance: cost vs precision
	TextChunkOverlap = 200  // Balance: context vs redundancy
)

// EmbeddingBatchSize controls embeddings per DB transaction. Smaller = more parallelism + overhead. Larger = less overhead + contention.
// Example: 10 files × 1000 embeddings ÷ 50 = 200 parallel activities. Increase to 100-200 under high load.
const EmbeddingBatchSize = 50

// ActivityTimeoutStandard is timeout for normal activities. ActivityTimeoutLong is for heavy operations.
// Too short = premature failures. Too long = blocked worker slots.
const (
	ActivityTimeoutStandard = 5 * time.Minute  // File I/O, DB, MinIO
	ActivityTimeoutLong     = 10 * time.Minute // File conversion, embeddings
)

// RetryInitialInterval, RetryBackoffCoefficient, RetryMaximumInterval*, and RetryMaximumAttempts control retry behavior.
// Prevents retry storms under high concurrency.
const (
	RetryInitialInterval         = 1 * time.Second   // Prevents retry storms
	RetryBackoffCoefficient      = 2.0               // Exponential: 1s→2s→4s
	RetryMaximumIntervalStandard = 30 * time.Second  // Transient failures
	RetryMaximumIntervalLong     = 100 * time.Second // Service recovery
	RetryMaximumAttempts         = 3                 // 3 attempts = ~7s max
)

// Worker implements the Temporal worker with all workflows and activities
type Worker struct {
	// Infrastructure dependencies (primitives)
	repository     repository.Repository
	pipelineClient pipelinepb.PipelinePublicServiceClient
	aclClient      *acl.ACLClient
	redisClient    *redis.Client

	// Temporal client for workflow execution
	temporalClient client.Client

	// Worker-specific dependencies
	aiProvider ai.Provider
	log        *zap.Logger
}

// TemporalClient returns the Temporal client for workflow execution
func (w *Worker) TemporalClient() client.Client {
	return w.temporalClient
}

// GetAIProvider returns the AI provider for external use (e.g., service layer)
// This enables the service layer to perform AI operations without circular dependencies
func (w *Worker) GetAIProvider() ai.Provider {
	return w.aiProvider
}

// GetRepository returns the repository for external use (e.g., EE worker overrides)
func (w *Worker) GetRepository() repository.Repository {
	return w.repository
}

// GetPipelineClient returns the pipeline client for external use (e.g., EE worker overrides)
func (w *Worker) GetPipelineClient() pipelinepb.PipelinePublicServiceClient {
	return w.pipelineClient
}

// GetLogger returns the logger for external use (e.g., EE worker overrides)
func (w *Worker) GetLogger() *zap.Logger {
	return w.log
}

// New creates a new worker instance with direct dependencies (no circular dependency)
func New(
	temporalClient client.Client,
	repo repository.Repository,
	pipelineClient pipelinepb.PipelinePublicServiceClient,
	aclClient *acl.ACLClient,
	redisClient *redis.Client,
	log *zap.Logger,
) (*Worker, error) {
	w := &Worker{
		repository:     repo,
		pipelineClient: pipelineClient,
		aclClient:      aclClient,
		redisClient:    redisClient,
		temporalClient: temporalClient,
		log:            log,
	}

	// Initialize AI provider for unstructured data content understanding
	// If not configured or initialization fails, worker will fall back to pipeline conversion
	var provider ai.Provider
	cfg := config.Config // Access global config
	if cfg.RAG.Model.Gemini.APIKey != "" {
		geminiProvider, err := gemini.NewProvider(context.Background(), cfg.RAG.Model.Gemini.APIKey)
		if err != nil {
			log.Warn("Failed to initialize AI provider for content understanding, will use pipeline fallback",
				zap.Error(err))
		} else {
			provider = geminiProvider
			log.Info("AI provider for content understanding initialized successfully",
				zap.String("provider", provider.Name()))
		}
	}

	// Future: Add support for additional AI providers (e.g., OpenAI, Claude)
	// if provider == nil && cfg.RAG.Model.OpenAI.APIKey != "" {
	//     openaiProvider, err := openai.NewProvider(context.Background(), cfg.RAG.Model.OpenAI.APIKey)
	//     if err == nil {
	//         provider = openaiProvider
	//     }
	// }

	w.aiProvider = provider

	return w, nil
}

// Use-case methods for workflow orchestration
// These provide a clean interface for handlers to trigger workflows

// ProcessFile orchestrates the file processing workflow for one or more files
func (w *Worker) ProcessFile(ctx context.Context, kbUID types.KBUIDType, fileUIDs []types.FileUIDType, userUID, requesterUID types.RequesterUIDType) error {
	workflow := NewProcessFileWorkflow(w.temporalClient, w)
	return workflow.Execute(ctx, ProcessFileWorkflowParam{
		KBUID:        kbUID,
		FileUIDs:     fileUIDs,
		UserUID:      userUID,
		RequesterUID: requesterUID,
	})
}

// CleanupFile orchestrates the file cleanup workflow
func (w *Worker) CleanupFile(ctx context.Context, fileUID types.FileUIDType, userUID, requesterUID types.RequesterUIDType, workflowID string, includeOriginalFile bool) error {
	workflow := NewCleanupFileWorkflow(w.temporalClient, w)
	return workflow.Execute(ctx, CleanupFileWorkflowParam{
		FileUID:             fileUID,
		UserUID:             userUID,
		RequesterUID:        requesterUID,
		WorkflowID:          workflowID,
		IncludeOriginalFile: includeOriginalFile,
	})
}

// GetFilesByPaths retrieves files by their paths using workflow
func (w *Worker) GetFilesByPaths(ctx context.Context, bucket string, filePaths []string) ([]FileContent, error) {
	workflow := NewGetFilesWorkflow(w.temporalClient, w)
	return workflow.Execute(ctx, GetFilesWorkflowParam{
		Bucket:    bucket,
		FilePaths: filePaths,
	})
}

// DeleteFiles deletes files using workflow
func (w *Worker) DeleteFiles(ctx context.Context, bucket string, filePaths []string) error {
	workflow := NewDeleteFilesWorkflow(w.temporalClient, w)
	return workflow.Execute(ctx, DeleteFilesWorkflowParam{
		Bucket:    bucket,
		FilePaths: filePaths,
	})
}

// CleanupKnowledgeBase cleans up a knowledge base using workflow
func (w *Worker) CleanupKnowledgeBase(ctx context.Context, kbUID types.KBUIDType) error {
	workflow := NewCleanupKnowledgeBaseWorkflow(w.temporalClient, w)
	return workflow.Execute(ctx, CleanupKnowledgeBaseWorkflowParam{
		KBUID: kbUID,
	})
}

// EmbedTexts embeds texts using workflow
func (w *Worker) EmbedTexts(ctx context.Context, texts []string, batchSize int, requestMetadata map[string][]string) ([][]float32, error) {
	workflow := NewEmbedTextsWorkflow(w.temporalClient, w)
	return workflow.Execute(ctx, EmbedTextsWorkflowParam{
		Texts:           texts,
		BatchSize:       batchSize,
		RequestMetadata: requestMetadata,
	})
}

// Helper methods (high-level operations used by activities)

// deleteFilesSync deletes multiple files from MinIO (synchronous implementation without workflow)
func (w *Worker) deleteFilesSync(ctx context.Context, bucket string, filePaths []string) error {
	if len(filePaths) == 0 {
		return nil
	}

	// Synchronous deletion (no workflow orchestration)
	// For parallel/reliable deletion, use worker.DeleteFiles workflow instead
	for _, filePath := range filePaths {
		if err := w.repository.DeleteFile(ctx, bucket, filePath); err != nil {
			return fmt.Errorf("failed to delete file %s: %w", filePath, err)
		}
	}

	return nil
}

// deleteKnowledgeBaseSync deletes all files in a knowledge base
func (w *Worker) deleteKnowledgeBaseSync(ctx context.Context, kbUID string) error {
	kbUUID, err := uuid.FromString(kbUID)
	if err != nil {
		return fmt.Errorf("invalid knowledge base UID: %w", err)
	}

	filePaths, err := w.repository.ListKnowledgeBaseFilePaths(ctx, kbUUID)
	if err != nil {
		return fmt.Errorf("failed to list knowledge base files: %w", err)
	}
	return w.deleteFilesSync(ctx, config.Config.Minio.BucketName, filePaths)
}

// deleteConvertedFileByFileUIDSync deletes converted files for a specific file UID
func (w *Worker) deleteConvertedFileByFileUIDSync(ctx context.Context, kbUID, fileUID types.FileUIDType) error {
	filePaths, err := w.repository.ListConvertedFilesByFileUID(ctx, kbUID, fileUID)
	if err != nil {
		return fmt.Errorf("failed to list converted files: %w", err)
	}
	return w.deleteFilesSync(ctx, config.Config.Minio.BucketName, filePaths)
}

// deleteTextChunksByFileUIDSync deletes text chunks for a specific file UID
func (w *Worker) deleteTextChunksByFileUIDSync(ctx context.Context, kbUID, fileUID types.FileUIDType) error {
	filePaths, err := w.repository.ListTextChunksByFileUID(ctx, kbUID, fileUID)
	if err != nil {
		return fmt.Errorf("failed to list text chunk files: %w", err)
	}
	return w.deleteFilesSync(ctx, config.Config.Minio.BucketName, filePaths)
}

// getTextChunksByFile returns the text chunks of a file
// Fetches text chunk content directly from MinIO using parallel goroutines for better performance
func (w *Worker) getTextChunksByFile(ctx context.Context, file *repository.KnowledgeBaseFileModel) (
	types.SourceTableType,
	types.SourceUIDType,
	[]repository.TextChunkModel,
	map[types.TextChunkUIDType]types.ContentType,
	[]string,
	error,
) {
	var sourceTable string
	var sourceUID types.SourceUIDType

	// Get converted file for all types
	convertedFile, err := w.repository.GetConvertedFileByFileUID(ctx, file.UID)
	if err != nil {
		w.log.Error("Failed to get converted file metadata.", zap.String("File uid", file.UID.String()))
		return sourceTable, sourceUID, nil, nil, nil, err
	}
	sourceTable = repository.ConvertedFileTableName
	sourceUID = convertedFile.UID

	// Get text chunks metadata from database
	chunks, err := w.repository.GetTextChunksBySource(ctx, sourceTable, sourceUID)
	if err != nil {
		w.log.Error("Failed to get text chunks from database.", zap.String("SourceUID", sourceUID.String()))
		return sourceTable, sourceUID, nil, nil, nil, err
	}

	// Fetch text chunks from MinIO in parallel using goroutines with retry
	texts := make([]string, len(chunks))
	var wg sync.WaitGroup
	var mu sync.Mutex
	var fetchErr error

	bucket := config.Config.Minio.BucketName

	for i, chunk := range chunks {
		wg.Add(1)
		go func(idx int, path string) {
			defer wg.Done()

			// Retry up to 3 times with exponential backoff for transient failures
			var content []byte
			var err error
			maxAttempts := 3

			for attempt := range maxAttempts {
				content, err = w.repository.GetFile(ctx, bucket, path)
				if err == nil {
					break // Success!
				}

				// Don't sleep after last attempt
				if attempt < maxAttempts-1 {
					// Exponential backoff: 1s, 2s
					backoff := time.Duration(1<<uint(attempt)) * time.Second
					time.Sleep(backoff)
				}
			}

			if err != nil {
				mu.Lock()
				if fetchErr == nil {
					fetchErr = fmt.Errorf("failed to fetch text chunk %s after %d attempts: %w", path, maxAttempts, err)
				}
				mu.Unlock()
				return
			}

			mu.Lock()
			texts[idx] = string(content)
			mu.Unlock()
		}(i, chunk.ContentDest)
	}

	wg.Wait()

	if fetchErr != nil {
		w.log.Error("Failed to get text chunks from MinIO.",
			zap.String("SourceTable", sourceTable),
			zap.String("SourceUID", sourceUID.String()),
			zap.Error(fetchErr))
		return sourceTable, sourceUID, nil, nil, nil, fetchErr
	}

	// Build text chunk UID to content map
	chunkUIDToContents := make(map[types.TextChunkUIDType]types.ContentType, len(chunks))
	for i, c := range chunks {
		chunkUIDToContents[c.UID] = types.ContentType(texts[i])
	}

	return sourceTable, sourceUID, chunks, chunkUIDToContents, texts, nil
}

// embedTextsSync generates embeddings for texts (synchronous implementation without workflow)
func (w *Worker) embedTextBatch(ctx context.Context, texts []string) ([][]float32, error) {
	// Synchronous embedding (no workflow orchestration)
	// For large batches with retries, use worker.EmbedTexts workflow instead

	// Get request metadata from context
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		md = metadata.MD{}
	}

	// Call standalone function
	embeddings, err := pipeline.EmbedTextPipe(ctx, w.pipelineClient, texts, md)
	if err != nil {
		return nil, fmt.Errorf("failed to embed texts: %w", err)
	}

	return embeddings, nil
}
