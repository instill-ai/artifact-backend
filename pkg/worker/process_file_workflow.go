package worker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/pipeline"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

// ProcessFileWorkflowParam defines the parameters for ProcessFileWorkflow
type ProcessFileWorkflowParam struct {
	FileUID      types.FileUIDType      // File unique identifier
	KBUID        types.KBUIDType        // Knowledge base unique identifier
	UserUID      types.UserUIDType      // User unique identifier
	RequesterUID types.RequesterUIDType // Requester unique identifier (for access control)
}

type processFileWorkflow struct {
	temporalClient client.Client
	worker         *Worker
}

// NewProcessFileWorkflow creates a new ProcessFileWorkflow instance
func NewProcessFileWorkflow(temporalClient client.Client, worker *Worker) *processFileWorkflow {
	return &processFileWorkflow{
		temporalClient: temporalClient,
		worker:         worker,
	}
}

func (w *processFileWorkflow) Execute(ctx context.Context, param ProcessFileWorkflowParam) error {
	workflowID := fmt.Sprintf("process-file-%s", param.FileUID.String())
	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	_, err := w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, w.worker.ProcessFileWorkflow, param)
	return err
}

// ProcessFileWorkflow orchestrates the file processing pipeline using parallel child workflows
// that share an AI cache for optimal performance.
//
// Architecture:
// 1. Create shared AI cache
// 2. Run in parallel:
//   - ProcessContentWorkflow: cleanup old file → format conversion → markdown conversion → save to DB
//   - ProcessSummaryWorkflow: summary generation → save to DB
//
// 3. Chunk content and summary into text chunks, generate embeddings, and save to vector DB
// 4. Update metadata and cleanup cache
func (w *Worker) ProcessFileWorkflow(ctx workflow.Context, param ProcessFileWorkflowParam) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting ProcessFileWorkflow",
		"fileUID", param.FileUID.String(),
		"userUID", param.UserUID.String(),
		"requesterUID", param.RequesterUID.String())

	// Extract UUIDs from parameters
	fileUID := param.FileUID
	kbUID := param.KBUID
	userUID := param.UserUID
	requesterUID := param.RequesterUID

	// Track workflow completion status
	workflowCompleted := false

	// Defer cleanup: If workflow is terminated/cancelled/timeout, mark file as FAILED
	defer func() {
		if !workflowCompleted {
			// Workflow did not complete successfully - mark as failed
			// Use disconnected context so this runs even if workflow is cancelled
			cleanupCtx, _ := workflow.NewDisconnectedContext(ctx)
			cleanupCtx = workflow.WithActivityOptions(cleanupCtx, workflow.ActivityOptions{
				StartToCloseTimeout: time.Minute,
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval:    time.Second,
					BackoffCoefficient: 2.0,
					MaximumInterval:    30 * time.Second,
					MaximumAttempts:    3,
				},
			})

			logger.Warn("Workflow did not complete successfully, marking file as FAILED",
				"fileUID", fileUID.String())

			// Best effort update - ignore errors
			_ = workflow.ExecuteActivity(cleanupCtx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
				FileUID: fileUID,
				Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_FAILED,
				Message: "File processing was interrupted or terminated before completion",
			}).Get(cleanupCtx, nil)
		}
	}()

	// Set workflow options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: ActivityTimeoutLong,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    RetryInitialInterval,
			BackoffCoefficient: RetryBackoffCoefficient,
			MaximumInterval:    RetryMaximumIntervalLong,
			MaximumAttempts:    RetryMaximumAttempts,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Helper function to handle errors and update status
	handleError := func(stage string, err error) error {
		logger.Error("Failed at stage", "stage", stage, "error", err)

		// Extract a clean error message for display
		errMsg := errorsx.MessageOrErr(err)

		// Update file status to FAILED
		statusErr := workflow.ExecuteActivity(ctx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
			FileUID: fileUID,
			Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_FAILED,
			Message: fmt.Sprintf("%s: %s", stage, errMsg),
		}).Get(ctx, nil)
		if statusErr != nil {
			logger.Error("Failed to update file status to FAILED", "statusError", statusErr)
		}

		// Return the error to mark the workflow as Failed in Temporal
		workflowErr := errorsx.AddMessage(
			fmt.Errorf("%s: %s", stage, errMsg),
			fmt.Sprintf("File processing failed at %s stage. %s", stage, errMsg),
		)
		return workflowErr
	}

	// Get current file status to determine starting point
	var startStatus artifactpb.FileProcessStatus
	if err := workflow.ExecuteActivity(ctx, w.GetFileStatusActivity, &GetFileStatusActivityParam{
		FileUID: fileUID,
	}).Get(ctx, &startStatus); err != nil {
		return handleError("get file status", err)
	}

	// Handle different starting statuses:
	// - COMPLETED/FAILED: Full reprocessing from start (go to PROCESSING)
	// - Old statuses (CONVERTING/SUMMARIZING): Restart from PROCESSING (parallel architecture)
	// - PROCESSING: Continue (retry/reconciliation)
	// - CHUNKING: Resume from text chunking phase (skip conversion/summarization)
	// - EMBEDDING: Resume from embedding phase (skip conversion/summarization/text chunking)
	if startStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED ||
		startStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_FAILED ||
		startStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CONVERTING ||
		startStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_SUMMARIZING {
		logger.Info("Reprocessing file from beginning",
			"fileUID", param.FileUID,
			"userUID", userUID.String(),
			"requesterUID", requesterUID.String(),
			"previousStatus", startStatus.String())
		startStatus = artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_PROCESSING
	}

	// Process file through parallel pipelines (full processing from start)
	if startStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_NOTSTARTED ||
		startStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_PROCESSING {

		// Step 1: Update status to PROCESSING
		logger.Info("Starting PROCESSING phase",
			"fileUID", fileUID.String(),
			"userUID", userUID.String(),
			"requesterUID", requesterUID.String())
		if err := workflow.ExecuteActivity(ctx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
			FileUID: fileUID,
			Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_PROCESSING,
		}).Get(ctx, nil); err != nil {
			logger.Warn("Failed to update status to PROCESSING", "error", err)
		}

		// Step 2: Get file and KB metadata (single DB read)
		var metadata GetFileMetadataActivityResult
		if err := workflow.ExecuteActivity(ctx, w.GetFileMetadataActivity, &GetFileMetadataActivityParam{
			FileUID: fileUID,
			KBUID:   kbUID,
		}).Get(ctx, &metadata); err != nil {
			// If file not found, it may have been deleted - exit gracefully
			if strings.Contains(err.Error(), "file not found") {
				logger.Info("File not found during processing, exiting workflow gracefully",
					"fileUID", fileUID.String(),
					"userUID", userUID.String(),
					"requesterUID", requesterUID.String())
				return nil
			}
			return handleError("get file metadata", err)
		}

		bucket := repository.BucketFromDestination(metadata.File.Destination)
		fileType := artifactpb.FileType(artifactpb.FileType_value[metadata.File.Type])

		// Step 3: Create shared AI cache for efficient processing
		// This cache will be used by both parallel workflows
		var cacheResult CacheContextActivityResult
		if err := workflow.ExecuteActivity(ctx, w.CacheContextActivity, &CacheContextActivityParam{
			FileUID:     fileUID,
			KBUID:       kbUID,
			Bucket:      bucket,
			Destination: metadata.File.Destination,
			FileType:    fileType,
			Filename:    metadata.File.Name,
			Metadata:    metadata.ExternalMetadata,
		}).Get(ctx, &cacheResult); err != nil {
			// Cache creation is optional - log and continue without cache
			logger.Warn("Cache creation failed, continuing without cache", "error", err)
			cacheResult.CacheEnabled = false
		}

		// Schedule cache cleanup if cache was created
		cacheName := ""
		if cacheResult.CacheEnabled {
			cacheName = cacheResult.CacheName
			logger.Info("AI cache created successfully",
				"cacheName", cacheName,
				"model", cacheResult.Model,
				"expireTime", cacheResult.ExpireTime,
				"fileUID", fileUID.String(),
				"userUID", userUID.String(),
				"requesterUID", requesterUID.String())

			// Defer cache cleanup to ensure it runs at the end
			defer func() {
				cleanupCtx, _ := workflow.NewDisconnectedContext(ctx)
				cleanupCtx = workflow.WithActivityOptions(cleanupCtx, workflow.ActivityOptions{
					StartToCloseTimeout: time.Minute,
					RetryPolicy: &temporal.RetryPolicy{
						InitialInterval:    time.Second,
						BackoffCoefficient: 2.0,
						MaximumInterval:    30 * time.Second,
						MaximumAttempts:    3,
					},
				})

				if err := workflow.ExecuteActivity(cleanupCtx, w.DeleteCacheActivity, &DeleteCacheActivityParam{
					CacheName: cacheName,
				}).Get(cleanupCtx, nil); err != nil {
					logger.Warn("Cache cleanup failed (cache will expire automatically)",
						"error", err,
						"cacheName", cacheName)
				} else {
					logger.Info("AI cache cleaned up successfully", "cacheName", cacheName)
				}
			}()
		}

		// Step 4: Run two child workflows in parallel with shared cache
		logger.Info("Starting parallel child workflows",
			"contentWorkflowID", fmt.Sprintf("process-content-%s", fileUID.String()),
			"summaryWorkflowID", fmt.Sprintf("process-summary-%s", fileUID.String()),
			"fileUID", fileUID.String(),
			"userUID", userUID.String(),
			"requesterUID", requesterUID.String())

		// Configure child workflow options
		contentWorkflowOptions := workflow.ChildWorkflowOptions{
			WorkflowID:         fmt.Sprintf("process-content-%s", fileUID.String()),
			TaskQueue:          workflow.GetInfo(ctx).TaskQueueName,
			WorkflowRunTimeout: 30 * time.Minute,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    time.Second,
				BackoffCoefficient: 2.0,
				MaximumInterval:    5 * time.Minute,
				MaximumAttempts:    3,
			},
		}

		summaryWorkflowOptions := workflow.ChildWorkflowOptions{
			WorkflowID:         fmt.Sprintf("process-summary-%s", fileUID.String()),
			TaskQueue:          workflow.GetInfo(ctx).TaskQueueName,
			WorkflowRunTimeout: 15 * time.Minute,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    time.Second,
				BackoffCoefficient: 2.0,
				MaximumInterval:    5 * time.Minute,
				MaximumAttempts:    3,
			},
		}

		contentCtx := workflow.WithChildOptions(ctx, contentWorkflowOptions)
		summaryCtx := workflow.WithChildOptions(ctx, summaryWorkflowOptions)

		// Start both child workflows in parallel using workflow.Go
		var contentResult ProcessContentWorkflowResult
		var summaryResult ProcessSummaryWorkflowResult
		var contentErr, summaryErr error

		// Create selectors for parallel execution
		contentFuture := workflow.ExecuteChildWorkflow(contentCtx, w.ProcessContentWorkflow, ProcessContentWorkflowParam{
			FileUID:             fileUID,
			KBUID:               kbUID,
			Bucket:              bucket,
			Destination:         metadata.File.Destination,
			FileType:            fileType,
			Filename:            metadata.File.Name,
			ConvertingPipelines: metadata.ConvertingPipelines,
			Metadata:            metadata.ExternalMetadata,
			CacheName:           cacheName, // Shared cache
		})

		summaryFuture := workflow.ExecuteChildWorkflow(summaryCtx, w.ProcessSummaryWorkflow, ProcessSummaryWorkflowParam{
			FileUID:     fileUID,
			KBUID:       kbUID,
			Bucket:      bucket,
			Destination: metadata.File.Destination,
			FileName:    metadata.File.Name,
			FileType:    metadata.File.Type,
			Metadata:    metadata.ExternalMetadata,
			CacheName:   cacheName, // Shared cache
		})

		// Wait for both workflows to complete
		contentErr = contentFuture.Get(contentCtx, &contentResult)
		summaryErr = summaryFuture.Get(summaryCtx, &summaryResult)

		// Check for errors in either workflow
		if contentErr != nil {
			return handleError("process converted content", contentErr)
		}
		if summaryErr != nil {
			return handleError("process summary", summaryErr)
		}

		logger.Info("Both parallel workflows completed successfully",
			"summaryLength", len(summaryResult.Summary),
			"fileUID", fileUID.String(),
			"userUID", userUID.String(),
			"requesterUID", requesterUID.String())

		// Step 6: Update conversion metadata (from content workflow)
		pipelineName := ""
		if contentResult.ConversionPipeline.ID != "" {
			pipelineName = contentResult.ConversionPipeline.Name()
		}
		if err := workflow.ExecuteActivity(ctx, w.UpdateConversionMetadataActivity, &UpdateConversionMetadataActivityParam{
			FileUID:  fileUID,
			Length:   contentResult.Length,
			Pipeline: pipelineName,
		}).Get(ctx, nil); err != nil {
			return handleError("update conversion metadata", err)
		}

		// Step 7: Chunk content and summary into text chunks
		logger.Info("Starting CHUNKING phase (sequential after parallel workflows)",
			"fileUID", fileUID.String(),
			"userUID", userUID.String(),
			"requesterUID", requesterUID.String())

		// Update status to CHUNKING
		if err := workflow.ExecuteActivity(ctx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
			FileUID: fileUID,
			Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING,
		}).Get(ctx, nil); err != nil {
			logger.Warn("Failed to update status to CHUNKING", "error", err)
		}

		// 7a. Get converted file for text chunking
		var convertedFile GetConvertedFileForChunkingActivityResult
		if err := workflow.ExecuteActivity(ctx, w.GetConvertedFileForChunkingActivity, &GetConvertedFileForChunkingActivityParam{
			FileUID: fileUID,
			KBUID:   kbUID,
		}).Get(ctx, &convertedFile); err != nil {
			return handleError("get converted file for chunking", err)
		}

		// 7b. Chunk content
		var contentChunks ChunkContentActivityResult
		if err := workflow.ExecuteActivity(ctx, w.ChunkContentActivity, &ChunkContentActivityParam{
			FileUID:   fileUID,
			KBUID:     kbUID,
			Content:   convertedFile.Content,
			Pipelines: convertedFile.ChunkingPipelines,
			Metadata:  metadata.ExternalMetadata,
		}).Get(ctx, &contentChunks); err != nil {
			return handleError("chunk content", err)
		}

		// 7c. Combine content and summary text chunks (if any)
		allTextChunks := contentChunks.Chunks
		if len(summaryResult.Summary) > 0 {
			var summaryChunks ChunkContentActivityResult
			if err := workflow.ExecuteActivity(ctx, w.ChunkContentActivity, &ChunkContentActivityParam{
				FileUID:   fileUID,
				KBUID:     kbUID,
				Content:   summaryResult.Summary,
				Pipelines: []pipeline.PipelineRelease{}, // Use default
				Metadata:  metadata.ExternalMetadata,
			}).Get(ctx, &summaryChunks); err != nil {
				return handleError("chunk summary", err)
			}
			allTextChunks = append(allTextChunks, summaryChunks.Chunks...)
		}

		// 7d. Save text chunks to DB (single DB transaction)
		if err := workflow.ExecuteActivity(ctx, w.SaveTextChunksToDBActivity, &SaveTextChunksToDBActivityParam{
			KBUID:      kbUID,
			FileUID:    fileUID,
			TextChunks: allTextChunks,
		}).Get(ctx, nil); err != nil {
			return handleError("save text chunks to DB", err)
		}

		// 7e. Update text chunking metadata
		if err := workflow.ExecuteActivity(ctx, w.UpdateChunkingMetadataActivity, &UpdateChunkingMetadataActivityParam{
			FileUID:          fileUID,
			KBUID:            kbUID,
			ChunkingPipeline: contentChunks.PipelineRelease.Name(),
			TextChunkCount:   uint32(len(allTextChunks)),
		}).Get(ctx, nil); err != nil {
			return handleError("update chunking metadata", err)
		}

		logger.Info("CHUNKING phase completed successfully",
			"fileUID", fileUID.String(),
			"userUID", userUID.String(),
			"requesterUID", requesterUID.String(),
			"totalChunks", len(allTextChunks))

		// Step 8: Generate embeddings
		logger.Info("Starting EMBEDDING phase",
			"fileUID", fileUID.String(),
			"userUID", userUID.String(),
			"requesterUID", requesterUID.String())

		// Update status to EMBEDDING
		if err := workflow.ExecuteActivity(ctx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
			FileUID: fileUID,
			Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING,
		}).Get(ctx, nil); err != nil {
			logger.Warn("Failed to update status to EMBEDDING", "error", err)
		}

		// 8a. Get text chunks from database (single DB read + service call)
		var chunksData GetTextChunksForEmbeddingActivityResult
		if err := workflow.ExecuteActivity(ctx, w.GetChunksForEmbeddingActivity, &GetChunksForEmbeddingActivityParam{
			FileUID: fileUID,
		}).Get(ctx, &chunksData); err != nil {
			return handleError("get text chunks for embedding", err)
		}

		// 8b. Generate embeddings using child workflow for proper parallel batching
		// Extract request metadata for authentication
		var requestMetadata map[string][]string
		if chunksData.Metadata != nil {
			md, err := extractRequestMetadata(chunksData.Metadata)
			if err != nil {
				return handleError("extract request metadata", err)
			}
			requestMetadata = md
		}

		embedWorkflowOptions := workflow.ChildWorkflowOptions{
			WorkflowID: fmt.Sprintf("embed-texts-%s", fileUID.String()),
		}
		embedCtx := workflow.WithChildOptions(ctx, embedWorkflowOptions)

		var embeddingVectors [][]float32
		if err := workflow.ExecuteChildWorkflow(embedCtx, w.EmbedTextsWorkflow, EmbedTextsWorkflowParam{
			Texts:           chunksData.Texts,
			BatchSize:       32, // Process 32 text chunks per batch
			RequestMetadata: requestMetadata,
		}).Get(embedCtx, &embeddingVectors); err != nil {
			return handleError("generate embeddings", err)
		}

		// Build embedding records with all required fields
		embeddings := make([]repository.EmbeddingModel, len(chunksData.Chunks))
		for i, chunk := range chunksData.Chunks {
			embeddings[i] = repository.EmbeddingModel{
				SourceTable: chunksData.SourceTable,
				SourceUID:   chunk.UID,
				Vector:      embeddingVectors[i],
				KBUID:       kbUID,
				KBFileUID:   fileUID,
				FileType:    string(types.DocumentFileType), // Standard file type
				ContentType: chunk.ContentType,              // From chunk (e.g., "text", "summary")
			}
		}

		// 8c. Save embeddings to Milvus + DB using concurrent workflow
		// This child workflow deletes old embeddings once, then saves batches in parallel
		childWorkflowOptions := workflow.ChildWorkflowOptions{
			WorkflowID: fmt.Sprintf("save-embeddings-%s", fileUID.String()),
		}
		childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)

		if err := workflow.ExecuteChildWorkflow(childCtx, w.SaveEmbeddingsToVectorDBWorkflow, SaveEmbeddingsToVectorDBWorkflowParam{
			KBUID:        kbUID,
			FileUID:      fileUID,
			FileName:     chunksData.FileName,
			Embeddings:   embeddings,
			UserUID:      userUID,
			RequesterUID: requesterUID,
		}).Get(childCtx, nil); err != nil {
			return handleError("save embeddings to vector DB", err)
		}

		// 8d. Update embedding metadata
		if err := workflow.ExecuteActivity(ctx, w.UpdateEmbeddingMetadataActivity, &UpdateEmbeddingMetadataActivityParam{
			FileUID:  fileUID,
			Pipeline: pipeline.EmbedTextPipeline.Name(),
		}).Get(ctx, nil); err != nil {
			return handleError("update embedding metadata", err)
		}

		// Step 9: Update final status to COMPLETED
		if err := workflow.ExecuteActivity(ctx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
			FileUID: fileUID,
			Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED,
			Message: "File processing completed successfully",
		}).Get(ctx, nil); err != nil {
			return handleError("update final status", err)
		}

		// Mark workflow as successfully completed
		workflowCompleted = true
	}

	// Resume from CHUNKING phase (skip conversion/summarization)
	if startStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING {
		logger.Info("Resuming from CHUNKING phase",
			"fileUID", fileUID.String(),
			"userUID", userUID.String(),
			"requesterUID", requesterUID.String())

		// Get file and KB metadata for text chunking
		var metadata GetFileMetadataActivityResult
		if err := workflow.ExecuteActivity(ctx, w.GetFileMetadataActivity, &GetFileMetadataActivityParam{
			FileUID: fileUID,
			KBUID:   kbUID,
		}).Get(ctx, &metadata); err != nil {
			if strings.Contains(err.Error(), "file not found") {
				logger.Info("File not found during processing, exiting workflow gracefully",
					"fileUID", fileUID.String(),
					"userUID", userUID.String(),
					"requesterUID", requesterUID.String())
				return nil
			}
			return handleError("get file metadata", err)
		}

		// Update status to CHUNKING
		if err := workflow.ExecuteActivity(ctx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
			FileUID: fileUID,
			Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING,
		}).Get(ctx, nil); err != nil {
			logger.Warn("Failed to update status to CHUNKING", "error", err)
		}

		// Get converted file for text chunking
		var convertedFile GetConvertedFileForChunkingActivityResult
		if err := workflow.ExecuteActivity(ctx, w.GetConvertedFileForChunkingActivity, &GetConvertedFileForChunkingActivityParam{
			FileUID: fileUID,
			KBUID:   kbUID,
		}).Get(ctx, &convertedFile); err != nil {
			return handleError("get converted file for chunking", err)
		}

		// Chunk content into text chunks
		var contentChunks ChunkContentActivityResult
		if err := workflow.ExecuteActivity(ctx, w.ChunkContentActivity, &ChunkContentActivityParam{
			FileUID:   fileUID,
			KBUID:     kbUID,
			Content:   convertedFile.Content,
			Pipelines: convertedFile.ChunkingPipelines,
			Metadata:  metadata.ExternalMetadata,
		}).Get(ctx, &contentChunks); err != nil {
			return handleError("chunk content into text chunks", err)
		}

		// Get summary for text chunking (if exists)
		allChunks := contentChunks.Chunks
		if len(metadata.File.Summary) > 0 {
			var summaryChunks ChunkContentActivityResult
			if err := workflow.ExecuteActivity(ctx, w.ChunkContentActivity, &ChunkContentActivityParam{
				FileUID:   fileUID,
				KBUID:     kbUID,
				Content:   string(metadata.File.Summary),
				Pipelines: []pipeline.PipelineRelease{},
				Metadata:  metadata.ExternalMetadata,
			}).Get(ctx, &summaryChunks); err != nil {
				return handleError("chunk summary into text chunks", err)
			}
			allChunks = append(allChunks, summaryChunks.Chunks...)
		}

		// Save text chunks to DB
		if err := workflow.ExecuteActivity(ctx, w.SaveTextChunksToDBActivity, &SaveTextChunksToDBActivityParam{
			KBUID:      kbUID,
			FileUID:    fileUID,
			TextChunks: allChunks,
		}).Get(ctx, nil); err != nil {
			return handleError("save text chunks to DB", err)
		}

		// Update text chunking metadata
		if err := workflow.ExecuteActivity(ctx, w.UpdateChunkingMetadataActivity, &UpdateChunkingMetadataActivityParam{
			FileUID:          fileUID,
			KBUID:            kbUID,
			ChunkingPipeline: contentChunks.PipelineRelease.Name(),
			TextChunkCount:   uint32(len(allChunks)),
		}).Get(ctx, nil); err != nil {
			return handleError("update text chunking metadata", err)
		}

		logger.Info("CHUNKING phase completed successfully",
			"fileUID", fileUID.String(),
			"userUID", userUID.String(),
			"requesterUID", requesterUID.String(),
			"totalTextChunks", len(allChunks))

		// Update status to EMBEDDING and continue
		if err := workflow.ExecuteActivity(ctx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
			FileUID: fileUID,
			Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING,
		}).Get(ctx, nil); err != nil {
			return handleError("update status to EMBEDDING after chunking", err)
		}

		// Continue to EMBEDDING phase
		startStatus = artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING
	}

	// Resume from EMBEDDING phase (skip conversion/summarization/text chunking)
	if startStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING {
		logger.Info("Resuming from EMBEDDING phase",
			"fileUID", fileUID.String(),
			"userUID", userUID.String(),
			"requesterUID", requesterUID.String())

		// Update status to EMBEDDING
		if err := workflow.ExecuteActivity(ctx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
			FileUID: fileUID,
			Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING,
		}).Get(ctx, nil); err != nil {
			logger.Warn("Failed to update status to EMBEDDING", "error", err)
		}

		// Get text chunks from database
		var chunksData GetTextChunksForEmbeddingActivityResult
		if err := workflow.ExecuteActivity(ctx, w.GetChunksForEmbeddingActivity, &GetChunksForEmbeddingActivityParam{
			FileUID: fileUID,
		}).Get(ctx, &chunksData); err != nil {
			return handleError("get text chunks for embedding", err)
		}

		// Generate embeddings using child workflow
		var requestMetadata map[string][]string
		if chunksData.Metadata != nil {
			md, err := extractRequestMetadata(chunksData.Metadata)
			if err != nil {
				return handleError("extract request metadata", err)
			}
			requestMetadata = md
		}

		embedWorkflowOptions := workflow.ChildWorkflowOptions{
			WorkflowID: fmt.Sprintf("embed-texts-%s", fileUID.String()),
		}
		embedCtx := workflow.WithChildOptions(ctx, embedWorkflowOptions)

		var embeddingVectors [][]float32
		if err := workflow.ExecuteChildWorkflow(embedCtx, w.EmbedTextsWorkflow, EmbedTextsWorkflowParam{
			Texts:           chunksData.Texts,
			BatchSize:       32,
			RequestMetadata: requestMetadata,
		}).Get(embedCtx, &embeddingVectors); err != nil {
			return handleError("generate embeddings", err)
		}

		// Build embedding records
		embeddings := make([]repository.EmbeddingModel, len(chunksData.Chunks))
		for i, chunk := range chunksData.Chunks {
			embeddings[i] = repository.EmbeddingModel{
				SourceTable: chunksData.SourceTable,
				SourceUID:   chunk.UID,
				Vector:      embeddingVectors[i],
				KBUID:       kbUID,
				KBFileUID:   fileUID,
				FileType:    string(types.DocumentFileType),
				ContentType: chunk.ContentType,
			}
		}

		// Save embeddings to Milvus + DB
		childWorkflowOptions := workflow.ChildWorkflowOptions{
			WorkflowID: fmt.Sprintf("save-embeddings-%s", fileUID.String()),
		}
		childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)

		if err := workflow.ExecuteChildWorkflow(childCtx, w.SaveEmbeddingsToVectorDBWorkflow, SaveEmbeddingsToVectorDBWorkflowParam{
			KBUID:        kbUID,
			FileUID:      fileUID,
			FileName:     chunksData.FileName,
			Embeddings:   embeddings,
			UserUID:      userUID,
			RequesterUID: requesterUID,
		}).Get(childCtx, nil); err != nil {
			return handleError("save embeddings to vector DB", err)
		}

		// Update embedding metadata
		if err := workflow.ExecuteActivity(ctx, w.UpdateEmbeddingMetadataActivity, &UpdateEmbeddingMetadataActivityParam{
			FileUID:  fileUID,
			Pipeline: pipeline.EmbedTextPipeline.Name(),
		}).Get(ctx, nil); err != nil {
			return handleError("update embedding metadata", err)
		}

		// Update final status to COMPLETED
		if err := workflow.ExecuteActivity(ctx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
			FileUID: fileUID,
			Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED,
			Message: "File processing completed successfully",
		}).Get(ctx, nil); err != nil {
			return handleError("update final status", err)
		}

		// Mark workflow as successfully completed
		workflowCompleted = true
	}

	logger.Info("ProcessFileWorkflow completed successfully",
		"fileUID", param.FileUID,
		"userUID", userUID.String(),
		"requesterUID", requesterUID.String())
	return nil
}

// ===== CHILD WORKFLOWS =====
// The following child workflows run in parallel to optimize file processing time.

// ProcessContentWorkflowParam defines the input parameters for ProcessContentWorkflow
type ProcessContentWorkflowParam struct {
	FileUID             types.FileUIDType          // File unique identifier
	KBUID               types.KBUIDType            // Knowledge base unique identifier
	Bucket              string                     // MinIO bucket containing the file
	Destination         string                     // MinIO path to the file
	FileType            artifactpb.FileType        // Original file type
	Filename            string                     // File name for identification
	ConvertingPipelines []pipeline.PipelineRelease // Pipelines for markdown conversion (fallback)
	Metadata            *structpb.Struct           // Request metadata for authentication
	CacheName           string                     // Shared AI cache from parent workflow
	ConversionMetadata  map[string]interface{}     // Conversion metadata to be saved by parent (unused, kept for consistency)
}

// ProcessContentWorkflowResult defines the output of ProcessContentWorkflow
type ProcessContentWorkflowResult struct {
	Markdown           string                   // Converted markdown content
	Length             []uint32                 // Length of markdown sections
	PositionData       *repository.PositionData // Position metadata (e.g., page mappings)
	ConversionPipeline pipeline.PipelineRelease // Pipeline used (empty if AI was used)
	ConvertedType      artifactpb.FileType      // File type after format conversion
	OriginalType       artifactpb.FileType      // Original file type
	FormatConverted    bool                     // Whether format conversion was performed
	UsageMetadata      any                      // Token usage metadata from AI conversion (nil if pipeline was used)
}

// ProcessContentWorkflow is a child workflow that handles:
// 1. Cleanup old converted file (if exists)
// 2. Format conversion (DOCX→PDF, etc.) if needed
// 3. Markdown conversion using shared AI cache
// 4. Save converted file to DB and MinIO
//
// This workflow runs in parallel with ProcessSummaryWorkflow to optimize processing time.
// Note: Text chunking and embedding are handled by the parent workflow after both child workflows complete,
// to ensure content and summary text chunks are combined and saved together.
func (w *Worker) ProcessContentWorkflow(ctx workflow.Context, param ProcessContentWorkflowParam) (*ProcessContentWorkflowResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("ProcessContentWorkflow started",
		"fileUID", param.FileUID.String(),
		"fileType", param.FileType.String(),
		"hasCacheName", param.CacheName != "")

	// Configure activity options
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: ActivityTimeoutLong,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    RetryInitialInterval,
			BackoffCoefficient: RetryBackoffCoefficient,
			MaximumInterval:    RetryMaximumIntervalLong,
			MaximumAttempts:    RetryMaximumAttempts,
		},
	})

	result := &ProcessContentWorkflowResult{
		OriginalType: param.FileType,
	}

	// ===== PHASE 0: CLEANUP OLD CONVERTED FILE =====
	if err := workflow.ExecuteActivity(activityCtx, w.CleanupOldConvertedFileActivity, &CleanupOldConvertedFileActivityParam{
		FileUID: param.FileUID,
	}).Get(activityCtx, nil); err != nil {
		logger.Error("Failed to cleanup old converted file", "error", err)
		return nil, err
	}

	// ===== PHASE 1: FORMAT CONVERSION =====
	var convertResult ConvertFileTypeActivityResult
	if err := workflow.ExecuteActivity(activityCtx, w.ConvertFileTypeActivity, &ConvertFileTypeActivityParam{
		FileUID:     param.FileUID,
		KBUID:       param.KBUID,
		Bucket:      param.Bucket,
		Destination: param.Destination,
		FileType:    param.FileType,
		Filename:    param.Filename,
		Pipelines:   []pipeline.PipelineRelease{pipeline.ConvertFileTypePipeline},
		Metadata:    param.Metadata,
	}).Get(activityCtx, &convertResult); err != nil {
		// Format conversion failure is not fatal - continue with original file
		logger.Warn("Format conversion failed, continuing with original file", "error", err)
		convertResult.Converted = false
		convertResult.ConvertedType = param.FileType
		convertResult.OriginalType = param.FileType
	}

	result.FormatConverted = convertResult.Converted
	result.ConvertedType = convertResult.ConvertedType

	// Determine effective file location for markdown conversion
	effectiveBucket := param.Bucket
	effectiveDestination := param.Destination
	effectiveFileType := convertResult.ConvertedType

	if convertResult.Converted {
		logger.Info("File format converted successfully",
			"originalType", convertResult.OriginalType.String(),
			"convertedType", convertResult.ConvertedType.String(),
			"pipeline", convertResult.PipelineRelease.Name())
		effectiveBucket = convertResult.ConvertedBucket
		effectiveDestination = convertResult.ConvertedDestination
	}

	// Schedule cleanup of temporary converted file if conversion happened
	if convertResult.Converted && convertResult.ConvertedDestination != "" {
		defer func() {
			cleanupCtx, _ := workflow.NewDisconnectedContext(ctx)
			cleanupCtx = workflow.WithActivityOptions(cleanupCtx, workflow.ActivityOptions{
				StartToCloseTimeout: time.Minute,
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval:    time.Second,
					BackoffCoefficient: 2.0,
					MaximumInterval:    30 * time.Second,
					MaximumAttempts:    3,
				},
			})

			if err := workflow.ExecuteActivity(cleanupCtx, w.DeleteTemporaryConvertedFileActivity, &DeleteTemporaryConvertedFileActivityParam{
				Bucket:      convertResult.ConvertedBucket,
				Destination: convertResult.ConvertedDestination,
			}).Get(cleanupCtx, nil); err != nil {
				logger.Warn("Temporary file cleanup failed", "error", err)
			}
		}()
	}

	// ===== PHASE 2: MARKDOWN CONVERSION =====
	var conversionResult ConvertToFileActivityResult
	if err := workflow.ExecuteActivity(activityCtx, w.ConvertToFileActivity, &ConvertToFileActivityParam{
		Bucket:      effectiveBucket,
		Destination: effectiveDestination,
		FileType:    effectiveFileType,
		Pipelines:   param.ConvertingPipelines,
		Metadata:    param.Metadata,
		CacheName:   param.CacheName, // Use shared cache from parent
	}).Get(activityCtx, &conversionResult); err != nil {
		logger.Error("Markdown conversion failed", "error", err)
		return nil, err
	}

	result.Markdown = conversionResult.Markdown
	result.Length = conversionResult.Length
	result.PositionData = conversionResult.PositionData
	result.ConversionPipeline = conversionResult.PipelineRelease
	result.UsageMetadata = conversionResult.UsageMetadata

	logger.Info("Markdown conversion completed",
		"markdownLength", len(conversionResult.Markdown),
		"pipeline", conversionResult.PipelineRelease.Name())

	// ===== PHASE 3: SAVE CONVERTED FILE =====
	var convertedFileUID types.FileUIDType

	// For TEXT/MARKDOWN files, create a record pointing to the original file (no MinIO upload)
	fileType := param.FileType
	if fileType == artifactpb.FileType_FILE_TYPE_TEXT || fileType == artifactpb.FileType_FILE_TYPE_MARKDOWN {
		convertedFileUID, _ = uuid.NewV4()

		if err := workflow.ExecuteActivity(activityCtx, w.CreateConvertedFileRecordActivity, &CreateConvertedFileRecordActivityParam{
			KBUID:            param.KBUID,
			FileUID:          param.FileUID,
			ConvertedFileUID: convertedFileUID,
			FileName:         "converted_" + param.Filename,
			Destination:      param.Destination, // Point to original file
			PositionData:     nil,               // No position data for text files
		}).Get(activityCtx, nil); err != nil {
			logger.Error("Failed to create converted file record", "error", err)
			return nil, err
		}
	} else {
		// For other files: Create DB record -> Upload to MinIO -> Update destination
		convertedFileUID, _ = uuid.NewV4()

		// Step 1: Create DB record with placeholder destination
		if err := workflow.ExecuteActivity(activityCtx, w.CreateConvertedFileRecordActivity, &CreateConvertedFileRecordActivityParam{
			KBUID:            param.KBUID,
			FileUID:          param.FileUID,
			ConvertedFileUID: convertedFileUID,
			FileName:         "converted_" + param.Filename,
			Destination:      fmt.Sprintf("placeholder-pending-upload-%s", convertedFileUID.String()),
			PositionData:     conversionResult.PositionData,
		}).Get(activityCtx, nil); err != nil {
			logger.Error("Failed to create converted file record", "error", err)
			return nil, err
		}

		// Step 2: Upload to MinIO
		var uploadResult UploadConvertedFileToMinIOActivityResult
		if err := workflow.ExecuteActivity(activityCtx, w.UploadConvertedFileToMinIOActivity, &UploadConvertedFileToMinIOActivityParam{
			KBUID:            param.KBUID,
			FileUID:          param.FileUID,
			ConvertedFileUID: convertedFileUID,
			Content:          conversionResult.Markdown,
		}).Get(activityCtx, &uploadResult); err != nil {
			// Compensating transaction: Delete the DB record since upload failed
			logger.Warn("MinIO upload failed, deleting DB record", "error", err)
			if cleanupErr := workflow.ExecuteActivity(activityCtx, w.DeleteConvertedFileRecordActivity, &DeleteConvertedFileRecordActivityParam{
				ConvertedFileUID: convertedFileUID,
			}).Get(activityCtx, nil); cleanupErr != nil {
				logger.Error("Failed to cleanup DB record after upload failure", "error", cleanupErr)
			}
			return nil, err
		}

		// Step 3: Update DB record with actual destination
		if err := workflow.ExecuteActivity(activityCtx, w.UpdateConvertedFileDestinationActivity, &UpdateConvertedFileDestinationActivityParam{
			ConvertedFileUID: convertedFileUID,
			Destination:      uploadResult.Destination,
		}).Get(activityCtx, nil); err != nil {
			// Compensating transactions: Delete both MinIO file and DB record
			logger.Warn("Failed to update destination, cleaning up MinIO and DB", "error", err)

			// Delete MinIO file
			if cleanupErr := workflow.ExecuteActivity(activityCtx, w.DeleteConvertedFileFromMinIOActivity, &DeleteConvertedFileFromMinIOActivityParam{
				Bucket:      config.Config.Minio.BucketName,
				Destination: uploadResult.Destination,
			}).Get(activityCtx, nil); cleanupErr != nil {
				logger.Error("Failed to cleanup MinIO file", "error", cleanupErr)
			}

			// Delete DB record
			if cleanupErr := workflow.ExecuteActivity(activityCtx, w.DeleteConvertedFileRecordActivity, &DeleteConvertedFileRecordActivityParam{
				ConvertedFileUID: convertedFileUID,
			}).Get(activityCtx, nil); cleanupErr != nil {
				logger.Error("Failed to cleanup DB record", "error", cleanupErr)
			}

			return nil, err
		}
	}

	logger.Info("Converted file saved successfully", "convertedFileUID", convertedFileUID.String())

	return result, nil
}

// ProcessSummaryWorkflowParam defines the input parameters for ProcessSummaryWorkflow
type ProcessSummaryWorkflowParam struct {
	FileUID     types.FileUIDType // File unique identifier
	KBUID       types.KBUIDType   // Knowledge base unique identifier (unused, kept for consistency)
	Bucket      string            // MinIO bucket containing the file
	Destination string            // MinIO path to the file
	FileName    string            // File name for identification
	FileType    string            // File type for summarization
	Metadata    *structpb.Struct  // Request metadata for authentication
	CacheName   string            // Shared AI cache from parent workflow
}

// ProcessSummaryWorkflowResult defines the output of ProcessSummaryWorkflow
type ProcessSummaryWorkflowResult struct {
	Summary       string // Generated summary text
	Pipeline      string // Pipeline used for summary generation (empty if AI was used)
	UsageMetadata any    // Token usage metadata from AI summarization (nil if pipeline was used)
}

// ProcessSummaryWorkflow is a child workflow that handles:
// 1. Generate summary using shared AI cache (or pipeline fallback)
// 2. Save summary to PostgreSQL database
//
// This workflow runs in parallel with ProcessContentWorkflow to optimize processing time.
// The summary is stored in the database for display purposes, not for vector search.
func (w *Worker) ProcessSummaryWorkflow(ctx workflow.Context, param ProcessSummaryWorkflowParam) (*ProcessSummaryWorkflowResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("ProcessSummaryWorkflow started",
		"fileUID", param.FileUID.String(),
		"hasCacheName", param.CacheName != "")

	// Configure activity options
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: ActivityTimeoutLong,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    RetryInitialInterval,
			BackoffCoefficient: RetryBackoffCoefficient,
			MaximumInterval:    RetryMaximumIntervalLong,
			MaximumAttempts:    RetryMaximumAttempts,
		},
	})

	// ===== PHASE 1: GENERATE SUMMARY =====
	// Generate summary from the original file
	// Note: Activity fetches content directly from MinIO to avoid passing large files through Temporal
	var summaryResult GenerateSummaryActivityResult
	if err := workflow.ExecuteActivity(activityCtx, w.GenerateSummaryActivity, &GenerateSummaryActivityParam{
		FileUID:     param.FileUID,
		Bucket:      param.Bucket,
		Destination: param.Destination,
		FileName:    param.FileName,
		FileType:    param.FileType,
		Metadata:    param.Metadata,
		CacheName:   param.CacheName, // Share cache from parent workflow
	}).Get(activityCtx, &summaryResult); err != nil {
		logger.Error("Summary generation failed", "error", err)
		return nil, err
	}

	logger.Info("Summary generated successfully",
		"summaryLength", len(summaryResult.Summary),
		"pipeline", summaryResult.Pipeline)

	// ===== PHASE 2: SAVE SUMMARY TO DATABASE =====
	if err := workflow.ExecuteActivity(activityCtx, w.SaveSummaryActivity, &SaveSummaryActivityParam{
		FileUID:  param.FileUID,
		Summary:  summaryResult.Summary,
		Pipeline: summaryResult.Pipeline,
	}).Get(activityCtx, nil); err != nil {
		logger.Error("Failed to save summary to database", "error", err)
		return nil, err
	}

	return &ProcessSummaryWorkflowResult{
		Summary:       summaryResult.Summary,
		Pipeline:      summaryResult.Pipeline,
		UsageMetadata: summaryResult.UsageMetadata,
	}, nil
}
