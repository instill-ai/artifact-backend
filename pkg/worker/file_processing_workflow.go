package worker

import (
	"context"
	"fmt"
	"strings"

	"github.com/gofrs/uuid"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/minio"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

type processFileWorkflow struct {
	temporalClient client.Client
	worker         *Worker
}

// NewProcessFileWorkflow creates a new ProcessFileWorkflow instance
func NewProcessFileWorkflow(temporalClient client.Client, worker *Worker) service.ProcessFileWorkflow {
	return &processFileWorkflow{
		temporalClient: temporalClient,
		worker:         worker,
	}
}

func (w *processFileWorkflow) Execute(ctx context.Context, param service.ProcessFileWorkflowParam) error {
	workflowID := fmt.Sprintf("process-file-%s", param.FileUID.String())
	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	_, err := w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, w.worker.ProcessFileWorkflow, param)
	return err
}

// ProcessFileWorkflow orchestrates the file processing pipeline using a state machine approach
func (w *Worker) ProcessFileWorkflow(ctx workflow.Context, param service.ProcessFileWorkflowParam) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting ProcessFileWorkflow", "fileUID", param.FileUID.String())

	// Extract UUIDs from parameters
	fileUID := param.FileUID
	knowledgeBaseUID := param.KnowledgeBaseUID

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
			// If we can't update the status, still return the original error
			// to mark the workflow as failed in Temporal
		} else {
			logger.Info("File marked as FAILED, workflow will be marked as failed", "stage", stage)
		}

		// Note: We don't clean up intermediate files on error to allow resuming from the failed step.
		// Cleanup happens automatically on reprocessing (each activity cleans up old data before creating new).

		// Return the error to mark the workflow as Failed in Temporal.
		// Temporal's retry policy (MaximumAttempts: 3) will be respected.
		// This ensures workflow status matches file status.
		return fmt.Errorf("%s: %s", stage, errMsg)
	}

	// Get current file status to determine starting point
	var startStatus artifactpb.FileProcessStatus
	if err := workflow.ExecuteActivity(ctx, w.GetFileStatusActivity, fileUID).Get(ctx, &startStatus); err != nil {
		return handleError("get file status", err)
	}

	// Handle different starting statuses:
	// - COMPLETED: Full reprocessing from start (go to CONVERTING)
	// - FAILED: Full reprocessing from start (go to CONVERTING)
	// - CONVERTING/SUMMARIZING/CHUNKING/EMBEDDING: Resume from that step (retry/reconciliation)
	if startStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED {
		logger.Info("File completed or waiting, starting at CONVERTING",
			"fileUID", param.FileUID,
			"previousStatus", startStatus.String())
		startStatus = artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CONVERTING
	}

	if startStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_FAILED {
		logger.Info("File previously failed, restarting from CONVERTING",
			"fileUID", param.FileUID,
			"previousStatus", startStatus.String())
		startStatus = artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CONVERTING
	}

	// Process file through pipeline steps using fallthrough pattern
	switch startStatus {
	case artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CONVERTING:
		// Step 2: Convert file
		logger.Info("Starting CONVERSION phase", "fileUID", fileUID.String())

		// Update status to CONVERTING
		if err := workflow.ExecuteActivity(ctx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
			FileUID: fileUID,
			Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CONVERTING,
		}).Get(ctx, nil); err != nil {
			logger.Warn("Failed to update status to CONVERTING", "error", err)
		}

		// 2a. Get file and KB metadata (single DB read - fast, cheap to retry)
		var metadata GetFileMetadataActivityResult
		if err := workflow.ExecuteActivity(ctx, w.GetFileMetadataActivity, &GetFileMetadataActivityParam{
			FileUID:          fileUID,
			KnowledgeBaseUID: knowledgeBaseUID,
		}).Get(ctx, &metadata); err != nil {
			// If file not found, it may have been deleted - exit gracefully
			if strings.Contains(err.Error(), "file not found") {
				logger.Info("File not found during conversion, exiting workflow gracefully",
					"fileUID", fileUID.String())
				return nil
			}
			return handleError("get file metadata", err)
		}

		// 2b. Get file content from MinIO (single MinIO read - fast, cheap to retry)
		var fileContent []byte
		bucket := minio.BucketFromDestination(metadata.File.Destination)
		if err := workflow.ExecuteActivity(ctx, w.GetFileContentActivity, &GetFileContentActivityParam{
			Bucket:      bucket,
			Destination: metadata.File.Destination,
			Metadata:    metadata.ExternalMetadata,
		}).Get(ctx, &fileContent); err != nil {
			return handleError("get file content", err)
		}

		// 2c. Convert to Markdown (THE KEY EXTERNAL API CALL - expensive, benefits most from isolated retry)
		var conversionResult ConvertToMarkdownActivityResult
		if err := workflow.ExecuteActivity(ctx, w.ConvertToMarkdownActivity, &ConvertToMarkdownActivityParam{
			Content:   fileContent,
			FileType:  artifactpb.FileType(artifactpb.FileType_value[metadata.File.Type]),
			Pipelines: metadata.ConvertingPipelines,
			Metadata:  metadata.ExternalMetadata,
		}).Get(ctx, &conversionResult); err != nil {
			return handleError("convert to markdown", err)
		}

		// 2d. Cleanup old converted file if exists
		// Note: Activity is idempotent - succeeds if no file exists, fails only on real errors (network, etc.)
		if err := workflow.ExecuteActivity(ctx, w.CleanupOldConvertedFileActivity, &CleanupOldConvertedFileActivityParam{
			FileUID: fileUID,
		}).Get(ctx, nil); err != nil {
			return handleError("cleanup old converted file", err)
		}

		// 2e. Save converted file using separate activities to decouple DB and MinIO operations
		fileType := artifactpb.FileType(artifactpb.FileType_value[metadata.File.Type])

		// For TEXT/MARKDOWN files, create a record pointing to the original file (no MinIO upload)
		if fileType == artifactpb.FileType_FILE_TYPE_TEXT || fileType == artifactpb.FileType_FILE_TYPE_MARKDOWN {
			logger.Info("Creating DB record for TEXT/MARKDOWN file")

			// Generate UUID for the converted file
			convertedFileUID, _ := uuid.NewV4()

			if err := workflow.ExecuteActivity(ctx, w.CreateConvertedFileRecordActivity, &CreateConvertedFileRecordActivityParam{
				KnowledgeBaseUID: knowledgeBaseUID,
				FileUID:          fileUID,
				ConvertedFileUID: convertedFileUID,
				FileName:         "converted_" + metadata.File.Name,
				Destination:      metadata.File.Destination, // Point to original file
				PositionData:     nil,                       // No position data for text files
			}).Get(ctx, nil); err != nil {
				return handleError("create converted file record", err)
			}
		} else {
			// For other files: Create DB record -> Upload to MinIO -> Update destination
			// This properly decouples operations with compensating transactions
			logger.Info("Saving converted file with separate DB and MinIO operations")

			// Step 1: Generate UUID for the converted file
			convertedFileUID, _ := uuid.NewV4()

			// Step 2: Create DB record with placeholder destination
			if err := workflow.ExecuteActivity(ctx, w.CreateConvertedFileRecordActivity, &CreateConvertedFileRecordActivityParam{
				KnowledgeBaseUID: knowledgeBaseUID,
				FileUID:          fileUID,
				ConvertedFileUID: convertedFileUID,
				FileName:         "converted_" + metadata.File.Name,
				Destination:      fmt.Sprintf("placeholder-pending-upload-%s", convertedFileUID.String()), // Unique placeholder per file
				PositionData:     conversionResult.PositionData,
			}).Get(ctx, nil); err != nil {
				return handleError("create converted file record", err)
			}

			// Step 3: Upload to MinIO
			var uploadResult UploadConvertedFileToMinIOActivityResult
			if err := workflow.ExecuteActivity(ctx, w.UploadConvertedFileToMinIOActivity, &UploadConvertedFileToMinIOActivityParam{
				KnowledgeBaseUID: knowledgeBaseUID,
				FileUID:          fileUID,
				ConvertedFileUID: convertedFileUID,
				Content:          conversionResult.Markdown,
			}).Get(ctx, &uploadResult); err != nil {
				// Compensating transaction: Delete the DB record since upload failed
				logger.Warn("MinIO upload failed, deleting DB record", "error", err)
				if cleanupErr := workflow.ExecuteActivity(ctx, w.DeleteConvertedFileRecordActivity, &DeleteConvertedFileRecordActivityParam{
					ConvertedFileUID: convertedFileUID,
				}).Get(ctx, nil); cleanupErr != nil {
					logger.Error("Failed to cleanup DB record after upload failure", "error", cleanupErr)
				}
				return handleError("upload converted file to MinIO", err)
			}

			// Step 4: Update DB record with actual destination
			if err := workflow.ExecuteActivity(ctx, w.UpdateConvertedFileDestinationActivity, &UpdateConvertedFileDestinationActivityParam{
				ConvertedFileUID: convertedFileUID,
				Destination:      uploadResult.Destination,
			}).Get(ctx, nil); err != nil {
				// Compensating transactions: Delete both MinIO file and DB record
				logger.Warn("Failed to update destination, cleaning up MinIO and DB", "error", err)

				// Delete MinIO file
				if cleanupErr := workflow.ExecuteActivity(ctx, w.DeleteConvertedFileFromMinIOActivity, &DeleteConvertedFileFromMinIOActivityParam{
					Bucket:      config.Config.Minio.BucketName,
					Destination: uploadResult.Destination,
				}).Get(ctx, nil); cleanupErr != nil {
					logger.Error("Failed to cleanup MinIO file after destination update failure", "error", cleanupErr)
				}

				// Delete DB record
				if cleanupErr := workflow.ExecuteActivity(ctx, w.DeleteConvertedFileRecordActivity, &DeleteConvertedFileRecordActivityParam{
					ConvertedFileUID: convertedFileUID,
				}).Get(ctx, nil); cleanupErr != nil {
					logger.Error("Failed to cleanup DB record after destination update failure", "error", cleanupErr)
				}

				return handleError("update converted file destination", err)
			}
		}

		// 2f. Update file metadata (single DB write)
		// For TEXT/MARKDOWN files, don't store pipeline info since no pipeline was used
		pipelineName := ""
		if conversionResult.PipelineRelease.ID != "" {
			pipelineName = conversionResult.PipelineRelease.Name()
		}
		if err := workflow.ExecuteActivity(ctx, w.UpdateConversionMetadataActivity, &UpdateConversionMetadataActivityParam{
			FileUID:  fileUID,
			Length:   conversionResult.Length,
			Pipeline: pipelineName,
		}).Get(ctx, nil); err != nil {
			return handleError("update conversion metadata", err)
		}

		logger.Info("CONVERSION phase completed successfully", "fileUID", fileUID.String())
		fallthrough

	case artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_SUMMARIZING:
		// Step 3: Generate summary
		logger.Info("Starting SUMMARY phase", "fileUID", fileUID.String())

		// Update status to SUMMARIZING
		if err := workflow.ExecuteActivity(ctx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
			FileUID: fileUID,
			Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_SUMMARIZING,
		}).Get(ctx, nil); err != nil {
			logger.Warn("Failed to update status to SUMMARIZING", "error", err)
		}

		// 3a. Get file metadata to determine file type and location
		var fileMeta GetFileMetadataActivityResult
		if err := workflow.ExecuteActivity(ctx, w.GetFileMetadataActivity, &GetFileMetadataActivityParam{
			FileUID:          fileUID,
			KnowledgeBaseUID: knowledgeBaseUID,
		}).Get(ctx, &fileMeta); err != nil {
			// If file not found, it may have been deleted - exit gracefully
			if strings.Contains(err.Error(), "file not found") {
				logger.Info("File not found during summary generation, exiting workflow gracefully",
					"fileUID", fileUID.String())
				return nil
			}
			return handleError("get file metadata for summary", err)
		}

		// 3b. Get content for summarization (DB read + MinIO read)
		bucket := minio.BucketFromDestination(fileMeta.File.Destination)
		var summaryContent GetFileContentForSummaryActivityResult
		if err := workflow.ExecuteActivity(ctx, w.GetFileContentForSummaryActivity, &GetFileContentForSummaryActivityParam{
			FileUID:     fileUID,
			Bucket:      bucket,
			Destination: fileMeta.File.Destination,
			FileType:    fileMeta.File.Type,
			Metadata:    fileMeta.ExternalMetadata,
		}).Get(ctx, &summaryContent); err != nil {
			return handleError("get content for summary", err)
		}

		// 3c. Generate summary (THE KEY EXTERNAL API CALL - expensive, benefits most from isolated retry)
		var summaryResult GenerateSummaryFromPipelineActivityResult
		if err := workflow.ExecuteActivity(ctx, w.GenerateSummaryFromPipelineActivity, &GenerateSummaryFromPipelineActivityParam{
			Content:  summaryContent.Content,
			FileName: fileMeta.File.Name,
			FileType: fileMeta.File.Type,
			Metadata: summaryContent.Metadata,
		}).Get(ctx, &summaryResult); err != nil {
			return handleError("generate summary", err)
		}

		// 3d. Save summary to database (single DB write)
		if err := workflow.ExecuteActivity(ctx, w.SaveSummaryActivity, &SaveSummaryActivityParam{
			FileUID:  fileUID,
			Summary:  summaryResult.Summary,
			Pipeline: summaryResult.Pipeline,
		}).Get(ctx, nil); err != nil {
			return handleError("save summary", err)
		}

		logger.Info("SUMMARY phase completed successfully", "fileUID", fileUID.String())
		fallthrough

	case artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING:
		// Step 4: Chunk file
		logger.Info("Starting CHUNKING phase", "fileUID", fileUID.String())

		// Update status to CHUNKING
		if err := workflow.ExecuteActivity(ctx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
			FileUID: fileUID,
			Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING,
		}).Get(ctx, nil); err != nil {
			logger.Warn("Failed to update status to CHUNKING", "error", err)
		}

		// 4a. Get file metadata to determine if we need converted file or original
		var fileInfo GetFileMetadataActivityResult
		if err := workflow.ExecuteActivity(ctx, w.GetFileMetadataActivity, &GetFileMetadataActivityParam{
			FileUID:          fileUID,
			KnowledgeBaseUID: knowledgeBaseUID,
		}).Get(ctx, &fileInfo); err != nil {
			// If file not found, it may have been deleted - exit gracefully
			if strings.Contains(err.Error(), "file not found") {
				logger.Info("File not found during chunking, exiting workflow gracefully",
					"fileUID", fileUID.String())
				return nil
			}
			return handleError("get file metadata for chunking", err)
		}

		// 4b. Get converted file - ALL files now have a converted file record
		// (TEXT/MARKDOWN files have a record pointing to the original file)
		var convertedFile GetConvertedFileForChunkingActivityResult
		if err := workflow.ExecuteActivity(ctx, w.GetConvertedFileForChunkingActivity, &GetConvertedFileForChunkingActivityParam{
			FileUID: fileUID,
		}).Get(ctx, &convertedFile); err != nil {
			return handleError("get converted file for chunking", err)
		}

		contentToChunk := convertedFile.Content
		sourceTable := convertedFile.SourceTable
		sourceUID := convertedFile.SourceUID
		positionData := &convertedFile
		fileType := fileInfo.File.Type

		// 4e. Chunk content (THE KEY EXTERNAL API CALL - expensive, benefits most from isolated retry)
		// All converted files are markdown (or treated as such)
		var contentChunks ChunkContentActivityResult
		if err := workflow.ExecuteActivity(ctx, w.ChunkContentActivity, &ChunkContentActivityParam{
			Content:      contentToChunk,
			IsMarkdown:   true, // All converted files are markdown
			ChunkSize:    TextChunkSize,
			ChunkOverlap: TextChunkOverlap,
			Metadata:     fileInfo.ExternalMetadata,
		}).Get(ctx, &contentChunks); err != nil {
			return handleError("chunk content", err)
		}

		// Add page references if we have position data from converted file
		// Note: Page references are stored in the chunk metadata during SaveChunksToDBActivity
		_ = positionData // Keep for future use

		// 4f. Chunk summary text (external API call)
		summaryBytes := []byte(fileInfo.File.Summary)
		var summaryChunks ChunkContentActivityResult
		if len(summaryBytes) > 0 {
			if err := workflow.ExecuteActivity(ctx, w.ChunkContentActivity, &ChunkContentActivityParam{
				Content:      summaryBytes,
				IsMarkdown:   false, // Summary is plain text
				ChunkSize:    TextChunkSize,
				ChunkOverlap: TextChunkOverlap,
				Metadata:     fileInfo.ExternalMetadata,
			}).Get(ctx, &summaryChunks); err != nil {
				return handleError("chunk summary", err)
			}
		}

		// 4f. Save chunks to DB with placeholders (single DB transaction)
		var dbResult SaveChunksToDBActivityResult
		if err := workflow.ExecuteActivity(ctx, w.SaveChunksToDBActivity, &SaveChunksToDBActivityParam{
			KnowledgeBaseUID: knowledgeBaseUID,
			FileUID:          fileUID,
			SourceUID:        sourceUID,
			SourceTable:      sourceTable,
			SummaryChunks:    summaryChunks.Chunks,
			ContentChunks:    contentChunks.Chunks,
			FileType:         fileType,
		}).Get(ctx, &dbResult); err != nil {
			return handleError("save chunks to DB", err)
		}

		// 4g. Save chunks to MinIO using child workflow (parallel I/O)
		if len(dbResult.ChunksToSave) > 0 {
			childWorkflowOptions := workflow.ChildWorkflowOptions{
				WorkflowID: fmt.Sprintf("save-chunks-%s", fileUID.String()),
			}
			childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)

			saveChunksParam := service.SaveChunksWorkflowParam{
				KnowledgeBaseUID: knowledgeBaseUID,
				FileUID:          fileUID,
				Chunks:           dbResult.ChunksToSave,
			}
			var destinations map[string]string
			if err := workflow.ExecuteChildWorkflow(childCtx, w.SaveChunksWorkflow, saveChunksParam).Get(ctx, &destinations); err != nil {
				return handleError("save chunks to MinIO", err)
			}

			// 4i. Update chunk destinations in database (single DB write)
			if err := workflow.ExecuteActivity(ctx, w.UpdateChunkDestinationsActivity, &UpdateChunkDestinationsActivityParam{
				Destinations: destinations,
			}).Get(ctx, nil); err != nil {
				return handleError("update chunk destinations", err)
			}
		}

		// 4j. Update file metadata (single DB write)
		if err := workflow.ExecuteActivity(ctx, w.UpdateChunkingMetadataActivity, &UpdateChunkingMetadataActivityParam{
			FileUID:  fileUID,
			Pipeline: service.ChunkTextPipeline.Name(),
		}).Get(ctx, nil); err != nil {
			return handleError("update chunking metadata", err)
		}

		logger.Info("CHUNKING phase completed successfully", "fileUID", fileUID.String())
		fallthrough

	case artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING:
		// Step 5: Generate embeddings
		logger.Info("Starting EMBEDDING phase", "fileUID", fileUID.String())

		// Update status to EMBEDDING
		if err := workflow.ExecuteActivity(ctx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
			FileUID: fileUID,
			Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING,
		}).Get(ctx, nil); err != nil {
			logger.Warn("Failed to update status to EMBEDDING", "error", err)
		}

		// 5a. Get chunks from database (single DB read + service call)
		var chunksData GetChunksForEmbeddingActivityResult
		if err := workflow.ExecuteActivity(ctx, w.GetChunksForEmbeddingActivity, &GetChunksForEmbeddingActivityParam{
			FileUID: fileUID,
		}).Get(ctx, &chunksData); err != nil {
			return handleError("get chunks for embedding", err)
		}

		// 5b. Generate embeddings (THE KEY EXTERNAL API CALL - expensive, benefits most from isolated retry)
		var embeddingResult GenerateEmbeddingsActivityResult
		if err := workflow.ExecuteActivity(ctx, w.GenerateEmbeddingsActivity, &GenerateEmbeddingsActivityParam{
			Texts:    chunksData.Texts,
			Metadata: chunksData.Metadata,
		}).Get(ctx, &embeddingResult); err != nil {
			return handleError("generate embeddings", err)
		}

		// Build embedding records with all required fields
		embeddings := make([]repository.Embedding, len(chunksData.Chunks))
		for i, chunk := range chunksData.Chunks {
			embeddings[i] = repository.Embedding{
				SourceTable: chunksData.SourceTable,
				SourceUID:   chunk.UID,
				Vector:      embeddingResult.Embeddings[i],
				KbUID:       knowledgeBaseUID,
				KbFileUID:   fileUID,
				FileType:    string(constant.DocumentFileType), // Standard file type
				ContentType: chunk.ContentType,                 // From chunk (e.g., "text", "summary")
			}
		}

		// 5c. Save embeddings to Milvus + DB using concurrent workflow
		// This child workflow deletes old embeddings once, then saves batches in parallel
		// Use file name from chunksData (already fetched in GetChunksForEmbeddingActivity)
		childWorkflowOptions := workflow.ChildWorkflowOptions{
			WorkflowID: fmt.Sprintf("save-embeddings-%s", fileUID.String()),
		}
		childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)

		if err := workflow.ExecuteChildWorkflow(childCtx, w.SaveEmbeddingsToVectorDBWorkflow, SaveEmbeddingsToVectorDBWorkflowParam{
			KnowledgeBaseUID: knowledgeBaseUID,
			FileUID:          fileUID,
			FileName:         chunksData.FileName,
			Embeddings:       embeddings,
		}).Get(childCtx, nil); err != nil {
			return handleError("save embeddings to vector DB", err)
		}

		// 5d. Update file metadata (single DB write)
		if err := workflow.ExecuteActivity(ctx, w.UpdateEmbeddingMetadataActivity, &UpdateEmbeddingMetadataActivityParam{
			FileUID:  fileUID,
			Pipeline: service.EmbedTextPipeline.Name(),
		}).Get(ctx, nil); err != nil {
			return handleError("update embedding metadata", err)
		}

		logger.Info("EMBEDDING phase completed successfully", "fileUID", fileUID.String())

		// Step 6: Update final status to COMPLETED
		if err := workflow.ExecuteActivity(ctx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
			FileUID: fileUID,
			Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED,
			Message: "File processing completed successfully",
		}).Get(ctx, nil); err != nil {
			return handleError("update final status", err)
		}
	}

	logger.Info("ProcessFileWorkflow completed successfully", "fileUID", param.FileUID)
	return nil
}
