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

	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/minio"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

type processFileWorkflow struct {
	temporalClient client.Client
}

// NewProcessFileWorkflow creates a new ProcessFileWorkflow instance
func NewProcessFileWorkflow(temporalClient client.Client) service.ProcessFileWorkflow {
	return &processFileWorkflow{temporalClient: temporalClient}
}

func (w *processFileWorkflow) Execute(ctx context.Context, param service.ProcessFileWorkflowParam) error {
	workflowID := fmt.Sprintf("process-file-%s", param.FileUID.String())
	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	_, err := w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, new(Worker).ProcessFileWorkflow, param)
	return err
}

// ProcessFileWorkflow orchestrates the file processing pipeline using a state machine approach
func (w *Worker) ProcessFileWorkflow(ctx workflow.Context, param service.ProcessFileWorkflowParam) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting ProcessFileWorkflow", "fileUID", param.FileUID.String())

	// Extract UUIDs from parameters
	fileUID := param.FileUID
	knowledgeBaseUID := param.KnowledgeBaseUID
	userUID := param.UserUID
	requesterUID := param.RequesterUID

	// Set workflow options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    100 * time.Second,
			MaximumAttempts:    3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Helper function to handle errors and update status
	handleError := func(stage string, err error) error {
		logger.Error("Failed at stage", "stage", stage, "error", err)

		// Update file status to FAILED
		statusErr := workflow.ExecuteActivity(ctx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
			FileUID: fileUID,
			Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_FAILED,
			Message: fmt.Sprintf("%s failed: %v", stage, err),
		}).Get(ctx, nil)
		if statusErr != nil {
			logger.Error("Failed to update file status to FAILED", "statusError", statusErr)
		}

		// Note: We don't clean up intermediate files on error to allow resuming from the failed step.
		// Cleanup happens automatically on reprocessing (each activity cleans up old data before creating new).

		return fmt.Errorf("failed at %s: %w", stage, err)
	}

	// Get current file status to determine starting point
	var startStatus artifactpb.FileProcessStatus
	if err := workflow.ExecuteActivity(ctx, w.GetFileStatusActivity, fileUID).Get(ctx, &startStatus); err != nil {
		return handleError("get file status", err)
	}

	// Handle different starting statuses:
	// - COMPLETED: Full reprocessing from start
	// - CONVERTING/SUMMARIZING/CHUNKING/EMBEDDING: Resume from that step (retry/reconciliation)
	// - FAILED: Don't process (requires manual intervention to set status to desired step)
	// - WAITING: Normal flow
	if startStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED {
		logger.Info("File completed, reprocessing from start",
			"fileUID", param.FileUID)
		startStatus = artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_WAITING
	}

	if startStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_FAILED {
		return fmt.Errorf("file processing previously failed - update status to desired step to retry")
	}

	// Step 1: Determine processing path based on file type
	if startStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_WAITING {
		processWaitingParam := &ProcessWaitingFileActivityParam{
			FileUID:          fileUID,
			KnowledgeBaseUID: knowledgeBaseUID,
			UserUID:          userUID,
			RequesterUID:     requesterUID,
		}
		var nextStatus artifactpb.FileProcessStatus
		if err := workflow.ExecuteActivity(ctx, w.ProcessWaitingFileActivity, processWaitingParam).Get(ctx, &nextStatus); err != nil {
			return handleError("process waiting file", err)
		}
		startStatus = nextStatus
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

		// 2d. Cleanup old converted file if exists (idempotent, won't fail workflow)
		if err := workflow.ExecuteActivity(ctx, w.CleanupOldConvertedFileActivity, &CleanupOldConvertedFileActivityParam{
			FileUID: fileUID,
		}).Get(ctx, nil); err != nil {
			logger.Warn("Failed to cleanup old converted file (continuing)", "error", err)
		}

		// 2e. Save converted file to DB + MinIO (single transaction)
		if err := workflow.ExecuteActivity(ctx, w.SaveConvertedFileActivity, &SaveConvertedFileActivityParam{
			KnowledgeBaseUID: knowledgeBaseUID,
			FileUID:          fileUID,
			FileName:         "converted_" + metadata.File.Name,
			ConversionResult: &conversionResult,
		}).Get(ctx, nil); err != nil {
			return handleError("save converted file", err)
		}

		// 2f. Update file metadata (single DB write)
		if err := workflow.ExecuteActivity(ctx, w.UpdateConversionMetadataActivity, &UpdateConversionMetadataActivityParam{
			FileUID:  fileUID,
			Length:   conversionResult.Length,
			Pipeline: conversionResult.PipelineRelease.Name(),
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
			Content:     summaryContent.Content,
			FileName:    fileMeta.File.Name,
			RequesterID: requesterUID.String(),
			Metadata:    summaryContent.Metadata,
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

		// Determine source based on file type
		var sourceTable string
		var sourceUID uuid.UUID
		var contentToChunk []byte
		var positionData *GetConvertedFileForChunkingActivityResult
		var isMarkdown bool

		fileType := fileInfo.File.Type
		switch fileType {
		case artifactpb.FileType_FILE_TYPE_PDF.String(),
			artifactpb.FileType_FILE_TYPE_DOC.String(),
			artifactpb.FileType_FILE_TYPE_DOCX.String(),
			artifactpb.FileType_FILE_TYPE_PPT.String(),
			artifactpb.FileType_FILE_TYPE_PPTX.String(),
			artifactpb.FileType_FILE_TYPE_HTML.String(),
			artifactpb.FileType_FILE_TYPE_XLSX.String(),
			artifactpb.FileType_FILE_TYPE_XLS.String(),
			artifactpb.FileType_FILE_TYPE_CSV.String():

			// 4c. Get converted file for document types
			if err := workflow.ExecuteActivity(ctx, w.GetConvertedFileForChunkingActivity, &GetConvertedFileForChunkingActivityParam{
				FileUID: fileUID,
			}).Get(ctx, &positionData); err != nil {
				return handleError("get converted file for chunking", err)
			}
			contentToChunk = positionData.Content
			sourceTable = positionData.SourceTable
			sourceUID = positionData.SourceUID
			isMarkdown = true

		default:
			// 4d. Get original file for text/markdown types
			bucket := minio.BucketFromDestination(fileInfo.File.Destination)
			var originalFile GetOriginalFileForChunkingActivityResult
			if err := workflow.ExecuteActivity(ctx, w.GetOriginalFileForChunkingActivity, &GetOriginalFileForChunkingActivityParam{
				FileUID:     fileUID,
				Bucket:      bucket,
				Destination: fileInfo.File.Destination,
				Metadata:    fileInfo.ExternalMetadata,
			}).Get(ctx, &originalFile); err != nil {
				return handleError("get original file for chunking", err)
			}
			contentToChunk = originalFile.Content
			sourceTable = originalFile.SourceTable
			sourceUID = originalFile.SourceUID
			isMarkdown = (fileType == artifactpb.FileType_FILE_TYPE_MARKDOWN.String())
		}

		// 4e. Chunk content (THE KEY EXTERNAL API CALL - expensive, benefits most from isolated retry)
		var contentChunks ChunkContentActivityResult
		if err := workflow.ExecuteActivity(ctx, w.ChunkContentActivity, &ChunkContentActivityParam{
			Content:      contentToChunk,
			IsMarkdown:   isMarkdown,
			ChunkSize:    1000,
			ChunkOverlap: 200,
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
				ChunkSize:    1000,
				ChunkOverlap: 200,
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

		// 5c. Save embeddings to Milvus + DB (combined operation for performance)
		// Use file name from chunksData (already fetched in GetChunksForEmbeddingActivity)
		if err := workflow.ExecuteActivity(ctx, w.SaveEmbeddingsToVectorDBActivity, &SaveEmbeddingsToVectorDBActivityParam{
			KnowledgeBaseUID: knowledgeBaseUID,
			FileUID:          fileUID,
			FileName:         chunksData.FileName,
			Embeddings:       embeddings,
		}).Get(ctx, nil); err != nil {
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
