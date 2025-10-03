package worker

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/instill-ai/artifact-backend/pkg/service"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

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

		// Cleanup temporary files created during failed processing
		cleanupParam := &CleanupFilesActivityParam{
			FileUID: fileUID,
			FileIDs: []string{},
		}
		cleanupErr := workflow.ExecuteActivity(ctx, w.CleanupFilesActivity, cleanupParam).Get(ctx, nil)
		if cleanupErr != nil {
			logger.Error("Failed to cleanup temporary files after error", "cleanupError", cleanupErr)
		}

		return fmt.Errorf("failed at %s: %w", stage, err)
	}

	// Get current file status to determine starting point
	var currentStatus artifactpb.FileProcessStatus
	if err := workflow.ExecuteActivity(ctx, w.GetFileStatusActivity, fileUID).Get(ctx, &currentStatus); err != nil {
		return handleError("get file status", err)
	}

	// If file is already completed or in intermediate state, this might be a reprocessing request
	// In that case, treat it as WAITING to restart the full pipeline
	if currentStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED ||
		currentStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CONVERTING ||
		currentStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_SUMMARIZING ||
		currentStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING ||
		currentStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING {
		logger.Info("File is in completed/intermediate state, resetting to WAITING for reprocessing",
			"currentStatus", currentStatus.String(),
			"fileUID", param.FileUID)
		currentStatus = artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_WAITING
	}

	// If file is already failed, don't process
	if currentStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_FAILED {
		return fmt.Errorf("file processing already failed")
	}

	// Step 1: Determine processing path based on file type (if still waiting)
	if currentStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_WAITING {
		processWaitingParam := &ProcessWaitingFileActivityParam{
			FileUID:          fileUID,
			KnowledgeBaseUID: knowledgeBaseUID,
			UserUID:          userUID,
			RequesterUID:     requesterUID,
		}
		if err := workflow.ExecuteActivity(ctx, w.ProcessWaitingFileActivity, processWaitingParam).Get(ctx, &currentStatus); err != nil {
			return handleError("process waiting file", err)
		}
	}

	// Step 2: Convert file if needed (for document types)
	if currentStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CONVERTING {
		convertParam := &ConvertFileActivityParam{
			FileUID:          fileUID,
			KnowledgeBaseUID: knowledgeBaseUID,
			UserUID:          userUID,
		}
		if err := workflow.ExecuteActivity(ctx, w.ConvertFileActivity, convertParam).Get(ctx, nil); err != nil {
			return handleError("file conversion", err)
		}
		currentStatus = artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_SUMMARIZING
	}

	// Step 3: Generate summary (if not already done)
	if currentStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_SUMMARIZING {
		generateSummaryParam := &GenerateSummaryActivityParam{
			FileUID:          fileUID,
			KnowledgeBaseUID: knowledgeBaseUID,
			UserUID:          userUID,
			RequesterUID:     requesterUID,
		}
		if err := workflow.ExecuteActivity(ctx, w.GenerateSummaryActivity, generateSummaryParam).Get(ctx, nil); err != nil {
			return handleError("summary generation", err)
		}
		currentStatus = artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING
	}

	// Step 4: Chunk file (if not already done)
	if currentStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING {
		chunkParam := &ChunkFileActivityParam{
			FileUID:          fileUID,
			KnowledgeBaseUID: knowledgeBaseUID,
			UserUID:          userUID,
			ChunkSize:        1000,
			ChunkOverlap:     200,
		}
		if err := workflow.ExecuteActivity(ctx, w.ChunkFileActivity, chunkParam).Get(ctx, nil); err != nil {
			return handleError("file chunking", err)
		}
		currentStatus = artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING
	}

	// Step 5: Generate embeddings (if not already done)
	if currentStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING {
		embedParam := &EmbedFileActivityParam{
			FileUID:          fileUID,
			KnowledgeBaseUID: knowledgeBaseUID,
			UserUID:          userUID,
			EmbeddingModel:   "text-embedding-ada-002",
		}
		if err := workflow.ExecuteActivity(ctx, w.EmbedFileActivity, embedParam).Get(ctx, nil); err != nil {
			return handleError("file embedding", err)
		}
		currentStatus = artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED
	}

	// Step 6: Update final status
	if err := workflow.ExecuteActivity(ctx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
		FileUID: fileUID,
		Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED,
		Message: "File processing completed successfully",
	}).Get(ctx, nil); err != nil {
		return handleError("update final status", err)
	}

	logger.Info("ProcessFileWorkflow completed successfully", "fileUID", param.FileUID)
	return nil
}
