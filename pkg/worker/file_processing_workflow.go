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
		// Step 2: Convert file (for document types)
		convertParam := &ConvertFileActivityParam{
			FileUID:          fileUID,
			KnowledgeBaseUID: knowledgeBaseUID,
			UserUID:          userUID,
		}
		if err := workflow.ExecuteActivity(ctx, w.ConvertFileActivity, convertParam).Get(ctx, nil); err != nil {
			return handleError("file conversion", err)
		}
		fallthrough

	case artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_SUMMARIZING:
		// Step 3: Generate summary
		generateSummaryParam := &GenerateSummaryActivityParam{
			FileUID:          fileUID,
			KnowledgeBaseUID: knowledgeBaseUID,
			UserUID:          userUID,
			RequesterUID:     requesterUID,
		}
		if err := workflow.ExecuteActivity(ctx, w.GenerateSummaryActivity, generateSummaryParam).Get(ctx, nil); err != nil {
			return handleError("summary generation", err)
		}
		fallthrough

	case artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING:
		// Step 4: Chunk file
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
		fallthrough

	case artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING:
		// Step 5: Generate embeddings
		embedParam := &EmbedFileActivityParam{
			FileUID:          fileUID,
			KnowledgeBaseUID: knowledgeBaseUID,
			UserUID:          userUID,
			EmbeddingModel:   "text-embedding-ada-002",
		}
		if err := workflow.ExecuteActivity(ctx, w.EmbedFileActivity, embedParam).Get(ctx, nil); err != nil {
			return handleError("file embedding", err)
		}

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
