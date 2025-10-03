package worker

import (
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/instill-ai/artifact-backend/pkg/service"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// ProcessFileWorkflow orchestrates the file processing pipeline using a state machine approach
func (w *Worker) ProcessFileWorkflow(ctx workflow.Context, param service.ProcessFileWorkflowParam) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting ProcessFileWorkflow", "fileUID", param.FileUID)

	// Parse UUIDs from strings
	fileUID, err := uuid.FromString(param.FileUID)
	if err != nil {
		return fmt.Errorf("invalid file UID: %w", err)
	}
	knowledgeBaseUID, err := uuid.FromString(param.KnowledgeBaseUID)
	if err != nil {
		return fmt.Errorf("invalid knowledge base UID: %w", err)
	}
	userUID, err := uuid.FromString(param.UserUID)
	if err != nil {
		return fmt.Errorf("invalid user UID: %w", err)
	}
	requesterUID, err := uuid.FromString(param.RequesterUID)
	if err != nil {
		return fmt.Errorf("invalid requester UID: %w", err)
	}

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
	err = workflow.ExecuteActivity(ctx, w.GetFileStatusActivity, fileUID).Get(ctx, &currentStatus)
	if err != nil {
		return handleError("get file status", err)
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
		err = workflow.ExecuteActivity(ctx, w.ProcessWaitingFileActivity, processWaitingParam).Get(ctx, &currentStatus)
		if err != nil {
			return handleError("process waiting file", err)
		}
	}

	// Step 2: Convert file if needed (for document types)
	if currentStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CONVERTING {
		convertParam := &ConvertFileActivityParam{
			FileUID:          fileUID,
			KnowledgeBaseUID: knowledgeBaseUID,
			UserUID:          userUID,
			ConversionType:   "markdown",
		}
		err = workflow.ExecuteActivity(ctx, w.ConvertFileActivity, convertParam).Get(ctx, nil)
		if err != nil {
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
		err = workflow.ExecuteActivity(ctx, w.GenerateSummaryActivity, generateSummaryParam).Get(ctx, nil)
		if err != nil {
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
		err = workflow.ExecuteActivity(ctx, w.ChunkFileActivity, chunkParam).Get(ctx, nil)
		if err != nil {
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
		err = workflow.ExecuteActivity(ctx, w.EmbedFileActivity, embedParam).Get(ctx, nil)
		if err != nil {
			return handleError("file embedding", err)
		}
		currentStatus = artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED
	}

	// Step 6: Update final status and notify
	err = workflow.ExecuteActivity(ctx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
		FileUID: fileUID,
		Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED,
		Message: "File processing completed successfully",
	}).Get(ctx, nil)
	if err != nil {
		return handleError("update final status", err)
	}

	// Step 7: Send notification (non-critical)
	notifyParam := &NotifyFileProcessedActivityParam{
		FileUID:          fileUID,
		KnowledgeBaseUID: knowledgeBaseUID,
		UserUID:          userUID,
		Status:           artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED,
	}
	err = workflow.ExecuteActivity(ctx, w.NotifyFileProcessedActivity, notifyParam).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to notify file processed (non-critical)", "error", err)
	}

	logger.Info("ProcessFileWorkflow completed successfully", "fileUID", param.FileUID)
	return nil
}
