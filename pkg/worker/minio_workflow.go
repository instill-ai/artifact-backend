package worker

import (
	"context"
	"fmt"

	"github.com/gofrs/uuid"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/instill-ai/artifact-backend/pkg/service"

	errorsx "github.com/instill-ai/x/errors"
)

type deleteFilesWorkflow struct {
	temporalClient client.Client
	worker         *Worker
}

// NewDeleteFilesWorkflow creates a new DeleteFilesWorkflow instance
func NewDeleteFilesWorkflow(temporalClient client.Client, worker *Worker) service.DeleteFilesWorkflow {
	return &deleteFilesWorkflow{
		temporalClient: temporalClient,
		worker:         worker,
	}
}

func (w *deleteFilesWorkflow) Execute(ctx context.Context, param service.DeleteFilesWorkflowParam) error {
	// Generate a unique workflow ID using UUID to prevent collisions
	workflowUUID, err := uuid.NewV4()
	if err != nil {
		return fmt.Errorf("failed to generate workflow UUID: %s", errorsx.MessageOrErr(err))
	}
	workflowID := fmt.Sprintf("delete-files-%s", workflowUUID.String())

	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	workflowRun, err := w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, w.worker.DeleteFilesWorkflow, param)
	if err != nil {
		return fmt.Errorf("failed to start delete files workflow: %s", errorsx.MessageOrErr(err))
	}

	if err = workflowRun.Get(ctx, nil); err != nil {
		return fmt.Errorf("delete files workflow failed: %s", errorsx.MessageOrErr(err))
	}

	return nil
}

type getFilesWorkflow struct {
	temporalClient client.Client
	worker         *Worker
}

// NewGetFilesWorkflow creates a new GetFilesWorkflow instance
func NewGetFilesWorkflow(temporalClient client.Client, worker *Worker) service.GetFilesWorkflow {
	return &getFilesWorkflow{
		temporalClient: temporalClient,
		worker:         worker,
	}
}

func (w *getFilesWorkflow) Execute(ctx context.Context, param service.GetFilesWorkflowParam) ([]service.FileContent, error) {
	// Generate a unique workflow ID using UUID to prevent collisions
	workflowUUID, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("failed to generate workflow UUID: %s", errorsx.MessageOrErr(err))
	}
	workflowID := fmt.Sprintf("get-files-%s", workflowUUID.String())

	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	workflowRun, err := w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, w.worker.GetFilesWorkflow, param)
	if err != nil {
		return nil, fmt.Errorf("failed to start get files workflow: %s", errorsx.MessageOrErr(err))
	}

	var results []service.FileContent
	if err = workflowRun.Get(ctx, &results); err != nil {
		return nil, fmt.Errorf("get files workflow failed: %s", errorsx.MessageOrErr(err))
	}

	return results, nil
}

// SaveChunksWorkflow orchestrates parallel saving of multiple text chunks
// Uses batched activities to reduce Temporal overhead
func (w *Worker) SaveChunksWorkflow(ctx workflow.Context, param service.SaveChunksWorkflowParam) (map[string]string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting SaveChunksWorkflow",
		"chunkCount", len(param.Chunks))

	if len(param.Chunks) == 0 {
		return make(map[string]string), nil
	}

	// Set activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: ActivityTimeoutStandard,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    RetryInitialInterval,
			BackoffCoefficient: RetryBackoffCoefficient,
			MaximumInterval:    RetryMaximumIntervalStandard,
			MaximumAttempts:    RetryMaximumAttempts,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Batch size for optimal performance (reduces activities while maintaining parallelism)
	const batchSize = 10

	// Create batches
	batches := make([]map[string][]byte, 0)
	currentBatch := make(map[string][]byte)

	for chunkUID, chunkContent := range param.Chunks {
		currentBatch[chunkUID] = chunkContent

		if len(currentBatch) >= batchSize {
			batches = append(batches, currentBatch)
			currentBatch = make(map[string][]byte)
		}
	}

	// Add remaining chunks
	if len(currentBatch) > 0 {
		batches = append(batches, currentBatch)
	}

	logger.Info("Created batches for parallel processing",
		"batchCount", len(batches),
		"batchSize", batchSize)

	// Execute batch activities in parallel
	futures := make([]workflow.Future, len(batches))
	for i, batch := range batches {
		activityParam := &SaveChunkBatchActivityParam{
			KnowledgeBaseUID: param.KnowledgeBaseUID,
			FileUID:          param.FileUID,
			Chunks:           batch,
		}

		futures[i] = workflow.ExecuteActivity(ctx, w.SaveChunkBatchActivity, activityParam)
	}

	// Wait for all batches and collect results
	destinations := make(map[string]string)
	for i, future := range futures {
		var result SaveChunkBatchActivityResult
		if err := future.Get(ctx, &result); err != nil {
			logger.Error("Batch save failed",
				"batchIndex", i,
				"error", err)
			return nil, fmt.Errorf("failed to save batch %d: %s", i, errorsx.MessageOrErr(err))
		}

		// Merge batch results
		for chunkUID, destination := range result.Destinations {
			destinations[chunkUID] = destination
		}
	}

	logger.Info("SaveChunksWorkflow completed successfully",
		"chunksSaved", len(destinations),
		"batchesProcessed", len(batches))

	return destinations, nil
}

// DeleteFilesWorkflow orchestrates parallel deletion of multiple files
func (w *Worker) DeleteFilesWorkflow(ctx workflow.Context, param service.DeleteFilesWorkflowParam) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting DeleteFilesWorkflow",
		"fileCount", len(param.FilePaths))

	if len(param.FilePaths) == 0 {
		return nil
	}

	// Set activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: ActivityTimeoutStandard,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    RetryInitialInterval,
			BackoffCoefficient: RetryBackoffCoefficient,
			MaximumInterval:    RetryMaximumIntervalStandard,
			MaximumAttempts:    RetryMaximumAttempts,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Create activities for all files
	futures := make([]workflow.Future, len(param.FilePaths))
	for i, filePath := range param.FilePaths {
		activityParam := &DeleteFileActivityParam{
			Bucket: param.Bucket,
			Path:   filePath,
		}

		futures[i] = workflow.ExecuteActivity(ctx, w.DeleteFileActivity, activityParam)
	}

	// Wait for all deletions
	for i, future := range futures {
		if err := future.Get(ctx, nil); err != nil {
			logger.Error("File deletion failed",
				"path", param.FilePaths[i],
				"error", err)
			return fmt.Errorf("failed to delete file %s: %s", param.FilePaths[i], errorsx.MessageOrErr(err))
		}
	}

	logger.Info("DeleteFilesWorkflow completed successfully",
		"filesDeleted", len(param.FilePaths))

	return nil
}

// GetFilesWorkflow orchestrates parallel retrieval of multiple files
func (w *Worker) GetFilesWorkflow(ctx workflow.Context, param service.GetFilesWorkflowParam) ([]service.FileContent, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting GetFilesWorkflow",
		"fileCount", len(param.FilePaths))

	if len(param.FilePaths) == 0 {
		return []service.FileContent{}, nil
	}

	// Set activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: ActivityTimeoutStandard,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    RetryInitialInterval,
			BackoffCoefficient: RetryBackoffCoefficient,
			MaximumInterval:    RetryMaximumIntervalStandard,
			MaximumAttempts:    RetryMaximumAttempts,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Create activities for all files
	futures := make([]workflow.Future, len(param.FilePaths))
	for i, filePath := range param.FilePaths {
		activityParam := &GetFileActivityParam{
			Bucket: param.Bucket,
			Path:   filePath,
			Index:  i,
		}

		futures[i] = workflow.ExecuteActivity(ctx, w.GetFileActivity, activityParam)
	}

	// Wait for all retrievals and collect results in order
	results := make([]service.FileContent, len(param.FilePaths))
	for _, future := range futures {
		var result GetFileActivityResult
		if err := future.Get(ctx, &result); err != nil {
			logger.Error("File retrieval failed", "error", err)
			return nil, fmt.Errorf("failed to retrieve file: %s", errorsx.MessageOrErr(err))
		}

		results[result.Index] = service.FileContent{
			Index:   result.Index,
			Name:    result.Name,
			Content: result.Content,
		}
	}

	logger.Info("GetFilesWorkflow completed successfully",
		"filesRetrieved", len(results))

	return results, nil
}
