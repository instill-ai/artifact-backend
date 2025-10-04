package worker

import (
	"context"
	"fmt"
	"time"

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
	workflowID := fmt.Sprintf("delete-files-%d-%d", time.Now().UnixNano(), len(param.FilePaths))
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
	workflowID := fmt.Sprintf("get-files-%d-%d", time.Now().UnixNano(), len(param.FilePaths))
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
func (w *Worker) SaveChunksWorkflow(ctx workflow.Context, param service.SaveChunksWorkflowParam) (map[string]string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting SaveChunksWorkflow",
		"chunkCount", len(param.Chunks))

	if len(param.Chunks) == 0 {
		return make(map[string]string), nil
	}

	// Set activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Create activities for all chunks
	type chunkFuture struct {
		chunkUID string
		future   workflow.Future
	}
	futures := make([]chunkFuture, 0, len(param.Chunks))

	for chunkUID, chunkContent := range param.Chunks {
		activityParam := &SaveChunkActivityParam{
			KnowledgeBaseUID: param.KnowledgeBaseUID,
			FileUID:          param.FileUID,
			ChunkUID:         chunkUID,
			ChunkContent:     chunkContent,
		}

		future := workflow.ExecuteActivity(ctx, w.SaveChunkActivity, activityParam)
		futures = append(futures, chunkFuture{
			chunkUID: chunkUID,
			future:   future,
		})
	}

	// Wait for all activities and collect results
	destinations := make(map[string]string)
	for _, cf := range futures {
		var result SaveChunkActivityResult
		if err := cf.future.Get(ctx, &result); err != nil {
			logger.Error("Chunk save failed",
				"chunkUID", cf.chunkUID,
				"error", err)
			return nil, fmt.Errorf("failed to save chunk %s: %s", cf.chunkUID, errorsx.MessageOrErr(err))
		}

		destinations[result.ChunkUID] = result.Destination
	}

	logger.Info("SaveChunksWorkflow completed successfully",
		"chunksSaved", len(destinations))

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
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    3,
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
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    3,
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
