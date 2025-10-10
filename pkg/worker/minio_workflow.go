package worker

import (
	"context"
	"fmt"

	"github.com/gofrs/uuid"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	errorsx "github.com/instill-ai/x/errors"
)

// DeleteFilesWorkflowParam defines the parameters for the DeleteFilesWorkflow
type DeleteFilesWorkflowParam struct {
	Bucket    string
	FilePaths []string
}

// GetFilesWorkflowParam defines the parameters for the GetFilesWorkflow
type GetFilesWorkflowParam struct {
	Bucket    string
	FilePaths []string
}

// FileContent represents file content with metadata
type FileContent struct {
	Index   int
	Name    string
	Content []byte
}

type deleteFilesWorkflow struct {
	temporalClient client.Client
	worker         *Worker
}

// NewDeleteFilesWorkflow creates a new DeleteFilesWorkflow instance
func NewDeleteFilesWorkflow(temporalClient client.Client, worker *Worker) *deleteFilesWorkflow {
	return &deleteFilesWorkflow{
		temporalClient: temporalClient,
		worker:         worker,
	}
}

func (w *deleteFilesWorkflow) Execute(ctx context.Context, param DeleteFilesWorkflowParam) error {
	// Generate a unique workflow ID using UUID to prevent collisions
	workflowUUID, err := uuid.NewV4()
	if err != nil {
		return errorsx.AddMessage(err, "Unable to generate workflow identifier. Please try again.")
	}
	workflowID := fmt.Sprintf("delete-files-%s", workflowUUID.String())

	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	workflowRun, err := w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, w.worker.DeleteFilesWorkflow, param)
	if err != nil {
		return errorsx.AddMessage(err, "Unable to start file deletion workflow. Please try again.")
	}

	if err = workflowRun.Get(ctx, nil); err != nil {
		return errorsx.AddMessage(err, "File deletion workflow failed. Please try again.")
	}

	return nil
}

type getFilesWorkflow struct {
	temporalClient client.Client
	worker         *Worker
}

// NewGetFilesWorkflow creates a new GetFilesWorkflow instance
func NewGetFilesWorkflow(temporalClient client.Client, worker *Worker) *getFilesWorkflow {
	return &getFilesWorkflow{
		temporalClient: temporalClient,
		worker:         worker,
	}
}

func (w *getFilesWorkflow) Execute(ctx context.Context, param GetFilesWorkflowParam) ([]FileContent, error) {
	// Generate a unique workflow ID using UUID to prevent collisions
	workflowUUID, err := uuid.NewV4()
	if err != nil {
		return nil, errorsx.AddMessage(err, "Unable to generate workflow identifier. Please try again.")
	}
	workflowID := fmt.Sprintf("get-files-%s", workflowUUID.String())

	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	workflowRun, err := w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, w.worker.GetFilesWorkflow, param)
	if err != nil {
		return nil, errorsx.AddMessage(err, "Unable to start file retrieval workflow. Please try again.")
	}

	var results []FileContent
	if err = workflowRun.Get(ctx, &results); err != nil {
		return nil, errorsx.AddMessage(err, "File retrieval workflow failed. Please try again.")
	}

	return results, nil
}

// DeleteFilesWorkflow orchestrates parallel deletion of multiple files
func (w *Worker) DeleteFilesWorkflow(ctx workflow.Context, param DeleteFilesWorkflowParam) error {
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
			return errorsx.AddMessage(err, fmt.Sprintf("Unable to delete file: %s. Please try again.", param.FilePaths[i]))
		}
	}

	logger.Info("DeleteFilesWorkflow completed successfully",
		"filesDeleted", len(param.FilePaths))

	return nil
}

// GetFilesWorkflow orchestrates parallel retrieval of multiple files
func (w *Worker) GetFilesWorkflow(ctx workflow.Context, param GetFilesWorkflowParam) ([]FileContent, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting GetFilesWorkflow",
		"fileCount", len(param.FilePaths))

	if len(param.FilePaths) == 0 {
		return []FileContent{}, nil
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
	results := make([]FileContent, len(param.FilePaths))
	for _, future := range futures {
		var result GetFileActivityResult
		if err := future.Get(ctx, &result); err != nil {
			logger.Error("File retrieval failed", "error", err)
			return nil, errorsx.AddMessage(err, "Unable to retrieve file. Please try again.")
		}

		results[result.Index] = FileContent(result)
	}

	logger.Info("GetFilesWorkflow completed successfully",
		"filesRetrieved", len(results))

	return results, nil
}
