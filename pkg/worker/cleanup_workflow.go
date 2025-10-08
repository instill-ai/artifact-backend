package worker

import (
	"context"
	"fmt"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/service"

	errorsx "github.com/instill-ai/x/errors"
)

type cleanupFileWorkflow struct {
	temporalClient client.Client
	worker         *Worker
}

// NewCleanupFileWorkflow creates a new CleanupFileWorkflow instance
func NewCleanupFileWorkflow(temporalClient client.Client, worker *Worker) service.CleanupFileWorkflow {
	return &cleanupFileWorkflow{
		temporalClient: temporalClient,
		worker:         worker,
	}
}

func (w *cleanupFileWorkflow) Execute(ctx context.Context, param service.CleanupFileWorkflowParam) error {
	workflowID := fmt.Sprintf("cleanup-file-%s", param.FileUID.String())
	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	_, err := w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, w.worker.CleanupFileWorkflow, param)
	if err != nil {
		return errorsx.AddMessage(err, "Unable to start file cleanup workflow. Please try again.")
	}
	return nil
}

type cleanupKnowledgeBaseWorkflow struct {
	temporalClient client.Client
	worker         *Worker
}

// NewCleanupKnowledgeBaseWorkflow creates a new CleanupKnowledgeBaseWorkflow instance
func NewCleanupKnowledgeBaseWorkflow(temporalClient client.Client, worker *Worker) service.CleanupKnowledgeBaseWorkflow {
	return &cleanupKnowledgeBaseWorkflow{
		temporalClient: temporalClient,
		worker:         worker,
	}
}

func (w *cleanupKnowledgeBaseWorkflow) Execute(ctx context.Context, param service.CleanupKnowledgeBaseWorkflowParam) error {
	workflowID := fmt.Sprintf("cleanup-kb-%s", param.KnowledgeBaseUID.String())
	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	_, err := w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, w.worker.CleanupKnowledgeBaseWorkflow, param)
	if err != nil {
		return errorsx.AddMessage(err, "Unable to start catalog cleanup workflow. Please try again.")
	}
	return nil
}

// CleanupFileWorkflow handles cleanup operations for a specific file.
// This workflow cleans up resources for a single file, including:
// - Original file from MinIO (if IncludeOriginalFile is true)
// - Converted files (markdown/text conversions)
// - Text chunks
// - Embeddings from both Milvus and Postgres
// Use this when deleting individual files from a knowledge base.
func (w *Worker) CleanupFileWorkflow(ctx workflow.Context, param service.CleanupFileWorkflowParam) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting CleanupFileWorkflow",
		"fileUID", param.FileUID.String(),
		"userUID", param.UserUID.String(),
		"requesterUID", param.RequesterUID.String(),
		"workflowID", param.WorkflowID,
		"includeOriginalFile", param.IncludeOriginalFile)

	fileUID := param.FileUID

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

	// Collect errors to ensure we attempt all cleanup operations
	// but still report failures
	var errors []string

	// Launch all cleanup activities in parallel for better performance
	// All these operations are independent and can run concurrently
	type cleanupTask struct {
		name   string
		future workflow.Future
	}

	tasks := []cleanupTask{}

	// Optional: Delete original file if requested
	if param.IncludeOriginalFile {
		tasks = append(tasks, cleanupTask{
			name: "delete original file",
			future: workflow.ExecuteActivity(ctx, w.DeleteOriginalFileActivity, &DeleteOriginalFileActivityParam{
				FileUID: fileUID,
				Bucket:  config.Config.Minio.BucketName,
			}),
		})
	}

	// Delete converted file
	tasks = append(tasks, cleanupTask{
		name: "delete converted file",
		future: workflow.ExecuteActivity(ctx, w.DeleteConvertedFileActivity, &DeleteConvertedFileActivityParam{
			FileUID: fileUID,
		}),
	})

	// Delete chunks
	tasks = append(tasks, cleanupTask{
		name: "delete chunks",
		future: workflow.ExecuteActivity(ctx, w.DeleteChunksFromMinIOActivity, &DeleteChunksFromMinIOActivityParam{
			FileUID: fileUID,
		}),
	})

	// Delete embeddings from Milvus (parallel-optimized)
	tasks = append(tasks, cleanupTask{
		name: "delete embeddings from Milvus",
		future: workflow.ExecuteActivity(ctx, w.DeleteEmbeddingsFromVectorDBActivity, &DeleteEmbeddingsFromVectorDBActivityParam{
			FileUID: fileUID,
		}),
	})

	// Delete embedding records from DB (parallel-optimized)
	tasks = append(tasks, cleanupTask{
		name: "delete embedding records",
		future: workflow.ExecuteActivity(ctx, w.DeleteEmbeddingRecordsActivity, &DeleteEmbeddingsFromVectorDBActivityParam{
			FileUID: fileUID,
		}),
	})

	// Wait for all tasks to complete and collect errors
	for _, task := range tasks {
		if err := task.future.Get(ctx, nil); err != nil {
			errMsg := fmt.Sprintf("%s: %s", task.name, errorsx.MessageOrErr(err))
			errors = append(errors, errMsg)
			logger.Error("Cleanup task failed",
				"task", task.name,
				"fileUID", fileUID.String(),
				"error", err.Error())
		}
	}

	// If any cleanup operation failed, return an error
	if len(errors) > 0 {
		logger.Error("CleanupFileWorkflow completed with errors",
			"fileUID", param.FileUID.String(),
			"workflowID", param.WorkflowID,
			"errorCount", len(errors),
			"errors", errors)
		err := errorsx.AddMessage(
			fmt.Errorf("cleanup failed with %d error(s): %v", len(errors), errors),
			"File cleanup encountered errors. Some cleanup operations may not have completed.",
		)
		return err
	}

	logger.Info("CleanupFileWorkflow completed successfully",
		"fileUID", param.FileUID.String(),
		"userUID", param.UserUID.String(),
		"requesterUID", param.RequesterUID.String(),
		"workflowID", param.WorkflowID)
	return nil
}

// CleanupKnowledgeBaseWorkflow orchestrates the complete cleanup of an entire knowledge base.
// This workflow performs a comprehensive cleanup of all knowledge base resources, including:
// - All files from MinIO for the knowledge base
// - Entire Milvus collection (all embeddings)
// - All file records in Postgres
// - All converted file records in Postgres
// - All chunk records in Postgres
// - All embedding records in Postgres
// - ACL permissions for the knowledge base
// Use this when deleting an entire knowledge base (not individual files).
func (w *Worker) CleanupKnowledgeBaseWorkflow(ctx workflow.Context, param service.CleanupKnowledgeBaseWorkflowParam) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting CleanupKnowledgeBaseWorkflow", "kbUID", param.KnowledgeBaseUID.String())

	kbUID := param.KnowledgeBaseUID

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

	// Collect errors to ensure we attempt all cleanup operations
	// but still report failures
	var errors []string

	// Step 1: Delete all files from MinIO
	err := workflow.ExecuteActivity(ctx, w.DeleteKBFilesFromMinIOActivity, &DeleteKBFilesFromMinIOActivityParam{
		KnowledgeBaseUID: kbUID,
	}).Get(ctx, nil)
	if err != nil {
		errMsg := fmt.Sprintf("delete files from MinIO: %s", errorsx.MessageOrErr(err))
		errors = append(errors, errMsg)
		logger.Error("Failed to delete files from MinIO",
			"kbUID", kbUID.String(),
			"error", err.Error())
	}

	// Step 2: Drop Milvus collection
	err = workflow.ExecuteActivity(ctx, w.DropVectorDBCollectionActivity, &DropVectorDBCollectionActivityParam{
		KnowledgeBaseUID: kbUID,
	}).Get(ctx, nil)
	if err != nil {
		errMsg := fmt.Sprintf("drop Milvus collection: %s", errorsx.MessageOrErr(err))
		errors = append(errors, errMsg)
		logger.Error("Failed to drop Milvus collection",
			"kbUID", kbUID.String(),
			"error", err.Error())
	}

	// Step 3: Delete file records
	err = workflow.ExecuteActivity(ctx, w.DeleteKBFileRecordsActivity, &DeleteKBFileRecordsActivityParam{
		KnowledgeBaseUID: kbUID,
	}).Get(ctx, nil)
	if err != nil {
		errMsg := fmt.Sprintf("delete file records: %s", errorsx.MessageOrErr(err))
		errors = append(errors, errMsg)
		logger.Error("Failed to delete file records",
			"kbUID", kbUID.String(),
			"error", err.Error())
	}

	// Step 4: Delete converted file records
	err = workflow.ExecuteActivity(ctx, w.DeleteKBConvertedFileRecordsActivity, &DeleteKBConvertedFileRecordsActivityParam{
		KnowledgeBaseUID: kbUID,
	}).Get(ctx, nil)
	if err != nil {
		errMsg := fmt.Sprintf("delete converted file records: %s", errorsx.MessageOrErr(err))
		errors = append(errors, errMsg)
		logger.Error("Failed to delete converted file records",
			"kbUID", kbUID.String(),
			"error", err.Error())
	}

	// Step 5: Delete chunk records
	err = workflow.ExecuteActivity(ctx, w.DeleteKBChunkRecordsActivity, &DeleteKBChunkRecordsActivityParam{
		KnowledgeBaseUID: kbUID,
	}).Get(ctx, nil)
	if err != nil {
		errMsg := fmt.Sprintf("delete chunk records: %s", errorsx.MessageOrErr(err))
		errors = append(errors, errMsg)
		logger.Error("Failed to delete chunk records",
			"kbUID", kbUID.String(),
			"error", err.Error())
	}

	// Step 6: Delete embedding records
	err = workflow.ExecuteActivity(ctx, w.DeleteKBEmbeddingRecordsActivity, &DeleteKBEmbeddingRecordsActivityParam{
		KnowledgeBaseUID: kbUID,
	}).Get(ctx, nil)
	if err != nil {
		errMsg := fmt.Sprintf("delete embedding records: %s", errorsx.MessageOrErr(err))
		errors = append(errors, errMsg)
		logger.Error("Failed to delete embedding records",
			"kbUID", kbUID.String(),
			"error", err.Error())
	}

	// Step 7: Purge ACL (must be done last)
	err = workflow.ExecuteActivity(ctx, w.PurgeKBACLActivity, &PurgeKBACLActivityParam{
		KnowledgeBaseUID: kbUID,
	}).Get(ctx, nil)
	if err != nil {
		errMsg := fmt.Sprintf("purge ACL: %s", errorsx.MessageOrErr(err))
		errors = append(errors, errMsg)
		logger.Error("Failed to purge ACL",
			"kbUID", kbUID.String(),
			"error", err.Error())
	}

	// If any cleanup operation failed, return an error
	if len(errors) > 0 {
		logger.Error("CleanupKnowledgeBaseWorkflow completed with errors",
			"kbUID", param.KnowledgeBaseUID.String(),
			"errorCount", len(errors),
			"errors", errors)
		err := errorsx.AddMessage(
			fmt.Errorf("cleanup failed with %d error(s): %v", len(errors), errors),
			"Catalog cleanup encountered errors. Some cleanup operations may not have completed.",
		)
		return err
	}

	logger.Info("CleanupKnowledgeBaseWorkflow completed successfully", "kbUID", param.KnowledgeBaseUID.String())
	return nil
}
