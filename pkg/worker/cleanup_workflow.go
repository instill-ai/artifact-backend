package worker

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/types"

	errorsx "github.com/instill-ai/x/errors"
)

// CleanupFileWorkflowParam defines the parameters for the CleanupFileWorkflow
type CleanupFileWorkflowParam struct {
	FileUID             types.FileUIDType
	UserUID             types.UserUIDType
	RequesterUID        types.RequesterUIDType
	WorkflowID          string
	IncludeOriginalFile bool
}

// CleanupKnowledgeBaseWorkflowParam defines the parameters for the CleanupKnowledgeBaseWorkflow
type CleanupKnowledgeBaseWorkflowParam struct {
	KBUID                  types.KBUIDType
	CleanupAfterSeconds    int64            // If > 0, wait this many seconds before cleanup (for retention-based cleanup)
	ProtectedCollectionUID *types.KBUIDType // Collection UID that must not be dropped (e.g., after swap)
}

type cleanupFileWorkflow struct {
	temporalClient client.Client
	worker         *Worker
}

// NewCleanupFileWorkflow creates a new CleanupFileWorkflow instance
func NewCleanupFileWorkflow(temporalClient client.Client, worker *Worker) *cleanupFileWorkflow {
	return &cleanupFileWorkflow{
		temporalClient: temporalClient,
		worker:         worker,
	}
}

func (w *cleanupFileWorkflow) Execute(ctx context.Context, param CleanupFileWorkflowParam) error {
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
func NewCleanupKnowledgeBaseWorkflow(temporalClient client.Client, worker *Worker) *cleanupKnowledgeBaseWorkflow {
	return &cleanupKnowledgeBaseWorkflow{
		temporalClient: temporalClient,
		worker:         worker,
	}
}

func (w *cleanupKnowledgeBaseWorkflow) Execute(ctx context.Context, param CleanupKnowledgeBaseWorkflowParam) error {
	workflowID := fmt.Sprintf("cleanup-kb-%s", param.KBUID.String())
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
func (w *Worker) CleanupFileWorkflow(ctx workflow.Context, param CleanupFileWorkflowParam) error {
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

	// Delete text chunks
	tasks = append(tasks, cleanupTask{
		name: "delete text chunks",
		future: workflow.ExecuteActivity(ctx, w.DeleteTextChunksFromMinIOActivity, &DeleteTextChunksFromMinIOActivityParam{
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
// - All text chunk records in Postgres
// - All embedding records in Postgres
// - ACL permissions for the knowledge base
//
// If CleanupAfterSeconds > 0, the workflow will wait that many seconds before performing cleanup.
// This is used for retention-based cleanup where old KBs should be deleted after a retention period.
//
// Use this when deleting an entire knowledge base (not individual files).
func (w *Worker) CleanupKnowledgeBaseWorkflow(ctx workflow.Context, param CleanupKnowledgeBaseWorkflowParam) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting CleanupKnowledgeBaseWorkflow",
		"kbUID", param.KBUID.String(),
		"cleanupAfterSeconds", param.CleanupAfterSeconds)

	kbUID := param.KBUID

	// If a delay is specified, wait before cleanup (for retention-based cleanup)
	if param.CleanupAfterSeconds > 0 {
		logger.Info("Waiting before cleanup (retention period)",
			"delaySeconds", param.CleanupAfterSeconds)

		waitDuration := time.Duration(param.CleanupAfterSeconds) * time.Second
		err := workflow.Sleep(ctx, waitDuration)
		if err != nil {
			logger.Error("Sleep interrupted", "error", err)
			return err
		}

		logger.Info("Retention period expired, proceeding with cleanup", "kbUID", kbUID.String())
	}

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

	// CRITICAL: Wait for any in-progress file processing to complete before cleanup
	// This prevents race conditions where:
	// 1. File processing workflows try to save embeddings to Milvus
	// 2. This cleanup workflow drops the Milvus collection
	// 3. File processing fails with "collection does not exist"
	//
	// We poll every 5 seconds for up to 2 minutes (24 attempts)
	// File processing (especially embedding) should complete within 1-2 minutes normally
	maxWaitAttempts := 24 // 24 * 5s = 2 minutes
	for attempt := range maxWaitAttempts {
		var inProgressCount int64
		err := workflow.ExecuteActivity(ctx, w.GetInProgressFileCountActivity, &GetInProgressFileCountActivityParam{
			KBUID: kbUID,
		}).Get(ctx, &inProgressCount)

		if err != nil {
			logger.Error("Failed to check for in-progress files, proceeding with cleanup anyway",
				"kbUID", kbUID.String(),
				"error", err.Error(),
				"attempt", attempt+1)
			break // Proceed with cleanup if we can't check
		}

		if inProgressCount == 0 {
			logger.Info("No in-progress files, safe to proceed with cleanup",
				"kbUID", kbUID.String(),
				"attempts", attempt+1)
			break
		}

		logger.Info("Waiting for file processing to complete before cleanup",
			"kbUID", kbUID.String(),
			"inProgressCount", inProgressCount,
			"attempt", attempt+1,
			"maxAttempts", maxWaitAttempts)

		if attempt < maxWaitAttempts-1 {
			err := workflow.Sleep(ctx, 5*time.Second)
			if err != nil {
				logger.Error("Sleep interrupted while waiting for file processing", "error", err)
				return err
			}
		}
	}

	// Collect errors to ensure we attempt all cleanup operations
	// but still report failures
	var errors []string

	// Step 1: Delete all files from MinIO
	err := workflow.ExecuteActivity(ctx, w.DeleteKBFilesFromMinIOActivity, &DeleteKBFilesFromMinIOActivityParam{
		KBUID: kbUID,
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
		KBUID:                  kbUID,
		ProtectedCollectionUID: param.ProtectedCollectionUID,
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
		KBUID: kbUID,
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
		KBUID: kbUID,
	}).Get(ctx, nil)
	if err != nil {
		errMsg := fmt.Sprintf("delete converted file records: %s", errorsx.MessageOrErr(err))
		errors = append(errors, errMsg)
		logger.Error("Failed to delete converted file records",
			"kbUID", kbUID.String(),
			"error", err.Error())
	}

	// Step 5: Delete text chunk records
	err = workflow.ExecuteActivity(ctx, w.DeleteKBTextChunkRecordsActivity, &DeleteKBTextChunkRecordsActivityParam{
		KBUID: kbUID,
	}).Get(ctx, nil)
	if err != nil {
		errMsg := fmt.Sprintf("delete text chunk records: %s", errorsx.MessageOrErr(err))
		errors = append(errors, errMsg)
		logger.Error("Failed to delete text chunk records",
			"kbUID", kbUID.String(),
			"error", err.Error())
	}

	// Step 6: Delete embedding records
	err = workflow.ExecuteActivity(ctx, w.DeleteKBEmbeddingRecordsActivity, &DeleteKBEmbeddingRecordsActivityParam{
		KBUID: kbUID,
	}).Get(ctx, nil)
	if err != nil {
		errMsg := fmt.Sprintf("delete embedding records: %s", errorsx.MessageOrErr(err))
		errors = append(errors, errMsg)
		logger.Error("Failed to delete embedding records",
			"kbUID", kbUID.String(),
			"error", err.Error())
	}

	// Step 7: Soft-delete the KB record itself
	err = workflow.ExecuteActivity(ctx, w.SoftDeleteKBRecordActivity, &SoftDeleteKBRecordActivityParam{
		KBUID: kbUID,
	}).Get(ctx, nil)
	if err != nil {
		errMsg := fmt.Sprintf("soft-delete KB record: %s", errorsx.MessageOrErr(err))
		errors = append(errors, errMsg)
		logger.Error("Failed to soft-delete KB record",
			"kbUID", kbUID.String(),
			"error", err.Error())
	}

	// Step 8: Purge ACL (must be done last)
	err = workflow.ExecuteActivity(ctx, w.PurgeKBACLActivity, &PurgeKBACLActivityParam{
		KBUID: kbUID,
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
			"kbUID", param.KBUID.String(),
			"errorCount", len(errors),
			"errors", errors)
		err := errorsx.AddMessage(
			fmt.Errorf("cleanup failed with %d error(s): %v", len(errors), errors),
			"Catalog cleanup encountered errors. Some cleanup operations may not have completed.",
		)
		return err
	}

	logger.Info("CleanupKnowledgeBaseWorkflow completed successfully", "kbUID", param.KBUID.String())
	return nil
}
