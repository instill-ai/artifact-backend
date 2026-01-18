package worker

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

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
	MinioBucket         string // Minio bucket name for object storage
}

// CleanupKnowledgeBaseWorkflowParam defines the parameters for the CleanupKnowledgeBaseWorkflow
type CleanupKnowledgeBaseWorkflowParam struct {
	KBUID                  types.KBUIDType
	CleanupAfterSeconds    int64            // If > 0, wait this many seconds before cleanup (for retention-based cleanup)
	ProtectedCollectionUID *types.KBUIDType // Collection UID that must not be dropped (e.g., after swap)
	MinioBucket            string           // Minio bucket name for object storage
	// PollIterationCount tracks how many poll cycles have been completed
	// Used to determine when to ContinueAsNew to avoid history size limits
	PollIterationCount int
}

// MaxPollIterationsBeforeContinueAsNew is the maximum number of poll iterations before
// using ContinueAsNew to reset workflow history. With 5-second polling intervals,
// 500 iterations = ~42 minutes before reset. This prevents unbounded history growth
// while still supporting long retention periods.
const MaxPollIterationsBeforeContinueAsNew = 500

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
		return errorsx.AddMessage(err, "Unable to start knowledge base cleanup workflow. Please try again.")
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
				Bucket:  param.MinioBucket,
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
// If CleanupAfterSeconds > 0, the workflow will poll the database periodically to check
// if the retention period has expired. This allows dynamic retention changes via
// SetRollbackRetentionAdmin API to take effect immediately.
//
// Use this when deleting an entire knowledge base (not individual files).
func (w *Worker) CleanupKnowledgeBaseWorkflow(ctx workflow.Context, param CleanupKnowledgeBaseWorkflowParam) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting CleanupKnowledgeBaseWorkflow",
		"kbUID", param.KBUID.String(),
		"cleanupAfterSeconds", param.CleanupAfterSeconds)

	kbUID := param.KBUID

	// If a delay is specified, poll the database for retention expiry
	// This allows SetRollbackRetentionAdmin to dynamically change the retention period
	if param.CleanupAfterSeconds > 0 {
		logger.Info("Polling for retention expiry (allows dynamic retention changes)",
			"initialDelaySeconds", param.CleanupAfterSeconds,
			"pollIterationCount", param.PollIterationCount)

		// Use short activity timeout for polling
		pollActivityOptions := workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    time.Second,
				BackoffCoefficient: 2.0,
				MaximumInterval:    10 * time.Second,
				MaximumAttempts:    3,
			},
		}
		pollCtx := workflow.WithActivityOptions(ctx, pollActivityOptions)

		// Poll every 5 seconds until retention expires
		// Max poll iterations = initialDelaySeconds / 5 + extra buffer for safety
		pollInterval := 5 * time.Second
		maxPollIterations := int(param.CleanupAfterSeconds/5) + 100 // Extra buffer
		pollIterationCount := param.PollIterationCount

		for i := pollIterationCount; i < maxPollIterations; i++ {
			var result CheckRollbackRetentionExpiredActivityResult
			err := workflow.ExecuteActivity(pollCtx, w.CheckRollbackRetentionExpiredActivity,
				&CheckRollbackRetentionExpiredActivityParam{RollbackKBUID: kbUID}).Get(pollCtx, &result)

			if err != nil {
				logger.Error("Failed to check retention expiry, will retry",
					"error", err, "attempt", i+1)
				// Sleep and retry
				if sleepErr := workflow.Sleep(ctx, pollInterval); sleepErr != nil {
					return sleepErr
				}
				continue
			}

			// Check if cleanup should proceed
			if result.Expired {
				if result.KBDeleted {
					logger.Info("Rollback KB already deleted, terminating workflow",
						"kbUID", kbUID.String())
					return nil // No cleanup needed
				}
				if result.ProductionKBDeleted {
					logger.Info("Production KB deleted, proceeding with rollback cleanup",
						"kbUID", kbUID.String())
				} else {
					logger.Info("Retention period expired, proceeding with cleanup",
						"kbUID", kbUID.String())
				}
				break // Exit polling loop and proceed with cleanup
			}

			// Retention not expired yet, sleep and poll again
			if i%12 == 0 { // Log every minute (12 * 5s = 60s)
				logger.Info("Retention period not expired, continuing to poll",
					"kbUID", kbUID.String(), "pollIteration", i+1)
			}

			pollIterationCount = i + 1

			// ContinueAsNew to reset workflow history and avoid hitting event count limits
			// This is a Temporal best practice for long-running workflows
			if pollIterationCount >= MaxPollIterationsBeforeContinueAsNew && pollIterationCount < maxPollIterations {
				logger.Info("CleanupKnowledgeBaseWorkflow: Reached max poll iterations, using ContinueAsNew to reset history",
					"pollIterations", pollIterationCount,
					"kbUID", kbUID.String())
				return workflow.NewContinueAsNewError(ctx, w.CleanupKnowledgeBaseWorkflow, CleanupKnowledgeBaseWorkflowParam{
					KBUID:                  param.KBUID,
					CleanupAfterSeconds:    param.CleanupAfterSeconds,
					ProtectedCollectionUID: param.ProtectedCollectionUID,
					MinioBucket:            param.MinioBucket,
					PollIterationCount:     pollIterationCount, // Continue from where we left off
				})
			}

			if err := workflow.Sleep(ctx, pollInterval); err != nil {
				logger.Error("Sleep interrupted during retention polling", "error", err)
				return err
			}
		}
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

	// Step 9: Clear retention field on production KB if this is a rollback KB cleanup
	// This should be done after all cleanup operations to ensure the rollback KB is fully cleaned up
	err = workflow.ExecuteActivity(ctx, w.ClearProductionKBRetentionActivity, &ClearProductionKBRetentionActivityParam{
		RollbackKBUID: kbUID,
	}).Get(ctx, nil)
	if err != nil {
		// Non-fatal: Log warning but don't fail the workflow
		logger.Warn("Failed to clear retention field on production KB (non-fatal)",
			"kbUID", kbUID.String(),
			"error", err.Error())
	}

	// Step 10: Verify cleanup completed successfully (best-effort verification)
	// This helps detect orphaned resources that may need manual cleanup
	if len(errors) == 0 {
		logger.Info("All cleanup operations succeeded, verifying cleanup completion")
		verifyCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    time.Second,
				BackoffCoefficient: 2.0,
				MaximumInterval:    10 * time.Second,
				MaximumAttempts:    2,
			},
		})

		// Get the collection UID for verification
		// For rollback KBs, we need to query the KB to get its active_collection_uid
		var collectionUID types.KBUIDType
		verifyErr := workflow.ExecuteActivity(verifyCtx, w.GetKBCollectionUIDActivity, &GetKBCollectionUIDActivityParam{
			KBUID: kbUID,
		}).Get(verifyCtx, &collectionUID)

		if verifyErr != nil {
			logger.Warn("Failed to get collection UID for verification",
				"kbUID", kbUID.String(),
				"error", verifyErr.Error())
		} else if collectionUID.String() != "" {
			verifyErr = workflow.ExecuteActivity(verifyCtx, w.VerifyKBCleanupActivity, &VerifyKBCleanupActivityParam{
				KBUID:         kbUID,
				CollectionUID: collectionUID,
			}).Get(verifyCtx, nil)

			if verifyErr != nil {
				logger.Warn("Rollback KB cleanup verification failed - resources may need manual cleanup",
					"kbUID", kbUID.String(),
					"error", verifyErr.Error())
			} else {
				logger.Info("Rollback KB cleanup verified successfully")
			}
		}
	} else {
		logger.Warn("Skipping verification due to cleanup errors",
			"kbUID", kbUID.String(),
			"errorCount", len(errors))
	}

	// If any cleanup operation failed, return an error
	if len(errors) > 0 {
		logger.Error("CleanupKnowledgeBaseWorkflow completed with errors",
			"kbUID", param.KBUID.String(),
			"errorCount", len(errors),
			"errors", errors)
		err := errorsx.AddMessage(
			fmt.Errorf("cleanup failed with %d error(s): %v", len(errors), errors),
			"Knowledge base cleanup encountered errors. Some cleanup operations may not have completed.",
		)
		return err
	}

	logger.Info("CleanupKnowledgeBaseWorkflow completed successfully", "kbUID", param.KBUID.String())
	return nil
}

// GCSCleanupContinuousWorkflowParam defines parameters for the GCS cleanup workflow
type GCSCleanupContinuousWorkflowParam struct {
	// IterationCount tracks how many cleanup cycles have been completed
	// Used to determine when to ContinueAsNew to avoid history size limits
	IterationCount int
}

// MaxIterationsBeforeContinueAsNew is the maximum number of iterations before
// using ContinueAsNew to reset workflow history. This prevents the workflow
// from accumulating too many events (Temporal warns at ~10K events).
// With 2-minute intervals, 100 iterations = ~3.3 hours before reset.
const MaxIterationsBeforeContinueAsNew = 100

// GCSCleanupContinuousWorkflow runs the GCS cleanup in a continuous loop with timer
// This is an alternative to using Temporal's cron schedules
// It runs every 2 minutes indefinitely until cancelled
//
// IMPORTANT: This workflow uses ContinueAsNew after MaxIterationsBeforeContinueAsNew
// cycles to prevent workflow history from growing unbounded. This is a Temporal
// best practice for long-running workflows.
//
// Workflow name: GCSCleanupContinuousWorkflow
// Singleton: Yes - Only one instance runs at a time using WorkflowID "artifact-backend-gcs-cleanup-continuous-singleton"
// Usage: Start once and it will run forever on a 2-minute interval
func (w *Worker) GCSCleanupContinuousWorkflow(ctx workflow.Context, param GCSCleanupContinuousWorkflowParam) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("GCSCleanupContinuousWorkflow: Starting continuous GCS cleanup workflow",
		"iterationCount", param.IterationCount)

	// Activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		HeartbeatTimeout:    30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    10 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    5 * time.Minute,
			MaximumAttempts:    3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Run cleanup every 2 minutes
	cleanupInterval := 2 * time.Minute
	iterationCount := param.IterationCount

	for {
		logger.Info("GCSCleanupContinuousWorkflow: Starting cleanup cycle",
			"iteration", iterationCount)

		// Execute cleanup activity
		activityParam := &CleanupExpiredGCSFilesActivityParam{
			MaxFilesToClean: 1000,
		}

		err := workflow.ExecuteActivity(ctx, w.CleanupExpiredGCSFilesActivity, activityParam).Get(ctx, nil)
		if err != nil {
			logger.Warn("GCSCleanupContinuousWorkflow: Cleanup cycle failed, will retry in next cycle",
				"error", err.Error())
			// Don't fail - just log and continue to next cycle
		} else {
			logger.Info("GCSCleanupContinuousWorkflow: Cleanup cycle completed successfully")
		}

		iterationCount++

		// ContinueAsNew to reset workflow history and avoid hitting event count limits
		// This is a Temporal best practice for long-running workflows
		if iterationCount >= MaxIterationsBeforeContinueAsNew {
			logger.Info("GCSCleanupContinuousWorkflow: Reached max iterations, using ContinueAsNew to reset history",
				"iterations", iterationCount)
			return workflow.NewContinueAsNewError(ctx, w.GCSCleanupContinuousWorkflow, GCSCleanupContinuousWorkflowParam{
				IterationCount: 0, // Reset counter
			})
		}

		// Wait for next cycle
		logger.Debug("GCSCleanupContinuousWorkflow: Sleeping until next cycle",
			"interval", cleanupInterval)

		if err := workflow.Sleep(ctx, cleanupInterval); err != nil {
			logger.Error("GCSCleanupContinuousWorkflow: Workflow sleep interrupted",
				"error", err.Error())
			return err
		}
	}
}
