package worker

import (
	"errors"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/types"
	errorsx "github.com/instill-ai/x/errors"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// UpdateKnowledgeBaseWorkflowParam defines parameters for updating a single KB
type UpdateKnowledgeBaseWorkflowParam struct {
	OriginalKBUID types.KBUIDType
	UserUID       types.UserUIDType
	RequesterUID  types.RequesterUIDType
	// SystemID specifies which system ID to use for the new embedding config
	// If empty, uses the KB's current embedding config (useful for reprocessing)
	SystemID string
}

// UpdateKnowledgeBaseWorkflow updates a single knowledge base through 6 phases:
// 1. Prepare: Create staging KB with new collection
// 2. Reprocess: Clone and reprocess all files with new config
// 3. Synchronize: Lock KB and wait for all dual-processed files to complete
// 4. Validate: Verify data integrity (file counts, embeddings, chunks)
// 5. Swap: Atomic pointer swap of collections and metadata
// 6. Cleanup: Schedule rollback KB deletion after retention period
func (w *Worker) UpdateKnowledgeBaseWorkflow(ctx workflow.Context, param UpdateKnowledgeBaseWorkflowParam) (retErr error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting UpdateKnowledgeBaseWorkflow",
		"originalKBUID", param.OriginalKBUID.String())

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

	workflowID := workflow.GetInfo(ctx).WorkflowExecution.ID

	// Track completion for cleanup
	var stagingKBUID types.KBUIDType
	updateCompleted := false

	// Defer cleanup on failure
	// Note: Does NOT run if workflow was explicitly canceled via AbortKnowledgeBaseUpdate
	// (abort service handles cleanup and status update to avoid race condition)
	defer func() {
		if !updateCompleted {
			// Prepare cleanup context
			cleanupCtx, _ := workflow.NewDisconnectedContext(ctx)
			cleanupCtx = workflow.WithActivityOptions(cleanupCtx, workflow.ActivityOptions{
				StartToCloseTimeout: time.Minute,
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval:    time.Second,
					BackoffCoefficient: 2.0,
					MaximumInterval:    30 * time.Second,
					MaximumAttempts:    3,
				},
			})

			// CRITICAL: Cleanup staging KB only if it was created
			// If workflow fails before Phase 1 completes, stagingKBUID will be empty
			if stagingKBUID.String() != "" {
				logger.Warn("update failed, cleaning up staging KB",
					"stagingKBUID", stagingKBUID)

				_ = workflow.ExecuteActivity(cleanupCtx, w.CleanupOldKnowledgeBaseActivity, &CleanupOldKnowledgeBaseActivityParam{
					KBUID: stagingKBUID,
				}).Get(cleanupCtx, nil)
			}

			// CRITICAL: ALWAYS mark production KB as failed, even if staging KB was never created
			// This ensures pollUpdateCompletion doesn't timeout waiting for a status that never comes
			// Mark original KB as failed with error message
			// IMPORTANT: This only runs for actual failures, not cancellations
			// Canceled workflows are handled by AbortKnowledgeBaseUpdate which sets status to ABORTED
			errorMessage := ""
			if retErr != nil {
				errorMessage = retErr.Error()
				// Truncate very long error messages to avoid DB issues
				if len(errorMessage) > 1000 {
					errorMessage = errorMessage[:1000] + "... (truncated)"
				}
			}

			_ = workflow.ExecuteActivity(cleanupCtx, w.UpdateKnowledgeBaseUpdateStatusActivity, &UpdateKnowledgeBaseUpdateStatusActivityParam{
				KBUID:             param.OriginalKBUID,
				Status:            artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_FAILED.String(),
				WorkflowID:        workflowID,
				ErrorMessage:      errorMessage,
				PreviousSystemUID: types.SystemUIDType(uuid.Nil), // Don't update, already set when status changed to UPDATING
			}).Get(cleanupCtx, nil)
		}
	}()

	// ========== Phase 1: Prepare ==========
	logger.Info("Phase 1: Prepare - Validating and creating staging KB")

	// Validate eligibility FIRST before marking as updating
	// This also captures the current system UID for audit trail
	var validateResult ValidateUpdateEligibilityActivityResult
	err := workflow.ExecuteActivity(ctx, w.ValidateUpdateEligibilityActivity, &ValidateUpdateEligibilityActivityParam{
		KBUID: param.OriginalKBUID,
	}).Get(ctx, &validateResult)
	if err != nil {
		logger.Error("KB is not eligible for update", "error", err)
		return err
	}

	previousSystemUID := validateResult.PreviousSystemUID
	logger.Info("Captured previous system UID for audit trail", "previousSystemUID", previousSystemUID.String())

	// ========== Phase 1.5: Take Snapshot ==========
	// CRITICAL: Take snapshot BEFORE status changes to UPDATING
	// This ensures clean separation between cloning and dual-processing:
	// - Files in snapshot: Will be cloned to staging
	// - Files uploaded after status change: Will be dual-processed to staging
	// - NO OVERLAP! (prevents duplicate files race condition)
	logger.Info("Phase 1.5: Taking snapshot of files before status change")

	var listFilesResult ListFilesForReprocessingActivityResult
	err = workflow.ExecuteActivity(ctx, w.ListFilesForReprocessingActivity, &ListFilesForReprocessingActivityParam{
		KBUID: param.OriginalKBUID,
	}).Get(ctx, &listFilesResult)
	if err != nil {
		logger.Error("Failed to list files for reprocessing", "error", err)
		return err
	}

	logger.Info("Snapshot taken", "fileCount", len(listFilesResult.FileUIDs))

	// Update original KB status to updating after snapshot is taken
	// Pass previousSystemUID for historical audit trail
	err = workflow.ExecuteActivity(ctx, w.UpdateKnowledgeBaseUpdateStatusActivity, &UpdateKnowledgeBaseUpdateStatusActivityParam{
		KBUID:             param.OriginalKBUID,
		Status:            artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_UPDATING.String(),
		WorkflowID:        workflowID,
		PreviousSystemUID: previousSystemUID,
	}).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to update KB status to updating", "error", err)
		return err
	}

	// Create staging KB
	var stagingResult CreateStagingKnowledgeBaseActivityResult
	err = workflow.ExecuteActivity(ctx, w.CreateStagingKnowledgeBaseActivity, &CreateStagingKnowledgeBaseActivityParam{
		OriginalKBUID: param.OriginalKBUID,
		SystemID:      param.SystemID,
	}).Get(ctx, &stagingResult)
	if err != nil {
		logger.Error("Failed to create staging KB", "error", err)
		return err
	}

	stagingKBUID = stagingResult.StagingKB.UID
	logger.Info("Staging KB created", "stagingKBUID", stagingKBUID.String())

	// ========== Phase 2: Reprocess ==========
	logger.Info("Phase 2: Reprocess - Reprocessing all files from snapshot", "fileCount", len(listFilesResult.FileUIDs))

	// Reprocess files in batches
	batchSize := config.Config.RAG.Update.BatchSize
	if batchSize <= 0 {
		batchSize = 10 // Default batch size
	}

	for i := 0; i < len(listFilesResult.FileUIDs); i += batchSize {
		end := i + batchSize
		if end > len(listFilesResult.FileUIDs) {
			end = len(listFilesResult.FileUIDs)
		}
		batch := listFilesResult.FileUIDs[i:end]

		logger.Info("Processing file batch", "batch", i/batchSize+1, "files", len(batch))

		// Reprocess each file in the batch
		var batchFutures []workflow.Future
		for _, fileUID := range batch {
			// Clone file to staging KB
			var cloneResult CloneFileToStagingKBActivityResult
			err = workflow.ExecuteActivity(ctx, w.CloneFileToStagingKBActivity, &CloneFileToStagingKBActivityParam{
				OriginalFileUID: fileUID,
				StagingKBUID:    stagingKBUID,
			}).Get(ctx, &cloneResult)
			if err != nil {
				logger.Error("Failed to clone file to staging KB", "fileUID", fileUID, "error", err)
				return err
			}

			// CRITICAL: Skip processing if file was deleted during update
			// CloneFileToStagingKBActivity returns nil UID when original file is not found
			// This is expected behavior - users can delete files during updates
			if cloneResult.NewFileUID.String() == uuid.Nil.String() {
				logger.Info("Skipping processing for deleted file", "originalFileUID", fileUID)
				continue
			}

			// Trigger ProcessFileWorkflow for the cloned file
			childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
				WorkflowID: fmt.Sprintf("process-file-%s-update", cloneResult.NewFileUID.String()),
			})

			future := workflow.ExecuteChildWorkflow(childCtx, "ProcessFileWorkflow", ProcessFileWorkflowParam{
				FileUIDs:     []types.FileUIDType{cloneResult.NewFileUID},
				KBUID:        stagingKBUID,
				UserUID:      param.UserUID,
				RequesterUID: param.UserUID,
			})
			batchFutures = append(batchFutures, future)
		}

		// Wait for batch to complete
		for _, future := range batchFutures {
			err = future.Get(ctx, nil)
			if err != nil {
				logger.Error("File processing failed in batch", "error", err)
				return err
			}
		}

		logger.Info("Batch completed successfully", "batch", i/batchSize+1)
	}

	logger.Info("All files reprocessed successfully", "totalFiles", len(listFilesResult.FileUIDs))

	// ========== Phase 3: Synchronize ==========
	logger.Info("Phase 3: Synchronize - Final synchronization before swap")

	// Synchronization needs extended retries during rapid operations (e.g., CC3 test)
	// where multiple files are uploaded/deleted concurrently and may take time to complete processing
	// IMPORTANT: We handle retries at workflow level (not activity level) to pass reconciled file UIDs between attempts
	syncCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: ActivityTimeoutLong,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1, // NO activity retries - we handle retries in workflow
		},
	})

	// Track reconciled file UIDs across retry attempts
	var reconciledFileUIDs []types.FileUIDType
	maxSyncAttempts := 20
	var syncResult SynchronizeKBActivityResult

	for attempt := 1; attempt <= maxSyncAttempts; attempt++ {
		err = workflow.ExecuteActivity(syncCtx, w.SynchronizeKBActivity, &SynchronizeKBActivityParam{
			OriginalKBUID:              param.OriginalKBUID,
			StagingKBUID:               stagingKBUID,
			RecentlyReconciledFileUIDs: reconciledFileUIDs, // Pass UIDs from previous attempt
		}).Get(syncCtx, &syncResult)

		if err == nil {
			// Success!
			logger.Info("Synchronization complete - KB locked and all files processed",
				"attempts", attempt)
			break
		}

		// Check if this is a non-retryable error (e.g., KB deleted)
		// Non-retryable errors should fail immediately, not retry 20 times
		var applicationErr *temporal.ApplicationError
		if errors.As(err, &applicationErr) && applicationErr.NonRetryable() {
			logger.Error("Synchronization failed with non-retryable error (failing immediately)",
				"error", err,
				"attempt", attempt)
			return temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("Synchronization failed with non-retryable error (attempt %d): %s", attempt, errorsx.MessageOrErr(err)),
				"UpdateKnowledgeBaseWorkflow",
				err,
			)
		}

		// Check if we got reconciled file UIDs in the result (even with error)
		if len(syncResult.ReconciledFileUIDs) > 0 {
			reconciledFileUIDs = append(reconciledFileUIDs, syncResult.ReconciledFileUIDs...)
			logger.Info("Reconciliation created files, will exclude from NOTSTARTED check on next attempt",
				"reconciledFileCount", len(syncResult.ReconciledFileUIDs),
				"totalReconciledFileCount", len(reconciledFileUIDs))
		}

		// If this was the last attempt, return the error
		if attempt >= maxSyncAttempts {
			logger.Error("Synchronization failed after maximum attempts", "error", err, "attempts", attempt)
			return fmt.Errorf("synchronization failed after %d attempts: %w", attempt, err)
		}

		// Sleep with exponential backoff before retry
		sleepDuration := workflow.Now(ctx).Add(time.Duration(attempt) * time.Second).Sub(workflow.Now(ctx))
		err := workflow.Sleep(ctx, sleepDuration)
		if err != nil {
			logger.Error("Sleep interrupted", "error", err)
			return err
		}
	}

	logger.Info("Synchronization complete - KB locked and all files processed")

	// ========== Phase 4: Validate ==========
	logger.Info("Phase 4: Validate - Validating KB resource integrity")

	if config.Config.RAG.Update.ValidationEnabled {
		// Validation should NEVER be retried - if it fails, it indicates a real bug in the locking/synchronization
		// Retries just mask failures and waste time. With proper locking, validation passes on first attempt.
		validationCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: ActivityTimeoutStandard, // 5 min timeout
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 1, // NO RETRIES - fail fast to expose bugs
			},
		})

		var validationResult ValidateUpdatedKBActivityResult
		err = workflow.ExecuteActivity(validationCtx, w.ValidateUpdatedKBActivity, &ValidateUpdatedKBActivityParam{
			OriginalKBUID:     param.OriginalKBUID,
			StagingKBUID:      stagingKBUID,
			ExpectedFileCount: len(listFilesResult.FileUIDs),
		}).Get(validationCtx, &validationResult)
		if err != nil || !validationResult.Success {
			logger.Error("Validation failed", "error", err, "validationErrors", validationResult.Errors)
			return fmt.Errorf("validation failed: %v", validationResult.Errors)
		}
	}

	// ========== Phase 5: Swap ==========
	logger.Info("Phase 5: Swap - Performing atomic swap")

	var swapResult SwapKnowledgeBasesActivityResult
	err = workflow.ExecuteActivity(ctx, w.SwapKnowledgeBasesActivity, &SwapKnowledgeBasesActivityParam{
		OriginalKBUID: param.OriginalKBUID,
		StagingKBUID:  stagingKBUID,
		RetentionDays: config.Config.RAG.Update.RollbackRetentionDays,
	}).Get(ctx, &swapResult)
	if err != nil {
		logger.Error("Failed to perform atomic swap", "error", err)
		return err
	}

	logger.Info("Atomic swap completed successfully",
		"rollbackKBUID", swapResult.RollbackKBUID.String(),
		"stagingKBUID", swapResult.StagingKBUID.String(),
		"newProductionCollectionUID", swapResult.NewProductionCollectionUID.String())

	// CRITICAL: Trigger staging KB cleanup with protected collection UID
	// This is deterministic and prevents race conditions - the cleanup workflow
	// will NOT drop the collection that was just swapped to production
	logger.Info("Triggering staging KB cleanup (deterministic protection for new production collection)")
	cleanupCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    3,
		},
	})
	if err := workflow.ExecuteActivity(cleanupCtx, w.CleanupOldKnowledgeBaseActivity, &CleanupOldKnowledgeBaseActivityParam{
		KBUID:                  swapResult.StagingKBUID,
		ProtectedCollectionUID: &swapResult.NewProductionCollectionUID,
	}).Get(cleanupCtx, nil); err != nil {
		// Log error but don't fail workflow - swap already succeeded
		// Orphaned staging KB will need manual cleanup
		logger.Error("Failed to cleanup staging KB after successful swap - manual cleanup may be needed",
			"stagingKBUID", swapResult.StagingKBUID.String(),
			"error", err)
	}

	// ========== Phase 6: Cleanup ==========
	logger.Info("Phase 6: Cleanup - Scheduling rollback KB deletion after retention period")

	// Schedule cleanup workflow for ROLLBACK KB after retention period
	retentionDuration := time.Duration(config.Config.RAG.Update.RollbackRetentionDays) * 24 * time.Hour
	retentionSeconds := int64(retentionDuration.Seconds())

	childWorkflowOptions := workflow.ChildWorkflowOptions{
		WorkflowID: fmt.Sprintf("cleanup-rollback-kb-%s", swapResult.RollbackKBUID.String()),
	}
	rollbackCleanupCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)

	// Start cleanup workflow with retention delay (it will wait before cleanup)
	// Note: We start the child workflow but don't wait for it to complete (fire-and-forget)
	// The workflow will run in background and clean up the rollback KB after retention period
	childWF := workflow.ExecuteChildWorkflow(rollbackCleanupCtx, w.CleanupKnowledgeBaseWorkflow, CleanupKnowledgeBaseWorkflowParam{
		KBUID:               swapResult.RollbackKBUID,
		CleanupAfterSeconds: retentionSeconds,
	})

	// Get the workflow execution to verify it started (but don't wait for completion)
	if err := childWF.GetChildWorkflowExecution().Get(rollbackCleanupCtx, nil); err != nil {
		// Log error but don't fail - rollback KB can be cleaned up manually later
		logger.Error("Failed to start cleanup workflow for rollback KB - manual cleanup may be needed",
			"rollbackKBUID", swapResult.RollbackKBUID.String(),
			"retentionDays", config.Config.RAG.Update.RollbackRetentionDays,
			"error", err)
	} else {
		// Workflow started successfully - it will run in background
		logger.Info("Cleanup workflow scheduled for rollback KB",
			"rollbackKBUID", swapResult.RollbackKBUID.String(),
			"retentionSeconds", retentionSeconds,
			"retentionDays", config.Config.RAG.Update.RollbackRetentionDays)
	}

	// Mark update as completed
	updateCompleted = true

	// CRITICAL: Set final COMPLETED status and clear workflow_id
	// This allows the KB to be deleted and prevents knowledge base pileup
	err = workflow.ExecuteActivity(ctx, w.UpdateKnowledgeBaseUpdateStatusActivity, &UpdateKnowledgeBaseUpdateStatusActivityParam{
		KBUID:             param.OriginalKBUID,
		Status:            artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED.String(),
		WorkflowID:        workflowID,                    // Will be cleared to NULL by UpdateKnowledgeBaseUpdateStatus for terminal states
		PreviousSystemUID: types.SystemUIDType(uuid.Nil), // Don't update, already set when status changed to UPDATING
	}).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to set final COMPLETED status (non-fatal)", "error", err)
		// Non-fatal: workflow still succeeded, just log the error
	}

	logger.Info("UpdateKnowledgeBaseWorkflow completed successfully",
		"originalKBUID", param.OriginalKBUID.String(),
		"stagingKBUID", stagingKBUID.String())

	return nil
}
