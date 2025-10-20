package worker

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/types"
)

// UpdateKnowledgeBaseWorkflowParam defines parameters for updating a single KB
type UpdateKnowledgeBaseWorkflowParam struct {
	OriginalKBUID types.KBUIDType
	UserUID       types.UserUIDType
	RequesterUID  types.RequesterUIDType
	// SystemProfile specifies which system profile to use for the new embedding config
	// If empty, uses the KB's current embedding config (useful for reprocessing)
	SystemProfile string
}

// UpdateKnowledgeBaseWorkflow upgrades a single knowledge base through 6 phases:
// 1. Prepare: Create staging KB with new collection
// 2. Reprocess: Clone and reprocess all files with new config
// 3. Synchronize: Lock KB and wait for all dual-processed files to complete
// 4. Validate: Verify data integrity (file counts, embeddings, chunks)
// 5. Swap: Atomic pointer swap of collections and metadata
// 6. Cleanup: Schedule rollback KB deletion after retention period
func (w *Worker) UpdateKnowledgeBaseWorkflow(ctx workflow.Context, param UpdateKnowledgeBaseWorkflowParam) error {
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
	upgradeCompleted := false

	// Defer cleanup on failure
	defer func() {
		if !upgradeCompleted && stagingKBUID.String() != "" {
			// Cleanup failed upgrade - delete staging KB
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

			logger.Warn("Upgrade failed, cleaning up staging KB",
				"stagingKBUID", stagingKBUID)

			_ = workflow.ExecuteActivity(cleanupCtx, w.CleanupOldKnowledgeBaseActivity, &CleanupOldKnowledgeBaseActivityParam{
				KBUID: stagingKBUID,
			}).Get(cleanupCtx, nil)

			// Mark original KB as failed
			_ = workflow.ExecuteActivity(cleanupCtx, w.UpdateKnowledgeBaseUpdateStatusActivity, &UpdateKnowledgeBaseUpdateStatusActivityParam{
				KBUID:      param.OriginalKBUID,
				Status:     "failed",
				WorkflowID: workflowID,
			}).Get(cleanupCtx, nil)
		}
	}()

	// ========== Phase 1: Prepare ==========
	logger.Info("Phase 1: Prepare - Validating and creating staging KB")

	// Validate eligibility FIRST before marking as updating
	err := workflow.ExecuteActivity(ctx, w.ValidateUpdateEligibilityActivity, &ValidateUpdateEligibilityActivityParam{
		KBUID: param.OriginalKBUID,
	}).Get(ctx, nil)
	if err != nil {
		logger.Error("KB is not eligible for update", "error", err)
		return err
	}

	// Update original KB status to updating after validation passes
	err = workflow.ExecuteActivity(ctx, w.UpdateKnowledgeBaseUpdateStatusActivity, &UpdateKnowledgeBaseUpdateStatusActivityParam{
		KBUID:      param.OriginalKBUID,
		Status:     "updating",
		WorkflowID: workflowID,
	}).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to update KB status to updating", "error", err)
		return err
	}

	// Create staging KB
	var stagingResult CreateStagingKnowledgeBaseActivityResult
	err = workflow.ExecuteActivity(ctx, w.CreateStagingKnowledgeBaseActivity, &CreateStagingKnowledgeBaseActivityParam{
		OriginalKBUID: param.OriginalKBUID,
		SystemProfile: param.SystemProfile,
	}).Get(ctx, &stagingResult)
	if err != nil {
		logger.Error("Failed to create staging KB", "error", err)
		return err
	}

	stagingKBUID = stagingResult.StagingKB.UID
	logger.Info("Staging KB created", "stagingKBUID", stagingKBUID.String())

	// ========== Phase 2: Reprocess ==========
	logger.Info("Phase 2: Reprocess - Reprocessing all files")

	// List all files from original KB
	var listFilesResult ListFilesForReprocessingActivityResult
	err = workflow.ExecuteActivity(ctx, w.ListFilesForReprocessingActivity, &ListFilesForReprocessingActivityParam{
		KBUID: param.OriginalKBUID,
	}).Get(ctx, &listFilesResult)
	if err != nil {
		logger.Error("Failed to list files for reprocessing", "error", err)
		return err
	}

	logger.Info("Files to reprocess", "count", len(listFilesResult.FileUIDs))

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

			// Trigger ProcessFileWorkflow for the cloned file
			childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
				WorkflowID: fmt.Sprintf("process-file-%s-upgrade", cloneResult.NewFileUID.String()),
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

	var syncResult SynchronizeKBActivityResult
	err = workflow.ExecuteActivity(ctx, w.SynchronizeKBActivity, &SynchronizeKBActivityParam{
		OriginalKBUID: param.OriginalKBUID,
		StagingKBUID:  stagingKBUID,
	}).Get(ctx, &syncResult)
	if err != nil {
		logger.Error("Synchronization failed", "error", err)
		return fmt.Errorf("synchronization failed: %w", err)
	}

	logger.Info("Synchronization complete - KB locked and all files processed")

	// ========== Phase 4: Validate ==========
	logger.Info("Phase 4: Validate - Validating KB resource integrity")

	if config.Config.RAG.Update.ValidationEnabled {
		var validationResult ValidateUpdatedKBActivityResult
		err = workflow.ExecuteActivity(ctx, w.ValidateUpdatedKBActivity, &ValidateUpdatedKBActivityParam{
			OriginalKBUID:     param.OriginalKBUID,
			StagingKBUID:      stagingKBUID,
			ExpectedFileCount: len(listFilesResult.FileUIDs),
		}).Get(ctx, &validationResult)
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
		"stagingKBUID", swapResult.StagingKBUID.String())

	// CRITICAL: Wait for swap transaction to be fully committed and visible to all queries
	// This prevents a race condition where IsCollectionInUse might not see the updated
	// active_collection_uid due to PostgreSQL transaction isolation
	// Extended delay ensures database transaction is visible across all connections
	logger.Info("Waiting for swap transaction to commit before cleanup")
	_ = workflow.Sleep(ctx, 5*time.Second)

	// CRITICAL: Trigger staging KB cleanup NOW (after swap transaction is committed)
	// This ensures the production KB's active_collection_uid update is visible before
	// the cleanup workflow checks IsCollectionInUse
	logger.Info("Triggering staging KB cleanup (after swap transaction committed)")
	cleanupCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    3,
		},
	})
	_ = workflow.ExecuteActivity(cleanupCtx, w.CleanupOldKnowledgeBaseActivity, &CleanupOldKnowledgeBaseActivityParam{
		KBUID: swapResult.StagingKBUID,
	}).Get(cleanupCtx, nil)

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
	_ = workflow.ExecuteChildWorkflow(rollbackCleanupCtx, w.CleanupKnowledgeBaseWorkflow, CleanupKnowledgeBaseWorkflowParam{
		KBUID:               swapResult.RollbackKBUID,
		CleanupAfterSeconds: retentionSeconds,
	})

	// Don't wait for cleanup - let it run in background
	logger.Info("Cleanup workflow scheduled for rollback KB",
		"rollbackKBUID", swapResult.RollbackKBUID.String(),
		"retentionSeconds", retentionSeconds,
		"retentionDays", config.Config.RAG.Update.RollbackRetentionDays)

	// Mark upgrade as completed
	upgradeCompleted = true

	logger.Info("UpdateKnowledgeBaseWorkflow completed successfully",
		"originalKBUID", param.OriginalKBUID.String(),
		"stagingKBUID", stagingKBUID.String())

	return nil
}
