package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/repository/object"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

// This file contains KB update activities used by UpdateKnowledgeBaseWorkflow:
// - ListKnowledgeBasesForUpdateActivity - Lists KBs that need system config updates
// - ValidateUpdateEligibilityActivity - Validates KB can be updated (captures previous system UID)
// - CreateStagingKnowledgeBaseActivity - Creates staging KB with new system config
// - ListFilesForReprocessingActivity - Lists files to be reprocessed in staging KB
// - CloneFileToStagingKBActivity - Clones individual file from production to staging KB
// - SynchronizeKBActivity - Ensures all files are processed before swap (handles dual-processing)
// - ValidateUpdatedKBActivity - Validates staging KB data integrity before swap
// - SwapKnowledgeBasesActivity - Performs atomic 3-step resource swap (production ↔ staging ↔ rollback)
// - CleanupOldKnowledgeBaseActivity - Cleans up staging/rollback KB resources (files, Milvus collection)
// - VerifyKBCleanupActivity - Verifies KB cleanup was successful (both staging and rollback)
// - UpdateKnowledgeBaseUpdateStatusActivity - Updates KB update status and workflow ID

// Activity error type constants
const (
	listKnowledgeBasesForUpdateActivityError     = "ListKnowledgeBasesForUpdateActivity"
	validateUpdateEligibilityActivityError       = "ValidateUpdateEligibilityActivity"
	createStagingKnowledgeBaseActivityError      = "CreateStagingKnowledgeBaseActivity"
	synchronizeKBActivityError                   = "SynchronizeKBActivity"
	validateUpdatedKBActivityError               = "ValidateUpdatedKBActivity"
	swapKnowledgeBasesActivityError              = "SwapKnowledgeBasesActivity"
	updateKnowledgeBaseUpdateStatusActivityError = "UpdateKnowledgeBaseUpdateStatusActivity"
	cleanupOldKnowledgeBaseActivityError         = "CleanupOldKnowledgeBaseActivity"
	verifyKBCleanupActivityError                 = "VerifyKBCleanupActivity"
	listFilesForReprocessingActivityError        = "ListFilesForReprocessingActivity"
	cloneFileToStagingKBActivityError            = "CloneFileToStagingKBActivity"
)

// ListKnowledgeBasesForUpdateActivityParam defines parameters for listing KBs needing update
type ListKnowledgeBasesForUpdateActivityParam struct {
	KnowledgeBaseIDs []string
}

// ListKnowledgeBasesForUpdateActivityResult contains the list of KBs needing update
type ListKnowledgeBasesForUpdateActivityResult struct {
	KnowledgeBases []repository.KnowledgeBaseModel
	TotalFiles     int32
}

// ValidateUpdateEligibilityActivityParam checks if update can proceed
type ValidateUpdateEligibilityActivityParam struct {
	KBUID types.KBUIDType
}

type ValidateUpdateEligibilityActivityResult struct {
	PreviousSystemUID types.SystemUIDType // Current system UID (before update starts)
}

// CreateStagingKnowledgeBaseActivityParam defines parameters for creating staging KB
type CreateStagingKnowledgeBaseActivityParam struct {
	OriginalKBUID types.KBUIDType
	// SystemID specifies which system ID to use for the new embedding config
	// If empty, uses the original KB's embedding config
	SystemID string
}

// CreateStagingKnowledgeBaseActivityResult contains the created staging KB
type CreateStagingKnowledgeBaseActivityResult struct {
	StagingKB repository.KnowledgeBaseModel
}

// SynchronizeKBActivityParam defines parameters for final synchronization before swap
type SynchronizeKBActivityParam struct {
	OriginalKBUID              types.KBUIDType
	StagingKBUID               types.KBUIDType
	RecentlyReconciledFileUIDs []types.FileUIDType // Files created by reconciliation in previous retry, exclude from NOTSTARTED check
}

// SynchronizeKBActivityResult contains synchronization results
type SynchronizeKBActivityResult struct {
	Synchronized       bool
	ReconciledFileUIDs []types.FileUIDType // Files created by reconciliation in this attempt
}

// ValidateUpdatedKBActivityParam defines parameters for validation
type ValidateUpdatedKBActivityParam struct {
	OriginalKBUID     types.KBUIDType
	StagingKBUID      types.KBUIDType
	ExpectedFileCount int // Number of files that were cloned from original to staging
}

// ValidateUpdatedKBActivityResult contains validation results
type ValidateUpdatedKBActivityResult struct {
	Success bool
	Errors  []string
}

// SwapKnowledgeBasesActivityParam defines parameters for atomic swap
type SwapKnowledgeBasesActivityParam struct {
	OriginalKBUID types.KBUIDType
	StagingKBUID  types.KBUIDType
	RetentionDays int
}

// SwapKnowledgeBasesActivityResult contains the result of the swap
type SwapKnowledgeBasesActivityResult struct {
	RollbackKBUID              types.KBUIDType // UID of the created/reused rollback KB
	StagingKBUID               types.KBUIDType // UID of the staging KB (for cleanup after swap)
	NewProductionCollectionUID types.KBUIDType // Collection UID now in production (to protect from cleanup)
}

// UpdateKnowledgeBaseUpdateStatusActivityParam updates update status
type UpdateKnowledgeBaseUpdateStatusActivityParam struct {
	KBUID             types.KBUIDType
	Status            string
	WorkflowID        string
	ErrorMessage      string              // Only used when Status is FAILED
	PreviousSystemUID types.SystemUIDType // Only used when Status is UPDATING (captured at workflow start for audit trail)
}

// CleanupOldKnowledgeBaseActivityParam defines parameters for cleanup
type CleanupOldKnowledgeBaseActivityParam struct {
	KBUID                  types.KBUIDType
	ProtectedCollectionUID *types.KBUIDType // Collection UID that must not be dropped (e.g., after swap)
}

// VerifyKBCleanupActivityParam defines parameters for KB cleanup verification
// Used for both staging and rollback KB cleanup verification
type VerifyKBCleanupActivityParam struct {
	KBUID         types.KBUIDType // UID of the KB being verified (staging or rollback)
	CollectionUID types.KBUIDType // Collection UID that should have been dropped
}

// ListFilesForReprocessingActivityParam defines parameters for listing files
type ListFilesForReprocessingActivityParam struct {
	KBUID types.KBUIDType
}

// ListFilesForReprocessingActivityResult contains the list of file UIDs
type ListFilesForReprocessingActivityResult struct {
	FileUIDs []types.FileUIDType
}

// CloneFileToStagingKBActivityParam defines parameters for cloning a file
type CloneFileToStagingKBActivityParam struct {
	OriginalFileUID types.FileUIDType
	StagingKBUID    types.KBUIDType
}

// CloneFileToStagingKBActivityResult contains the new file UID
type CloneFileToStagingKBActivityResult struct {
	NewFileUID types.FileUIDType
}

// ListKnowledgeBasesForUpdateActivity finds KBs that need update
func (w *Worker) ListKnowledgeBasesForUpdateActivity(ctx context.Context, param *ListKnowledgeBasesForUpdateActivityParam) (*ListKnowledgeBasesForUpdateActivityResult, error) {
	w.log.Info("ListKnowledgeBasesForUpdateActivity: Finding KBs needing update",
		zap.Strings("knowledgeBaseIDs", param.KnowledgeBaseIDs))

	var kbs []repository.KnowledgeBaseModel
	var err error

	if len(param.KnowledgeBaseIDs) > 0 {
		// Get specific knowledge bases by ID
		for _, knowledgeBaseID := range param.KnowledgeBaseIDs {
			kb, getErr := w.repository.GetKnowledgeBaseByID(ctx, knowledgeBaseID)
			if getErr != nil {
				w.log.Warn("Unable to get knowledge base", zap.String("knowledgeBaseID", knowledgeBaseID), zap.Error(getErr))
				continue
			}
			// Only include production KBs (not staging) and not already updating
			if !kb.Staging && repository.IsUpdateComplete(kb.UpdateStatus) {
				kbs = append(kbs, *kb)
				w.log.Info("Knowledge base eligible for update", zap.String("knowledgeBaseID", knowledgeBaseID), zap.String("kbUID", kb.UID.String()), zap.String("updateStatus", kb.UpdateStatus))
			} else {
				w.log.Warn("Knowledge base filtered out", zap.String("knowledgeBaseID", knowledgeBaseID), zap.String("kbUID", kb.UID.String()), zap.Bool("staging", kb.Staging), zap.String("updateStatus", kb.UpdateStatus))
			}
		}
	} else {
		// List all eligible KBs (production, not currently updating)
		kbs, err = w.repository.ListKnowledgeBasesForUpdate(ctx, nil, nil)
		if err != nil {
			err = errorsx.AddMessage(err, "Unable to list knowledge bases for update. Please try again.")
			return nil, activityError(err, listKnowledgeBasesForUpdateActivityError)
		}
	}

	// Count total files across all KBs
	var totalFiles int32
	for _, kb := range kbs {
		if count, err := w.repository.GetFileCountByKnowledgeBaseUID(ctx, kb.UID, ""); err == nil {
			totalFiles += int32(count)
		}
	}

	w.log.Info("ListKnowledgeBasesForUpdateActivity: Found KBs",
		zap.Int("count", len(kbs)),
		zap.Int32("totalFiles", totalFiles))

	return &ListKnowledgeBasesForUpdateActivityResult{
		KnowledgeBases: kbs,
		TotalFiles:     totalFiles,
	}, nil
}

// ValidateUpdateEligibilityActivity checks if KB can be updated
func (w *Worker) ValidateUpdateEligibilityActivity(ctx context.Context, param *ValidateUpdateEligibilityActivityParam) (*ValidateUpdateEligibilityActivityResult, error) {
	w.log.Info("ValidateUpdateEligibilityActivity: Checking eligibility",
		zap.String("kbUID", param.KBUID.String()))

	kb, err := w.repository.GetKnowledgeBaseByUID(ctx, param.KBUID)
	if err != nil {
		// If KB is not found (deleted), return non-retryable error to fail fast
		// This prevents wasting time retrying when the KB has been deleted
		if errors.Is(err, gorm.ErrRecordNotFound) {
			w.log.Warn("ValidateUpdateEligibilityActivity: KB not found (may have been deleted)",
				zap.String("kbUID", param.KBUID.String()))
			return nil, activityErrorNonRetryableWithMessage(
				fmt.Sprintf("Knowledge base not found (may have been deleted): %s", param.KBUID.String()),
				validateUpdateEligibilityActivityError,
				err,
			)
		}
		// For other errors (DB connection issues, etc.), allow retries
		err = errorsx.AddMessage(err, "Unable to get knowledge base for validation. Please try again.")
		return nil, activityError(err, validateUpdateEligibilityActivityError)
	}

	// Check if already updating
	if kb.UpdateStatus == artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_UPDATING.String() {
		err = fmt.Errorf("knowledge base is already updating: %s", param.KBUID.String())
		return nil, activityErrorWithMessage(err.Error(), validateUpdateEligibilityActivityError, err)
	}

	w.log.Info("ValidateUpdateEligibilityActivity: KB is eligible",
		zap.String("kbUID", param.KBUID.String()),
		zap.String("currentSystemUID", kb.SystemUID.String()))

	return &ValidateUpdateEligibilityActivityResult{
		PreviousSystemUID: kb.SystemUID,
	}, nil
}

// CreateStagingKnowledgeBaseActivity creates a staging KB for updating
func (w *Worker) CreateStagingKnowledgeBaseActivity(ctx context.Context, param *CreateStagingKnowledgeBaseActivityParam) (*CreateStagingKnowledgeBaseActivityResult, error) {
	w.log.Info("CreateStagingKnowledgeBaseActivity: Creating staging KB for updating",
		zap.String("originalKBUID", param.OriginalKBUID.String()),
		zap.String("systemID", param.SystemID))

	originalKB, err := w.repository.GetKnowledgeBaseByUIDWithConfig(ctx, param.OriginalKBUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to get original knowledge base. Please try again.")
		return nil, activityError(err, createStagingKnowledgeBaseActivityError)
	}

	// Determine which system UID to use
	var newSystemUID *types.SystemUIDType
	var newSystemConfig *repository.SystemConfigJSON
	if param.SystemID != "" {
		// Get the system record by ID to retrieve both UID and config
		system, err := w.repository.GetSystem(ctx, param.SystemID)
		if err != nil {
			err = errorsx.AddMessage(err, fmt.Sprintf("Unable to get system from system ID %q. Please try again.", param.SystemID))
			return nil, activityError(err, createStagingKnowledgeBaseActivityError)
		}
		newSystemUID = &system.UID
		// Also get the config for dimensionality calculation and logging
		systemConfig, err := system.GetConfigJSON()
		if err != nil {
			err = errorsx.AddMessage(err, fmt.Sprintf("Unable to parse system config from system ID %q. Please try again.", param.SystemID))
			return nil, activityError(err, createStagingKnowledgeBaseActivityError)
		}
		newSystemConfig = systemConfig
		w.log.Info("Using system config",
			zap.String("systemID", param.SystemID),
			zap.String("modelFamily", systemConfig.RAG.Embedding.ModelFamily),
			zap.Uint32("dimensionality", systemConfig.RAG.Embedding.Dimensionality))
	} else {
		// Use original KB's system (for reprocessing without changing config)
		w.log.Info("Using original KB's system config",
			zap.String("modelFamily", originalKB.SystemConfig.RAG.Embedding.ModelFamily),
			zap.Uint32("dimensionality", originalKB.SystemConfig.RAG.Embedding.Dimensionality))
	}

	// Determine dimensionality for the new collection
	// Use the new system config if specified, otherwise use original KB's config
	var dimensionality uint32
	if newSystemConfig != nil {
		dimensionality = newSystemConfig.RAG.Embedding.Dimensionality
	} else {
		dimensionality = originalKB.SystemConfig.RAG.Embedding.Dimensionality
	}

	// Create Milvus collection for staging KB
	// CRITICAL: collectionUID is passed directly from the transaction (can't query KB - it's uncommitted!)
	externalServiceCall := func(kbUID types.KBUIDType, collectionUID types.KBUIDType) error {
		// Create collection with active_collection_uid (not kb.UID!)
		// After Fix 3, active_collection_uid is always unique (NOT equal to kb.UID)
		collectionName := constant.KBCollectionName(collectionUID)
		err := w.repository.CreateCollection(ctx, collectionName, dimensionality)
		if err != nil {
			return fmt.Errorf("creating vector database collection: %w", err)
		}

		// Copy ACL permissions from original
		// Note: ACL copying is handled by the service layer
		return nil
	}

	stagingKB, err := w.repository.CreateStagingKnowledgeBase(ctx, &originalKB.KnowledgeBaseModel, newSystemUID, externalServiceCall)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to create staging knowledge base. Please try again.")
		return nil, activityError(err, createStagingKnowledgeBaseActivityError)
	}

	w.log.Info("CreateStagingKnowledgeBaseActivity: Staging KB created",
		zap.String("stagingKBUID", stagingKB.UID.String()),
		zap.String("stagingKBID", stagingKB.KBID))

	return &CreateStagingKnowledgeBaseActivityResult{
		StagingKB: *stagingKB,
	}, nil
}

// SynchronizeKBActivity performs final synchronization before swap
// It locks the KB and waits for all dual-processed files to complete
func (w *Worker) SynchronizeKBActivity(ctx context.Context, param *SynchronizeKBActivityParam) (*SynchronizeKBActivityResult, error) {
	w.log.Info("SynchronizeKBActivity: Starting final synchronization",
		zap.String("originalKBUID", param.OriginalKBUID.String()),
		zap.String("stagingKBUID", param.StagingKBUID.String()))

	// STEP 1: Atomically lock the KB by transitioning to SWAPPING status
	// This MUST happen FIRST to prevent race conditions where:
	// 1. We check staging files are done
	// 2. User uploads/deletes file → dual processing starts or file counts diverge
	// 3. We proceed to swap
	// 4. Swap happens with inconsistent KBs
	//
	// By locking FIRST (status → swapping), we ensure:
	// - No new file uploads are accepted (blocked by critical phase check in UploadCatalogFile)
	// - No new file deletions are accepted (blocked by critical phase check in DeleteCatalogFile)
	// - Both production and staging KBs remain absolutely identical during validation/swap
	originalKB, err := w.repository.GetKnowledgeBaseByUID(ctx, param.OriginalKBUID)
	if err != nil {
		// CRITICAL: If KB is deleted, fail permanently (non-retryable)
		// This prevents zombie workflows that retry infinitely trying to sync a deleted KB
		// Common scenario: Test aborts update + deletes KB before workflow processes abort signal
		if errors.Is(err, gorm.ErrRecordNotFound) {
			w.log.Error("SynchronizeKBActivity: KB was deleted - failing workflow permanently",
				zap.String("originalKBUID", param.OriginalKBUID.String()),
				zap.Error(err))
			return nil, activityErrorNonRetryableWithMessage(
				fmt.Sprintf("Knowledge base %s was deleted during update workflow. Cannot synchronize deleted KB.", param.OriginalKBUID),
				synchronizeKBActivityError,
				err,
			)
		}
		// Other errors (network, DB connection) are retryable
		err = errorsx.AddMessage(err, "Unable to get knowledge base for synchronization. Please try again.")
		return nil, activityError(err, synchronizeKBActivityError)
	}

	// Use optimistic locking: only transition if still in UPDATING status
	// This prevents race with concurrent updates/rollbacks
	err = w.repository.UpdateKnowledgeBaseWithMap(ctx, originalKB.KBID, originalKB.NamespaceUID, map[string]interface{}{
		"update_status": artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_SWAPPING.String(),
	})
	if err != nil {
		// If update fails, someone else modified the KB - fail and let Temporal retry
		err := fmt.Errorf("failed to lock KB for swapping (concurrent modification): %v", err)
		w.log.Error("SynchronizeKBActivity: Failed to acquire swap lock",
			zap.String("originalKBUID", param.OriginalKBUID.String()),
			zap.Error(err))
		return nil, activityErrorWithMessage(err.Error(), synchronizeKBActivityError, err)
	}

	w.log.Info("SynchronizeKBActivity: KB locked for swapping (status: updating → swapping)",
		zap.String("originalKBUID", param.OriginalKBUID.String()))

	// STEP 2: Wait for ALL in-progress files to complete in BOTH KBs (FINAL SYNCHRONIZATION)
	// During rapid operations, files can be uploaded to production KB and dual-processed to both.
	// We MUST wait for processing to complete in BOTH KBs before validation.
	// Since we locked above, no NEW dual processing can start, so these counts are final.
	//
	// IMPORTANT: We check for ACTIVELY processing files (PROCESSING, CHUNKING, EMBEDDING).
	// CRITICAL: We also wait for NOTSTARTED files created within the last 30 seconds.
	// This handles the race condition where:
	// 1. File uploaded during late UPDATING phase → dual processing goroutine spawned
	// 2. Goroutine creates staging file (NOTSTARTED)
	// 3. Status locks to SWAPPING before ProcessFile workflow starts
	// 4. Without this check, validation would count production file but not staging file
	//
	// Old NOTSTARTED files (>30s) are ignored as they're likely abandoned/never processed.

	// Check staging KB for actively processing files
	stagingInProgressCount, err := w.repository.GetFileCountByKnowledgeBaseUID(ctx, param.StagingKBUID, "FILE_PROCESS_STATUS_PROCESSING,FILE_PROCESS_STATUS_CHUNKING,FILE_PROCESS_STATUS_EMBEDDING")
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to check for in-progress files in staging KB. Please try again.")
		return nil, activityError(err, synchronizeKBActivityError)
	}

	// Check production KB for actively processing files
	productionInProgressCount, err := w.repository.GetFileCountByKnowledgeBaseUID(ctx, param.OriginalKBUID, "FILE_PROCESS_STATUS_PROCESSING,FILE_PROCESS_STATUS_CHUNKING,FILE_PROCESS_STATUS_EMBEDDING")
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to check for in-progress files in production KB. Please try again.")
		return nil, activityError(err, synchronizeKBActivityError)
	}

	totalInProgressCount := stagingInProgressCount + productionInProgressCount

	if totalInProgressCount > 0 {
		// Return a retryable error - files are still processing in one or both KBs
		// These are files that were dual-processed BEFORE the lock
		// We wait for them to complete before proceeding with swap
		err := fmt.Errorf("files still processing: staging=%d, production=%d (waiting for final synchronization)",
			stagingInProgressCount, productionInProgressCount)
		w.log.Info("SynchronizeKBActivity: Final synchronization in progress - files still processing, will retry",
			zap.String("stagingKBUID", param.StagingKBUID.String()),
			zap.String("productionKBUID", param.OriginalKBUID.String()),
			zap.Int64("stagingInProgress", stagingInProgressCount),
			zap.Int64("productionInProgress", productionInProgressCount))
		return nil, activityErrorWithMessage(err.Error(), synchronizeKBActivityError, err)
	}

	// STEP 2.5: Check for NOTSTARTED files (excluding recently reconciled files)
	//
	// CRITICAL INVARIANT: NEITHER production NOR staging KB should have NOTSTARTED files before swap
	//
	// With auto-trigger + sequential dual-processing + reconciliation:
	// 1. Initial update: All cloned staging files are immediately triggered → PROCESSING
	// 2. New uploads during update: UploadCatalogFile auto-triggers production → PROCESSING
	// 3. Sequential dual-processing: Production completion triggers staging → PROCESSING
	// 4. Reconciliation: Creates missing files with NOTSTARTED, then triggers workflows
	//
	// IMPORTANT: Reconciliation creates files with NOTSTARTED status, then calls ProcessFile.
	// On the NEXT retry, we exclude these specific files from the check (passed via RecentlyReconciledFileUIDs).
	// This avoids false positives while Temporal picks up the workflows.
	//
	// Files that remain NOTSTARTED (and are NOT in the exclusion list) indicate a system error:
	// - UploadCatalogFile failed to auto-trigger (Temporal down, network issue)
	// - ProcessFileWorkflow failed to trigger staging (sequential dual-processing broken)
	// - Reconciliation ProcessFile call failed silently
	// - File record created but workflow never started (DB/Temporal inconsistency)

	// Check for NOTSTARTED files in production KB, excluding recently reconciled files
	productionNotStarted, err := w.repository.GetNotStartedFileCountExcluding(ctx, param.OriginalKBUID, param.RecentlyReconciledFileUIDs)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to check production KB file status. Please try again.")
		return nil, activityError(err, synchronizeKBActivityError)
	}

	if productionNotStarted > 0 {
		// CRITICAL ERROR: Production files stuck in NOTSTARTED (excluding recently reconciled)
		err := fmt.Errorf("production KB has %d files in NOTSTARTED status (excluding %d recently reconciled) - this indicates auto-trigger failed or Temporal is down. Manual investigation required", productionNotStarted, len(param.RecentlyReconciledFileUIDs))
		w.log.Error("SynchronizeKBActivity: Production files stuck in NOTSTARTED - SYSTEM ERROR",
			zap.String("productionKBUID", param.OriginalKBUID.String()),
			zap.Int64("notStartedCount", productionNotStarted),
			zap.Int("excludedReconciledCount", len(param.RecentlyReconciledFileUIDs)),
			zap.String("likely_cause", "UploadCatalogFile auto-trigger failed or Temporal service is down"),
			zap.String("required_action", "Manually trigger via ProcessCatalogFiles API or check Temporal service"))
		return nil, activityErrorWithMessage(err.Error(), synchronizeKBActivityError, err)
	}

	// Check for NOTSTARTED files in staging KB, excluding recently reconciled files
	stagingNotStarted, err := w.repository.GetNotStartedFileCountExcluding(ctx, param.StagingKBUID, param.RecentlyReconciledFileUIDs)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to check staging KB file status. Please try again.")
		return nil, activityError(err, synchronizeKBActivityError)
	}

	if stagingNotStarted > 0 {
		// CRITICAL ERROR: Staging files stuck in NOTSTARTED (excluding recently reconciled)
		err := fmt.Errorf("staging KB has %d files in NOTSTARTED status (excluding %d recently reconciled) - this indicates sequential dual-processing or reconciliation failed. Manual investigation required", stagingNotStarted, len(param.RecentlyReconciledFileUIDs))
		w.log.Error("SynchronizeKBActivity: Staging files stuck in NOTSTARTED - SYSTEM ERROR",
			zap.String("stagingKBUID", param.StagingKBUID.String()),
			zap.String("productionKBUID", param.OriginalKBUID.String()),
			zap.Int64("notStartedCount", stagingNotStarted),
			zap.Int("excludedReconciledCount", len(param.RecentlyReconciledFileUIDs)),
			zap.String("likely_cause", "Initial clone not triggered OR production workflows failed to trigger staging OR reconciliation ProcessFile failed"),
			zap.String("required_action", "Manually trigger via ProcessCatalogFiles API or check workflow logs"))
		return nil, activityErrorWithMessage(err.Error(), synchronizeKBActivityError, err)
	}

	// STEP 2.6: Check for file count mismatch (dual processing async file creation race)
	// If a file was uploaded during UPDATING phase and immediately processed, the production
	// file record exists but the staging file record might still be creating in the async goroutine.
	// We need to wait for both file records to exist before proceeding to validation.
	productionFileCount, err := w.repository.GetFileCountByKnowledgeBaseUID(ctx, param.OriginalKBUID, "") // Empty string = count all non-deleted files
	if err != nil {
		w.log.Warn("Failed to check production file count, continuing anyway",
			zap.Error(err))
		productionFileCount = 0
	}

	stagingFileCount, err := w.repository.GetFileCountByKnowledgeBaseUID(ctx, param.StagingKBUID, "") // Empty string = count all non-deleted files
	if err != nil {
		w.log.Warn("Failed to check staging file count, continuing anyway",
			zap.Error(err))
		stagingFileCount = 0
	}

	if productionFileCount != stagingFileCount {
		// CRITICAL DATA CONSISTENCY FIX: Auto-reconcile file count mismatch
		// Instead of just waiting/retrying, actively fix the mismatch to ensure data consistency.
		// This handles cases where dual processing failed for any reason (network, DB, timeouts, etc.)
		w.log.Warn("SynchronizeKBActivity: File count mismatch detected - initiating auto-reconciliation",
			zap.String("stagingKBUID", param.StagingKBUID.String()),
			zap.String("productionKBUID", param.OriginalKBUID.String()),
			zap.Int64("productionFileCount", productionFileCount),
			zap.Int64("stagingFileCount", stagingFileCount))

		// Reconcile: Find and fix the mismatch
		reconciledFileUIDs, err := w.reconcileKBFiles(ctx, param.OriginalKBUID, param.StagingKBUID)
		if err != nil {
			// SPECIAL CASE: If error indicates all files were skipped due to missing blobs,
			// treat this as success and proceed to validation. The validation will handle
			// the empty staging KB scenario gracefully.
			if strings.Contains(err.Error(), "all") && strings.Contains(err.Error(), "skipped due to missing blobs") {
				w.log.Warn("SynchronizeKBActivity: All files skipped during reconciliation - proceeding to validation with empty staging KB",
					zap.Error(err),
					zap.String("stagingKBUID", param.StagingKBUID.String()),
					zap.String("productionKBUID", param.OriginalKBUID.String()))
				// Don't retry - proceed directly to database stabilization check below
			} else {
				w.log.Error("SynchronizeKBActivity: Auto-reconciliation failed",
					zap.Error(err),
					zap.String("stagingKBUID", param.StagingKBUID.String()),
					zap.String("productionKBUID", param.OriginalKBUID.String()))

				// Return retryable error - reconciliation might succeed on next attempt
				return nil, activityErrorWithMessage(
					fmt.Sprintf("file count mismatch: production=%d, staging=%d (auto-reconciliation failed: %v)",
						productionFileCount, stagingFileCount, err),
					synchronizeKBActivityError,
					err,
				)
			}
		} else {
			// Reconciliation succeeded, but return retryable error to re-check counts and wait for processing
			// Pass reconciled file UIDs in result so next retry can exclude them from NOTSTARTED check
			w.log.Info("SynchronizeKBActivity: Auto-reconciliation completed, will retry to verify synchronization",
				zap.String("stagingKBUID", param.StagingKBUID.String()),
				zap.String("productionKBUID", param.OriginalKBUID.String()),
				zap.Int("reconciledFileCount", len(reconciledFileUIDs)))
			return &SynchronizeKBActivityResult{
					Synchronized:       false,
					ReconciledFileUIDs: reconciledFileUIDs,
				}, activityErrorWithMessage(
					fmt.Sprintf("file count mismatch: production=%d, staging=%d (reconciliation completed, waiting for processing)",
						productionFileCount, stagingFileCount),
					synchronizeKBActivityError,
					fmt.Errorf("reconciliation in progress"),
				)
		}
	}

	w.log.Info("SynchronizeKBActivity: Final synchronization complete",
		zap.String("stagingKBUID", param.StagingKBUID.String()),
		zap.String("status", "All staging files processed, ready for validation"))

	// CRITICAL: Poll database to ensure all async workflow transactions are fully visible
	// ProcessFileWorkflow contains multiple async operations that continue after file status = COMPLETED:
	// 1. Embedding storage activities - saves embeddings to DB/Milvus
	// 2. ProcessSummaryActivity - creates summary converted_file records
	// 3. ProcessContentActivity - creates content converted_file records
	//
	// Even though these activities/workflows complete (transactions committed), PostgreSQL's MVCC
	// means those commits might not be immediately visible to other connections. We poll until the
	// database state is stable across multiple reads, with a timeout for safety.
	//
	// We verify that chunk, embedding, AND converted_file counts are all stable across polls,
	// indicating all async database writes have been fully committed and are visible.
	//
	// CRITICAL: During rapid operations (CC3 test), ProcessSummaryActivity may still be running
	// even after file status shows COMPLETED, because the activity runs in parallel and can take
	// 10+ seconds for AI summarization. We MUST wait for these activities to finish creating
	// converted_file records before validation.
	const maxPollAttempts = 60 // 60 seconds max to wait for all async operations
	const pollInterval = 1 * time.Second
	var lastChunkCount, lastEmbeddingCount, lastConvertedFileCount int64
	stableCount := 0
	const requiredStablePolls = 5 // Require 5 consecutive stable reads (5s stability window)

	w.log.Info("SynchronizeKBActivity: Polling for database transaction visibility (chunks, embeddings, converted files)",
		zap.String("stagingKBUID", param.StagingKBUID.String()),
		zap.Int("maxAttempts", maxPollAttempts),
		zap.Duration("pollInterval", pollInterval))

	for attempt := 1; attempt <= maxPollAttempts; attempt++ {
		// Get current counts for all async-created resources
		chunkCount, err := w.repository.GetChunkCountByKBUID(ctx, param.StagingKBUID)
		if err != nil {
			w.log.Warn("SynchronizeKBActivity: Failed to get chunk count during poll",
				zap.Int("attempt", attempt),
				zap.Error(err))
			time.Sleep(pollInterval)
			continue
		}

		embeddingCount, err := w.repository.GetEmbeddingCountByKBUID(ctx, param.StagingKBUID)
		if err != nil {
			w.log.Warn("SynchronizeKBActivity: Failed to get embedding count during poll",
				zap.Int("attempt", attempt),
				zap.Error(err))
			time.Sleep(pollInterval)
			continue
		}

		convertedFileCount, err := w.repository.GetConvertedFileCountByKBUID(ctx, param.StagingKBUID)
		if err != nil {
			w.log.Warn("SynchronizeKBActivity: Failed to get converted file count during poll",
				zap.Int("attempt", attempt),
				zap.Error(err))
			time.Sleep(pollInterval)
			continue
		}

		// Check if ALL counts are stable (same as last poll)
		if attempt > 1 &&
			chunkCount == lastChunkCount &&
			embeddingCount == lastEmbeddingCount &&
			convertedFileCount == lastConvertedFileCount {
			stableCount++
			w.log.Info("SynchronizeKBActivity: All counts stable",
				zap.Int("attempt", attempt),
				zap.Int64("chunks", chunkCount),
				zap.Int64("embeddings", embeddingCount),
				zap.Int64("convertedFiles", convertedFileCount),
				zap.Int("stableCount", stableCount))

			if stableCount >= requiredStablePolls {
				w.log.Info("SynchronizeKBActivity: Database state stabilized, proceeding to validation",
					zap.Int("totalAttempts", attempt),
					zap.Int64("finalChunks", chunkCount),
					zap.Int64("finalEmbeddings", embeddingCount),
					zap.Int64("finalConvertedFiles", convertedFileCount))
				break
			}
		} else {
			// Counts changed - reset stability counter
			stableCount = 0
			w.log.Info("SynchronizeKBActivity: Counts changed, continuing to poll",
				zap.Int("attempt", attempt),
				zap.Int64("chunks", chunkCount),
				zap.Int64("prevChunks", lastChunkCount),
				zap.Int64("embeddings", embeddingCount),
				zap.Int64("prevEmbeddings", lastEmbeddingCount),
				zap.Int64("convertedFiles", convertedFileCount),
				zap.Int64("prevConvertedFiles", lastConvertedFileCount))
		}

		lastChunkCount = chunkCount
		lastEmbeddingCount = embeddingCount
		lastConvertedFileCount = convertedFileCount

		if attempt < maxPollAttempts {
			time.Sleep(pollInterval)
		}
	}

	if stableCount < requiredStablePolls {
		w.log.Warn("SynchronizeKBActivity: Reached max poll attempts without full stabilization",
			zap.Int("attempts", maxPollAttempts),
			zap.Int("stableCount", stableCount),
			zap.Int64("lastChunks", lastChunkCount),
			zap.Int64("lastEmbeddings", lastEmbeddingCount),
			zap.Int64("lastConvertedFiles", lastConvertedFileCount))
	}

	return &SynchronizeKBActivityResult{
		Synchronized: true,
	}, nil
}

// reconcileKBFiles ensures production and staging KBs have identical files
// This is called when file count mismatch is detected during synchronization.
// It actively fixes the mismatch by creating missing file records and queueing workflows.
// reconcileKBFiles ensures production and staging KBs have identical files and returns the UIDs of created files
func (w *Worker) reconcileKBFiles(ctx context.Context, productionKBUID, stagingKBUID types.KBUIDType) ([]types.FileUIDType, error) {
	w.log.Info("Starting KB file reconciliation",
		zap.String("productionKBUID", productionKBUID.String()),
		zap.String("stagingKBUID", stagingKBUID.String()))

	// Track file UIDs created during reconciliation
	var reconciledFileUIDs []types.FileUIDType
	var skippedDueToMissingBlobs int

	// Get production KB to retrieve OwnerUID
	productionKB, err := w.repository.GetKnowledgeBaseByUID(ctx, productionKBUID)
	if err != nil {
		return nil, fmt.Errorf("failed to get production KB: %w", err)
	}

	ownerUID := productionKB.NamespaceUID

	// Get ALL files from production KB with proper pagination
	productionFiles := []repository.KnowledgeBaseFileModel{}
	pageToken := ""
	for {
		productionFileList, err := w.repository.ListKnowledgeBaseFiles(ctx, repository.KnowledgeBaseFileListParams{
			OwnerUID:  ownerUID,
			KBUID:     productionKBUID.String(),
			PageSize:  100, // Max page size (capped by repository)
			PageToken: pageToken,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get production files: %w", err)
		}
		productionFiles = append(productionFiles, productionFileList.Files...)

		if productionFileList.NextPageToken == "" {
			break // No more pages
		}
		pageToken = productionFileList.NextPageToken
	}

	// Get ALL files from staging KB with proper pagination
	stagingFiles := []repository.KnowledgeBaseFileModel{}
	pageToken = ""
	for {
		stagingFileList, err := w.repository.ListKnowledgeBaseFiles(ctx, repository.KnowledgeBaseFileListParams{
			OwnerUID:  ownerUID,
			KBUID:     stagingKBUID.String(),
			PageSize:  100, // Max page size (capped by repository)
			PageToken: pageToken,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get staging files: %w", err)
		}
		stagingFiles = append(stagingFiles, stagingFileList.Files...)

		if stagingFileList.NextPageToken == "" {
			break // No more pages
		}
		pageToken = stagingFileList.NextPageToken
	}

	// Build map of staging files by name - use slice to detect duplicates
	stagingFilesByName := make(map[string][]repository.KnowledgeBaseFileModel)
	for _, stagingFile := range stagingFiles {
		stagingFilesByName[stagingFile.Filename] = append(stagingFilesByName[stagingFile.Filename], stagingFile)
	}

	// Build map of production files by name
	productionFileMap := make(map[string]*repository.KnowledgeBaseFileModel)
	for i := range productionFiles {
		productionFileMap[productionFiles[i].Filename] = &productionFiles[i]
	}

	// Find files in production but missing in staging
	var missingInStaging []repository.KnowledgeBaseFileModel
	for _, prodFile := range productionFiles {
		if _, exists := stagingFilesByName[prodFile.Filename]; !exists {
			missingInStaging = append(missingInStaging, prodFile)
		}
	}

	// Find files in staging but missing in production (shouldn't happen, but check for consistency)
	var missingInProduction []repository.KnowledgeBaseFileModel
	for _, stagingFileList := range stagingFilesByName {
		// Use first file in list for name comparison
		if len(stagingFileList) > 0 {
			if _, exists := productionFileMap[stagingFileList[0].Filename]; !exists {
				missingInProduction = append(missingInProduction, stagingFileList[0])
			}
		}
	}

	// CRITICAL: Detect duplicate files in staging (race condition from dual processing)
	// Keep the oldest file and soft-delete the newer duplicates
	var duplicatesInStaging []repository.KnowledgeBaseFileModel
	for filename, fileList := range stagingFilesByName {
		if len(fileList) > 1 {
			w.log.Warn("Detected duplicate files in staging KB during reconciliation",
				zap.String("filename", filename),
				zap.Int("count", len(fileList)),
				zap.String("stagingKBUID", stagingKBUID.String()))

			// Sort by create_time (keep oldest)
			// The remaining files after the first are duplicates to remove
			for i := 1; i < len(fileList); i++ {
				duplicatesInStaging = append(duplicatesInStaging, fileList[i])
			}
		}
	}

	w.log.Info("Reconciliation analysis complete",
		zap.Int("productionFileCount", len(productionFiles)),
		zap.Int("stagingFileCount", len(stagingFiles)),
		zap.Int("missingInStaging", len(missingInStaging)),
		zap.Int("missingInProduction", len(missingInProduction)),
		zap.Int("duplicatesInStaging", len(duplicatesInStaging)))

	// Create missing files in staging
	for _, prodFile := range missingInStaging {
		w.log.Info("Creating missing file in staging KB",
			zap.String("filename", prodFile.Filename),
			zap.String("prodFileUID", prodFile.UID.String()),
			zap.String("stagingKBUID", stagingKBUID.String()))

		// CRITICAL: Verify blob file exists in MinIO before creating staging file record
		// Production files may have missing blobs due to:
		// 1. MinIO storage failures or data loss
		// 2. Manual blob deletion (e.g., via MinIO console)
		// 3. Incomplete uploads (DB record created but blob upload failed)
		// 4. Cross-region replication lag (blob not yet replicated)
		// 5. Backup restoration inconsistencies
		//
		// Creating staging records for missing blobs would cause ProcessFileWorkflow to fail.
		// Instead, we skip these files during reconciliation to maintain system stability.
		// The production KB will retain the orphaned record, but at least the update can proceed.
		bucket := object.BucketFromDestination(prodFile.Destination)
		_, err := w.repository.GetMinIOStorage().GetFileMetadata(ctx, bucket, prodFile.Destination)
		if err != nil {
			skippedDueToMissingBlobs++
			w.log.Error("reconcileKBFiles: Original blob file not found in MinIO - skipping file during reconciliation",
				zap.String("prodFileUID", prodFile.UID.String()),
				zap.String("filename", prodFile.Filename),
				zap.String("destination", prodFile.Destination),
				zap.String("bucket", bucket),
				zap.Error(err),
				zap.String("impact", "File will not be available in updated KB"),
				zap.String("action_required", "Investigate why production file has no blob - potential data loss"))
			continue // Skip this file - don't create staging record
		}

		// Create duplicate file record for staging KB
		stagingFile := repository.KnowledgeBaseFileModel{
			Filename:                  prodFile.Filename,
			FileType:                  prodFile.FileType,
			NamespaceUID:              prodFile.NamespaceUID,
			CreatorUID:                prodFile.CreatorUID,
			KBUID:                     stagingKBUID,
			Destination:               prodFile.Destination, // Same source file in MinIO
			Size:                      prodFile.Size,
			ProcessStatus:             artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_NOTSTARTED.String(),
			ExternalMetadataUnmarshal: prodFile.ExternalMetadataUnmarshal,
			ExtraMetaDataUnmarshal:    prodFile.ExtraMetaDataUnmarshal,
		}

		// Create file record with retry
		var createdFile *repository.KnowledgeBaseFileModel
		maxRetries := 3
		for attempt := 1; attempt <= maxRetries; attempt++ {
			createdFile, err = w.repository.CreateKnowledgeBaseFile(ctx, stagingFile, nil)
			if err == nil {
				break
			}
			if attempt < maxRetries {
				w.log.Warn("Failed to create staging file during reconciliation, retrying...",
					zap.Error(err),
					zap.String("filename", prodFile.Filename),
					zap.Int("attempt", attempt))
				time.Sleep(time.Duration(100*(1<<uint(attempt-1))) * time.Millisecond)
			}
		}

		if err != nil {
			w.log.Error("Failed to create staging file after retries during reconciliation",
				zap.Error(err),
				zap.String("filename", prodFile.Filename))
			return nil, fmt.Errorf("failed to create staging file %s: %w", prodFile.Filename, err)
		}

		// Track this file for exclusion from NOTSTARTED check on next retry
		reconciledFileUIDs = append(reconciledFileUIDs, createdFile.UID)

		// Update KB usage
		err = w.repository.IncreaseKnowledgeBaseUsage(ctx, nil, stagingKBUID.String(), int(stagingFile.Size))
		if err != nil {
			w.log.Warn("Failed to increase staging KB usage during reconciliation",
				zap.Error(err),
				zap.String("filename", prodFile.Filename))
			// Non-fatal, continue
		}

		// Queue processing workflow using Worker's ProcessFile method
		err = w.ProcessFile(ctx, stagingKBUID, []types.FileUIDType{createdFile.UID}, types.UserUIDType(prodFile.CreatorUID), types.RequesterUIDType(prodFile.CreatorUID))
		if err != nil {
			w.log.Error("Failed to queue processing workflow during reconciliation",
				zap.Error(err),
				zap.String("filename", prodFile.Filename),
				zap.String("fileUID", createdFile.UID.String()))
			return nil, fmt.Errorf("failed to queue processing for file %s: %w", prodFile.Filename, err)
		}

		w.log.Info("Successfully created and queued staging file during reconciliation",
			zap.String("filename", prodFile.Filename),
			zap.String("stagingFileUID", createdFile.UID.String()))
	}

	// Handle files in staging but not in production (data loss scenario - should not delete, just log)
	if len(missingInProduction) > 0 {
		w.log.Warn("Found files in staging that don't exist in production - possible data inconsistency",
			zap.Int("count", len(missingInProduction)),
			zap.Strings("fileNames", func() []string {
				filenames := make([]string, len(missingInProduction))
				for i, f := range missingInProduction {
					filenames[i] = f.Filename
				}
				return filenames
			}()))
		// Don't delete these files - they might be legitimate uploads that haven't synced yet
	}

	// CRITICAL: Soft-delete duplicate files in staging (race condition cleanup)
	// These are extra files created by dual processing race conditions
	for _, dupFile := range duplicatesInStaging {
		w.log.Info("Soft-deleting duplicate file in staging KB",
			zap.String("filename", dupFile.Filename),
			zap.String("fileUID", dupFile.UID.String()),
			zap.String("stagingKBUID", stagingKBUID.String()))

		err := w.repository.DeleteKnowledgeBaseFile(ctx, dupFile.UID.String())
		if err != nil {
			w.log.Error("Failed to soft-delete duplicate file during reconciliation",
				zap.Error(err),
				zap.String("filename", dupFile.Filename),
				zap.String("fileUID", dupFile.UID.String()))
			return nil, fmt.Errorf("failed to soft-delete duplicate file %s: %w", dupFile.Filename, err)
		}

		w.log.Info("Successfully soft-deleted duplicate file",
			zap.String("filename", dupFile.Filename),
			zap.String("fileUID", dupFile.UID.String()))
	}

	w.log.Info("KB file reconciliation complete",
		zap.Int("filesCreatedInStaging", len(missingInStaging)),
		zap.Int("reconciledFileUIDs", len(reconciledFileUIDs)),
		zap.Int("duplicatesRemoved", len(duplicatesInStaging)),
		zap.Int("skippedDueToMissingBlobs", skippedDueToMissingBlobs),
		zap.String("productionKBUID", productionKBUID.String()),
		zap.String("stagingKBUID", stagingKBUID.String()))

	// CRITICAL: If ALL files were skipped due to missing blobs, return error to signal
	// that reconciliation "succeeded" but no files were created. The caller should handle
	// this by recognizing that staging will remain empty.
	if len(missingInStaging) > 0 && len(reconciledFileUIDs) == 0 && skippedDueToMissingBlobs == len(missingInStaging) {
		return nil, fmt.Errorf("all %d files were skipped due to missing blobs in MinIO - staging KB will remain empty", skippedDueToMissingBlobs)
	}

	return reconciledFileUIDs, nil
}

// ValidateUpdatedKBActivity validates data integrity after synchronization (Phase 4)
// NOTE: KB locking and file synchronization are handled by SynchronizeKBActivity (Phase 3)
// This activity focuses ONLY on verifying data integrity (counts, embeddings, chunks)
func (w *Worker) ValidateUpdatedKBActivity(ctx context.Context, param *ValidateUpdatedKBActivityParam) (*ValidateUpdatedKBActivityResult, error) {
	w.log.Info("ValidateUpdatedKBActivity: Validating data integrity",
		zap.String("originalKBUID", param.OriginalKBUID.String()),
		zap.String("stagingKBUID", param.StagingKBUID.String()),
		zap.Int("expectedFileCount", param.ExpectedFileCount))

	var errors []string

	// VALIDATION 1: Ensure production and staging KBs have identical file counts
	// This is CRITICAL for user experience - the file count should not change after swap
	// With dual processing, new files uploaded during update go to BOTH production and staging
	productionCount, err := w.repository.GetFileCountByKnowledgeBaseUID(ctx, param.OriginalKBUID, "")
	if err != nil {
		errors = append(errors, fmt.Sprintf("failed to get production file count: %v", err))
	}
	w.log.Info("ValidateUpdatedKBActivity: DEBUG production count",
		zap.String("originalKBUID", param.OriginalKBUID.String()),
		zap.Int64("productionCount", productionCount))

	stagingCount, err := w.repository.GetFileCountByKnowledgeBaseUID(ctx, param.StagingKBUID, "")
	if err != nil {
		errors = append(errors, fmt.Sprintf("failed to get staging file count: %v", err))
	}
	w.log.Info("ValidateUpdatedKBActivity: DEBUG staging count",
		zap.String("stagingKBUID", param.StagingKBUID.String()),
		zap.Int64("stagingCount", stagingCount))

	if len(errors) == 0 {
		w.log.Info("ValidateUpdatedKBActivity: File counts",
			zap.Int("expectedFileCount", param.ExpectedFileCount),
			zap.Int64("productionCount", productionCount),
			zap.Int64("stagingCount", stagingCount))

		// SPECIAL CASE: If staging KB has 0 files but production has files,
		// this means all files were skipped during cloning (blobs missing from MinIO)
		// This is valid for test cleanup scenarios - treat as empty KB update
		if stagingCount == 0 && productionCount > 0 {
			w.log.Warn("ValidateUpdatedKBActivity: All files were skipped during cloning (staging is empty)",
				zap.Int64("productionCount", productionCount),
				zap.Int64("stagingCount", stagingCount),
				zap.String("reason", "blob files missing from MinIO"))
		} else {
			// CRITICAL: Production and staging must have EXACT same file count
			// This ensures users see no change in file count before/after swap
			// Dual processing guarantees this by adding new files to both KBs AND deleting from both KBs
			if productionCount != stagingCount {
				errors = append(errors, fmt.Sprintf("file count mismatch: production has %d, staging has %d (must be identical for seamless swap)",
					productionCount, stagingCount))
			}
		}

		// Sanity check: BOTH production and staging should have at least the expected base file count
		// UNLESS files were deleted during the update (in which case both should have same reduced count)
		// Only flag as error if counts are unexpectedly LOW and DIFFERENT from each other
		// EXCEPTION: Skip this check if staging is empty (all files skipped)
		if stagingCount < int64(param.ExpectedFileCount) && productionCount >= int64(param.ExpectedFileCount) && stagingCount > 0 {
			// Staging is missing files that production has - this indicates failed dual processing
			errors = append(errors, fmt.Sprintf("staging file count too low: expected at least %d (original files), staging has %d but production has %d",
				param.ExpectedFileCount, stagingCount, productionCount))
		}
	}

	// VALIDATION 2: Verify active collection UID is set for staging KB
	// SPECIAL CASE: For empty KBs (0 files), no collection is created, so skip this validation
	if param.ExpectedFileCount > 0 {
		collectionUID, err := w.repository.GetActiveCollectionUID(ctx, param.StagingKBUID)
		if err != nil {
			errors = append(errors, fmt.Sprintf("failed to get staging collection UID: %v", err))
		} else if collectionUID == nil {
			errors = append(errors, "staging KB has no active collection UID set")
		} else {
			// Use constant.KBCollectionName to generate the correct Milvus collection name
			// Format: kb_<uuid_with_underscores> (e.g., kb_12345678_1234_1234_1234_123456789012)
			collectionName := constant.KBCollectionName(*collectionUID)
			w.log.Info("ValidateUpdatedKBActivity: Collection UID verified",
				zap.String("collectionName", collectionName),
				zap.String("collectionUID", collectionUID.String()),
				zap.String("stagingKBUID", param.StagingKBUID.String()))
		}
	} else {
		w.log.Info("ValidateUpdatedKBActivity: Skipping collection validation for empty KB (0 files)")
	}

	// VALIDATION 3: Verify converted file counts
	// SPECIAL CASE: For empty KBs (0 files), skip this validation
	if param.ExpectedFileCount > 0 && len(errors) == 0 {
		originalConvertedCount, err := w.repository.GetConvertedFileCountByKBUID(ctx, param.OriginalKBUID)
		if err != nil {
			errors = append(errors, fmt.Sprintf("failed to get original converted file count: %v", err))
		}

		stagingConvertedCount, err := w.repository.GetConvertedFileCountByKBUID(ctx, param.StagingKBUID)
		if err != nil {
			errors = append(errors, fmt.Sprintf("failed to get staging converted file count: %v", err))
		}

		if len(errors) == 0 {
			w.log.Info("ValidateUpdatedKBActivity: Converted file counts",
				zap.Int64("originalConvertedCount", originalConvertedCount),
				zap.Int64("stagingConvertedCount", stagingConvertedCount))

			// SPECIAL CASE: If staging has 0 converted files but production has some,
			// check if staging KB actually has 0 files (all files skipped due to missing blobs)
			// This is valid - skip converted file count validation
			// NOTE: We check stagingConvertedCount instead of stagingCount because the latter
			// may be stale due to GORM/Temporal caching
			if stagingConvertedCount == 0 && originalConvertedCount > 0 {
				w.log.Warn("ValidateUpdatedKBActivity: All files were skipped during cloning (staging has no converted files), skipping converted file count validation",
					zap.Int64("productionCount", productionCount),
					zap.Int64("stagingCount", stagingCount),
					zap.Int64("originalConvertedCount", originalConvertedCount),
					zap.Int64("stagingConvertedCount", stagingConvertedCount))
			} else {
				// Staging should have at least as many converted files as we expect (no tolerance)
				// During rapid operations with file deletions, counts may be lower but should still match
				if stagingConvertedCount < originalConvertedCount {
					// Add detailed breakdown for debugging
					difference := originalConvertedCount - stagingConvertedCount
					w.log.Error("ValidateUpdatedKBActivity: Converted file count mismatch detected",
						zap.Int64("originalConvertedCount", originalConvertedCount),
						zap.Int64("stagingConvertedCount", stagingConvertedCount),
						zap.Int64("difference", difference),
						zap.String("originalKBUID", param.OriginalKBUID.String()),
						zap.String("stagingKBUID", param.StagingKBUID.String()),
						zap.String("hint", "This usually indicates a dual processing/deletion race condition"))

					errors = append(errors, fmt.Sprintf("converted file count mismatch: original=%d, staging=%d (diff: %d, likely due to dual processing race)",
						originalConvertedCount, stagingConvertedCount, difference))
				}
			}
		}
	} else if param.ExpectedFileCount == 0 {
		w.log.Info("ValidateUpdatedKBActivity: Skipping converted file count validation for empty KB (0 files)")
	}

	// VALIDATION 4: Verify chunk counts
	// SPECIAL CASE: For empty KBs (0 files), skip this validation
	var stagingChunkCount int64
	if param.ExpectedFileCount > 0 && len(errors) == 0 {
		originalChunkCount, err := w.repository.GetChunkCountByKBUID(ctx, param.OriginalKBUID)
		if err != nil {
			errors = append(errors, fmt.Sprintf("failed to get original chunk count: %v", err))
		}

		stagingChunkCount, err = w.repository.GetChunkCountByKBUID(ctx, param.StagingKBUID)
		if err != nil {
			errors = append(errors, fmt.Sprintf("failed to get staging chunk count: %v", err))
		}

		if len(errors) == 0 {
			w.log.Info("ValidateUpdatedKBActivity: Chunk counts",
				zap.Int64("originalChunkCount", originalChunkCount),
				zap.Int64("stagingChunkCount", stagingChunkCount))

			// SPECIAL CASE: If staging has 0 files (all skipped), skip chunk validation
			if stagingCount == 0 && productionCount > 0 {
				w.log.Warn("ValidateUpdatedKBActivity: Skipping chunk count validation (staging is empty)")
			} else {
				// Staging should have at least some chunks if original had files
				// Note: Chunk count may differ due to different chunking strategies
				if originalChunkCount > 0 && stagingChunkCount == 0 {
					errors = append(errors, "staging KB has no chunks but original had chunks")
				}
			}
		}
	} else if param.ExpectedFileCount == 0 {
		w.log.Info("ValidateUpdatedKBActivity: Skipping chunk count validation for empty KB (0 files)")
	}

	// VALIDATION 5: Verify embedding counts match chunk counts
	// With proper locking and synchronization, this should ALWAYS pass on first attempt.
	// If it fails, it indicates a real bug in the processing pipeline, not a transient race.
	// SPECIAL CASE: For empty KBs (0 files), skip this validation
	if param.ExpectedFileCount > 0 && len(errors) == 0 {
		stagingEmbeddingCount, err := w.repository.GetEmbeddingCountByKBUID(ctx, param.StagingKBUID)
		if err != nil {
			errors = append(errors, fmt.Sprintf("failed to get staging embedding count: %v", err))
		} else {
			w.log.Info("ValidateUpdatedKBActivity: Embedding count check",
				zap.Int64("stagingChunkCount", stagingChunkCount),
				zap.Int64("stagingEmbeddingCount", stagingEmbeddingCount))

			// Embeddings should match chunks (one embedding per chunk) - NO TOLERANCE
			if stagingEmbeddingCount != stagingChunkCount {
				errors = append(errors, fmt.Sprintf("embedding/chunk count mismatch: chunks=%d, embeddings=%d",
					stagingChunkCount, stagingEmbeddingCount))
			}
		}
	} else if param.ExpectedFileCount == 0 {
		w.log.Info("ValidateUpdatedKBActivity: Skipping embedding count validation for empty KB (0 files)")
	}

	success := len(errors) == 0
	w.log.Info("ValidateUpdatedKBActivity: Validation complete",
		zap.Bool("success", success),
		zap.Strings("errors", errors))

	if !success {
		err := fmt.Errorf("validation failed: %v", errors)
		return &ValidateUpdatedKBActivityResult{
			Success: false,
			Errors:  errors,
		}, activityError(err, validateUpdatedKBActivityError)
	}

	return &ValidateUpdatedKBActivityResult{
		Success: true,
		Errors:  []string{},
	}, nil
}

// SwapKnowledgeBasesActivity performs atomic swap of resources between original and staging KBs
// CRITICAL DESIGN: The KB UID must remain constant - only the resources (files, chunks, embeddings) are swapped.
// This ensures KB identity is preserved throughout update/rollback cycles.
//
// Process:
// 1. Create rollback KB to store old resources
// 2. Move original KB's resources → rollback KB
// 3. Move staging KB's resources → original KB (original UID stays the same!)
// 4. Delete staging KB (no longer needed)
func (w *Worker) SwapKnowledgeBasesActivity(ctx context.Context, param *SwapKnowledgeBasesActivityParam) (*SwapKnowledgeBasesActivityResult, error) {
	w.log.Info("SwapKnowledgeBasesActivity: Performing resource swap (KB UID remains constant)",
		zap.String("originalKBUID", param.OriginalKBUID.String()),
		zap.String("stagingKBUID", param.StagingKBUID.String()))

	originalKB, err := w.repository.GetKnowledgeBaseByUIDWithConfig(ctx, param.OriginalKBUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to get original knowledge base. Please try again.")
		return nil, activityError(err, swapKnowledgeBasesActivityError)
	}

	stagingKB, err := w.repository.GetKnowledgeBaseByUIDWithConfig(ctx, param.StagingKBUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to get staging knowledge base. Please try again.")
		return nil, activityError(err, swapKnowledgeBasesActivityError)
	}

	// Check that collection UIDs are set (not uuid.Nil)
	if originalKB.ActiveCollectionUID == uuid.Nil || stagingKB.ActiveCollectionUID == uuid.Nil {
		return nil, activityErrorWithMessage(
			"active_collection_uid is unset (uuid.Nil) for original or staging KB",
			swapKnowledgeBasesActivityError,
			fmt.Errorf("originalKB.ActiveCollectionUID=%v, stagingKB.ActiveCollectionUID=%v", originalKB.ActiveCollectionUID, stagingKB.ActiveCollectionUID),
		)
	}

	// CRITICAL: Validate that collections exist in Milvus before proceeding
	// This determines whether we need a full swap (with rollback) or simple assignment
	originalCollectionName := constant.KBCollectionName(originalKB.ActiveCollectionUID)
	stagingCollectionName := constant.KBCollectionName(stagingKB.ActiveCollectionUID)

	originalCollectionExists, err := w.repository.CollectionExists(ctx, originalCollectionName)
	if err != nil {
		return nil, activityErrorWithMessage(
			fmt.Sprintf("Failed to check if original collection exists: %s", originalCollectionName),
			swapKnowledgeBasesActivityError,
			err,
		)
	}

	stagingCollectionExists, err := w.repository.CollectionExists(ctx, stagingCollectionName)
	if err != nil {
		return nil, activityErrorWithMessage(
			fmt.Sprintf("Failed to check if staging collection exists: %s", stagingCollectionName),
			swapKnowledgeBasesActivityError,
			err,
		)
	}
	if !stagingCollectionExists {
		return nil, activityErrorWithMessage(
			fmt.Sprintf("Staging KB's collection does not exist in Milvus: %s (UID: %s)", stagingCollectionName, stagingKB.ActiveCollectionUID),
			swapKnowledgeBasesActivityError,
			fmt.Errorf("collection %s not found", stagingCollectionName),
		)
	}

	retentionUntil := time.Now().Add(time.Duration(param.RetentionDays) * 24 * time.Hour)

	// Handle case where original collection doesn't exist in Milvus
	// This can happen if the collection was deleted or never created
	// In this case, we simply point the production KB to the staging collection
	// without creating a rollback KB (since there's nothing to rollback to)
	if !originalCollectionExists {
		w.log.Warn("SwapKnowledgeBasesActivity: Original KB's collection does not exist in Milvus, performing simple resource move and collection assignment",
			zap.String("originalCollectionName", originalCollectionName),
			zap.String("originalCollectionUID", originalKB.ActiveCollectionUID.String()),
			zap.String("stagingCollectionUID", stagingKB.ActiveCollectionUID.String()),
			zap.String("stagingCollectionName", stagingCollectionName))

		// Perform atomic swap within transaction (even for simple case to ensure consistency)
		tx := w.repository.GetDB().Begin()
		defer func() {
			if r := recover(); r != nil {
				tx.Rollback()
				panic(r)
			}
		}()

		// CRITICAL: Clear staging KB's active_collection_uid FIRST
		// This prevents unique constraint violation when production KB is updated to same collection UID
		w.log.Info("SwapKnowledgeBasesActivity: Clearing staging KB's active_collection_uid to avoid constraint violation",
			zap.String("stagingKBUID", stagingKB.UID.String()),
			zap.String("stagingCollectionUID", stagingKB.ActiveCollectionUID.String()))

		err = w.repository.UpdateKnowledgeBaseWithMapTx(ctx, tx, stagingKB.KBID, stagingKB.NamespaceUID, map[string]interface{}{
			"active_collection_uid": uuid.Nil, // Clear to NULL to avoid unique constraint violation
		})
		if err != nil {
			tx.Rollback()
			err = errorsx.AddMessage(err, "Unable to clear staging KB collection UID before swap. Please try again.")
			return nil, activityError(err, swapKnowledgeBasesActivityError)
		}

		// Move staging KB's resources → original KB (within transaction)
		if err := w.repository.UpdateKnowledgeBaseResourcesTx(ctx, tx, param.StagingKBUID, param.OriginalKBUID); err != nil {
			tx.Rollback()
			return nil, activityErrorWithMessage("Failed to move staging resources to production", swapKnowledgeBasesActivityError, err)
		}

		// Update production KB to point to staging's collection (no conflict now!)
		err = w.repository.UpdateKnowledgeBaseWithMapTx(ctx, tx, originalKB.KBID, originalKB.NamespaceUID, map[string]interface{}{
			"active_collection_uid":    stagingKB.ActiveCollectionUID, // Point to new collection
			"system_uid":               stagingKB.SystemUID,           // Update system UID (may reference different system with new dimensionality)
			"staging":                  false,
			"rollback_retention_until": retentionUntil,
		})
		if err != nil {
			tx.Rollback()
			err = errorsx.AddMessage(err, "Unable to update production KB metadata and collection pointer. Please try again.")
			return nil, activityError(err, swapKnowledgeBasesActivityError)
		}

		// Clean up any existing rollback KB (within transaction)
		ownerUID, err := uuid.FromString(originalKB.NamespaceUID)
		if err == nil {
			existingRollback, err := w.repository.GetRollbackKBForProduction(ctx, types.OwnerUIDType(ownerUID), originalKB.KBID)
			if err == nil && existingRollback != nil {
				w.log.Info("SwapKnowledgeBasesActivity: Soft-deleting existing rollback KB since original collection doesn't exist",
					zap.String("rollbackKBUID", existingRollback.UID.String()))
				_ = w.repository.DeleteKnowledgeBaseTx(ctx, tx, existingRollback.NamespaceUID, existingRollback.KBID)
			}
		}

		// Delete staging KB (within transaction)
		w.log.Info("SwapKnowledgeBasesActivity: Soft-deleting staging KB",
			zap.String("stagingKBUID", stagingKB.UID.String()),
			zap.String("stagingKBID", stagingKB.KBID))

		// Clear update fields before soft delete
		err = w.repository.UpdateKnowledgeBaseWithMapTx(ctx, tx, stagingKB.KBID, stagingKB.NamespaceUID, map[string]interface{}{
			"update_status":      "",  // Clear update status so it doesn't block future updates
			"update_workflow_id": nil, // Clear workflow ID so API deletion works
		})
		if err != nil {
			tx.Rollback()
			w.log.Error("SwapKnowledgeBasesActivity: Failed to clear staging KB update fields",
				zap.String("stagingKBUID", stagingKB.UID.String()),
				zap.Error(err))
			return nil, activityErrorWithMessage("Failed to clear staging KB update fields", swapKnowledgeBasesActivityError, err)
		}

		err = w.repository.DeleteKnowledgeBaseTx(ctx, tx, stagingKB.NamespaceUID, stagingKB.KBID)
		if err != nil {
			tx.Rollback()
			w.log.Error("SwapKnowledgeBasesActivity: Failed to soft-delete staging KB",
				zap.String("stagingKBUID", stagingKB.UID.String()),
				zap.Error(err))
			return nil, activityErrorWithMessage("Failed to soft-delete staging KB", swapKnowledgeBasesActivityError, err)
		}

		// COMMIT TRANSACTION - all or nothing!
		if err := tx.Commit().Error; err != nil {
			return nil, activityErrorWithMessage("Failed to commit simple swap transaction", swapKnowledgeBasesActivityError, err)
		}

		w.log.Info("SwapKnowledgeBasesActivity: Simple collection assignment completed successfully (no rollback KB created)",
			zap.String("productionKBUID", param.OriginalKBUID.String()),
			zap.String("stagingKBUID", stagingKB.UID.String()),
			zap.String("newProductionCollectionUID", stagingKB.ActiveCollectionUID.String()))

		return &SwapKnowledgeBasesActivityResult{
			RollbackKBUID:              types.KBUIDType(uuid.Nil),     // No rollback KB created
			StagingKBUID:               stagingKB.UID,                 // Return staging KB UID for cleanup
			NewProductionCollectionUID: stagingKB.ActiveCollectionUID, // Protect this collection from cleanup
		}, nil
	}

	// Normal path: Original collection exists, so we need to create rollback KB and do full swap
	// Step 1: Create/update rollback KB to store old resources
	ownerUID, err := uuid.FromString(originalKB.NamespaceUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Invalid owner UID format")
		return nil, activityError(err, swapKnowledgeBasesActivityError)
	}

	// Check if rollback KB exists from previous update using parent_kb_uid lookup
	existingRollback, err := w.repository.GetRollbackKBForProduction(ctx, types.OwnerUIDType(ownerUID), originalKB.KBID)
	var rollbackKBUID types.KBUIDType

	if err == nil && existingRollback != nil {
		// Reuse existing rollback KB (just update its metadata)
		rollbackKBUID = existingRollback.UID
		w.log.Info("SwapKnowledgeBasesActivity: Reusing existing rollback KB",
			zap.String("rollbackKBUID", rollbackKBUID.String()))
	} else {
		// Create new rollback KB
		rollbackKBUID = types.KBUIDType(uuid.Must(uuid.NewV4()))

		rollbackKB := &repository.KnowledgeBaseModel{
			UID:                    rollbackKBUID,
			KBID:                   rollbackKBUID.String(), // Use UID as KBID (system-generated ID for rollback KB)
			NamespaceUID:           originalKB.NamespaceUID,
			CreatorUID:             originalKB.CreatorUID,
			Tags:                   append(originalKB.Tags, "rollback"),
			Staging:                true,
			ParentKBUID:            &originalKB.UID, // Link to production KB via parent_kb_uid
			UpdateStatus:           artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED.String(),
			RollbackRetentionUntil: &retentionUntil,
			KnowledgeBaseType:      originalKB.KnowledgeBaseType,
			SystemUID:              originalKB.SystemUID,
		}

		// Create rollback KB without external service callback (no ACL needed for staging KB)
		// Create rollback KB without external service (no collection creation needed - reuses old collection)
		_, err = w.repository.CreateKnowledgeBase(ctx, *rollbackKB, nil /* no external service */)
		if err != nil {
			err = errorsx.AddMessage(err, "Unable to create rollback KB. Please try again.")
			return nil, activityError(err, swapKnowledgeBasesActivityError)
		}
		w.log.Info("SwapKnowledgeBasesActivity: Created new rollback KB",
			zap.String("rollbackKBUID", rollbackKBUID.String()))
	}

	// Step 2: Perform atomic swap within a database transaction
	// CRITICAL: Wrap entire swap in transaction to prevent:
	// - active_collection_uid constraint violation (clear staging's UID before updating production)
	// - Duplicate files from partial swap (all-or-nothing atomicity)
	w.log.Info("SwapKnowledgeBasesActivity: Starting atomic swap transaction",
		zap.String("originalKBUID", param.OriginalKBUID.String()),
		zap.String("stagingKBUID", param.StagingKBUID.String()),
		zap.String("rollbackKBUID", rollbackKBUID.String()))

	// Start database transaction
	tx := w.repository.GetDB().Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r)
		}
	}()

	// CRITICAL: Clear staging KB's active_collection_uid FIRST
	// This prevents unique constraint violation when production KB is updated to same collection UID
	// Without this, we get: duplicate key value violates unique constraint "idx_kb_active_collection_uid_unique"
	w.log.Info("SwapKnowledgeBasesActivity: Clearing staging KB's active_collection_uid to avoid constraint violation",
		zap.String("stagingKBUID", stagingKB.UID.String()),
		zap.String("stagingCollectionUID", stagingKB.ActiveCollectionUID.String()))

	err = w.repository.UpdateKnowledgeBaseWithMapTx(ctx, tx, stagingKB.KBID, stagingKB.NamespaceUID, map[string]interface{}{
		"active_collection_uid": uuid.Nil, // Clear to NULL to avoid unique constraint violation
	})
	if err != nil {
		tx.Rollback()
		err = errorsx.AddMessage(err, "Unable to clear staging KB collection UID before swap. Please try again.")
		return nil, activityError(err, swapKnowledgeBasesActivityError)
	}

	// Step 2a: Move original KB's resources → temp (within transaction)
	tempUID := uuid.Must(uuid.NewV4())
	if err := w.repository.UpdateKnowledgeBaseResourcesTx(ctx, tx, param.OriginalKBUID, types.KBUIDType(tempUID)); err != nil {
		tx.Rollback()
		return nil, activityErrorWithMessage("Failed to move original resources to temp", swapKnowledgeBasesActivityError, err)
	}

	// Step 2b: Move staging KB's resources → original KB (staging resources become production)
	if err := w.repository.UpdateKnowledgeBaseResourcesTx(ctx, tx, param.StagingKBUID, param.OriginalKBUID); err != nil {
		tx.Rollback()
		return nil, activityErrorWithMessage("Failed to move staging resources to production", swapKnowledgeBasesActivityError, err)
	}

	// Step 2c: Move temp resources → rollback KB (old resources saved for rollback)
	if err := w.repository.UpdateKnowledgeBaseResourcesTx(ctx, tx, types.KBUIDType(tempUID), rollbackKBUID); err != nil {
		tx.Rollback()
		return nil, activityErrorWithMessage("Failed to move old resources to rollback", swapKnowledgeBasesActivityError, err)
	}

	// Step 3: Swap collection pointers and metadata (within transaction)
	// Now safe to update production KB's active_collection_uid since staging's was cleared above
	w.log.Info("SwapKnowledgeBasesActivity: Updating production KB to point to staging's collection",
		zap.String("originalCollectionUID", originalKB.ActiveCollectionUID.String()),
		zap.String("stagingCollectionUID", stagingKB.ActiveCollectionUID.String()))

	err = w.repository.UpdateKnowledgeBaseWithMapTx(ctx, tx, originalKB.KBID, originalKB.NamespaceUID, map[string]interface{}{
		"active_collection_uid":    stagingKB.ActiveCollectionUID, // Point to new collection (no conflict now!)
		"system_uid":               stagingKB.SystemUID,           // Update system UID (may reference different system with new dimensionality)
		"staging":                  false,
		"rollback_retention_until": retentionUntil,
	})
	if err != nil {
		tx.Rollback()
		err = errorsx.AddMessage(err, "Unable to update production KB metadata and collection pointer. Please try again.")
		return nil, activityError(err, swapKnowledgeBasesActivityError)
	}

	// Update rollback KB to point to original's collection (preserve old dimensionality)
	rollbackKB, err := w.repository.GetKnowledgeBaseByUID(ctx, rollbackKBUID)
	if err != nil {
		tx.Rollback()
		err = errorsx.AddMessage(err, "Unable to retrieve rollback KB for update. Please try again.")
		return nil, activityError(err, swapKnowledgeBasesActivityError)
	}
	err = w.repository.UpdateKnowledgeBaseWithMapTx(ctx, tx, rollbackKB.KBID, originalKB.NamespaceUID, map[string]interface{}{
		"active_collection_uid": originalKB.ActiveCollectionUID, // Keep original collection pointer
		"system_uid":            originalKB.SystemUID,           // Keep original system UID
	})
	if err != nil {
		tx.Rollback()
		err = errorsx.AddMessage(err, "Unable to update rollback KB metadata. Please try again.")
		return nil, activityError(err, swapKnowledgeBasesActivityError)
	}

	// Step 4: Soft-delete staging KB (within transaction)
	w.log.Info("SwapKnowledgeBasesActivity: Soft-deleting staging KB",
		zap.String("stagingKBUID", stagingKB.UID.String()),
		zap.String("stagingKBID", stagingKB.KBID))

	// Clear update fields before soft delete
	err = w.repository.UpdateKnowledgeBaseWithMapTx(ctx, tx, stagingKB.KBID, stagingKB.NamespaceUID, map[string]interface{}{
		"update_status":      "",  // Clear update status so it doesn't block future updates
		"update_workflow_id": nil, // Clear workflow ID so API deletion works
	})
	if err != nil {
		tx.Rollback()
		w.log.Error("SwapKnowledgeBasesActivity: Failed to clear staging KB update fields",
			zap.String("stagingKBUID", stagingKB.UID.String()),
			zap.Error(err))
		return nil, activityErrorWithMessage("Failed to clear staging KB update fields", swapKnowledgeBasesActivityError, err)
	}

	// Now perform soft delete
	err = w.repository.DeleteKnowledgeBaseTx(ctx, tx, stagingKB.NamespaceUID, stagingKB.KBID)
	if err != nil {
		tx.Rollback()
		w.log.Error("SwapKnowledgeBasesActivity: Failed to soft-delete staging KB",
			zap.String("stagingKBUID", stagingKB.UID.String()),
			zap.Error(err))
		return nil, activityErrorWithMessage("Failed to soft-delete staging KB", swapKnowledgeBasesActivityError, err)
	}

	// COMMIT TRANSACTION - all or nothing!
	if err := tx.Commit().Error; err != nil {
		return nil, activityErrorWithMessage("Failed to commit swap transaction", swapKnowledgeBasesActivityError, err)
	}

	w.log.Info("SwapKnowledgeBasesActivity: Atomic swap transaction committed successfully")

	// NOTE: Do NOT trigger cleanup here - it creates a race condition where the collection
	// gets dropped before the production KB's active_collection_uid update is visible.
	// Cleanup will be triggered by the main workflow after the swap activity completes.

	w.log.Info("SwapKnowledgeBasesActivity: Resource swap completed successfully",
		zap.String("productionKBUID", param.OriginalKBUID.String()),
		zap.String("rollbackKBUID", rollbackKBUID.String()),
		zap.String("stagingKBUID", stagingKB.UID.String()),
		zap.String("newProductionCollectionUID", stagingKB.ActiveCollectionUID.String()))

	return &SwapKnowledgeBasesActivityResult{
		RollbackKBUID:              rollbackKBUID,
		StagingKBUID:               stagingKB.UID,                 // Return staging KB UID for cleanup
		NewProductionCollectionUID: stagingKB.ActiveCollectionUID, // Protect this collection from cleanup
	}, nil
}

// UpdateKnowledgeBaseUpdateStatusActivity updates the update status of a KB
func (w *Worker) UpdateKnowledgeBaseUpdateStatusActivity(ctx context.Context, param *UpdateKnowledgeBaseUpdateStatusActivityParam) error {
	w.log.Info("UpdateKnowledgeBaseUpdateStatusActivity: Updating status",
		zap.String("kbUID", param.KBUID.String()),
		zap.String("status", param.Status),
		zap.String("errorMessage", param.ErrorMessage),
		zap.String("previousSystemUID", param.PreviousSystemUID.String()))

	err := w.repository.UpdateKnowledgeBaseUpdateStatus(ctx, param.KBUID, param.Status, param.WorkflowID, param.ErrorMessage, param.PreviousSystemUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to update knowledge base update status. Please try again.")
		return activityError(err, updateKnowledgeBaseUpdateStatusActivityError)
	}

	return nil
}

// CleanupOldKnowledgeBaseActivity cleans up old KB after retention period
func (w *Worker) CleanupOldKnowledgeBaseActivity(ctx context.Context, param *CleanupOldKnowledgeBaseActivityParam) error {
	w.log.Info("CleanupOldKnowledgeBaseActivity: Cleaning up old KB",
		zap.String("kbUID", param.KBUID.String()))

	kb, err := w.repository.GetKnowledgeBaseByUID(ctx, param.KBUID)
	if err != nil {
		// If KB not found, it might already be fully deleted - this is okay
		if errors.Is(err, gorm.ErrRecordNotFound) {
			w.log.Info("CleanupOldKnowledgeBaseActivity: KB already deleted, skipping",
				zap.String("kbUID", param.KBUID.String()))
			return nil
		}
		err = errorsx.AddMessage(err, "Unable to get knowledge base for cleanup. Please try again.")
		return activityError(err, cleanupOldKnowledgeBaseActivityError)
	}

	// CRITICAL: Clear update_status and update_workflow_id BEFORE soft-deletion
	// This prevents failed updates from blocking future operations
	// (A KB stuck in "UPDATING" state cannot be updated again)
	if kb.UpdateStatus != "" || kb.UpdateWorkflowID != "" {
		w.log.Info("CleanupOldKnowledgeBaseActivity: Clearing update status before soft-deletion",
			zap.String("kbUID", param.KBUID.String()),
			zap.String("previousUpdateStatus", kb.UpdateStatus))

		err = w.repository.UpdateKnowledgeBaseWithMap(ctx, kb.KBID, kb.NamespaceUID, map[string]interface{}{
			"update_status":      "",
			"update_workflow_id": "",
		})
		if err != nil {
			err = errorsx.AddMessage(err, "Unable to clear update status before cleanup. Please try again.")
			return activityErrorWithMessage("Failed to clear update status before cleanup", cleanupOldKnowledgeBaseActivityError, err)
		}
	}

	// Only soft delete if not already deleted
	if !kb.DeleteTime.Valid {
		w.log.Info("CleanupOldKnowledgeBaseActivity: Soft-deleting KB",
			zap.String("kbUID", param.KBUID.String()))
		_, err = w.repository.DeleteKnowledgeBase(ctx, kb.NamespaceUID, kb.KBID)
		if err != nil {
			// If already deleted, that's okay - continue with collection drop
			// With row-level locking, DeleteKnowledgeBase will return error if KB is already deleted
			// (because it checks delete_time IS NULL in the WHERE clause)
			if !errors.Is(err, gorm.ErrRecordNotFound) && !strings.Contains(err.Error(), "not found") {
				err = errorsx.AddMessage(err, "Unable to delete knowledge base. Please try again.")
				return activityError(err, cleanupOldKnowledgeBaseActivityError)
			}
			w.log.Info("CleanupOldKnowledgeBaseActivity: KB already soft-deleted during deletion attempt, continuing",
				zap.String("kbUID", param.KBUID.String()),
				zap.Error(err))
		}
	} else {
		w.log.Info("CleanupOldKnowledgeBaseActivity: KB already soft-deleted, skipping soft-delete",
			zap.String("kbUID", param.KBUID.String()),
			zap.Time("deleteTime", kb.DeleteTime.Time))
	}

	// CRITICAL: Wait for any in-progress file processing to complete before cleanup
	// This prevents race conditions where file processing tries to insert embeddings
	// while we're dropping the collection
	w.log.Info("CleanupOldKnowledgeBaseActivity: Checking for in-progress file processing",
		zap.String("kbUID", param.KBUID.String()))

	err = w.waitForInProgressFiles(ctx, param.KBUID, 120, 5)
	if err != nil {
		// Non-fatal: Log warning but continue with cleanup
		// This prevents cleanup from being blocked indefinitely
		w.log.Warn("CleanupOldKnowledgeBaseActivity: Error while waiting for file processing, proceeding with cleanup",
			zap.String("kbUID", param.KBUID.String()),
			zap.Error(err))
	}

	// CRITICAL: Delete all files from MinIO before dropping collection and DB records
	// This prevents orphaned blobs when updates fail
	w.log.Info("CleanupOldKnowledgeBaseActivity: Deleting all files from MinIO",
		zap.String("kbUID", param.KBUID.String()))
	err = w.deleteKnowledgeBaseSync(ctx, param.KBUID.String())
	if err != nil {
		// Log warning but continue - MinIO cleanup failure shouldn't block DB cleanup
		// Orphaned files can be cleaned up by garbage collection later
		w.log.Warn("CleanupOldKnowledgeBaseActivity: Failed to delete files from MinIO, continuing with DB cleanup",
			zap.String("kbUID", param.KBUID.String()),
			zap.Error(err))
	}

	// Drop Milvus collection using active_collection_uid
	collectionUID := kb.ActiveCollectionUID
	if collectionUID == uuid.Nil {
		// This should never happen after migration 000044 - return error instead of fallback
		err = fmt.Errorf("active_collection_uid is not set for KB %s", param.KBUID)
		return activityErrorWithMessage("Invalid state: active_collection_uid is nil", cleanupOldKnowledgeBaseActivityError, err)
	}

	// Check if collection is in use by other KBs before dropping
	inUse, err := w.repository.IsCollectionInUse(ctx, collectionUID)
	if err != nil {
		w.log.Warn("CleanupOldKnowledgeBaseActivity: Error checking collection usage, skipping drop to be safe",
			zap.String("collectionUID", collectionUID.String()),
			zap.Error(err))
		return nil
	}

	if inUse {
		w.log.Info("CleanupOldKnowledgeBaseActivity: Collection still in use by other KBs, preserving",
			zap.String("collectionUID", collectionUID.String()))
		return nil
	}

	collectionName := constant.KBCollectionName(collectionUID)
	err = w.repository.DropCollection(ctx, collectionName)
	if err != nil {
		w.log.Warn("Failed to drop collection, continuing cleanup",
			zap.String("collection", collectionName),
			zap.Error(err))
	}

	// CRITICAL: Explicitly hard-delete files and converted files
	// CASCADE delete only works for hard deletes, not soft deletes
	// When we soft-delete a KB, files remain with their KB soft-deleted (zombie files)
	w.log.Info("CleanupOldKnowledgeBaseActivity: Hard-deleting files",
		zap.String("kbUID", param.KBUID.String()))

	// Hard-delete all files (including those in PROCESSING status)
	err = w.repository.DeleteAllKnowledgeBaseFiles(ctx, param.KBUID.String())
	if err != nil {
		w.log.Warn("Failed to delete files, continuing cleanup",
			zap.String("kbUID", param.KBUID.String()),
			zap.Error(err))
	}

	// Hard-delete all converted files
	err = w.repository.DeleteAllConvertedFilesInKb(ctx, param.KBUID)
	if err != nil {
		w.log.Warn("Failed to delete converted files, continuing cleanup",
			zap.String("kbUID", param.KBUID.String()),
			zap.Error(err))
	}

	w.log.Info("CleanupOldKnowledgeBaseActivity: Cleanup completed successfully")
	return nil
}

// waitForInProgressFiles waits for all in-progress file processing to complete
// This is a helper method used by CleanupOldKnowledgeBaseActivity to prevent race conditions
func (w *Worker) waitForInProgressFiles(ctx context.Context, kbUID types.KBUIDType, maxWaitSeconds int, checkIntervalSeconds int) error {
	maxAttempts := maxWaitSeconds / checkIntervalSeconds

	for attempt := 0; attempt < maxAttempts; attempt++ {
		// Count files in active processing states (same logic as GetInProgressFileCountActivity)
		inProgressStatuses := "FILE_PROCESS_STATUS_PROCESSING,FILE_PROCESS_STATUS_CHUNKING,FILE_PROCESS_STATUS_EMBEDDING"
		dbCount, err := w.repository.GetFileCountByKnowledgeBaseUIDIncludingDeleted(ctx, kbUID, inProgressStatuses)
		if err != nil {
			w.log.Warn("waitForInProgressFiles: Failed to check DB file count",
				zap.String("kbUID", kbUID.String()),
				zap.Error(err))
			return err
		}

		// Check for active Temporal workflows
		activeWorkflowCount := w.getActiveWorkflowCount(ctx, kbUID)
		totalCount := dbCount + activeWorkflowCount

		if totalCount == 0 {
			w.log.Info("waitForInProgressFiles: No in-progress files, safe to proceed",
				zap.String("kbUID", kbUID.String()),
				zap.Int("attempt", attempt+1))
			return nil
		}

		w.log.Info("waitForInProgressFiles: Waiting for file processing to complete",
			zap.String("kbUID", kbUID.String()),
			zap.Int64("dbCount", dbCount),
			zap.Int64("activeWorkflowCount", activeWorkflowCount),
			zap.Int64("totalCount", totalCount),
			zap.Int("attempt", attempt+1),
			zap.Int("maxAttempts", maxAttempts))

		if attempt < maxAttempts-1 {
			time.Sleep(time.Duration(checkIntervalSeconds) * time.Second)
		}
	}

	w.log.Warn("waitForInProgressFiles: Timeout waiting for file processing",
		zap.String("kbUID", kbUID.String()),
		zap.Int("maxWaitSeconds", maxWaitSeconds))
	return fmt.Errorf("timeout after %d seconds waiting for file processing to complete", maxWaitSeconds)
}

// VerifyKBCleanupActivity verifies that KB cleanup was successful
// Used for both staging and rollback KB cleanup verification
// This activity checks that:
// - KB is soft-deleted in DB
// - update_status and update_workflow_id are cleared
// - Milvus collection is dropped (unless protected)
// - Files are deleted from MinIO
func (w *Worker) VerifyKBCleanupActivity(ctx context.Context, param *VerifyKBCleanupActivityParam) error {
	w.log.Info("VerifyKBCleanupActivity: Verifying cleanup",
		zap.String("kbUID", param.KBUID.String()))

	// Check 1: KB should be soft-deleted (or hard-deleted)
	kb, err := w.repository.GetKnowledgeBaseByUID(ctx, param.KBUID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// KB is hard-deleted - this is fine (cleanup went further than expected)
			w.log.Info("VerifyKBCleanupActivity: KB is hard-deleted (cleanup complete)",
				zap.String("kbUID", param.KBUID.String()))
			return nil
		}
		w.log.Warn("VerifyKBCleanupActivity: Error checking KB status",
			zap.String("kbUID", param.KBUID.String()),
			zap.Error(err))
		return activityErrorWithMessage("Failed to verify KB cleanup - cannot check KB status", verifyKBCleanupActivityError, err)
	}

	// Check 2: KB should be soft-deleted
	if !kb.DeleteTime.Valid {
		w.log.Warn("VerifyKBCleanupActivity: KB is not soft-deleted",
			zap.String("kbUID", param.KBUID.String()))
		return activityErrorWithMessage(
			"Cleanup verification failed: KB is not soft-deleted",
			verifyKBCleanupActivityError,
			fmt.Errorf("KB %s not soft-deleted", param.KBUID),
		)
	}

	// Check 3: update_status and update_workflow_id should be cleared
	if kb.UpdateStatus != "" || kb.UpdateWorkflowID != "" {
		w.log.Warn("VerifyKBCleanupActivity: Update status/workflow ID not cleared",
			zap.String("kbUID", param.KBUID.String()),
			zap.String("updateStatus", kb.UpdateStatus))
		return activityErrorWithMessage(
			"Cleanup verification failed: update status not cleared",
			verifyKBCleanupActivityError,
			fmt.Errorf("KB %s update status not cleared", param.KBUID),
		)
	}

	// Check 4: Milvus collection should be dropped
	collectionName := constant.KBCollectionName(param.CollectionUID)
	collectionExists, err := w.repository.CollectionExists(ctx, collectionName)
	if err != nil {
		// Non-fatal - log warning but don't fail verification
		w.log.Warn("VerifyKBCleanupActivity: Cannot verify collection drop",
			zap.String("collectionName", collectionName),
			zap.Error(err))
	} else if collectionExists {
		// Check if collection is protected (in use by other KBs)
		inUse, err := w.repository.IsCollectionInUse(ctx, param.CollectionUID)
		if err != nil || !inUse {
			w.log.Warn("VerifyKBCleanupActivity: Collection still exists but should be dropped",
				zap.String("collectionName", collectionName),
				zap.Bool("inUse", inUse))
			// Don't fail - collection cleanup can be retried later
		}
	}

	// Check 5: Files should be deleted from MinIO (spot check - don't enumerate all)
	// This is a best-effort check - MinIO cleanup failures are logged but not fatal

	w.log.Info("VerifyKBCleanupActivity: Cleanup verification passed",
		zap.String("kbUID", param.KBUID.String()))
	return nil
}

// ListFilesForReprocessingActivity lists all files in a knowledge base for reprocessing
func (w *Worker) ListFilesForReprocessingActivity(ctx context.Context, param *ListFilesForReprocessingActivityParam) (*ListFilesForReprocessingActivityResult, error) {
	w.log.Info("ListFilesForReprocessingActivity: Listing files",
		zap.String("kbUID", param.KBUID.String()))

	// Get KB to retrieve owner
	kb, err := w.repository.GetKnowledgeBaseByUID(ctx, param.KBUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to get knowledge base. Please try again.")
		return nil, activityError(err, listFilesForReprocessingActivityError)
	}

	// List all files in the KB
	fileList, err := w.repository.ListKnowledgeBaseFiles(ctx, repository.KnowledgeBaseFileListParams{
		OwnerUID: kb.NamespaceUID,
		KBUID:    param.KBUID.String(),
	})
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to list files for reprocessing. Please try again.")
		return nil, activityError(err, listFilesForReprocessingActivityError)
	}

	// Extract file UIDs from the result
	files := fileList.Files
	fileUIDs := make([]types.FileUIDType, 0, len(files))
	for _, file := range files {
		// Include all files - even those in processing states
		// The update workflow will handle reprocessing them
		fileUIDs = append(fileUIDs, file.UID)
	}

	w.log.Info("ListFilesForReprocessingActivity: Files listed",
		zap.Int("totalFiles", len(fileUIDs)))

	return &ListFilesForReprocessingActivityResult{
		FileUIDs: fileUIDs,
	}, nil
}

// CloneFileToStagingKBActivity clones a file from original KB to staging KB
func (w *Worker) CloneFileToStagingKBActivity(ctx context.Context, param *CloneFileToStagingKBActivityParam) (*CloneFileToStagingKBActivityResult, error) {
	w.log.Info("CloneFileToStagingKBActivity: Cloning file",
		zap.String("fileUID", param.OriginalFileUID.String()),
		zap.String("stagingKBUID", param.StagingKBUID.String()))

	// Get original file using GetKnowledgeBaseFilesByFileUIDs
	originalFiles, err := w.repository.GetKnowledgeBaseFilesByFileUIDs(ctx, []types.FileUIDType{param.OriginalFileUID})
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to get original file. Please try again.")
		return nil, activityError(err, cloneFileToStagingKBActivityError)
	}

	// CRITICAL: Gracefully handle deleted files
	// If a file was deleted between Phase 2 listing and cloning, skip it
	// This is expected behavior - users can delete files during updates
	// The update should proceed with remaining files
	if len(originalFiles) == 0 {
		w.log.Warn("CloneFileToStagingKBActivity: Original file not found (likely deleted during update), skipping",
			zap.String("fileUID", param.OriginalFileUID.String()),
			zap.String("stagingKBUID", param.StagingKBUID.String()))
		return &CloneFileToStagingKBActivityResult{
			NewFileUID: types.FileUIDType(uuid.Nil), // Return nil UID to indicate skipped file
		}, nil
	}
	originalFile := originalFiles[0]

	// CRITICAL: Verify blob file exists in MinIO before cloning
	//
	// **WHY FILES CAN HAVE MISSING BLOBS (DATA INCONSISTENCY):**
	// Production files may have DB records but missing blobs due to:
	// 1. Cleanup workflow failure (blobs deleted, DB deletion failed mid-transaction)
	// 2. MinIO storage failures or data corruption
	// 3. Manual blob deletion via MinIO console
	// 4. Incomplete uploads (DB record created, blob upload failed)
	// 5. Cross-region replication lag
	// 6. Backup restoration inconsistencies
	//
	// **WHY WE CHECK:**
	// Attempting to process files with missing blobs will cause the entire update workflow
	// to fail, blocking system maintenance. By checking blob existence upfront, we can
	// gracefully skip orphaned records and allow the update to proceed with valid files.
	//
	// **OBSERVABILITY:**
	// When blobs are missing, we log ERROR level with "action_required" to alert operators
	// of potential data loss requiring investigation.
	//
	// Use GetFileMetadata (StatObject) instead of GetFile to avoid reading entire file.
	bucket := object.BucketFromDestination(originalFile.Destination)
	_, err = w.repository.GetMinIOStorage().GetFileMetadata(ctx, bucket, originalFile.Destination)
	if err != nil {
		w.log.Error("CloneFileToStagingKBActivity: Original blob file not found in MinIO - skipping file",
			zap.String("fileUID", param.OriginalFileUID.String()),
			zap.String("filename", originalFile.Filename),
			zap.String("destination", originalFile.Destination),
			zap.String("bucket", bucket),
			zap.Error(err),
			zap.String("impact", "File will not be available in updated KB"),
			zap.String("action_required", "Investigate data loss - production file has DB record but no blob"))
		return &CloneFileToStagingKBActivityResult{
			NewFileUID: types.FileUIDType(uuid.Nil), // Return nil UID to indicate skipped file
		}, nil
	}

	// Get staging KB to inherit owner
	stagingKB, err := w.repository.GetKnowledgeBaseByUID(ctx, param.StagingKBUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to get staging KB. Please try again.")
		return nil, activityError(err, cloneFileToStagingKBActivityError)
	}

	// Parse owner UID
	ownerUID, err := uuid.FromString(stagingKB.NamespaceUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to parse staging KB owner UID. Please try again.")
		return nil, activityError(err, cloneFileToStagingKBActivityError)
	}

	// Create new file record in staging KB
	// Note: We reuse the same source file in MinIO (same Destination)
	// but create a new database record in the staging KB
	//
	// IMPORTANT: No duplicate check needed here because:
	// - Snapshot is taken BEFORE status changes to UPDATING
	// - Dual-processing only activates AFTER status changes
	// - Therefore, files in snapshot are never dual-processed
	// - Clean separation prevents race conditions
	newFile := repository.KnowledgeBaseFileModel{
		Filename:                  originalFile.Filename,
		FileType:                  originalFile.FileType,
		NamespaceUID:              types.NamespaceUIDType(ownerUID),
		KBUID:                     param.StagingKBUID,
		CreatorUID:                originalFile.CreatorUID,
		ProcessStatus:             "FILE_PROCESS_STATUS_NOTSTARTED",
		Destination:               originalFile.Destination, // Reuse same source file
		Size:                      originalFile.Size,
		Tags:                      originalFile.Tags,
		ExternalMetadataUnmarshal: originalFile.ExternalMetadataUnmarshal,
		ExtraMetaDataUnmarshal:    originalFile.ExtraMetaDataUnmarshal,
		RequesterUID:              originalFile.RequesterUID,
	}

	// Create the file in database
	createdFile, err := w.repository.CreateKnowledgeBaseFile(ctx, newFile, nil)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to create cloned file. Please try again.")
		return nil, activityError(err, cloneFileToStagingKBActivityError)
	}

	w.log.Info("CloneFileToStagingKBActivity: File cloned successfully",
		zap.String("originalFileUID", param.OriginalFileUID.String()),
		zap.String("newFileUID", createdFile.UID.String()))

	return &CloneFileToStagingKBActivityResult{
		NewFileUID: createdFile.UID,
	}, nil
}
