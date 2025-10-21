package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	errorsx "github.com/instill-ai/x/errors"
)

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
	listFilesForReprocessingActivityError        = "ListFilesForReprocessingActivity"
	cloneFileToStagingKBActivityError            = "CloneFileToStagingKBActivity"
)

// ListKnowledgeBasesForUpdateActivityParam defines parameters for listing KBs needing update
type ListKnowledgeBasesForUpdateActivityParam struct {
	CatalogIDs []string
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

// CreateStagingKnowledgeBaseActivityParam defines parameters for creating staging KB
type CreateStagingKnowledgeBaseActivityParam struct {
	OriginalKBUID types.KBUIDType
	// SystemProfile specifies which system profile to use for the new embedding config
	// If empty, uses the original KB's embedding config
	SystemProfile string
}

// CreateStagingKnowledgeBaseActivityResult contains the created staging KB
type CreateStagingKnowledgeBaseActivityResult struct {
	StagingKB repository.KnowledgeBaseModel
}

// SynchronizeKBActivityParam defines parameters for final synchronization before swap
type SynchronizeKBActivityParam struct {
	OriginalKBUID types.KBUIDType
	StagingKBUID  types.KBUIDType
}

// SynchronizeKBActivityResult contains synchronization results
type SynchronizeKBActivityResult struct {
	Synchronized bool
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
	RollbackKBUID types.KBUIDType // UID of the created/reused rollback KB
	StagingKBUID  types.KBUIDType // UID of the staging KB (for cleanup after swap)
}

// UpdateKnowledgeBaseUpdateStatusActivityParam updates update status
type UpdateKnowledgeBaseUpdateStatusActivityParam struct {
	KBUID      types.KBUIDType
	Status     string
	WorkflowID string
}

// CleanupOldKnowledgeBaseActivityParam defines parameters for cleanup
type CleanupOldKnowledgeBaseActivityParam struct {
	KBUID types.KBUIDType
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
		zap.Strings("catalogIDs", param.CatalogIDs))

	var kbs []repository.KnowledgeBaseModel
	var err error

	if len(param.CatalogIDs) > 0 {
		// Get specific catalogs by ID
		for _, catalogID := range param.CatalogIDs {
			kb, getErr := w.repository.GetKnowledgeBaseByID(ctx, catalogID)
			if getErr != nil {
				w.log.Warn("Unable to get catalog", zap.String("catalogID", catalogID), zap.Error(getErr))
				continue
			}
			// Only include production KBs (not staging) and not already updating
			if !kb.Staging && (kb.UpdateStatus == "" || kb.UpdateStatus == "completed" || kb.UpdateStatus == "failed" || kb.UpdateStatus == "rolled_back") {
				kbs = append(kbs, *kb)
				w.log.Info("Catalog eligible for update", zap.String("catalogID", catalogID), zap.String("kbUID", kb.UID.String()), zap.String("updateStatus", kb.UpdateStatus))
			} else {
				w.log.Warn("Catalog filtered out", zap.String("catalogID", catalogID), zap.String("kbUID", kb.UID.String()), zap.Bool("staging", kb.Staging), zap.String("updateStatus", kb.UpdateStatus))
			}
		}
	} else {
		// List all eligible KBs (production, not currently updating)
		kbs, err = w.repository.ListKnowledgeBasesForUpdate(ctx, nil, nil)
		if err != nil {
			err = errorsx.AddMessage(err, "Unable to list knowledge bases for update. Please try again.")
			return nil, temporal.NewApplicationErrorWithCause(
				errorsx.MessageOrErr(err),
				listKnowledgeBasesForUpdateActivityError,
				err,
			)
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
func (w *Worker) ValidateUpdateEligibilityActivity(ctx context.Context, param *ValidateUpdateEligibilityActivityParam) error {
	w.log.Info("ValidateUpdateEligibilityActivity: Checking eligibility",
		zap.String("kbUID", param.KBUID.String()))

	kb, err := w.repository.GetKnowledgeBaseByUID(ctx, param.KBUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to get knowledge base for validation. Please try again.")
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			validateUpdateEligibilityActivityError,
			err,
		)
	}

	// Check if already updating
	if kb.UpdateStatus == "updating" {
		err = fmt.Errorf("knowledge base is already updating: %s", param.KBUID.String())
		return temporal.NewApplicationErrorWithCause(
			err.Error(),
			validateUpdateEligibilityActivityError,
			err,
		)
	}

	w.log.Info("ValidateUpdateEligibilityActivity: KB is eligible",
		zap.String("kbUID", param.KBUID.String()))

	return nil
}

// CreateStagingKnowledgeBaseActivity creates a staging KB for updating
func (w *Worker) CreateStagingKnowledgeBaseActivity(ctx context.Context, param *CreateStagingKnowledgeBaseActivityParam) (*CreateStagingKnowledgeBaseActivityResult, error) {
	w.log.Info("CreateStagingKnowledgeBaseActivity: Creating staging KB for updating",
		zap.String("originalKBUID", param.OriginalKBUID.String()),
		zap.String("systemProfile", param.SystemProfile))

	originalKB, err := w.repository.GetKnowledgeBaseByUID(ctx, param.OriginalKBUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to get original knowledge base. Please try again.")
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			createStagingKnowledgeBaseActivityError,
			err,
		)
	}

	// Determine which embedding config to use
	var newEmbeddingConfig *repository.EmbeddingConfigJSON
	if param.SystemProfile != "" {
		// Use config from specified system profile
		embeddingConfig, err := w.repository.GetDefaultEmbeddingConfig(ctx, param.SystemProfile)
		if err != nil {
			err = errorsx.AddMessage(err, fmt.Sprintf("Unable to get embedding config from system profile %q. Please try again.", param.SystemProfile))
			return nil, temporal.NewApplicationErrorWithCause(
				errorsx.MessageOrErr(err),
				createStagingKnowledgeBaseActivityError,
				err,
			)
		}
		newEmbeddingConfig = embeddingConfig
		w.log.Info("Using embedding config from system profile",
			zap.String("systemProfile", param.SystemProfile),
			zap.String("modelFamily", embeddingConfig.ModelFamily),
			zap.Uint32("dimensionality", embeddingConfig.Dimensionality))
	} else {
		// Use original KB's config (for reprocessing without changing config)
		w.log.Info("Using original KB's embedding config",
			zap.String("modelFamily", originalKB.EmbeddingConfig.ModelFamily),
			zap.Uint32("dimensionality", originalKB.EmbeddingConfig.Dimensionality))
	}

	// Determine dimensionality for the new collection
	// Use the new embedding config if specified (from system profile), otherwise use original KB's config
	var dimensionality uint32
	if newEmbeddingConfig != nil {
		dimensionality = newEmbeddingConfig.Dimensionality
	} else {
		dimensionality = originalKB.EmbeddingConfig.Dimensionality
	}

	// Create Milvus collection for staging KB
	externalServiceCall := func(kbUID types.KBUIDType) error {
		// Create collection with updated dimensionality from embedding config
		// Use KBCollectionName to convert UUID to valid collection name (replace hyphens with underscores)
		collectionName := constant.KBCollectionName(kbUID)
		err := w.repository.CreateCollection(ctx, collectionName, dimensionality)
		if err != nil {
			return fmt.Errorf("creating vector database collection: %w", err)
		}

		// Copy ACL permissions from original
		// Note: ACL copying is handled by the service layer
		return nil
	}

	stagingKB, err := w.repository.CreateStagingKnowledgeBase(ctx, originalKB, newEmbeddingConfig, externalServiceCall)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to create staging knowledge base. Please try again.")
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			createStagingKnowledgeBaseActivityError,
			err,
		)
	}

	w.log.Info("CreateStagingKnowledgeBaseActivity: Staging KB created",
		zap.String("stagingKBUID", stagingKB.UID.String()),
		zap.String("stagingKBName", stagingKB.Name))

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
	// 2. User uploads new file → dual processing starts
	// 3. We proceed to swap
	// 4. Swap happens with incomplete staging file
	//
	// By locking FIRST, we ensure no new dual processing can start after this point
	originalKB, err := w.repository.GetKnowledgeBaseByUID(ctx, param.OriginalKBUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to get knowledge base for synchronization. Please try again.")
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			synchronizeKBActivityError,
			err,
		)
	}

	// Use optimistic locking: only transition if still in UPDATING status
	// This prevents race with concurrent updates/rollbacks
	err = w.repository.UpdateKnowledgeBaseWithMap(ctx, originalKB.KBID, originalKB.Owner, map[string]interface{}{
		"update_status": "swapping",
	})
	if err != nil {
		// If update fails, someone else modified the KB - fail and let Temporal retry
		err := fmt.Errorf("failed to lock KB for swapping (concurrent modification): %w", err)
		w.log.Error("SynchronizeKBActivity: Failed to acquire swap lock",
			zap.String("originalKBUID", param.OriginalKBUID.String()),
			zap.Error(err))
		return nil, temporal.NewApplicationErrorWithCause(
			err.Error(),
			synchronizeKBActivityError,
			err,
		)
	}

	w.log.Info("SynchronizeKBActivity: KB locked for swapping (status: updating → swapping)",
		zap.String("originalKBUID", param.OriginalKBUID.String()))

	// STEP 2: Wait for all in-progress files to complete (FINAL SYNCHRONIZATION)
	// This ensures all files that were dual-processed BEFORE the lock are now complete
	// Since we locked above, no NEW dual processing can start, so this count is final
	inProgressCount, err := w.repository.GetFileCountByKnowledgeBaseUID(ctx, param.StagingKBUID, "FILE_PROCESS_STATUS_NOTSTARTED,FILE_PROCESS_STATUS_WAITING,FILE_PROCESS_STATUS_PROCESSING,FILE_PROCESS_STATUS_CONVERTING,FILE_PROCESS_STATUS_CHUNKING,FILE_PROCESS_STATUS_EMBEDDING")
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to check for in-progress files. Please try again.")
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			synchronizeKBActivityError,
			err,
		)
	}

	if inProgressCount > 0 {
		// Return a retryable error - files are still processing
		// These are files that were dual-processed BEFORE the lock
		// We wait for them to complete before proceeding with swap
		err := fmt.Errorf("staging KB has %d files still processing (waiting for final synchronization)", inProgressCount)
		w.log.Info("SynchronizeKBActivity: Final synchronization in progress - files still processing, will retry",
			zap.String("stagingKBUID", param.StagingKBUID.String()),
			zap.Int64("inProgressCount", inProgressCount))
		return nil, temporal.NewApplicationErrorWithCause(
			err.Error(),
			synchronizeKBActivityError,
			err,
		)
	}

	w.log.Info("SynchronizeKBActivity: Final synchronization complete",
		zap.String("stagingKBUID", param.StagingKBUID.String()),
		zap.String("status", "All staging files processed, ready for validation"))

	return &SynchronizeKBActivityResult{
		Synchronized: true,
	}, nil
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

	stagingCount, err := w.repository.GetFileCountByKnowledgeBaseUID(ctx, param.StagingKBUID, "")
	if err != nil {
		errors = append(errors, fmt.Sprintf("failed to get staging file count: %v", err))
	}

	if len(errors) == 0 {
		w.log.Info("ValidateUpdatedKBActivity: File counts",
			zap.Int("expectedFileCount", param.ExpectedFileCount),
			zap.Int64("productionCount", productionCount),
			zap.Int64("stagingCount", stagingCount))

		// CRITICAL: Production and staging must have EXACT same file count
		// This ensures users see no change in file count before/after swap
		// Dual processing guarantees this by adding new files to both KBs
		if productionCount != stagingCount {
			errors = append(errors, fmt.Sprintf("file count mismatch: production has %d, staging has %d (must be identical for seamless swap)",
				productionCount, stagingCount))
		}

		// Sanity check: staging should have at least the expected base file count
		// (original files + any files uploaded during update)
		if stagingCount < int64(param.ExpectedFileCount) {
			errors = append(errors, fmt.Sprintf("staging file count too low: expected at least %d (original files), staging has %d",
				param.ExpectedFileCount, stagingCount))
		}
	}

	// VALIDATION 2: Verify active collection UID is set for staging KB
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

	// VALIDATION 3: Verify converted file counts
	if len(errors) == 0 {
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

			// Staging should have at least as many converted files as we expect
			// (original count or more due to dual processing)
			if stagingConvertedCount < originalConvertedCount {
				errors = append(errors, fmt.Sprintf("converted file count mismatch: original=%d, staging=%d",
					originalConvertedCount, stagingConvertedCount))
			}
		}
	}

	// VALIDATION 4: Verify chunk counts
	var stagingChunkCount int64
	if len(errors) == 0 {
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

			// Staging should have at least some chunks if original had files
			// Note: Chunk count may differ due to different chunking strategies
			if originalChunkCount > 0 && stagingChunkCount == 0 {
				errors = append(errors, "staging KB has no chunks but original had chunks")
			}
		}
	}

	// VALIDATION 5: Verify embedding counts match chunk counts
	if len(errors) == 0 {
		stagingEmbeddingCount, err := w.repository.GetEmbeddingCountByKBUID(ctx, param.StagingKBUID)
		if err != nil {
			errors = append(errors, fmt.Sprintf("failed to get staging embedding count: %v", err))
		} else {
			w.log.Info("ValidateUpdatedKBActivity: Embedding counts",
				zap.Int64("stagingChunkCount", stagingChunkCount),
				zap.Int64("stagingEmbeddingCount", stagingEmbeddingCount))

			// Embeddings should match chunks (one embedding per chunk)
			if stagingEmbeddingCount != stagingChunkCount {
				errors = append(errors, fmt.Sprintf("embedding/chunk count mismatch: chunks=%d, embeddings=%d",
					stagingChunkCount, stagingEmbeddingCount))
			}
		}
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
			}, temporal.NewApplicationErrorWithCause(
				errorsx.MessageOrErr(err),
				validateUpdatedKBActivityError,
				err,
			)
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

	originalKB, err := w.repository.GetKnowledgeBaseByUID(ctx, param.OriginalKBUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to get original knowledge base. Please try again.")
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			swapKnowledgeBasesActivityError,
			err,
		)
	}

	stagingKB, err := w.repository.GetKnowledgeBaseByUID(ctx, param.StagingKBUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to get staging knowledge base. Please try again.")
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			swapKnowledgeBasesActivityError,
			err,
		)
	}

	// Step 1: Create/update rollback KB to store old resources
	rollbackName := fmt.Sprintf("%s-rollback", originalKB.Name)
	rollbackKBID := fmt.Sprintf("%s-rollback", originalKB.KBID)

	ownerUID, err := uuid.FromString(originalKB.Owner)
	if err != nil {
		err = errorsx.AddMessage(err, "Invalid owner UID format")
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			swapKnowledgeBasesActivityError,
			err,
		)
	}

	// Check if rollback KB exists from previous update
	existingRollback, err := w.repository.GetKnowledgeBaseByOwnerAndKbID(ctx, types.OwnerUIDType(ownerUID), rollbackKBID)
	var rollbackKBUID types.KBUIDType

	if err == nil && existingRollback != nil {
		// Reuse existing rollback KB (just update its metadata)
		rollbackKBUID = existingRollback.UID
		w.log.Info("SwapKnowledgeBasesActivity: Reusing existing rollback KB",
			zap.String("rollbackKBUID", rollbackKBUID.String()))
	} else {
		// Create new rollback KB
		rollbackKBUID = types.KBUIDType(uuid.Must(uuid.NewV4()))
		retentionUntil := time.Now().Add(time.Duration(param.RetentionDays) * 24 * time.Hour)

		rollbackKB := &repository.KnowledgeBaseModel{
			UID:                    rollbackKBUID,
			KBID:                   rollbackKBID, // Use rollback catalog ID
			Name:                   rollbackName,
			Owner:                  originalKB.Owner,
			CreatorUID:             originalKB.CreatorUID,
			Tags:                   append(originalKB.Tags, "rollback"),
			Staging:                true,
			UpdateStatus:           "completed",
			RollbackRetentionUntil: &retentionUntil,
			CatalogType:            originalKB.CatalogType,
			EmbeddingConfig:        originalKB.EmbeddingConfig,
		}

		// Create rollback KB without external service callback (no ACL needed for staging KB)
		_, err = w.repository.CreateKnowledgeBase(ctx, *rollbackKB, nil)
		if err != nil {
			err = errorsx.AddMessage(err, "Unable to create rollback KB. Please try again.")
			return nil, temporal.NewApplicationErrorWithCause(
				errorsx.MessageOrErr(err),
				swapKnowledgeBasesActivityError,
				err,
			)
		}
		w.log.Info("SwapKnowledgeBasesActivity: Created new rollback KB",
			zap.String("rollbackKBUID", rollbackKBUID.String()))
	}

	// Step 2: Swap resources AND collection pointers (using temp UID to avoid conflicts)
	// CRITICAL: The original KB UID remains unchanged!
	// NEW: We also swap active_collection_uid to support dimension changes
	w.log.Info("SwapKnowledgeBasesActivity: Swapping resources and collection pointers (keeping original KB UID constant)",
		zap.String("originalKBUID", param.OriginalKBUID.String()),
		zap.String("stagingKBUID", param.StagingKBUID.String()),
		zap.String("rollbackKBUID", rollbackKBUID.String()))

	tempUID := uuid.Must(uuid.NewV4())

	// Step 2a: Move original KB's resources → temp
	if err := w.updateResourceKBUIDs(ctx, param.OriginalKBUID, types.KBUIDType(tempUID)); err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			"Failed to move original resources to temp",
			swapKnowledgeBasesActivityError,
			err,
		)
	}

	// Step 2b: Move staging KB's resources → original KB (staging resources become production)
	if err := w.updateResourceKBUIDs(ctx, param.StagingKBUID, param.OriginalKBUID); err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			"Failed to move staging resources to production",
			swapKnowledgeBasesActivityError,
			err,
		)
	}

	// Step 2c: Move temp resources → rollback KB (old resources saved for rollback)
	if err := w.updateResourceKBUIDs(ctx, types.KBUIDType(tempUID), rollbackKBUID); err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			"Failed to move old resources to rollback",
			swapKnowledgeBasesActivityError,
			err,
		)
	}

	// Step 3: Swap collection pointers and metadata
	// This is the KEY to supporting dimension changes:
	// - Production KB now points to staging's collection (which may have different dimensionality)
	// - Rollback KB points to original's collection (preserves old dimensionality for rollback)
	// - Collections themselves are NOT moved - only the pointers are swapped
	retentionUntil := time.Now().Add(time.Duration(param.RetentionDays) * 24 * time.Hour)

	// Check that collection UIDs are set (not uuid.Nil)
	if originalKB.ActiveCollectionUID == uuid.Nil || stagingKB.ActiveCollectionUID == uuid.Nil {
		return nil, temporal.NewApplicationErrorWithCause(
			"active_collection_uid is unset (uuid.Nil) for original or staging KB",
			swapKnowledgeBasesActivityError,
			fmt.Errorf("originalKB.ActiveCollectionUID=%v, stagingKB.ActiveCollectionUID=%v", originalKB.ActiveCollectionUID, stagingKB.ActiveCollectionUID),
		)
	}

	w.log.Info("SwapKnowledgeBasesActivity: Swapping collection pointers",
		zap.String("originalCollectionUID", originalKB.ActiveCollectionUID.String()),
		zap.String("stagingCollectionUID", stagingKB.ActiveCollectionUID.String()))

	// Update production KB to point to staging's collection
	err = w.repository.UpdateKnowledgeBaseWithMap(ctx, originalKB.KBID, originalKB.Owner, map[string]interface{}{
		"active_collection_uid":    stagingKB.ActiveCollectionUID, // Point to new collection
		"embedding_config":         stagingKB.EmbeddingConfig,     // Update embedding config (may have new dimensionality)
		"staging":                  false,
		"update_status":            "completed",
		"rollback_retention_until": retentionUntil,
	})
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to update production KB metadata and collection pointer. Please try again.")
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			swapKnowledgeBasesActivityError,
			err,
		)
	}

	// Update rollback KB to point to original's collection (preserve old dimensionality)
	// Note: Use the KBID string, not the UID, for the lookup
	rollbackKBIDStr := fmt.Sprintf("%s-rollback", originalKB.KBID)
	err = w.repository.UpdateKnowledgeBaseWithMap(ctx, rollbackKBIDStr, originalKB.Owner, map[string]interface{}{
		"active_collection_uid": originalKB.ActiveCollectionUID, // Keep original collection pointer
		"embedding_config":      originalKB.EmbeddingConfig,     // Keep original embedding config
	})
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to update rollback KB metadata. Please try again.")
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			swapKnowledgeBasesActivityError,
			err,
		)
	}

	// Step 4: Delete staging KB immediately (no longer needed - its resources are now in production)
	w.log.Info("SwapKnowledgeBasesActivity: Marking staging KB for deletion",
		zap.String("stagingKBUID", stagingKB.UID.String()),
		zap.String("stagingKBID", stagingKB.KBID))

	// Mark staging KB for immediate deletion to prevent it from blocking future updates
	now := time.Now()
	err = w.repository.UpdateKnowledgeBaseWithMap(ctx, stagingKB.KBID, stagingKB.Owner, map[string]interface{}{
		"delete_time":   &now,
		"update_status": "", // Clear update status so it doesn't block future updates
	})
	if err != nil {
		w.log.Error("SwapKnowledgeBasesActivity: Failed to mark staging KB for deletion (non-fatal)",
			zap.String("stagingKBUID", stagingKB.UID.String()),
			zap.Error(err))
		// Non-fatal: Continue anyway, cleanup will happen later
	}

	// NOTE: Do NOT trigger cleanup here - it creates a race condition where the collection
	// gets dropped before the production KB's active_collection_uid update is visible.
	// Cleanup will be triggered by the main workflow after the swap activity completes.

	w.log.Info("SwapKnowledgeBasesActivity: Resource swap completed successfully",
		zap.String("productionKBUID", param.OriginalKBUID.String()),
		zap.String("rollbackKBUID", rollbackKBUID.String()),
		zap.String("stagingKBUID", stagingKB.UID.String()))

	return &SwapKnowledgeBasesActivityResult{
		RollbackKBUID: rollbackKBUID,
		StagingKBUID:  stagingKB.UID, // Return staging KB UID for cleanup
	}, nil
}

// updateResourceKBUIDs updates kb_uid in all resource tables (files, chunks, embeddings, converted_files)
func (w *Worker) updateResourceKBUIDs(ctx context.Context, fromKBUID, toKBUID types.KBUIDType) error {
	// Update kb_uid in all related resource tables using repository methods
	if err := w.repository.UpdateKnowledgeBaseResources(ctx, fromKBUID, toKBUID); err != nil {
		w.log.Error("Failed to update kb_uid in resource tables",
			zap.String("fromKBUID", fromKBUID.String()),
			zap.String("toKBUID", toKBUID.String()),
			zap.Error(err))
		return fmt.Errorf("updating kb_uid in resources: %w", err)
	}

	w.log.Info("Successfully updated kb_uid references in all resource tables",
		zap.String("fromKBUID", fromKBUID.String()),
		zap.String("toKBUID", toKBUID.String()))

	return nil
}

// UpdateKnowledgeBaseUpdateStatusActivity updates the update status of a KB
func (w *Worker) UpdateKnowledgeBaseUpdateStatusActivity(ctx context.Context, param *UpdateKnowledgeBaseUpdateStatusActivityParam) error {
	w.log.Info("UpdateKnowledgeBaseUpdateStatusActivity: Updating status",
		zap.String("kbUID", param.KBUID.String()),
		zap.String("status", param.Status))

	err := w.repository.UpdateKnowledgeBaseUpdateStatus(ctx, param.KBUID, param.Status, param.WorkflowID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to update knowledge base update status. Please try again.")
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			updateKnowledgeBaseUpdateStatusActivityError,
			err,
		)
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
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			cleanupOldKnowledgeBaseActivityError,
			err,
		)
	}

	// Only soft delete if not already deleted
	if kb.DeleteTime == nil {
		_, err = w.repository.DeleteKnowledgeBase(ctx, kb.Owner, kb.KBID)
		if err != nil {
			// If already deleted, that's okay - continue with collection drop
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				err = errorsx.AddMessage(err, "Unable to delete knowledge base. Please try again.")
				return temporal.NewApplicationErrorWithCause(
					errorsx.MessageOrErr(err),
					cleanupOldKnowledgeBaseActivityError,
					err,
				)
			}
			w.log.Info("CleanupOldKnowledgeBaseActivity: KB already soft-deleted, continuing",
				zap.String("kbUID", param.KBUID.String()))
		}
	} else {
		w.log.Info("CleanupOldKnowledgeBaseActivity: KB already soft-deleted, skipping soft-delete",
			zap.String("kbUID", param.KBUID.String()),
			zap.Time("deleteTime", *kb.DeleteTime))
	}

	// Drop Milvus collection using active_collection_uid
	collectionUID := kb.ActiveCollectionUID
	if collectionUID == uuid.Nil {
		// Fallback to KB UID if no active_collection_uid (shouldn't happen but be safe)
		collectionUID = param.KBUID
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

	// Note: Embeddings and files are handled by CASCADE delete in PostgreSQL

	w.log.Info("CleanupOldKnowledgeBaseActivity: Cleanup completed successfully")
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
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			listFilesForReprocessingActivityError,
			err,
		)
	}

	// List all files in the KB
	fileList, err := w.repository.ListKnowledgeBaseFiles(ctx, repository.KnowledgeBaseFileListParams{
		OwnerUID: kb.Owner,
		KBUID:    param.KBUID.String(),
	})
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to list files for reprocessing. Please try again.")
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			listFilesForReprocessingActivityError,
			err,
		)
	}

	// Extract file UIDs from the result
	files := fileList.Files
	fileUIDs := make([]types.FileUIDType, 0, len(files))
	for _, file := range files {
		// Only include successfully processed or failed files (skip already processing ones)
		if file.ProcessStatus != "FILE_PROCESS_STATUS_WAITING" &&
			file.ProcessStatus != "FILE_PROCESS_STATUS_CONVERTING" {
			fileUIDs = append(fileUIDs, file.UID)
		}
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
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			cloneFileToStagingKBActivityError,
			err,
		)
	}
	if len(originalFiles) == 0 {
		err := fmt.Errorf("original file not found: %s", param.OriginalFileUID.String())
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			cloneFileToStagingKBActivityError,
			err,
		)
	}
	originalFile := originalFiles[0]

	// Get staging KB to inherit owner
	stagingKB, err := w.repository.GetKnowledgeBaseByUID(ctx, param.StagingKBUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to get staging KB. Please try again.")
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			cloneFileToStagingKBActivityError,
			err,
		)
	}

	// Parse owner UID
	ownerUID, err := uuid.FromString(stagingKB.Owner)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to parse staging KB owner UID. Please try again.")
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			cloneFileToStagingKBActivityError,
			err,
		)
	}

	// Create new file record in staging KB
	// Note: We reuse the same source file in MinIO (same Destination)
	// but create a new database record in the staging KB
	newFile := repository.KnowledgeBaseFileModel{
		Name:                      originalFile.Name,
		FileType:                  originalFile.FileType,
		Owner:                     types.NamespaceUIDType(ownerUID),
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
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			cloneFileToStagingKBActivityError,
			err,
		)
	}

	w.log.Info("CloneFileToStagingKBActivity: File cloned successfully",
		zap.String("originalFileUID", param.OriginalFileUID.String()),
		zap.String("newFileUID", createdFile.UID.String()))

	return &CloneFileToStagingKBActivityResult{
		NewFileUID: createdFile.UID,
	}, nil
}
