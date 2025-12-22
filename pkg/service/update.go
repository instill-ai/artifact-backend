package service

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	logx "github.com/instill-ai/x/log"
)

// RollbackAdmin rolls back a knowledge base to its previous state
// CRITICAL DESIGN: The production KB UID remains constant - only resources are swapped
// This preserves the KB identity and ACL permissions throughout the rollback
func (s *service) RollbackAdmin(ctx context.Context, ownerUID types.OwnerUIDType, namespaceID string, knowledgeBaseID string) (*artifactpb.RollbackAdminResponse, error) {
	// Get the current production knowledge base
	productionKB, err := s.repository.GetKnowledgeBaseByOwnerAndKbID(ctx, ownerUID, knowledgeBaseID)
	if err != nil {
		return nil, fmt.Errorf("knowledge base not found: %w", err)
	}

	// Find the rollback KB (contains the old resources to restore)
	// Use parent_kb_uid lookup instead of KBID string manipulation
	rollbackKB, err := s.repository.GetRollbackKBForProduction(ctx, ownerUID, knowledgeBaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to find rollback KB: %w", err)
	}
	if rollbackKB == nil {
		return nil, fmt.Errorf("rollback version not found for knowledge base: %s", knowledgeBaseID)
	}

	logger, _ := logx.GetZapLogger(ctx)

	// SYNCHRONIZATION: Wait for all in-progress file operations to complete
	// This is critical to prevent corrupted state if files are being processed during rollback
	// Same pattern as production ↔ staging swap in update workflow
	logger.Info("Rollback: Checking for in-progress file operations",
		zap.String("productionKBUID", productionKB.UID.String()),
		zap.String("rollbackKBUID", rollbackKB.UID.String()))

	// Check both production and rollback KBs for in-progress files
	// These are files in any non-final state (not COMPLETED or FAILED)
	inProgressStatuses := "FILE_PROCESS_STATUS_NOTSTARTED,FILE_PROCESS_STATUS_PROCESSING,FILE_PROCESS_STATUS_CHUNKING,FILE_PROCESS_STATUS_EMBEDDING"

	prodInProgress, err := s.repository.GetFileCountByKnowledgeBaseUID(ctx, productionKB.UID, inProgressStatuses)
	if err != nil {
		return nil, fmt.Errorf("failed to check production KB for in-progress files: %w", err)
	}

	rollbackInProgress, err := s.repository.GetFileCountByKnowledgeBaseUID(ctx, rollbackKB.UID, inProgressStatuses)
	if err != nil {
		return nil, fmt.Errorf("failed to check rollback KB for in-progress files: %w", err)
	}

	totalInProgress := prodInProgress + rollbackInProgress

	if totalInProgress > 0 {
		logger.Info("Rollback: Waiting for in-progress file operations to complete",
			zap.Int64("prodInProgress", prodInProgress),
			zap.Int64("rollbackInProgress", rollbackInProgress),
			zap.Int64("totalInProgress", totalInProgress))

		// Poll with timeout until all files are complete
		// Same pattern as synchronization in update workflow but with explicit polling
		maxWaitTime := 10 * time.Minute // Generous timeout for large files
		pollInterval := 5 * time.Second
		deadline := time.Now().Add(maxWaitTime)
		pollCount := 0

		for time.Now().Before(deadline) {
			time.Sleep(pollInterval)
			pollCount++

			// Check again
			prodInProgress, _ = s.repository.GetFileCountByKnowledgeBaseUID(ctx, productionKB.UID, inProgressStatuses)
			rollbackInProgress, _ = s.repository.GetFileCountByKnowledgeBaseUID(ctx, rollbackKB.UID, inProgressStatuses)
			totalInProgress = prodInProgress + rollbackInProgress

			if totalInProgress == 0 {
				logger.Info("Rollback: All in-progress files completed",
					zap.Int("pollCount", pollCount),
					zap.Duration("waitTime", time.Duration(pollCount)*pollInterval))
				break
			}

			// Log progress every 10 polls (50 seconds)
			if pollCount%10 == 0 {
				logger.Info("Rollback: Still waiting for file operations",
					zap.Int64("prodInProgress", prodInProgress),
					zap.Int64("rollbackInProgress", rollbackInProgress),
					zap.Int64("totalRemaining", totalInProgress),
					zap.Int("pollCount", pollCount))
			}
		}

		// Final check - fail if still in progress after timeout
		if totalInProgress > 0 {
			return nil, fmt.Errorf(
				"timeout waiting for file operations to complete after %v: %d files still processing (prod=%d, rollback=%d). "+
					"Please wait for ongoing operations to finish and try again",
				maxWaitTime, totalInProgress, prodInProgress, rollbackInProgress)
		}
	} else {
		logger.Info("Rollback: No in-progress file operations, proceeding with swap")
	}

	// Swap resources between production and rollback KBs
	// CRITICAL: Production KB UID stays the same, only its resources change
	logger.Info("Rollback: Swapping resources (keeping production KB UID constant)",
		zap.String("productionKBUID", productionKB.UID.String()),
		zap.String("rollbackKBUID", rollbackKB.UID.String()))

	// Save system UIDs before swapping - these need to be swapped along with resources
	// Production currently has new system config, rollback has old system config
	productionSystemUID := productionKB.SystemUID
	rollbackSystemUID := rollbackKB.SystemUID

	logger.Info("Rollback: System configs before swap",
		zap.String("productionSystemUID", productionSystemUID.String()),
		zap.String("rollbackSystemUID", rollbackSystemUID.String()))

	// Use temp UID to avoid conflicts during the swap
	tempUID := uuid.Must(uuid.NewV4())

	// Step 1: Move production KB's resources → temp (current/new data)
	if err := s.repository.UpdateKnowledgeBaseResources(ctx, productionKB.UID, types.KBUIDType(tempUID)); err != nil {
		return nil, fmt.Errorf("failed to move production resources to temp: %w", err)
	}

	// Step 2: Move rollback KB's resources → production KB (restore old data)
	// CRITICAL: Resources now point to the SAME production KB UID
	if err := s.repository.UpdateKnowledgeBaseResources(ctx, rollbackKB.UID, productionKB.UID); err != nil {
		return nil, fmt.Errorf("failed to restore rollback resources to production: %w", err)
	}

	// Step 3: Move temp resources → rollback KB (save current data for potential re-rollback)
	if err := s.repository.UpdateKnowledgeBaseResources(ctx, types.KBUIDType(tempUID), rollbackKB.UID); err != nil {
		return nil, fmt.Errorf("failed to move current resources to rollback: %w", err)
	}

	// Step 4: Swap system configs
	// Production KB gets the old system config (from rollback)
	// This ensures the restored embeddings match their original system config
	err = s.repository.UpdateKnowledgeBaseWithMap(ctx, productionKB.KBID, productionKB.NamespaceUID, map[string]any{
		"update_status": artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_ROLLED_BACK.String(),
		"staging":       false, // Ensure it stays as production
		"system_uid":    rollbackSystemUID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to update production KB metadata: %w", err)
	}

	// Step 5: Update rollback KB to have the new system config
	// Rollback KB gets the new system config (from production)
	// This maintains consistency - rollback KB represents what was rolled back from
	err = s.repository.UpdateKnowledgeBaseWithMap(ctx, rollbackKB.KBID, productionKB.NamespaceUID, map[string]any{
		"system_uid": productionSystemUID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to update rollback KB system config: %w", err)
	}

	logger.Info("Rollback: System configs after swap",
		zap.String("productionSystemUID", rollbackSystemUID.String()),
		zap.String("rollbackSystemUID", productionSystemUID.String()))

	// Get updated knowledge base
	updatedKB, err := s.repository.GetKnowledgeBaseByOwnerAndKbID(ctx, ownerUID, knowledgeBaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to get updated knowledge base: %w", err)
	}

	logger.Info("Rollback: Successfully rolled back",
		zap.String("knowledgeBaseID", knowledgeBaseID),
		zap.String("productionKBUID", productionKB.UID.String()))

	return &artifactpb.RollbackAdminResponse{
		KnowledgeBase: convertKBToCatalogProto(updatedKB, namespaceID),
		Message:       "Successfully rolled back to previous version",
	}, nil
}

// PurgeRollbackAdmin manually purges the rollback knowledge base immediately
func (s *service) PurgeRollbackAdmin(ctx context.Context, ownerUID types.OwnerUIDType, knowledgeBaseID string) (*artifactpb.PurgeRollbackAdminResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Find rollback KB using parent_kb_uid lookup
	rollbackKB, err := s.repository.GetRollbackKBForProduction(ctx, ownerUID, knowledgeBaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to find rollback KB: %w", err)
	}
	if rollbackKB == nil {
		return nil, fmt.Errorf("rollback version not found for knowledge base: %s", knowledgeBaseID)
	}

	// SYNCHRONIZATION: Wait for any in-progress file operations to complete
	// This prevents purging while files are still being processed
	// Same pattern as rollback synchronization
	logger.Info("PurgeRollback: Checking for in-progress file operations",
		zap.String("rollbackKBUID", rollbackKB.UID.String()))

	inProgressStatuses := "FILE_PROCESS_STATUS_NOTSTARTED,FILE_PROCESS_STATUS_PROCESSING,FILE_PROCESS_STATUS_CHUNKING,FILE_PROCESS_STATUS_EMBEDDING"

	rollbackInProgress, err := s.repository.GetFileCountByKnowledgeBaseUID(ctx, rollbackKB.UID, inProgressStatuses)
	if err != nil {
		return nil, fmt.Errorf("failed to check rollback KB for in-progress files: %w", err)
	}

	if rollbackInProgress > 0 {
		logger.Info("PurgeRollback: Waiting for in-progress file operations to complete",
			zap.Int64("rollbackInProgress", rollbackInProgress))

		// Poll with timeout until all files are complete
		maxWaitTime := 5 * time.Minute // Shorter timeout for purge operation
		pollInterval := 5 * time.Second
		deadline := time.Now().Add(maxWaitTime)
		pollCount := 0

		for time.Now().Before(deadline) {
			time.Sleep(pollInterval)
			pollCount++

			// Check again
			rollbackInProgress, _ = s.repository.GetFileCountByKnowledgeBaseUID(ctx, rollbackKB.UID, inProgressStatuses)

			if rollbackInProgress == 0 {
				logger.Info("PurgeRollback: All in-progress files completed",
					zap.Int("pollCount", pollCount),
					zap.Duration("waitTime", time.Duration(pollCount)*pollInterval))
				break
			}

			// Log progress every 6 polls (30 seconds)
			if pollCount%6 == 0 {
				logger.Info("PurgeRollback: Still waiting for file operations",
					zap.Int64("rollbackInProgress", rollbackInProgress),
					zap.Int("pollCount", pollCount))
			}
		}

		// Final check - fail if still in progress after timeout
		if rollbackInProgress > 0 {
			return nil, fmt.Errorf(
				"timeout waiting for file operations to complete after %v: %d files still processing. "+
					"Please wait for ongoing operations to finish and try again",
				maxWaitTime, rollbackInProgress)
		}
	} else {
		logger.Info("PurgeRollback: No in-progress file operations, proceeding with purge")
	}

	// Count files before deletion
	fileCount := int32(0)
	if count, err := s.repository.GetFileCountByKnowledgeBaseUID(ctx, rollbackKB.UID, ""); err == nil {
		fileCount = int32(count)
	}

	// Trigger cleanup workflow via worker
	// Note: Source files in MinIO are always preserved as they may be shared with production
	err = s.worker.CleanupKnowledgeBase(ctx, rollbackKB.UID)
	if err != nil {
		return nil, fmt.Errorf("failed to start cleanup workflow: %w", err)
	}

	// Clear the rollback_retention_until field on the production KB
	// Since the rollback KB is now being purged, there's no need to track retention anymore
	productionKB, err := s.repository.GetKnowledgeBaseByOwnerAndKbID(ctx, ownerUID, knowledgeBaseID)
	if err == nil && productionKB != nil {
		err = s.repository.UpdateKnowledgeBaseWithMap(ctx, knowledgeBaseID, productionKB.NamespaceUID, map[string]any{
			"rollback_retention_until": nil,
		})
		if err != nil {
			logger.Warn("PurgeRollback: Failed to clear retention field on production KB",
				zap.String("knowledgeBaseID", knowledgeBaseID),
				zap.Error(err))
			// Don't fail the purge operation if we can't clear the retention field
		} else {
			logger.Info("PurgeRollback: Cleared retention field on production KB",
				zap.String("knowledgeBaseID", knowledgeBaseID))
		}
	}

	logger.Info("PurgeRollback: Successfully triggered cleanup workflow",
		zap.String("rollbackKBUID", rollbackKB.UID.String()),
		zap.Int32("fileCount", fileCount))

	return &artifactpb.PurgeRollbackAdminResponse{
		Success:                true,
		PurgedKnowledgeBaseUid: rollbackKB.UID.String(),
		DeletedFiles:           fileCount,
		Message:                "Rollback knowledge base purged successfully",
	}, nil
}

// SetRollbackRetentionAdmin sets the rollback retention period with flexible time units.
// This also reschedules the cleanup workflow to use the new retention period.
func (s *service) SetRollbackRetentionAdmin(ctx context.Context, ownerUID types.OwnerUIDType, knowledgeBaseID string, duration int32, timeUnit artifactpb.SetRollbackRetentionAdminRequest_TimeUnit) (*artifactpb.SetRollbackRetentionAdminResponse, error) {
	// Get current knowledge base
	currentKB, err := s.repository.GetKnowledgeBaseByOwnerAndKbID(ctx, ownerUID, knowledgeBaseID)
	if err != nil {
		return nil, fmt.Errorf("knowledge base not found: %w", err)
	}

	if currentKB.RollbackRetentionUntil == nil {
		return nil, fmt.Errorf("no rollback retention set for this knowledge base")
	}

	previousRetention := *currentKB.RollbackRetentionUntil

	// Convert duration to time.Duration based on time unit
	var retentionDuration time.Duration
	switch timeUnit {
	case artifactpb.SetRollbackRetentionAdminRequest_TIME_UNIT_SECOND:
		retentionDuration = time.Duration(duration) * time.Second
	case artifactpb.SetRollbackRetentionAdminRequest_TIME_UNIT_MINUTE:
		retentionDuration = time.Duration(duration) * time.Minute
	case artifactpb.SetRollbackRetentionAdminRequest_TIME_UNIT_HOUR:
		retentionDuration = time.Duration(duration) * time.Hour
	case artifactpb.SetRollbackRetentionAdminRequest_TIME_UNIT_DAY:
		retentionDuration = time.Duration(duration) * 24 * time.Hour
	default:
		return nil, fmt.Errorf("invalid time unit: %v", timeUnit)
	}

	// Set retention to exactly duration from NOW
	newRetention := time.Now().Add(retentionDuration)

	// Find the rollback KB using parent_kb_uid lookup
	// IMPORTANT: Retry logic for race conditions
	// The rollback KB might not be immediately visible after the update workflow completes
	// due to DB commit timing. Retry up to 3 times with 1s delay.
	logger, _ := logx.GetZapLogger(ctx)
	var rollbackKB *repository.KnowledgeBaseModel
	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		rollbackKB, err = s.repository.GetRollbackKBForProduction(ctx, ownerUID, currentKB.KBID)
		if err == nil && rollbackKB != nil {
			break
		}
		if attempt < maxRetries {
			logger.Warn("Rollback KB not found yet, retrying...",
				zap.String("productionKBID", currentKB.KBID),
				zap.Int("attempt", attempt),
				zap.Int("maxRetries", maxRetries))
			time.Sleep(time.Second)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to find rollback KB after %d attempts: %w", maxRetries, err)
	}
	if rollbackKB == nil {
		return nil, fmt.Errorf("rollback KB not found for production KB %s after %d attempts", currentKB.KBID, maxRetries)
	}

	// CRITICAL: Update retention timestamp on ROLLBACK KB (not production KB)
	// The cleanup workflow checks the rollback KB's retention field to decide when to clean up
	_, err = s.repository.UpdateKnowledgeBase(ctx, rollbackKB.KBID, rollbackKB.NamespaceUID, repository.KnowledgeBaseModel{
		RollbackRetentionUntil: &newRetention,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to update rollback KB retention: %w", err)
	}

	// Also update production KB's retention field for reference/audit trail
	_, err = s.repository.UpdateKnowledgeBase(ctx, currentKB.KBID, currentKB.NamespaceUID, repository.KnowledgeBaseModel{
		RollbackRetentionUntil: &newRetention,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to update production KB retention: %w", err)
	}

	// Reschedule cleanup workflow with new retention period
	retentionSeconds := int64(retentionDuration.Seconds())
	err = s.worker.RescheduleCleanupWorkflow(ctx, rollbackKB.UID, retentionSeconds)
	if err != nil {
		return nil, fmt.Errorf("failed to reschedule cleanup workflow: %w", err)
	}

	logger.Info("Successfully rescheduled cleanup workflow",
		zap.String("rollbackKBUID", rollbackKB.UID.String()),
		zap.Int64("newRetentionSeconds", retentionSeconds))

	totalSeconds := int64(time.Until(newRetention).Seconds())

	return &artifactpb.SetRollbackRetentionAdminResponse{
		PreviousRetentionUntil: previousRetention.Format(time.RFC3339),
		NewRetentionUntil:      newRetention.Format(time.RFC3339),
		TotalRetentionSeconds:  totalSeconds,
	}, nil
}

// GetKnowledgeBaseUpdateStatusAdmin returns the current status of system update
func (s *service) GetKnowledgeBaseUpdateStatusAdmin(ctx context.Context) (*artifactpb.GetKnowledgeBaseUpdateStatusAdminResponse, error) {
	// Get all production KBs across all owners
	kbs, err := s.repository.ListAllKnowledgeBasesAdmin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list all knowledge bases: %w", err)
	}

	if len(kbs) == 0 {
		// Return empty status
		return &artifactpb.GetKnowledgeBaseUpdateStatusAdminResponse{
			UpdateInProgress: false,
			Details:          []*artifactpb.KnowledgeBaseUpdateDetails{},
			Message:          "No knowledge bases found",
		}, nil
	}

	// OPTIMIZATION: Pre-fetch all systems in a single query to avoid N+1 problem
	// Fetch all systems once and build a UID->ID lookup map
	systems, err := s.repository.ListSystems(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list systems: %w", err)
	}

	systemCache := make(map[string]string) // systemUID -> systemID
	for _, system := range systems {
		systemCache[system.UID.String()] = system.ID
	}

	var knowledgeBaseStatuses []*artifactpb.KnowledgeBaseUpdateDetails
	updateInProgress := false
	totalKnowledgeBases := len(kbs)
	knowledgeBasesCompleted := 0
	knowledgeBasesFailed := 0
	knowledgeBasesInProgress := 0

	for _, kb := range kbs {
		// Count status for summary
		switch kb.UpdateStatus {
		case artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_UPDATING.String():
			updateInProgress = true
			knowledgeBasesInProgress++
		case artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED.String():
			knowledgeBasesCompleted++
		case artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_FAILED.String():
			knowledgeBasesFailed++
		}

		// Get total file count for this KB
		totalFiles := int32(0)
		if count, err := s.repository.GetFileCountByKnowledgeBaseUID(ctx, kb.UID, ""); err == nil {
			totalFiles = int32(count)
		}

		// For updating KBs, find the associated staging KB to count completed files and get current system ID
		filesProcessed := int32(0)
		currentSystemID := ""
		previousSystemID := ""

		// Get previous system ID from previous_system_uid (if set)
		if kb.PreviousSystemUID.String() != uuid.Nil.String() {
			if prevSystemID, ok := systemCache[kb.PreviousSystemUID.String()]; ok {
				previousSystemID = prevSystemID
			}
		}

		// Get current system ID - for UPDATING status, this is the staging KB's system (what we're updating TO)
		// For all other statuses, this is the production KB's current system
		switch kb.UpdateStatus {
		case artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_UPDATING.String():
			// Find the staging KB (it has the same ID with "-staging" suffix)
			stagingKBID := fmt.Sprintf("%s-staging", kb.KBID)
			stagingKB, err := s.repository.GetKnowledgeBaseByOwnerAndKbID(ctx, types.OwnerUIDType(uuid.FromStringOrNil(kb.NamespaceUID)), stagingKBID)
			if err == nil && stagingKB != nil {
				// Count completed files in the staging KB
				if count, err := s.repository.GetFileCountByKnowledgeBaseUID(ctx, stagingKB.UID, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED.String()); err == nil {
					filesProcessed = int32(count)
				}

				// Get current system ID from staging KB's system_uid (use cache)
				// During UPDATING, the staging KB represents the "current" target we're updating to
				if stagingSystemID, ok := systemCache[stagingKB.SystemUID.String()]; ok {
					currentSystemID = stagingSystemID
				}
			}

		default:
			// For all other statuses (COMPLETED, FAILED, ROLLED_BACK, ABORTED, NONE),
			// show the production KB's current system ID
			if prodSystemID, ok := systemCache[kb.SystemUID.String()]; ok {
				currentSystemID = prodSystemID
			}
		}

		// Convert database string status to protobuf enum using value map
		// If update_status is empty or null, it defaults to NONE
		statusEnum := artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_NONE
		if kb.UpdateStatus != "" {
			if val, ok := artifactpb.KnowledgeBaseUpdateStatus_value[kb.UpdateStatus]; ok {
				statusEnum = artifactpb.KnowledgeBaseUpdateStatus(val)
			}
		}

		knowledgeBaseStatuses = append(knowledgeBaseStatuses, &artifactpb.KnowledgeBaseUpdateDetails{
			KnowledgeBaseUid: kb.UID.String(),
			Status:           statusEnum,
			WorkflowId:       kb.UpdateWorkflowID,
			StartedAt:        formatTime(kb.UpdateStartedAt),
			CompletedAt:      formatTime(kb.UpdateCompletedAt),
			FilesProcessed:   filesProcessed,
			TotalFiles:       totalFiles,
			ErrorMessage:     kb.UpdateErrorMessage, // Populated only for FAILED status
			CurrentSystemId:  currentSystemID,       // Current system (staging during UPDATING, production otherwise)
			PreviousSystemId: previousSystemID,      // System before update started (for audit trail)
		})
	}

	response := &artifactpb.GetKnowledgeBaseUpdateStatusAdminResponse{
		UpdateInProgress: updateInProgress,
		Details:          knowledgeBaseStatuses,
		Message:          fmt.Sprintf("Total knowledge bases: %d. Update status: %d in progress, %d completed, %d failed", totalKnowledgeBases, knowledgeBasesInProgress, knowledgeBasesCompleted, knowledgeBasesFailed),
	}

	return response, nil
}

// ExecuteKnowledgeBaseUpdateAdmin executes update for specified knowledge bases or all eligible knowledge bases
func (s *service) ExecuteKnowledgeBaseUpdateAdmin(ctx context.Context, req *artifactpb.ExecuteKnowledgeBaseUpdateAdminRequest) (*artifactpb.ExecuteKnowledgeBaseUpdateAdminResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// DESIGN: Per-KB lock instead of global lock
	// Each KB has its own update_status field that prevents concurrent updates of the same KB
	// Multiple different KBs can update simultaneously (scalability + parallel testing)
	// The filtering logic below already excludes KBs with update_status="updating"

	// Get target KBs (filtering already excludes KBs with update_status="updating")
	var kbs []repository.KnowledgeBaseModel
	if len(req.KnowledgeBaseIds) > 0 {
		// Update specific knowledge bases
		for _, knowledgeBaseID := range req.KnowledgeBaseIds {
			kb, err := s.repository.GetKnowledgeBaseByID(ctx, knowledgeBaseID)
			if err != nil {
				logger.Error("ExecuteKnowledgeBaseUpdate: Failed to get knowledge base", zap.String("knowledgeBaseID", knowledgeBaseID), zap.Error(err))
				return nil, fmt.Errorf("failed to get knowledge base %s: %w", knowledgeBaseID, err)
			}
			logger.Info("ExecuteKnowledgeBaseUpdate: Checking knowledge base eligibility", zap.String("knowledgeBaseID", knowledgeBaseID), zap.String("kbUID", kb.UID.String()), zap.Bool("staging", kb.Staging), zap.String("updateStatus", kb.UpdateStatus))
			// Only include production KBs (not staging) and not already updating
			if !kb.Staging && repository.IsUpdateComplete(kb.UpdateStatus) {
				kbs = append(kbs, *kb)
				logger.Info("ExecuteKnowledgeBaseUpdate: Knowledge base eligible for update", zap.String("knowledgeBaseID", knowledgeBaseID), zap.String("kbUID", kb.UID.String()))
			} else {
				logger.Warn("ExecuteKnowledgeBaseUpdate: Knowledge base not eligible", zap.String("knowledgeBaseID", knowledgeBaseID), zap.String("kbUID", kb.UID.String()), zap.Bool("staging", kb.Staging), zap.String("updateStatus", kb.UpdateStatus))
			}
		}
	} else {
		// Update all eligible KBs (production, not currently updating)
		var err error
		kbs, err = s.repository.ListKnowledgeBasesForUpdate(ctx, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to list eligible knowledge bases: %w", err)
		}
	}

	if len(kbs) == 0 {
		return &artifactpb.ExecuteKnowledgeBaseUpdateAdminResponse{
			Started: false,
			Message: "No eligible knowledge bases found for update",
		}, nil
	}

	// Extract knowledge base IDs
	knowledgeBaseIDs := make([]string, len(kbs))
	for i, kb := range kbs {
		knowledgeBaseIDs[i] = kb.KBID
	}

	// Execute knowledge base update via worker
	// Pass system_id to worker (empty string if not specified)
	systemID := ""
	if req.SystemId != nil {
		systemID = *req.SystemId
	}

	result, err := s.worker.ExecuteKnowledgeBaseUpdate(ctx, knowledgeBaseIDs, systemID)
	if err != nil {
		return nil, fmt.Errorf("failed to execute knowledge base update: %w", err)
	}

	return &artifactpb.ExecuteKnowledgeBaseUpdateAdminResponse{
		Started: result.Started,
		Message: result.Message,
	}, nil
}

// AbortKnowledgeBaseUpdateAdmin aborts ongoing KB update workflows
func (s *service) AbortKnowledgeBaseUpdateAdmin(ctx context.Context, req *artifactpb.AbortKnowledgeBaseUpdateAdminRequest) (*artifactpb.AbortKnowledgeBaseUpdateAdminResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	logger.Info("AbortKnowledgeBaseUpdateAdmin called", zap.Int("knowledgeBaseCount", len(req.KnowledgeBaseIds)), zap.Strings("knowledgeBaseIds", req.KnowledgeBaseIds))

	// Execute abort via worker
	result, err := s.worker.AbortKnowledgeBaseUpdate(ctx, req.KnowledgeBaseIds)
	if err != nil {
		logger.Error("AbortKnowledgeBaseUpdateAdmin failed", zap.Error(err))
		return nil, fmt.Errorf("failed to abort knowledge base update: %w", err)
	}

	// Convert worker result to protobuf response
	var knowledgeBaseStatuses []*artifactpb.KnowledgeBaseUpdateDetails
	for _, status := range result.KnowledgeBaseStatus {
		// Convert status string to protobuf enum using value map
		statusEnum := artifactpb.KnowledgeBaseUpdateStatus(artifactpb.KnowledgeBaseUpdateStatus_value[status.Status])
		if statusEnum == 0 && status.Status != "" {
			// If conversion failed and string is not empty, default to UNSPECIFIED
			statusEnum = artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_UNSPECIFIED
		}

		knowledgeBaseStatuses = append(knowledgeBaseStatuses, &artifactpb.KnowledgeBaseUpdateDetails{
			KnowledgeBaseUid: status.KnowledgeBaseUID,
			Status:           statusEnum,
			WorkflowId:       status.WorkflowID,
		})
	}

	logger.Info("AbortKnowledgeBaseUpdateAdmin completed",
		zap.Bool("success", result.Success),
		zap.Int("abortedCount", result.AbortedCount),
		zap.String("message", result.Message))

	return &artifactpb.AbortKnowledgeBaseUpdateAdminResponse{
		Success: result.Success,
		Message: result.Message,
		Details: knowledgeBaseStatuses,
	}, nil
}

// Helper functions

func convertKBToCatalogProto(kb *repository.KnowledgeBaseModel, namespaceID string) *artifactpb.KnowledgeBase {
	// Construct Google AIP resource name: namespaces/{namespace}/knowledge-bases/{knowledge_base}
	// Note: namespace format is "users/user-123" or "organizations/org-456"
	resourceName := fmt.Sprintf("namespaces/%s/knowledge-bases/%s", namespaceID, kb.KBID)

	return &artifactpb.KnowledgeBase{
		Name:           resourceName,
		Uid:            kb.UID.String(),
		Id:             kb.KBID,
		Description:    kb.Description,
		Tags:           kb.Tags,
		OwnerName:      kb.NamespaceUID,
		CreateTime:     timestamppb.New(*kb.CreateTime),
		UpdateTime:     timestamppb.New(*kb.UpdateTime),
		DownstreamApps: []string{},
		TotalFiles:     0, // Would need to query file count if needed
		TotalTokens:    0,
	}
}

func formatTime(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Format(time.RFC3339)
}
