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
	"github.com/instill-ai/artifact-backend/pkg/types"

	errorsx "github.com/instill-ai/x/errors"
)

// This file contains cleanup activities used by CleanupFileWorkflow and CleanupKnowledgeBaseWorkflow:
// - DeleteOriginalFileActivity - Deletes original uploaded files from MinIO
// - DeleteConvertedFileActivity - Deletes converted markdown files from database
// - DeleteTextChunksFromMinIOActivity - Deletes text chunk content files from MinIO
// - DeleteEmbeddingsFromVectorDBActivity - Removes embeddings from vector database
// - DeleteEmbeddingRecordsActivity - Removes embedding records from database
// - DeleteKBFilesFromMinIOActivity - Deletes all files for a knowledge base
// - DropVectorDBCollectionActivity - Drops vector database collection
// - DeleteKBFileRecordsActivity - Deletes file records for a knowledge base
// - DeleteKBConvertedFileRecordsActivity - Deletes converted file records
// - DeleteKBTextChunkRecordsActivity - Deletes text chunk records for a knowledge base
// - DeleteKBEmbeddingRecordsActivity - Deletes embedding records for a knowledge base
// - PurgeKBACLActivity - Removes ACL permissions for a knowledge base
// - SoftDeleteKBRecordActivity - Soft-deletes knowledge base record (clears update_status first)
// - GetInProgressFileCountActivity - Checks for in-progress files before cleanup
// - GetKBCollectionUIDActivity - Retrieves active_collection_uid for verification
// - ClearProductionKBRetentionActivity - Clears rollback retention timestamp on production KB

// Activity error type constants
const (
	deleteOriginalFileActivityError             = "DeleteOriginalFileActivity"
	deleteConvertedFileActivityError            = "DeleteConvertedFileActivity"
	deleteTextChunksActivityError               = "DeleteTextChunksFromMinIOActivity"
	deleteEmbeddingsActivityError               = "DeleteEmbeddingsFromVectorDBActivity"
	deleteKBFilesActivityError                  = "DeleteKBFilesFromMinIOActivity"
	dropCollectionActivityError                 = "DropVectorDBCollectionActivity"
	deleteKBFileRecordsActivityError            = "DeleteKBFileRecordsActivity"
	deleteKBConvertedRecordsActivityError       = "DeleteKBConvertedFileRecordsActivity"
	deleteKBTextChunkRecordsActivityError       = "DeleteKBTextChunkRecordsActivity"
	deleteKBEmbeddingRecordsActivityError       = "DeleteKBEmbeddingRecordsActivity"
	purgeKBACLActivityError                     = "PurgeKBACLActivity"
	softDeleteKBRecordActivityError             = "SoftDeleteKBRecordActivity"
	getInProgressFileCountActivityError         = "GetInProgressFileCountActivity"
	getKBCollectionUIDActivityError             = "GetKBCollectionUIDActivity"
	cleanupExpiredGCSFilesActivityError         = "CleanupExpiredGCSFilesActivity"
	checkRollbackRetentionExpiredActivityError  = "CheckRollbackRetentionExpiredActivity"
)

// DeleteOriginalFileActivityParam defines parameters for deleting original file
type DeleteOriginalFileActivityParam struct {
	FileUID types.FileUIDType // File unique identifier
	Bucket  string            // MinIO bucket containing the file
}

// DeleteConvertedFileActivityParam defines parameters for deleting converted file
type DeleteConvertedFileActivityParam struct {
	FileUID types.FileUIDType // File unique identifier
}

// DeleteTextChunksFromMinIOActivityParam defines parameters for deleting text chunks from MinIO
type DeleteTextChunksFromMinIOActivityParam struct {
	FileUID types.FileUIDType // File unique identifier
}

// DeleteEmbeddingsFromVectorDBActivityParam defines parameters for deleting embeddings from vector db
type DeleteEmbeddingsFromVectorDBActivityParam struct {
	FileUID types.FileUIDType // File unique identifier
}

// DeleteFileRecordsActivityParam defines parameters for deleting file records from DB
type DeleteFileRecordsActivityParam struct {
	FileUID types.FileUIDType // File unique identifier
}

// DeleteOriginalFileActivity deletes the original uploaded file from MinIO
func (w *Worker) DeleteOriginalFileActivity(ctx context.Context, param *DeleteOriginalFileActivityParam) error {
	w.log.Info("DeleteOriginalFileActivity: Deleting original file",
		zap.String("fileUID", param.FileUID.String()))

	// Fetch file record to get destination
	files, err := w.repository.GetFilesByFileUIDs(ctx, []types.FileUIDType{param.FileUID})
	if err != nil || len(files) == 0 {
		w.log.Info("DeleteOriginalFileActivity: File not found, may have been deleted already",
			zap.String("fileUID", param.FileUID.String()))
		return nil
	}

	file := files[0]
	if file.StoragePath == "" {
		w.log.Info("DeleteOriginalFileActivity: File has no storage path, skipping",
			zap.String("fileUID", param.FileUID.String()))
		return nil
	}

	err = w.deleteFilesSync(ctx, param.Bucket, []string{file.StoragePath})
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to delete file from storage. Please try again.")
		return activityError(err, deleteOriginalFileActivityError)
	}

	// CRITICAL: Also delete the blob object record from the object table
	// This prevents orphaned records when blob files are deleted from MinIO
	// The storage path format is "ns-{namespaceUID}/obj-{objectUID}"
	// We need to delete the object record to keep DB and MinIO in sync
	if err := w.repository.DeleteObjectByStoragePath(ctx, file.StoragePath); err != nil {
		// Log but don't fail - this is cleanup, and the object might already be deleted
		w.log.Warn("DeleteOriginalFileActivity: Failed to delete object record (may not exist)",
			zap.String("fileUID", param.FileUID.String()),
			zap.String("storagePath", file.StoragePath),
			zap.Error(err))
	}

	return nil
}

// DeleteConvertedFileActivity deletes converted file from MinIO and DB
func (w *Worker) DeleteConvertedFileActivity(ctx context.Context, param *DeleteConvertedFileActivityParam) error {
	w.log.Info("DeleteConvertedFileActivity: Deleting converted files",
		zap.String("fileUID", param.FileUID.String()))

	// CRITICAL: Get ALL converted files for this file UID (not just one)
	// Files can have multiple converted versions (e.g., content + summary)
	// Using GetConvertedFileByFileUID() only returns ONE, leaving orphaned records
	convertedFiles, err := w.repository.GetAllConvertedFilesByFileUID(ctx, param.FileUID)
	if err != nil {
		w.log.Info("DeleteConvertedFileActivity: Error fetching converted files, skipping",
			zap.String("fileUID", param.FileUID.String()),
			zap.Error(err))
		return nil
	}

	if len(convertedFiles) == 0 {
		w.log.Info("DeleteConvertedFileActivity: No converted files found, skipping",
			zap.String("fileUID", param.FileUID.String()))
		return nil
	}

	w.log.Info("DeleteConvertedFileActivity: Found converted files to delete",
		zap.String("fileUID", param.FileUID.String()),
		zap.Int("count", len(convertedFiles)))

	// Delete all converted files from MinIO and DB
	for _, convertedFile := range convertedFiles {
		// Delete from MinIO using KB UID from the converted file record
		err = w.deleteConvertedFileByFileUIDSync(ctx, convertedFile.KnowledgeBaseUID, param.FileUID)
		if err != nil {
			w.log.Error("DeleteConvertedFileActivity: Failed to delete converted file from MinIO",
				zap.String("convertedFileUID", convertedFile.UID.String()),
				zap.Error(err))
			err = errorsx.AddMessage(err, "Unable to delete converted file from storage. Please try again.")
			return activityError(err, deleteConvertedFileActivityError)
		}

		w.log.Info("DeleteConvertedFileActivity: Deleted from MinIO",
			zap.String("convertedFileUID", convertedFile.UID.String()),
			zap.String("kbUID", convertedFile.KnowledgeBaseUID.String()),
			zap.String("convertedType", convertedFile.ConvertedType))
	}

	// Delete all records from DB in one operation
	err = w.repository.HardDeleteConvertedFileByFileUID(ctx, param.FileUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to delete converted file records. Please try again.")
		return activityError(err, deleteConvertedFileActivityError)
	}

	w.log.Info("DeleteConvertedFileActivity: Successfully deleted all converted files",
		zap.String("fileUID", param.FileUID.String()),
		zap.Int("count", len(convertedFiles)))

	return nil
}

// DeleteTextChunksFromMinIOActivity deletes text chunks from MinIO and DB
func (w *Worker) DeleteTextChunksFromMinIOActivity(ctx context.Context, param *DeleteTextChunksFromMinIOActivityParam) error {
	w.log.Info("DeleteTextChunksFromMinIOActivity: Deleting text chunks",
		zap.String("fileUID", param.FileUID.String()))

	// Check if text textChunks exist and get KB UID from them
	textChunks, err := w.repository.ListTextChunksByKBFileUID(ctx, param.FileUID)
	if err != nil || len(textChunks) == 0 {
		w.log.Info("DeleteTextChunksFromMinIOActivity: No text chunks found, skipping",
			zap.String("fileUID", param.FileUID.String()))
		return nil
	}

	// Get KB UID from the first text chunk (all text chunks have the same KB UID)
	kbUID := textChunks[0].KnowledgeBaseUID

	// Delete from MinIO using KB UID from the text chunk records
	err = w.deleteTextChunksByFileUIDSync(ctx, kbUID, param.FileUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to delete text chunks from storage. Please try again.")
		return activityError(err, deleteTextChunksActivityError)
	}

	w.log.Info("DeleteTextChunksFromMinIOActivity: Deleted from MinIO",
		zap.Int("textChunkCount", len(textChunks)),
		zap.String("kbUID", kbUID.String()))

	// Delete records from DB
	err = w.repository.HardDeleteTextChunksByKBFileUID(ctx, param.FileUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to delete text chunk records. Please try again.")
		return activityError(err, deleteTextChunksActivityError)
	}

	return nil
}

// DeleteEmbeddingsFromVectorDBActivity deletes embeddings from vector db only (cleanup workflow)
// This is the parallel-optimized version - use with DeleteEmbeddingRecordsActivity
func (w *Worker) DeleteEmbeddingsFromVectorDBActivity(ctx context.Context, param *DeleteEmbeddingsFromVectorDBActivityParam) error {
	w.log.Info("DeleteEmbeddingsFromVectorDBActivity: Deleting embeddings from vector db",
		zap.String("fileUID", param.FileUID.String()))

	// Get embeddings to extract KB UID
	embeddings, err := w.repository.ListEmbeddingsByKBFileUID(ctx, param.FileUID)
	if err != nil || len(embeddings) == 0 {
		w.log.Info("DeleteEmbeddingsFromVectorDBActivity: No embeddings found, skipping",
			zap.String("fileUID", param.FileUID.String()))
		return nil
	}

	// Get KB UID from the first embedding (all embeddings have the same KB UID)
	kbUID := embeddings[0].KnowledgeBaseUID

	// CRITICAL: Query KB to get active_collection_uid (not kbUID) for the Milvus collection
	kb, err := w.repository.GetKnowledgeBaseByUID(ctx, kbUID)
	if err != nil {
		// If KB is already deleted, collection is likely already cleaned up
		if strings.Contains(err.Error(), "not found") {
			w.log.Info("DeleteEmbeddingsFromVectorDBActivity: KB not found (already cleaned up)",
				zap.String("kbUID", kbUID.String()))
			return nil
		}
		err = errorsx.AddMessage(err, "Unable to fetch knowledge base. Please try again.")
		return activityError(err, deleteEmbeddingsActivityError)
	}

	collection := constant.KBCollectionName(kb.ActiveCollectionUID)

	// Delete from vector db
	err = w.repository.DeleteEmbeddingsWithFileUID(ctx, collection, param.FileUID)
	if err != nil {
		// If collection doesn't exist, that's fine - it's already cleaned up
		if strings.Contains(err.Error(), "can't find collection") {
			w.log.Info("DeleteEmbeddingsFromVectorDBActivity: Collection not found (already cleaned up) in vector db",
				zap.String("collection", collection))
			return nil
		}
		err = errorsx.AddMessage(err, "Unable to delete embeddings from vector database. Please try again.")
		return activityError(err, deleteEmbeddingsActivityError)
	}

	w.log.Info("DeleteEmbeddingsFromVectorDBActivity: Deleted from vector db",
		zap.String("kbUID", kbUID.String()),
		zap.Int("embeddingCount", len(embeddings)))

	return nil
}

// DeleteEmbeddingRecordsActivity hard deletes embedding records from PostgreSQL (cleanup workflow)
// This is the parallel-optimized version for the cleanup workflow
func (w *Worker) DeleteEmbeddingRecordsActivity(ctx context.Context, param *DeleteEmbeddingsFromVectorDBActivityParam) error {
	w.log.Info("DeleteEmbeddingRecordsActivity: Deleting embedding records from DB",
		zap.String("fileUID", param.FileUID.String()))

	err := w.repository.HardDeleteEmbeddingsByKBFileUID(ctx, param.FileUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to delete embedding records. Please try again.")
		return activityError(err, deleteEmbeddingsActivityError)
	}

	return nil
}

// ===== KNOWLEDGE BASE CLEANUP ACTIVITIES =====

// DeleteKBFilesFromMinIOActivityParam defines parameters for deleting KB files from MinIO
type DeleteKBFilesFromMinIOActivityParam struct {
	KBUID types.KBUIDType // Knowledge base unique identifier
}

// DropVectorDBCollectionActivityParam defines parameters for dropping vector db collection
type DropVectorDBCollectionActivityParam struct {
	KBUID                  types.KBUIDType  // Knowledge base unique identifier
	ProtectedCollectionUID *types.KBUIDType // Collection UID that must not be dropped (e.g., after swap)
}

// DeleteKBFileRecordsActivityParam defines parameters for deleting KB file records
type DeleteKBFileRecordsActivityParam struct {
	KBUID types.KBUIDType // Knowledge base unique identifier
}

// DeleteKBConvertedFileRecordsActivityParam defines parameters for deleting KB converted file records
type DeleteKBConvertedFileRecordsActivityParam struct {
	KBUID types.KBUIDType // Knowledge base unique identifier
}

// DeleteKBTextChunkRecordsActivityParam defines parameters for deleting KB text chunk records
type DeleteKBTextChunkRecordsActivityParam struct {
	KBUID types.KBUIDType // Knowledge base unique identifier
}

// DeleteKBEmbeddingRecordsActivityParam defines parameters for deleting KB embedding records
type DeleteKBEmbeddingRecordsActivityParam struct {
	KBUID types.KBUIDType // Knowledge base unique identifier
}

// PurgeKBACLActivityParam defines parameters for purging KB ACL
type PurgeKBACLActivityParam struct {
	KBUID types.KBUIDType // Knowledge base unique identifier
}

// GetInProgressFileCountActivityParam defines parameters for checking in-progress file count
type GetInProgressFileCountActivityParam struct {
	KBUID types.KBUIDType // Knowledge base unique identifier
}

// DeleteKBFilesFromMinIOActivity deletes all files from MinIO for a knowledge base
func (w *Worker) DeleteKBFilesFromMinIOActivity(ctx context.Context, param *DeleteKBFilesFromMinIOActivityParam) error {
	w.log.Info("DeleteKBFilesFromMinIOActivity: Deleting files from MinIO",
		zap.String("kbUID", param.KBUID.String()))

	err := w.deleteKnowledgeBaseSync(ctx, param.KBUID.String())
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to delete knowledge base files from storage. Please try again.")
		return activityError(err, deleteKBFilesActivityError)
	}

	return nil
}

// DropVectorDBCollectionActivity drops the vector db collection for a knowledge base
// IMPORTANT: Must verify the collection is not in use by other KBs before dropping
// (e.g., race condition protection during concurrent operations, or edge cases where
// multiple KBs might temporarily reference the same collection during transitions)
func (w *Worker) DropVectorDBCollectionActivity(ctx context.Context, param *DropVectorDBCollectionActivityParam) error {
	w.log.Info("DropVectorDBCollectionActivity: Preparing to drop collection",
		zap.String("kbUID", param.KBUID.String()))

	// Get the KB to find its active collection
	// CRITICAL: Must include soft-deleted KBs because the KB may be soft-deleted before cleanup workflow runs
	kb, err := w.repository.GetKnowledgeBaseByUIDIncludingDeleted(ctx, param.KBUID)
	if err != nil {
		w.log.Warn("DropVectorDBCollectionActivity: KB not found in database (including soft-deleted)",
			zap.String("kbUID", param.KBUID.String()),
			zap.Error(err))
		return nil
	}

	// CRITICAL: active_collection_uid must ALWAYS be set and NEVER be nil after migration 000044
	// No fallback to param.KBUID - that would drop the wrong collection
	if kb.ActiveCollectionUID == uuid.Nil {
		w.log.Error("DropVectorDBCollectionActivity: KB has nil active_collection_uid, cannot drop collection",
			zap.String("kbUID", param.KBUID.String()))
		return activityErrorWithMessage(
			fmt.Sprintf("KB has nil active_collection_uid: %s.", param.KBUID),
			dropCollectionActivityError,
			fmt.Errorf("nil active_collection_uid for KB %s", param.KBUID),
		)
	}

	collectionUID := kb.ActiveCollectionUID

	// CRITICAL: Check if this collection is explicitly protected (e.g., just swapped to production)
	// This is deterministic and prevents race conditions with swap transaction commits
	if param.ProtectedCollectionUID != nil && collectionUID == *param.ProtectedCollectionUID {
		w.log.Info("DropVectorDBCollectionActivity: Collection is explicitly protected (recently swapped to production)",
			zap.String("collectionUID", collectionUID.String()))
		return nil
	}

	// Check if the collection is still in use by other KBs
	inUse, err := w.repository.IsCollectionInUse(ctx, collectionUID)
	if err != nil {
		w.log.Error("DropVectorDBCollectionActivity: Error checking collection usage",
			zap.String("collectionUID", collectionUID.String()),
			zap.Error(err))
		// Continue with drop attempt even if check fails
	}

	if inUse {
		w.log.Info("DropVectorDBCollectionActivity: Collection still in use by other KBs, preserving",
			zap.String("collectionUID", collectionUID.String()))
		return nil
	}

	// CRITICAL: Re-check IMMEDIATELY before dropping to prevent race condition
	// Between the first check and now, a swap might have completed that pointed production KB to this collection
	inUse, err = w.repository.IsCollectionInUse(ctx, collectionUID)
	if err != nil {
		w.log.Warn("DropVectorDBCollectionActivity: Error on final collection usage check, aborting drop to be safe",
			zap.String("collectionUID", collectionUID.String()),
			zap.Error(err))
		return nil // Abort drop if check fails - better safe than sorry
	}

	if inUse {
		w.log.Info("DropVectorDBCollectionActivity: Collection became in-use after initial check (race condition avoided)",
			zap.String("collectionUID", collectionUID.String()))
		return nil
	}

	// Collection not in use - safe to drop
	collection := constant.KBCollectionName(collectionUID)
	w.log.Info("DropVectorDBCollectionActivity: Dropping collection",
		zap.String("collection", collection),
		zap.String("collectionUID", collectionUID.String()))

	err = w.repository.DropCollection(ctx, collection)
	if err != nil {
		// If collection doesn't exist, that's fine - it's already cleaned up
		if strings.Contains(err.Error(), "can't find collection") {
			w.log.Info("DropVectorDBCollectionActivity: Collection not found (already dropped)",
				zap.String("collection", collection))
			return nil
		}
		err = errorsx.AddMessage(err, "Unable to drop vector database collection. Please try again.")
		return activityError(err, dropCollectionActivityError)
	}

	w.log.Info("DropVectorDBCollectionActivity: Collection dropped successfully",
		zap.String("collection", collection))

	return nil
}

// DeleteKBFileRecordsActivity deletes all file records for a knowledge base
func (w *Worker) DeleteKBFileRecordsActivity(ctx context.Context, param *DeleteKBFileRecordsActivityParam) error {
	w.log.Info("DeleteKBFileRecordsActivity: Deleting file records",
		zap.String("kbUID", param.KBUID.String()))

	err := w.repository.DeleteAllFiles(ctx, param.KBUID.String())
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to delete file records. Please try again.")
		return activityError(err, deleteKBFileRecordsActivityError)
	}

	return nil
}

// DeleteKBConvertedFileRecordsActivity deletes all converted file records for a knowledge base
func (w *Worker) DeleteKBConvertedFileRecordsActivity(ctx context.Context, param *DeleteKBConvertedFileRecordsActivityParam) error {
	w.log.Info("DeleteKBConvertedFileRecordsActivity: Deleting converted file records",
		zap.String("kbUID", param.KBUID.String()))

	err := w.repository.DeleteAllConvertedFilesInKb(ctx, param.KBUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to delete converted file records. Please try again.")
		return activityError(err, deleteKBConvertedRecordsActivityError)
	}

	return nil
}

// DeleteKBTextChunkRecordsActivity deletes all text chunk records for a knowledge base
func (w *Worker) DeleteKBTextChunkRecordsActivity(ctx context.Context, param *DeleteKBTextChunkRecordsActivityParam) error {
	w.log.Info("DeleteKBChunkRecordsActivity: Deleting text chunk records",
		zap.String("kbUID", param.KBUID.String()))

	err := w.repository.HardDeleteTextChunksByKBUID(ctx, param.KBUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to delete text chunk records. Please try again.")
		return activityError(err, deleteKBTextChunkRecordsActivityError)
	}

	return nil
}

// DeleteKBEmbeddingRecordsActivity deletes all embedding records for a knowledge base
func (w *Worker) DeleteKBEmbeddingRecordsActivity(ctx context.Context, param *DeleteKBEmbeddingRecordsActivityParam) error {
	w.log.Info("DeleteKBEmbeddingRecordsActivity: Deleting embedding records",
		zap.String("kbUID", param.KBUID.String()))

	err := w.repository.HardDeleteEmbeddingsByKBUID(ctx, param.KBUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to delete embedding records. Please try again.")
		return activityError(err, deleteKBEmbeddingRecordsActivityError)
	}

	return nil
}

// PurgeKBACLActivity purges ACL permissions for a knowledge base
func (w *Worker) PurgeKBACLActivity(ctx context.Context, param *PurgeKBACLActivityParam) error {
	w.log.Info("PurgeKBACLActivity: Purging ACL",
		zap.String("kbUID", param.KBUID.String()))

	err := w.aclClient.Purge(ctx, "knowledgebase", param.KBUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to purge access control. Please try again.")
		return activityError(err, purgeKBACLActivityError)
	}

	return nil
}

// SoftDeleteKBRecordActivityParam defines parameters for soft-deleting KB record
type SoftDeleteKBRecordActivityParam struct {
	KBUID types.KBUIDType // KB unique identifier
}

// SoftDeleteKBRecordActivity soft-deletes the knowledge base record itself
func (w *Worker) SoftDeleteKBRecordActivity(ctx context.Context, param *SoftDeleteKBRecordActivityParam) error {
	w.log.Info("SoftDeleteKBRecordActivity: Soft-deleting KB record",
		zap.String("kbUID", param.KBUID.String()))

	// Get KB to retrieve owner and ID
	kb, err := w.repository.GetKnowledgeBaseByUID(ctx, param.KBUID)
	if err != nil {
		w.log.Info("SoftDeleteKBRecordActivity: KB not found, may have been deleted already",
			zap.String("kbUID", param.KBUID.String()))
		return nil
	}

	// CRITICAL: Clear update_status and update_workflow_id BEFORE soft-deletion
	// This prevents rollback KBs from blocking future operations with stale update status
	// Same fix as CleanupOldKnowledgeBaseActivity (for staging KBs)
	if kb.UpdateStatus != "" || kb.UpdateWorkflowID != "" {
		w.log.Info("SoftDeleteKBRecordActivity: Clearing update status before soft-deletion",
			zap.String("kbUID", param.KBUID.String()),
			zap.String("previousUpdateStatus", kb.UpdateStatus))

		err = w.repository.UpdateKnowledgeBaseWithMap(ctx, kb.ID, kb.NamespaceUID, map[string]interface{}{
			"update_status":      "",
			"update_workflow_id": "",
		})
		if err != nil {
			err = errorsx.AddMessage(err, "Unable to clear update status before soft-deletion. Please try again.")
			return activityErrorWithMessage("Failed to clear update status before soft-deletion", softDeleteKBRecordActivityError, err)
		}
	}

	// Soft delete the KB record
	_, err = w.repository.DeleteKnowledgeBase(ctx, kb.NamespaceUID, kb.ID)
	if err != nil {
		// Check if KB was already deleted (record not found means it was already soft-deleted)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			w.log.Info("SoftDeleteKBRecordActivity: KB already soft-deleted",
				zap.String("kbUID", param.KBUID.String()),
				zap.String("kbID", kb.ID))
			return nil
		}

		err = errorsx.AddMessage(err, "Unable to soft-delete knowledge base record. Please try again.")
		return activityError(err, softDeleteKBRecordActivityError)
	}

	w.log.Info("SoftDeleteKBRecordActivity: KB record soft-deleted successfully",
		zap.String("kbUID", param.KBUID.String()),
		zap.String("kbID", kb.ID))

	return nil
}

// GetInProgressFileCountActivity returns the count of files currently being processed for a KB
// This is used by CleanupKnowledgeBaseWorkflow to ensure no files are being processed
// before dropping the Milvus collection, preventing "collection does not exist" errors
//
// CRITICAL: This checks BOTH database file status AND active Temporal workflows
// to prevent race conditions where:
// 1. File status is optimistically updated to COMPLETED in DB
// 2. But embedding activities are still running/retrying
// 3. Cleanup drops collection while embedding flush activity runs
func (w *Worker) GetInProgressFileCountActivity(ctx context.Context, param *GetInProgressFileCountActivityParam) (int64, error) {
	w.log.Info("GetInProgressFileCountActivity: Checking for in-progress files and workflows",
		zap.String("kbUID", param.KBUID.String()))

	// STEP 1: Count files in active processing states (PROCESSING, CHUNKING, EMBEDDING)
	// Do NOT count NOTSTARTED files as they haven't begun processing yet
	//
	// CRITICAL: We must count soft-deleted files too!
	// When KB is deleted, files are CASCADE soft-deleted, but their workflows may still be running.
	// If we only count non-deleted files, cleanup will drop the collection while workflows are still saving embeddings.
	inProgressStatuses := "FILE_PROCESS_STATUS_PROCESSING,FILE_PROCESS_STATUS_CHUNKING,FILE_PROCESS_STATUS_EMBEDDING"

	dbCount, err := w.repository.GetFileCountByKnowledgeBaseUIDIncludingDeleted(ctx, param.KBUID, inProgressStatuses)
	if err != nil {
		w.log.Error("GetInProgressFileCountActivity: Failed to get DB file count",
			zap.String("kbUID", param.KBUID.String()),
			zap.Error(err))
		return 0, activityError(err, getInProgressFileCountActivityError)
	}

	// STEP 2: Check for active Temporal workflows related to this KB
	// This catches ProcessFileWorkflow that may still be running
	// even after the parent workflow updated the file status to COMPLETED
	activeWorkflowCount := w.getActiveWorkflowCount(ctx, param.KBUID)

	totalCount := dbCount + activeWorkflowCount

	w.log.Info("GetInProgressFileCountActivity: Activity count",
		zap.String("kbUID", param.KBUID.String()),
		zap.Int64("dbFileCount", dbCount),
		zap.Int64("activeWorkflowCount", activeWorkflowCount),
		zap.Int64("totalCount", totalCount))

	return totalCount, nil
}

// getActiveWorkflowCount queries Temporal for running workflows related to this KB
// Returns the count of ProcessFileWorkflow instances
func (w *Worker) getActiveWorkflowCount(ctx context.Context, kbUID types.KBUIDType) int64 {
	// Get the KB first to retrieve owner information
	kb, err := w.repository.GetKnowledgeBaseByUID(ctx, kbUID)
	if err != nil {
		w.log.Warn("Failed to get KB for workflow check, assuming 0 active workflows",
			zap.String("kbUID", kbUID.String()),
			zap.Error(err))
		return 0
	}

	// Get all files for this KB (including COMPLETED ones, since their workflows might still be running)
	files, err := w.repository.ListFiles(ctx, repository.KnowledgeBaseFileListParams{
		OwnerUID: kb.NamespaceUID,
		KBUID:    kbUID.String(),
	})
	if err != nil {
		w.log.Warn("Failed to list files for workflow check, assuming 0 active workflows",
			zap.String("kbUID", kbUID.String()),
			zap.Error(err))
		return 0
	}

	if len(files.Files) == 0 {
		// No files, so no workflows
		return 0
	}

	activeCount := int64(0)

	// Check for each file's ProcessFileWorkflow
	// Embedding storage is now done inline in ProcessFileWorkflow (no separate child workflow)
	for _, file := range files.Files {
		// ProcessFileWorkflow ID format: "process-file-{fileUID}"
		processFileWorkflowID := fmt.Sprintf("process-file-%s", file.UID.String())
		if w.isWorkflowRunning(ctx, processFileWorkflowID) {
			w.log.Info("Found active ProcessFileWorkflow",
				zap.String("workflowID", processFileWorkflowID),
				zap.String("fileUID", file.UID.String()))
			activeCount++
		}
	}

	return activeCount
}

// isWorkflowRunning checks if a workflow with the given ID is currently running
func (w *Worker) isWorkflowRunning(ctx context.Context, workflowID string) bool {
	// Use DescribeWorkflowExecution to check workflow status
	// If workflow doesn't exist or is completed, this will return an error
	resp, err := w.temporalClient.DescribeWorkflowExecution(ctx, workflowID, "")
	if err != nil {
		// Workflow doesn't exist or error occurred - assume not running
		return false
	}

	// Check if workflow is still running
	if resp.WorkflowExecutionInfo.Status == 1 { // 1 = WORKFLOW_EXECUTION_STATUS_RUNNING
		return true
	}

	return false
}

// GetKBCollectionUIDActivityParam defines parameters for getting KB collection UID
type GetKBCollectionUIDActivityParam struct {
	KBUID types.KBUIDType
}

// GetKBCollectionUIDActivity retrieves the active_collection_uid for a KB
// This is used by CleanupKnowledgeBaseWorkflow for verification
func (w *Worker) GetKBCollectionUIDActivity(ctx context.Context, param *GetKBCollectionUIDActivityParam) (types.KBUIDType, error) {
	w.log.Info("GetKBCollectionUIDActivity: Getting collection UID",
		zap.String("kbUID", param.KBUID.String()))

	// Get KB (including deleted ones for rollback KB verification)
	kb, err := w.repository.GetKnowledgeBaseByUIDIncludingDeleted(ctx, param.KBUID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			w.log.Info("GetKBCollectionUIDActivity: KB not found",
				zap.String("kbUID", param.KBUID.String()))
			return types.KBUIDType{}, nil
		}
		return types.KBUIDType{}, activityErrorWithMessage("Failed to get KB for collection UID retrieval", getKBCollectionUIDActivityError, err)
	}

	w.log.Info("GetKBCollectionUIDActivity: Collection UID retrieved",
		zap.String("kbUID", param.KBUID.String()),
		zap.String("collectionUID", kb.ActiveCollectionUID.String()))

	return kb.ActiveCollectionUID, nil
}

// ClearProductionKBRetentionActivityParam defines parameters for clearing production KB retention
type ClearProductionKBRetentionActivityParam struct {
	RollbackKBUID types.KBUIDType
}

// ClearProductionKBRetentionActivity clears the rollback_retention_until field on the production KB
// This is called after the rollback KB cleanup completes (either via automatic retention expiry or manual purge)
func (w *Worker) ClearProductionKBRetentionActivity(ctx context.Context, param *ClearProductionKBRetentionActivityParam) error {
	w.log.Info("ClearProductionKBRetentionActivity: Clearing retention field",
		zap.String("rollbackKBUID", param.RollbackKBUID.String()))

	// Get the rollback KB to find its ID
	// Use IncludingDeleted to query soft-deleted records since the rollback KB is already soft-deleted
	rollbackKB, err := w.repository.GetKnowledgeBaseByUIDIncludingDeleted(ctx, param.RollbackKBUID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Rollback KB already deleted - this is expected
			w.log.Info("ClearProductionKBRetentionActivity: Rollback KB not found (already deleted)",
				zap.String("rollbackKBUID", param.RollbackKBUID.String()))
			return nil
		}
		w.log.Error("ClearProductionKBRetentionActivity: Failed to get rollback KB",
			zap.String("rollbackKBUID", param.RollbackKBUID.String()),
			zap.Error(err))
		// Non-fatal - don't fail the workflow
		return nil
	}

	// Check if this is a rollback KB (has parent_kb_uid set)
	if rollbackKB.ParentKBUID == nil {
		w.log.Info("ClearProductionKBRetentionActivity: Not a rollback KB (no parent_kb_uid), skipping",
			zap.String("kbID", rollbackKB.ID))
		return nil
	}

	// Get production KB using parent_kb_uid
	productionKB, err := w.repository.GetKnowledgeBaseByUID(ctx, *rollbackKB.ParentKBUID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			w.log.Info("ClearProductionKBRetentionActivity: Production KB not found",
				zap.String("productionKBUID", rollbackKB.ParentKBUID.String()))
			return nil
		}
		w.log.Error("ClearProductionKBRetentionActivity: Failed to get production KB",
			zap.String("productionKBUID", rollbackKB.ParentKBUID.String()),
			zap.Error(err))
		// Non-fatal - don't fail the workflow
		return nil
	}

	// Clear the retention field
	err = w.repository.UpdateKnowledgeBaseWithMap(ctx, productionKB.ID, productionKB.NamespaceUID, map[string]any{
		"rollback_retention_until": nil,
	})
	if err != nil {
		w.log.Error("ClearProductionKBRetentionActivity: Failed to clear retention field",
			zap.String("productionID", productionKB.ID),
			zap.Error(err))
		// Non-fatal - don't fail the workflow
		return nil
	}

	w.log.Info("ClearProductionKBRetentionActivity: Cleared retention field successfully",
		zap.String("productionID", productionKB.ID),
		zap.String("rollbackID", rollbackKB.ID))

	return nil
}

// CheckRollbackRetentionExpiredActivityParam defines parameters for checking retention expiry
type CheckRollbackRetentionExpiredActivityParam struct {
	RollbackKBUID types.KBUIDType
}

// CheckRollbackRetentionExpiredActivityResult contains the result of retention check
type CheckRollbackRetentionExpiredActivityResult struct {
	// Expired is true if the retention period has expired and cleanup should proceed
	Expired bool
	// KBDeleted is true if the rollback KB has been deleted (should proceed with cleanup)
	KBDeleted bool
	// ProductionKBDeleted is true if the production KB no longer exists (should abort cleanup)
	ProductionKBDeleted bool
}

// CheckRollbackRetentionExpiredActivity checks if the rollback retention period has expired
// by querying the rollback KB's own rollback_retention_until field.
// This activity is used by CleanupKnowledgeBaseWorkflow to poll for retention expiry
// instead of doing one long sleep, allowing dynamic retention changes via SetRollbackRetentionAdmin.
// Note: SetRollbackRetentionAdmin updates this field directly on the rollback KB.
func (w *Worker) CheckRollbackRetentionExpiredActivity(ctx context.Context, param *CheckRollbackRetentionExpiredActivityParam) (*CheckRollbackRetentionExpiredActivityResult, error) {
	w.log.Debug("CheckRollbackRetentionExpiredActivity: Checking retention",
		zap.String("rollbackKBUID", param.RollbackKBUID.String()))

	result := &CheckRollbackRetentionExpiredActivityResult{}

	// Get the rollback KB to find its parent (production KB)
	// Use IncludingDeleted since we need to handle soft-deleted KBs
	rollbackKB, err := w.repository.GetKnowledgeBaseByUIDIncludingDeleted(ctx, param.RollbackKBUID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Rollback KB already hard-deleted - cleanup not needed
			w.log.Info("CheckRollbackRetentionExpiredActivity: Rollback KB not found (hard deleted)",
				zap.String("rollbackKBUID", param.RollbackKBUID.String()))
			result.KBDeleted = true
			result.Expired = true // Signal to proceed with cleanup termination
			return result, nil
		}
		return nil, activityErrorWithMessage("Failed to get rollback KB", checkRollbackRetentionExpiredActivityError, err)
	}

	// Check if this is actually a rollback KB (has parent_kb_uid set)
	if rollbackKB.ParentKBUID == nil {
		// Not a rollback KB - should not have reached this point, but proceed with cleanup
		w.log.Warn("CheckRollbackRetentionExpiredActivity: KB has no parent (not a rollback KB), proceeding",
			zap.String("kbUID", param.RollbackKBUID.String()))
		result.Expired = true
		return result, nil
	}

	// Check if retention has expired using the ROLLBACK KB's own RollbackRetentionUntil field
	// SetRollbackRetentionAdmin updates this field directly on the rollback KB, so we should check it here
	// rather than looking at the production KB
	if rollbackKB.RollbackRetentionUntil == nil {
		// No retention set - proceed with cleanup
		w.log.Info("CheckRollbackRetentionExpiredActivity: No retention set on rollback KB, proceeding",
			zap.String("rollbackKBUID", param.RollbackKBUID.String()))
		result.Expired = true
		return result, nil
	}

	now := time.Now()
	if now.After(*rollbackKB.RollbackRetentionUntil) {
		w.log.Info("CheckRollbackRetentionExpiredActivity: Retention expired, proceeding",
			zap.String("rollbackKBUID", param.RollbackKBUID.String()),
			zap.Time("retentionUntil", *rollbackKB.RollbackRetentionUntil),
			zap.Time("now", now))
		result.Expired = true
		return result, nil
	}

	w.log.Debug("CheckRollbackRetentionExpiredActivity: Retention not yet expired",
		zap.String("rollbackKBUID", param.RollbackKBUID.String()),
		zap.Time("retentionUntil", *rollbackKB.RollbackRetentionUntil),
		zap.Time("now", now),
		zap.Duration("remaining", rollbackKB.RollbackRetentionUntil.Sub(now)))

	result.Expired = false
	return result, nil
}

// CleanupExpiredGCSFilesActivityParam defines parameters for cleaning up expired GCS files
type CleanupExpiredGCSFilesActivityParam struct {
	MaxFilesToClean int64 // Maximum number of files to clean up in one batch
}

// CleanupExpiredGCSFilesActivity scans Redis for expired GCS files and deletes them
// This activity should be run periodically (e.g., every 1-2 minutes) to clean up
// on-demand GCS files that have exceeded their 10-minute TTL
func (w *Worker) CleanupExpiredGCSFilesActivity(ctx context.Context, param *CleanupExpiredGCSFilesActivityParam) error {
	w.log.Info("CleanupExpiredGCSFilesActivity: Scanning for expired GCS files",
		zap.Int64("maxFiles", param.MaxFilesToClean))

	// Scan Redis for GCS files that are expiring soon (within 1 minute)
	gcsFiles, err := w.repository.ScanGCSFilesForCleanup(ctx, param.MaxFilesToClean)
	if err != nil {
		w.log.Error("CleanupExpiredGCSFilesActivity: Failed to scan Redis for GCS files",
			zap.Error(err))
		return activityErrorWithMessage("Failed to scan Redis for expired GCS files", cleanupExpiredGCSFilesActivityError, err)
	}

	if len(gcsFiles) == 0 {
		w.log.Debug("CleanupExpiredGCSFilesActivity: No expired GCS files found")
		return nil
	}

	w.log.Info("CleanupExpiredGCSFilesActivity: Found GCS files to clean up",
		zap.Int("count", len(gcsFiles)))

	// Delete each file from GCS and remove from Redis cache
	successCount := 0
	errorCount := 0

	for _, gcsFile := range gcsFiles {
		kbUID := types.KBUIDType(uuid.FromStringOrNil(gcsFile.KBUIDStr))
		fileUID := types.FileUIDType(uuid.FromStringOrNil(gcsFile.FileUIDStr))

		w.log.Debug("CleanupExpiredGCSFilesActivity: Deleting GCS file",
			zap.String("bucket", gcsFile.GCSBucket),
			zap.String("path", gcsFile.GCSObjectPath),
			zap.String("view", gcsFile.View),
			zap.Time("uploadTime", gcsFile.UploadTime),
			zap.Time("expiresAt", gcsFile.ExpiresAt))

		// Delete file from GCS
		err := w.repository.GetMinIOStorage().DeleteFile(ctx, gcsFile.GCSBucket, gcsFile.GCSObjectPath)
		if err != nil {
			w.log.Warn("CleanupExpiredGCSFilesActivity: Failed to delete GCS file (may not exist)",
				zap.String("path", gcsFile.GCSObjectPath),
				zap.Error(err))
			errorCount++
			// Continue with next file - don't fail the entire activity
		} else {
			w.log.Debug("CleanupExpiredGCSFilesActivity: Successfully deleted GCS file",
				zap.String("path", gcsFile.GCSObjectPath))
			successCount++
		}

		// Remove from Redis cache (always attempt, even if GCS deletion failed)
		err = w.repository.DeleteGCSFileCache(ctx, kbUID, fileUID, gcsFile.View)
		if err != nil {
			w.log.Warn("CleanupExpiredGCSFilesActivity: Failed to delete Redis cache entry",
				zap.String("kbUID", kbUID.String()),
				zap.String("fileUID", fileUID.String()),
				zap.String("view", gcsFile.View),
				zap.Error(err))
			// Non-fatal - Redis will auto-expire the key anyway
		}
	}

	w.log.Info("CleanupExpiredGCSFilesActivity: Cleanup completed",
		zap.Int("total", len(gcsFiles)),
		zap.Int("success", successCount),
		zap.Int("errors", errorCount))

	// Don't fail the activity if some files couldn't be deleted
	// These files will be retried in the next cleanup cycle
	return nil
}
