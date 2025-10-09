package worker

import (
	"context"
	"strings"

	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/constant"
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
	files, err := w.repository.GetKnowledgeBaseFilesByFileUIDs(ctx, []types.FileUIDType{param.FileUID})
	if err != nil || len(files) == 0 {
		w.log.Info("DeleteOriginalFileActivity: File not found, may have been deleted already",
			zap.String("fileUID", param.FileUID.String()))
		return nil
	}

	file := files[0]
	if file.Destination == "" {
		w.log.Info("DeleteOriginalFileActivity: File has no destination, skipping",
			zap.String("fileUID", param.FileUID.String()))
		return nil
	}

	err = w.deleteFilesSync(ctx, param.Bucket, []string{file.Destination})
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to delete file from storage. Please try again.")
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			deleteOriginalFileActivityError,
			err,
		)
	}

	return nil
}

// DeleteConvertedFileActivity deletes converted file from MinIO and DB
func (w *Worker) DeleteConvertedFileActivity(ctx context.Context, param *DeleteConvertedFileActivityParam) error {
	w.log.Info("DeleteConvertedFileActivity: Deleting converted file",
		zap.String("fileUID", param.FileUID.String()))

	// Check if converted file exists and get KB UID from it
	convertedFile, err := w.repository.GetConvertedFileByFileUID(ctx, param.FileUID)
	if err != nil {
		w.log.Info("DeleteConvertedFileActivity: No converted file found, skipping",
			zap.String("fileUID", param.FileUID.String()))
		return nil
	}

	// Delete from MinIO using KB UID from the converted file record
	err = w.deleteConvertedFileByFileUIDSync(ctx, convertedFile.KBUID, param.FileUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to delete converted file from storage. Please try again.")
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			deleteConvertedFileActivityError,
			err,
		)
	}

	w.log.Info("DeleteConvertedFileActivity: Deleted from MinIO",
		zap.String("convertedFileUID", convertedFile.UID.String()),
		zap.String("kbUID", convertedFile.KBUID.String()))

	// Delete record from DB
	err = w.repository.HardDeleteConvertedFileByFileUID(ctx, param.FileUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to delete converted file record. Please try again.")
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			deleteConvertedFileActivityError,
			err,
		)
	}

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
	kbUID := textChunks[0].KBUID

	// Delete from MinIO using KB UID from the text chunk records
	err = w.deleteTextChunksByFileUIDSync(ctx, kbUID, param.FileUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to delete text chunks from storage. Please try again.")
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			deleteTextChunksActivityError,
			err,
		)
	}

	w.log.Info("DeleteTextChunksFromMinIOActivity: Deleted from MinIO",
		zap.Int("textChunkCount", len(textChunks)),
		zap.String("kbUID", kbUID.String()))

	// Delete records from DB
	err = w.repository.HardDeleteTextChunksByKBFileUID(ctx, param.FileUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to delete text chunk records. Please try again.")
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			deleteTextChunksActivityError,
			err,
		)
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
	kbUID := embeddings[0].KBUID
	collection := constant.KBCollectionName(kbUID)

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
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			deleteEmbeddingsActivityError,
			err,
		)
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
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			deleteEmbeddingsActivityError,
			err,
		)
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
	KBUID types.KBUIDType // Knowledge base unique identifier
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

// DeleteKBFilesFromMinIOActivity deletes all files from MinIO for a knowledge base
func (w *Worker) DeleteKBFilesFromMinIOActivity(ctx context.Context, param *DeleteKBFilesFromMinIOActivityParam) error {
	w.log.Info("DeleteKBFilesFromMinIOActivity: Deleting files from MinIO",
		zap.String("kbUID", param.KBUID.String()))

	err := w.deleteKnowledgeBaseSync(ctx, param.KBUID.String())
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to delete catalog files from storage. Please try again.")
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			deleteKBFilesActivityError,
			err,
		)
	}

	return nil
}

// DropVectorDBCollectionActivity drops the vector db collection for a knowledge base
func (w *Worker) DropVectorDBCollectionActivity(ctx context.Context, param *DropVectorDBCollectionActivityParam) error {
	w.log.Info("DropVectorDBCollectionActivity: Dropping collection",
		zap.String("kbUID", param.KBUID.String()))

	collection := constant.KBCollectionName(param.KBUID)
	err := w.repository.DropCollection(ctx, collection)
	if err != nil {
		// If collection doesn't exist, that's fine - it's already cleaned up
		if strings.Contains(err.Error(), "can't find collection") {
			w.log.Info("DropVectorDBCollectionActivity: Collection not found (already dropped)",
				zap.String("collection", collection))
			return nil
		}
		err = errorsx.AddMessage(err, "Unable to drop vector database collection. Please try again.")
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			dropCollectionActivityError,
			err,
		)
	}

	return nil
}

// DeleteKBFileRecordsActivity deletes all file records for a knowledge base
func (w *Worker) DeleteKBFileRecordsActivity(ctx context.Context, param *DeleteKBFileRecordsActivityParam) error {
	w.log.Info("DeleteKBFileRecordsActivity: Deleting file records",
		zap.String("kbUID", param.KBUID.String()))

	err := w.repository.DeleteAllKnowledgeBaseFiles(ctx, param.KBUID.String())
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to delete file records. Please try again.")
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			deleteKBFileRecordsActivityError,
			err,
		)
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
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			deleteKBConvertedRecordsActivityError,
			err,
		)
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
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			deleteKBTextChunkRecordsActivityError,
			err,
		)
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
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			deleteKBEmbeddingRecordsActivityError,
			err,
		)
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
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			purgeKBACLActivityError,
			err,
		)
	}

	return nil
}

// Activity error type constants
const (
	deleteOriginalFileActivityError       = "DeleteOriginalFileActivity"
	deleteConvertedFileActivityError      = "DeleteConvertedFileActivity"
	deleteTextChunksActivityError         = "DeleteTextChunksFromMinIOActivity"
	deleteEmbeddingsActivityError         = "DeleteEmbeddingsFromVectorDBActivity"
	deleteKBFilesActivityError            = "DeleteKBFilesFromMinIOActivity"
	dropCollectionActivityError           = "DropVectorDBCollectionActivity"
	deleteKBFileRecordsActivityError      = "DeleteKBFileRecordsActivity"
	deleteKBConvertedRecordsActivityError = "DeleteKBConvertedFileRecordsActivity"
	deleteKBTextChunkRecordsActivityError = "DeleteKBTextChunkRecordsActivity"
	deleteKBEmbeddingRecordsActivityError = "DeleteKBEmbeddingRecordsActivity"
	purgeKBACLActivityError               = "PurgeKBACLActivity"
)
