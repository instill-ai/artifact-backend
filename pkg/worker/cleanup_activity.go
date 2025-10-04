package worker

import (
	"context"
	"fmt"
	"strings"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/service"
)

// DeleteOriginalFileActivityParam defines parameters for deleting original file
type DeleteOriginalFileActivityParam struct {
	FileUID uuid.UUID
	Bucket  string
}

// DeleteConvertedFileActivityParam defines parameters for deleting converted file
type DeleteConvertedFileActivityParam struct {
	FileUID uuid.UUID
}

// DeleteChunksFromMinIOActivityParam defines parameters for deleting chunks from MinIO
type DeleteChunksFromMinIOActivityParam struct {
	FileUID uuid.UUID
}

// DeleteEmbeddingsFromVectorDBActivityParam defines parameters for deleting embeddings from Milvus
type DeleteEmbeddingsFromVectorDBActivityParam struct {
	FileUID uuid.UUID
}

// DeleteFileRecordsActivityParam defines parameters for deleting file records from DB
type DeleteFileRecordsActivityParam struct {
	FileUID uuid.UUID
}

// DeleteOriginalFileActivity deletes the original uploaded file from MinIO
func (w *Worker) DeleteOriginalFileActivity(ctx context.Context, param *DeleteOriginalFileActivityParam) error {
	w.log.Info("DeleteOriginalFileActivity: Deleting original file",
		zap.String("fileUID", param.FileUID.String()))

	// Fetch file record to get destination
	files, err := w.repository.GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{param.FileUID})
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

	err = w.service.DeleteFiles(ctx, param.Bucket, []string{file.Destination})
	if err != nil {
		return fmt.Errorf("failed to delete original file: %w", err)
	}

	w.log.Info("DeleteOriginalFileActivity: Successfully deleted original file",
		zap.String("destination", file.Destination))
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
	err = w.service.DeleteConvertedFileByFileUID(ctx, convertedFile.KbUID, param.FileUID)
	if err != nil {
		return fmt.Errorf("failed to delete converted file from MinIO: %w", err)
	}

	w.log.Info("DeleteConvertedFileActivity: Deleted from MinIO",
		zap.String("convertedFileUID", convertedFile.UID.String()),
		zap.String("kbUID", convertedFile.KbUID.String()))

	// Delete record from DB
	err = w.repository.HardDeleteConvertedFileByFileUID(ctx, param.FileUID)
	if err != nil {
		return fmt.Errorf("failed to delete converted file record: %w", err)
	}

	w.log.Info("DeleteConvertedFileActivity: Successfully deleted converted file and record")
	return nil
}

// DeleteChunksFromMinIOActivity deletes chunks from MinIO and DB
func (w *Worker) DeleteChunksFromMinIOActivity(ctx context.Context, param *DeleteChunksFromMinIOActivityParam) error {
	w.log.Info("DeleteChunksFromMinIOActivity: Deleting chunks",
		zap.String("fileUID", param.FileUID.String()))

	// Check if chunks exist and get KB UID from them
	chunks, err := w.repository.ListChunksByKbFileUID(ctx, param.FileUID)
	if err != nil || len(chunks) == 0 {
		w.log.Info("DeleteChunksFromMinIOActivity: No chunks found, skipping",
			zap.String("fileUID", param.FileUID.String()))
		return nil
	}

	// Get KB UID from the first chunk (all chunks have the same KB UID)
	kbUID := chunks[0].KbUID

	// Delete from MinIO using KB UID from the chunk records
	err = w.service.DeleteTextChunksByFileUID(ctx, kbUID, param.FileUID)
	if err != nil {
		return fmt.Errorf("failed to delete chunks from MinIO: %w", err)
	}

	w.log.Info("DeleteChunksFromMinIOActivity: Deleted from MinIO",
		zap.Int("chunkCount", len(chunks)),
		zap.String("kbUID", kbUID.String()))

	// Delete records from DB
	err = w.repository.HardDeleteChunksByKbFileUID(ctx, param.FileUID)
	if err != nil {
		return fmt.Errorf("failed to delete chunk records: %w", err)
	}

	w.log.Info("DeleteChunksFromMinIOActivity: Successfully deleted chunks and records",
		zap.Int("chunkCount", len(chunks)))
	return nil
}

// DeleteEmbeddingsFromVectorDBActivity deletes embeddings from Milvus and DB
func (w *Worker) DeleteEmbeddingsFromVectorDBActivity(ctx context.Context, param *DeleteEmbeddingsFromVectorDBActivityParam) error {
	w.log.Info("DeleteEmbeddingsFromVectorDBActivity: Deleting embeddings",
		zap.String("fileUID", param.FileUID.String()))

	// Get embeddings to extract KB UID
	embeddings, err := w.repository.ListEmbeddingsByKbFileUID(ctx, param.FileUID)
	if err != nil || len(embeddings) == 0 {
		w.log.Info("DeleteEmbeddingsFromVectorDBActivity: No embeddings found, skipping",
			zap.String("fileUID", param.FileUID.String()))
		return nil
	}

	// Get KB UID from the first embedding (all embeddings have the same KB UID)
	kbUID := embeddings[0].KbUID
	collection := service.KBCollectionName(kbUID)

	// Delete from Milvus
	err = w.service.VectorDB().DeleteEmbeddingsWithFileUID(ctx, collection, param.FileUID)
	if err != nil {
		// If collection doesn't exist, that's fine - it's already cleaned up
		if strings.Contains(err.Error(), "can't find collection") {
			w.log.Info("DeleteEmbeddingsFromVectorDBActivity: Collection not found (already cleaned up)",
				zap.String("collection", collection))
		} else {
			return fmt.Errorf("failed to delete embeddings from Milvus: %w", err)
		}
	} else {
		w.log.Info("DeleteEmbeddingsFromVectorDBActivity: Deleted from Milvus",
			zap.String("kbUID", kbUID.String()),
			zap.Int("embeddingCount", len(embeddings)))
	}

	// Delete records from DB
	err = w.repository.HardDeleteEmbeddingsByKbFileUID(ctx, param.FileUID)
	if err != nil {
		return fmt.Errorf("failed to delete embedding records: %w", err)
	}

	w.log.Info("DeleteEmbeddingsFromVectorDBActivity: Successfully deleted embeddings and records")
	return nil
}

// ===== KNOWLEDGE BASE CLEANUP ACTIVITIES =====

// DeleteKBFilesFromMinIOActivityParam defines parameters for deleting KB files from MinIO
type DeleteKBFilesFromMinIOActivityParam struct {
	KnowledgeBaseUID uuid.UUID
}

// DropVectorDBCollectionActivityParam defines parameters for dropping Milvus collection
type DropVectorDBCollectionActivityParam struct {
	KnowledgeBaseUID uuid.UUID
}

// DeleteKBFileRecordsActivityParam defines parameters for deleting KB file records
type DeleteKBFileRecordsActivityParam struct {
	KnowledgeBaseUID uuid.UUID
}

// DeleteKBConvertedFileRecordsActivityParam defines parameters for deleting KB converted file records
type DeleteKBConvertedFileRecordsActivityParam struct {
	KnowledgeBaseUID uuid.UUID
}

// DeleteKBChunkRecordsActivityParam defines parameters for deleting KB chunk records
type DeleteKBChunkRecordsActivityParam struct {
	KnowledgeBaseUID uuid.UUID
}

// DeleteKBEmbeddingRecordsActivityParam defines parameters for deleting KB embedding records
type DeleteKBEmbeddingRecordsActivityParam struct {
	KnowledgeBaseUID uuid.UUID
}

// PurgeKBACLActivityParam defines parameters for purging KB ACL
type PurgeKBACLActivityParam struct {
	KnowledgeBaseUID uuid.UUID
}

// DeleteKBFilesFromMinIOActivity deletes all files from MinIO for a knowledge base
func (w *Worker) DeleteKBFilesFromMinIOActivity(ctx context.Context, param *DeleteKBFilesFromMinIOActivityParam) error {
	w.log.Info("DeleteKBFilesFromMinIOActivity: Deleting files from MinIO",
		zap.String("kbUID", param.KnowledgeBaseUID.String()))

	err := w.service.DeleteKnowledgeBase(ctx, param.KnowledgeBaseUID.String())
	if err != nil {
		return fmt.Errorf("failed to delete files from MinIO: %w", err)
	}

	w.log.Info("DeleteKBFilesFromMinIOActivity: Successfully deleted files from MinIO")
	return nil
}

// DropVectorDBCollectionActivity drops the Milvus collection for a knowledge base
func (w *Worker) DropVectorDBCollectionActivity(ctx context.Context, param *DropVectorDBCollectionActivityParam) error {
	w.log.Info("DropVectorDBCollectionActivity: Dropping collection",
		zap.String("kbUID", param.KnowledgeBaseUID.String()))

	collection := service.KBCollectionName(param.KnowledgeBaseUID)
	err := w.service.VectorDB().DropCollection(ctx, collection)
	if err != nil {
		// If collection doesn't exist, that's fine - it's already cleaned up
		if strings.Contains(err.Error(), "can't find collection") {
			w.log.Info("DropVectorDBCollectionActivity: Collection not found (already dropped)",
				zap.String("collection", collection))
			return nil
		}
		return fmt.Errorf("failed to drop collection: %w", err)
	}

	w.log.Info("DropVectorDBCollectionActivity: Successfully dropped collection")
	return nil
}

// DeleteKBFileRecordsActivity deletes all file records for a knowledge base
func (w *Worker) DeleteKBFileRecordsActivity(ctx context.Context, param *DeleteKBFileRecordsActivityParam) error {
	w.log.Info("DeleteKBFileRecordsActivity: Deleting file records",
		zap.String("kbUID", param.KnowledgeBaseUID.String()))

	err := w.repository.DeleteAllKnowledgeBaseFiles(ctx, param.KnowledgeBaseUID.String())
	if err != nil {
		return fmt.Errorf("failed to delete file records: %w", err)
	}

	w.log.Info("DeleteKBFileRecordsActivity: Successfully deleted file records")
	return nil
}

// DeleteKBConvertedFileRecordsActivity deletes all converted file records for a knowledge base
func (w *Worker) DeleteKBConvertedFileRecordsActivity(ctx context.Context, param *DeleteKBConvertedFileRecordsActivityParam) error {
	w.log.Info("DeleteKBConvertedFileRecordsActivity: Deleting converted file records",
		zap.String("kbUID", param.KnowledgeBaseUID.String()))

	err := w.repository.DeleteAllConvertedFilesInKb(ctx, param.KnowledgeBaseUID)
	if err != nil {
		return fmt.Errorf("failed to delete converted file records: %w", err)
	}

	w.log.Info("DeleteKBConvertedFileRecordsActivity: Successfully deleted converted file records")
	return nil
}

// DeleteKBChunkRecordsActivity deletes all chunk records for a knowledge base
func (w *Worker) DeleteKBChunkRecordsActivity(ctx context.Context, param *DeleteKBChunkRecordsActivityParam) error {
	w.log.Info("DeleteKBChunkRecordsActivity: Deleting chunk records",
		zap.String("kbUID", param.KnowledgeBaseUID.String()))

	err := w.repository.HardDeleteChunksByKbUID(ctx, param.KnowledgeBaseUID)
	if err != nil {
		return fmt.Errorf("failed to delete chunk records: %w", err)
	}

	w.log.Info("DeleteKBChunkRecordsActivity: Successfully deleted chunk records")
	return nil
}

// DeleteKBEmbeddingRecordsActivity deletes all embedding records for a knowledge base
func (w *Worker) DeleteKBEmbeddingRecordsActivity(ctx context.Context, param *DeleteKBEmbeddingRecordsActivityParam) error {
	w.log.Info("DeleteKBEmbeddingRecordsActivity: Deleting embedding records",
		zap.String("kbUID", param.KnowledgeBaseUID.String()))

	err := w.repository.HardDeleteEmbeddingsByKbUID(ctx, param.KnowledgeBaseUID)
	if err != nil {
		return fmt.Errorf("failed to delete embedding records: %w", err)
	}

	w.log.Info("DeleteKBEmbeddingRecordsActivity: Successfully deleted embedding records")
	return nil
}

// PurgeKBACLActivity purges ACL permissions for a knowledge base
func (w *Worker) PurgeKBACLActivity(ctx context.Context, param *PurgeKBACLActivityParam) error {
	w.log.Info("PurgeKBACLActivity: Purging ACL",
		zap.String("kbUID", param.KnowledgeBaseUID.String()))

	err := w.service.ACLClient().Purge(ctx, "knowledgebase", param.KnowledgeBaseUID)
	if err != nil {
		return fmt.Errorf("failed to purge ACL: %w", err)
	}

	w.log.Info("PurgeKBACLActivity: Successfully purged ACL")
	return nil
}
