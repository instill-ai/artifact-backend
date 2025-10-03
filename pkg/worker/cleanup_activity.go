package worker

import (
	"context"
	"fmt"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/service"
)

// CleanupFilesActivity cleans up files and resources for a specific file.
// Used by CleanupFileWorkflow to clean up resources for failed workflows or explicit file deletion.
// This activity handles file-level cleanup including:
// - Original file from MinIO (if IncludeOriginalFile is true)
// - Converted files (markdown/text conversions)
// - Text chunks
// - Embeddings from both Milvus and Postgres
func (w *Worker) CleanupFilesActivity(ctx context.Context, param *CleanupFilesActivityParam) error {
	w.log.Info("Starting cleanup of files",
		zap.String("fileUID", param.FileUID.String()),
		zap.Int("specificFileIDs", len(param.FileIDs)),
		zap.Bool("includeOriginalFile", param.IncludeOriginalFile))

	file, err := getFileByUID(ctx, w.repository, param.FileUID)
	if err != nil {
		// If file doesn't exist, it may have been deleted - that's okay
		return nil
	}

	kbUID := file.KnowledgeBaseUID
	cleanupCount := 0
	errors := []string{}

	// 0. Cleanup original file if requested (for explicit file deletion)
	if param.IncludeOriginalFile && file.Destination != "" {
		objectPaths := []string{file.Destination}
		err := w.service.DeleteFiles(ctx, config.Config.Minio.BucketName, objectPaths)
		if err != nil {
			w.log.Error("Failed to delete original file from MinIO",
				zap.String("fileUID", param.FileUID.String()),
				zap.String("destination", file.Destination),
				zap.Error(err))
			errors = append(errors, fmt.Sprintf("original file: %v", err))
		} else {
			cleanupCount++
			w.log.Info("Deleted original file from MinIO",
				zap.String("fileUID", param.FileUID.String()),
				zap.String("destination", file.Destination))
		}
	}

	// 1. Cleanup converted files if they exist
	convertedFile, err := w.repository.GetConvertedFileByFileUID(ctx, param.FileUID)
	if err == nil && convertedFile != nil {
		err = w.service.DeleteConvertedFileByFileUID(ctx, kbUID, param.FileUID)
		if err != nil {
			w.log.Error("Failed to delete converted file from MinIO",
				zap.String("fileUID", param.FileUID.String()),
				zap.Error(err))
			errors = append(errors, fmt.Sprintf("converted file: %v", err))
		} else {
			cleanupCount++
			w.log.Info("Deleted converted file from MinIO",
				zap.String("convertedFileUID", convertedFile.UID.String()))
		}

		err = w.repository.HardDeleteConvertedFileByFileUID(ctx, param.FileUID)
		if err != nil {
			w.log.Error("Failed to delete converted file record",
				zap.String("fileUID", param.FileUID.String()),
				zap.Error(err))
			errors = append(errors, fmt.Sprintf("converted file record: %v", err))
		} else {
			cleanupCount++
		}
	}

	// 2. Cleanup text chunks if they exist
	chunks, err := w.repository.ListChunksByKbFileUID(ctx, param.FileUID)
	if err == nil && len(chunks) > 0 {
		err = w.service.DeleteTextChunksByFileUID(ctx, kbUID, param.FileUID)
		if err != nil {
			w.log.Error("Failed to delete chunks from MinIO",
				zap.String("fileUID", param.FileUID.String()),
				zap.Int("chunkCount", len(chunks)),
				zap.Error(err))
			errors = append(errors, fmt.Sprintf("chunks: %v", err))
		} else {
			cleanupCount += len(chunks)
			w.log.Info("Deleted chunks from MinIO",
				zap.String("fileUID", param.FileUID.String()),
				zap.Int("chunkCount", len(chunks)))
		}

		err = w.repository.HardDeleteChunksByKbFileUID(ctx, param.FileUID)
		if err != nil {
			w.log.Error("Failed to delete chunk records",
				zap.String("fileUID", param.FileUID.String()),
				zap.Error(err))
			errors = append(errors, fmt.Sprintf("chunk records: %v", err))
		} else {
			cleanupCount++
		}
	}

	// 3. Cleanup embeddings if they exist
	collection := service.KBCollectionName(kbUID)
	err = w.service.VectorDB().DeleteEmbeddingsWithFileUID(ctx, collection, param.FileUID)
	if err != nil {
		w.log.Error("Failed to delete embeddings from Milvus",
			zap.String("fileUID", param.FileUID.String()),
			zap.Error(err))
		errors = append(errors, fmt.Sprintf("embeddings in Milvus: %v", err))
	} else {
		w.log.Info("Deleted embeddings from Milvus by fileUID",
			zap.String("fileUID", param.FileUID.String()))
		cleanupCount++
	}

	err = w.repository.HardDeleteEmbeddingsByKbFileUID(ctx, param.FileUID)
	if err != nil {
		w.log.Error("Failed to delete embedding records",
			zap.String("fileUID", param.FileUID.String()),
			zap.Error(err))
		errors = append(errors, fmt.Sprintf("embedding records: %v", err))
	} else {
		cleanupCount++
	}

	// 4. Cleanup specific temporary files if provided
	if len(param.FileIDs) > 0 {
		w.log.Info("Cleaning up specific temporary files",
			zap.Int("count", len(param.FileIDs)))
	}

	if len(errors) > 0 {
		w.log.Warn("Cleanup completed with some errors",
			zap.String("fileUID", param.FileUID.String()),
			zap.Int("successfulCleanups", cleanupCount),
			zap.Strings("errors", errors))
		return nil
	}

	w.log.Info("Cleanup completed successfully",
		zap.String("fileUID", param.FileUID.String()),
		zap.Int("itemsCleaned", cleanupCount))

	return nil
}

// CleanupKnowledgeBaseActivity performs complete cleanup of an entire knowledge base.
// Used by CleanupKnowledgeBaseWorkflow to clean up all resources when deleting a knowledge base.
// This activity handles knowledge-base-level cleanup including:
// - All files from MinIO for the knowledge base
// - Entire Milvus collection (all embeddings)
// - All file, converted file, chunk, and embedding records in Postgres
// - ACL permissions for the knowledge base
func (w *Worker) CleanupKnowledgeBaseActivity(ctx context.Context, param service.CleanupKnowledgeBaseWorkflowParam) error {
	logger := w.log.With(zap.String("kbUID", param.KnowledgeBaseUID))
	logger.Info("Starting CleanupKnowledgeBaseActivity")

	kbUUID, err := uuid.FromString(param.KnowledgeBaseUID)
	if err != nil {
		return fmt.Errorf("invalid knowledge base UID: %w", err)
	}

	allPass := true
	errors := []string{}

	// 1. Delete files from MinIO
	logger.Info("Deleting files from MinIO")
	if err := w.service.DeleteKnowledgeBase(ctx, param.KnowledgeBaseUID); err != nil {
		logger.Error("Failed to delete files in MinIO", zap.Error(err))
		errors = append(errors, fmt.Sprintf("MinIO deletion: %v", err))
		allPass = false
	}

	// 2. Delete collection in Milvus
	logger.Info("Deleting collection in Milvus")
	if err := w.service.VectorDB().DropCollection(ctx, service.KBCollectionName(kbUUID)); err != nil {
		logger.Error("Failed to delete collection in Milvus", zap.Error(err))
		errors = append(errors, fmt.Sprintf("Milvus deletion: %v", err))
		allPass = false
	}

	// 3. Delete all files in Postgres
	logger.Info("Deleting files in Postgres")
	if err := w.repository.DeleteAllKnowledgeBaseFiles(ctx, param.KnowledgeBaseUID); err != nil {
		logger.Error("Failed to delete files in Postgres", zap.Error(err))
		errors = append(errors, fmt.Sprintf("Postgres files deletion: %v", err))
		allPass = false
	}

	// 4. Delete converted files in Postgres
	logger.Info("Deleting converted files in Postgres")
	if err := w.repository.DeleteAllConvertedFilesInKb(ctx, kbUUID); err != nil {
		logger.Error("Failed to delete converted files in Postgres", zap.Error(err))
		errors = append(errors, fmt.Sprintf("Postgres converted files deletion: %v", err))
		allPass = false
	}

	// 5. Delete all chunks in Postgres
	logger.Info("Deleting chunks in Postgres")
	if err := w.repository.HardDeleteChunksByKbUID(ctx, kbUUID); err != nil {
		logger.Error("Failed to delete chunks in Postgres", zap.Error(err))
		errors = append(errors, fmt.Sprintf("Postgres chunks deletion: %v", err))
		allPass = false
	}

	// 6. Delete all embeddings in Postgres
	logger.Info("Deleting embeddings in Postgres")
	if err := w.repository.HardDeleteEmbeddingsByKbUID(ctx, kbUUID); err != nil {
		logger.Error("Failed to delete embeddings in Postgres", zap.Error(err))
		errors = append(errors, fmt.Sprintf("Postgres embeddings deletion: %v", err))
		allPass = false
	}

	// 7. Delete ACL (must be done after deleting the catalog)
	logger.Info("Purging ACL")
	if err := w.service.ACLClient().Purge(ctx, "knowledgebase", kbUUID); err != nil {
		logger.Error("Failed to purge ACL", zap.Error(err))
		errors = append(errors, fmt.Sprintf("ACL purge: %v", err))
		allPass = false
	}

	if allPass {
		logger.Info("Successfully completed CleanupKnowledgeBaseActivity")
		return nil
	}

	return fmt.Errorf("cleanup completed with errors: %v", errors)
}
