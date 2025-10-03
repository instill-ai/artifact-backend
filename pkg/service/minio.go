package service

import (
	"context"
	"fmt"

	"github.com/gofrs/uuid"

	"github.com/instill-ai/artifact-backend/config"
)

func (s *service) GetFilesByPaths(ctx context.Context, bucket string, filePaths []string) ([]FileContent, error) {
	if len(filePaths) == 0 {
		return []FileContent{}, nil
	}

	return s.getFilesWorkflow.Execute(ctx, GetFilesWorkflowParam{
		Bucket:    bucket,
		FilePaths: filePaths,
	})
}

func (s *service) DeleteFiles(ctx context.Context, bucket string, filePaths []string) error {
	if len(filePaths) == 0 {
		return nil
	}

	return s.deleteFilesWorkflow.Execute(ctx, DeleteFilesWorkflowParam{
		Bucket:    bucket,
		FilePaths: filePaths,
	})
}

func (s *service) DeleteFilesWithPrefix(ctx context.Context, bucket string, prefix string) error {
	filePaths, err := s.minIO.ListFilePathsWithPrefix(ctx, bucket, prefix)
	if err != nil {
		return fmt.Errorf("failed to list files with prefix: %w", err)
	}
	return s.DeleteFiles(ctx, bucket, filePaths)
}

func (s *service) DeleteKnowledgeBase(ctx context.Context, kbUID string) error {
	kbUUID, err := uuid.FromString(kbUID)
	if err != nil {
		return fmt.Errorf("invalid knowledge base UID: %w", err)
	}

	filePaths, err := s.minIO.ListKnowledgeBaseFiles(ctx, kbUUID)
	if err != nil {
		return fmt.Errorf("failed to list knowledge base files: %w", err)
	}
	return s.DeleteFiles(ctx, config.Config.Minio.BucketName, filePaths)
}

func (s *service) DeleteConvertedFileByFileUID(ctx context.Context, kbUID, fileUID uuid.UUID) error {
	filePaths, err := s.minIO.ListConvertedFilesByFileUID(ctx, kbUID, fileUID)
	if err != nil {
		return fmt.Errorf("failed to list converted files: %w", err)
	}
	return s.DeleteFiles(ctx, config.Config.Minio.BucketName, filePaths)
}

func (s *service) DeleteTextChunksByFileUID(ctx context.Context, kbUID, fileUID uuid.UUID) error {
	filePaths, err := s.minIO.ListTextChunksByFileUID(ctx, kbUID, fileUID)
	if err != nil {
		return fmt.Errorf("failed to list chunk files: %w", err)
	}
	return s.DeleteFiles(ctx, config.Config.Minio.BucketName, filePaths)
}

func (s *service) SaveTextChunks(ctx context.Context, kbUID, fileUID uuid.UUID, chunks map[string][]byte) (map[string]string, error) {
	if len(chunks) == 0 {
		return map[string]string{}, nil
	}

	return s.saveChunksWorkflow.Execute(ctx, SaveChunksWorkflowParam{
		KnowledgeBaseUID: kbUID,
		FileUID:          fileUID,
		Chunks:           chunks,
	})
}

func (s *service) TriggerCleanupKnowledgeBaseWorkflow(ctx context.Context, kbUID string) error {
	if s.cleanupKnowledgeBaseWorkflow == nil {
		return fmt.Errorf("cleanup knowledge base workflow not available")
	}

	// Fire-and-forget - start workflow but don't wait for completion
	return s.cleanupKnowledgeBaseWorkflow.Execute(ctx, CleanupKnowledgeBaseWorkflowParam{
		KnowledgeBaseUID: kbUID,
	})
}
