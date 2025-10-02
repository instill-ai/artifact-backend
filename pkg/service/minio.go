package service

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"go.temporal.io/sdk/client"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/temporal"
)

func (s *service) GetFilesByPaths(ctx context.Context, bucket string, filePaths []string) ([]temporal.FileContent, error) {
	if len(filePaths) == 0 {
		return []temporal.FileContent{}, nil
	}

	workflowID := fmt.Sprintf("get-files-%d-%d", time.Now().UnixNano(), len(filePaths))
	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: temporal.TaskQueue,
	}

	workflowRun, err := s.temporalClient.ExecuteWorkflow(ctx, workflowOptions, "GetFilesWorkflow", temporal.GetFilesWorkflowParam{
		Bucket:    bucket,
		FilePaths: filePaths,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start get files workflow: %w", err)
	}

	var results []temporal.FileContent
	if err = workflowRun.Get(ctx, &results); err != nil {
		return nil, fmt.Errorf("get files workflow failed: %w", err)
	}

	return results, nil
}

func (s *service) DeleteFiles(ctx context.Context, bucket string, filePaths []string) error {
	if len(filePaths) == 0 {
		return nil
	}

	workflowID := fmt.Sprintf("delete-files-%d-%d", time.Now().UnixNano(), len(filePaths))
	workflowRun, err := s.temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: temporal.TaskQueue,
	}, "DeleteFilesWorkflow", temporal.DeleteFilesWorkflowParam{
		Bucket:    bucket,
		FilePaths: filePaths,
	})
	if err != nil {
		return fmt.Errorf("failed to start delete files workflow: %w", err)
	}

	if err = workflowRun.Get(ctx, nil); err != nil {
		return fmt.Errorf("delete files workflow failed: %w", err)
	}

	return nil
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

	workflowID := fmt.Sprintf("save-chunks-%s-%d", fileUID.String(), time.Now().UnixNano())
	workflowRun, err := s.temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: temporal.TaskQueue,
	}, "SaveChunksWorkflow", temporal.SaveChunksWorkflowParam{
		KnowledgeBaseUID: kbUID,
		FileUID:          fileUID,
		Chunks:           chunks,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start save chunks workflow: %w", err)
	}

	var destinations map[string]string
	if err = workflowRun.Get(ctx, &destinations); err != nil {
		return nil, fmt.Errorf("save chunks workflow failed: %w", err)
	}

	return destinations, nil
}

func (s *service) TriggerCleanupKnowledgeBaseWorkflow(ctx context.Context, kbUID string) error {
	if s.temporalClient == nil {
		return fmt.Errorf("temporal client not available")
	}

	workflowID := fmt.Sprintf("cleanup-kb-%s-%d", kbUID, time.Now().UnixNano())
	workflowRun, err := s.temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: temporal.TaskQueue,
	}, "CleanupKnowledgeBaseWorkflow", temporal.CleanupKnowledgeBaseWorkflowParam{
		KnowledgeBaseUID: kbUID,
	})
	if err != nil {
		return fmt.Errorf("failed to start cleanup knowledge base workflow: %w", err)
	}

	// Don't wait for completion - this is a fire-and-forget background operation
	_ = workflowRun

	return nil
}
