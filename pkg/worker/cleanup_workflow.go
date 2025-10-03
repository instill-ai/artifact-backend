package worker

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/instill-ai/artifact-backend/pkg/service"
)

// CleanupFileWorkflow handles cleanup operations for a specific file.
// This workflow cleans up resources for a single file, including:
// - Original file from MinIO (if IncludeOriginalFile is true)
// - Converted files (markdown/text conversions)
// - Text chunks
// - Embeddings from both Milvus and Postgres
// Use this when deleting individual files from a knowledge base.
func (w *Worker) CleanupFileWorkflow(ctx workflow.Context, param service.CleanupFileWorkflowParam) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting CleanupFileWorkflow",
		"fileUID", param.FileUID.String(),
		"userUID", param.UserUID.String(),
		"workflowID", param.WorkflowID,
		"includeOriginalFile", param.IncludeOriginalFile)

	fileUID := param.FileUID

	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	cleanupParam := &CleanupFilesActivityParam{
		FileUID:             fileUID,
		FileIDs:             []string{},
		IncludeOriginalFile: param.IncludeOriginalFile,
	}

	err := workflow.ExecuteActivity(ctx, w.CleanupFilesActivity, cleanupParam).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to cleanup files", "error", err)
		return fmt.Errorf("failed to cleanup files: %w", err)
	}

	logger.Info("CleanupFileWorkflow completed successfully",
		"fileUID", param.FileUID.String(),
		"workflowID", param.WorkflowID)
	return nil
}

// CleanupKnowledgeBaseWorkflow orchestrates the complete cleanup of an entire knowledge base.
// This workflow performs a comprehensive cleanup of all knowledge base resources, including:
// - All files from MinIO for the knowledge base
// - Entire Milvus collection (all embeddings)
// - All file records in Postgres
// - All converted file records in Postgres
// - All chunk records in Postgres
// - All embedding records in Postgres
// - ACL permissions for the knowledge base
// Use this when deleting an entire knowledge base (not individual files).
func (w *Worker) CleanupKnowledgeBaseWorkflow(ctx workflow.Context, param service.CleanupKnowledgeBaseWorkflowParam) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting CleanupKnowledgeBaseWorkflow", "kbUID", param.KnowledgeBaseUID.String())

	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    100 * time.Second,
			MaximumAttempts:    3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	err := workflow.ExecuteActivity(ctx, w.CleanupKnowledgeBaseActivity, param).Get(ctx, nil)
	if err != nil {
		logger.Error("CleanupKnowledgeBaseWorkflow failed", "error", err)
		return err
	}

	logger.Info("CleanupKnowledgeBaseWorkflow completed successfully", "kbUID", param.KnowledgeBaseUID.String())
	return nil
}
