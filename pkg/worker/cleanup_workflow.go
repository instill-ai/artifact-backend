package worker

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/service"

	errorsx "github.com/instill-ai/x/errors"
)

type cleanupFileWorkflow struct {
	temporalClient client.Client
}

// NewCleanupFileWorkflow creates a new CleanupFileWorkflow instance
func NewCleanupFileWorkflow(temporalClient client.Client) service.CleanupFileWorkflow {
	return &cleanupFileWorkflow{temporalClient: temporalClient}
}

func (w *cleanupFileWorkflow) Execute(ctx context.Context, param service.CleanupFileWorkflowParam) error {
	workflowID := fmt.Sprintf("cleanup-file-%s", param.FileUID.String())
	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	_, err := w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, new(Worker).CleanupFileWorkflow, param)
	if err != nil {
		return fmt.Errorf("failed to start cleanup file workflow: %s", errorsx.MessageOrErr(err))
	}
	return nil
}

type cleanupKnowledgeBaseWorkflow struct {
	temporalClient client.Client
}

// NewCleanupKnowledgeBaseWorkflow creates a new CleanupKnowledgeBaseWorkflow instance
func NewCleanupKnowledgeBaseWorkflow(temporalClient client.Client) service.CleanupKnowledgeBaseWorkflow {
	return &cleanupKnowledgeBaseWorkflow{temporalClient: temporalClient}
}

func (w *cleanupKnowledgeBaseWorkflow) Execute(ctx context.Context, param service.CleanupKnowledgeBaseWorkflowParam) error {
	workflowID := fmt.Sprintf("cleanup-kb-%s", param.KnowledgeBaseUID.String())
	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	_, err := w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, new(Worker).CleanupKnowledgeBaseWorkflow, param)
	if err != nil {
		return fmt.Errorf("failed to start cleanup knowledge base workflow: %s", errorsx.MessageOrErr(err))
	}
	return nil
}

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

	// Step 1: Delete original file if requested
	// The activity will fetch file metadata to get destination and bucket
	if param.IncludeOriginalFile {
		err := workflow.ExecuteActivity(ctx, w.DeleteOriginalFileActivity, &DeleteOriginalFileActivityParam{
			FileUID: fileUID,
			Bucket:  config.Config.Minio.BucketName,
		}).Get(ctx, nil)
		if err != nil {
			logger.Warn("Failed to delete original file, continuing",
				"fileUID", fileUID.String(),
				"error", err.Error())
		}
	}

	// Step 2: Delete converted file
	err := workflow.ExecuteActivity(ctx, w.DeleteConvertedFileActivity, &DeleteConvertedFileActivityParam{
		FileUID: fileUID,
	}).Get(ctx, nil)
	if err != nil {
		logger.Warn("Failed to delete converted file, continuing",
			"fileUID", fileUID.String(),
			"error", err.Error())
	}

	// Step 3: Delete chunks
	err = workflow.ExecuteActivity(ctx, w.DeleteChunksFromMinIOActivity, &DeleteChunksFromMinIOActivityParam{
		FileUID: fileUID,
	}).Get(ctx, nil)
	if err != nil {
		logger.Warn("Failed to delete chunks, continuing",
			"fileUID", fileUID.String(),
			"error", err.Error())
	}

	// Step 4: Delete embeddings
	// The activity will get KB UID from the embedding records
	err = workflow.ExecuteActivity(ctx, w.DeleteEmbeddingsFromVectorDBActivity, &DeleteEmbeddingsFromVectorDBActivityParam{
		FileUID: fileUID,
	}).Get(ctx, nil)
	if err != nil {
		logger.Warn("Failed to delete embeddings, continuing",
			"fileUID", fileUID.String(),
			"error", err.Error())
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

	kbUID := param.KnowledgeBaseUID

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

	// Step 1: Delete all files from MinIO
	err := workflow.ExecuteActivity(ctx, w.DeleteKBFilesFromMinIOActivity, &DeleteKBFilesFromMinIOActivityParam{
		KnowledgeBaseUID: kbUID,
	}).Get(ctx, nil)
	if err != nil {
		logger.Warn("Failed to delete files from MinIO, continuing",
			"kbUID", kbUID.String(),
			"error", err.Error())
	}

	// Step 2: Drop Milvus collection
	err = workflow.ExecuteActivity(ctx, w.DropVectorDBCollectionActivity, &DropVectorDBCollectionActivityParam{
		KnowledgeBaseUID: kbUID,
	}).Get(ctx, nil)
	if err != nil {
		logger.Warn("Failed to drop Milvus collection, continuing",
			"kbUID", kbUID.String(),
			"error", err.Error())
	}

	// Step 3: Delete file records
	err = workflow.ExecuteActivity(ctx, w.DeleteKBFileRecordsActivity, &DeleteKBFileRecordsActivityParam{
		KnowledgeBaseUID: kbUID,
	}).Get(ctx, nil)
	if err != nil {
		logger.Warn("Failed to delete file records, continuing",
			"kbUID", kbUID.String(),
			"error", err.Error())
	}

	// Step 4: Delete converted file records
	err = workflow.ExecuteActivity(ctx, w.DeleteKBConvertedFileRecordsActivity, &DeleteKBConvertedFileRecordsActivityParam{
		KnowledgeBaseUID: kbUID,
	}).Get(ctx, nil)
	if err != nil {
		logger.Warn("Failed to delete converted file records, continuing",
			"kbUID", kbUID.String(),
			"error", err.Error())
	}

	// Step 5: Delete chunk records
	err = workflow.ExecuteActivity(ctx, w.DeleteKBChunkRecordsActivity, &DeleteKBChunkRecordsActivityParam{
		KnowledgeBaseUID: kbUID,
	}).Get(ctx, nil)
	if err != nil {
		logger.Warn("Failed to delete chunk records, continuing",
			"kbUID", kbUID.String(),
			"error", err.Error())
	}

	// Step 6: Delete embedding records
	err = workflow.ExecuteActivity(ctx, w.DeleteKBEmbeddingRecordsActivity, &DeleteKBEmbeddingRecordsActivityParam{
		KnowledgeBaseUID: kbUID,
	}).Get(ctx, nil)
	if err != nil {
		logger.Warn("Failed to delete embedding records, continuing",
			"kbUID", kbUID.String(),
			"error", err.Error())
	}

	// Step 7: Purge ACL (must be done last)
	err = workflow.ExecuteActivity(ctx, w.PurgeKBACLActivity, &PurgeKBACLActivityParam{
		KnowledgeBaseUID: kbUID,
	}).Get(ctx, nil)
	if err != nil {
		logger.Warn("Failed to purge ACL, continuing",
			"kbUID", kbUID.String(),
			"error", err.Error())
	}

	logger.Info("CleanupKnowledgeBaseWorkflow completed successfully", "kbUID", param.KnowledgeBaseUID.String())
	return nil
}
