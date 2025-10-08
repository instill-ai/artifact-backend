package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"

	errorsx "github.com/instill-ai/x/errors"
)

type embedTextsWorkflow struct {
	temporalClient client.Client
	worker         *Worker
}

// NewEmbedTextsWorkflow creates a new EmbedTextsWorkflow instance
func NewEmbedTextsWorkflow(temporalClient client.Client, worker *Worker) service.EmbedTextsWorkflow {
	return &embedTextsWorkflow{
		temporalClient: temporalClient,
		worker:         worker,
	}
}

func (w *embedTextsWorkflow) Execute(ctx context.Context, param service.EmbedTextsWorkflowParam) ([][]float32, error) {
	workflowID := fmt.Sprintf("embed-texts-%d-%d", time.Now().UnixNano(), len(param.Texts))
	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	workflowRun, err := w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, w.worker.EmbedTextsWorkflow, param)
	if err != nil {
		return nil, errorsx.AddMessage(err, "Unable to start embedding workflow. Please try again.")
	}

	var vectors [][]float32
	if err = workflowRun.Get(ctx, &vectors); err != nil {
		return nil, errorsx.AddMessage(err, "Embedding workflow failed. Please try again.")
	}

	return vectors, nil
}

// EmbedTextsWorkflow orchestrates parallel embedding of text batches
func (w *Worker) EmbedTextsWorkflow(ctx workflow.Context, param service.EmbedTextsWorkflowParam) ([][]float32, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting EmbedTextsWorkflow",
		"totalTexts", len(param.Texts),
		"batchSize", param.BatchSize)

	if len(param.Texts) == 0 {
		return [][]float32{}, nil
	}

	batchSize := param.BatchSize
	if batchSize <= 0 {
		batchSize = 32
	}

	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: ActivityTimeoutStandard,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    RetryInitialInterval,
			BackoffCoefficient: RetryBackoffCoefficient,
			MaximumInterval:    RetryMaximumIntervalStandard,
			MaximumAttempts:    RetryMaximumAttempts,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	totalBatches := (len(param.Texts) + batchSize - 1) / batchSize
	logger.Info("Calculated batches", "totalBatches", totalBatches)

	futures := make([]workflow.Future, totalBatches)
	for i := range totalBatches {
		start := i * batchSize
		end := min(start+batchSize, len(param.Texts))

		batchTexts := param.Texts[start:end]
		activityParam := &EmbedTextsActivityParam{
			Texts:           batchTexts,
			BatchIndex:      i,
			RequestMetadata: param.RequestMetadata,
		}

		futures[i] = workflow.ExecuteActivity(ctx, w.EmbedTextsActivity, activityParam)
	}

	allVectors := make([][]float32, 0, len(param.Texts))
	for i, future := range futures {
		var vectors [][]float32
		if err := future.Get(ctx, &vectors); err != nil {
			logger.Error("Batch failed",
				"batchIndex", i,
				"error", err)
			return nil, errorsx.AddMessage(err, fmt.Sprintf("Unable to generate embeddings for batch %d. Please try again.", i))
		}

		logger.Info("Batch completed",
			"batchIndex", i,
			"vectorCount", len(vectors))

		allVectors = append(allVectors, vectors...)
	}

	logger.Info("EmbedTextsWorkflow completed successfully",
		"totalTexts", len(param.Texts),
		"totalVectors", len(allVectors))

	return allVectors, nil
}

// SaveEmbeddingsToVectorDBWorkflowParam saves embeddings to vector db
type SaveEmbeddingsToVectorDBWorkflowParam struct {
	KnowledgeBaseUID uuid.UUID              // Knowledge base unique identifier
	FileUID          uuid.UUID              // File unique identifier
	FileName         string                 // File name for identification
	Embeddings       []repository.Embedding // Embeddings to save
	UserUID          uuid.UUID              // User unique identifier
	RequesterUID     uuid.UUID              // Requester unique identifier
}

// SaveEmbeddingsToVectorDBWorkflow orchestrates parallel saving of embedding batches
// This workflow provides better performance than the single-activity approach by:
// 1. Deleting old embeddings once upfront
// 2. Saving batches in parallel (concurrent DB + Milvus writes)
func (w *Worker) SaveEmbeddingsToVectorDBWorkflow(ctx workflow.Context, param SaveEmbeddingsToVectorDBWorkflowParam) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting SaveEmbeddingsToVectorDBWorkflow",
		"kbUID", param.KnowledgeBaseUID.String(),
		"fileUID", param.FileUID.String(),
		"userUID", param.UserUID.String(),
		"requesterUID", param.RequesterUID.String(),
		"embeddingCount", len(param.Embeddings))

	if len(param.Embeddings) == 0 {
		logger.Info("No embeddings to save")
		return nil
	}

	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: ActivityTimeoutLong,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    RetryInitialInterval,
			BackoffCoefficient: RetryBackoffCoefficient,
			MaximumInterval:    RetryMaximumIntervalLong,
			MaximumAttempts:    RetryMaximumAttempts,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Step 1 & 2: Delete old embeddings in parallel (VectorDB + DB)
	deleteParam := &DeleteOldEmbeddingsActivityParam{
		KnowledgeBaseUID: param.KnowledgeBaseUID,
		FileUID:          param.FileUID,
	}

	// Execute both delete operations in parallel
	deleteVectorDBFuture := workflow.ExecuteActivity(ctx, w.DeleteOldEmbeddingsFromVectorDBActivity, deleteParam)
	deleteDBFuture := workflow.ExecuteActivity(ctx, w.DeleteOldEmbeddingsFromDBActivity, deleteParam)

	// Wait for both to complete
	if err := deleteVectorDBFuture.Get(ctx, nil); err != nil {
		logger.Error("Failed to delete old embeddings from VectorDB", "error", err)
		return errorsx.AddMessage(err, "Unable to delete old embeddings from vector database. Please try again.")
	}

	if err := deleteDBFuture.Get(ctx, nil); err != nil {
		logger.Error("Failed to delete old embeddings from DB", "error", err)
		return errorsx.AddMessage(err, "Unable to delete old embedding records. Please try again.")
	}

	// Step 3: Save batches in parallel
	batchSize := EmbeddingBatchSize
	totalBatches := (len(param.Embeddings) + batchSize - 1) / batchSize
	logger.Info("Calculated batches for parallel processing", "totalBatches", totalBatches)

	futures := make([]workflow.Future, totalBatches)
	for i := range totalBatches {
		start := i * batchSize
		end := min(start+batchSize, len(param.Embeddings))

		batchEmbeddings := param.Embeddings[start:end]
		batchParam := &SaveEmbeddingBatchActivityParam{
			KnowledgeBaseUID: param.KnowledgeBaseUID,
			FileUID:          param.FileUID,
			FileName:         param.FileName,
			Embeddings:       batchEmbeddings,
			BatchNumber:      i + 1,
			TotalBatches:     totalBatches,
		}

		// Execute activities in parallel (no .Get() here)
		futures[i] = workflow.ExecuteActivity(ctx, w.SaveEmbeddingBatchActivity, batchParam)
	}

	// Wait for all batches to complete
	for i, future := range futures {
		if err := future.Get(ctx, nil); err != nil {
			logger.Error("Batch failed",
				"batchNumber", i+1,
				"totalBatches", totalBatches,
				"error", err)
			return errorsx.AddMessage(err, fmt.Sprintf("Unable to save embedding batch %d/%d. Please try again.", i+1, totalBatches))
		}

		logger.Info("Batch completed",
			"batchNumber", i+1,
			"totalBatches", totalBatches)
	}

	// Step 4: Flush the collection to ensure immediate search availability
	// Note: Milvus 2.x has auto-flush, but we flush manually here to guarantee
	// that newly uploaded files are immediately searchable when marked as "completed"
	//
	// Alternative: If eventual consistency is acceptable (1-60s delay), this can be removed
	// to rely on Milvus auto-flush, improving workflow performance by ~150-200ms
	logger.Info("Flushing collection after all batches completed")
	localActivityOptions := workflow.LocalActivityOptions{
		StartToCloseTimeout: ActivityTimeoutStandard,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    RetryInitialInterval,
			BackoffCoefficient: RetryBackoffCoefficient,
			MaximumInterval:    RetryMaximumIntervalStandard,
			MaximumAttempts:    RetryMaximumAttempts,
		},
	}
	localCtx := workflow.WithLocalActivityOptions(ctx, localActivityOptions)

	if err := workflow.ExecuteLocalActivity(localCtx, w.FlushCollectionActivity, deleteParam).Get(localCtx, nil); err != nil {
		logger.Error("Failed to flush collection", "error", err)
		return errorsx.AddMessage(err, "Unable to flush vector database collection. Please try again.")
	}

	logger.Info("SaveEmbeddingsToVectorDBWorkflow completed successfully",
		"kbUID", param.KnowledgeBaseUID.String(),
		"fileUID", param.FileUID.String(),
		"userUID", param.UserUID.String(),
		"requesterUID", param.RequesterUID.String(),
		"totalEmbeddings", len(param.Embeddings),
		"totalBatches", totalBatches)

	return nil
}
