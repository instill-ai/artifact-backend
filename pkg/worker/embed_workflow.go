package worker

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	errorsx "github.com/instill-ai/x/errors"
)

// EmbedTextsWorkflowParam defines the parameters for the EmbedTextsWorkflow
type EmbedTextsWorkflowParam struct {
	KBUID    *types.KBUIDType // Optional: Knowledge base UID for provider selection
	Texts    []string
	TaskType string // Task type for embedding optimization (e.g., "RETRIEVAL_DOCUMENT", "RETRIEVAL_QUERY")
}

type embedTextsWorkflow struct {
	temporalClient client.Client
	worker         *Worker
}

// NewEmbedTextsWorkflow creates a new EmbedTextsWorkflow instance
func NewEmbedTextsWorkflow(temporalClient client.Client, worker *Worker) *embedTextsWorkflow {
	return &embedTextsWorkflow{
		temporalClient: temporalClient,
		worker:         worker,
	}
}

func (w *embedTextsWorkflow) Execute(ctx context.Context, param EmbedTextsWorkflowParam) ([][]float32, error) {
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

// EmbedTextsWorkflow orchestrates embedding of texts
// NOTE: This is a thin wrapper around EmbedTextsActivity, primarily used for external API calls.
// Internal workflows (like ProcessFileWorkflow) should call EmbedTextsActivity directly for simplicity.
func (w *Worker) EmbedTextsWorkflow(ctx workflow.Context, param EmbedTextsWorkflowParam) ([][]float32, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting EmbedTextsWorkflow",
		"totalTexts", len(param.Texts),
		"taskType", param.TaskType)

	if len(param.Texts) == 0 {
		return [][]float32{}, nil
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

	// Execute single activity for all texts
	activityParam := &EmbedTextsActivityParam{
		KBUID:    param.KBUID,
		Texts:    param.Texts,
		TaskType: param.TaskType,
	}

	var vectors [][]float32
	if err := workflow.ExecuteActivity(ctx, w.EmbedTextsActivity, activityParam).Get(ctx, &vectors); err != nil {
		logger.Error("Embedding failed", "error", err)
		return nil, errorsx.AddMessage(err, "Failed to generate embeddings. Please try again.")
	}

	logger.Info("EmbedTextsWorkflow completed successfully",
		"totalTexts", len(param.Texts),
		"totalVectors", len(vectors))

	return vectors, nil
}

// SaveEmbeddingsWorkflowParam saves embeddings to vector db
type SaveEmbeddingsWorkflowParam struct {
	KBUID        types.KBUIDType             // Knowledge base unique identifier
	FileUID      types.FileUIDType           // File unique identifier
	FileName     string                      // File name for identification
	Embeddings   []repository.EmbeddingModel // Embeddings to save
	UserUID      types.UserUIDType           // User unique identifier
	RequesterUID types.RequesterUIDType      // Requester unique identifier
}

// SaveEmbeddingsWorkflow orchestrates parallel saving of embedding batches
// This workflow provides better performance than the single-activity approach by:
// 1. Deleting old embeddings once upfront
// 2. Saving batches in parallel (concurrent DB + Milvus writes)
func (w *Worker) SaveEmbeddingsWorkflow(ctx workflow.Context, param SaveEmbeddingsWorkflowParam) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting SaveEmbeddingsWorkflow",
		"kbUID", param.KBUID.String(),
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

	// Step 1: Delete old embeddings (both VectorDB + DB in single activity)
	deleteParam := &DeleteOldEmbeddingsActivityParam{
		KBUID:   param.KBUID,
		FileUID: param.FileUID,
	}

	if err := workflow.ExecuteActivity(ctx, w.DeleteOldEmbeddingsActivity, deleteParam).Get(ctx, nil); err != nil {
		logger.Error("Failed to delete old embeddings", "error", err)
		return errorsx.AddMessage(err, "Unable to delete old embeddings. Please try again.")
	}

	// Step 2: Save batches in parallel
	batchSize := EmbeddingBatchSize
	totalBatches := (len(param.Embeddings) + batchSize - 1) / batchSize
	logger.Info("Calculated batches for parallel processing", "totalBatches", totalBatches)

	futures := make([]workflow.Future, totalBatches)
	for i := range totalBatches {
		start := i * batchSize
		end := min(start+batchSize, len(param.Embeddings))

		batchEmbeddings := param.Embeddings[start:end]
		batchParam := &SaveEmbeddingBatchActivityParam{
			KBUID:        param.KBUID,
			FileUID:      param.FileUID,
			FileName:     param.FileName,
			Embeddings:   batchEmbeddings,
			BatchNumber:  i + 1,
			TotalBatches: totalBatches,
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

	// Step 3: Flush the collection to ensure immediate search availability
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

	logger.Info("SaveEmbeddingsWorkflow completed successfully",
		"kbUID", param.KBUID.String(),
		"fileUID", param.FileUID.String(),
		"userUID", param.UserUID.String(),
		"requesterUID", param.RequesterUID.String(),
		"totalEmbeddings", len(param.Embeddings),
		"totalBatches", totalBatches)

	return nil
}
