package worker

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/instill-ai/artifact-backend/pkg/service"
)

type embedTextsWorkflow struct {
	temporalClient client.Client
}

// NewEmbedTextsWorkflow creates a new EmbedTextsWorkflow instance
func NewEmbedTextsWorkflow(temporalClient client.Client) service.EmbedTextsWorkflow {
	return &embedTextsWorkflow{temporalClient: temporalClient}
}

func (w *embedTextsWorkflow) Execute(ctx context.Context, param service.EmbedTextsWorkflowParam) ([][]float32, error) {
	workflowID := fmt.Sprintf("embed-texts-%d-%d", time.Now().UnixNano(), len(param.Texts))
	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	workflowRun, err := w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, new(Worker).EmbedTextsWorkflow, param)
	if err != nil {
		return nil, err
	}

	var vectors [][]float32
	if err = workflowRun.Get(ctx, &vectors); err != nil {
		return nil, err
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
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    3,
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
			return nil, fmt.Errorf("batch %d failed: %w", i, err)
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
