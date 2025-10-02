package worker

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	artifacttemporal "github.com/instill-ai/artifact-backend/pkg/temporal"
)

// EmbedTextsWorkflow orchestrates parallel embedding of text batches
func (w *worker) EmbedTextsWorkflow(ctx workflow.Context, param artifacttemporal.EmbedTextsWorkflowParam) ([][]float32, error) {
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
