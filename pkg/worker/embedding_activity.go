package worker

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
)

// EmbedTextsActivity handles embedding a single batch of texts
func (w *worker) EmbedTextsActivity(ctx context.Context, param *EmbedTextsActivityParam) ([][]float32, error) {
	w.log.Info("Starting EmbedTextsActivity",
		zap.Int("batchSize", len(param.Texts)),
		zap.Int("batchIndex", param.BatchIndex))

	if len(param.Texts) == 0 {
		return [][]float32{}, nil
	}

	// Create authenticated context from request metadata
	authCtx := ctx
	if len(param.RequestMetadata) > 0 {
		authCtx = metadata.NewOutgoingContext(ctx, metadata.MD(param.RequestMetadata))
	}

	vectors, err := w.service.EmbeddingTextBatch(authCtx, param.Texts)
	if err != nil {
		w.log.Error("Failed to embed text batch",
			zap.Int("batchIndex", param.BatchIndex),
			zap.Int("batchSize", len(param.Texts)),
			zap.Error(err))
		return nil, fmt.Errorf("failed to embed batch %d: %w", param.BatchIndex, err)
	}

	w.log.Info("Batch embedding completed",
		zap.Int("batchIndex", param.BatchIndex),
		zap.Int("vectorCount", len(vectors)))

	return vectors, nil
}
