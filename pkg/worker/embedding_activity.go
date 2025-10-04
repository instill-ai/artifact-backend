package worker

import (
	"context"
	"fmt"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/pkg/repository"
)

// EmbedTextsActivityParam defines the parameters for the EmbedTextsActivity
type EmbedTextsActivityParam struct {
	Texts           []string
	BatchIndex      int
	RequestMetadata map[string][]string // gRPC metadata for authentication
}

// GetChunksForEmbeddingActivityParam retrieves chunks for embedding
type GetChunksForEmbeddingActivityParam struct {
	FileUID uuid.UUID
}

// GetChunksForEmbeddingActivityResult contains chunk data
type GetChunksForEmbeddingActivityResult struct {
	SourceTable string
	SourceUID   uuid.UUID
	Chunks      []repository.TextChunk
	Texts       []string
	Metadata    *structpb.Struct
	FileName    string
}

// GenerateEmbeddingsActivityParam for external embedding pipeline call
type GenerateEmbeddingsActivityParam struct {
	Texts    []string
	Metadata *structpb.Struct
}

// GenerateEmbeddingsActivityResult contains generated embeddings
type GenerateEmbeddingsActivityResult struct {
	Embeddings [][]float32
}

// SaveEmbeddingsToVectorDBActivityParam saves embeddings to Milvus
type SaveEmbeddingsToVectorDBActivityParam struct {
	KnowledgeBaseUID uuid.UUID
	FileUID          uuid.UUID
	FileName         string
	Embeddings       []repository.Embedding
}

// SaveEmbeddingsToDBActivityParam saves embedding metadata to DB
type SaveEmbeddingsToDBActivityParam struct {
	FileUID    uuid.UUID
	Embeddings []repository.Embedding
}

// UpdateEmbeddingMetadataActivityParam updates metadata after embedding
type UpdateEmbeddingMetadataActivityParam struct {
	FileUID  uuid.UUID
	Pipeline string
}

// GetChunksForEmbeddingActivity retrieves chunks and texts for embedding
// This is a DB read operation - idempotent
func (w *Worker) GetChunksForEmbeddingActivity(ctx context.Context, param *GetChunksForEmbeddingActivityParam) (*GetChunksForEmbeddingActivityResult, error) {
	w.log.Info("GetChunksForEmbeddingActivity: Fetching chunks",
		zap.String("fileUID", param.FileUID.String()))

	// Get file
	files, err := w.repository.GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{param.FileUID})
	if err != nil || len(files) == 0 {
		return nil, fmt.Errorf("failed to get file: %w", err)
	}
	file := files[0]

	// Get chunks by file
	sourceTable, sourceUID, chunks, _, texts, err := w.service.GetChunksByFile(ctx, &file)
	if err != nil {
		return nil, fmt.Errorf("failed to get chunks: %w", err)
	}

	w.log.Info("GetChunksForEmbeddingActivity: Chunks retrieved",
		zap.Int("chunkCount", len(chunks)),
		zap.Int("textCount", len(texts)))

	return &GetChunksForEmbeddingActivityResult{
		SourceTable: sourceTable,
		SourceUID:   sourceUID,
		Chunks:      chunks,
		Texts:       texts,
		Metadata:    file.ExternalMetadataUnmarshal,
		FileName:    file.Name,
	}, nil
}

// GenerateEmbeddingsActivity calls external pipeline to generate embeddings
// This is a single external API call - idempotent (pipeline should be deterministic)
func (w *Worker) GenerateEmbeddingsActivity(ctx context.Context, param *GenerateEmbeddingsActivityParam) (*GenerateEmbeddingsActivityResult, error) {
	w.log.Info("GenerateEmbeddingsActivity: Generating embeddings",
		zap.Int("textCount", len(param.Texts)))

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = createAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			w.log.Warn("Failed to create authenticated context, using original", zap.Error(err))
			authCtx = ctx
		}
	}

	// Call the embedding pipeline - THIS IS THE KEY EXTERNAL CALL
	embeddings, err := w.service.EmbeddingTextPipe(authCtx, param.Texts)
	if err != nil {
		return nil, fmt.Errorf("embedding pipeline failed: %w", err)
	}

	w.log.Info("GenerateEmbeddingsActivity: Embeddings generated successfully",
		zap.Int("embeddingCount", len(embeddings)))

	return &GenerateEmbeddingsActivityResult{
		Embeddings: embeddings,
	}, nil
}

// SaveEmbeddingsToVectorDBActivity saves embeddings to Milvus and database
// This is a combined Milvus write + DB write operation - idempotent
func (w *Worker) SaveEmbeddingsToVectorDBActivity(ctx context.Context, param *SaveEmbeddingsToVectorDBActivityParam) error {
	w.log.Info("SaveEmbeddingsToVectorDBActivity: Saving embeddings to Milvus",
		zap.String("kbUID", param.KnowledgeBaseUID.String()),
		zap.Int("embeddingCount", len(param.Embeddings)))

	// Use helper function from common.go
	err := saveEmbeddings(ctx, w.service, param.KnowledgeBaseUID, param.FileUID, param.Embeddings, param.FileName)
	if err != nil {
		return fmt.Errorf("failed to save embeddings: %w", err)
	}

	w.log.Info("SaveEmbeddingsToVectorDBActivity: Embeddings saved successfully")
	return nil
}

// UpdateEmbeddingMetadataActivity updates file metadata after embedding
// This is a single DB write operation - idempotent
func (w *Worker) UpdateEmbeddingMetadataActivity(ctx context.Context, param *UpdateEmbeddingMetadataActivityParam) error {
	w.log.Info("UpdateEmbeddingMetadataActivity: Updating file metadata",
		zap.String("fileUID", param.FileUID.String()))

	mdUpdate := repository.ExtraMetaData{
		EmbeddingPipe: param.Pipeline,
	}

	err := w.repository.UpdateKBFileMetadata(ctx, param.FileUID, mdUpdate)
	if err != nil {
		// If file not found, it may have been deleted during processing - this is OK
		if err.Error() == "record not found" || err.Error() == "fetching file: record not found" {
			w.log.Info("UpdateEmbeddingMetadataActivity: File not found (may have been deleted), skipping metadata update",
				zap.String("fileUID", param.FileUID.String()))
			return nil
		}
		return fmt.Errorf("failed to update file metadata: %w", err)
	}

	w.log.Info("UpdateEmbeddingMetadataActivity: Metadata updated successfully")
	return nil
}

// EmbedTextsActivity handles embedding a single batch of texts
func (w *Worker) EmbedTextsActivity(ctx context.Context, param *EmbedTextsActivityParam) ([][]float32, error) {
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
