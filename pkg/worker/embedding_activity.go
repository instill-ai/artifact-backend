package worker

import (
	"context"
	"fmt"

	"github.com/gofrs/uuid"
	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"

	errorsx "github.com/instill-ai/x/errors"
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

// SaveEmbeddingsToVectorDBWorkflowParam saves embeddings to vector db
type SaveEmbeddingsToVectorDBWorkflowParam struct {
	KnowledgeBaseUID uuid.UUID
	FileUID          uuid.UUID
	FileName         string
	Embeddings       []repository.Embedding
}

// SaveEmbeddingBatchActivityParam saves a single batch of embeddings
type SaveEmbeddingBatchActivityParam struct {
	KnowledgeBaseUID uuid.UUID
	FileUID          uuid.UUID
	FileName         string
	Embeddings       []repository.Embedding
	BatchNumber      int
	TotalBatches     int
}

// DeleteOldEmbeddingsActivityParam for deleting old embeddings before batch save
type DeleteOldEmbeddingsActivityParam struct {
	KnowledgeBaseUID uuid.UUID
	FileUID          uuid.UUID
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
	files, err := w.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{param.FileUID})
	if err != nil || len(files) == 0 {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to get file: %s", errorsx.MessageOrErr(err)),
			getChunksForEmbeddingActivityError,
			err,
		)
	}
	file := files[0]

	// Get chunks by file
	sourceTable, sourceUID, chunks, _, texts, err := w.service.GetChunksByFile(ctx, &file)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to retrieve chunks: %s", errorsx.MessageOrErr(err)),
			getChunksForEmbeddingActivityError,
			err,
		)
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
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to create authenticated context: %s", errorsx.MessageOrErr(err)),
				generateEmbeddingsActivityError,
				err,
			)
		}
	}

	// Call the embedding pipeline batch method - THIS IS THE KEY EXTERNAL CALL
	// Note: We use EmbeddingTextBatch instead of EmbeddingTextPipe to avoid nested workflows
	embeddings, err := w.service.EmbeddingTextBatch(authCtx, param.Texts)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Embedding generation failed: %s", errorsx.MessageOrErr(err)),
			generateEmbeddingsActivityError,
			err,
		)
	}

	w.log.Info("GenerateEmbeddingsActivity: Embeddings generated successfully",
		zap.Int("embeddingCount", len(embeddings)))

	return &GenerateEmbeddingsActivityResult{
		Embeddings: embeddings,
	}, nil
}

// SaveEmbeddingBatchActivity saves a single batch of embeddings to vector db and database
// This is designed for parallel execution - each batch is independent
func (w *Worker) SaveEmbeddingBatchActivity(ctx context.Context, param *SaveEmbeddingBatchActivityParam) error {
	w.log.Info("SaveEmbeddingBatchActivity: Saving batch",
		zap.String("kbUID", param.KnowledgeBaseUID.String()),
		zap.Int("batchNumber", param.BatchNumber),
		zap.Int("totalBatches", param.TotalBatches),
		zap.Int("embeddingCount", len(param.Embeddings)))

	if len(param.Embeddings) == 0 {
		w.log.Warn("SaveEmbeddingBatchActivity: Empty batch, skipping")
		return nil
	}

	// Build vectors for vector db
	collection := service.KBCollectionName(param.KnowledgeBaseUID)

	externalServiceCall := func(insertedEmbeddings []repository.Embedding) error {
		vectors := make([]service.Embedding, len(insertedEmbeddings))
		for j, emb := range insertedEmbeddings {
			vectors[j] = service.Embedding{
				SourceTable:  emb.SourceTable,
				SourceUID:    emb.SourceUID.String(),
				EmbeddingUID: emb.UID.String(),
				Vector:       emb.Vector,
				FileUID:      emb.KbFileUID,
				FileName:     param.FileName,
				FileType:     emb.FileType,
				ContentType:  emb.ContentType,
			}
		}
		if err := w.service.VectorDB().InsertVectorsInCollection(ctx, collection, vectors); err != nil {
			return fmt.Errorf("saving embeddings in vector db: %s", errorsx.MessageOrErr(err))
		}
		return nil
	}

	// Insert embeddings in transaction
	_, err := w.service.Repository().CreateEmbeddings(ctx, param.Embeddings, externalServiceCall)
	if err != nil {
		return temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to save batch %d/%d in vector db: %s", param.BatchNumber, param.TotalBatches, errorsx.MessageOrErr(err)),
			saveEmbeddingsActivityError,
			err,
		)
	}

	w.log.Info("SaveEmbeddingBatchActivity: Batch saved successfully in vector db",
		zap.Int("batchNumber", param.BatchNumber))
	return nil
}

// DeleteOldEmbeddingsFromVectorDBActivity deletes embeddings from vector db for a file
// This is used by the concurrent embedding workflow - idempotent
func (w *Worker) DeleteOldEmbeddingsFromVectorDBActivity(ctx context.Context, param *DeleteOldEmbeddingsActivityParam) error {
	w.log.Info("DeleteOldEmbeddingsFromVectorDBActivity: Deleting embeddings from vector db",
		zap.String("kbUID", param.KnowledgeBaseUID.String()),
		zap.String("fileUID", param.FileUID.String()))

	collection := service.KBCollectionName(param.KnowledgeBaseUID)

	if err := w.service.VectorDB().DeleteEmbeddingsWithFileUID(ctx, collection, param.FileUID); err != nil {
		return temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to delete embeddings from vector db: %s", errorsx.MessageOrErr(err)),
			saveEmbeddingsActivityError,
			err,
		)
	}

	w.log.Info("DeleteOldEmbeddingsFromVectorDBActivity: Embeddings deleted from vector db successfully in vector db")
	return nil
}

// DeleteOldEmbeddingsFromDBActivity deletes embeddings from PostgreSQL for a file
// This is used by the concurrent embedding workflow - idempotent
func (w *Worker) DeleteOldEmbeddingsFromDBActivity(ctx context.Context, param *DeleteOldEmbeddingsActivityParam) error {
	w.log.Info("DeleteOldEmbeddingsFromDBActivity: Deleting embeddings from DB",
		zap.String("fileUID", param.FileUID.String()))

	if err := w.service.Repository().DeleteEmbeddingsByKbFileUID(ctx, param.FileUID); err != nil {
		return temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to delete embeddings from DB: %s", errorsx.MessageOrErr(err)),
			saveEmbeddingsActivityError,
			err,
		)
	}

	w.log.Info("DeleteOldEmbeddingsFromDBActivity: Embeddings deleted from DB successfully")
	return nil
}

// FlushCollectionActivity flushes a vector db collection to persist all data immediately
// This is called once at the end after all batches are saved
func (w *Worker) FlushCollectionActivity(ctx context.Context, param *DeleteOldEmbeddingsActivityParam) error {
	w.log.Info("FlushCollectionActivity: Flushing collection",
		zap.String("kbUID", param.KnowledgeBaseUID.String()))

	collection := service.KBCollectionName(param.KnowledgeBaseUID)

	if err := w.service.VectorDB().FlushCollection(ctx, collection); err != nil {
		return temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to flush collection from vector db: %s", errorsx.MessageOrErr(err)),
			flushCollectionActivityError,
			err,
		)
	}

	w.log.Info("FlushCollectionActivity: Collection flushed from vector db successfully")
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

	err := w.service.Repository().UpdateKBFileMetadata(ctx, param.FileUID, mdUpdate)
	if err != nil {
		// If file not found, it may have been deleted during processing - this is OK
		if err.Error() == "record not found" || err.Error() == "fetching file: record not found" {
			w.log.Info("UpdateEmbeddingMetadataActivity: File not found (may have been deleted), skipping metadata update",
				zap.String("fileUID", param.FileUID.String()))
			return nil
		}
		return temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to update file metadata from DB: %s", errorsx.MessageOrErr(err)),
			updateEmbeddingMetadataActivityError,
			err,
		)
	}

	w.log.Info("UpdateEmbeddingMetadataActivity: Metadata updated from DB successfully")
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
		w.log.Error("Failed to embed text batch in vector db",
			zap.Int("batchIndex", param.BatchIndex),
			zap.Int("batchSize", len(param.Texts)),
			zap.Error(err))
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Embedding batch %d failed in vector db: %s", param.BatchIndex, errorsx.MessageOrErr(err)),
			embedTextsActivityError,
			err,
		)
	}

	w.log.Info("Batch embedding completed in vector db",
		zap.Int("batchIndex", param.BatchIndex),
		zap.Int("vectorCount", len(vectors)))

	return vectors, nil
}

// Activity error type constants
const (
	getChunksForEmbeddingActivityError   = "GetChunksForEmbeddingActivity"
	generateEmbeddingsActivityError      = "GenerateEmbeddingsActivity"
	saveEmbeddingsActivityError          = "SaveEmbeddingBatchActivity"
	flushCollectionActivityError         = "FlushCollectionActivity"
	updateEmbeddingMetadataActivityError = "UpdateEmbeddingMetadataActivity"
	embedTextsActivityError              = "EmbedTextsActivity"
)
