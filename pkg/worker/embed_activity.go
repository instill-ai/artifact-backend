package worker

import (
	"context"
	"errors"
	"fmt"

	"github.com/gofrs/uuid"
	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"
	"gorm.io/gorm"

	"github.com/instill-ai/artifact-backend/internal/ai"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

// This file contains embedding activities used by ProcessFileWorkflow and SaveEmbeddingsWorkflow:
// - GetTextChunksForEmbeddingActivity - Retrieves text chunk content for embedding generation
// - SaveEmbeddingBatchActivity - Saves embedding vectors to vector database in batches
// - DeleteOldEmbeddingsActivity - Removes outdated embeddings from both VectorDB and PostgreSQL
// - FlushCollectionActivity - Flushes vector database collection to ensure immediate search availability
// - UpdateEmbeddingMetadataActivity - Updates embedding metadata after processing

// Activity error type constants
const (
	getChunksForEmbeddingActivityError   = "GetChunksForEmbeddingActivity"
	saveEmbeddingsActivityError          = "SaveEmbeddingBatchActivity"
	deleteOldEmbeddingsActivityError     = "DeleteOldEmbeddingsActivity"
	flushCollectionActivityError         = "FlushCollectionActivity"
	updateEmbeddingMetadataActivityError = "UpdateEmbeddingMetadataActivity"
	embedTextsActivityError              = "EmbedTextsActivity"
)

// GetChunksForEmbeddingActivityParam retrieves text chunks for embedding generation
type GetChunksForEmbeddingActivityParam struct {
	FileUID types.FileUIDType // File unique identifier
}

// GetTextChunksForEmbeddingActivityResult contains text chunk data
type GetTextChunksForEmbeddingActivityResult struct {
	SourceTable string                      // Source table name (e.g., "converted_file")
	SourceUID   types.SourceUIDType         // Source record unique identifier
	Chunks      []repository.TextChunkModel // Text chunks with metadata
	Texts       []string                    // Text chunk content for embedding generation
	Metadata    *structpb.Struct            // External metadata from request
	FileName    string                      // File name for identification
	Tags        []string                    // File tags to propagate to embeddings
	ContentType string                      // MIME type of the file content (e.g., "text/markdown", "application/pdf")
}

// SaveEmbeddingBatchActivityParam saves a single batch of embeddings
type SaveEmbeddingBatchActivityParam struct {
	KBUID        types.KBUIDType             // Knowledge base unique identifier
	FileUID      types.FileUIDType           // File unique identifier
	FileName     string                      // File name for identification
	Embeddings   []repository.EmbeddingModel // Embeddings batch to save
	BatchNumber  int                         // Current batch number (1-based)
	TotalBatches int                         // Total number of batches
}

// DeleteOldEmbeddingsActivityParam for deleting old embeddings before batch save
type DeleteOldEmbeddingsActivityParam struct {
	KBUID   types.KBUIDType   // Knowledge base unique identifier
	FileUID types.FileUIDType // File unique identifier
}

// SaveEmbeddingsToDBActivityParam saves embedding metadata to DB
type SaveEmbeddingsToDBActivityParam struct {
	FileUID    types.FileUIDType           // File unique identifier
	Embeddings []repository.EmbeddingModel // Embeddings to save to database
}

// UpdateEmbeddingMetadataActivityParam updates metadata after embedding
type UpdateEmbeddingMetadataActivityParam struct {
	FileUID  types.FileUIDType // File unique identifier
	Pipeline string            // Pipeline used for embedding generation
}

// GetChunksForEmbeddingActivity retrieves text chunks and texts for embedding
// This is a DB read operation - idempotent
func (w *Worker) GetChunksForEmbeddingActivity(ctx context.Context, param *GetChunksForEmbeddingActivityParam) (*GetTextChunksForEmbeddingActivityResult, error) {
	w.log.Info("GetChunksForEmbeddingActivity: Fetching text chunks",
		zap.String("fileUID", param.FileUID.String()))

	// Get file
	files, err := w.repository.GetKnowledgeBaseFilesByFileUIDs(ctx, []types.FileUIDType{param.FileUID})
	if err != nil || len(files) == 0 {
		err = errorsx.AddMessage(err, "Unable to retrieve file information. Please try again.")
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			getChunksForEmbeddingActivityError,
			err,
		)
	}
	file := files[0]

	// Get text chunks by file
	sourceTable, sourceUID, chunks, _, texts, err := w.getTextChunksByFile(ctx, &file)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to retrieve text chunks. Please try again.")
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			getChunksForEmbeddingActivityError,
			err,
		)
	}

	w.log.Info("GetChunksForEmbeddingActivity: Text chunks retrieved",
		zap.Int("chunkCount", len(chunks)),
		zap.Int("textCount", len(texts)))

	// Convert FileType enum string to MIME type
	// FileType in DB is stored as enum string (e.g., "TYPE_PDF", "TYPE_TEXT")
	fileTypeEnum := artifactpb.File_Type(artifactpb.File_Type_value[file.FileType])
	contentType := ai.FileTypeToMIME(fileTypeEnum)

	return &GetTextChunksForEmbeddingActivityResult{
		SourceTable: sourceTable,
		SourceUID:   sourceUID,
		Chunks:      chunks,
		Texts:       texts,
		Metadata:    file.ExternalMetadataUnmarshal,
		FileName:    file.Name,
		Tags:        file.Tags,
		ContentType: contentType,
	}, nil
}

// SaveEmbeddingBatchActivity saves a single batch of embeddings to vector db and database
// This is designed for parallel execution - each batch is independent
func (w *Worker) SaveEmbeddingBatchActivity(ctx context.Context, param *SaveEmbeddingBatchActivityParam) error {
	w.log.Info("SaveEmbeddingBatchActivity: Saving batch",
		zap.String("kbUID", param.KBUID.String()),
		zap.Int("batchNumber", param.BatchNumber),
		zap.Int("totalBatches", param.TotalBatches),
		zap.Int("embeddingCount", len(param.Embeddings)))

	if len(param.Embeddings) == 0 {
		w.log.Warn("SaveEmbeddingBatchActivity: Empty batch, skipping")
		return nil
	}

	// CRITICAL: Query active_collection_uid from database
	// A KB's active collection may be different from its own UID (e.g., during updates)
	kb, err := w.repository.GetKnowledgeBaseByUID(ctx, param.KBUID)
	if err != nil {
		return temporal.NewApplicationErrorWithCause(
			"Unable to get knowledge base for embedding save",
			saveEmbeddingsActivityError,
			err,
		)
	}

	collectionUID := param.KBUID
	if kb.ActiveCollectionUID != uuid.Nil {
		collectionUID = kb.ActiveCollectionUID
	}

	w.log.Info("SaveEmbeddingBatchActivity: Using active collection",
		zap.String("kbUID", param.KBUID.String()),
		zap.String("activeCollectionUID", collectionUID.String()))

	// Build vectors for vector db
	collection := constant.KBCollectionName(collectionUID)

	externalServiceCall := func(insertedEmbeddings []repository.EmbeddingModel) error {
		// Convert repository.Embedding to repository.VectorEmbedding for vector DB
		vectors := make([]repository.VectorEmbedding, len(insertedEmbeddings))
		for j, emb := range insertedEmbeddings {
			vectors[j] = repository.VectorEmbedding{
				SourceTable:  emb.SourceTable,
				SourceUID:    emb.SourceUID.String(),
				EmbeddingUID: emb.UID.String(),
				Vector:       emb.Vector,
				FileUID:      emb.KBFileUID,
				FileName:     param.FileName,
				ContentType:  emb.ContentType,
				ChunkType:    emb.ChunkType,
				Tags:         emb.Tags,
			}
		}
		// Insert embeddings into vector database
		if err := w.repository.InsertVectorsInCollection(ctx, collection, vectors); err != nil {
			return fmt.Errorf("saving embeddings in vector db: %s", errorsx.MessageOrErr(err))
		}
		return nil
	}

	// Insert embeddings in transaction
	_, err = w.repository.CreateEmbeddings(ctx, param.Embeddings, externalServiceCall)
	if err != nil {
		err = errorsx.AddMessage(err, fmt.Sprintf("Unable to save embeddings (batch %d/%d). Please try again.", param.BatchNumber, param.TotalBatches))
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			saveEmbeddingsActivityError,
			err,
		)
	}

	return nil
}

// DeleteOldEmbeddingsActivity deletes embeddings from both VectorDB and PostgreSQL for a file
// This combines VectorDB and DB deletion into a single activity for symmetric workflow structure
func (w *Worker) DeleteOldEmbeddingsActivity(ctx context.Context, param *DeleteOldEmbeddingsActivityParam) error {
	w.log.Info("DeleteOldEmbeddingsActivity: Starting cleanup",
		zap.String("kbUID", param.KBUID.String()),
		zap.String("fileUID", param.FileUID.String()))

	// Query active_collection_uid from database (don't use KB UID directly)
	kb, err := w.repository.GetKnowledgeBaseByUID(ctx, param.KBUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to retrieve knowledge base configuration. Please try again.")
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			deleteOldEmbeddingsActivityError,
			err,
		)
	}

	activeCollectionUID := kb.ActiveCollectionUID
	if activeCollectionUID.IsNil() {
		err = errorsx.AddMessage(fmt.Errorf("active collection UID is nil"), "Knowledge base has no active collection. Please try again.")
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			deleteOldEmbeddingsActivityError,
			err,
		)
	}

	collection := constant.KBCollectionName(activeCollectionUID)
	w.log.Info("DeleteOldEmbeddingsActivity: Using active collection",
		zap.String("kbUID", param.KBUID.String()),
		zap.String("activeCollectionUID", activeCollectionUID.String()))

	// Step 1: Delete from VectorDB (Milvus)
	if err := w.repository.DeleteEmbeddingsWithFileUID(ctx, collection, param.FileUID); err != nil {
		err = errorsx.AddMessage(err, "Unable to delete old embeddings from vector database. Please try again.")
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			deleteOldEmbeddingsActivityError,
			err,
		)
	}

	// Step 2: Delete from PostgreSQL
	if err := w.repository.DeleteEmbeddingsByKBFileUID(ctx, param.FileUID); err != nil {
		err = errorsx.AddMessage(err, "Unable to delete old embedding records. Please try again.")
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			deleteOldEmbeddingsActivityError,
			err,
		)
	}

	w.log.Info("DeleteOldEmbeddingsActivity: Successfully deleted embeddings",
		zap.String("kbUID", param.KBUID.String()),
		zap.String("fileUID", param.FileUID.String()))

	return nil
}

// FlushCollectionActivity flushes a vector db collection to persist all data immediately
// This is called once at the end after all batches are saved
func (w *Worker) FlushCollectionActivity(ctx context.Context, param *DeleteOldEmbeddingsActivityParam) error {
	w.log.Info("FlushCollectionActivity: Flushing collection",
		zap.String("kbUID", param.KBUID.String()))

	// Query active_collection_uid from database (don't use KB UID directly)
	kb, err := w.repository.GetKnowledgeBaseByUID(ctx, param.KBUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to retrieve knowledge base configuration. Please try again.")
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			flushCollectionActivityError,
			err,
		)
	}

	activeCollectionUID := kb.ActiveCollectionUID
	if activeCollectionUID.IsNil() {
		err = errorsx.AddMessage(fmt.Errorf("active collection UID is nil"), "Knowledge base has no active collection. Please try again.")
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			flushCollectionActivityError,
			err,
		)
	}

	collection := constant.KBCollectionName(activeCollectionUID)
	w.log.Info("FlushCollectionActivity: Using active collection",
		zap.String("kbUID", param.KBUID.String()),
		zap.String("activeCollectionUID", activeCollectionUID.String()))

	if err := w.repository.FlushCollection(ctx, collection); err != nil {
		err = errorsx.AddMessage(err, "Unable to flush vector database collection. Please try again.")
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			flushCollectionActivityError,
			err,
		)
	}

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

	err := w.repository.UpdateKnowledgeFileMetadata(ctx, param.FileUID, mdUpdate)
	if err != nil {
		// If file not found, it may have been deleted during processing - this is OK
		if errors.Is(err, gorm.ErrRecordNotFound) {
			w.log.Info("UpdateEmbeddingMetadataActivity: File not found (may have been deleted), skipping metadata update",
				zap.String("fileUID", param.FileUID.String()))
			return nil
		}
		err = errorsx.AddMessage(err, "Unable to update file metadata. Please try again.")
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			updateEmbeddingMetadataActivityError,
			err,
		)
	}

	return nil
}

// ===== EMBED TEXTS ACTIVITY =====

// EmbedTextsActivityParam defines parameters for EmbedTextsActivity
type EmbedTextsActivityParam struct {
	KBUID    *types.KBUIDType // Optional: Knowledge base UID for client selection
	Texts    []string         // Texts to embed
	TaskType string           // Task type for embedding optimization (e.g., "RETRIEVAL_DOCUMENT", "RETRIEVAL_QUERY")
}

// EmbedTextsActivity handles embedding texts for workflow use
// NOTE: This activity is ONLY for use within workflows (like ProcessFileWorkflow).
// For synchronous user requests (retrieval/QA), use service.EmbedTexts() directly.
func (w *Worker) EmbedTextsActivity(ctx context.Context, param *EmbedTextsActivityParam) ([][]float32, error) {
	w.log.Info("Starting EmbedTextsActivity",
		zap.Int("textCount", len(param.Texts)),
		zap.String("taskType", param.TaskType))

	if len(param.Texts) == 0 {
		return [][]float32{}, nil
	}

	// Use the specified task type for optimal embedding generation
	// Select client based on KB's embedding_config if KBUID is provided
	vectors, err := w.embedTextsWithKBConfig(ctx, param.KBUID, param.Texts, param.TaskType)
	if err != nil {
		w.log.Error("Failed to embed texts",
			zap.Int("textCount", len(param.Texts)),
			zap.String("taskType", param.TaskType),
			zap.Error(err))
		err = errorsx.AddMessage(err, "Unable to generate embeddings. Please try again.")
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			embedTextsActivityError,
			err,
		)
	}

	w.log.Info("Embedding completed",
		zap.Int("vectorCount", len(vectors)),
		zap.String("taskType", param.TaskType))

	return vectors, nil
}

// embedTextsWithKBConfig generates embeddings using the appropriate client
// based on the KB's embedding configuration. This delegates to the shared helper in the ai package.
func (w *Worker) embedTextsWithKBConfig(ctx context.Context, kbUID *types.KBUIDType, texts []string, taskType string) ([][]float32, error) {
	if kbUID == nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("KB UID is required for embedding"),
			"Knowledge base context is required for query processing.",
		)
	}

	// Use GetKnowledgeBaseByUID which doesn't filter by delete_time, as we need to support
	// embedding activities that are still running for staging KBs that have been soft-deleted
	kb, err := w.repository.GetKnowledgeBaseByUID(ctx, *kbUID)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to fetch KB embedding config: %w", err),
			"Unable to retrieve knowledge base configuration. Please try again.",
		)
	}

	// Delegate to shared helper
	vectors, err := ai.EmbedTexts(
		ctx,
		w.aiClient,
		kb.EmbeddingConfig.ModelFamily,
		int32(kb.EmbeddingConfig.Dimensionality),
		texts,
		taskType,
	)
	if err != nil {
		return nil, err
	}

	w.log.Info("Embedded texts using client routing",
		zap.Int("textCount", len(texts)),
		zap.String("taskType", taskType))

	return vectors, nil
}
