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

	"github.com/instill-ai/artifact-backend/pkg/ai"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/pipeline"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

// This file contains embedding activities used by ProcessFileWorkflow and SaveEmbeddingsWorkflow:
// - GetChunksForEmbeddingActivity - Retrieves text chunk content for embedding generation
// - EmbedTextsActivity - Generates embeddings using AI/ML models
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
	Filename    string                      // File name for identification
	Tags        []string                    // File tags to propagate to embeddings
	ContentType string                      // MIME type of the file content (e.g., "text/markdown", "application/pdf")
}

// SaveEmbeddingBatchActivityParam saves a single batch of embeddings
type SaveEmbeddingBatchActivityParam struct {
	KBUID        types.KBUIDType             // Knowledge base unique identifier
	FileUID      types.FileUIDType           // File unique identifier
	Filename     string                      // File name for identification
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

	// CRITICAL: Get file INCLUDING soft-deleted files
	// During dual processing, a file might be soft-deleted (via dual deletion) while its
	// SaveEmbeddingsWorkflow is still queued/running. We MUST still generate embeddings
	// for the chunks that already exist, even if the file has been marked for deletion.
	// The chunks will be cleaned up later by CleanupFileWorkflow.
	files, err := w.repository.GetKnowledgeBaseFilesByFileUIDsIncludingDeleted(ctx, []types.FileUIDType{param.FileUID})
	if err != nil || len(files) == 0 {
		if err == nil {
			err = fmt.Errorf("no file found with UID %s", param.FileUID.String())
		}
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
		Filename:    file.Filename,
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
	// CRITICAL: Must include soft-deleted KBs because SaveEmbeddingsWorkflow may run
	// after the staging KB has been soft-deleted during swap
	kb, err := w.repository.GetKnowledgeBaseByUIDIncludingDeleted(ctx, param.KBUID)
	if err != nil {
		return temporal.NewApplicationErrorWithCause(
			"Unable to get knowledge base for embedding save",
			saveEmbeddingsActivityError,
			err,
		)
	}

	// CRITICAL FIX 3: active_collection_uid must ALWAYS be set and NEVER be nil
	// No fallback to param.KBUID - that would query the wrong collection
	if kb.ActiveCollectionUID == uuid.Nil {
		return temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("KB has nil active_collection_uid: %s. This should never happen after migration 000044.", param.KBUID),
			saveEmbeddingsActivityError,
			fmt.Errorf("nil active_collection_uid for KB %s", param.KBUID),
		)
	}

	collection := constant.KBCollectionName(kb.ActiveCollectionUID)

	w.log.Info("SaveEmbeddingBatchActivity: Using active collection",
		zap.String("kbUID", param.KBUID.String()),
		zap.String("activeCollectionUID", kb.ActiveCollectionUID.String()),
		zap.String("collectionName", collection))

	// CRITICAL: Validate collection exists before attempting to insert embeddings
	// If missing, auto-recreate it for self-healing behavior
	collectionExists, err := w.repository.CollectionExists(ctx, collection)
	if err != nil {
		return temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to verify collection existence: %s", collection),
			saveEmbeddingsActivityError,
			err,
		)
	}
	if !collectionExists {
		w.log.Warn("Collection does not exist in Milvus, attempting to recreate",
			zap.String("collection", collection),
			zap.String("kbUID", param.KBUID.String()),
			zap.String("activeCollectionUID", kb.ActiveCollectionUID.String()))

		// Self-healing: Recreate the missing collection
		// Get the system config to retrieve dimensionality
		system, err := w.repository.GetSystemByUID(ctx, kb.SystemUID)
		if err != nil {
			return temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to get system config for collection recreation: KB UID %s, System UID %s", param.KBUID, kb.SystemUID),
				saveEmbeddingsActivityError,
				err,
			)
		}

		systemConfig, err := system.GetConfigJSON()
		if err != nil {
			return temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to parse system config for collection recreation: KB UID %s", param.KBUID),
				saveEmbeddingsActivityError,
				err,
			)
		}

		w.log.Info("Recreating missing collection",
			zap.String("collection", collection),
			zap.String("modelFamily", systemConfig.RAG.Embedding.ModelFamily),
			zap.Uint32("dimensionality", systemConfig.RAG.Embedding.Dimensionality))

		// Recreate the collection with the correct dimensionality
		err = w.repository.CreateCollection(ctx, collection, systemConfig.RAG.Embedding.Dimensionality)
		if err != nil {
			return temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to recreate missing collection: %s (KB UID: %s, Collection UID: %s). Original error: %v", collection, param.KBUID, kb.ActiveCollectionUID, err),
				saveEmbeddingsActivityError,
				err,
			)
		}

		w.log.Info("Successfully recreated missing collection",
			zap.String("collection", collection),
			zap.String("kbUID", param.KBUID.String()))
	}

	externalServiceCall := func(insertedEmbeddings []repository.EmbeddingModel) error {
		// Convert repository.Embedding to repository.VectorEmbedding for vector DB
		vectors := make([]repository.VectorEmbedding, len(insertedEmbeddings))
		for j, emb := range insertedEmbeddings {
			vectors[j] = repository.VectorEmbedding{
				SourceTable:  emb.SourceTable,
				SourceUID:    emb.SourceUID.String(),
				EmbeddingUID: emb.UID.String(),
				Vector:       emb.Vector,
				FileUID:      emb.FileUID,
				Filename:     param.Filename,
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
	// CRITICAL: Must include soft-deleted KBs because DeleteOldEmbeddingsActivity may run
	// after the staging KB has been soft-deleted during swap
	kb, err := w.repository.GetKnowledgeBaseByUIDIncludingDeleted(ctx, param.KBUID)
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
		err := errorsx.AddMessage(fmt.Errorf("active collection UID is nil"), "Knowledge base has no active collection. Please try again.")
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
	// CRITICAL: Must include soft-deleted KBs because FlushCollectionActivity may run
	// after the staging KB has been soft-deleted during swap
	kb, err := w.repository.GetKnowledgeBaseByUIDIncludingDeleted(ctx, param.KBUID)
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
		err := errorsx.AddMessage(fmt.Errorf("active collection UID is nil"), "Knowledge base has no active collection. Please try again.")
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

	// Check if collection exists before flushing, recreate if missing (self-healing)
	collectionExists, err := w.repository.CollectionExists(ctx, collection)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to verify collection existence. Please try again.")
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			flushCollectionActivityError,
			err,
		)
	}

	if !collectionExists {
		w.log.Warn("Collection does not exist during flush, attempting to recreate",
			zap.String("collection", collection),
			zap.String("kbUID", param.KBUID.String()),
			zap.String("activeCollectionUID", activeCollectionUID.String()))

		// Self-healing: Recreate the missing collection
		system, err := w.repository.GetSystemByUID(ctx, kb.SystemUID)
		if err != nil {
			err = errorsx.AddMessage(err, "Unable to get system config for collection recreation. Please try again.")
			return temporal.NewApplicationErrorWithCause(
				errorsx.MessageOrErr(err),
				flushCollectionActivityError,
				err,
			)
		}

		systemConfig, err := system.GetConfigJSON()
		if err != nil {
			err = errorsx.AddMessage(err, "Unable to parse system config for collection recreation. Please try again.")
			return temporal.NewApplicationErrorWithCause(
				errorsx.MessageOrErr(err),
				flushCollectionActivityError,
				err,
			)
		}

		w.log.Info("Recreating missing collection before flush",
			zap.String("collection", collection),
			zap.String("modelFamily", systemConfig.RAG.Embedding.ModelFamily),
			zap.Uint32("dimensionality", systemConfig.RAG.Embedding.Dimensionality))

		err = w.repository.CreateCollection(ctx, collection, systemConfig.RAG.Embedding.Dimensionality)
		if err != nil {
			err = errorsx.AddMessage(err, "Unable to recreate missing collection. Please try again.")
			return temporal.NewApplicationErrorWithCause(
				errorsx.MessageOrErr(err),
				flushCollectionActivityError,
				err,
			)
		}

		w.log.Info("Successfully recreated missing collection",
			zap.String("collection", collection),
			zap.String("kbUID", param.KBUID.String()))
	}

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
	Metadata *structpb.Struct // Request metadata for authentication
}

// EmbedTextsActivityResult defines the result from EmbedTextsActivity
type EmbedTextsActivityResult struct {
	Vectors  [][]float32 // Generated embedding vectors
	Pipeline string      // Pipeline used (e.g., "preset/indexing-embed@v1.0.0" for OpenAI route, empty for AI client)
}

// EmbedTextsActivity handles embedding texts for workflow use
// NOTE: This activity is ONLY for use within workflows (like ProcessFileWorkflow).
//
// RAG Phase: INDEXING - Used during document ingestion to embed chunks for storage in vector DB
// Typical task type: "RETRIEVAL_DOCUMENT" (optimizes embeddings to be stored and retrieved)
//
// For RAG RETRIEVAL phase (embedding user queries), use service.EmbedTexts() directly.
func (w *Worker) EmbedTextsActivity(ctx context.Context, param *EmbedTextsActivityParam) (*EmbedTextsActivityResult, error) {
	w.log.Info("Starting EmbedTextsActivity",
		zap.Int("textCount", len(param.Texts)),
		zap.String("taskType", param.TaskType))

	result := &EmbedTextsActivityResult{}

	if len(param.Texts) == 0 {
		result.Vectors = [][]float32{}
		return result, nil
	}

	// Validate KB UID is provided
	if param.KBUID == nil {
		err := errorsx.AddMessage(
			fmt.Errorf("KB UID is required for embedding"),
			"Knowledge base context is required for query processing.",
		)
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			embedTextsActivityError,
			err,
		)
	}

	// Create authenticated context if metadata provided
	// This is required for calling pipeline-backend with proper authentication
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = CreateAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			w.log.Error("Failed to create authenticated context", zap.Error(err))
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to create authenticated context: %s", errorsx.MessageOrErr(err)),
				embedTextsActivityError,
				err,
			)
		}
	}

	// Fetch KB with system configuration (model family and dimensionality)
	// Use GetKnowledgeBaseByUIDWithConfig which doesn't filter by delete_time, as we need to support
	// embedding activities that are still running for staging KBs that have been soft-deleted
	kb, err := w.repository.GetKnowledgeBaseByUIDWithConfig(authCtx, *param.KBUID)
	if err != nil {
		err = errorsx.AddMessage(
			fmt.Errorf("failed to fetch KB system config: %w", err),
			"Unable to retrieve knowledge base configuration. Please try again.",
		)
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			embedTextsActivityError,
			err,
		)
	}

	// Generate embeddings - route based on model family
	var vectors [][]float32

	if kb.SystemConfig.RAG.Embedding.ModelFamily == "openai" {
		// Use OpenAI embedding pipeline
		w.log.Info("Using OpenAI embedding pipeline route")

		if w.pipelineClient == nil {
			return nil, temporal.NewApplicationErrorWithCause(
				"Pipeline client not configured for OpenAI embedding route",
				embedTextsActivityError,
				fmt.Errorf("pipeline client is nil"))
		}

		// Use authenticated context for pipeline call
		vectors, err = pipeline.EmbedPipe(authCtx, w.pipelineClient, param.Texts)
		if err != nil {
			w.log.Error("OpenAI embedding pipeline failed",
				zap.Int("textCount", len(param.Texts)),
				zap.String("taskType", param.TaskType),
				zap.Error(err))
			err = errorsx.AddMessage(err, "Unable to generate embeddings using OpenAI pipeline. Please try again.")
			return nil, temporal.NewApplicationErrorWithCause(
				errorsx.MessageOrErr(err),
				embedTextsActivityError,
				err,
			)
		}

		// Record the pipeline used
		result.Pipeline = pipeline.EmbedPipeline.Name()

		w.log.Info("OpenAI embedding pipeline completed",
			zap.Int("vectorCount", len(vectors)),
			zap.String("pipeline", result.Pipeline))

	} else {
		// Use AI client (Gemini) for other model families
		w.log.Info("Using AI client route for embeddings",
			zap.String("modelFamily", kb.SystemConfig.RAG.Embedding.ModelFamily))

		// Use authenticated context for AI client call
		vectors, err = ai.EmbedTexts(
			authCtx,
			w.aiClient,
			kb.SystemConfig.RAG.Embedding.ModelFamily,
			int32(kb.SystemConfig.RAG.Embedding.Dimensionality),
			param.Texts,
			param.TaskType,
		)
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

		// No pipeline used (AI client), leave result.Pipeline empty
	}

	result.Vectors = vectors

	w.log.Info("Embedding completed",
		zap.Int("vectorCount", len(vectors)),
		zap.String("taskType", param.TaskType))

	return result, nil
}
