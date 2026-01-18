package worker

import (
	"context"
	"errors"
	"fmt"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"
	"gorm.io/gorm"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/ai"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/pipeline"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
	filetype "github.com/instill-ai/x/file"
)

// This file contains embedding activities used by ProcessFileWorkflow:
// - EmbedAndSaveChunksActivity - Combined activity: queries chunks, generates embeddings, saves to DB/Milvus
// - UpdateEmbeddingMetadataActivity - Updates embedding metadata after processing

// Activity error type constants
const (
	embedAndSaveChunksActivityError      = "EmbedAndSaveChunksActivity"
	updateEmbeddingMetadataActivityError = "UpdateEmbeddingMetadataActivity"
)

// UpdateEmbeddingMetadataActivityParam updates metadata after embedding
type UpdateEmbeddingMetadataActivityParam struct {
	FileUID  types.FileUIDType // File unique identifier
	Pipeline string            // Pipeline used for embedding generation
}

// UpdateEmbeddingMetadataActivity updates file metadata after embedding
// This is a single DB write operation - idempotent
func (w *Worker) UpdateEmbeddingMetadataActivity(ctx context.Context, param *UpdateEmbeddingMetadataActivityParam) error {
	w.log.Info("UpdateEmbeddingMetadataActivity: Updating file metadata",
		zap.String("fileUID", param.FileUID.String()))

	mdUpdate := repository.ExtraMetaData{
		EmbeddingPipe: param.Pipeline,
	}

	err := w.repository.UpdateFileMetadata(ctx, param.FileUID, mdUpdate)
	if err != nil {
		// If file not found, it may have been deleted during processing - this is OK
		if errors.Is(err, gorm.ErrRecordNotFound) {
			w.log.Info("UpdateEmbeddingMetadataActivity: File not found (may have been deleted), skipping metadata update",
				zap.String("fileUID", param.FileUID.String()))
			return nil
		}
		err = errorsx.AddMessage(err, "Unable to update file metadata. Please try again.")
		return activityError(err, updateEmbeddingMetadataActivityError)
	}

	return nil
}

// ===== EMBED AND SAVE CHUNKS ACTIVITY =====
// Combined activity that queries chunks from DB, generates embeddings, and saves them
// This eliminates large data transfer between workflow and activities

// EmbedAndSaveChunksActivityParam defines parameters for EmbedAndSaveChunksActivity
type EmbedAndSaveChunksActivityParam struct {
	KBUID    types.KBUIDType   // Knowledge base unique identifier
	FileUID  types.FileUIDType // File unique identifier
	Metadata *structpb.Struct  // Request metadata for authentication
}

// EmbedAndSaveChunksActivityResult defines the result from EmbedAndSaveChunksActivity
type EmbedAndSaveChunksActivityResult struct {
	ChunkCount     int    // Number of chunks processed
	EmbeddingCount int    // Number of embeddings saved
	Pipeline       string // Pipeline used (e.g., "preset/indexing-embed@v1.0.0" for OpenAI, empty for AI client)
}

// EmbedAndSaveChunksActivity is a combined activity that:
// 1. Queries text chunks from DB
// 2. Generates embeddings using AI/ML models
// 3. Saves embeddings to vector DB and PostgreSQL
//
// This design eliminates large data transfer issues by keeping all data within the activity.
// The workflow only passes lightweight identifiers (KBUID, FileUID).
func (w *Worker) EmbedAndSaveChunksActivity(ctx context.Context, param *EmbedAndSaveChunksActivityParam) (*EmbedAndSaveChunksActivityResult, error) {
	w.log.Info("Starting EmbedAndSaveChunksActivity",
		zap.String("kbUID", param.KBUID.String()),
		zap.String("fileUID", param.FileUID.String()))

	result := &EmbedAndSaveChunksActivityResult{}

	// Step 1: Get file metadata (including soft-deleted files for dual processing)
	files, err := w.repository.GetFilesByFileUIDsIncludingDeleted(ctx, []types.FileUIDType{param.FileUID})
	if err != nil || len(files) == 0 {
		if err == nil {
			err = fmt.Errorf("no file found with UID %s", param.FileUID.String())
		}
		err = errorsx.AddMessage(err, "Unable to retrieve file information. Please try again.")
		return nil, activityError(err, embedAndSaveChunksActivityError)
	}
	file := files[0]

	// Step 2: Query text chunks from DB
	chunks, err := w.repository.ListTextChunksByKBFileUID(ctx, param.FileUID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			w.log.Info("No chunks found for file, skipping embedding",
				zap.String("fileUID", param.FileUID.String()))
			return result, nil
		}
		err = errorsx.AddMessage(err, "Unable to retrieve text chunks. Please try again.")
		return nil, activityError(err, embedAndSaveChunksActivityError)
	}

	if len(chunks) == 0 {
		w.log.Info("No chunks to embed for file",
			zap.String("fileUID", param.FileUID.String()))
		return result, nil
	}

	result.ChunkCount = len(chunks)
	w.log.Info("Retrieved chunks for embedding",
		zap.String("fileUID", param.FileUID.String()),
		zap.Int("chunkCount", len(chunks)))

	// Step 3: Load chunk text content from MinIO
	// StoragePath contains MinIO paths, we need to fetch the actual content
	texts := make([]string, len(chunks))
	bucket := config.Config.Minio.BucketName

	for i, chunk := range chunks {
		content, err := w.repository.GetMinIOStorage().GetFile(ctx, bucket, chunk.StoragePath)
		if err != nil {
			err = errorsx.AddMessage(
				fmt.Errorf("failed to load chunk content from MinIO: %v", err),
				"Unable to load chunk content for embedding. Please try again.",
			)
			return nil, activityError(err, embedAndSaveChunksActivityError)
		}
		texts[i] = string(content)
	}

	w.log.Info("Loaded chunk content from MinIO",
		zap.Int("chunkCount", len(texts)))

	// Step 4: Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var ctxErr error
		authCtx, ctxErr = CreateAuthenticatedContext(ctx, param.Metadata)
		if ctxErr != nil {
			w.log.Error("Failed to create authenticated context", zap.Error(ctxErr))
			return nil, activityErrorWithMessage(
				fmt.Sprintf("Failed to create authenticated context: %s", errorsx.MessageOrErr(ctxErr)),
				embedAndSaveChunksActivityError,
				ctxErr,
			)
		}
	}

	// Step 5: Fetch KB with system configuration (model family and dimensionality)
	kb, err := w.repository.GetKnowledgeBaseByUIDWithConfig(authCtx, param.KBUID)
	if err != nil {
		err = errorsx.AddMessage(
			fmt.Errorf("failed to fetch KB system config: %v", err),
			"Unable to retrieve knowledge base configuration. Please try again.",
		)
		return nil, activityError(err, embedAndSaveChunksActivityError)
	}

	// Step 6: Generate embeddings - route based on model family
	var vectors [][]float32
	taskType := "RETRIEVAL_DOCUMENT" // For indexing document chunks

	if kb.SystemConfig.RAG.Embedding.ModelFamily == "openai" {
		w.log.Info("Using OpenAI embedding pipeline route")

		if w.pipelineClient == nil {
			return nil, activityErrorWithMessage(
				"Pipeline client not configured for OpenAI embedding route",
				embedAndSaveChunksActivityError,
				fmt.Errorf("pipeline client is nil"))
		}

		vectors, err = pipeline.EmbedPipe(authCtx, w.pipelineClient, texts)
		if err != nil {
			w.log.Error("OpenAI embedding pipeline failed",
				zap.Int("textCount", len(texts)),
				zap.Error(err))
			err = errorsx.AddMessage(err, "Unable to generate embeddings using OpenAI pipeline. Please try again.")
			return nil, activityError(err, embedAndSaveChunksActivityError)
		}

		result.Pipeline = pipeline.EmbedPipeline.Name()
		w.log.Info("OpenAI embedding pipeline completed",
			zap.Int("vectorCount", len(vectors)),
			zap.String("pipeline", result.Pipeline))
	} else {
		w.log.Info("Using AI client route for embeddings",
			zap.String("modelFamily", kb.SystemConfig.RAG.Embedding.ModelFamily))

		vectors, err = ai.EmbedTexts(
			authCtx,
			w.aiClient,
			kb.SystemConfig.RAG.Embedding.ModelFamily,
			int32(kb.SystemConfig.RAG.Embedding.Dimensionality),
			texts,
			taskType,
		)
		if err != nil {
			w.log.Error("Failed to embed texts",
				zap.Int("textCount", len(texts)),
				zap.Error(err))
			err = errorsx.AddMessage(err, "Unable to generate embeddings. Please try again.")
			return nil, activityError(err, embedAndSaveChunksActivityError)
		}
	}

	w.log.Info("Embedding generation completed",
		zap.Int("vectorCount", len(vectors)))

	// Step 7: Build embedding records
	// Convert FileType enum string to MIME type
	fileTypeEnum := artifactpb.File_Type(artifactpb.File_Type_value[file.FileType])
	contentType := filetype.FileTypeToMimeType(fileTypeEnum)

	embeddings := make([]repository.EmbeddingModel, len(chunks))
	for i, chunk := range chunks {
		embeddings[i] = repository.EmbeddingModel{
			UID:              types.EmbeddingUIDType(uuid.Must(uuid.NewV4())),
			SourceTable:      repository.ChunkTableName,
			SourceUID:        chunk.UID,
			Vector:           vectors[i],
			KnowledgeBaseUID: param.KBUID,
			FileUID:          param.FileUID,
			ContentType:      contentType,
			ChunkType:        chunk.ChunkType,
			Tags:             file.Tags,
		}
	}

	// Step 8: Delete old embeddings from both VectorDB and DB
	w.log.Info("Deleting old embeddings",
		zap.String("fileUID", param.FileUID.String()))

	// Get collection name for vector DB operations
	activeCollectionUID := kb.ActiveCollectionUID
	if activeCollectionUID.IsNil() {
		err := errorsx.AddMessage(fmt.Errorf("active collection UID is nil"), "Knowledge base has no active collection. Please try again.")
		return nil, activityError(err, embedAndSaveChunksActivityError)
	}
	collection := constant.KBCollectionName(activeCollectionUID)

	// Delete from VectorDB
	if err := w.repository.DeleteEmbeddingsWithFileUID(ctx, collection, param.FileUID); err != nil {
		w.log.Error("Failed to delete old embeddings from VectorDB", zap.Error(err))
		return nil, activityErrorWithMessage(
			"Unable to delete old embeddings from vector database. Please try again.",
			embedAndSaveChunksActivityError,
			err,
		)
	}

	// Delete from PostgreSQL
	if err := w.repository.DeleteEmbeddingsByKBFileUID(ctx, param.FileUID); err != nil {
		w.log.Error("Failed to delete old embeddings from DB", zap.Error(err))
		return nil, activityErrorWithMessage(
			"Unable to delete old embeddings from database. Please try again.",
			embedAndSaveChunksActivityError,
			err,
		)
	}

	// Step 9: Save embeddings in batches
	batchSize := EmbeddingBatchSize
	totalBatches := (len(embeddings) + batchSize - 1) / batchSize
	w.log.Info("Saving embeddings in batches",
		zap.Int("totalEmbeddings", len(embeddings)),
		zap.Int("batchSize", batchSize),
		zap.Int("totalBatches", totalBatches))

	for i := range totalBatches {
		start := i * batchSize
		end := min(start+batchSize, len(embeddings))
		batch := embeddings[start:end]

		// Create callback to save to VectorDB within transaction
		externalServiceCall := func(insertedEmbeddings []repository.EmbeddingModel) error {
			vectors := make([]repository.VectorEmbedding, len(insertedEmbeddings))
			for j, emb := range insertedEmbeddings {
				// Find the corresponding chunk text for BM25 sparse vector generation
				// The batch offset maps to the original chunks array
				chunkIndex := start + j
				chunkText := ""
				if chunkIndex < len(texts) {
					chunkText = texts[chunkIndex]
				}

				vectors[j] = repository.VectorEmbedding{
					SourceTable:  emb.SourceTable,
					SourceUID:    emb.SourceUID.String(),
					EmbeddingUID: emb.UID.String(),
					Vector:       emb.Vector,
					FileUID:      emb.FileUID,
					FileDisplayName: file.DisplayName,
					ContentType:  emb.ContentType,
					ChunkType:    emb.ChunkType,
					Tags:         emb.Tags,
					Text:         chunkText, // Add text for BM25 sparse vector generation
				}
			}
			if err := w.repository.InsertVectorsInCollection(ctx, collection, vectors); err != nil {
				return fmt.Errorf("saving embeddings in vector db: %s", errorsx.MessageOrErr(err))
			}
			return nil
		}

		// Save to PostgreSQL with VectorDB callback
		if _, err := w.repository.CreateEmbeddings(ctx, batch, externalServiceCall); err != nil {
			w.log.Error("Failed to save embeddings",
				zap.Int("batchNumber", i+1),
				zap.Int("totalBatches", totalBatches),
				zap.Error(err))
			return nil, activityErrorWithMessage(
				fmt.Sprintf("Unable to save embedding batch %d/%d. Please try again.", i+1, totalBatches),
				embedAndSaveChunksActivityError,
				err,
			)
		}

		w.log.Info("Saved embedding batch",
			zap.Int("batchNumber", i+1),
			zap.Int("totalBatches", totalBatches),
			zap.Int("batchSize", len(batch)))
	}

	// Step 10: Flush collection to ensure immediate search availability
	w.log.Info("Flushing collection after embedding save")
	if err := w.repository.FlushCollection(ctx, collection); err != nil {
		w.log.Error("Failed to flush collection", zap.Error(err))
		return nil, activityErrorWithMessage(
			"Unable to flush vector database collection. Please try again.",
			embedAndSaveChunksActivityError,
			err,
		)
	}

	result.EmbeddingCount = len(embeddings)
	w.log.Info("EmbedAndSaveChunksActivity completed successfully",
		zap.String("fileUID", param.FileUID.String()),
		zap.Int("chunkCount", result.ChunkCount),
		zap.Int("embeddingCount", result.EmbeddingCount))

	return result, nil
}
