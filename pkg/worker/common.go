package worker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"

	logx "github.com/instill-ai/x/log"
)

// getFileByUID is a helper function to retrieve a single file by UID.
// It returns the file or an error if not found.
func getFileByUID(ctx context.Context, repo repository.RepositoryI, fileUID uuid.UUID) (repository.KnowledgeBaseFile, error) {
	files, err := repo.GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{fileUID})
	if err != nil {
		return repository.KnowledgeBaseFile{}, fmt.Errorf("failed to get file: %w", err)
	}
	if len(files) == 0 {
		return repository.KnowledgeBaseFile{}, fmt.Errorf("file not found: %s", fileUID.String())
	}
	return files[0], nil
}

func saveConvertedFile(ctx context.Context, svc service.Service, kbUID, fileUID uuid.UUID, name string, conversion *service.MDConversionResult) error {
	saveToMinIO := func(convertedFileUID uuid.UUID) (string, error) {
		blobStorage := svc.MinIO()
		dest, err := blobStorage.SaveConvertedFile(ctx, kbUID, fileUID, convertedFileUID, "md", []byte(conversion.Markdown))
		if err != nil {
			return "", fmt.Errorf("storing converted file as blob: %w", err)
		}

		return dest, nil
	}

	convertedFile := repository.ConvertedFile{
		KbUID:        kbUID,
		FileUID:      fileUID,
		Name:         name,
		Type:         "text/markdown",
		Destination:  "destination",
		PositionData: conversion.PositionData,
	}
	if _, err := svc.Repository().CreateConvertedFile(ctx, convertedFile, saveToMinIO); err != nil {
		return fmt.Errorf("storing converted file in repository: %w", err)
	}

	return nil
}

// saveChunks saves chunks into object storage and updates the metadata in the database.
func saveChunks(
	ctx context.Context,
	svc service.Service,
	kbUID, kbFileUID, sourceUID uuid.UUID,
	sourceTable string,
	summaryChunks, contentChunks []service.Chunk,
	fileType string,
) error {
	textChunks := make([]*repository.TextChunk, len(summaryChunks)+len(contentChunks))
	texts := make([]string, len(summaryChunks)+len(contentChunks))

	for i, c := range summaryChunks {
		textChunks[i] = &repository.TextChunk{
			SourceUID:   sourceUID,
			SourceTable: sourceTable,
			StartPos:    0,
			EndPos:      0,
			ContentDest: "not set yet because we need to save the chunks in db to get the uid",
			Tokens:      c.Tokens,
			Retrievable: true,
			InOrder:     i,
			KbUID:       kbUID,
			KbFileUID:   kbFileUID,
			FileType:    fileType,
			ContentType: string(constant.SummaryContentType),
		}
		texts[i] = c.Text
	}
	for i, c := range contentChunks {
		ii := i + len(summaryChunks)
		textChunks[ii] = &repository.TextChunk{
			SourceUID:   sourceUID,
			SourceTable: sourceTable,
			StartPos:    c.Start,
			EndPos:      c.End,
			Reference:   c.Reference,
			ContentDest: "not set yet because we need to save the chunks in db to get the uid",
			Tokens:      c.Tokens,
			Retrievable: true,
			InOrder:     ii,
			KbUID:       kbUID,
			KbFileUID:   kbFileUID,
			FileType:    fileType,
			ContentType: string(constant.ChunkContentType),
		}

		texts[ii] = c.Text
	}

	saveToMinIO := func(chunkUIDs []string) (map[string]string, error) {
		chunksForMinIO := make(map[string][]byte, len(textChunks))
		for i, uid := range chunkUIDs {
			chunksForMinIO[uid] = []byte(texts[i])
		}

		destinations, err := svc.SaveTextChunks(ctx, kbUID, kbFileUID, chunksForMinIO)
		if err != nil {
			return nil, fmt.Errorf("storing chunk blobs: %w", err)
		}
		return destinations, nil
	}

	// Save new chunks to MinIO and database atomically (database handles transaction)
	_, err := svc.Repository().DeleteAndCreateChunks(ctx, kbFileUID, textChunks, saveToMinIO)
	if err != nil {
		return fmt.Errorf("storing chunk records in repository: %w", err)
	}

	return nil
}

const batchSize = 50

// saveEmbeddings saves a collection of embeddings extracted from a file into
// the vector and relational databases. The process is done in batches to avoid
// timeouts with the vector DB. If previous embeddings associated to the file
// exist in either database, they're cleaned up.
func saveEmbeddings(ctx context.Context, svc service.Service, kbUID, fileUID uuid.UUID, embeddings []repository.Embedding, fileName string) error {
	logger, _ := logx.GetZapLogger(ctx)
	logger = logger.With(zap.String("KbUID", kbUID.String()))

	if len(embeddings) == 0 {
		logger.Debug("No embeddings to save")
		return nil
	}

	totalEmbeddings := len(embeddings)
	logger = logger.With(zap.Int("total", totalEmbeddings))

	// Delete existing embeddings in the vector database
	if err := svc.VectorDB().DeleteEmbeddingsWithFileUID(ctx, kbUID, fileUID); err != nil {
		return fmt.Errorf("deleting existing embeddings in vector database: %w", err)
	}

	// Process embeddings in batches
	for i := 0; i < totalEmbeddings; i += batchSize {
		// Add context check
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context cancelled while processing embeddings: %w", err)
		}

		end := min(totalEmbeddings, i+batchSize)
		currentBatch := embeddings[i:end]

		logger := logger.With(
			zap.Int("batch", i/batchSize+1),
			zap.Int("batchSize", len(currentBatch)),
			zap.Int("progress", end),
		)

		externalServiceCall := func(insertedEmbeddings []repository.Embedding) error {
			// save the embeddings into vector database
			vectors := make([]service.Embedding, len(insertedEmbeddings))
			for j, emb := range insertedEmbeddings {
				vectors[j] = service.Embedding{
					SourceTable:  emb.SourceTable,
					SourceUID:    emb.SourceUID.String(),
					EmbeddingUID: emb.UID.String(),
					Vector:       emb.Vector,
					FileUID:      emb.KbFileUID,
					FileName:     fileName,
					FileType:     emb.FileType,
					ContentType:  emb.ContentType,
					Tags:         emb.Tags,
				}
			}
			if err := svc.VectorDB().UpsertVectorsInCollection(ctx, kbUID, vectors); err != nil {
				return fmt.Errorf("saving embeddings in vector database: %w", err)
			}

			return nil
		}

		_, err := svc.Repository().DeleteAndCreateEmbeddings(ctx, fileUID, currentBatch, externalServiceCall)
		if err != nil {
			return fmt.Errorf("saving embeddings metadata into database: %w", err)
		}

		logger.Info("Embeddings batch saved successfully")
	}

	logger.Info("All embeddings saved into vector database and metadata into database.")
	return nil
}

// extractRequestMetadata extracts the gRPC metadata from a file's ExternalMetadata
// and returns it as metadata.MD that can be used to create an authenticated context.
func extractRequestMetadata(externalMetadata *structpb.Struct) (metadata.MD, error) {
	md := metadata.MD{}
	if externalMetadata == nil {
		return md, nil
	}

	if externalMetadata.Fields[constant.MetadataRequestKey] == nil {
		return md, nil
	}

	// In order to simplify the code translating metadata.MD <->
	// structpb.Struct, JSON marshalling is used. This is less efficient than
	// leveraging the knowledge about the metadata structure (a
	// map[string][]string), but readability has been prioritized.
	j, err := externalMetadata.Fields[constant.MetadataRequestKey].GetStructValue().MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("marshalling metadata: %w", err)
	}

	if err := json.Unmarshal(j, &md); err != nil {
		return nil, fmt.Errorf("unmarshalling metadata: %w", err)
	}

	return md, nil
}

// createAuthenticatedContext creates a context with the authentication metadata
// from the file's ExternalMetadata. This allows activities to make authenticated
// calls to other services (like pipeline-backend).
func createAuthenticatedContext(ctx context.Context, externalMetadata *structpb.Struct) (context.Context, error) {
	md, err := extractRequestMetadata(externalMetadata)
	if err != nil {
		return ctx, fmt.Errorf("extracting request metadata: %w", err)
	}

	if len(md) == 0 {
		return ctx, nil
	}

	return metadata.NewOutgoingContext(ctx, md), nil
}
