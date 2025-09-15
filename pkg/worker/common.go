package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/minio"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"

	logx "github.com/instill-ai/x/log"
)

const periodOfDispatcher = 5 * time.Second
const extensionHelperPeriod = 5 * time.Second
const workerLifetime = 45 * time.Second
const workerPrefix = "worker-processing-file-"

var ErrFileStatusNotMatch = errors.New("file status not match")

func getWorkerKey(fileUID string) string {
	return workerPrefix + fileUID
}

// checkFileStatus checks if the file status from argument is the same as the file in database
func checkFileStatus(ctx context.Context, svc service.Service, file repository.KnowledgeBaseFile) error {
	dbFiles, err := svc.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{file.UID})
	if err != nil {
		return err
	}
	if len(dbFiles) == 0 {
		return fmt.Errorf("file uid not found in database. file uid: %s", file.UID)
	}
	// if the file's status from argument is not the same as the file in database, skip the processing
	// because the file in argument is not the latest file in database. Instead, it is from the queue.
	if dbFiles[0].ProcessStatus != file.ProcessStatus {
		err := fmt.Errorf("%w - file uid: %s, database file status: %v, file status in argument: %v", ErrFileStatusNotMatch, file.UID, dbFiles[0].ProcessStatus, file.ProcessStatus)
		return err
	}
	return nil
}

// registerFileWorker registers a file worker in the worker pool and sets a worker key in Redis with the given fileUID and workerLifetime.
// It periodically extends the worker's lifetime in Redis until the worker is done processing.
// It returns a boolean indicating success and a stopRegisterWorkerFunc that can be used to cancel the worker's lifetime extension and remove the worker key from Redis.
// period: duration between lifetime extensions
// workerLifetime: total duration the worker key should be kept in Redis
func registerFileWorker(ctx context.Context, svc service.Service, fileUID string, period time.Duration, workerLifetime time.Duration) (ok bool, stopRegisterWorker stopRegisterWorkerFunc) {
	logger, _ := logx.GetZapLogger(ctx)
	stopRegisterWorker = func() {
		logger.Warn("stopRegisterWorkerFunc is not implemented yet")
	}
	ok, err := svc.RedisClient().SetNX(ctx, getWorkerKey(fileUID), "1", workerLifetime).Result()
	if err != nil {
		logger.Error("Error when setting worker key in redis", zap.Error(err))
		return
	}
	if !ok {
		logger.Warn("Key exists in redis, file is already being processed by worker", zap.String("fileUID", fileUID))
		return
	}
	ctx, lifetimeHelperCancel := context.WithCancel(ctx)

	// lifetimeExtHelper is a helper function that extends the lifetime of the worker by periodically updating the worker key's expiration time in Redis.
	lifetimeExtHelper := func(ctx context.Context) {
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				// Context is done, exit the worker
				logger.Debug("Finish worker lifetime extend helper received termination signal", zap.String("worker", getWorkerKey(fileUID)))
				return
			case <-ticker.C:
				// extend the lifetime of the worker
				logger.Debug("Extending worker lifetime", zap.String("worker", getWorkerKey(fileUID)), zap.Duration("lifetime", workerLifetime))
				err := svc.RedisClient().Expire(ctx, getWorkerKey(fileUID), workerLifetime).Err()
				if err != nil {
					logger.Error("Error when extending worker lifetime in redis", zap.Error(err), zap.String("worker", getWorkerKey(fileUID)))
					return
				}
			}
		}
	}
	go lifetimeExtHelper(ctx)

	// stopRegisterWorker function will cancel the lifetimeExtHelper and remove the worker key in redis
	stopRegisterWorker = func() {
		lifetimeHelperCancel()
		svc.RedisClient().Del(ctx, getWorkerKey(fileUID))
	}

	return true, stopRegisterWorker
}

// checkFileWorker checks if any of the provided fileUIDs have active workers
func checkRegisteredFilesWorker(ctx context.Context, svc service.Service, fileUIDs []string) map[string]struct{} {
	logger, _ := logx.GetZapLogger(ctx)
	pipe := svc.RedisClient().Pipeline()

	// Create a map to hold the results
	results := make(map[string]*redis.IntCmd)

	// Add EXISTS commands to the pipeline for each fileUID
	for _, fileUID := range fileUIDs {
		key := getWorkerKey(fileUID)
		results[fileUID] = pipe.Exists(ctx, key)
	}

	// Execute the pipeline
	_, err := pipe.Exec(ctx)
	if err != nil {
		logger.Error("Error executing redis pipeline", zap.Error(err))
		return nil
	}

	// Collect keys that do not exist
	nonExistentKeys := make(map[string]struct{})
	for fileUID, result := range results {
		exists, err := result.Result()
		if err != nil {
			logger.Error("Error getting result for %s", zap.String("fileUID", fileUID), zap.Error(err))
			return nil
		}
		if exists == 0 {
			nonExistentKeys[fileUID] = struct{}{}
		}
	}
	return nonExistentKeys
}

// saveConvertedFile saves a converted file into object storage and updates the
// metadata in the database.
func (wp *persistentCatalogFileToEmbWorkerPool) saveConvertedFile(ctx context.Context, kbUID, fileUID uuid.UUID, name string, conversion *service.MDConversionResult) error {
	saveToMinIO := func(convertedFileUID uuid.UUID) (map[string]any, error) {
		blobStorage := wp.svc.MinIO()
		err := blobStorage.SaveConvertedFile(ctx, kbUID.String(), convertedFileUID.String(), "md", []byte(conversion.Markdown))
		if err != nil {
			return nil, fmt.Errorf("storing converted file as blob: %w", err)
		}

		output := make(map[string]any)
		output[repository.ConvertedFileColumn.Destination] = blobStorage.GetConvertedFilePathInKnowledgeBase(kbUID.String(), convertedFileUID.String(), "md")

		return output, nil
	}

	convertedFile := repository.ConvertedFile{
		KbUID:        kbUID,
		FileUID:      fileUID,
		Name:         name,
		Type:         "text/markdown",
		Destination:  "destination",
		PositionData: conversion.PositionData,
	}
	if _, err := wp.svc.Repository().CreateConvertedFile(ctx, convertedFile, saveToMinIO); err != nil {
		return fmt.Errorf("storing converted file in repository: %w", err)
	}

	return nil
}

// saveChunks saves chunks into object storage and updates the metadata in the database.
func (wp *persistentCatalogFileToEmbWorkerPool) saveChunks(
	ctx context.Context,
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

	saveToMinIO := func(chunkUIDs []string) (map[string]any, error) {
		chunksForMinIO := make(map[minio.ChunkUIDType]minio.ChunkContentType, len(textChunks))
		for i, uid := range chunkUIDs {
			chunksForMinIO[minio.ChunkUIDType(uid)] = minio.ChunkContentType([]byte(texts[i]))
		}

		err := wp.svc.MinIO().SaveTextChunks(ctx, kbUID.String(), chunksForMinIO)
		if err != nil {
			return nil, fmt.Errorf("storing chunk blobs: %w", err)
		}

		chunkDestMap := make(map[string]any, len(chunkUIDs))
		for _, chunkUID := range chunkUIDs {
			chunkDestMap[chunkUID] = wp.svc.MinIO().GetChunkPathInKnowledgeBase(kbUID.String(), string(chunkUID))
		}

		return chunkDestMap, nil
	}

	_, err := wp.svc.Repository().DeleteAndCreateChunks(ctx, sourceTable, sourceUID, textChunks, saveToMinIO)
	if err != nil {
		return fmt.Errorf("storing chunk records in repository: %w", err)
	}

	return nil
}

// saveEmbeddings saves embeddings into the vector database and updates the metadata in the database.
// Processes embeddings in batches of 50 to avoid timeout issues.
const batchSize = 50

func saveEmbeddings(ctx context.Context, svc service.Service, kbUID uuid.UUID, embeddings []repository.Embedding, fileName string) error {
	logger, _ := logx.GetZapLogger(ctx)
	logger = logger.With(zap.String("KbUID", kbUID.String()))

	if len(embeddings) == 0 {
		logger.Debug("No embeddings to save")
		return nil
	}

	totalEmbeddings := len(embeddings)
	logger = logger.With(zap.Int("total", totalEmbeddings))

	// Process embeddings in batches
	for i := 0; i < totalEmbeddings; i += batchSize {
		// Add context check
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context cancelled while processing embeddings: %w", err)
		}

		end := i + batchSize
		if end > totalEmbeddings {
			end = totalEmbeddings
		}

		currentBatch := embeddings[i:end]

		logger := logger.With(
			zap.Int("batch", i/batchSize+1),
			zap.Int("batchSize", len(currentBatch)),
			zap.Int("progress", end),
		)

		externalServiceCall := func(_ []string) error {
			// save the embeddings into vector database
			milvusEmbeddings := make([]service.Embedding, len(currentBatch))
			for j, emb := range currentBatch {
				milvusEmbeddings[j] = service.Embedding{
					SourceTable:  emb.SourceTable,
					SourceUID:    emb.SourceUID.String(),
					EmbeddingUID: emb.UID.String(),
					Vector:       emb.Vector,
					FileUID:      emb.KbFileUID,
					FileName:     fileName,
					FileType:     emb.FileType,
					ContentType:  emb.ContentType,
				}
			}
			err := svc.VectorDB().InsertVectorsInCollection(ctx, service.KBCollectionName(kbUID), milvusEmbeddings)
			if err != nil {
				return fmt.Errorf("saving embeddings in vector database: %w", err)
			}
			return nil
		}

		_, err := svc.Repository().UpsertEmbeddings(ctx, currentBatch, externalServiceCall)
		if err != nil {
			return fmt.Errorf("saving embeddings metadata into database: %w", err)
		}

		logger.Info("Embeddings batch saved successfully")
	}

	logger.Info("All embeddings saved into vector database and metadata into database.")
	return nil
}
