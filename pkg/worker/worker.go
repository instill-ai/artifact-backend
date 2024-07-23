package worker

import (
	"context"
	"encoding/base64"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/milvus"
	"github.com/instill-ai/artifact-backend/pkg/minio"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const periodOfDispatcher = 5 * time.Second
const extensionHelperPeriod = 5 * time.Second
const workerLifetime = 45 * time.Second
const workerPrefix = "worker-processing-file-"

type fileToEmbWorkerPool struct {
	numberOfWorkers int
	svc             *service.Service
	channel         chan repository.KnowledgeBaseFile
	wg              sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
}

func NewFileToEmbWorkerPool(ctx context.Context, svc *service.Service, nums int) *fileToEmbWorkerPool {
	ctx, cancel := context.WithCancel(ctx)
	return &fileToEmbWorkerPool{
		numberOfWorkers: nums,
		svc:             svc,
		channel:         make(chan repository.KnowledgeBaseFile, 100),
		wg:              sync.WaitGroup{},
		ctx:             ctx,
		cancel:          cancel,
	}
}

func (wp *fileToEmbWorkerPool) Start(ctx context.Context) {
	logger, _ := logger.GetZapLogger(ctx)
	for i := 0; i < wp.numberOfWorkers; i++ {
		wp.wg.Add(1)
		go wp.startWorker(ctx, i+1)
	}
	// start dispatcher
	wp.wg.Add(1)
	go wp.startDispatcher(ctx)
	logger.Info("Worker pool started")
}

// dispatcher is responsible for dispatching the incomplete file to the worker
func (wp *fileToEmbWorkerPool) startDispatcher(ctx context.Context) {
	logger, _ := logger.GetZapLogger(ctx)
	defer wp.wg.Done()
	ticker := time.NewTicker(periodOfDispatcher)
	defer ticker.Stop()

	logger.Info("Worker dispatcher started")
	for {
		select {
		case <-ctx.Done():
			// Context is done, exit the dispatcher
			fmt.Println("Dispatcher received termination signal")
			return
		case <-ticker.C:
			// Periodically check for incomplete files
			incompleteFiles := wp.svc.Repository.GetIncompleteFile(ctx)
			// Check if any of the incomplete files have active workers
			fileUID := make([]string, len(incompleteFiles))
			for i, file := range incompleteFiles {
				fileUID[i] = file.UID.String()
			}
			nonExistentKeys := wp.checkRegisteredFilesWorker(ctx, fileUID)

			// Dispatch the files that do not have active workers
			incompleteAndNonRegisteredFiles := make([]repository.KnowledgeBaseFile, 0)
			for _, file := range incompleteFiles {
				if _, ok := nonExistentKeys[file.UID.String()]; ok {
					incompleteAndNonRegisteredFiles = append(incompleteAndNonRegisteredFiles, file)
				}
			}

			for _, file := range incompleteAndNonRegisteredFiles {
				select {
				case <-ctx.Done():
					fmt.Println("Dispatcher received termination signal while dispatching")
					return
				case wp.channel <- file:
					fmt.Printf("Dispatcher dispatched file. fileUID: %s\n", file.UID.String())
				}
			}
		}
	}
}

// REFACTOR : in the future, we can use async process
// so that we just need one worker to trigger the process and one worker to
// check the status of triggered process and extend the lifetime in redis...
// pros: less connection to pipeline service and less resource consumption

func (wp *fileToEmbWorkerPool) startWorker(ctx context.Context, workerID int) {
	logger, _ := logger.GetZapLogger(ctx)
	logger.Info("Worker started", zap.Int("WorkerID", workerID))
	defer wp.wg.Done()
	// Defer a function to catch panics
	defer func() {
		if r := recover(); r != nil {
			logger.Error("Panic recovered in worker",
				zap.Int("WorkerID", workerID),
				zap.Any("panic", r),
				zap.String("stack", string(debug.Stack())))
			// Start a new worker
			logger.Info("Restarting worker after panic", zap.Int("WorkerID", workerID))
			wp.wg.Add(1)
			go wp.startWorker(ctx, workerID)
		}
	}()
	for {
		select {
		case <-ctx.Done():
			// Context is done, exit the worker
			fmt.Printf("Worker %d received termination signal\n", workerID)
			return
		case file, ok := <-wp.channel:
			if !ok {
				// Job channel is closed, exit the worker
				fmt.Printf("Job channel closed, worker %d exiting\n", workerID)
				return
			}

			// register file process worker in redis and extend the lifetime
			ok, stopRegisterFunc := wp.registerFileWorker(ctx, file.UID.String(), extensionHelperPeriod, workerLifetime)

			if !ok {
				continue
			}
			// start file processing tracing
			fmt.Printf("Worker %d processing file: %s\n", workerID, file.UID.String())

			// process file
			err := wp.processFile(ctx, file)
			if err != nil {
				fmt.Printf("Error processing file: %s, error: %v\n", file.UID.String(), err)
			} else {
				fmt.Printf("Worker %d finished processing fileUID: %s\n", workerID, file.UID.String())
			}
			// cancel the lifetime extend helper when the file is processed
			stopRegisterFunc()
		}
	}
}

// stop
func (wp *fileToEmbWorkerPool) GraceFulStop() {
	logger, _ := logger.GetZapLogger(wp.ctx)
	logger.Info("Worker pool received termination signal")
	close(wp.channel)
	wp.cancel()
	wp.wg.Wait()
	logger.Info("Worker pool exited")
}

type stopRegisterWorkerFunc func()

// registerFileWorker registers a file worker in the worker pool.
// It sets a worker key in Redis with the given fileUID and workerLifetime.
// It returns a stopRegisterWorkerFunc that can be used to cancel the worker's lifetime extension and remove the worker key from Redis.
// period: second
// workerLifetime: second
func (wp *fileToEmbWorkerPool) registerFileWorker(ctx context.Context, fileUID string, period time.Duration, workerLifetime time.Duration) (bool, stopRegisterWorkerFunc) {
	ok, err := wp.svc.RedisClient.SetNX(ctx, getWorkerKey(fileUID), "1", workerLifetime).Result()
	if err != nil {
		fmt.Printf("Error when setting worker key in redis. Error: %v\n", err)
		return false, nil
	}
	if !ok {
		fmt.Printf("File is already being processed in redis. fileUID: %s\n", fileUID)
		return false, nil
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
				fmt.Printf("Finish %v's lifetime extend helper received termination signal\n", getWorkerKey(fileUID))
				return
			case <-ticker.C:
				// extend the lifetime of the worker
				fmt.Printf("Extending %v's lifetime: %v \n", getWorkerKey(fileUID), workerLifetime)
				err := wp.svc.RedisClient.Expire(ctx, getWorkerKey(fileUID), workerLifetime).Err()
				if err != nil {
					fmt.Printf("Error when extending worker lifetime in redis. Error: %v, worker: %v\n", err, getWorkerKey(fileUID))
					return
				}
			}
		}
	}
	go lifetimeExtHelper(ctx)

	// stopRegisterWorker function will cancel the lifetimeExtHelper and remove the worker key in redis
	stopRegisterWorker := func() {
		lifetimeHelperCancel()
		wp.svc.RedisClient.Del(ctx, getWorkerKey(fileUID))
	}

	return true, stopRegisterWorker
}

// checkFileWorker checks if any of the provided fileUIDs have active workers
func (wp *fileToEmbWorkerPool) checkRegisteredFilesWorker(ctx context.Context, fileUIDs []string) map[string]struct{} {
	pipe := wp.svc.RedisClient.Pipeline()

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
		fmt.Println("Error executing pipeline:", err)
		return nil
	}

	// Collect keys that do not exist
	nonExistentKeys := make(map[string]struct{})
	for fileUID, result := range results {
		exists, err := result.Result()
		if err != nil {
			fmt.Printf("Error getting result for %s: %v\n", fileUID, err)
			return nil
		}
		if exists == 0 {
			nonExistentKeys[fileUID] = struct{}{}
		}
	}
	return nonExistentKeys
}

// process file using state machine
func (wp *fileToEmbWorkerPool) processFile(ctx context.Context, file repository.KnowledgeBaseFile) error {
	var status artifactpb.FileProcessStatus
	if statusInt, ok := artifactpb.FileProcessStatus_value[file.ProcessStatus]; !ok {
		return fmt.Errorf("invalid process status: %v", file.ProcessStatus)
	} else {
		status = artifactpb.FileProcessStatus(statusInt)
	}
	for {
		switch status {
		case artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_WAITING:
			updatedFile, nextStatus, err := wp.processWaitingFile(ctx, file)
			if err != nil {
				return err
			}
			status = nextStatus
			file = *updatedFile
		case artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CONVERTING:
			updatedFile, nextStatus, err := wp.processConvertingFile(ctx, file)
			if err != nil {
				return err
			}
			status = nextStatus
			file = *updatedFile

		case artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING:
			updatedFile, nextStatus, err := wp.processChunkingFile(ctx, file)
			if err != nil {
				return err
			}
			status = nextStatus
			file = *updatedFile

		case artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING:
			updatedFile, nextStatus, err := wp.processEmbeddingFile(ctx, file)
			if err != nil {
				return err
			}
			status = nextStatus
			file = *updatedFile

		case artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED:
			err := wp.processCompletedFile(ctx, file)
			if err != nil {
				return err
			}
			return nil
		}
	}
}

// processWaitingFile checks the file type and determines the next status based on it.
// If the file type is pdf, it moves to the converting status.
// If the file type is txt or md, it moves to the chunking status.
// If the file type is neither pdf, txt, nor md, it returns an error.
func (wp *fileToEmbWorkerPool) processWaitingFile(ctx context.Context, file repository.KnowledgeBaseFile) (updatedFile *repository.KnowledgeBaseFile, nextStatus artifactpb.FileProcessStatus, err error) {
	// check if file process status is waiting
	if file.ProcessStatus != artifactpb.FileProcessStatus_name[int32(artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_WAITING)] {
		return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, fmt.Errorf("file process status should be waiting. status: %v", file.ProcessStatus)
	}

	switch file.Type {

	// Deal with pdf files
	case artifactpb.FileType_name[int32(artifactpb.FileType_FILE_TYPE_PDF)]:

		updateMap := map[string]interface{}{
			repository.KnowledgeBaseFileColumn.ProcessStatus: artifactpb.FileProcessStatus_name[int32(artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CONVERTING)],
		}
		updatedFile, err := wp.svc.Repository.UpdateKnowledgeBaseFile(ctx, file.UID.String(), updateMap)
		if err != nil {
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}
		return updatedFile, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CONVERTING, nil

	// Deal with text and markdown files
	case artifactpb.FileType_name[int32(artifactpb.FileType_FILE_TYPE_TEXT)],
		artifactpb.FileType_name[int32(artifactpb.FileType_FILE_TYPE_MARKDOWN)]:

		updateMap := map[string]interface{}{
			repository.KnowledgeBaseFileColumn.ProcessStatus: artifactpb.FileProcessStatus_name[int32(artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING)],
		}
		updatedFile, err := wp.svc.Repository.UpdateKnowledgeBaseFile(ctx, file.UID.String(), updateMap)
		if err != nil {
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}
		return updatedFile, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING, nil

	default:
		return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, fmt.Errorf("unsupported file type in processWaitingFile: %v", file.Type)
	}
}

// if it is pdf and file in converting status, get the file from minIO,
// convert it to md using pdf-to-markdown pipeline. then move to chunking status
// save the converted file into object storage and metadata into database
// update the file status to chunking status in database
// if it is not pdf, return error
func (wp *fileToEmbWorkerPool) processConvertingFile(ctx context.Context, file repository.KnowledgeBaseFile) (updatedFile *repository.KnowledgeBaseFile, nextStatus artifactpb.FileProcessStatus, err error) {
	logger, _ := logger.GetZapLogger(ctx)
	fileInMinIOPath := file.Destination
	data, err := wp.svc.MinIO.GetFile(ctx, fileInMinIOPath)
	if err != nil {
		logger.Error("Failed to get file from minIO.", zap.String("File path", fileInMinIOPath))
		return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
	}

	// encode data to base64
	base64Data := base64.StdEncoding.EncodeToString(data)

	// convert the pdf file to md
	convertedMD, err := wp.svc.ConvertPDFToMD(ctx, file.CreatorUID, base64Data)
	if err != nil {
		logger.Error("Failed to convert pdf to md.", zap.String("File path", fileInMinIOPath))
		return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
	}

	// save the converted file into object storage and metadata into database
	err = wp.saveConvertedFile(ctx, file.KnowledgeBaseUID, file.UID, "converted_"+file.Name, []byte(convertedMD))
	if err != nil {
		logger.Error("Failed to save converted data.", zap.String("File path", fileInMinIOPath))
		return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
	}

	// update the file status to chunking status in database
	updateMap := map[string]interface{}{
		repository.KnowledgeBaseFileColumn.ProcessStatus: artifactpb.FileProcessStatus_name[int32(artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING)],
	}
	updatedFile, err = wp.svc.Repository.UpdateKnowledgeBaseFile(ctx, file.UID.String(), updateMap)
	if err != nil {
		logger.Error("Failed to update file status.", zap.String("File uid", file.UID.String()))
		return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
	}

	return updatedFile, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING, nil
}

// check if the file status is chunking,
// if it is pdf get the md-converted file from minIO, then call the chunking pipeline
// if it is text or md,file from minIO, then call the chunking pipeline
// save the chunks into object storage and metadata into database
// update the file status to embedding in database
func (wp *fileToEmbWorkerPool) processChunkingFile(ctx context.Context, file repository.KnowledgeBaseFile) (*repository.KnowledgeBaseFile, artifactpb.FileProcessStatus, error) {
	logger, _ := logger.GetZapLogger(ctx)
	logger.Info("Processing chunking status file.", zap.String("File uid", file.UID.String()))
	// check the file status is chunking
	if file.ProcessStatus != artifactpb.FileProcessStatus_name[int32(artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING)] {
		return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, fmt.Errorf("file process status should be chunking. status: %v", file.ProcessStatus)
	}
	// check the file type
	switch file.Type {
	// get the file from minIO (check destination from converted file table) and call the chunking pipeline
	case artifactpb.FileType_name[int32(artifactpb.FileType_FILE_TYPE_PDF)]:
		// get the converted file metadata from database
		convertedFile, err := wp.svc.Repository.GetConvertedFileByFileUID(ctx, file.UID)
		if err != nil {
			logger.Error("Failed to get converted file metadata.", zap.String("File uid", file.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}
		// get the converted file from minIO
		convertedFileData, err := wp.svc.MinIO.GetFile(ctx, convertedFile.Destination)
		if err != nil {
			logger.Error("Failed to get converted file from minIO.", zap.String("Converted file uid", convertedFile.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}
		// call the markdown chunking pipeline
		chunks, err := wp.svc.SplitMarkdown(ctx, file.CreatorUID, string(convertedFileData))
		if err != nil {
			logger.Error("Failed to get chunks from converted file.", zap.String("Converted file uid", convertedFile.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}

		//  Save the chunks into object storage(minIO) and metadata into database
		err = wp.saveChunks(ctx, file.KnowledgeBaseUID.String(), file.UID, wp.svc.Repository.ConvertedFileTableName(), convertedFile.UID, chunks)
		if err != nil {
			logger.Error("Failed to save chunks into object storage and metadata into database.", zap.String("File uid", file.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}
		// update the file status to embedding status in database
		updateMap := map[string]interface{}{
			repository.KnowledgeBaseFileColumn.ProcessStatus: artifactpb.FileProcessStatus_name[int32(artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING)],
		}
		updatedFile, err := wp.svc.Repository.UpdateKnowledgeBaseFile(ctx, file.UID.String(), updateMap)
		if err != nil {
			logger.Error("Failed to update file status.", zap.String("File uid", file.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}
		return updatedFile, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING, nil

	case artifactpb.FileType_name[int32(artifactpb.FileType_FILE_TYPE_TEXT)]:

		//  Get file from minIO
		originalFile, err := wp.svc.MinIO.GetFile(ctx, file.Destination)
		if err != nil {
			logger.Error("Failed to get file from minIO.", zap.String("File uid", file.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}

		//  Call the text chunking pipeline
		chunks, err := wp.svc.SplitText(ctx, file.CreatorUID, string(originalFile))
		if err != nil {
			logger.Error("Failed to get chunks from original file.", zap.String("File uid", file.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}
		//  Save the chunks into object storage(minIO) and metadata into database
		err = wp.saveChunks(ctx, file.KnowledgeBaseUID.String(), file.UID, wp.svc.Repository.KnowledgeBaseFileTableName(), file.UID, chunks)
		if err != nil {
			logger.Error("Failed to save chunks into object storage and metadata into database.", zap.String("File uid", file.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}
		// update the file status to embedding status in database
		updateMap := map[string]interface{}{
			repository.KnowledgeBaseFileColumn.ProcessStatus: artifactpb.FileProcessStatus_name[int32(artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING)],
		}
		updatedFile, err := wp.svc.Repository.UpdateKnowledgeBaseFile(ctx, file.UID.String(), updateMap)
		if err != nil {
			logger.Error("Failed to update file status.", zap.String("File uid", file.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}
		return updatedFile, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING, nil
	case artifactpb.FileType_name[int32(artifactpb.FileType_FILE_TYPE_MARKDOWN)]:
		//  Get file from minIO
		originalFile, err := wp.svc.MinIO.GetFile(ctx, file.Destination)
		if err != nil {
			logger.Error("Failed to get file from minIO.", zap.String("File uid", file.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}

		//  Call the text chunking pipeline
		chunks, err := wp.svc.SplitMarkdown(ctx, file.CreatorUID, string(originalFile))
		if err != nil {
			logger.Error("Failed to get chunks from original file.", zap.String("File uid", file.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}
		//  Save the chunks into object storage(minIO) and metadata into database
		err = wp.saveChunks(ctx, file.KnowledgeBaseUID.String(), file.UID, wp.svc.Repository.KnowledgeBaseFileTableName(), file.UID, chunks)
		if err != nil {
			logger.Error("Failed to save chunks into object storage and metadata into database.", zap.String("File uid", file.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}
		// update the file status to embedding status in database
		updateMap := map[string]interface{}{
			repository.KnowledgeBaseFileColumn.ProcessStatus: artifactpb.FileProcessStatus_name[int32(artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING)],
		}
		updatedFile, err := wp.svc.Repository.UpdateKnowledgeBaseFile(ctx, file.UID.String(), updateMap)
		if err != nil {
			logger.Error("Failed to update file status.", zap.String("File uid", file.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}
		return updatedFile, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING, nil
	default:
		return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, fmt.Errorf("unsupported file type in processChunkingFile: %v", file.Type)
	}

}

// check the file status is embedding
// if it is pdf, txt, or md, all get the chunks from minIO, then call the embedding pipeline
// save the embedding into vector database and metadata into database
// update the file status to completed in database
func (wp *fileToEmbWorkerPool) processEmbeddingFile(ctx context.Context, file repository.KnowledgeBaseFile) (updatedFile *repository.KnowledgeBaseFile, nextStatus artifactpb.FileProcessStatus, err error) {
	logger, _ := logger.GetZapLogger(ctx)
	// check the file status is embedding
	if file.ProcessStatus != artifactpb.FileProcessStatus_name[int32(artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING)] {
		return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, fmt.Errorf("file process status should be embedding. status: %v", file.ProcessStatus)
	}

	// check the file type
	// if it is pdf, sourceTable is converted file table, sourceUID is converted file UID
	// if it is txt or md, sourceTable is knowledge base file table, sourceUID is knowledge base file UID
	var sourceTable string
	var sourceUID uuid.UUID
	switch file.Type {
	case artifactpb.FileType_name[int32(artifactpb.FileType_FILE_TYPE_PDF)]:
		// set the sourceTable and sourceUID
		convertedFile, err := wp.svc.Repository.GetConvertedFileByFileUID(ctx, file.UID)
		if err != nil {
			logger.Error("Failed to get converted file metadata.", zap.String("File uid", file.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}
		sourceTable = wp.svc.Repository.ConvertedFileTableName()
		sourceUID = convertedFile.UID
	case artifactpb.FileType_name[int32(artifactpb.FileType_FILE_TYPE_TEXT)],
		artifactpb.FileType_name[int32(artifactpb.FileType_FILE_TYPE_MARKDOWN)]:
		// set the sourceTable and sourceUID
		sourceTable = wp.svc.Repository.KnowledgeBaseFileTableName()
		sourceUID = file.UID
	default:
		return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, fmt.Errorf("unsupported file type in processEmbeddingFile: %v", file.Type)
	}
	// get the chunks's path
	chunks, err := wp.svc.Repository.GetTextChunksBySource(ctx, sourceTable, sourceUID)
	if err != nil {
		logger.Error("Failed to get chunks from database.", zap.String("SourceUID", sourceUID.String()))
		return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
	}
	// get the chunks from minIO
	chunksPaths := make([]string, len(chunks))
	for i, c := range chunks {
		chunksPaths[i] = c.ContentDest
	}
	chunkFiles, err := wp.svc.MinIO.GetFilesByPaths(ctx, chunksPaths)
	if err != nil {
		// log error source table and source UID
		logger.Error("Failed to get chunks from minIO.", zap.String("SourceTable", sourceTable), zap.String("SourceUID", sourceUID.String()))
		return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
	}
	texts := make([]string, len(chunkFiles))
	for i, f := range chunkFiles {
		texts[i] = string(f.Content)
	}
	// call the embedding pipeline
	vectors, err := wp.svc.VectorizeText(ctx, file.CreatorUID, texts)
	// vectors, err := mockVectorizeText(ctx, file.CreatorUID, texts)
	if err != nil {
		logger.Error("Failed to get embeddings from chunks.", zap.String("SourceTable", sourceTable), zap.String("SourceUID", sourceUID.String()))
		return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
	}

	// save the embeddings into milvus and metadata into database
	collection := wp.svc.MilvusClient.GetKnowledgeBaseCollectionName(file.KnowledgeBaseUID.String())
	embeddings := make([]repository.Embedding, len(vectors))
	for i, v := range vectors {
		embeddings[i] = repository.Embedding{
			SourceTable: wp.svc.Repository.TextChunkTableName(),
			SourceUID:   chunks[i].UID,
			Vector:      v,
			Collection:  collection,
			KbUID:       file.KnowledgeBaseUID,
			KbFileUID:   file.UID,
		}
	}
	err = wp.saveEmbeddings(ctx, file.KnowledgeBaseUID.String(), embeddings)
	if err != nil {
		logger.Error("Failed to save embeddings into vector database and metadata into database.", zap.String("SourceUID", sourceUID.String()))
		return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
	}

	// update the file status to chunking status in database
	updateMap := map[string]interface{}{
		repository.KnowledgeBaseFileColumn.ProcessStatus: artifactpb.FileProcessStatus_name[int32(artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED)],
	}
	updatedFile, err = wp.svc.Repository.UpdateKnowledgeBaseFile(ctx, file.UID.String(), updateMap)
	if err != nil {
		logger.Error("Failed to update file status.", zap.String("File uid", file.UID.String()))
		return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
	}
	return updatedFile, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED, nil
}

// check the file status is completed. maybe just print the info and currently do nothing.
func (wp *fileToEmbWorkerPool) processCompletedFile(ctx context.Context, file repository.KnowledgeBaseFile) error {
	logger, _ := logger.GetZapLogger(ctx)
	logger.Info("File to embeddings process completed.", zap.String("File uid", file.UID.String()))
	return nil
}

// save converted file into object storage and metadata into database
func (wp *fileToEmbWorkerPool) saveConvertedFile(ctx context.Context, kbUID, fileUID uuid.UUID, name string, convertedFile []byte) error {
	logger, _ := logger.GetZapLogger(ctx)
	_, err := wp.svc.Repository.CreateConvertedFile(
		ctx,
		repository.ConvertedFile{KbUID: kbUID, FileUID: fileUID, Name: name, Type: "text/markdown", Destination: "destination"},
		func(convertedFileUID uuid.UUID) (map[string]any, error) {
			// save the converted file into object storage
			err := wp.svc.MinIO.SaveConvertedFile(ctx, kbUID.String(), convertedFileUID.String(), "md", convertedFile)
			if err != nil {
				return nil, err
			}
			output := make(map[string]any)
			output[repository.ConvertedFileColumn.Destination] = wp.svc.MinIO.GetConvertedFilePathInKnowledgeBase(kbUID.String(), convertedFileUID.String(), "md")
			return output, nil
		})
	if err != nil {
		logger.Error("Failed to save converted file into object storage and metadata into database.", zap.String("FileUID", fileUID.String()))
		return err
	}

	return nil
}

type chunk = struct {
	End    int
	Start  int
	Text   string
	Tokens int
}

// save chunk into object storage and metadata into database
func (wp *fileToEmbWorkerPool) saveChunks(ctx context.Context, kbUID string, kbFileUID uuid.UUID, sourceTable string, sourceUID uuid.UUID, chunks []chunk) error {
	logger, _ := logger.GetZapLogger(ctx)
	textChunks := make([]*repository.TextChunk, len(chunks))

	// turn kbuid to uuid no must parse
	kbUIDuuid, _ := uuid.Parse(kbUID)

	for i, c := range chunks {
		textChunks[i] = &repository.TextChunk{
			SourceUID:   sourceUID,
			SourceTable: sourceTable,
			StartPos:    c.Start,
			EndPos:      c.End,
			ContentDest: "not set yet becasue we need to save the chunks in db to get the uid",
			Tokens:      c.Tokens,
			Retrievable: true,
			InOrder:     i,
			KbUID:       kbUIDuuid,
			KbFileUID:   kbFileUID,
		}
	}
	_, err := wp.svc.Repository.DeleteAndCreateChunks(ctx, sourceTable, sourceUID, textChunks,
		func(chunkUIDs []string) (map[string]any, error) {
			// save the chunksForMinIO into object storage
			chunksForMinIO := make(map[minio.ChunkUIDType]minio.ChunkContentType, len(textChunks))
			for i, uid := range chunkUIDs {
				chunksForMinIO[minio.ChunkUIDType(uid)] = minio.ChunkContentType([]byte(chunks[i].Text))
			}
			err := wp.svc.MinIO.SaveChunks(ctx, kbUID, chunksForMinIO)
			if err != nil {
				logger.Error("Failed to save chunks into object storage.", zap.String("SourceUID", sourceUID.String()))
				return nil, err
			}
			chunkDestMap := make(map[string]any, len(chunkUIDs))
			for _, chunkUID := range chunkUIDs {
				chunkDestMap[chunkUID] = wp.svc.MinIO.GetChunkPathInKnowledgeBase(kbUID, string(chunkUID))
			}
			return chunkDestMap, nil
		},
	)
	if err != nil {
		logger.Error("Failed to save chunks into object storage and metadata into database.", zap.String("SourceUID", sourceUID.String()))
		return err
	}
	return nil
}

type MilvusEmbedding struct {
	SourceTable  string
	SourceUID    string
	EmbeddingUID string
	Vector       []float32
}

// save embedding into vector database and metadata into database
func (wp *fileToEmbWorkerPool) saveEmbeddings(ctx context.Context, kbUID string, embeddings []repository.Embedding) error {
	logger, _ := logger.GetZapLogger(ctx)
	externalServiceCall := func(embUIDs []string) error {
		// save the embeddings into vector database
		milvusEmbeddings := make([]milvus.Embedding, len(embeddings))
		for i, emb := range embeddings {
			milvusEmbeddings[i] = milvus.Embedding{
				SourceTable:  emb.SourceTable,
				SourceUID:    emb.SourceUID.String(),
				EmbeddingUID: emb.UID.String(),
				Vector:       emb.Vector,
			}
		}
		err := wp.svc.MilvusClient.InsertVectorsToKnowledgeBaseCollection(ctx, kbUID, milvusEmbeddings)
		if err != nil {
			logger.Error("Failed to save embeddings into vector database.", zap.String("KbUID", kbUID))
			return err
		}
		return nil
	}
	_, err := wp.svc.Repository.UpsertEmbeddings(ctx, embeddings, externalServiceCall)
	if err != nil {
		logger.Error("Failed to save embeddings into vector database and metadata into database.", zap.String("KbUID", kbUID))
		return err
	}
	// info how many embeddings saved in which kb
	logger.Info("Embeddings saved into vector database and metadata into database.",
		zap.String("KbUID", kbUID), zap.Int("Embeddings count", len(embeddings)))
	return nil
}

func getWorkerKey(fileUID string) string {
	return workerPrefix + fileUID
}

// mockVectorizeText is a mock implementation of the VectorizeText function.
//nolint:unused
func mockVectorizeText(_ context.Context, _ uuid.UUID, texts []string) ([][]float32, error) {
	vectors := make([][]float32, len(texts))
	for i := range texts {
		vectors[i] = make([]float32, milvus.VectorDim)
	}
	return vectors, nil
}
