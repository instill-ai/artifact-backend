package worker

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/milvus"
	"github.com/instill-ai/artifact-backend/pkg/minio"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"
	"github.com/instill-ai/artifact-backend/pkg/utils"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const periodOfDispatcher = 5 * time.Second
const extensionHelperPeriod = 5 * time.Second
const workerLifetime = 45 * time.Second
const workerPrefix = "worker-processing-file-"

var ErrFileStatusNotMatch = errors.New("file status not match")

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
		// channel is un-buffered because we dont want the out of date file to be processed
		channel: make(chan repository.KnowledgeBaseFile),
		wg:      sync.WaitGroup{},
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (wp *fileToEmbWorkerPool) Start() {
	logger, _ := logger.GetZapLogger(wp.ctx)
	for i := 0; i < wp.numberOfWorkers; i++ {
		wp.wg.Add(1)
		go utils.GoRecover(func() {
			wp.startWorker(wp.ctx, i+1)
		}, fmt.Sprintf("Worker %d", i+1))
	}
	// start dispatcher
	wp.wg.Add(1)
	go utils.GoRecover(func() {
		wp.startDispatcher()
	}, "Dispatcher")
	logger.Info("Worker pool started")
}

// dispatcher is responsible for dispatching the incomplete file to the worker
func (wp *fileToEmbWorkerPool) startDispatcher() {
	logger, _ := logger.GetZapLogger(wp.ctx)
	defer wp.wg.Done()
	ticker := time.NewTicker(periodOfDispatcher)
	defer ticker.Stop()

	logger.Info("Worker dispatcher started")
	for {
		select {
		case <-wp.ctx.Done():
			// Context is done, exit the dispatcher
			fmt.Println("Dispatcher received termination signal")
			return
		case <-ticker.C:
			// Periodically check for incomplete files
			incompleteFiles := wp.svc.Repository.GetNeedProcessFiles(wp.ctx)
			// Check if any of the incomplete files have active workers
			fileUIDs := make([]string, len(incompleteFiles))
			for i, file := range incompleteFiles {
				fileUIDs[i] = file.UID.String()
			}
			nonExistentKeys := wp.checkRegisteredFilesWorker(wp.ctx, fileUIDs)

			// Dispatch the files that do not have active workers
			incompleteAndNonRegisteredFiles := make([]repository.KnowledgeBaseFile, 0)
			for _, file := range incompleteFiles {
				if _, ok := nonExistentKeys[file.UID.String()]; ok {
					incompleteAndNonRegisteredFiles = append(incompleteAndNonRegisteredFiles, file)
				}
			}

		dispatchLoop:
			for _, file := range incompleteAndNonRegisteredFiles {
				select {
				case <-wp.ctx.Done():
					fmt.Println("Dispatcher received termination signal while dispatching")
					return
				default:
					select {
					case wp.channel <- file:
						logger.Info("Dispatcher dispatched file.", zap.String("fileUID", file.UID.String()))
					default:
						logger.Debug("channel is full, skip dispatching remaining files.", zap.String("fileUID", file.UID.String()))
						break dispatchLoop
					}
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
				if stopRegisterFunc != nil {
					stopRegisterFunc()
				}
				continue
			}
			// check if the file is already processed
			// Because the file is from the dispatcher, the file status is guaranteed to be incomplete
			// but when the worker wakes up and tries to process the file, the file status might have been updated by other workers.
			// So we need to check the file status again to ensure the file is still same as when the worker wakes up
			err := wp.checkFileStatus(ctx, file)
			if err != nil {
				logger.Warn("File status not match. skip processing", zap.String("file uid", file.UID.String()), zap.Error(err))
				if stopRegisterFunc != nil {
					stopRegisterFunc()
				}
				continue
			}
			// start file processing tracing
			fmt.Printf("Worker %d processing file: %s\n", workerID, file.UID.String())

			// process
			t0 := time.Now()
			err = wp.processFile(ctx, file)

			if err != nil {
				logger.Error("Error processing file", zap.String("file uid", file.UID.String()), zap.Error(err))
				err = wp.svc.Repository.UpdateKbFileExtraMetaData(ctx, file.UID, err.Error(), "", "", "", nil, nil, nil, nil)
				if err != nil {
					fmt.Printf("Error marshaling extra metadata: %v\n", err)
				}
				_, err := wp.svc.Repository.UpdateKnowledgeBaseFile(ctx, file.UID.String(), map[string]interface{}{
					repository.KnowledgeBaseFileColumn.ProcessStatus: artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_FAILED.String(),
				})
				if err != nil {
					fmt.Printf("Error updating file status: %v\n", err)
				}
			} else {
				fmt.Printf("Worker %d finished processing fileUID: %s\n", workerID, file.UID.String())
			}
			processingTime := int64(time.Since(t0).Seconds())
			err = wp.svc.Repository.UpdateKbFileExtraMetaData(ctx, file.UID, "", "", "", "", &processingTime, nil, nil, nil)
			if err != nil {
				fmt.Printf("Error updating file extra metadata: %v\n", err)
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

// registerFileWorker registers a file worker in the worker pool and sets a worker key in Redis with the given fileUID and workerLifetime.
// It periodically extends the worker's lifetime in Redis until the worker is done processing.
// It returns a boolean indicating success and a stopRegisterWorkerFunc that can be used to cancel the worker's lifetime extension and remove the worker key from Redis.
// period: duration between lifetime extensions
// workerLifetime: total duration the worker key should be kept in Redis
func (wp *fileToEmbWorkerPool) registerFileWorker(ctx context.Context, fileUID string, period time.Duration, workerLifetime time.Duration) (ok bool, stopRegisterWorker stopRegisterWorkerFunc) {
	logger, _ := logger.GetZapLogger(ctx)
	stopRegisterWorker = func() {
		logger.Warn("stopRegisterWorkerFunc is not implemented yet")
	}
	ok, err := wp.svc.RedisClient.SetNX(ctx, getWorkerKey(fileUID), "1", workerLifetime).Result()
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
				err := wp.svc.RedisClient.Expire(ctx, getWorkerKey(fileUID), workerLifetime).Err()
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
		wp.svc.RedisClient.Del(ctx, getWorkerKey(fileUID))
	}

	return true, stopRegisterWorker
}

// checkFileWorker checks if any of the provided fileUIDs have active workers
func (wp *fileToEmbWorkerPool) checkRegisteredFilesWorker(ctx context.Context, fileUIDs []string) map[string]struct{} {
	logger, _ := logger.GetZapLogger(ctx)
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

// processFile handles the processing of a file through various stages using a state machine.
func (wp *fileToEmbWorkerPool) processFile(ctx context.Context, file repository.KnowledgeBaseFile) error {
	logger, _ := logger.GetZapLogger(ctx)
	var status artifactpb.FileProcessStatus
	if statusInt, ok := artifactpb.FileProcessStatus_value[file.ProcessStatus]; !ok {
		return fmt.Errorf("invalid process status: %v", file.ProcessStatus)
	} else {
		status = artifactpb.FileProcessStatus(statusInt)
	}

	// check if the file is already processed
	err := wp.checkFileStatus(ctx, file)
	if err != nil {
		return err
	}

	for {
		switch status {
		case artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_WAITING:
			updatedFile, nextStatus, err := wp.processWaitingFile(ctx, file)
			if err != nil {
				return fmt.Errorf("error processing waiting file: %w", err)
			}
			status = nextStatus
			file = *updatedFile
		case artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CONVERTING:
			t0 := time.Now()
			updatedFile, nextStatus, err := wp.processConvertingFile(ctx, file)
			if err != nil {
				return fmt.Errorf("error processing converting file: %w", err)
			}
			status = nextStatus
			file = *updatedFile
			convertingTime := int64(time.Since(t0).Seconds())
			err = wp.svc.Repository.UpdateKbFileExtraMetaData(ctx, file.UID, "", "", "", "", nil, &convertingTime, nil, nil)
			if err != nil {
				logger.Error("Error updating file extra metadata", zap.Error(err))
			}

		case artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING:
			t0 := time.Now()
			updatedFile, nextStatus, err := wp.processChunkingFile(ctx, file)
			if err != nil {
				return fmt.Errorf("error processing chunking file: %w", err)
			}
			status = nextStatus
			file = *updatedFile
			chunkingTime := int64(time.Since(t0).Seconds())
			err = wp.svc.Repository.UpdateKbFileExtraMetaData(ctx, file.UID, "", "", "", "", nil, nil, &chunkingTime, nil)
			if err != nil {
				logger.Error("Error updating file extra metadata", zap.Error(err))
			}

		case artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING:
			t0 := time.Now()
			updatedFile, nextStatus, err := wp.processEmbeddingFile(ctx, file)
			if err != nil {
				return fmt.Errorf("error processing embedding file: %w", err)
			}
			status = nextStatus
			file = *updatedFile
			embeddingTime := int64(time.Since(t0).Seconds())
			err = wp.svc.Repository.UpdateKbFileExtraMetaData(ctx, file.UID, "", "", "", "", nil, nil, nil, &embeddingTime)
			if err != nil {
				logger.Error("Error updating file extra metadata", zap.Error(err))
			}

		case artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED:
			err := wp.processCompletedFile(ctx, file)
			if err != nil {
				return err
			}
			return nil
		}
	}
}

// processWaitingFile determines the next status based on the file type.
// For pdf, doc, docx, ppt, pptx, html, and xlsx files, it transitions to the converting status.
// For text and markdown files, it transitions to the chunking status.
// For unsupported file types, it returns an error.
func (wp *fileToEmbWorkerPool) processWaitingFile(ctx context.Context, file repository.KnowledgeBaseFile) (updatedFile *repository.KnowledgeBaseFile, nextStatus artifactpb.FileProcessStatus, err error) {
	// check if file process status is waiting
	if file.ProcessStatus != artifactpb.FileProcessStatus_name[int32(artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_WAITING)] {
		return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, fmt.Errorf("file process status should be waiting. status: %v", file.ProcessStatus)
	}

	// Determine the next status based on the file type.
	switch file.Type {
	// For pdf, doc, docx, ppt, pptx, html, and xlsx files, it transitions to the converting status.
	case artifactpb.FileType_FILE_TYPE_PDF.String(),
		artifactpb.FileType_FILE_TYPE_DOC.String(),
		artifactpb.FileType_FILE_TYPE_DOCX.String(),
		artifactpb.FileType_FILE_TYPE_PPT.String(),
		artifactpb.FileType_FILE_TYPE_PPTX.String(),
		artifactpb.FileType_FILE_TYPE_HTML.String(),
		artifactpb.FileType_FILE_TYPE_XLSX.String(),
		artifactpb.FileType_FILE_TYPE_XLS.String(),
		artifactpb.FileType_FILE_TYPE_CSV.String():
		// update the file status to converting status in database
		updateMap := map[string]interface{}{
			repository.KnowledgeBaseFileColumn.ProcessStatus: artifactpb.FileProcessStatus_name[int32(artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CONVERTING)],
		}
		updatedFile, err := wp.svc.Repository.UpdateKnowledgeBaseFile(ctx, file.UID.String(), updateMap)
		if err != nil {
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}
		return updatedFile, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CONVERTING, nil

	// For text and markdown files, it transitions to the chunking status.
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

// processConvertingFile processes a file with converting status.
// If the file is a PDF, it retrieves the file from MinIO, converts it to Markdown using the PDF-to-Markdown pipeline, and then transitions to chunking status.
// The converted file is saved into object storage and the metadata is updated in the database.
// Finally, the file status is updated to chunking in the database.
// If the file is not a PDF, it returns an error.
func (wp *fileToEmbWorkerPool) processConvertingFile(ctx context.Context, file repository.KnowledgeBaseFile) (updatedFile *repository.KnowledgeBaseFile, nextStatus artifactpb.FileProcessStatus, err error) {
	logger, _ := logger.GetZapLogger(ctx)

	fileInMinIOPath := file.Destination
	data, err := wp.svc.MinIO.GetFile(ctx, minio.KnowledgeBaseBucketName, fileInMinIOPath)
	if err != nil {
		logger.Error("Failed to get file from minIO.", zap.String("File path", fileInMinIOPath))
		return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
	}

	// encode data to base64
	base64Data := base64.StdEncoding.EncodeToString(data)

	// save the converting pipeline metadata into database
	convertingPipelineMetadata := service.NamespaceID + "/" + service.ConvertPDFToMDPipelineID + "@" + service.PDFToMDVersion
	err = wp.svc.Repository.UpdateKbFileExtraMetaData(ctx, file.UID, "", convertingPipelineMetadata, "", "", nil, nil, nil, nil)
	if err != nil {
		logger.Error("Failed to save converting pipeline metadata.", zap.String("File uid:", file.UID.String()))
		return nil,
			artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED,
			fmt.Errorf("failed to save converting pipeline metadata: %w", err)
	}

	// convert the pdf file to md
	requesterUID := file.RequesterUID
	convertedMD, err := wp.svc.ConvertToMDPipe(ctx, file.CreatorUID, requesterUID, base64Data, artifactpb.FileType(artifactpb.FileType_value[file.Type]))
	if err != nil {
		logger.Error("Failed to convert pdf to md using pdf-to-md pipeline.", zap.String("File path", fileInMinIOPath))
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

// Processes a file with the status "chunking".
// If the file is a PDF or other document type, it retrieves the converted file from MinIO and calls the markdown chunking pipeline.
// If the file is a text or markdown file, it retrieves the file from MinIO and calls the respective chunking pipeline.
// The resulting chunks are saved into object storage and metadata is updated in the database.
// Finally, the file status is updated to "embedding" in the database.
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
	case artifactpb.FileType_FILE_TYPE_PDF.String(),
		artifactpb.FileType_FILE_TYPE_DOC.String(),
		artifactpb.FileType_FILE_TYPE_DOCX.String(),
		artifactpb.FileType_FILE_TYPE_PPT.String(),
		artifactpb.FileType_FILE_TYPE_PPTX.String(),
		artifactpb.FileType_FILE_TYPE_HTML.String(),
		artifactpb.FileType_FILE_TYPE_XLSX.String(),
		artifactpb.FileType_FILE_TYPE_XLS.String(),
		artifactpb.FileType_FILE_TYPE_CSV.String():
		// get the converted file metadata from database
		convertedFile, err := wp.svc.Repository.GetConvertedFileByFileUID(ctx, file.UID)
		if err != nil {
			logger.Error("Failed to get converted file metadata.", zap.String("File uid", file.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}
		// get the converted file from minIO
		convertedFileData, err := wp.svc.MinIO.GetFile(ctx, minio.KnowledgeBaseBucketName, convertedFile.Destination)
		if err != nil {
			logger.Error("Failed to get converted file from minIO.", zap.String("Converted file uid", convertedFile.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}
		// call the markdown chunking pipeline
		requesterUID := file.RequesterUID
		chunks, err := wp.svc.SplitMarkdownPipe(ctx, file.CreatorUID, requesterUID, string(convertedFileData))
		if err != nil {
			logger.Error("Failed to get chunks from converted file using markdown chunking pipeline.", zap.String("Converted file uid", convertedFile.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}

		//  Save the chunks into object storage(minIO) and metadata into database
		err = wp.saveChunks(ctx, file.KnowledgeBaseUID.String(), file.UID, wp.svc.Repository.ConvertedFileTableName(), convertedFile.UID, chunks)
		if err != nil {
			logger.Error("Failed to save chunks into object storage and metadata into database.", zap.String("File uid", file.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}
		// save chunking pipeline metadata into file's extra metadata
		chunkingPipelineMetadata := service.NamespaceID + "/" + service.MdSplitPipelineID + "@" + service.MdSplitVersion
		err = wp.svc.Repository.UpdateKbFileExtraMetaData(ctx, file.UID, "", "", chunkingPipelineMetadata, "", nil, nil, nil, nil)
		if err != nil {
			logger.Error("Failed to save chunking pipeline metadata.", zap.String("File uid:", file.UID.String()))
			return nil,
				artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED,
				fmt.Errorf("failed to save chunking pipeline metadata: %w", err)
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
		originalFile, err := wp.svc.MinIO.GetFile(ctx, minio.KnowledgeBaseBucketName, file.Destination)
		if err != nil {
			logger.Error("Failed to get file from minIO.", zap.String("File uid", file.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}

		//  Call the text chunking pipeline
		requesterUID := file.RequesterUID
		chunks, err := wp.svc.SplitTextPipe(ctx, file.CreatorUID, requesterUID, string(originalFile))
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
		// save chunking pipeline metadata into file's extra metadata
		chunkingPipelineMetadata := service.NamespaceID + "/" + service.TextSplitPipelineID + "@" + service.TextSplitVersion
		err = wp.svc.Repository.UpdateKbFileExtraMetaData(ctx, file.UID, "", "", chunkingPipelineMetadata, "", nil, nil, nil, nil)
		if err != nil {
			logger.Error("Failed to save chunking pipeline metadata.", zap.String("File uid:", file.UID.String()))
			return nil,
				artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED,
				fmt.Errorf("failed to save chunking pipeline metadata: %w", err)
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
		originalFile, err := wp.svc.MinIO.GetFile(ctx, minio.KnowledgeBaseBucketName, file.Destination)
		if err != nil {
			logger.Error("Failed to get file from minIO.", zap.String("File uid", file.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}

		// save chunking pipeline metadata into file's extra metadata
		chunkingPipelineMetadata := service.NamespaceID + "/" + service.MdSplitPipelineID + "@" + service.MdSplitVersion
		err = wp.svc.Repository.UpdateKbFileExtraMetaData(ctx, file.UID, "", "", chunkingPipelineMetadata, "", nil, nil, nil, nil)
		if err != nil {
			logger.Error("Failed to save chunking pipeline metadata.", zap.String("File uid:", file.UID.String()))
			return nil,
				artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED,
				fmt.Errorf("failed to save chunking pipeline metadata: %w", err)
		}

		//  Call the text chunking pipeline
		requesterUID := file.RequesterUID
		chunks, err := wp.svc.SplitMarkdownPipe(ctx, file.CreatorUID, requesterUID, string(originalFile))
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

// processEmbeddingFile processes a file with embedding status.
// It retrieves chunks from MinIO, calls the embedding pipeline, saves the embeddings into the vector database and metadata into the database,
// and updates the file status to completed in the database.
func (wp *fileToEmbWorkerPool) processEmbeddingFile(ctx context.Context, file repository.KnowledgeBaseFile) (updatedFile *repository.KnowledgeBaseFile, nextStatus artifactpb.FileProcessStatus, err error) {
	logger, _ := logger.GetZapLogger(ctx)
	// check the file status is embedding
	if file.ProcessStatus != artifactpb.FileProcessStatus_name[int32(artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING)] {
		return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, fmt.Errorf("file process status should be embedding. status: %v", file.ProcessStatus)
	}

	sourceTable, sourceUID, chunks, _, texts, err := wp.svc.GetChunksByFile(ctx, &file)
	if err != nil {
		logger.Error("Failed to get chunks from database first time.", zap.String("SourceUID", sourceUID.String()))
		// TODO: investigate minIO failure. Ref: Last-Modified time format not recognized. Please report this issue at https://github.com/minio/minio-go/issues.
		// retry once when get chunks failed
		time.Sleep(1 * time.Second)
		logger.Info("Retrying to get chunks from database.", zap.String("SourceUID", sourceUID.String()))
		sourceTable, sourceUID, chunks, _, texts, err = wp.svc.GetChunksByFile(ctx, &file)
		if err != nil {
			logger.Error("Failed to get chunks from database second time.", zap.String("SourceUID", sourceUID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}
	}

	// save embedding pipeline metadata into file's extra metadata
	embeddingPipelineMetadata := service.NamespaceID + "/" + service.TextEmbedPipelineID + "@" + service.TextEmbedVersion
	err = wp.svc.Repository.UpdateKbFileExtraMetaData(ctx, file.UID, "", "", "", embeddingPipelineMetadata, nil, nil, nil, nil)
	if err != nil {
		logger.Error("Failed to save embedding pipeline metadata.", zap.String("File uid:", file.UID.String()))
		return nil,
			artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED,
			fmt.Errorf("failed to save embedding pipeline metadata: %w", err)
	}

	// call the embedding pipeline
	requesterUID := file.RequesterUID
	vectors, err := wp.svc.EmbeddingTextPipe(ctx, file.CreatorUID, requesterUID, texts)
	if err != nil {
		logger.Error("Failed to get embeddings from chunks. using embedding pipeline", zap.String("SourceTable", sourceTable), zap.String("SourceUID", sourceUID.String()))
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

	// update the file status to complete status in database
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

// processCompletedFile logs the completion of the file-to-embeddings process.
// It checks if the file status is completed and logs the information.
func (wp *fileToEmbWorkerPool) processCompletedFile(ctx context.Context, file repository.KnowledgeBaseFile) error {
	logger, _ := logger.GetZapLogger(ctx)
	logger.Info("File to embeddings process completed.", zap.String("File uid", file.UID.String()))
	return nil
}

// saveConvertedFile saves a converted file into object storage and updates the metadata in the database.
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

// saveChunks saves chunks into object storage and updates the metadata in the database.
func (wp *fileToEmbWorkerPool) saveChunks(ctx context.Context, kbUID string, kbFileUID uuid.UUID, sourceTable string, sourceUID uuid.UUID, chunks []chunk) error {
	logger, _ := logger.GetZapLogger(ctx)
	textChunks := make([]*repository.TextChunk, len(chunks))

	// turn kbUid to uuid no must parse
	kbUIDuuid, err := uuid.FromString(kbUID)
	if err != nil {
		logger.Error("Failed to parse kbUID to uuid.", zap.String("KbUID", kbUID))
		return err
	}
	for i, c := range chunks {
		textChunks[i] = &repository.TextChunk{
			SourceUID:   sourceUID,
			SourceTable: sourceTable,
			StartPos:    c.Start,
			EndPos:      c.End,
			ContentDest: "not set yet because we need to save the chunks in db to get the uid",
			Tokens:      c.Tokens,
			Retrievable: true,
			InOrder:     i,
			KbUID:       kbUIDuuid,
			KbFileUID:   kbFileUID,
		}
	}
	_, err = wp.svc.Repository.DeleteAndCreateChunks(ctx, sourceTable, sourceUID, textChunks,
		func(chunkUIDs []string) (map[string]any, error) {
			// save the chunksForMinIO into object storage
			chunksForMinIO := make(map[minio.ChunkUIDType]minio.ChunkContentType, len(textChunks))
			for i, uid := range chunkUIDs {
				chunksForMinIO[minio.ChunkUIDType(uid)] = minio.ChunkContentType([]byte(chunks[i].Text))
			}
			err := wp.svc.MinIO.SaveTextChunks(ctx, kbUID, chunksForMinIO)
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

// saveEmbeddings saves embeddings into the vector database and updates the metadata in the database.
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

// checkFileStatus checks if the file status from argument is the same as the file in database
func (wp *fileToEmbWorkerPool) checkFileStatus(ctx context.Context, file repository.KnowledgeBaseFile) error {
	dbFiles, err := wp.svc.Repository.GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{file.UID})
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

func getWorkerKey(fileUID string) string {
	return workerPrefix + fileUID
}

// mockVectorizeText is a mock implementation of the VectorizeText function.
// It generates mock vectors for the given texts.
//
//nolint:unused
func mockVectorizeText(_ context.Context, _ uuid.UUID, texts []string) ([][]float32, error) {
	vectors := make([][]float32, len(texts))
	for i := range texts {
		vectors[i] = make([]float32, milvus.VectorDim)
	}
	return vectors, nil
}
