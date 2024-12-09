package worker

import (
	"context"
	"encoding/base64"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/minio"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"
	"github.com/instill-ai/artifact-backend/pkg/utils"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	"go.uber.org/zap"
)

type persistentCatalogFileToEmbWorkerPool struct {
	numberOfWorkers int
	svc             *service.Service
	channel         chan repository.KnowledgeBaseFile
	wg              sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
	catalogType     artifactpb.CatalogType
}

func NewPersistentCatalogFileToEmbWorkerPool(ctx context.Context, svc *service.Service, nums int, catalogType artifactpb.CatalogType) *persistentCatalogFileToEmbWorkerPool {
	ctx, cancel := context.WithCancel(ctx)
	return &persistentCatalogFileToEmbWorkerPool{
		numberOfWorkers: nums,
		svc:             svc,
		// channel is un-buffered because we dont want the out of date and duplicate file in queue
		channel:     make(chan repository.KnowledgeBaseFile),
		wg:          sync.WaitGroup{},
		ctx:         ctx,
		cancel:      cancel,
		catalogType: catalogType,
	}
}

func (wp *persistentCatalogFileToEmbWorkerPool) Start() {
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
func (wp *persistentCatalogFileToEmbWorkerPool) startDispatcher() {
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
			incompleteFiles := wp.svc.Repository.GetNeedProcessFiles(wp.ctx, wp.catalogType)
			// Check if any of the incomplete files have active workers
			fileUIDs := make([]string, len(incompleteFiles))
			for i, file := range incompleteFiles {
				fileUIDs[i] = file.UID.String()
			}
			nonExistentKeys := checkRegisteredFilesWorker(wp.ctx, wp.svc, fileUIDs)

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

func (wp *persistentCatalogFileToEmbWorkerPool) startWorker(ctx context.Context, workerID int) {
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
			ok, stopRegisterFunc := registerFileWorker(ctx, wp.svc, file.UID.String(), extensionHelperPeriod, workerLifetime)
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
			err := checkFileStatus(ctx, wp.svc, file)
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
func (wp *persistentCatalogFileToEmbWorkerPool) GraceFulStop() {
	logger, _ := logger.GetZapLogger(wp.ctx)
	logger.Info("Worker pool received termination signal")
	close(wp.channel)
	wp.cancel()
	wp.wg.Wait()
	logger.Info("Worker pool exited")
}

type stopRegisterWorkerFunc func()

// processFile handles the processing of a file through various stages using a state machine.
func (wp *persistentCatalogFileToEmbWorkerPool) processFile(ctx context.Context, file repository.KnowledgeBaseFile) error {
	logger, _ := logger.GetZapLogger(ctx)
	var status artifactpb.FileProcessStatus
	if statusInt, ok := artifactpb.FileProcessStatus_value[file.ProcessStatus]; !ok {
		return fmt.Errorf("invalid process status: %v", file.ProcessStatus)
	} else {
		status = artifactpb.FileProcessStatus(statusInt)
	}

	// check if the file is already processed
	err := checkFileStatus(ctx, wp.svc, file)
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
func (wp *persistentCatalogFileToEmbWorkerPool) processWaitingFile(ctx context.Context, file repository.KnowledgeBaseFile) (updatedFile *repository.KnowledgeBaseFile, nextStatus artifactpb.FileProcessStatus, err error) {
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
func (wp *persistentCatalogFileToEmbWorkerPool) processConvertingFile(ctx context.Context, file repository.KnowledgeBaseFile) (updatedFile *repository.KnowledgeBaseFile, nextStatus artifactpb.FileProcessStatus, err error) {
	logger, _ := logger.GetZapLogger(ctx)

	fileInMinIOPath := file.Destination
	data, err := wp.svc.MinIO.GetFile(ctx, minio.KnowledgeBaseBucketName, fileInMinIOPath)
	if err != nil {
		logger.Error("Failed to get file from minIO.", zap.String("File path", fileInMinIOPath))
		return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
	}

	// encode data to base64
	base64Data := base64.StdEncoding.EncodeToString(data)

	// convert the pdf file to md
	requesterUID := file.RequesterUID
	convertedMD, err := wp.svc.ConvertToMDPipeForFilesInPersistentCatalog(ctx, file.UID, file.CreatorUID, requesterUID, base64Data, artifactpb.FileType(artifactpb.FileType_value[file.Type]))
	if err != nil {
		logger.Error("Failed to convert pdf to md using pdf-to-md pipeline.", zap.String("File path", fileInMinIOPath))
		return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
	}

	// save the converted file into object storage and metadata into database
	err = saveConvertedFile(ctx, wp.svc, file.KnowledgeBaseUID, file.UID, "converted_"+file.Name, []byte(convertedMD))
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

// Processes a file with the status "chunking" by splitting it into text chunks.
// The processing varies by file type:
//
// For PDF, DOC, DOCX, PPT, PPTX, HTML, XLSX, XLS, CSV:
// - Retrieves converted file from MinIO
// - For spreadsheet files (XLSX, XLS, CSV): Uses markdown chunking pipeline
// - For other document types: Uses text chunking pipeline
//
// For TEXT files:
// - Retrieves original file from MinIO
// - Uses text chunking pipeline
//
// For MARKDOWN files:
// - Retrieves original file from MinIO
// - Uses markdown chunking pipeline
//
// For all file types:
// - Saves chunks to object storage
// - Updates metadata in database with chunking pipeline info
// - Updates file status to "embedding"
//
// Parameters:
//   - ctx: Context for the operation
//   - file: KnowledgeBaseFile struct containing file metadata
//
// Returns:
//   - updatedFile: Updated KnowledgeBaseFile after processing
//   - nextStatus: Next file process status (EMBEDDING if successful)
//   - err: Error if any step fails
//
// The function handles errors at each step and returns appropriate status codes.
func (wp *persistentCatalogFileToEmbWorkerPool) processChunkingFile(ctx context.Context, file repository.KnowledgeBaseFile) (*repository.KnowledgeBaseFile, artifactpb.FileProcessStatus, error) {
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
		chunks := []service.Chunk{}
		switch file.Type {
		case artifactpb.FileType_FILE_TYPE_XLSX.String(),
			artifactpb.FileType_FILE_TYPE_XLS.String(),
			artifactpb.FileType_FILE_TYPE_CSV.String(),
			artifactpb.FileType_FILE_TYPE_HTML.String():
			requesterUID := file.RequesterUID
			chunks, err = wp.svc.ChunkMarkdownPipe(ctx, file.CreatorUID, requesterUID, string(convertedFileData))
		case artifactpb.FileType_FILE_TYPE_PDF.String(),
			artifactpb.FileType_FILE_TYPE_DOCX.String(),
			artifactpb.FileType_FILE_TYPE_DOC.String(),
			artifactpb.FileType_FILE_TYPE_PPTX.String(),
			artifactpb.FileType_FILE_TYPE_PPT.String():
			requesterUID := file.RequesterUID
			chunks, err = wp.svc.ChunkTextPipeForPersistentCatalog(ctx, file.CreatorUID, requesterUID, string(convertedFileData))
		}
		if err != nil {
			logger.Error("Failed to get chunks from converted file using markdown chunking pipeline.", zap.String("Converted file uid", convertedFile.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}

		//  Save the chunks into object storage(minIO) and metadata into database
		err = saveChunks(ctx, wp.svc, file.KnowledgeBaseUID.String(), file.UID, wp.svc.Repository.ConvertedFileTableName(), convertedFile.UID, chunks)
		if err != nil {
			logger.Error("Failed to save chunks into object storage and metadata into database.", zap.String("File uid", file.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}
		// save chunking pipeline metadata into file's extra metadata
		chunkingPipelineMetadata := service.NamespaceID + "/" + service.ChunkMdPipelineID + "@" + service.ChunkMdVersion
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
		chunks, err := wp.svc.ChunkTextPipeForPersistentCatalog(ctx, file.CreatorUID, requesterUID, string(originalFile))
		if err != nil {
			logger.Error("Failed to get chunks from original file.", zap.String("File uid", file.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}
		//  Save the chunks into object storage(minIO) and metadata into database
		err = saveChunks(ctx, wp.svc, file.KnowledgeBaseUID.String(), file.UID, wp.svc.Repository.KnowledgeBaseFileTableName(), file.UID, chunks)
		if err != nil {
			logger.Error("Failed to save chunks into object storage and metadata into database.", zap.String("File uid", file.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}
		// save chunking pipeline metadata into file's extra metadata
		chunkingPipelineMetadata := service.NamespaceID + "/" + service.ChunkTextPipelineID + "@" + service.ChunkTextVersion
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
		chunkingPipelineMetadata := service.NamespaceID + "/" + service.ChunkMdPipelineID + "@" + service.ChunkMdVersion
		err = wp.svc.Repository.UpdateKbFileExtraMetaData(ctx, file.UID, "", "", chunkingPipelineMetadata, "", nil, nil, nil, nil)
		if err != nil {
			logger.Error("Failed to save chunking pipeline metadata.", zap.String("File uid:", file.UID.String()))
			return nil,
				artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED,
				fmt.Errorf("failed to save chunking pipeline metadata: %w", err)
		}

		//  Call the text chunking pipeline
		requesterUID := file.RequesterUID
		chunks, err := wp.svc.ChunkMarkdownPipe(ctx, file.CreatorUID, requesterUID, string(originalFile))
		if err != nil {
			logger.Error("Failed to get chunks from original file.", zap.String("File uid", file.UID.String()))
			return nil, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
		}

		//  Save the chunks into object storage(minIO) and metadata into database
		err = saveChunks(ctx, wp.svc, file.KnowledgeBaseUID.String(), file.UID, wp.svc.Repository.KnowledgeBaseFileTableName(), file.UID, chunks)
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

// processEmbeddingFile processes a file that is ready for embedding by:
// 1. Validating the file's process status is "EMBEDDING"
// 2. Retrieving text chunks from MinIO storage and database metadata
//   - Will retry once if initial chunk retrieval fails
//
// 3. Updating file metadata with embedding pipeline version info
//   - Uses TextEmbedPipelineID and TextEmbedVersion from service config
//
// 4. Calling the embedding pipeline to generate vectors from text chunks
//   - Uses file creator and requester UIDs for pipeline execution
//
// 5. Saving embeddings to vector database (Milvus) and metadata to SQL database
//   - Creates embeddings collection named after knowledge base UID
//   - Links embeddings to source text chunks and file metadata
//
// 6. Updating file status to "COMPLETED" in database
//
// Parameters:
//   - ctx: Context for the operation
//   - file: KnowledgeBaseFile struct containing file metadata
//
// Returns:
//   - updatedFile: Updated KnowledgeBaseFile after processing
//   - nextStatus: Next file process status (COMPLETED if successful)
//   - err: Error if any step fails
//
// The function handles errors at each step and returns appropriate status codes.
// If chunk retrieval fails initially, it will retry once after a 1 second delay.
func (wp *persistentCatalogFileToEmbWorkerPool) processEmbeddingFile(ctx context.Context, file repository.KnowledgeBaseFile) (updatedFile *repository.KnowledgeBaseFile, nextStatus artifactpb.FileProcessStatus, err error) {
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
	embeddingPipelineMetadata := service.NamespaceID + "/" + service.EmbedTextPipelineID + "@" + service.EmbedTextVersion
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
	err = saveEmbeddings(ctx, wp.svc, file.KnowledgeBaseUID.String(), embeddings)
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
func (wp *persistentCatalogFileToEmbWorkerPool) processCompletedFile(ctx context.Context, file repository.KnowledgeBaseFile) error {
	logger, _ := logger.GetZapLogger(ctx)
	logger.Info("File to embeddings process completed.", zap.String("File uid", file.UID.String()))
	return nil
}