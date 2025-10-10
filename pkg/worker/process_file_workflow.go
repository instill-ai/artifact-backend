package worker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/instill-ai/artifact-backend/pkg/pipeline"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

// ProcessFileWorkflowParam defines the parameters for ProcessFileWorkflow
type ProcessFileWorkflowParam struct {
	FileUIDs     []types.FileUIDType    // File unique identifiers (supports batch processing)
	KBUID        types.KBUIDType        // Knowledge base unique identifier
	UserUID      types.UserUIDType      // User unique identifier
	RequesterUID types.RequesterUIDType // Requester unique identifier (for access control)
}

type processFileWorkflow struct {
	temporalClient client.Client
	worker         *Worker
}

// NewProcessFileWorkflow creates a new ProcessFileWorkflow instance
func NewProcessFileWorkflow(temporalClient client.Client, worker *Worker) *processFileWorkflow {
	return &processFileWorkflow{
		temporalClient: temporalClient,
		worker:         worker,
	}
}

func (w *processFileWorkflow) Execute(ctx context.Context, param ProcessFileWorkflowParam) error {
	// Generate workflow ID based on file count
	var workflowID string
	if len(param.FileUIDs) == 1 {
		workflowID = fmt.Sprintf("process-file-%s", param.FileUIDs[0].String())
	} else {
		// For batch processing, use first file UID as prefix
		workflowID = fmt.Sprintf("process-files-batch-%s-count-%d", param.FileUIDs[0].String(), len(param.FileUIDs))
	}

	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	_, err := w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, w.worker.ProcessFileWorkflow, param)
	return err
}

// ProcessFileWorkflow orchestrates the file processing pipeline for multiple files
// with optimized batch caching for improved performance.
//
// Architecture (Batch Processing):
// 1. Format conversion for all files in parallel (DOCX→PDF, GIF→PNG, MKV→MP4, etc.)
// 2. Create n+1 AI caches using CONVERTED files:
//   - n individual caches (one per file) for content/summary processing
//   - 1 chat cache (all files together) for future Chat API
//
// 3. For each file in parallel:
//   - ProcessContentActivity: cleanup → markdown conversion → save to DB (using individual cache)
//   - ProcessSummaryActivity: summary generation → save to DB (using individual cache)
//
// 4. For each file: Chunk content/summary and generate embeddings
// 5. Store chat cache metadata and cleanup all caches
//
// Note: Format conversion happens BEFORE caching to ensure AI providers cache the correct file format
// (e.g., cache PDF instead of DOCX, cache PNG instead of GIF)
func (w *Worker) ProcessFileWorkflow(ctx workflow.Context, param ProcessFileWorkflowParam) error {
	logger := workflow.GetLogger(ctx)
	fileCount := len(param.FileUIDs)
	logger.Info("Starting ProcessFileWorkflow",
		"fileCount", fileCount,
		"fileUIDs", param.FileUIDs,
		"userUID", param.UserUID.String(),
		"requesterUID", param.RequesterUID.String())

	// Validate input
	if fileCount == 0 {
		return fmt.Errorf("no files provided for processing")
	}

	// Extract common UUIDs from parameters
	kbUID := param.KBUID
	userUID := param.UserUID
	requesterUID := param.RequesterUID

	// Track workflow completion status for each file
	filesCompleted := make(map[string]bool, fileCount)
	allFilesCompleted := false

	// Defer cleanup: If workflow is terminated/cancelled/timeout, mark all incomplete files as FAILED
	defer func() {
		if !allFilesCompleted {
			// Workflow did not complete successfully - mark incomplete files as failed
			// Use disconnected context so this runs even if workflow is cancelled
			cleanupCtx, _ := workflow.NewDisconnectedContext(ctx)
			cleanupCtx = workflow.WithActivityOptions(cleanupCtx, workflow.ActivityOptions{
				StartToCloseTimeout: time.Minute,
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval:    time.Second,
					BackoffCoefficient: 2.0,
					MaximumInterval:    30 * time.Second,
					MaximumAttempts:    3,
				},
			})

			logger.Warn("Workflow did not complete successfully, marking incomplete files as FAILED")

			// Mark each incomplete file as failed
			for _, fileUID := range param.FileUIDs {
				if !filesCompleted[fileUID.String()] {
					// Best effort update - ignore errors
					_ = workflow.ExecuteActivity(cleanupCtx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
						FileUID: fileUID,
						Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_FAILED,
						Message: "File processing was interrupted or terminated before completion",
					}).Get(cleanupCtx, nil)
				}
			}
		}
	}()

	// Set workflow options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: ActivityTimeoutLong,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    RetryInitialInterval,
			BackoffCoefficient: RetryBackoffCoefficient,
			MaximumInterval:    RetryMaximumIntervalLong,
			MaximumAttempts:    RetryMaximumAttempts,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Helper function to handle errors for a specific file
	handleFileError := func(fileUID types.FileUIDType, stage string, err error) error {
		logger.Error("Failed at stage for file", "stage", stage, "fileUID", fileUID.String(), "error", err)

		// Extract a clean error message for display
		errMsg := errorsx.MessageOrErr(err)

		// Update file status to FAILED
		statusErr := workflow.ExecuteActivity(ctx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
			FileUID: fileUID,
			Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_FAILED,
			Message: fmt.Sprintf("%s: %s", stage, errMsg),
		}).Get(ctx, nil)
		if statusErr != nil {
			logger.Error("Failed to update file status to FAILED", "fileUID", fileUID.String(), "statusError", statusErr)
		}

		// Return the error to mark the workflow as Failed in Temporal
		workflowErr := errorsx.AddMessage(
			fmt.Errorf("%s: %s", stage, errMsg),
			fmt.Sprintf("File %s processing failed at %s stage. %s", fileUID.String(), stage, errMsg),
		)
		return workflowErr
	}

	// Step 1: Get metadata for all files and check their statuses
	logger.Info("Fetching metadata for all files", "fileCount", fileCount)

	type fileMetadata struct {
		fileUID            types.FileUIDType
		metadata           GetFileMetadataActivityResult
		startStatus        artifactpb.FileProcessStatus
		bucket             string
		fileType           artifactpb.FileType
		shouldProcessFull  bool // Full processing (NOTSTARTED/PROCESSING)
		shouldProcessChunk bool // Resume from chunking
		shouldProcessEmbed bool // Resume from embedding
	}

	filesMetadata := make([]fileMetadata, 0, fileCount)

	for _, fileUID := range param.FileUIDs {
		// Get current file status
		var startStatus artifactpb.FileProcessStatus
		if err := workflow.ExecuteActivity(ctx, w.GetFileStatusActivity, &GetFileStatusActivityParam{
			FileUID: fileUID,
		}).Get(ctx, &startStatus); err != nil {
			return handleFileError(fileUID, "get file status", err)
		}

		// Handle different starting statuses
		if startStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED ||
			startStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_FAILED ||
			startStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CONVERTING ||
			startStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_SUMMARIZING {
			logger.Info("Reprocessing file from beginning",
				"fileUID", fileUID.String(),
				"previousStatus", startStatus.String())
			startStatus = artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_PROCESSING
		}

		// Get file metadata
		var metadata GetFileMetadataActivityResult
		if err := workflow.ExecuteActivity(ctx, w.GetFileMetadataActivity, &GetFileMetadataActivityParam{
			FileUID: fileUID,
			KBUID:   kbUID,
		}).Get(ctx, &metadata); err != nil {
			// If file not found, it may have been deleted - skip it
			if strings.Contains(err.Error(), "file not found") {
				logger.Info("File not found during processing, skipping",
					"fileUID", fileUID.String())
				filesCompleted[fileUID.String()] = true
				continue
			}
			return handleFileError(fileUID, "get file metadata", err)
		}

		bucket := repository.BucketFromDestination(metadata.File.Destination)
		fileType := artifactpb.FileType(artifactpb.FileType_value[metadata.File.Type])

		filesMetadata = append(filesMetadata, fileMetadata{
			fileUID:            fileUID,
			metadata:           metadata,
			startStatus:        startStatus,
			bucket:             bucket,
			fileType:           fileType,
			shouldProcessFull:  startStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_NOTSTARTED || startStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_PROCESSING,
			shouldProcessChunk: startStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING,
			shouldProcessEmbed: startStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING,
		})
	}

	// If all files were skipped, we're done
	if len(filesMetadata) == 0 {
		logger.Info("All files were skipped, workflow complete")
		allFilesCompleted = true
		return nil
	}

	// Step 2: Process files that need full processing (NOTSTARTED/PROCESSING)
	filesToProcessFull := make([]fileMetadata, 0)
	for _, fm := range filesMetadata {
		if fm.shouldProcessFull {
			filesToProcessFull = append(filesToProcessFull, fm)
		}
	}

	if len(filesToProcessFull) > 0 {
		logger.Info("Starting full processing for files", "count", len(filesToProcessFull))

		// Step 2a: Update all files status to PROCESSING
		for _, fm := range filesToProcessFull {
			if err := workflow.ExecuteActivity(ctx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
				FileUID: fm.fileUID,
				Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_PROCESSING,
			}).Get(ctx, nil); err != nil {
				logger.Warn("Failed to update status to PROCESSING", "fileUID", fm.fileUID.String(), "error", err)
			}
		}

		// Step 2b: Create chat cache for all files (for future Chat API)
		// Optimization: If only 1 file, we'll reuse its individual file cache instead of creating a separate chat cache
		var chatCacheFuture workflow.Future
		var chatCacheName string

		if fileCount > 1 {
			// Multiple files: Create a separate chat cache (runs in parallel with individual file processing)
			logger.Info("Creating chat cache for multiple files", "fileCount", len(filesToProcessFull))

			chatCacheFuture = workflow.ExecuteActivity(ctx, w.CacheChatContextActivity, &CacheChatContextActivityParam{
				FileUIDs: param.FileUIDs,
				KBUID:    kbUID,
				Metadata: filesToProcessFull[0].metadata.ExternalMetadata,
			})
		} else {
			// Single file: Will reuse the individual file cache as chat cache (no separate chat cache needed)
			logger.Info("Single file upload - will reuse file cache as chat cache")
			// chatCacheFuture will be set when we create the individual file cache below
		}

		// Store chat cache metadata in Redis (async, non-blocking)
		// This runs in parallel and doesn't block file processing
		workflow.Go(ctx, func(gCtx workflow.Context) {
			if chatCacheFuture != nil {
				var chatCacheResult CacheChatContextActivityResult
				if err := chatCacheFuture.Get(gCtx, &chatCacheResult); err != nil {
					logger.Warn("Failed to get chat cache result for Redis storage",
						"error", err)
					return
				}

				// Store metadata in Redis even if cached context is not enabled (for uncached file refs)
				// Skip only if no cached context AND no file references
				if !chatCacheResult.CachedContextEnabled && len(chatCacheResult.FileRefs) == 0 {
					logger.Info("No chat cache or file references, skipping Redis storage")
					return
				}

				// Calculate TTL based on expire time
				ttl := chatCacheResult.ExpireTime.Sub(workflow.Now(gCtx))
				if ttl <= 0 {
					// For uncached files, use default TTL
					ttl = 5 * time.Minute
					chatCacheResult.CreateTime = workflow.Now(gCtx)
					chatCacheResult.ExpireTime = workflow.Now(gCtx).Add(ttl)
				}

				// Store metadata in Redis with TTL matching AI cache expiration (or default for uncached)
				storeCtx := workflow.WithActivityOptions(gCtx, workflow.ActivityOptions{
					StartToCloseTimeout: 30 * time.Second,
					RetryPolicy: &temporal.RetryPolicy{
						InitialInterval:    time.Second,
						BackoffCoefficient: 2.0,
						MaximumInterval:    10 * time.Second,
						MaximumAttempts:    3,
					},
				})

				err := workflow.ExecuteActivity(storeCtx, w.StoreChatCacheMetadataActivity, &StoreChatCacheMetadataActivityParam{
					KBUID:                kbUID,
					FileUIDs:             param.FileUIDs,
					CacheName:            chatCacheResult.CacheName,
					Model:                chatCacheResult.Model,
					FileCount:            len(param.FileUIDs),
					CreateTime:           chatCacheResult.CreateTime,
					ExpireTime:           chatCacheResult.ExpireTime,
					TTL:                  ttl,
					CachedContextEnabled: chatCacheResult.CachedContextEnabled,
					FileRefs:             chatCacheResult.FileRefs,
				}).Get(storeCtx, nil)

				if err != nil {
					logger.Warn("Failed to store chat cache metadata in Redis (non-fatal)",
						"cachedContextEnabled", chatCacheResult.CachedContextEnabled,
						"fileRefCount", len(chatCacheResult.FileRefs),
						"error", err)
				} else {
					if chatCacheResult.CachedContextEnabled {
						logger.Info("Chat cache metadata stored in Redis",
							"cacheName", chatCacheResult.CacheName,
							"fileCount", len(param.FileUIDs),
							"ttl", ttl)
					} else {
						logger.Info("File content stored in Redis for uncached chat",
							"fileCount", len(chatCacheResult.FileRefs),
							"ttl", ttl)
					}
				}
			}
		})

		// Schedule cleanup for chat cache at the end
		// This handles both cases:
		// - Multi-file: cleanup the chat cache
		// - Single-file: cleanup the reused file cache
		defer func() {
			if chatCacheFuture != nil {
				var chatCacheResult CacheChatContextActivityResult
				if err := chatCacheFuture.Get(ctx, &chatCacheResult); err == nil && chatCacheResult.CachedContextEnabled {
					// Use the cache name from the result if we haven't set it yet
					if chatCacheName == "" {
						chatCacheName = chatCacheResult.CacheName
					}

					cleanupCtx, _ := workflow.NewDisconnectedContext(ctx)
					cleanupCtx = workflow.WithActivityOptions(cleanupCtx, workflow.ActivityOptions{
						StartToCloseTimeout: time.Minute,
						RetryPolicy: &temporal.RetryPolicy{
							InitialInterval:    time.Second,
							BackoffCoefficient: 2.0,
							MaximumInterval:    30 * time.Second,
							MaximumAttempts:    3,
						},
					})

					if err := workflow.ExecuteActivity(cleanupCtx, w.DeleteCacheActivity, &DeleteCacheActivityParam{
						CacheName: chatCacheName,
					}).Get(cleanupCtx, nil); err != nil {
						logger.Warn("Chat cache cleanup failed (cache will expire automatically)",
							"cacheName", chatCacheName, "error", err)
					} else {
						cacheType := "chat (multi-file)"
						if fileCount == 1 {
							cacheType = "chat (reused file cache)"
						}
						logger.Info("Chat cache cleaned up successfully",
							"cacheName", chatCacheName, "type", cacheType)
					}
				}
			}
		}()

		// Step 2c: File type standardization for all files (DOCX→PDF, etc.) - MUST happen before caching
		logger.Info("Starting file type standardization for all files", "fileCount", len(filesToProcessFull))

		type stdFileResult struct {
			fileUID              types.FileUIDType
			fileMetadata         *fileMetadata
			effectiveBucket      string
			effectiveDestination string
			effectiveFileType    artifactpb.FileType
			convertedBucket      string
			convertedDestination string
			converted            bool
		}

		stdResults := make([]stdFileResult, len(filesToProcessFull))

		// Convert all files in parallel
		for i, fm := range filesToProcessFull {
			fm := fm // Create a copy for closure/pointer safety

			var result StandardizeFileTypeActivityResult
			if err := workflow.ExecuteActivity(ctx, w.StandardizeFileTypeActivity, &StandardizeFileTypeActivityParam{
				FileUID:     fm.fileUID,
				KBUID:       kbUID,
				Bucket:      fm.bucket,
				Destination: fm.metadata.File.Destination,
				FileType:    fm.fileType,
				Filename:    fm.metadata.File.Name,
				Pipelines:   []pipeline.PipelineRelease{pipeline.ConvertFileTypePipeline},
				Metadata:    fm.metadata.ExternalMetadata,
			}).Get(ctx, &result); err != nil {
				// Format conversion failure is not fatal - continue with original file
				// Note: This may cause downstream AI providers to fail if they don't support the original format
				logger.Error("Format conversion failed, continuing with original file (AI may reject this format)",
					"fileUID", fm.fileUID.String(),
					"originalType", fm.fileType.String(),
					"error", err)
				result = StandardizeFileTypeActivityResult{
					Converted:     false,
					ConvertedType: fm.fileType,
					OriginalType:  fm.fileType,
				}
			}

			// Determine effective file location for caching and processing
			effectiveBucket := fm.bucket
			effectiveDestination := fm.metadata.File.Destination
			effectiveFileType := result.ConvertedType

			if result.Converted {
				logger.Info("File format converted successfully",
					"fileUID", fm.fileUID.String(),
					"originalType", result.OriginalType.String(),
					"convertedType", result.ConvertedType.String())
				effectiveBucket = result.ConvertedBucket
				effectiveDestination = result.ConvertedDestination
			}

			stdResults[i] = stdFileResult{
				fileUID:              fm.fileUID,
				fileMetadata:         &fm,
				effectiveBucket:      effectiveBucket,
				effectiveDestination: effectiveDestination,
				effectiveFileType:    effectiveFileType,
				convertedBucket:      result.ConvertedBucket,
				convertedDestination: result.ConvertedDestination,
				converted:            result.Converted,
			}
		}

		// Step 2d: Create caches using the converted files
		// Each file: cache → content/summary → chunking → embedding (no blocking between files)
		logger.Info("Starting cache creation and file processing", "fileCount", len(stdResults))

		type fileProcessingFuture struct {
			fileUID         types.FileUIDType
			fileCacheFuture workflow.Future
			conversionData  *stdFileResult
		}

		processingFutures := make([]fileProcessingFuture, len(stdResults))

		// Start cache creation for each file immediately (using converted files)
		for i, cr := range stdResults {
			cr := cr // Create a copy for closure/pointer safety

			// Start file cache creation for this file (non-blocking)
			// Use the CONVERTED file for caching, not the original
			fileCacheFuture := workflow.ExecuteActivity(ctx, w.CacheFileContextActivity, &CacheFileContextActivityParam{
				FileUID:     cr.fileUID,
				KBUID:       kbUID,
				Bucket:      cr.effectiveBucket,
				Destination: cr.effectiveDestination,
				FileType:    cr.effectiveFileType,
				Filename:    cr.fileMetadata.metadata.File.Name,
				Metadata:    cr.fileMetadata.metadata.ExternalMetadata,
			})

			processingFutures[i] = fileProcessingFuture{
				fileUID:         cr.fileUID,
				fileCacheFuture: fileCacheFuture,
				conversionData:  &cr,
			}

			// For single file case: reuse the file cache as the chat cache
			if fileCount == 1 {
				chatCacheFuture = fileCacheFuture
				logger.Info("Single file: reusing file cache as chat cache")
			}

			// Schedule cleanup of temporary converted file (if conversion happened)
			if cr.converted && cr.convertedDestination != "" {
				defer func(bucket, destination string, fuid types.FileUIDType) {
					cleanupCtx, _ := workflow.NewDisconnectedContext(ctx)
					cleanupCtx = workflow.WithActivityOptions(cleanupCtx, workflow.ActivityOptions{
						StartToCloseTimeout: time.Minute,
						RetryPolicy: &temporal.RetryPolicy{
							InitialInterval:    time.Second,
							BackoffCoefficient: 2.0,
							MaximumInterval:    30 * time.Second,
							MaximumAttempts:    3,
						},
					})

					if err := workflow.ExecuteActivity(cleanupCtx, w.DeleteTemporaryConvertedFileActivity, &DeleteTemporaryConvertedFileActivityParam{
						Bucket:      bucket,
						Destination: destination,
					}).Get(cleanupCtx, nil); err != nil {
						logger.Warn("Temporary converted file cleanup failed", "fileUID", fuid.String(), "error", err)
					}
				}(cr.convertedBucket, cr.convertedDestination, cr.fileUID)
			}
		}

		// Now start content/summary activities for each file - they'll wait for their own file cache
		type fileWorkflowFutures struct {
			fileUID        types.FileUIDType
			contentFuture  workflow.Future // Activity future (not child workflow)
			summaryFuture  workflow.Future // Activity future (not child workflow)
			conversionData *stdFileResult
		}

		workflowFutures := make([]fileWorkflowFutures, len(processingFutures))

		for i, pf := range processingFutures {
			cr := pf.conversionData

			// Get file cache name for this file (wait only for THIS file's cache)
			var fileCacheResult CacheFileContextActivityResult
			var fileCacheName string
			if err := pf.fileCacheFuture.Get(ctx, &fileCacheResult); err != nil {
				logger.Warn("File cache creation failed (processing without cache)",
					"fileUID", pf.fileUID.String(), "error", err)
			} else if fileCacheResult.CachedContextEnabled {
				fileCacheName = fileCacheResult.CacheName
				logger.Info("File cache created",
					"fileUID", pf.fileUID.String(),
					"cacheName", fileCacheName)

				// For single file: cache is reused as chat cache, so skip individual cleanup
				// (chat cache cleanup will handle it)
				if fileCount > 1 {
					// Schedule file cache cleanup for this file (multi-file case only)
					defer func(cn string, fuid types.FileUIDType) {
						cleanupCtx, _ := workflow.NewDisconnectedContext(ctx)
						cleanupCtx = workflow.WithActivityOptions(cleanupCtx, workflow.ActivityOptions{
							StartToCloseTimeout: time.Minute,
							RetryPolicy: &temporal.RetryPolicy{
								InitialInterval:    time.Second,
								BackoffCoefficient: 2.0,
								MaximumInterval:    30 * time.Second,
								MaximumAttempts:    3,
							},
						})

						if err := workflow.ExecuteActivity(cleanupCtx, w.DeleteCacheActivity, &DeleteCacheActivityParam{
							CacheName: cn,
						}).Get(cleanupCtx, nil); err != nil {
							logger.Warn("File cache cleanup failed (will expire automatically)",
								"fileUID", fuid.String(), "cacheName", cn, "error", err)
						} else {
							logger.Info("File cache cleaned up", "fileUID", fuid.String(), "cacheName", cn)
						}
					}(fileCacheName, pf.fileUID)
				} else {
					// Store chat cache name for cleanup in the deferred chat cleanup function
					chatCacheName = fileCacheName
					logger.Info("Single file: cache will be cleaned up as chat cache",
						"cacheName", fileCacheName)
				}
			}

			// Start both content and summary activities in parallel for this file
			// Use the CONVERTED file location (effectiveBucket/effectiveDestination/effectiveFileType)
			contentFuture := workflow.ExecuteActivity(ctx, w.ProcessContentActivity, &ProcessContentActivityParam{
				FileUID:          cr.fileUID,
				KBUID:            kbUID,
				Bucket:           cr.effectiveBucket,
				Destination:      cr.effectiveDestination,
				FileType:         cr.effectiveFileType,
				Filename:         cr.fileMetadata.metadata.File.Name,
				FallbackPipeline: cr.fileMetadata.metadata.GenerateContentPipeline,
				Metadata:         cr.fileMetadata.metadata.ExternalMetadata,
				CacheName:        fileCacheName,
			})

			summaryFuture := workflow.ExecuteActivity(ctx, w.ProcessSummaryActivity, &ProcessSummaryActivityParam{
				FileUID:          cr.fileUID,
				KBUID:            kbUID,
				Bucket:           cr.effectiveBucket,
				Destination:      cr.effectiveDestination,
				FileName:         cr.fileMetadata.metadata.File.Name,
				FileType:         cr.effectiveFileType,
				Metadata:         cr.fileMetadata.metadata.ExternalMetadata,
				CacheName:        fileCacheName,
				FallbackPipeline: cr.fileMetadata.metadata.GenerateSummaryPipeline,
			})

			workflowFutures[i] = fileWorkflowFutures{
				fileUID:        cr.fileUID,
				contentFuture:  contentFuture,
				summaryFuture:  summaryFuture,
				conversionData: cr,
			}
		}

		// Wait for all activity executions to complete and collect results
		for _, wf := range workflowFutures {
			var contentResult ProcessContentActivityResult
			var summaryResult ProcessSummaryActivityResult

			contentErr := wf.contentFuture.Get(ctx, &contentResult)
			summaryErr := wf.summaryFuture.Get(ctx, &summaryResult)

			// Check for errors
			if contentErr != nil {
				_ = handleFileError(wf.fileUID, "process content workflow", contentErr)
				filesCompleted[wf.fileUID.String()] = true
				continue
			}
			if summaryErr != nil {
				_ = handleFileError(wf.fileUID, "process summary workflow", summaryErr)
				filesCompleted[wf.fileUID.String()] = true
				continue
			}

			logger.Info("File processing workflows completed successfully",
				"fileUID", wf.fileUID.String(),
				"summaryLength", len(summaryResult.Summary))

			// Update conversion metadata for this file
			pipelineName := ""
			if contentResult.ConversionPipeline.ID != "" {
				pipelineName = contentResult.ConversionPipeline.Name()
			}
			if err := workflow.ExecuteActivity(ctx, w.UpdateConversionMetadataActivity, &UpdateConversionMetadataActivityParam{
				FileUID:  wf.fileUID,
				Length:   contentResult.Length,
				Pipeline: pipelineName,
			}).Get(ctx, nil); err != nil {
				_ = handleFileError(wf.fileUID, "update conversion metadata", err)
				filesCompleted[wf.fileUID.String()] = true
				continue
			}

			// Step 2d: Process chunking and embedding for this file
			fileUID := wf.fileUID

			logger.Info("Starting CHUNKING phase for file", "fileUID", fileUID.String())

			// Update status to CHUNKING
			if err := workflow.ExecuteActivity(ctx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
				FileUID: fileUID,
				Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING,
			}).Get(ctx, nil); err != nil {
				logger.Warn("Failed to update status to CHUNKING", "fileUID", fileUID.String(), "error", err)
			}

			// Get converted file for text chunking
			var convertedFile GetConvertedFileForChunkingActivityResult
			if err := workflow.ExecuteActivity(ctx, w.GetConvertedFileForChunkingActivity, &GetConvertedFileForChunkingActivityParam{
				FileUID:           fileUID,
				KBUID:             kbUID,
				ChunkTextPipeline: wf.conversionData.fileMetadata.metadata.ChunkMarkdownPipeline,
				EmbedTextPipeline: wf.conversionData.fileMetadata.metadata.EmbedPipeline,
			}).Get(ctx, &convertedFile); err != nil {
				_ = handleFileError(fileUID, "get converted file for chunking", err)
				filesCompleted[fileUID.String()] = true
				continue
			}

			// Chunk content
			var contentChunks ChunkContentActivityResult
			if err := workflow.ExecuteActivity(ctx, w.ChunkContentActivity, &ChunkContentActivityParam{
				FileUID:           fileUID,
				KBUID:             kbUID,
				Content:           convertedFile.Content,
				ChunkTextPipeline: convertedFile.ChunkTextPipeline,
				Metadata:          wf.conversionData.fileMetadata.metadata.ExternalMetadata,
			}).Get(ctx, &contentChunks); err != nil {
				_ = handleFileError(fileUID, "chunk content", err)
				filesCompleted[fileUID.String()] = true
				continue
			}

			// Combine content and summary text chunks (if any)
			allTextChunks := contentChunks.TextChunks
			if len(summaryResult.Summary) > 0 {
				var summaryChunks ChunkContentActivityResult
				if err := workflow.ExecuteActivity(ctx, w.ChunkContentActivity, &ChunkContentActivityParam{
					FileUID:           fileUID,
					KBUID:             kbUID,
					Content:           summaryResult.Summary,
					ChunkTextPipeline: pipeline.PipelineRelease{}, // Use default
					Metadata:          wf.conversionData.fileMetadata.metadata.ExternalMetadata,
				}).Get(ctx, &summaryChunks); err != nil {
					_ = handleFileError(fileUID, "chunk summary", err)
					filesCompleted[fileUID.String()] = true
					continue
				}
				allTextChunks = append(allTextChunks, summaryChunks.TextChunks...)
			}

			// Save text chunks to DB
			if err := workflow.ExecuteActivity(ctx, w.SaveTextChunksToDBActivity, &SaveTextChunksToDBActivityParam{
				KBUID:      kbUID,
				FileUID:    fileUID,
				TextChunks: allTextChunks,
			}).Get(ctx, nil); err != nil {
				_ = handleFileError(fileUID, "save text chunks to DB", err)
				filesCompleted[fileUID.String()] = true
				continue
			}

			// Update text chunking metadata
			if err := workflow.ExecuteActivity(ctx, w.UpdateChunkingMetadataActivity, &UpdateChunkingMetadataActivityParam{
				FileUID:        fileUID,
				KBUID:          kbUID,
				Pipeline:       contentChunks.PipelineRelease.Name(),
				TextChunkCount: uint32(len(allTextChunks)),
			}).Get(ctx, nil); err != nil {
				_ = handleFileError(fileUID, "update chunking metadata", err)
				filesCompleted[fileUID.String()] = true
				continue
			}

			logger.Info("CHUNKING phase completed for file",
				"fileUID", fileUID.String(),
				"totalChunks", len(allTextChunks))

			// Start EMBEDDING phase
			logger.Info("Starting EMBEDDING phase for file", "fileUID", fileUID.String())

			// Update status to EMBEDDING
			if err := workflow.ExecuteActivity(ctx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
				FileUID: fileUID,
				Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING,
			}).Get(ctx, nil); err != nil {
				logger.Warn("Failed to update status to EMBEDDING", "fileUID", fileUID.String(), "error", err)
			}

			// Get text chunks from database
			var chunksData GetTextChunksForEmbeddingActivityResult
			if err := workflow.ExecuteActivity(ctx, w.GetChunksForEmbeddingActivity, &GetChunksForEmbeddingActivityParam{
				FileUID: fileUID,
			}).Get(ctx, &chunksData); err != nil {
				_ = handleFileError(fileUID, "get text chunks for embedding", err)
				filesCompleted[fileUID.String()] = true
				continue
			}

			// Generate embeddings using child workflow
			var requestMetadata map[string][]string
			if chunksData.Metadata != nil {
				md, err := extractRequestMetadata(chunksData.Metadata)
				if err != nil {
					_ = handleFileError(fileUID, "extract request metadata", err)
					filesCompleted[fileUID.String()] = true
					continue
				}
				requestMetadata = md
			}

			embedWorkflowOptions := workflow.ChildWorkflowOptions{
				WorkflowID: fmt.Sprintf("embed-texts-%s", fileUID.String()),
			}
			embedCtx := workflow.WithChildOptions(ctx, embedWorkflowOptions)

			var embeddingVectors [][]float32
			if err := workflow.ExecuteChildWorkflow(embedCtx, w.EmbedTextsWorkflow, EmbedTextsWorkflowParam{
				Texts:           chunksData.Texts,
				BatchSize:       32,
				RequestMetadata: requestMetadata,
			}).Get(embedCtx, &embeddingVectors); err != nil {
				_ = handleFileError(fileUID, "generate embeddings", err)
				filesCompleted[fileUID.String()] = true
				continue
			}

			// Build embedding records
			embeddings := make([]repository.EmbeddingModel, len(chunksData.Chunks))
			for j, chunk := range chunksData.Chunks {
				embeddings[j] = repository.EmbeddingModel{
					SourceTable: repository.TextChunkTableName,
					SourceUID:   chunk.UID,
					Vector:      embeddingVectors[j],
					KBUID:       kbUID,
					KBFileUID:   fileUID,
					FileType:    string(types.DocumentFileType),
					ContentType: chunk.ContentType,
					Tags:        chunksData.Tags,
				}
			}

			// Save embeddings to Milvus + DB
			childWorkflowOptions := workflow.ChildWorkflowOptions{
				WorkflowID: fmt.Sprintf("save-embeddings-%s", fileUID.String()),
			}
			childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)

			if err := workflow.ExecuteChildWorkflow(childCtx, w.SaveEmbeddingsToVectorDBWorkflow, SaveEmbeddingsToVectorDBWorkflowParam{
				KBUID:        kbUID,
				FileUID:      fileUID,
				FileName:     chunksData.FileName,
				Embeddings:   embeddings,
				UserUID:      userUID,
				RequesterUID: requesterUID,
			}).Get(childCtx, nil); err != nil {
				_ = handleFileError(fileUID, "save embeddings to vector DB", err)
				filesCompleted[fileUID.String()] = true
				continue
			}

			// Update embedding metadata
			if err := workflow.ExecuteActivity(ctx, w.UpdateEmbeddingMetadataActivity, &UpdateEmbeddingMetadataActivityParam{
				FileUID:  fileUID,
				Pipeline: pipeline.EmbedTextPipeline.Name(),
			}).Get(ctx, nil); err != nil {
				_ = handleFileError(fileUID, "update embedding metadata", err)
				filesCompleted[fileUID.String()] = true
				continue
			}

			// Update final status to COMPLETED
			if err := workflow.ExecuteActivity(ctx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
				FileUID: fileUID,
				Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED,
				Message: "File processing completed successfully",
			}).Get(ctx, nil); err != nil {
				_ = handleFileError(fileUID, "update final status", err)
				filesCompleted[fileUID.String()] = true
				continue
			}

			// Mark file as successfully completed
			filesCompleted[fileUID.String()] = true
			logger.Info("File processing completed successfully", "fileUID", fileUID.String())
		}
	}

	// Mark all successfully completed files
	allFilesCompleted = true
	logger.Info("ProcessFileWorkflow completed successfully",
		"fileCount", len(filesMetadata),
		"completedCount", len(filesCompleted))
	return nil
}

// TODO: Resume functionality for CHUNKING and EMBEDDING phases
// For now, batch processing only supports full processing from the start
// Individual file resume can be handled by processing files one at a time (batch size = 1)
