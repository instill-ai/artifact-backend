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

	"github.com/gofrs/uuid"
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
// Note: Format conversion happens BEFORE caching to ensure AI clients cache the correct file format
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
	filesFailed := make(map[string]error, fileCount) // Track failures to return error at end
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
	// We'll also get the KB's model family from the first file to determine caching strategy
	logger.Info("Fetching metadata for all files", "fileCount", fileCount)

	type fileMetadata struct {
		fileUID            types.FileUIDType
		metadata           GetFileMetadataActivityResult
		startStatus        artifactpb.FileProcessStatus
		bucket             string
		fileType           artifactpb.File_Type
		shouldProcessFull  bool // Full processing (NOTSTARTED/PROCESSING)
		shouldProcessChunk bool // Resume from chunking
		shouldProcessEmbed bool // Resume from embedding
	}

	filesMetadata := make([]fileMetadata, 0, fileCount)
	var kbModelFamily string                                // Will be set from first file's metadata
	var dualProcessingInfo *repository.DualProcessingTarget // Will be set from first file's metadata

	for _, fileUID := range param.FileUIDs {
		// Get current file status
		var startStatus artifactpb.FileProcessStatus
		if err := workflow.ExecuteActivity(ctx, w.GetFileStatusActivity, &GetFileStatusActivityParam{
			FileUID: fileUID,
		}).Get(ctx, &startStatus); err != nil {
			// If file not found, it may have been deleted during processing - mark as failed
			// This handles concurrent deletion scenarios (e.g., files deleted during KB updates)
			// GetFileStatusActivity returns: "File not found. It may have been deleted." (capital F)
			if strings.Contains(err.Error(), "File not found") {
				logger.Warn("File not found when checking status, marking as failed",
					"fileUID", fileUID.String())
				filesFailed[fileUID.String()] = handleFileError(fileUID, "get file status", err)
				filesCompleted[fileUID.String()] = true
				continue
			}
			return handleFileError(fileUID, "get file status", err)
		}

		// Handle different starting statuses
		if startStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED ||
			startStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_FAILED {
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
			// If file not found, it may have been deleted - mark as failed
			// GetFileMetadataActivity returns: "file not found: <uid>" (lowercase f)
			if strings.Contains(err.Error(), "file not found") {
				logger.Warn("File not found during processing, marking as failed",
					"fileUID", fileUID.String())
				filesFailed[fileUID.String()] = handleFileError(fileUID, "get file metadata", err)
				filesCompleted[fileUID.String()] = true
				continue
			}
			return handleFileError(fileUID, "get file metadata", err)
		}

		bucket := repository.BucketFromDestination(metadata.File.Destination)
		fileType := artifactpb.File_Type(artifactpb.File_Type_value[metadata.File.FileType])

		// Capture KB model family and dual-processing info from first file
		// (all files in a workflow belong to same KB, so we only need to check once)
		if kbModelFamily == "" {
			kbModelFamily = metadata.KBModelFamily
			dualProcessingInfo = metadata.DualProcessingInfo
			logger.Info("KB model family detected from first file",
				"modelFamily", kbModelFamily,
				"fileUID", fileUID.String())
			if dualProcessingInfo != nil && dualProcessingInfo.IsNeeded {
				logger.Info("Dual-processing will be coordinated after completion",
					"targetKBUID", dualProcessingInfo.TargetKB.UID.String(),
					"phase", dualProcessingInfo.Phase)
			}
		}

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

	// Determine caching strategy based on KB model family
	// Caching is only supported for Gemini model family
	useCaching := kbModelFamily == "gemini"
	logger.Info("Determined caching strategy",
		"modelFamily", kbModelFamily,
		"useCaching", useCaching)

	// Step 2: Process files that need full processing (NOTSTARTED/PROCESSING)
	filesToProcessFull := make([]fileMetadata, 0)
	for _, fm := range filesMetadata {
		logger.Info("Checking file processing requirements",
			"fileUID", fm.fileUID.String(),
			"startStatus", fm.startStatus.String(),
			"shouldProcessFull", fm.shouldProcessFull,
			"shouldProcessChunk", fm.shouldProcessChunk,
			"shouldProcessEmbed", fm.shouldProcessEmbed)
		if fm.shouldProcessFull {
			filesToProcessFull = append(filesToProcessFull, fm)
		}
	}

	logger.Info("Determined files for full processing",
		"totalFiles", len(filesMetadata),
		"filesToProcessFull", len(filesToProcessFull))

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

		// Step 2b: Clean up old converted files BEFORE standardization (for reprocessing)
		// This ensures old PDFs/content/summary are deleted before new ones are created
		logger.Info("Cleaning up old converted files before standardization", "fileCount", len(filesToProcessFull))
		for _, fm := range filesToProcessFull {
			if err := workflow.ExecuteActivity(ctx, w.DeleteOldConvertedFilesActivity, &DeleteOldConvertedFilesActivityParam{
				FileUID: fm.fileUID,
			}).Get(ctx, nil); err != nil {
				logger.Error("Failed to cleanup old converted files before standardization",
					"fileUID", fm.fileUID.String(),
					"error", err)
				// Don't fail the workflow - continue with standardization
			}
		}

		// Step 2d: File type standardization for all files (DOCX→PDF, etc.) - MUST happen after cleanup and before caching
		logger.Info("Starting file type standardization for all files", "fileCount", len(filesToProcessFull))

		type stdFileResult struct {
			fileUID              types.FileUIDType
			fileMetadata         *fileMetadata
			effectiveBucket      string
			effectiveDestination string
			effectiveFileType    artifactpb.File_Type
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
				Filename:    fm.metadata.File.Filename,
				Pipelines:   []pipeline.Release{pipeline.ConvertFileTypePipeline},
				Metadata:    fm.metadata.ExternalMetadata,
			}).Get(ctx, &result); err != nil {
				// Format conversion failure is not fatal - continue with original file
				// Note: This may cause downstream AI clients to fail if they don't support the original format
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

		// Step 2d: Create caches using the converted files (Gemini only)
		// Each file: cache → content/summary → chunking → embedding (no blocking between files)
		logger.Info("Starting cache creation and file processing", "fileCount", len(stdResults))

		type fileProcessingFuture struct {
			fileUID         types.FileUIDType
			fileCacheFuture workflow.Future
			conversionData  *stdFileResult
		}

		processingFutures := make([]fileProcessingFuture, len(stdResults))

		// Start cache creation for each file immediately (using converted files) - Gemini only
		for i, cr := range stdResults {
			cr := cr // Create a copy for closure/pointer safety

			var fileCacheFuture workflow.Future

			if useCaching {
				// Start file cache creation for this file (non-blocking) - Gemini only
				// Use the CONVERTED file for caching, not the original
				// Use 2-minute timeout for cache creation (AI API calls should complete quickly)
				cacheCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
					StartToCloseTimeout: 2 * time.Minute,
					RetryPolicy: &temporal.RetryPolicy{
						InitialInterval:    RetryInitialInterval,
						BackoffCoefficient: RetryBackoffCoefficient,
						MaximumInterval:    RetryMaximumIntervalStandard,
						MaximumAttempts:    RetryMaximumAttempts,
					},
				})
				fileCacheFuture = workflow.ExecuteActivity(cacheCtx, w.CacheFileContextActivity, &CacheFileContextActivityParam{
					FileUID:     cr.fileUID,
					KBUID:       kbUID,
					Bucket:      cr.effectiveBucket,
					Destination: cr.effectiveDestination,
					FileType:    cr.effectiveFileType,
					Filename:    cr.fileMetadata.metadata.File.Filename,
					Metadata:    cr.fileMetadata.metadata.ExternalMetadata,
				})
				logger.Info("Creating file cache (Gemini)", "fileUID", cr.fileUID.String())
			} else {
				// OpenAI or other non-Gemini: No file cache needed
				logger.Info("Skipping file cache creation (non-Gemini)", "fileUID", cr.fileUID.String())
			}

			processingFutures[i] = fileProcessingFuture{
				fileUID:         cr.fileUID,
				fileCacheFuture: fileCacheFuture,
				conversionData:  &cr,
			}

			// REFACTORED: PDF handling moved to StandardizeFileTypeActivity
			// - PDFs (converted or original) are now saved directly to converted-file folder during standardization
			// - No need for deferred SavePDFAsConvertedFileActivity calls
			// - Non-PDF temporary files (GIF→PNG, MKV→MP4) still need cleanup
			if cr.converted && cr.convertedDestination != "" && cr.effectiveFileType != artifactpb.File_TYPE_PDF {
				// For non-PDF (PNG, OGG, MP4): Delete temporary file after processing
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
			fileUID            types.FileUIDType
			contentFuture      workflow.Future  // Activity future (not child workflow)
			summaryFuture      workflow.Future  // Activity future (not child workflow) - for Gemini, or placeholder for OpenAI
			summaryFutureChan  workflow.Channel // For OpenAI: channel to receive actual summary future after content completes
			isOpenAISequential bool             // Flag to indicate OpenAI sequential processing
			conversionData     *stdFileResult
		}

		workflowFutures := make([]fileWorkflowFutures, len(processingFutures))
		fileCacheNames := make(map[string]string) // Map fileUID -> cacheName

		// Phase 1: Collect cache names for all files (Gemini only)
		for _, pf := range processingFutures {
			// Skip cache collection if caching is disabled (OpenAI or other non-Gemini)
			if !useCaching || pf.fileCacheFuture == nil {
				logger.Info("No file cache to collect (non-Gemini)",
					"fileUID", pf.fileUID.String())
				continue
			}

			// Get file cache name for this file (wait only for THIS file's cache)
			var fileCacheResult CacheFileContextActivityResult
			var fileCacheName string
			if err := pf.fileCacheFuture.Get(ctx, &fileCacheResult); err != nil {
				logger.Warn("File cache creation failed (processing without cache)",
					"fileUID", pf.fileUID.String(), "error", err)
			} else if fileCacheResult.CachedContextEnabled {
				fileCacheName = fileCacheResult.CacheName
				fileCacheNames[pf.fileUID.String()] = fileCacheName
				logger.Info("File cache created",
					"fileUID", pf.fileUID.String(),
					"cacheName", fileCacheName)

				// Schedule file cache cleanup for this file
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
			}
		}

		// Phase 3: Start content and summary activities
		// Old converted files already cleaned up in Step 2c (before standardization)
		// For Gemini: Both activities run in parallel (both process raw file independently)
		// For OpenAI: Sequential execution (summary needs markdown from content generation)
		for i, pf := range processingFutures {
			cr := pf.conversionData
			// Get cache name for this file (if it exists - Gemini only)
			fileCacheName := fileCacheNames[cr.fileUID.String()]

			// Use the CONVERTED file location (effectiveBucket/effectiveDestination/effectiveFileType)
			contentFuture := workflow.ExecuteActivity(ctx, w.ProcessContentActivity, &ProcessContentActivityParam{
				FileUID:     cr.fileUID,
				KBUID:       kbUID,
				Bucket:      cr.effectiveBucket,
				Destination: cr.effectiveDestination,
				FileType:    cr.effectiveFileType,
				Filename:    cr.fileMetadata.metadata.File.Filename,
				Metadata:    cr.fileMetadata.metadata.ExternalMetadata,
				CacheName:   fileCacheName,
			})

			var summaryFuture workflow.Future
			var summaryFutureChan workflow.Channel

			if kbModelFamily == "gemini" {
				// Gemini route: Start summary in parallel (processes raw file independently)
				logger.Info("Starting content and summary in parallel (Gemini)",
					"fileUID", cr.fileUID.String())

				summaryCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
					StartToCloseTimeout: ActivityTimeoutLong, // 5 minutes (pipeline timeout is 5 min)
					RetryPolicy: &temporal.RetryPolicy{
						InitialInterval:    RetryInitialInterval,
						BackoffCoefficient: RetryBackoffCoefficient,
						MaximumInterval:    RetryMaximumIntervalLong,
						MaximumAttempts:    RetryMaximumAttempts,
					},
				})
				summaryFuture = workflow.ExecuteActivity(summaryCtx, w.ProcessSummaryActivity, &ProcessSummaryActivityParam{
					FileUID:     cr.fileUID,
					KBUID:       kbUID,
					Bucket:      cr.effectiveBucket,
					Destination: cr.effectiveDestination,
					Filename:    cr.fileMetadata.metadata.File.Filename,
					FileType:    cr.effectiveFileType,
					Metadata:    cr.fileMetadata.metadata.ExternalMetadata,
					CacheName:   fileCacheName,
					// ContentMarkdown left empty - Gemini reads raw file
				})
				summaryFutureChan = nil // No channel for Gemini
			} else {
				// OpenAI route: Summary depends on content generation output
				// Execute content → summary sequentially using goroutine
				logger.Info("Sequential content → summary (OpenAI)",
					"fileUID", cr.fileUID.String())

				// Capture variables for closure
				fileUID := cr.fileUID
				bucket := cr.effectiveBucket
				destination := cr.effectiveDestination
				filename := cr.fileMetadata.metadata.File.Filename
				fileType := cr.effectiveFileType
				metadata := cr.fileMetadata.metadata.ExternalMetadata

				// Create a channel to communicate the summary future
				summaryFutureChan = workflow.NewChannel(ctx)

				// Start goroutine to wait for content, then execute summary
				workflow.Go(ctx, func(gCtx workflow.Context) {
					// Step 1: Wait for content generation to complete
					var contentResult ProcessContentActivityResult
					if err := contentFuture.Get(gCtx, &contentResult); err != nil {
						logger.Error("Content generation failed, cannot start summary (OpenAI)",
							"fileUID", fileUID.String(),
							"error", err.Error())
						// Send nil future to indicate failure
						summaryFutureChan.Send(gCtx, nil)
						return
					}

					logger.Info("Content generation completed, starting summary with markdown (OpenAI)",
						"fileUID", fileUID.String(),
						"contentLength", len(contentResult.Content))

					// Step 2: Execute summary with the generated markdown content
					summaryCtx := workflow.WithActivityOptions(gCtx, workflow.ActivityOptions{
						StartToCloseTimeout: ActivityTimeoutLong, // 5 minutes (pipeline timeout is 5 min)
						RetryPolicy: &temporal.RetryPolicy{
							InitialInterval:    RetryInitialInterval,
							BackoffCoefficient: RetryBackoffCoefficient,
							MaximumInterval:    RetryMaximumIntervalLong,
							MaximumAttempts:    RetryMaximumAttempts,
						},
					})

					actualSummaryFuture := workflow.ExecuteActivity(summaryCtx, w.ProcessSummaryActivity, &ProcessSummaryActivityParam{
						FileUID:         fileUID,
						KBUID:           kbUID,
						Bucket:          bucket,      // Not used when ContentMarkdown is provided
						Destination:     destination, // Not used when ContentMarkdown is provided
						Filename:        filename,
						FileType:        fileType,
						Metadata:        metadata,
						CacheName:       "",                    // No cache for OpenAI
						ContentMarkdown: contentResult.Content, // Pass markdown from content generation
					})

					// Send the actual future to the channel
					summaryFutureChan.Send(gCtx, actualSummaryFuture)
				})

				// Don't block here - we'll receive from the channel in the main waiting loop
				// Set summaryFuture to nil as placeholder
				summaryFuture = nil
			}

			workflowFutures[i] = fileWorkflowFutures{
				fileUID:            cr.fileUID,
				contentFuture:      contentFuture,
				summaryFuture:      summaryFuture,     // nil for OpenAI (will receive from channel later)
				summaryFutureChan:  summaryFutureChan, // Only set for OpenAI
				isOpenAISequential: kbModelFamily != "gemini",
				conversionData:     cr,
			}
		}

		// Wait for all activity executions to complete and collect results
		for _, wf := range workflowFutures {
			var contentResult ProcessContentActivityResult
			var summaryResult ProcessSummaryActivityResult

			contentErr := wf.contentFuture.Get(ctx, &contentResult)

			// For OpenAI sequential processing, receive the summary future from the channel
			// (which was sent after content completed in the goroutine)
			var summaryErr error
			if wf.isOpenAISequential && wf.summaryFutureChan != nil {
				// OpenAI route: Receive the actual summary future from the goroutine
				logger.Info("OpenAI sequential processing: receiving summary future from channel",
					"fileUID", wf.fileUID.String())
				var actualSummaryFuture workflow.Future
				wf.summaryFutureChan.Receive(ctx, &actualSummaryFuture)

				if actualSummaryFuture == nil {
					// Content failed, summary was not started
					logger.Warn("Summary future is nil (content failed)", "fileUID", wf.fileUID.String())
					summaryErr = contentErr // Use content error
				} else {
					// Get result from the actual summary future
					summaryErr = actualSummaryFuture.Get(ctx, &summaryResult)
				}
			} else {
				// Gemini route: get result from pre-started summary future
				logger.Info("Gemini parallel processing: getting summary result",
					"fileUID", wf.fileUID.String(),
					"isOpenAISequential", wf.isOpenAISequential,
					"summaryFutureChan", wf.summaryFutureChan != nil,
					"summaryFuture", wf.summaryFuture != nil)

				if wf.summaryFuture == nil {
					// This should never happen for Gemini, but handle gracefully
					logger.Error("Summary future is nil for Gemini processing (BUG!)",
						"fileUID", wf.fileUID.String())
					summaryErr = fmt.Errorf("summary future is nil")
				} else {
					summaryErr = wf.summaryFuture.Get(ctx, &summaryResult)
				}
			}

			// Check for errors
			if contentErr != nil {
				filesFailed[wf.fileUID.String()] = handleFileError(wf.fileUID, "process content workflow", contentErr)
				filesCompleted[wf.fileUID.String()] = true
				continue
			}
			if summaryErr != nil {
				filesFailed[wf.fileUID.String()] = handleFileError(wf.fileUID, "process summary workflow", summaryErr)
				filesCompleted[wf.fileUID.String()] = true
				continue
			}

			logger.Info("File processing workflows completed successfully",
				"fileUID", wf.fileUID.String(),
				"summaryLength", len(summaryResult.Summary))

			// Update conversion metadata for this file
			// Pipelines array contains all pipelines used: [content_pipeline, summary_pipeline]
			// Each can be a pipeline name (e.g., "instill-ai/indexing-generate-content@v1.4.0") or empty if AI client was used
			pipelines := []string{contentResult.Pipeline, summaryResult.Pipeline}
			if err := workflow.ExecuteActivity(ctx, w.UpdateConversionMetadataActivity, &UpdateConversionMetadataActivityParam{
				FileUID:   wf.fileUID,
				Length:    contentResult.Length,
				Pipelines: pipelines,
			}).Get(ctx, nil); err != nil {
				filesFailed[wf.fileUID.String()] = handleFileError(wf.fileUID, "update conversion metadata", err)
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

			// Delete old text chunks before creating new ones
			if err := workflow.ExecuteActivity(ctx, w.DeleteOldTextChunksActivity, &DeleteOldTextChunksActivityParam{
				FileUID: fileUID,
			}).Get(ctx, nil); err != nil {
				filesFailed[fileUID.String()] = handleFileError(fileUID, "delete old text chunks", err)
				filesCompleted[fileUID.String()] = true
				continue
			}

			// Chunk content (using content from ProcessContentActivity result)
			var contentChunks ChunkContentActivityResult
			if err := workflow.ExecuteActivity(ctx, w.ChunkContentActivity, &ChunkContentActivityParam{
				FileUID:      fileUID,
				KBUID:        kbUID,
				Content:      contentResult.Content, // Use content from ProcessContentActivity
				Metadata:     wf.conversionData.fileMetadata.metadata.ExternalMetadata,
				Type:         artifactpb.Chunk_TYPE_CONTENT, // Mark as content chunks
				PositionData: contentResult.PositionData,    // Pass position data from content processing
			}).Get(ctx, &contentChunks); err != nil {
				filesFailed[fileUID.String()] = handleFileError(fileUID, "chunk content", err)
				filesCompleted[fileUID.String()] = true
				continue
			}

			// Save content chunks (reference the content converted_file)
			if err := workflow.ExecuteActivity(ctx, w.SaveTextChunksActivity, &SaveTextChunksActivityParam{
				KBUID:            kbUID,
				FileUID:          fileUID,
				TextChunks:       contentChunks.TextChunks,
				ConvertedFileUID: contentResult.ConvertedFileUID, // Use UID from ProcessContentActivity
			}).Get(ctx, nil); err != nil {
				filesFailed[fileUID.String()] = handleFileError(fileUID, "save content chunks", err)
				filesCompleted[fileUID.String()] = true
				continue
			}

			// Process summary chunks if present (summary converted_file already created in ProcessSummaryActivity)
			var totalChunkCount int
			totalChunkCount = len(contentChunks.TextChunks)

			if len(summaryResult.Summary) > 0 && summaryResult.ConvertedFileUID != uuid.Nil {
				// Chunk the summary
				var summaryChunks ChunkContentActivityResult
				if err := workflow.ExecuteActivity(ctx, w.ChunkContentActivity, &ChunkContentActivityParam{
					FileUID:      fileUID,
					KBUID:        kbUID,
					Content:      summaryResult.Summary,
					Metadata:     wf.conversionData.fileMetadata.metadata.ExternalMetadata,
					Type:         artifactpb.Chunk_TYPE_SUMMARY, // Mark as summary chunks
					PositionData: summaryResult.PositionData,    // Pass position data from summary processing
				}).Get(ctx, &summaryChunks); err != nil {
					filesFailed[fileUID.String()] = handleFileError(fileUID, "chunk summary", err)
					filesCompleted[fileUID.String()] = true
					continue
				}

				// Save summary chunks (reference the summary converted_file created by ProcessSummaryActivity)
				if err := workflow.ExecuteActivity(ctx, w.SaveTextChunksActivity, &SaveTextChunksActivityParam{
					KBUID:            kbUID,
					FileUID:          fileUID,
					TextChunks:       summaryChunks.TextChunks,
					ConvertedFileUID: summaryResult.ConvertedFileUID, // Summary chunks reference summary converted_file
				}).Get(ctx, nil); err != nil {
					filesFailed[fileUID.String()] = handleFileError(fileUID, "save summary chunks", err)
					filesCompleted[fileUID.String()] = true
					continue
				}

				totalChunkCount += len(summaryChunks.TextChunks)
			}

			logger.Info("CHUNKING phase completed for file",
				"fileUID", fileUID.String(),
				"totalChunks", totalChunkCount)

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
			// Use shorter timeout (5 min) for this I/O-only activity (DB + MinIO reads)
			chunksCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: ActivityTimeoutStandard, // 5 minutes for I/O operations
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval:    RetryInitialInterval,
					BackoffCoefficient: RetryBackoffCoefficient,
					MaximumInterval:    RetryMaximumIntervalStandard,
					MaximumAttempts:    RetryMaximumAttempts,
				},
			})
			var chunksData GetTextChunksForEmbeddingActivityResult
			if err := workflow.ExecuteActivity(chunksCtx, w.GetChunksForEmbeddingActivity, &GetChunksForEmbeddingActivityParam{
				FileUID: fileUID,
			}).Get(chunksCtx, &chunksData); err != nil {
				filesFailed[fileUID.String()] = handleFileError(fileUID, "get text chunks for embedding", err)
				filesCompleted[fileUID.String()] = true
				continue
			}

			// Generate embeddings using activity
			// Pass KBUID to enable client selection based on KB's embedding config
			// Use 2-minute timeout (embedding should complete within 1 minute normally)
			embedCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 2 * time.Minute, // Embedding should be fast, timeout if taking too long
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval:    RetryInitialInterval,
					BackoffCoefficient: RetryBackoffCoefficient,
					MaximumInterval:    RetryMaximumIntervalStandard,
					MaximumAttempts:    RetryMaximumAttempts,
				},
			})
			var embeddingResult EmbedTextsActivityResult
			if err := workflow.ExecuteActivity(embedCtx, w.EmbedTextsActivity, &EmbedTextsActivityParam{
				KBUID:    &kbUID, // Pass KBUID for client selection (Gemini 3072-dim vs OpenAI 1536-dim)
				Texts:    chunksData.Texts,
				TaskType: "RETRIEVAL_DOCUMENT",                                     // For indexing document chunks
				Metadata: wf.conversionData.fileMetadata.metadata.ExternalMetadata, // Pass metadata for authentication
			}).Get(embedCtx, &embeddingResult); err != nil {
				filesFailed[fileUID.String()] = handleFileError(fileUID, "generate embeddings", err)
				filesCompleted[fileUID.String()] = true
				continue
			}

			// Build embedding records
			embeddings := make([]repository.EmbeddingModel, len(chunksData.Chunks))
			for j, chunk := range chunksData.Chunks {
				embeddings[j] = repository.EmbeddingModel{
					UID:         types.EmbeddingUIDType(uuid.Must(uuid.NewV4())),
					SourceTable: repository.TextChunkTableName,
					SourceUID:   chunk.UID,
					Vector:      embeddingResult.Vectors[j],
					KBUID:       kbUID,
					FileUID:     fileUID,
					ContentType: chunksData.ContentType,
					ChunkType:   chunk.ChunkType,
					Tags:        chunksData.Tags,
				}
			}

			// Save embeddings to Milvus + DB
			childWorkflowOptions := workflow.ChildWorkflowOptions{
				WorkflowID: fmt.Sprintf("save-embeddings-%s", fileUID.String()),
			}
			childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)

			if err := workflow.ExecuteChildWorkflow(childCtx, w.SaveEmbeddingsWorkflow, SaveEmbeddingsWorkflowParam{
				KBUID:        kbUID,
				FileUID:      fileUID,
				Filename:     chunksData.Filename,
				Embeddings:   embeddings,
				UserUID:      userUID,
				RequesterUID: requesterUID,
			}).Get(childCtx, nil); err != nil {
				filesFailed[fileUID.String()] = handleFileError(fileUID, "save embeddings to vector DB", err)
				filesCompleted[fileUID.String()] = true
				continue
			}

			// Update embedding metadata with pipeline information
			// Pipeline field contains the embedding pipeline name if used (e.g., "preset/indexing-embed@v1.0.0"),
			// or empty string if AI client was used directly
			if err := workflow.ExecuteActivity(ctx, w.UpdateEmbeddingMetadataActivity, &UpdateEmbeddingMetadataActivityParam{
				FileUID:  fileUID,
				Pipeline: embeddingResult.Pipeline,
			}).Get(ctx, nil); err != nil {
				filesFailed[fileUID.String()] = handleFileError(fileUID, "update embedding metadata", err)
				filesCompleted[fileUID.String()] = true
				continue
			}

			// Update final status to COMPLETED
			if err := workflow.ExecuteActivity(ctx, w.UpdateFileStatusActivity, &UpdateFileStatusActivityParam{
				FileUID: fileUID,
				Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED,
				Message: "File processing completed successfully",
			}).Get(ctx, nil); err != nil {
				filesFailed[fileUID.String()] = handleFileError(fileUID, "update final status", err)
				filesCompleted[fileUID.String()] = true
				continue
			}

			// Mark file as successfully completed
			filesCompleted[fileUID.String()] = true
			logger.Info("File processing completed successfully", "fileUID", fileUID.String())
		}
	}

	// Mark all files as processed
	allFilesCompleted = true

	// Check if any files failed
	if len(filesFailed) > 0 {
		// Build error message with all failures
		failedFileIDs := make([]string, 0, len(filesFailed))
		for fileUID := range filesFailed {
			failedFileIDs = append(failedFileIDs, fileUID)
		}

		logger.Error("ProcessFileWorkflow completed with failures",
			"fileCount", len(filesMetadata),
			"completedCount", len(filesCompleted),
			"failedCount", len(filesFailed),
			"failedFiles", failedFileIDs)

		// Return the first error (all are already logged and files marked as FAILED)
		var firstError error
		for _, err := range filesFailed {
			firstError = err
			break
		}
		return fmt.Errorf("%d of %d files failed processing: %w", len(filesFailed), len(filesMetadata), firstError)
	}

	logger.Info("ProcessFileWorkflow completed successfully",
		"fileCount", len(filesMetadata),
		"completedCount", len(filesCompleted))

	// SEQUENTIAL DUAL PROCESSING: After production files complete successfully,
	// trigger target (staging/rollback) file processing if needed.
	// This ensures proper synchronization - target only processes if production succeeds.
	if dualProcessingInfo != nil && dualProcessingInfo.IsNeeded {
		logger.Info("Sequential dual-processing: Production completed, triggering target file processing",
			"prodKBUID", kbUID.String(),
			"targetKBUID", dualProcessingInfo.TargetKB.UID.String(),
			"phase", dualProcessingInfo.Phase,
			"fileCount", len(param.FileUIDs))

		// Find target files by matching names
		targetFileUIDs := make([]types.FileUIDType, 0, len(filesMetadata))
		for _, prodFileMeta := range filesMetadata {
			var findResult FindTargetFileByNameActivityResult
			err := workflow.ExecuteActivity(ctx, w.FindTargetFileByNameActivity, &FindTargetFileByNameActivityParam{
				TargetKBUID:    dualProcessingInfo.TargetKB.UID,
				TargetOwnerUID: dualProcessingInfo.TargetKB.Owner,
				Filename:       prodFileMeta.metadata.File.Filename,
			}).Get(ctx, &findResult)
			if err != nil {
				// CRITICAL: Activity execution failed (e.g., not registered, DB error)
				// This is a system error, not a "file not found" scenario
				logger.Error("FindTargetFileByNameActivity failed - dual-processing cannot proceed",
					"filename", prodFileMeta.metadata.File.Filename,
					"targetKBUID", dualProcessingInfo.TargetKB.UID.String(),
					"error", err)
				return err
			}
			if !findResult.Found {
				// WARNING: Target file not found - expected during dual-processing
				// This could happen if target file was deleted or cloning failed
				logger.Warn("Target file not found for dual processing",
					"filename", prodFileMeta.metadata.File.Filename,
					"targetKBUID", dualProcessingInfo.TargetKB.UID.String())
				// Continue - we'll process what we can find
				continue
			}
			targetFileUIDs = append(targetFileUIDs, findResult.FileUID)
		}

		// Trigger target file processing workflows
		if len(targetFileUIDs) > 0 {
			// Use ExecuteChildWorkflow to trigger target processing asynchronously
			// (fire-and-forget pattern - we don't wait for completion)
			// IMPORTANT: Use nanosecond precision to avoid workflow ID collisions when multiple files complete in the same second
			// CRITICAL: Set ParentClosePolicy to ABANDON so child workflows continue running after parent completes
			childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
				WorkflowID:        fmt.Sprintf("process-file-target-%s-%d", kbUID.String(), workflow.Now(ctx).UnixNano()),
				ParentClosePolicy: enums.PARENT_CLOSE_POLICY_ABANDON,
			})

			childFuture := workflow.ExecuteChildWorkflow(childCtx, w.ProcessFileWorkflow, ProcessFileWorkflowParam{
				FileUIDs:     targetFileUIDs,
				KBUID:        dualProcessingInfo.TargetKB.UID,
				UserUID:      param.UserUID,
				RequesterUID: param.RequesterUID,
			})

			// Wait just for the workflow to start (not complete)
			var childWE workflow.Execution
			if err := childFuture.GetChildWorkflowExecution().Get(ctx, &childWE); err != nil {
				// CRITICAL: Failed to start target workflow during dual-processing
				// This means target files won't be processed, which will cause synchronization to fail
				// (SynchronizeKBActivity checks for NOTSTARTED files in target KB)
				logger.Error("Failed to start target file processing workflow - dual-processing incomplete",
					"targetFileCount", len(targetFileUIDs),
					"targetKBUID", dualProcessingInfo.TargetKB.UID.String(),
					"error", err)
				return err
			}

			logger.Info("Sequential dual-processing: Target file processing started",
				"targetFileCount", len(targetFileUIDs),
				"targetKBUID", dualProcessingInfo.TargetKB.UID.String(),
				"targetWorkflowID", childWE.ID)
		} else {
			logger.Warn("No target files found for dual processing",
				"prodFileCount", len(filesMetadata),
				"targetKBUID", dualProcessingInfo.TargetKB.UID.String())
		}
	}

	return nil
}
