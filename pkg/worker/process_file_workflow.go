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
	"go.uber.org/zap"

	"github.com/gofrs/uuid"
	"github.com/instill-ai/artifact-backend/pkg/pipeline"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/repository/object"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
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

	// Terminate any existing workflow with the same ID before starting a new one
	// This handles "reprocess" scenarios where a file might be stuck in processing
	// We don't check for errors here because the workflow might not exist (which is fine)
	_ = w.terminateExistingWorkflow(ctx, workflowID)

	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	_, err := w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, w.worker.ProcessFileWorkflow, param)
	return err
}

// terminateExistingWorkflow attempts to terminate an existing workflow if it's running.
// Returns nil if no workflow exists or if termination succeeds.
func (w *processFileWorkflow) terminateExistingWorkflow(ctx context.Context, workflowID string) error {
	// First, check if the workflow exists and is running by describing it
	desc, err := w.temporalClient.DescribeWorkflowExecution(ctx, workflowID, "")
	if err != nil {
		// Workflow doesn't exist or other error - that's fine, nothing to terminate
		return nil
	}

	// Check if the workflow is in a running state
	status := desc.WorkflowExecutionInfo.Status
	if status == enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
		// Terminate the running workflow
		w.worker.log.Info("Terminating existing workflow for reprocessing",
			zap.String("workflowID", workflowID),
			zap.String("status", status.String()))

		err = w.temporalClient.TerminateWorkflow(ctx, workflowID, "", "Terminated for file reprocessing")
		if err != nil {
			w.worker.log.Warn("Failed to terminate existing workflow",
				zap.String("workflowID", workflowID),
				zap.Error(err))
			// Don't return error - we'll try to start the new workflow anyway
			// If the old one is truly stuck, ALLOW_DUPLICATE policy will handle it
		}

		// Give Temporal a moment to process the termination
		// This helps ensure the workflow ID is available for reuse
		time.Sleep(100 * time.Millisecond)
	}

	return nil
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
			// GetFileMetadataActivity failed - fail the workflow immediately
			// This includes "file not found" errors where file record doesn't exist in DB
			// (which is different from missing MinIO objects, caught later in processing)
			return handleFileError(fileUID, "get file metadata", err)
		}

		bucket := object.BucketFromDestination(metadata.File.StoragePath)
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
				FileUID:         fm.fileUID,
				KBUID:           kbUID,
				Bucket:          fm.bucket,
				Destination:     fm.metadata.File.StoragePath,
				FileType:        fm.fileType,
				FileDisplayName: fm.metadata.File.DisplayName,
				Pipelines:       []pipeline.Release{pipeline.ConvertFileTypePipeline},
				Metadata:        fm.metadata.ExternalMetadata,
				RequesterUID:    param.RequesterUID, // Pass requester UID for permission checks
			}).Get(ctx, &result); err != nil {
				// File type standardization failure is fatal - fail the workflow immediately
				// This prevents downstream AI processing failures when the format is not supported
				return handleFileError(fm.fileUID, "file type standardization", err)
			}

			// Determine effective file location for caching and processing
			effectiveBucket := fm.bucket
			effectiveDestination := fm.metadata.File.StoragePath
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

			// Invoke post-standardization callback if set
			// This fires as soon as the file is standardized (PDF/PNG/etc.), before content conversion.
			if w.postStandardization != nil {
				if callbackErr := w.postStandardization(
					ctx,
					uuid.UUID(fm.fileUID),
					uuid.UUID(fm.metadata.File.NamespaceUID),
					uuid.UUID(kbUID),
					effectiveBucket,
					effectiveDestination,
					effectiveFileType,
					fm.metadata.File.DisplayName,
				); callbackErr != nil {
					logger.Warn("Post-standardization callback failed (non-fatal)",
						"fileUID", fm.fileUID.String(),
						"error", callbackErr.Error())
				}
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
				// Use ActivityTimeoutLong for AI API operations that may take time for large files
				cacheCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
					StartToCloseTimeout: ActivityTimeoutLong, // 5 min for heavy AI operations
					RetryPolicy: &temporal.RetryPolicy{
						InitialInterval:    RetryInitialInterval,
						BackoffCoefficient: RetryBackoffCoefficient,
						MaximumInterval:    RetryMaximumIntervalStandard,
						MaximumAttempts:    RetryMaximumAttempts,
					},
				})
				fileCacheFuture = workflow.ExecuteActivity(cacheCtx, w.CacheFileContextActivity, &CacheFileContextActivityParam{
					FileUID:         cr.fileUID,
					KBUID:           kbUID,
					Bucket:          cr.effectiveBucket,
					Destination:     cr.effectiveDestination,
					FileType:        cr.effectiveFileType,
					FileDisplayName: cr.fileMetadata.metadata.File.DisplayName,
					Metadata:        cr.fileMetadata.metadata.ExternalMetadata,
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

			// Calculate dynamic timeout based on file size
			// Larger files need more time for AI processing
			fileSizeBytes := int(cr.fileMetadata.metadata.File.Size)
			aiTimeout := CalculateAIProcessingTimeout(fileSizeBytes)

			logger.Info("Using dynamic AI processing timeout",
				"fileUID", cr.fileUID.String(),
				"fileSizeBytes", fileSizeBytes,
				"timeout", aiTimeout.String())

			// Create activity context with dynamic timeout for AI processing
			aiActivityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: aiTimeout,
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval:    RetryInitialInterval,
					BackoffCoefficient: RetryBackoffCoefficient,
					MaximumInterval:    RetryMaximumIntervalLong,
					MaximumAttempts:    RetryMaximumAttempts,
				},
			})

			// Use the CONVERTED file location (effectiveBucket/effectiveDestination/effectiveFileType)
			contentFuture := workflow.ExecuteActivity(aiActivityCtx, w.ProcessContentActivity, &ProcessContentActivityParam{
				FileUID:         cr.fileUID,
				KBUID:           kbUID,
				Bucket:          cr.effectiveBucket,
				Destination:     cr.effectiveDestination,
				FileType:        cr.effectiveFileType,
				FileDisplayName: cr.fileMetadata.metadata.File.DisplayName,
				Metadata:        cr.fileMetadata.metadata.ExternalMetadata,
				CacheName:       fileCacheName,
			})

			var summaryFuture workflow.Future
			var summaryFutureChan workflow.Channel

			if kbModelFamily == "gemini" {
				// Gemini route: Start summary in parallel (processes raw file independently)
				logger.Info("Starting content and summary in parallel (Gemini)",
					"fileUID", cr.fileUID.String())

				// Use the same dynamic timeout for summary activity
				summaryFuture = workflow.ExecuteActivity(aiActivityCtx, w.ProcessSummaryActivity, &ProcessSummaryActivityParam{
					FileUID:         cr.fileUID,
					KBUID:           kbUID,
					Bucket:          cr.effectiveBucket,
					Destination:     cr.effectiveDestination,
					FileDisplayName: cr.fileMetadata.metadata.File.DisplayName,
					FileType:        cr.effectiveFileType,
					Metadata:        cr.fileMetadata.metadata.ExternalMetadata,
					CacheName:       fileCacheName,
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
				filename := cr.fileMetadata.metadata.File.DisplayName
				fileType := cr.effectiveFileType
				metadata := cr.fileMetadata.metadata.ExternalMetadata
				capturedAITimeout := aiTimeout // Capture the dynamic timeout for the goroutine

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
					// Use dynamic timeout based on file size (captured from outer scope)
					summaryCtx := workflow.WithActivityOptions(gCtx, workflow.ActivityOptions{
						StartToCloseTimeout: capturedAITimeout,
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
						FileDisplayName: filename,
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

			// Invoke post-content-conversion callback if set and content succeeded
			// This fires as soon as markdown content is ready, before chunking/embedding
			if contentErr == nil && w.postContentConversion != nil {
				// Get filename from file metadata
				filename := ""
				if wf.conversionData != nil && wf.conversionData.fileMetadata != nil &&
					wf.conversionData.fileMetadata.metadata.File != nil {
					filename = wf.conversionData.fileMetadata.metadata.File.DisplayName
				}
				if callbackErr := w.postContentConversion(
					ctx,
					uuid.UUID(wf.fileUID),
					uuid.UUID(param.UserUID),
					uuid.UUID(kbUID),
					contentResult.ContentBucket,
					contentResult.ContentPath,
					filename,
				); callbackErr != nil {
					logger.Warn("Post-content-conversion callback failed (non-fatal)",
						"fileUID", wf.fileUID.String(),
						"error", callbackErr.Error())
				}
			}

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

			// Invoke post-summary-conversion callback if set and summary succeeded
			// This fires as soon as summary is ready, before chunking/embedding
			if summaryErr == nil && w.postSummaryConversion != nil && summaryResult.SummaryBucket != "" {
				// Get filename from file metadata
				filename := ""
				if wf.conversionData != nil && wf.conversionData.fileMetadata != nil &&
					wf.conversionData.fileMetadata.metadata.File != nil {
					filename = wf.conversionData.fileMetadata.metadata.File.DisplayName
				}
				if callbackErr := w.postSummaryConversion(
					ctx,
					uuid.UUID(wf.fileUID),
					uuid.UUID(param.UserUID),
					uuid.UUID(kbUID),
					summaryResult.SummaryBucket,
					summaryResult.SummaryPath,
					filename,
				); callbackErr != nil {
					logger.Warn("Post-summary-conversion callback failed (non-fatal)",
						"fileUID", wf.fileUID.String(),
						"error", callbackErr.Error())
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

			// Update usage metadata (token counts from AI processing)
			// Store the usage metadata from both content and summary for later retrieval
			// According to Gemini API docs: https://ai.google.dev/gemini-api/docs/tokens?lang=python
			if err := workflow.ExecuteActivity(ctx, w.UpdateUsageMetadataActivity, &UpdateUsageMetadataActivityParam{
				FileUID:         wf.fileUID,
				ContentMetadata: contentResult.UsageMetadata,
				SummaryMetadata: summaryResult.UsageMetadata,
			}).Get(ctx, nil); err != nil {
				// Non-fatal error - log warning and continue
				// Usage metadata is for billing/monitoring, not critical for file processing
				logger.Warn("Failed to update usage metadata (continuing anyway)",
					"fileUID", wf.fileUID.String(),
					"error", err.Error())
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
			if err := workflow.ExecuteActivity(ctx, w.SaveChunksActivity, &SaveChunksActivityParam{
				KBUID:            kbUID,
				FileUID:          fileUID,
				Chunks:           contentChunks.Chunks,
				ConvertedFileUID: contentResult.ConvertedFileUID, // Use UID from ProcessContentActivity
			}).Get(ctx, nil); err != nil {
				filesFailed[fileUID.String()] = handleFileError(fileUID, "save content chunks", err)
				filesCompleted[fileUID.String()] = true
				continue
			}

			// Process summary chunks if present (summary converted_file already created in ProcessSummaryActivity)
			var totalChunkCount int
			totalChunkCount = len(contentChunks.Chunks)

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
				if err := workflow.ExecuteActivity(ctx, w.SaveChunksActivity, &SaveChunksActivityParam{
					KBUID:            kbUID,
					FileUID:          fileUID,
					Chunks:           summaryChunks.Chunks,
					ConvertedFileUID: summaryResult.ConvertedFileUID, // Summary chunks reference summary converted_file
				}).Get(ctx, nil); err != nil {
					filesFailed[fileUID.String()] = handleFileError(fileUID, "save summary chunks", err)
					filesCompleted[fileUID.String()] = true
					continue
				}

				totalChunkCount += len(summaryChunks.Chunks)
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

			// Combined activity: query chunks, generate embeddings, save to DB/Milvus
			// This eliminates large data transfer between workflow and activities
			embedCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: ActivityTimeoutEmbedding, // 10 min for embedding large files
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval:    RetryInitialInterval,
					BackoffCoefficient: RetryBackoffCoefficient,
					MaximumInterval:    RetryMaximumIntervalStandard,
					MaximumAttempts:    RetryMaximumAttempts,
				},
			})
			var embedResult EmbedAndSaveChunksActivityResult
			if err := workflow.ExecuteActivity(embedCtx, w.EmbedAndSaveChunksActivity, &EmbedAndSaveChunksActivityParam{
				KBUID:    kbUID,
				FileUID:  fileUID,
				Metadata: wf.conversionData.fileMetadata.metadata.ExternalMetadata,
			}).Get(embedCtx, &embedResult); err != nil {
				filesFailed[fileUID.String()] = handleFileError(fileUID, "embed and save chunks", err)
				filesCompleted[fileUID.String()] = true
				continue
			}

			logger.Info("Embeddings saved successfully",
				"fileUID", fileUID.String(),
				"chunkCount", embedResult.ChunkCount,
				"embeddingCount", embedResult.EmbeddingCount)

			// Update embedding metadata with pipeline information
			// Pipeline field contains the embedding pipeline name if used (e.g., "preset/indexing-embed@v1.0.0"),
			// or empty string if AI client was used directly
			if err := workflow.ExecuteActivity(ctx, w.UpdateEmbeddingMetadataActivity, &UpdateEmbeddingMetadataActivityParam{
				FileUID:  fileUID,
				Pipeline: embedResult.Pipeline,
			}).Get(ctx, nil); err != nil {
				filesFailed[fileUID.String()] = handleFileError(fileUID, "update embedding metadata", err)
				filesCompleted[fileUID.String()] = true
				continue
			}

			// IMPORTANT: Execute post-completion logic BEFORE marking as COMPLETED
			// This ensures that if the post completion logic fails, the file remains in PROCESSING state
			// and can be properly marked as FAILED
			if w.postFileCompletion != nil {
				effectiveFileType := wf.conversionData.effectiveFileType
				file := wf.conversionData.fileMetadata.metadata.File
				if err := w.postFileCompletion(ctx, file, effectiveFileType); err != nil {
					filesFailed[fileUID.String()] = handleFileError(fileUID, "credit subtraction", err)
					filesCompleted[fileUID.String()] = true
					continue
				}
			}

			// Update final status to COMPLETED
			// This is done AFTER post file completion to ensure proper failure handling
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

		// Create context with explicit activity options for FindTargetFileByNameActivity
		findActivityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: ActivityTimeoutStandard, // 1 minute for DB query
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    RetryInitialInterval,
				BackoffCoefficient: RetryBackoffCoefficient,
				MaximumInterval:    RetryMaximumIntervalStandard,
				MaximumAttempts:    RetryMaximumAttempts,
			},
		})

		for _, prodFileMeta := range filesMetadata {
			var findResult FindTargetFileByNameActivityResult
			err := workflow.ExecuteActivity(findActivityCtx, w.FindTargetFileByNameActivity, &FindTargetFileByNameActivityParam{
				TargetKBUID:     dualProcessingInfo.TargetKB.UID,
				TargetOwnerUID:  dualProcessingInfo.TargetKB.NamespaceUID,
				FileDisplayName: prodFileMeta.metadata.File.DisplayName,
			}).Get(findActivityCtx, &findResult)
			if err != nil {
				// CRITICAL: Activity execution failed (e.g., not registered, DB error)
				// This is a system error, not a "file not found" scenario
				logger.Error("FindTargetFileByNameActivity failed - dual-processing cannot proceed",
					"filename", prodFileMeta.metadata.File.DisplayName,
					"targetKBUID", dualProcessingInfo.TargetKB.UID.String(),
					"error", err)
				return err
			}
			if !findResult.Found {
				// WARNING: Target file not found - expected during dual-processing
				// This could happen if target file was deleted or cloning failed
				logger.Warn("Target file not found for dual processing",
					"filename", prodFileMeta.metadata.File.DisplayName,
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
				"targetWorkflowID", childWE.ID,
				"targetFileUIDs", func() []string {
					uids := make([]string, len(targetFileUIDs))
					for i, uid := range targetFileUIDs {
						uids[i] = uid.String()
					}
					return uids
				}())
		} else {
			logger.Warn("No target files found for dual processing - rollback KB files may not have been cloned",
				"prodFileCount", len(filesMetadata),
				"targetKBUID", dualProcessingInfo.TargetKB.UID.String(),
				"prodFileNames", func() []string {
					names := make([]string, len(filesMetadata))
					for i, meta := range filesMetadata {
						names[i] = meta.metadata.File.DisplayName
					}
					return names
				}())
		}
	}

	return nil
}
