package worker

import (
	"context"
	"errors"
	"fmt"
	"math"
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

// fileMetadata holds per-file metadata fetched at the start of the workflow.
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

// stdFileResult holds per-file results from standardization used throughout the workflow.
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

// fileProcessingFuture tracks cache creation and standardization data for a single file.
type fileProcessingFuture struct {
	fileUID         types.FileUIDType
	fileCacheFuture workflow.Future
	conversionData  *stdFileResult
}

// fileWorkflowFutures aggregates all futures for a single file's content and summary processing.
type fileWorkflowFutures struct {
	fileUID            types.FileUIDType
	contentFuture      workflow.Future
	summaryFuture      workflow.Future
	summaryFutureChan  workflow.Channel
	isOpenAISequential bool
	conversionData     *stdFileResult
	isLongMedia        bool
}

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
	_ = w.terminateExistingWorkflow(ctx, workflowID, param.FileUIDs)

	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	_, err := w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, w.worker.ProcessFileWorkflow, param)
	return err
}

// terminateExistingWorkflow attempts to terminate an existing workflow if it's running.
// After successful termination, it marks all associated files as FAILED in the DB
// because TerminateWorkflow kills the goroutine immediately — defer blocks don't run.
// Returns nil if no workflow exists or if termination succeeds.
func (w *processFileWorkflow) terminateExistingWorkflow(ctx context.Context, workflowID string, fileUIDs []types.FileUIDType) error {
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
		} else {
			// Workflow terminated — mark all files as FAILED directly in the DB.
			// The terminated workflow's defer block won't run, so we must reconcile here.
			w.markFilesAsFailed(ctx, fileUIDs, "File processing was interrupted for reprocessing")
		}

		// Give Temporal a moment to process the termination
		// This helps ensure the workflow ID is available for reuse
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

// markFilesAsFailed updates file statuses to FAILED directly in the database.
// Used when a workflow is terminated and its defer cleanup cannot run.
func (w *processFileWorkflow) markFilesAsFailed(ctx context.Context, fileUIDs []types.FileUIDType, message string) {
	repo := w.worker.GetRepository()
	for _, fileUID := range fileUIDs {
		updateMap := map[string]any{
			repository.FileColumn.ProcessStatus: artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_FAILED.String(),
		}
		if _, err := repo.UpdateFile(ctx, fileUID.String(), updateMap); err != nil {
			w.worker.log.Warn("Failed to mark file as FAILED after workflow termination",
				zap.String("fileUID", fileUID.String()),
				zap.Error(err))
			continue
		}
		if err := repo.UpdateFileMetadata(ctx, fileUID, repository.ExtraMetaData{StatusMessage: message}); err != nil {
			w.worker.log.Warn("Failed to update file metadata after workflow termination",
				zap.String("fileUID", fileUID.String()),
				zap.Error(err))
		}
	}
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

		// Step 2b: Clean up old converted files (for reprocessing)
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

		stdResults := make([]stdFileResult, len(filesToProcessFull))

		// Convert all files in parallel
		for i, fm := range filesToProcessFull {
			fm := fm // Create a copy for closure/pointer safety

			stdTimeout := CalculateStandardizationTimeout(int(fm.metadata.File.Size))
			logger.Info("Using dynamic standardization timeout",
				"fileUID", fm.fileUID.String(),
				"fileSizeBytes", fm.metadata.File.Size,
				"timeout", stdTimeout.String())

			stdCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: stdTimeout,
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval:    RetryInitialInterval,
					BackoffCoefficient: RetryBackoffCoefficient,
					MaximumInterval:    RetryMaximumIntervalLong,
					MaximumAttempts:    RetryMaximumAttempts,
				},
			})

			var result StandardizeFileTypeActivityResult
			if err := workflow.ExecuteActivity(stdCtx, w.StandardizeFileTypeActivity, &StandardizeFileTypeActivityParam{
				FileUID:         fm.fileUID,
				KBUID:           kbUID,
				Bucket:          fm.bucket,
				Destination:     fm.metadata.File.StoragePath,
				FileType:        fm.fileType,
				FileDisplayName: fm.metadata.File.DisplayName,
				Pipelines:       []pipeline.Release{pipeline.ConvertFileTypePipeline},
				Metadata:        fm.metadata.ExternalMetadata,
				RequesterUID:    param.RequesterUID, // Pass requester UID for permission checks
			}).Get(stdCtx, &result); err != nil {
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

		// Step 2d-pre: Probe media duration with ffprobe (before cache creation).
		// For media files, duration must be known locally because long videos
		// (> MaxVideoChunkDuration) cannot be cached by Gemini at all.
		fileDurationSec := make(map[string]int32)  // Map fileUID -> media duration in seconds
		longMediaFiles := make(map[string]bool)     // Files that need physical chunking

		for _, cr := range stdResults {
			if !isMediaFileType(cr.effectiveFileType) {
				continue
			}

			durationCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 2 * time.Minute,
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval:    RetryInitialInterval,
					BackoffCoefficient: RetryBackoffCoefficient,
					MaximumInterval:    RetryMaximumIntervalStandard,
					MaximumAttempts:    3,
				},
			})

			var durResult GetMediaDurationActivityResult
			if err := workflow.ExecuteActivity(durationCtx, w.GetMediaDurationActivity, &GetMediaDurationActivityParam{
				Bucket:      cr.effectiveBucket,
				Destination: cr.effectiveDestination,
				Metadata:    cr.fileMetadata.metadata.ExternalMetadata,
			}).Get(ctx, &durResult); err != nil {
				logger.Warn("Media duration probe failed, will rely on cache metadata",
					"fileUID", cr.fileUID.String(), "error", err)
				continue
			}

			fileDurationSec[cr.fileUID.String()] = int32(durResult.DurationSeconds)
			logger.Info("Media duration probed via ffprobe",
				"fileUID", cr.fileUID.String(),
				"durationSeconds", durResult.DurationSeconds)

			maxChunkDur := MaxVideoChunkDuration
			if isAudioFileType(cr.effectiveFileType) {
				maxChunkDur = MaxAudioChunkDuration
			}
			mediaDuration := time.Duration(durResult.DurationSeconds * float64(time.Second))
			if mediaDuration > maxChunkDur {
				longMediaFiles[cr.fileUID.String()] = true
				logger.Info("Long media file detected, will use physical chunking path",
					"fileUID", cr.fileUID.String(),
					"duration", mediaDuration.String(),
					"threshold", maxChunkDur.String())
			}
		}

		// Step 2d: Create caches using the converted files (Gemini only)
		// Long media files skip cache creation here — they'll be cached per-chunk later.
		logger.Info("Starting cache creation and file processing", "fileCount", len(stdResults))

		processingFutures := make([]fileProcessingFuture, len(stdResults))

		for i, cr := range stdResults {
			cr := cr

			var fileCacheFuture workflow.Future

			if longMediaFiles[cr.fileUID.String()] {
				logger.Info("Skipping full-file cache for long media (will cache per-chunk)", "fileUID", cr.fileUID.String())
			} else if useCaching {
				cacheCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
					StartToCloseTimeout: ActivityTimeoutLong,
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
				logger.Info("Skipping file cache creation (non-Gemini)", "fileUID", cr.fileUID.String())
			}

			processingFutures[i] = fileProcessingFuture{
				fileUID:         cr.fileUID,
				fileCacheFuture: fileCacheFuture,
				conversionData:  &cr,
			}
		}

		workflowFutures := make([]fileWorkflowFutures, len(processingFutures))
		fileCacheNames := make(map[string]string)

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

				// Only populate duration from cache metadata if not already set by ffprobe
				if _, haveDuration := fileDurationSec[pf.fileUID.String()]; !haveDuration {
					if fileCacheResult.AudioDurationSeconds > 0 {
						fileDurationSec[pf.fileUID.String()] = fileCacheResult.AudioDurationSeconds
					} else if fileCacheResult.VideoDurationSeconds > 0 {
						fileDurationSec[pf.fileUID.String()] = fileCacheResult.VideoDurationSeconds
					}
				}

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

		// Pre-query page counts for cached files to decide conversion route.
		// Media files with known duration use time-range segmentation and skip
		// this query entirely. Documents with more pages than the profile's
		// DirectMaxPages skip single-shot and go directly to concurrent batch dispatch.
		filePageCounts := make(map[string]int) // fileUID -> pageCount (0 = unknown)
		for _, pf := range processingFutures {
			cacheName := fileCacheNames[pf.fileUID.String()]
			if cacheName == "" {
				continue
			}

			profile := batchProfile(pf.conversionData.effectiveFileType)
			duration := time.Duration(fileDurationSec[pf.fileUID.String()]) * time.Second
			if profile.SegmentDuration > 0 && duration > 0 {
				logger.Info("Using time-range segmentation, skipping page count query",
					"fileUID", pf.fileUID.String(),
					"duration", duration.String(),
					"segmentDuration", profile.SegmentDuration.String())
				continue
			}

			pageCountCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 5 * time.Minute,
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval:    RetryInitialInterval,
					BackoffCoefficient: RetryBackoffCoefficient,
					MaximumInterval:    RetryMaximumIntervalStandard,
					MaximumAttempts:    3,
				},
			})
			var pgResult GetPageCountActivityResult
			if pgErr := workflow.ExecuteActivity(pageCountCtx, w.GetPageCountActivity, &GetPageCountActivityParam{
				CacheName: cacheName,
				FileType:  pf.conversionData.effectiveFileType,
			}).Get(ctx, &pgResult); pgErr != nil {
				logger.Warn("Page count query failed, will use single-shot attempt",
					"fileUID", pf.fileUID.String(), "error", pgErr)
			} else {
				filePageCounts[pf.fileUID.String()] = pgResult.PageCount
				logger.Info("Page count determined",
					"fileUID", pf.fileUID.String(),
					"pageCount", pgResult.PageCount)
			}
		}

		// Phase 3: Start content and summary activities
		// Long media files skip this phase entirely — they're handled in the wait loop below.
		for i, pf := range processingFutures {
			cr := pf.conversionData
			isLong := longMediaFiles[cr.fileUID.String()]

			if isLong {
				logger.Info("Long media file: deferring to physical chunk processing",
					"fileUID", cr.fileUID.String())
				workflowFutures[i] = fileWorkflowFutures{
					fileUID:        cr.fileUID,
					conversionData: cr,
					isLongMedia:    true,
				}
				continue
			}

			fileCacheName := fileCacheNames[cr.fileUID.String()]

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

			// Decide whether to attempt single-shot conversion or go directly to
			// concurrent batch dispatch, avoiding a doomed single-shot attempt.
			//
			// Media files with known duration use time-range based routing:
			//   duration <= DirectMaxDuration → single-shot
			//   duration > DirectMaxDuration  → skip to batch (time-range segments)
			//
			// Documents use page-count based routing (existing logic).
			pageCount := filePageCounts[cr.fileUID.String()]
			profile := batchProfile(cr.effectiveFileType)
			mediaDuration := time.Duration(fileDurationSec[cr.fileUID.String()]) * time.Second
			var contentFuture workflow.Future
			skipSingleShot := false
			if profile.SegmentDuration > 0 && mediaDuration > 0 && fileCacheName != "" {
				if mediaDuration > profile.DirectMaxDuration {
					logger.Info("Large media file detected, skipping single-shot and going directly to time-range batch conversion",
						"fileUID", cr.fileUID.String(),
						"duration", mediaDuration.String(),
						"directMaxDuration", profile.DirectMaxDuration.String())
					skipSingleShot = true
				}
			} else if pageCount > profile.DirectMaxPages && fileCacheName != "" {
				logger.Info("Large document detected, skipping single-shot attempt and going directly to batch conversion",
					"fileUID", cr.fileUID.String(),
					"pageCount", pageCount)
				skipSingleShot = true
			}
			if skipSingleShot {
				contentFuture = nil
			} else {
				// Use the CONVERTED file location (effectiveBucket/effectiveDestination/effectiveFileType)
				contentFuture = workflow.ExecuteActivity(aiActivityCtx, w.ProcessContentActivity, &ProcessContentActivityParam{
					FileUID:         cr.fileUID,
					KBUID:           kbUID,
					Bucket:          cr.effectiveBucket,
					Destination:     cr.effectiveDestination,
					FileType:        cr.effectiveFileType,
					FileDisplayName: cr.fileMetadata.metadata.File.DisplayName,
					Metadata:        cr.fileMetadata.metadata.ExternalMetadata,
					CacheName:       fileCacheName,
				})
			}

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
			var contentErr error

			// ───── Long media path: split → per-chunk cache+batch → offset → assemble ─────
			if wf.isLongMedia {
				contentErr = w.processLongMedia(ctx, logger, &wf, kbUID, &contentResult, &summaryResult, fileDurationSec)
				if contentErr != nil {
					logger.Error("Long media processing failed",
						"fileUID", wf.fileUID.String(), "error", contentErr)
				}
				goto postContent
			}

			// ───── Normal path: single-shot or batch conversion ─────
			{
			needsBatchConversion := false
			preQueriedPageCount := filePageCounts[wf.fileUID.String()]

			if wf.contentFuture == nil {
				needsBatchConversion = true
			} else {
				contentErr = wf.contentFuture.Get(ctx, &contentResult)

				// Check for activity-level post-LLM error: the LLM call succeeded
				// but post-processing (DB write, MinIO upload) failed. The result
				// (including UsageMetadata) is still valid for usage tracking.
				if contentErr == nil && contentResult.Error != "" {
					contentErr = fmt.Errorf("%s", contentResult.Error)
				}
				if contentErr == nil && contentResult.NeedsChunkedConversion {
					needsBatchConversion = true
				}
			}

			if needsBatchConversion {
				cacheName := fileCacheNames[wf.fileUID.String()]
				if cacheName == "" {
					contentErr = fmt.Errorf("batch conversion requested but no cache available for file %s", wf.fileUID.String())
				} else {
					// Only sleep for quota recovery if single-shot was actually attempted
					if wf.contentFuture != nil {
						logger.Info("Sleeping before batch conversion to let API quota recover from single-shot attempts",
							"fileUID", wf.fileUID.String())
						_ = workflow.Sleep(ctx, RateLimitCooldown)
					}

					logger.Info("Starting per-batch chunked conversion",
						"fileUID", wf.fileUID.String(),
						"cacheName", cacheName)

					batchProfile := batchProfile(wf.conversionData.effectiveFileType)
					fileDuration := time.Duration(fileDurationSec[wf.fileUID.String()]) * time.Second
					useTimeRange := batchProfile.SegmentDuration > 0 && fileDuration > 0

					// For page-based mode, determine total pages.
					totalPages := 0
					if !useTimeRange {
						totalPages = preQueriedPageCount
						if totalPages == 0 {
							pageCountCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
								StartToCloseTimeout: 5 * time.Minute,
								RetryPolicy: &temporal.RetryPolicy{
									InitialInterval:    RetryInitialInterval,
									BackoffCoefficient: RetryBackoffCoefficient,
									MaximumInterval:    RetryMaximumIntervalStandard,
									MaximumAttempts:    3,
								},
							})
							var pgResult GetPageCountActivityResult
							if pgErr := workflow.ExecuteActivity(pageCountCtx, w.GetPageCountActivity, &GetPageCountActivityParam{
								CacheName: cacheName,
								FileType:  wf.conversionData.effectiveFileType,
							}).Get(ctx, &pgResult); pgErr != nil {
								contentErr = fmt.Errorf("failed to get page count: %w", pgErr)
							} else {
								totalPages = pgResult.PageCount
							}
						}
					}

				if contentErr == nil && (useTimeRange || totalPages > 0) {
					batchCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
						StartToCloseTimeout: batchProfile.ActivityTimeout,
						RetryPolicy: &temporal.RetryPolicy{
							InitialInterval:    5 * time.Second,
							BackoffCoefficient: 2.0,
							MaximumInterval:    120 * time.Second,
							MaximumAttempts:    5,
						},
					})

						workflowRunID := workflow.GetInfo(ctx).WorkflowExecution.RunID

						type batchSlot struct {
							Index          int
							StartPage      int
							EndPage        int
							StartTimestamp time.Duration
							EndTimestamp   time.Duration
							SegmentIndex   int
						}

					var allSlots []batchSlot
					if useTimeRange {
						segCount := int(math.Ceil(fileDuration.Seconds() / batchProfile.SegmentDuration.Seconds()))
						for i := 0; i < segCount; i++ {
							start := time.Duration(i) * batchProfile.SegmentDuration
							end := start + batchProfile.SegmentDuration
							if end > fileDuration {
								end = fileDuration
							}
							allSlots = append(allSlots, batchSlot{
								Index:          i,
								SegmentIndex:   i + 1,
								StartTimestamp: start,
								EndTimestamp:   end,
							})
						}
					} else {
						idx := 0
						for start := 1; start <= totalPages; start += batchProfile.PagesPerBatch {
							end := start + batchProfile.PagesPerBatch - 1
							if end > totalPages {
								end = totalPages
							}
							allSlots = append(allSlots, batchSlot{Index: idx, StartPage: start, EndPage: end})
							idx++
						}
					}

						completedPaths := make(map[int]string, len(allSlots))
						completedUsage := make(map[int]map[string]interface{}, len(allSlots))
						var batchModel string

						isTransientBatchErr := func(err error) (isCacheExp bool, isTransient bool) {
							var appErr *temporal.ApplicationError
							if errors.As(err, &appErr) && appErr.Type() == "CacheExpired" {
								return true, true
							}
							msg := err.Error()
							if strings.Contains(msg, "RESOURCE_EXHAUSTED") || strings.Contains(msg, "429") ||
								strings.Contains(msg, "transient error retries exhausted") ||
								strings.Contains(msg, "DEADLINE_EXCEEDED") || strings.Contains(msg, "504") ||
								strings.Contains(msg, "UNAVAILABLE") || strings.Contains(msg, "503") ||
								strings.Contains(msg, "document has no pages") {
								return false, true
							}
							return false, false
						}

						runBatchRound := func(slots []batchSlot, cn string) (failed []batchSlot, hasCacheExp bool, permErr error) {
							selector := workflow.NewSelector(ctx)
							pending := 0

							for _, slot := range slots {
								s := slot
					activityParam := &ConvertBatchActivityParam{
						CacheName:      cn,
						StartPage:      s.StartPage,
						EndPage:        s.EndPage,
						TotalPages:     totalPages,
						KBUID:          kbUID,
						FileUID:        wf.fileUID,
						WorkflowRunID:  workflowRunID,
						BatchIndex:     s.Index,
						ChunkTimeout:   batchProfile.ChunkTimeout,
						PagesPerChunk:  batchProfile.PagesPerChunk,
						UseTimeRange:   useTimeRange,
						StartTimestamp: s.StartTimestamp,
						EndTimestamp:   s.EndTimestamp,
						SegmentIndex:   s.SegmentIndex,
						FileType:       wf.conversionData.effectiveFileType,
					}
							future := workflow.ExecuteActivity(batchCtx, w.ConvertBatchActivity, activityParam)
								selector.AddFuture(future, func(f workflow.Future) {
									var result ConvertBatchActivityResult
									if err := f.Get(ctx, &result); err != nil {
										cacheExp, transient := isTransientBatchErr(err)
										if transient {
											failed = append(failed, s)
											if cacheExp {
												hasCacheExp = true
											}
											logger.Warn("Batch failed with transient error",
												"fileUID", wf.fileUID.String(),
												"batchIndex", s.Index,
												"cacheExpired", cacheExp,
												"error", err.Error())
										} else {
											permErr = fmt.Errorf("batch %d failed: %w", s.Index, err)
										}
								} else {
									completedPaths[s.Index] = result.TempMinIOPath
									if result.UsageMetadata != nil {
										completedUsage[s.Index] = result.UsageMetadata
									}
									if batchModel == "" && result.Model != "" {
										batchModel = result.Model
									}
								}
									pending--
								})
								pending++

								if pending >= batchProfile.MaxConcurrentBatches {
									selector.Select(ctx)
								}
							}
							for pending > 0 {
								selector.Select(ctx)
							}
							return
						}

						logger.Info("Phase 1: concurrent batch dispatch",
							"fileUID", wf.fileUID.String(),
							"totalBatches", len(allSlots),
							"maxConcurrent", batchProfile.MaxConcurrentBatches,
							"useTimeRange", useTimeRange)

						failedSlots, hasCacheExpired, permanentErr := runBatchRound(allSlots, cacheName)

						// Phase 2: retry transient failures (rate-limit and/or cache-expired)
						if permanentErr == nil && len(failedSlots) > 0 {
							logger.Info("Phase 2: retrying transient failures",
								"fileUID", wf.fileUID.String(),
								"failedBatches", len(failedSlots),
								"hasCacheExpired", hasCacheExpired)

							if hasCacheExpired {
								cacheCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
									StartToCloseTimeout: ActivityTimeoutLong,
									RetryPolicy: &temporal.RetryPolicy{
										InitialInterval:    RetryInitialInterval,
										BackoffCoefficient: RetryBackoffCoefficient,
										MaximumInterval:    RetryMaximumIntervalStandard,
										MaximumAttempts:    RetryMaximumAttempts,
									},
								})

								var freshCacheResult CacheFileContextActivityResult
								cacheErr := workflow.ExecuteActivity(cacheCtx, w.CacheFileContextActivity, &CacheFileContextActivityParam{
									FileUID:         wf.fileUID,
									KBUID:           kbUID,
									Bucket:          wf.conversionData.effectiveBucket,
									Destination:     wf.conversionData.effectiveDestination,
									FileType:        wf.conversionData.effectiveFileType,
									FileDisplayName: wf.conversionData.fileMetadata.metadata.File.DisplayName,
									Metadata:        wf.conversionData.fileMetadata.metadata.ExternalMetadata,
								}).Get(ctx, &freshCacheResult)

								if cacheErr != nil || !freshCacheResult.CachedContextEnabled {
									permanentErr = fmt.Errorf("failed to refresh cache for retry round: %v", cacheErr)
								} else {
									cacheName = freshCacheResult.CacheName
									fileCacheNames[wf.fileUID.String()] = cacheName

									defer func(cn string) {
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
										_ = workflow.ExecuteActivity(cleanupCtx, w.DeleteCacheActivity, &DeleteCacheActivityParam{
											CacheName: cn,
										}).Get(cleanupCtx, nil)
									}(cacheName)
								}
							}

							if permanentErr == nil {
								var retryFailed []batchSlot
								retryFailed, _, permanentErr = runBatchRound(failedSlots, cacheName)
								if permanentErr == nil && len(retryFailed) > 0 {
									permanentErr = fmt.Errorf("%d batches still failing after retry round", len(retryFailed))
								}
							}
						}

						if permanentErr != nil {
							contentErr = permanentErr
							// Clean up temp files from completed batches
							if len(completedPaths) > 0 {
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
								paths := make([]string, 0, len(completedPaths))
								for _, p := range completedPaths {
									paths = append(paths, p)
								}
								_ = workflow.ExecuteActivity(cleanupCtx, w.DeleteFilesBatchActivity, &DeleteFilesBatchActivityParam{
									Bucket:    wf.conversionData.effectiveBucket,
									FilePaths: paths,
								}).Get(cleanupCtx, nil)
							}
						}

						if contentErr == nil {
							// Assemble paths in batch index order
							tempPaths := make([]string, len(allSlots))
							for idx, path := range completedPaths {
								tempPaths[idx] = path
							}

							saveCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
								StartToCloseTimeout: ActivityTimeoutLong,
								RetryPolicy: &temporal.RetryPolicy{
									InitialInterval:    RetryInitialInterval,
									BackoffCoefficient: RetryBackoffCoefficient,
									MaximumInterval:    RetryMaximumIntervalStandard,
									MaximumAttempts:    RetryMaximumAttempts,
								},
							})

						var saveResult SaveAssembledContentActivityResult
						if saveErr := workflow.ExecuteActivity(saveCtx, w.SaveAssembledContentActivity, &SaveAssembledContentActivityParam{
							FileUID:        wf.fileUID,
							KBUID:          kbUID,
							FileType:       wf.conversionData.effectiveFileType,
							TempMinIOPaths: tempPaths,
							IsMedia:        useTimeRange,
						}).Get(ctx, &saveResult); saveErr != nil {
								contentErr = fmt.Errorf("failed to assemble batched content: %w", saveErr)
							} else {
								contentResult.Content = saveResult.Content
								contentResult.ConvertedFileUID = saveResult.ConvertedFileUID
								contentResult.ContentBucket = saveResult.ContentBucket
								contentResult.ContentPath = saveResult.ContentPath
								contentResult.Length = saveResult.Length
								contentResult.PageCount = saveResult.PageCount
								contentResult.PositionData = saveResult.PositionData
								contentResult.ConvertedType = wf.conversionData.effectiveFileType
								contentResult.NeedsChunkedConversion = false
								contentResult.Model = batchModel

								aggregated := aggregateBatchUsage(completedUsage)
								contentResult.UsageMetadata = aggregated

								logger.Info("Per-batch chunked conversion completed successfully",
									"fileUID", wf.fileUID.String(),
									"pageCount", saveResult.PageCount,
									"contentLen", len(saveResult.Content),
									"model", batchModel,
									"aggregatedUsage", aggregated)
							}
						}
					}
				}
			}
			} // end normal path block

		postContent:
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

		// For long media files, processLongMedia already populated summaryResult
		// directly — skip the future-based summary wait entirely.
		var summaryErr error
		if wf.isLongMedia {
			// summaryResult was populated inside processLongMedia; nothing to wait for.
			// If processLongMedia failed, contentErr is already set and will be handled below.
		} else if wf.isOpenAISequential && wf.summaryFutureChan != nil {
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

			// Check for activity-level post-LLM error on summary (same as content above).
			if summaryErr == nil && summaryResult.Error != "" {
				summaryErr = fmt.Errorf("%s", summaryResult.Error)
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

			// Helper to call the post-file-failure hook with collected usage data.
			// contentResult and summaryResult are captured by reference — they
			// contain whatever was populated before the failure occurred.
			// embedResult is passed explicitly since it may not exist yet.
			callPostFileFailure := func(stage string, failErr error, embedResult *EmbedAndSaveChunksActivityResult) {
				if w.postFileFailure == nil || wf.conversionData == nil {
					return
				}
				usageData := &FileProcessingUsageData{
					ContentUsageMetadata: contentResult.UsageMetadata,
					ContentModel:         contentResult.Model,
					SummaryUsageMetadata: summaryResult.UsageMetadata,
					SummaryModel:         summaryResult.Model,
				}
				if embedResult != nil {
					usageData.EmbeddingUsageMetadata = embedResult.UsageMetadata
					usageData.EmbeddingModel = embedResult.Model
				}
				file := wf.conversionData.fileMetadata.metadata.File
				effectiveFileType := wf.conversionData.effectiveFileType
				if hookErr := w.postFileFailure(ctx, file, effectiveFileType, stage, failErr, usageData); hookErr != nil {
					logger.Warn("Post file failure hook failed (non-fatal)",
						"fileUID", wf.fileUID.String(),
						"stage", stage,
						"error", hookErr.Error())
				}
			}

			// Check for errors
			if contentErr != nil {
				filesFailed[wf.fileUID.String()] = handleFileError(wf.fileUID, "process content workflow", contentErr)
				callPostFileFailure("process content", contentErr, nil)
				filesCompleted[wf.fileUID.String()] = true
				continue
			}
			if summaryErr != nil {
				filesFailed[wf.fileUID.String()] = handleFileError(wf.fileUID, "process summary workflow", summaryErr)
				callPostFileFailure("process summary", summaryErr, nil)
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
				FileUID:         wf.fileUID,
				Length:          contentResult.Length,
				PageCount:       contentResult.PageCount,
				DurationSeconds: fileDurationSec[wf.fileUID.String()],
				Pipelines:       pipelines,
			}).Get(ctx, nil); err != nil {
				filesFailed[wf.fileUID.String()] = handleFileError(wf.fileUID, "update conversion metadata", err)
				callPostFileFailure("update conversion metadata", err, nil)
				filesCompleted[wf.fileUID.String()] = true
				continue
			}

			// Note: Usage metadata (content + summary + embedding) is updated after embedding phase completes.
			// This allows us to collect all three sources of usage metadata in a single write.

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
				callPostFileFailure("delete old text chunks", err, nil)
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
				callPostFileFailure("chunk content", err, nil)
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
				callPostFileFailure("save content chunks", err, nil)
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
					callPostFileFailure("chunk summary", err, nil)
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
					callPostFileFailure("save summary chunks", err, nil)
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
				callPostFileFailure("embed and save chunks", err, nil)
				filesCompleted[fileUID.String()] = true
				continue
			}
			// Check for activity-level post-LLM error (embedding succeeded but save failed).
			if embedResult.Error != "" {
				embedErr := fmt.Errorf("%s", embedResult.Error)
				filesFailed[fileUID.String()] = handleFileError(fileUID, "embed and save chunks", embedErr)
				callPostFileFailure("embed and save chunks", embedErr, &embedResult)
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
				callPostFileFailure("update embedding metadata", err, &embedResult)
				filesCompleted[fileUID.String()] = true
				continue
			}

			// Update usage metadata (token counts from all AI processing phases)
			// Store the usage metadata from content, summary, and embedding for monitoring
			// According to Gemini API docs: https://ai.google.dev/gemini-api/docs/tokens?lang=python
			if err := workflow.ExecuteActivity(ctx, w.UpdateUsageMetadataActivity, &UpdateUsageMetadataActivityParam{
				FileUID:           fileUID,
				ContentMetadata:   contentResult.UsageMetadata,
				SummaryMetadata:   summaryResult.UsageMetadata,
				EmbeddingMetadata: embedResult.UsageMetadata,
			}).Get(ctx, nil); err != nil {
				// Non-fatal error - log warning and continue
				// Usage metadata is for monitoring, not critical for file processing
				logger.Warn("Failed to update usage metadata (continuing anyway)",
					"fileUID", fileUID.String(),
					"error", err.Error())
			}

			// IMPORTANT: Execute post-completion logic BEFORE marking as COMPLETED
			// This ensures that if the post completion logic fails, the file remains in PROCESSING state
			// and can be properly marked as FAILED
			if w.postFileCompletion != nil {
				effectiveFileType := wf.conversionData.effectiveFileType
				file := wf.conversionData.fileMetadata.metadata.File

				durationSec := fileDurationSec[fileUID.String()]
				if len(contentResult.Length) > 0 || durationSec > 0 || contentResult.PageCount > 0 {
					if file.ExtraMetaDataUnmarshal == nil {
						file.ExtraMetaDataUnmarshal = &repository.ExtraMetaData{}
					}
					if len(contentResult.Length) > 0 {
						file.ExtraMetaDataUnmarshal.Length = contentResult.Length
					}
					if contentResult.PageCount > 0 {
						file.ExtraMetaDataUnmarshal.PageCount = contentResult.PageCount
					}
					if durationSec > 0 {
						file.ExtraMetaDataUnmarshal.DurationSeconds = durationSec
					}
				}
				completionUsageData := &FileProcessingUsageData{
					ContentUsageMetadata:   contentResult.UsageMetadata,
					ContentModel:           contentResult.Model,
					SummaryUsageMetadata:   summaryResult.UsageMetadata,
					SummaryModel:           summaryResult.Model,
					EmbeddingUsageMetadata: embedResult.UsageMetadata,
					EmbeddingModel:         embedResult.Model,
				}
				if err := w.postFileCompletion(ctx, file, effectiveFileType, completionUsageData); err != nil {
					filesFailed[fileUID.String()] = handleFileError(fileUID, "post-completion hook", err)
					callPostFileFailure("post-completion hook", err, &embedResult)
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
				callPostFileFailure("update final status", err, &embedResult)
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

// aggregateBatchUsage sums per-batch usage maps (from ConvertBatchActivityResult)
// into a single map suitable for UpdateUsageMetadataActivity.
func aggregateBatchUsage(batches map[int]map[string]interface{}) map[string]interface{} {
	if len(batches) == 0 {
		return nil
	}
	var (
		promptTokens        int64
		candidatesTokens    int64
		totalTokens         int64
		cachedContentTokens int64
		callCount           int64
	)
	for _, m := range batches {
		promptTokens += toInt64(m["promptTokenCount"])
		candidatesTokens += toInt64(m["candidatesTokenCount"])
		totalTokens += toInt64(m["totalTokenCount"])
		cachedContentTokens += toInt64(m["cachedContentTokenCount"])
		callCount += toInt64(m["callCount"])
	}
	return map[string]interface{}{
		"promptTokenCount":        promptTokens,
		"candidatesTokenCount":    candidatesTokens,
		"totalTokenCount":         totalTokens,
		"cachedContentTokenCount": cachedContentTokens,
		"callCount":               callCount,
	}
}

func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	case int32:
		return int64(n)
	default:
		return 0
	}
}

// processLongMedia handles the physical chunk processing path for media files
// that exceed Gemini's duration limit. It splits the file into chunks, processes
// each chunk through the standard cache+batch pipeline, offsets timestamps to
// absolute values, assembles the result, and generates a summary from the
// assembled transcript text.
func (w *Worker) processLongMedia(
	ctx workflow.Context,
	logger interface{ Info(string, ...interface{}); Warn(string, ...interface{}); Error(string, ...interface{}) },
	wf *fileWorkflowFutures,
	kbUID types.KBUIDType,
	contentResult *ProcessContentActivityResult,
	summaryResult *ProcessSummaryActivityResult,
	fileDurationSec map[string]int32,
) error {
	cr := wf.conversionData
	fileUID := cr.fileUID
	workflowRunID := workflow.GetInfo(ctx).WorkflowExecution.RunID

	durationSec := float64(fileDurationSec[fileUID.String()])
	chunkDur := MaxVideoChunkDuration
	if isAudioFileType(cr.effectiveFileType) {
		chunkDur = MaxAudioChunkDuration
	}

	logger.Info("processLongMedia: Starting physical chunk processing",
		"fileUID", fileUID.String(),
		"durationSec", durationSec,
		"chunkDuration", chunkDur.String())

	// Step 1: Split into physical chunks
	splitCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: StdMaxTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    RetryInitialInterval,
			BackoffCoefficient: RetryBackoffCoefficient,
			MaximumInterval:    RetryMaximumIntervalStandard,
			MaximumAttempts:    3,
		},
	})

	var splitResult SplitMediaChunksActivityResult
	if err := workflow.ExecuteActivity(splitCtx, w.SplitMediaChunksActivity, &SplitMediaChunksActivityParam{
		Bucket:          cr.effectiveBucket,
		Destination:     cr.effectiveDestination,
		FileUID:         fileUID.String(),
		WorkflowRunID:   workflowRunID,
		DurationSeconds: durationSec,
		ChunkDuration:   chunkDur,
		FileType:        cr.effectiveFileType,
		Metadata:        cr.fileMetadata.metadata.ExternalMetadata,
	}).Get(ctx, &splitResult); err != nil {
		return fmt.Errorf("SplitMediaChunksActivity failed: %w", err)
	}

	logger.Info("processLongMedia: Split complete",
		"fileUID", fileUID.String(),
		"chunks", len(splitResult.Chunks))

	// Step 2: Create ALL chunk caches concurrently, then dispatch ALL batches concurrently
	profile := batchProfile(cr.effectiveFileType)
	var batchModel string

	// Phase 2a: Create caches for all chunks concurrently
	cacheCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: ActivityTimeoutLong,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    RetryInitialInterval,
			BackoffCoefficient: RetryBackoffCoefficient,
			MaximumInterval:    RetryMaximumIntervalStandard,
			MaximumAttempts:    RetryMaximumAttempts,
		},
	})

	chunkCacheNames := make([]string, len(splitResult.Chunks))
	cacheErrs := make([]error, len(splitResult.Chunks))
	cacheSel := workflow.NewSelector(ctx)
	cachePending := 0

	for ci, chunk := range splitResult.Chunks {
		i := ci
		ch := chunk
		future := workflow.ExecuteActivity(cacheCtx, w.CacheFileContextActivity, &CacheFileContextActivityParam{
			FileUID:         fileUID,
			KBUID:           kbUID,
			Bucket:          cr.effectiveBucket,
			Destination:     ch.MinIOPath,
			FileType:        cr.effectiveFileType,
			FileDisplayName: fmt.Sprintf("%s (chunk %d)", cr.fileMetadata.metadata.File.DisplayName, ch.Index),
			Metadata:        cr.fileMetadata.metadata.ExternalMetadata,
		})
		cacheSel.AddFuture(future, func(f workflow.Future) {
			var result CacheFileContextActivityResult
			if err := f.Get(ctx, &result); err != nil {
				cacheErrs[i] = err
			} else if !result.CachedContextEnabled {
				cacheErrs[i] = fmt.Errorf("caching not enabled for chunk %d", i)
			} else {
				chunkCacheNames[i] = result.CacheName
			}
			cachePending--
		})
		cachePending++
	}
	for cachePending > 0 {
		cacheSel.Select(ctx)
	}

	// Schedule cleanup for all caches (runs on function return)
	for _, cn := range chunkCacheNames {
		if cn == "" {
			continue
		}
		cacheName := cn
		defer func() {
			cleanupCtx, _ := workflow.NewDisconnectedContext(ctx)
			cleanupCtx = workflow.WithActivityOptions(cleanupCtx, workflow.ActivityOptions{
				StartToCloseTimeout: time.Minute,
				RetryPolicy:         &temporal.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2.0, MaximumInterval: 30 * time.Second, MaximumAttempts: 3},
			})
			_ = workflow.ExecuteActivity(cleanupCtx, w.DeleteCacheActivity, &DeleteCacheActivityParam{CacheName: cacheName}).Get(cleanupCtx, nil)
		}()
	}

	for i, err := range cacheErrs {
		if err != nil {
			return fmt.Errorf("cache creation failed for chunk %d: %w", i, err)
		}
	}

	logger.Info("processLongMedia: All chunk caches created concurrently",
		"fileUID", fileUID.String(),
		"cacheCount", len(chunkCacheNames))

	// Phase 2b: Identify speakers from chunk 0 for cross-chunk consistency.
	var speakerContext string
	{
		speakerCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
			RetryPolicy:         &temporal.RetryPolicy{InitialInterval: 2 * time.Second, BackoffCoefficient: 2.0, MaximumInterval: 30 * time.Second, MaximumAttempts: 3},
		})
		var speakerResult IdentifySpeakersActivityResult
		if err := workflow.ExecuteActivity(speakerCtx, w.IdentifySpeakersActivity, &IdentifySpeakersActivityParam{
			CacheName: chunkCacheNames[0],
			FileType:  cr.effectiveFileType,
		}).Get(ctx, &speakerResult); err != nil {
			logger.Warn("processLongMedia: Speaker identification failed, proceeding without",
				"fileUID", fileUID.String(), "error", err)
		} else {
			speakerContext = speakerResult.SpeakerContext
		}
		if speakerContext != "" {
			logger.Info("processLongMedia: Speakers identified",
				"fileUID", fileUID.String(),
				"speakerContext", speakerContext)
		}
	}

	// Phase 2c: Build batch slots for ALL chunks
	type chunkBatchSlot struct {
		ChunkIndex     int
		SlotIndex      int
		StartTimestamp time.Duration
		EndTimestamp   time.Duration
		SegmentIndex   int
	}

	chunkSegCounts := make([]int, len(splitResult.Chunks))
	var allSlots []chunkBatchSlot
	for ci, chunk := range splitResult.Chunks {
		chunkDuration := chunk.EndOffset - chunk.StartOffset
		segCount := int(math.Ceil(chunkDuration.Seconds() / profile.SegmentDuration.Seconds()))
		chunkSegCounts[ci] = segCount
		for si := 0; si < segCount; si++ {
			start := time.Duration(si) * profile.SegmentDuration
			end := start + profile.SegmentDuration
			if end > chunkDuration {
				end = chunkDuration
			}
			allSlots = append(allSlots, chunkBatchSlot{
				ChunkIndex:     ci,
				SlotIndex:      si,
				StartTimestamp: start,
				EndTimestamp:   end,
				SegmentIndex:   si + 1,
			})
		}
	}

	logger.Info("processLongMedia: Dispatching all batches concurrently across all chunks",
		"fileUID", fileUID.String(),
		"totalChunks", len(splitResult.Chunks),
		"totalBatches", len(allSlots),
		"maxConcurrent", profile.MaxConcurrentBatches)

	// Phase 2d: Dispatch ALL batches from ALL chunks concurrently
	batchCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: profile.ActivityTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    120 * time.Second,
			MaximumAttempts:    5,
		},
	})

	completedPaths := make([]map[int]string, len(splitResult.Chunks))
	completedUsage := make([]map[int]map[string]interface{}, len(splitResult.Chunks))
	for i := range splitResult.Chunks {
		completedPaths[i] = make(map[int]string)
		completedUsage[i] = make(map[int]map[string]interface{})
	}

	batchSel := workflow.NewSelector(ctx)
	pending := 0

	for _, slot := range allSlots {
		s := slot
		future := workflow.ExecuteActivity(batchCtx, w.ConvertBatchActivity, &ConvertBatchActivityParam{
			CacheName:      chunkCacheNames[s.ChunkIndex],
			KBUID:          kbUID,
			FileUID:        fileUID,
			WorkflowRunID:  workflowRunID,
			BatchIndex:     s.ChunkIndex*1000 + s.SlotIndex,
			ChunkTimeout:   profile.ChunkTimeout,
			UseTimeRange:   true,
			StartTimestamp: s.StartTimestamp,
			EndTimestamp:   s.EndTimestamp,
			SegmentIndex:   s.SegmentIndex,
			FileType:       cr.effectiveFileType,
			SpeakerContext: speakerContext,
		})
		batchSel.AddFuture(future, func(f workflow.Future) {
			var result ConvertBatchActivityResult
			if err := f.Get(ctx, &result); err != nil {
				logger.Warn("Chunk batch failed",
					"fileUID", fileUID.String(),
					"chunkIndex", s.ChunkIndex,
					"batchIndex", s.SlotIndex,
					"error", err.Error())
			} else {
				completedPaths[s.ChunkIndex][s.SlotIndex] = result.TempMinIOPath
				if result.UsageMetadata != nil {
					completedUsage[s.ChunkIndex][s.SlotIndex] = result.UsageMetadata
				}
				if batchModel == "" && result.Model != "" {
					batchModel = result.Model
				}
			}
			pending--
		})
		pending++

		if pending >= profile.MaxConcurrentBatches {
			batchSel.Select(ctx)
		}
	}
	for pending > 0 {
		batchSel.Select(ctx)
	}

	// Verify completeness and assemble into ordered slices per chunk
	allChunkTempPaths := make([][]string, len(splitResult.Chunks))
	for ci := range splitResult.Chunks {
		if len(completedPaths[ci]) != chunkSegCounts[ci] {
			return fmt.Errorf("chunk %d: only %d/%d batches completed", ci, len(completedPaths[ci]), chunkSegCounts[ci])
		}
		paths := make([]string, chunkSegCounts[ci])
		for idx, path := range completedPaths[ci] {
			paths[idx] = path
		}
		allChunkTempPaths[ci] = paths
	}

	// Step 3: Flatten paths and build chunk offset metadata for assembly.
	// Timestamp offsetting is performed inside SaveAssembledContentActivity,
	// which already reads every batch file — this avoids redundant I/O.
	var allTempPaths []string
	chunkOffsets := make([]ChunkOffsetInfo, len(splitResult.Chunks))
	for ci, chunk := range splitResult.Chunks {
		paths := allChunkTempPaths[ci]
		allTempPaths = append(allTempPaths, paths...)
		var overlap time.Duration
		if ci > 0 {
			overlap = ChunkOverlap
		}
		chunkOffsets[ci] = ChunkOffsetInfo{
			PathCount:       len(paths),
			Offset:          chunk.StartOffset,
			OverlapDuration: overlap,
		}
	}

	// Step 4: Assemble all chunk results
	saveCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: ActivityTimeoutLong,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    RetryInitialInterval,
			BackoffCoefficient: RetryBackoffCoefficient,
			MaximumInterval:    RetryMaximumIntervalStandard,
			MaximumAttempts:    RetryMaximumAttempts,
		},
	})

	var saveResult SaveAssembledContentActivityResult
	if err := workflow.ExecuteActivity(saveCtx, w.SaveAssembledContentActivity, &SaveAssembledContentActivityParam{
		FileUID:        fileUID,
		KBUID:          kbUID,
		FileType:       cr.effectiveFileType,
		TempMinIOPaths: allTempPaths,
		IsMedia:        true,
		ChunkOffsets:   chunkOffsets,
	}).Get(ctx, &saveResult); err != nil {
		return fmt.Errorf("SaveAssembledContentActivity failed: %w", err)
	}

	// Aggregate content usage metadata across all chunks.
	allUsage := make(map[int]map[string]interface{})
	for ci := range splitResult.Chunks {
		for si, usage := range completedUsage[ci] {
			allUsage[ci*1000+si] = usage
		}
	}

	contentResult.Content = saveResult.Content
	contentResult.ConvertedFileUID = saveResult.ConvertedFileUID
	contentResult.ContentBucket = saveResult.ContentBucket
	contentResult.ContentPath = saveResult.ContentPath
	contentResult.Length = saveResult.Length
	contentResult.PageCount = saveResult.PageCount
	contentResult.PositionData = saveResult.PositionData
	contentResult.ConvertedType = cr.effectiveFileType
	contentResult.Model = batchModel
	contentResult.UsageMetadata = aggregateBatchUsage(allUsage)

	logger.Info("processLongMedia: Content assembly complete",
		"fileUID", fileUID.String(),
		"contentLen", len(saveResult.Content))

	// Step 5: Generate summary from assembled transcript text (not from cache/raw file)
	summaryCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: CalculateAIProcessingTimeout(len(saveResult.Content)),
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    RetryInitialInterval,
			BackoffCoefficient: RetryBackoffCoefficient,
			MaximumInterval:    RetryMaximumIntervalLong,
			MaximumAttempts:    RetryMaximumAttempts,
		},
	})

	if err := workflow.ExecuteActivity(summaryCtx, w.ProcessSummaryActivity, &ProcessSummaryActivityParam{
		FileUID:         fileUID,
		KBUID:           kbUID,
		Bucket:          cr.effectiveBucket,
		Destination:     cr.effectiveDestination,
		FileDisplayName: cr.fileMetadata.metadata.File.DisplayName,
		FileType:        cr.effectiveFileType,
		Metadata:        cr.fileMetadata.metadata.ExternalMetadata,
		ContentMarkdown: saveResult.Content,
	}).Get(ctx, summaryResult); err != nil {
		logger.Warn("processLongMedia: Summary generation failed (non-fatal for content)",
			"fileUID", fileUID.String(), "error", err)
	}

	// Step 6: Clean up temp chunk files from MinIO
	cleanupCtx, _ := workflow.NewDisconnectedContext(ctx)
	cleanupCtx = workflow.WithActivityOptions(cleanupCtx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2.0, MaximumInterval: 30 * time.Second, MaximumAttempts: 3},
	})
	var chunkPaths []string
	for _, chunk := range splitResult.Chunks {
		chunkPaths = append(chunkPaths, chunk.MinIOPath)
	}
	_ = workflow.ExecuteActivity(cleanupCtx, w.DeleteFilesBatchActivity, &DeleteFilesBatchActivityParam{
		Bucket:    cr.effectiveBucket,
		FilePaths: chunkPaths,
	}).Get(cleanupCtx, nil)

	return nil
}
