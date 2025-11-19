package worker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"
	"gorm.io/gorm"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/ai"
	"github.com/instill-ai/artifact-backend/pkg/ai/gemini"
	"github.com/instill-ai/artifact-backend/pkg/pipeline"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
	filetype "github.com/instill-ai/x/file"
)

// This file contains file processing activities used by ProcessFileWorkflow:
//
// Workflow Execution Flow:
// 1. File Preparation Phase:
//    - GetFileMetadataActivity - Retrieves file information and pipeline configuration
//    - GetFileContentActivity - Retrieves file content from MinIO
//    - StandardizeFileTypeActivity - Standardizes file formats (DOCX→PDF, GIF→PNG, MKV→MP4, etc.)
//    - FindTargetFileByNameActivity - Finds target file by name for dual processing
//
// 2. Caching Phase (uses standardized files):
//    - CacheFileContextActivity - Creates individual AI cache per file for efficient processing
//
// 3. Content & Summary Processing (run in parallel, both use the same cache):
//    - ProcessContentActivity - Converts files to Markdown, creates content converted_file
//      Returns: Content, ConvertedFileUID, Length, PositionData, Types, UsageMetadata
//    - ProcessSummaryActivity - Generates file summary, creates summary converted_file
//      Returns: Summary, ConvertedFileUID, Length, PositionData, Types, UsageMetadata
//      Note: Both activities return symmetric data structures for consistent processing
//
// 4. Chunking & Embedding Phase:
//    - ChunkContentActivity - Splits content/summary into manageable text chunks
//    - DeleteOldTextChunksActivity - Removes outdated text chunks before saving new ones
//    - SaveTextChunksActivity - Persists text chunks to database and MinIO storage
//      Content chunks reference content converted_file UID
//      Summary chunks reference summary converted_file UID (separate source_uid)
//    - (GetChunksForEmbeddingActivity - in embed_activity.go)
//    - (EmbedTextsActivity - in embed_activity.go)
//    - (SaveEmbeddingsWorkflow - in embed_workflow.go)
//
// 5. Metadata & Cleanup:
//    - UpdateConversionMetadataActivity - Updates file conversion metadata
//    - (UpdateEmbeddingMetadataActivity - in embed_activity.go)
//    - (UpdateFileStatusActivity - in status_activity.go)
//    - DeleteCacheActivity - Cleans up AI caches
//    - DeleteTemporaryConvertedFileActivity - Cleans up temporary converted files from MinIO
//
// Sub-activities called by ProcessContentActivity:
// - (Inline AI conversion to Markdown - AI is required)
// - DeleteOldConvertedFilesActivity - Removes outdated converted files before creating new ones
// - CreateConvertedFileRecordActivity - Creates DB record for content converted_file
// - UploadConvertedFileToMinIOActivity - Uploads converted content to MinIO
// - UpdateConvertedFileDestinationActivity - Updates DB with actual MinIO destination
// - DeleteConvertedFileRecordActivity - Cleanup activity for DB record (on error)
// - DeleteConvertedFileFromMinIOActivity - Cleanup activity for MinIO file (on error)
//
// Sub-activities called by ProcessSummaryActivity:
// - (Inline AI summary generation - AI is required)
// - DeleteOldConvertedFilesActivity - Removes outdated converted files before creating new ones
// - CreateConvertedFileRecordActivity - Creates DB record for summary converted_file
// - UploadConvertedFileToMinIOActivity - Uploads summary to MinIO
// - UpdateConvertedFileDestinationActivity - Updates DB with actual MinIO destination
// - DeleteConvertedFileRecordActivity - Cleanup activity for DB record (on error)
// - DeleteConvertedFileFromMinIOActivity - Cleanup activity for MinIO file (on error)

// ===== ERROR CONSTANTS =====

const (
	getFileMetadataActivityError                = "GetFileMetadataActivity"
	getFileContentActivityError                 = "GetFileContentActivity"
	deleteOldConvertedFilesActivityError        = "DeleteOldConvertedFilesActivity"
	createConvertedFileRecordActivityError      = "CreateConvertedFileRecordActivity"
	uploadConvertedFileToMinIOActivityError     = "UploadConvertedFileToMinIOActivity"
	deleteConvertedFileRecordActivityError      = "DeleteConvertedFileRecordActivity"
	updateConvertedFileDestinationActivityError = "UpdateConvertedFileDestinationActivity"
	deleteConvertedFileFromMinIOActivityError   = "DeleteConvertedFileFromMinIOActivity"
	updateConversionMetadataActivityError       = "UpdateConversionMetadataActivity"
	updateUsageMetadataActivityError            = "UpdateUsageMetadataActivity"
	deleteOldTextChunksActivityError            = "DeleteOldTextChunksActivity"
	saveTextChunksActivityError                 = "SaveTextChunksActivity"
	standardizeFileTypeActivityError            = "StandardizeFileTypeActivity"
	processContentActivityError                 = "ProcessContentActivity"
	processSummaryActivityError                 = "ProcessSummaryActivity"
)

// ===== METADATA & CONTENT ACTIVITIES =====

// GetFileMetadataActivityParam for retrieving file and KB metadata
type GetFileMetadataActivityParam struct {
	FileUID types.FileUIDType // File unique identifier
	KBUID   types.KBUIDType   // Knowledge base unique identifier
}

// GetFileMetadataActivityResult contains file and KB configuration
type GetFileMetadataActivityResult struct {
	File               *repository.KnowledgeBaseFileModel // File metadata from database
	ExternalMetadata   *structpb.Struct                   // External metadata from request
	KBModelFamily      string                             // KB's model family (e.g., "openai", "gemini") - used for caching decisions
	DualProcessingInfo *repository.DualProcessingTarget   // Dual-processing target info (if needed) - used for sequential coordination after completion
}

// GetFileContentActivityParam for retrieving file content from MinIO
type GetFileContentActivityParam struct {
	Bucket      string           // MinIO bucket containing the file
	Destination string           // MinIO path to the file
	Metadata    *structpb.Struct // Request metadata for authentication context
}

// GetFileMetadataActivity retrieves file and knowledge base metadata from DB
// This is a single DB read operation - idempotent
func (w *Worker) GetFileMetadataActivity(ctx context.Context, param *GetFileMetadataActivityParam) (*GetFileMetadataActivityResult, error) {
	w.log.Info("GetFileMetadataActivity: Fetching file and KB metadata",
		zap.String("fileUID", param.FileUID.String()))

	// Get file metadata
	files, err := w.repository.GetKnowledgeBaseFilesByFileUIDs(ctx, []types.FileUIDType{param.FileUID})
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			getFileMetadataActivityError,
			err,
		)
	}
	if len(files) == 0 {
		// File record not found in database - this indicates:
		// - File was deleted before processing started
		// - Invalid file UID was provided
		// - Test uploaded file without creating proper DB record (e.g., undefined content constant)
		// Return a non-retryable error to fail the workflow immediately without retries
		w.log.Info("GetFileMetadataActivity: File not found (file record missing from database)",
			zap.String("fileUID", param.FileUID.String()))
		err := fmt.Errorf("file not found: %s", param.FileUID.String())
		return nil, temporal.NewNonRetryableApplicationError(
			"File not found",
			getFileMetadataActivityError, err,
		)
	}
	file := files[0]

	// Get KB's model family to determine caching strategy (Gemini only)
	kbWithConfig, err := w.repository.GetKnowledgeBaseByUIDWithConfig(ctx, param.KBUID)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to get KB config: %s", errorsx.MessageOrErr(err)),
			getFileMetadataActivityError,
			err,
		)
	}

	modelFamily := kbWithConfig.SystemConfig.RAG.Embedding.ModelFamily

	// Check if dual-processing is needed (for sequential coordination after completion)
	// We query this here to avoid additional DB roundtrips later
	kb, err := w.repository.GetKnowledgeBaseByUID(ctx, param.KBUID)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to get KB: %s", errorsx.MessageOrErr(err)),
			getFileMetadataActivityError,
			err,
		)
	}

	// Check if dual-processing is needed (critical for KB updates)
	// IMPORTANT: Only check for production KBs - staging/rollback KBs are already targets
	var dualProcessingInfo *repository.DualProcessingTarget
	if kb.Staging {
		// This is a staging or rollback KB - no dual-processing needed
		w.log.Debug("Skipping dual-processing check (staging/rollback KB)",
			zap.String("kbUID", param.KBUID.String()),
			zap.String("kbID", kb.KBID))
	} else {
		// This is a production KB - check if we need to trigger target files
		dualTarget, err := w.repository.GetDualProcessingTarget(ctx, kb)
		if err != nil {
			// CRITICAL: If we can't check dual-processing requirements, we might miss triggering target files
			// This would cause target KB files to remain NOTSTARTED, blocking synchronization
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to check dual-processing requirements for KB %s: %s", param.KBUID.String(), errorsx.MessageOrErr(err)),
				getFileMetadataActivityError,
				err,
			)
		}
		if dualTarget != nil && dualTarget.IsNeeded {
			dualProcessingInfo = dualTarget
			w.log.Info("Dual-processing will be coordinated after completion",
				zap.String("targetKBUID", dualTarget.TargetKB.UID.String()),
				zap.String("phase", dualTarget.Phase))
		}
	}

	return &GetFileMetadataActivityResult{
		File:               &file,
		ExternalMetadata:   file.ExternalMetadataUnmarshal,
		KBModelFamily:      modelFamily,
		DualProcessingInfo: dualProcessingInfo,
	}, nil
}

// GetFileContentActivity retrieves file content from MinIO
// This is a single MinIO read operation - idempotent
func (w *Worker) GetFileContentActivity(ctx context.Context, param *GetFileContentActivityParam) ([]byte, error) {
	w.log.Info("GetFileContentActivity: Fetching file from MinIO",
		zap.String("bucket", param.Bucket),
		zap.String("destination", param.Destination))

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = CreateAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to create authenticated context: %s", errorsx.MessageOrErr(err)),
				getFileContentActivityError,
				err,
			)
		}
	}

	content, err := w.repository.GetMinIOStorage().GetFile(authCtx, param.Bucket, param.Destination)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to retrieve file from storage: %s", errorsx.MessageOrErr(err)),
			getFileContentActivityError,
			err,
		)
	}

	return content, nil
}

// ===== FILE MANAGEMENT ACTIVITIES =====

// DeleteOldConvertedFilesActivityParam for removing old converted files
type DeleteOldConvertedFilesActivityParam struct {
	FileUID types.FileUIDType // File unique identifier to clean up old conversions for
}

// CreateConvertedFileRecordActivityParam for creating a converted file DB record
type CreateConvertedFileRecordActivityParam struct {
	KBUID            types.KBUIDType              // Knowledge base unique identifier
	FileUID          types.FileUIDType            // Original file unique identifier
	ConvertedFileUID types.FileUIDType            // Converted file unique identifier
	ConvertedType    artifactpb.ConvertedFileType // Converted file type: content or summary
	Destination      string                       // MinIO destination path
	PositionData     *types.PositionData          // Position data from conversion (e.g., page mappings)
}

// CreateConvertedFileRecordActivityResult returns the created converted file UID
type CreateConvertedFileRecordActivityResult struct {
	ConvertedFileUID types.FileUIDType // Created converted file unique identifier
}

// UploadConvertedFileToMinIOActivityParam for uploading converted file to MinIO
type UploadConvertedFileToMinIOActivityParam struct {
	KBUID            types.KBUIDType   // Knowledge base unique identifier
	FileUID          types.FileUIDType // Original file unique identifier
	ConvertedFileUID types.FileUIDType // Converted file unique identifier
	Content          string            // Markdown content to upload
}

// UploadConvertedFileToMinIOActivityResult returns the actual MinIO destination
type UploadConvertedFileToMinIOActivityResult struct {
	Destination string // Actual MinIO destination path where file was uploaded
}

// DeleteConvertedFileRecordActivityParam for deleting a converted file DB record (compensating transaction)
type DeleteConvertedFileRecordActivityParam struct {
	ConvertedFileUID types.FileUIDType // Converted file unique identifier to delete
}

// DeleteConvertedFileFromMinIOActivityParam for deleting a converted file from MinIO (compensating transaction)
type DeleteConvertedFileFromMinIOActivityParam struct {
	Bucket      string // MinIO bucket containing the file
	Destination string // MinIO path to the file to delete
}

// UpdateConvertedFileDestinationActivityParam for updating the converted file destination in DB
type UpdateConvertedFileDestinationActivityParam struct {
	ConvertedFileUID types.FileUIDType // Converted file unique identifier
	Destination      string            // New MinIO destination path
}

// UpdateConversionMetadataActivityParam for updating file metadata after conversion
type UpdateConversionMetadataActivityParam struct {
	FileUID   types.FileUIDType // File unique identifier
	Length    []uint32          // Length of markdown sections
	Pipelines []string          // Pipelines used: [content_pipeline, summary_pipeline] (empty strings if AI client was used)
}

// UpdateUsageMetadataActivityParam for updating file usage metadata after content/summary processing
type UpdateUsageMetadataActivityParam struct {
	FileUID         types.FileUIDType // File unique identifier
	ContentMetadata any               // Usage metadata from content processing (from AI response)
	SummaryMetadata any               // Usage metadata from summary processing (from AI response)
}

// DeleteOldConvertedFilesActivity removes old converted file from MinIO and DB if it exists
// This is critical for reprocessing to avoid duplicate converted_file records
func (w *Worker) DeleteOldConvertedFilesActivity(ctx context.Context, param *DeleteOldConvertedFilesActivityParam) error {
	w.log.Info("DeleteOldConvertedFilesActivity: Checking for old converted files",
		zap.String("fileUID", param.FileUID.String()))

	// Get ALL converted files for this file UID (content + summary + any others)
	allConvertedFiles, err := w.repository.GetAllConvertedFilesByFileUID(ctx, param.FileUID)
	if err != nil {
		return temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to list converted files: %s", errorsx.MessageOrErr(err)),
			deleteOldConvertedFilesActivityError,
			err,
		)
	}

	if len(allConvertedFiles) == 0 {
		w.log.Info("DeleteOldConvertedFilesActivity: No old converted files found, skipping cleanup")
		return nil
	}

	w.log.Info("DeleteOldConvertedFilesActivity: Found converted files to delete",
		zap.Int("count", len(allConvertedFiles)))

	// Delete ALL old converted files (content, summary, PDF, etc.)
	// When reprocessing, we recreate all converted files from scratch
	// This runs BEFORE StandardizeFileTypeActivity, so old standardized files (PDF, PNG, etc.) are cleaned before new ones are created
	for _, file := range allConvertedFiles {
		w.log.Info("DeleteOldConvertedFilesActivity: Deleting old converted file",
			zap.String("oldConvertedFileUID", file.UID.String()),
			zap.String("convertedType", file.ConvertedType),
			zap.String("destination", file.Destination))

		// CRITICAL: Delete old chunk blobs FIRST (before deleting converted_file DB record)
		// This is necessary because chunks reference the converted_file UID
		// Once we delete the converted_file record, we lose track of which chunks belong to it
		oldChunks, err := w.repository.GetTextChunksBySource(ctx, repository.ConvertedFileTableName, file.UID)
		if err != nil {
			w.log.Warn("DeleteOldConvertedFilesActivity: Failed to get old chunks (continuing anyway)",
				zap.String("convertedFileUID", file.UID.String()),
				zap.Error(err))
		} else if len(oldChunks) > 0 {
			oldChunkPaths := make([]string, len(oldChunks))
			for i, chunk := range oldChunks {
				oldChunkPaths[i] = chunk.ContentDest
			}
			w.log.Info("DeleteOldConvertedFilesActivity: Deleting old chunk blobs",
				zap.String("convertedFileUID", file.UID.String()),
				zap.Int("chunkCount", len(oldChunkPaths)))

			err = w.deleteFilesSync(ctx, config.Config.Minio.BucketName, oldChunkPaths)
			if err != nil {
				w.log.Error("DeleteOldConvertedFilesActivity: Failed to delete old chunk blobs from MinIO",
					zap.String("convertedFileUID", file.UID.String()),
					zap.Error(err))
				return temporal.NewApplicationErrorWithCause(
					fmt.Sprintf("Failed to delete old chunk blobs from MinIO: %s", errorsx.MessageOrErr(err)),
					deleteOldConvertedFilesActivityError,
					err,
				)
			}
			w.log.Info("DeleteOldConvertedFilesActivity: Successfully deleted old chunk blobs",
				zap.String("convertedFileUID", file.UID.String()),
				zap.Int("deletedCount", len(oldChunkPaths)))
		}

		// Delete converted file from MinIO
		// Note: MinIO DeleteFile is idempotent - if file doesn't exist, it succeeds.
		// Any error returned here is a real error (network, permissions, etc.) that should be retried.
		err = w.repository.GetMinIOStorage().DeleteFile(ctx, config.Config.Minio.BucketName, file.Destination)
		if err != nil {
			w.log.Error("DeleteOldConvertedFilesActivity: Failed to delete old converted file from MinIO",
				zap.String("destination", file.Destination),
				zap.Error(err))
			return temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to delete old converted file from MinIO: %s", errorsx.MessageOrErr(err)),
				deleteOldConvertedFilesActivityError,
				err,
			)
		}

		// IMPORTANT: Also delete the old converted_file DB record
		// This prevents queries from returning the old record during reprocessing,
		// which would cause duplicate key errors when creating new chunks with the old source_uid
		err = w.repository.DeleteConvertedFile(ctx, file.UID)
		if err != nil {
			w.log.Error("DeleteOldConvertedFilesActivity: Failed to delete old converted file DB record",
				zap.String("convertedFileUID", file.UID.String()),
				zap.Error(err))
			return temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to delete old converted file DB record: %s", errorsx.MessageOrErr(err)),
				deleteOldConvertedFilesActivityError,
				err,
			)
		}

		w.log.Info("DeleteOldConvertedFilesActivity: Successfully deleted old converted file from both MinIO and DB",
			zap.String("oldConvertedFileUID", file.UID.String()))
	}

	return nil
}

// CreateConvertedFileRecordActivity creates a DB record for the converted file
// This is a separate activity from the MinIO upload to properly decouple DB and storage operations
// Returns the created converted file UID
func (w *Worker) CreateConvertedFileRecordActivity(ctx context.Context, param *CreateConvertedFileRecordActivityParam) (*CreateConvertedFileRecordActivityResult, error) {
	w.log.Info("CreateConvertedFileRecordActivity: Creating DB record",
		zap.String("fileUID", param.FileUID.String()),
		zap.String("convertedFileUID", param.ConvertedFileUID.String()))

	convertedFile := repository.ConvertedFileModel{
		UID:           param.ConvertedFileUID,
		KBUID:         param.KBUID,
		FileUID:       param.FileUID,
		ContentType:   "text/markdown",
		ConvertedType: param.ConvertedType.String(),
		Destination:   param.Destination,
		PositionData:  param.PositionData,
	}

	createdFile, err := w.repository.CreateConvertedFileWithDestination(ctx, convertedFile)
	if err != nil {
		w.log.Error("CreateConvertedFileRecordActivity: Failed to create DB record",
			zap.String("fileUID", param.FileUID.String()),
			zap.Error(err))
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to create converted file record: %s", errorsx.MessageOrErr(err)),
			createConvertedFileRecordActivityError,
			err,
		)
	}

	return &CreateConvertedFileRecordActivityResult{
		ConvertedFileUID: createdFile.UID,
	}, nil
}

// UploadConvertedFileToMinIOActivity uploads the converted file content to MinIO
// This is a separate activity from the DB record creation to properly decouple operations
func (w *Worker) UploadConvertedFileToMinIOActivity(ctx context.Context, param *UploadConvertedFileToMinIOActivityParam) (*UploadConvertedFileToMinIOActivityResult, error) {
	w.log.Info("UploadConvertedFileToMinIOActivity: Uploading to MinIO",
		zap.String("convertedFileUID", param.ConvertedFileUID.String()))

	blobStorage := w.repository
	destination, err := blobStorage.GetMinIOStorage().SaveConvertedFile(
		ctx,
		param.KBUID,
		param.FileUID,
		param.ConvertedFileUID,
		"md",
		[]byte(param.Content),
	)
	if err != nil {
		w.log.Error("UploadConvertedFileToMinIOActivity: Failed to upload to MinIO",
			zap.Error(err))
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to upload converted file: %s", errorsx.MessageOrErr(err)),
			uploadConvertedFileToMinIOActivityError,
			err,
		)
	}

	return &UploadConvertedFileToMinIOActivityResult{
		Destination: destination,
	}, nil
}

// DeleteConvertedFileRecordActivity deletes a converted file DB record
// This is used as a compensating transaction when MinIO upload fails
func (w *Worker) DeleteConvertedFileRecordActivity(ctx context.Context, param *DeleteConvertedFileRecordActivityParam) error {
	w.log.Info("DeleteConvertedFileRecordActivity: Deleting DB record",
		zap.String("convertedFileUID", param.ConvertedFileUID.String()))

	err := w.repository.DeleteConvertedFile(ctx, param.ConvertedFileUID)
	if err != nil {
		w.log.Error("DeleteConvertedFileRecordActivity: Failed to delete DB record",
			zap.String("convertedFileUID", param.ConvertedFileUID.String()),
			zap.Error(err))
		return temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to delete converted file record: %s", errorsx.MessageOrErr(err)),
			deleteConvertedFileRecordActivityError,
			err,
		)
	}

	return nil
}

// UpdateConvertedFileDestinationActivity updates the DB record with the actual MinIO destination
// This is called after successful MinIO upload to update the placeholder destination
func (w *Worker) UpdateConvertedFileDestinationActivity(ctx context.Context, param *UpdateConvertedFileDestinationActivityParam) error {
	w.log.Info("UpdateConvertedFileDestinationActivity: Updating destination",
		zap.String("convertedFileUID", param.ConvertedFileUID.String()),
		zap.String("destination", param.Destination))

	// Update the destination
	update := map[string]any{"destination": param.Destination}
	err := w.repository.UpdateConvertedFile(ctx, param.ConvertedFileUID, update)
	if err != nil {
		w.log.Error("UpdateConvertedFileDestinationActivity: Failed to update destination",
			zap.String("convertedFileUID", param.ConvertedFileUID.String()),
			zap.Error(err))
		return temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to update file destination: %s", errorsx.MessageOrErr(err)),
			updateConvertedFileDestinationActivityError,
			err,
		)
	}

	return nil
}

// DeleteConvertedFileFromMinIOActivity deletes a converted file from MinIO
// This is used as a compensating transaction when DB operations fail after successful upload
func (w *Worker) DeleteConvertedFileFromMinIOActivity(ctx context.Context, param *DeleteConvertedFileFromMinIOActivityParam) error {
	w.log.Info("DeleteConvertedFileFromMinIOActivity: Deleting file from MinIO",
		zap.String("destination", param.Destination))

	blobStorage := w.repository
	err := blobStorage.GetMinIOStorage().DeleteFile(ctx, param.Bucket, param.Destination)
	if err != nil {
		w.log.Error("DeleteConvertedFileFromMinIOActivity: Failed to delete from MinIO",
			zap.String("destination", param.Destination),
			zap.Error(err))
		return temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to delete converted file from storage: %s", errorsx.MessageOrErr(err)),
			deleteConvertedFileFromMinIOActivityError,
			err,
		)
	}

	return nil
}

// UpdateConversionMetadataActivity updates file metadata after conversion
// This is a single DB write operation - idempotent
func (w *Worker) UpdateConversionMetadataActivity(ctx context.Context, param *UpdateConversionMetadataActivityParam) error {
	w.log.Info("UpdateConversionMetadataActivity: Updating file metadata",
		zap.String("fileUID", param.FileUID.String()),
		zap.Strings("pipelines", param.Pipelines))

	// Extract content and summary pipelines from array
	var contentPipeline, summaryPipeline string
	if len(param.Pipelines) > 0 {
		contentPipeline = param.Pipelines[0]
	}
	if len(param.Pipelines) > 1 {
		summaryPipeline = param.Pipelines[1]
	}

	mdUpdate := repository.ExtraMetaData{
		Length:          param.Length,
		ConvertingPipe:  contentPipeline,
		SummarizingPipe: summaryPipeline,
	}

	err := w.repository.UpdateKnowledgeFileMetadata(ctx, param.FileUID, mdUpdate)
	if err != nil {
		// If file not found, it may have been deleted during processing - this is OK
		if errors.Is(err, gorm.ErrRecordNotFound) {
			w.log.Info("UpdateConversionMetadataActivity: File not found (may have been deleted), skipping metadata update",
				zap.String("fileUID", param.FileUID.String()))
			return nil
		}
		return temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to update file metadata: %s", errorsx.MessageOrErr(err)),
			updateConversionMetadataActivityError,
			err,
		)
	}

	return nil
}

// UpdateUsageMetadataActivity stores AI usage metadata (token counts) from content and summary processing
// This activity aggregates usage metadata from both content and summary processing activities
// and stores it in the file's usage_metadata JSONB column for later retrieval
// According to Gemini API docs: https://ai.google.dev/gemini-api/docs/tokens?lang=python
// The usage metadata includes prompt_token_count, candidates_token_count, total_token_count, etc.
func (w *Worker) UpdateUsageMetadataActivity(ctx context.Context, param *UpdateUsageMetadataActivityParam) error {
	w.log.Info("UpdateUsageMetadataActivity: Storing usage metadata",
		zap.String("fileUID", param.FileUID.String()),
		zap.Bool("hasContentMetadata", param.ContentMetadata != nil),
		zap.Bool("hasSummaryMetadata", param.SummaryMetadata != nil))

	// Early return if both metadata are nil - no need to do a database update
	// This will be the case until we implement CountTokens in ProcessContent/ProcessSummary activities
	if param.ContentMetadata == nil && param.SummaryMetadata == nil {
		w.log.Info("UpdateUsageMetadataActivity: Skipping - no usage metadata available yet",
			zap.String("fileUID", param.FileUID.String()))
		return nil
	}

	// Build usage metadata structure
	// Format: {"content": {...}, "summary": {...}}
	usageMetadata := repository.UsageMetadata{
		Content: make(map[string]interface{}),
		Summary: make(map[string]interface{}),
	}

	// Store content metadata if available
	if param.ContentMetadata != nil {
		if contentMap, ok := param.ContentMetadata.(map[string]interface{}); ok {
			usageMetadata.Content = contentMap
		} else {
			// If it's a struct, convert it to map via JSON marshaling
			contentBytes, err := json.Marshal(param.ContentMetadata)
			if err == nil {
				_ = json.Unmarshal(contentBytes, &usageMetadata.Content)
			}
		}
	}

	// Store summary metadata if available
	if param.SummaryMetadata != nil {
		if summaryMap, ok := param.SummaryMetadata.(map[string]interface{}); ok {
			usageMetadata.Summary = summaryMap
		} else {
			// If it's a struct, convert it to map via JSON marshaling
			summaryBytes, err := json.Marshal(param.SummaryMetadata)
			if err == nil {
				_ = json.Unmarshal(summaryBytes, &usageMetadata.Summary)
			}
		}
	}

	// Update file record with usage metadata using the repository method
	err := w.repository.UpdateKnowledgeFileUsageMetadata(ctx, param.FileUID, usageMetadata)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			w.log.Info("UpdateUsageMetadataActivity: File not found (may have been deleted), skipping metadata update",
				zap.String("fileUID", param.FileUID.String()))
			return nil
		}
		return temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to update usage metadata: %s", errorsx.MessageOrErr(err)),
			updateUsageMetadataActivityError,
			err,
		)
	}

	w.log.Info("UpdateUsageMetadataActivity: Successfully stored usage metadata",
		zap.String("fileUID", param.FileUID.String()))

	return nil
}

// ===== CHUNKING ACTIVITIES =====

// ChunkContentActivityParam for internal text chunking (page-based)
type ChunkContentActivityParam struct {
	FileUID      types.FileUIDType     // File unique identifier
	KBUID        types.KBUIDType       // Knowledge base unique identifier
	Content      string                // Content to chunk into text chunks
	Metadata     *structpb.Struct      // Request metadata for authentication
	Type         artifactpb.Chunk_Type // Type of chunk: TYPE_CONTENT or TYPE_SUMMARY
	PositionData *types.PositionData   // Position data from conversion (page delimiters)
}

// ChunkContentActivityResult contains chunked content
type ChunkContentActivityResult struct {
	TextChunks []types.TextChunk // Generated text chunks
}

// ChunkContentActivity chunks content by pages using position data
// Each page becomes a separate chunk for accurate page-level citations
// For non-paginated content (like summaries), returns empty to skip chunking
func (w *Worker) ChunkContentActivity(ctx context.Context, param *ChunkContentActivityParam) (*ChunkContentActivityResult, error) {
	w.log.Info("ChunkContentActivity: Chunking content by pages",
		zap.String("fileUID", param.FileUID.String()),
		zap.String("type", param.Type.String()),
		zap.Bool("hasPositionData", param.PositionData != nil))

	// Check if we have position data (passed from ProcessContent/ProcessSummary activities)
	if param.PositionData == nil || len(param.PositionData.PageDelimiters) == 0 {
		w.log.Info("ChunkContentActivity: No page delimiters, treating entire file as single chunk")
		// For non-paginated files (TXT, MD, CSV, HTML), treat entire content as one chunk
		// TODO: Replace EstimateTokenCount with actual AI token count from CountTokens API
		// This estimation is inaccurate and should be replaced with actual token counts from the AI model
		// For now, we use a simple heuristic: 1 token ≈ 4 characters
		tokens := ai.EstimateTokenCount(param.Content)
		chunk := types.TextChunk{
			Text:   param.Content,
			Start:  0,
			End:    len(param.Content),
			Tokens: tokens,
			Reference: &types.TextChunkReference{
				PageRange: [2]uint32{1, 1}, // Treat as single-page file
			},
			Type: param.Type, // Set type (chunk or summary)
		}
		w.log.Info("ChunkContentActivity: Created single text chunk",
			zap.Int("contentLength", len(param.Content)),
			zap.Int("tokens", tokens))
		return &ChunkContentActivityResult{
			TextChunks: []types.TextChunk{chunk},
		}, nil
	}

	contentRunes := []rune(param.Content)
	lastDelimiter := param.PositionData.PageDelimiters[len(param.PositionData.PageDelimiters)-1]

	// Check if content length matches page delimiters (i.e., it's the converted markdown, not summary)
	// If content is much shorter/longer than expected, it's probably summary or other text
	contentLen := uint32(len(contentRunes))
	if contentLen < lastDelimiter/2 || contentLen > lastDelimiter*2 {
		w.log.Info("ChunkContentActivity: Content length mismatch with page delimiters, treating as single chunk",
			zap.Uint32("contentLen", contentLen),
			zap.Uint32("expectedLen", lastDelimiter))
		// Fall back to single chunk for summaries or mismatched content
		// TODO: Replace EstimateTokenCount with actual AI token count from CountTokens API
		tokens := ai.EstimateTokenCount(param.Content)
		chunk := types.TextChunk{
			Text:   param.Content,
			Start:  0,
			End:    len(param.Content),
			Tokens: tokens,
			Reference: &types.TextChunkReference{
				PageRange: [2]uint32{1, 1}, // Treat as single-page file
			},
			Type: param.Type, // Set type (chunk or summary)
		}
		w.log.Info("ChunkContentActivity: Created single text chunk (fallback)",
			zap.Int("contentLength", len(param.Content)),
			zap.Int("tokens", tokens))
		return &ChunkContentActivityResult{
			TextChunks: []types.TextChunk{chunk},
		}, nil
	}

	// Chunk by pages using position data
	w.log.Info("ChunkContentActivity: Chunking by page delimiters",
		zap.Int("pageCount", len(param.PositionData.PageDelimiters)))

	chunks := make([]types.TextChunk, 0, len(param.PositionData.PageDelimiters))
	var startPos uint32

	for pageNum, endPos := range param.PositionData.PageDelimiters {
		// Handle edge cases
		if int(endPos) > len(contentRunes) {
			endPos = uint32(len(contentRunes))
		}
		if startPos >= endPos {
			continue
		}

		// Extract page text
		pageText := string(contentRunes[startPos:endPos])

		// Skip empty pages
		if len(strings.TrimSpace(pageText)) == 0 {
			startPos = endPos
			continue
		}

		// Create chunk for this page
		// TODO: Replace EstimateTokenCount with actual AI token count from CountTokens API
		tokens := ai.EstimateTokenCount(pageText)
		chunk := types.TextChunk{
			Text:   pageText,
			Start:  int(startPos),
			End:    int(endPos),
			Tokens: tokens,
			Reference: &types.TextChunkReference{
				PageRange: [2]uint32{uint32(pageNum + 1), uint32(pageNum + 1)}, // Single page
			},
			Type: param.Type, // Set type (chunk or summary)
		}
		chunks = append(chunks, chunk)

		startPos = endPos
	}

	w.log.Info("ChunkContentActivity: Page-based chunking successful",
		zap.Int("chunkCount", len(chunks)),
		zap.Int("pageCount", len(param.PositionData.PageDelimiters)))

	return &ChunkContentActivityResult{
		TextChunks: chunks,
	}, nil
}

// DeleteOldTextChunksActivityParam for deleting old text chunk DB records
type DeleteOldTextChunksActivityParam struct {
	FileUID types.FileUIDType // File unique identifier
}

// DeleteOldTextChunksActivity deletes old text chunk DB records for a file
// Note: Chunk blobs are deleted by DeleteOldConvertedFilesActivity (when converted files are deleted)
// This activity only deletes the chunk table records
func (w *Worker) DeleteOldTextChunksActivity(ctx context.Context, param *DeleteOldTextChunksActivityParam) error {
	w.log.Info("DeleteOldTextChunksActivity: Deleting old text chunk records",
		zap.String("fileUID", param.FileUID.String()))

	// Delete all text chunk records for this file from the database
	// This uses hard delete (Unscoped) to remove records completely
	err := w.repository.HardDeleteTextChunksByKBFileUID(ctx, param.FileUID)
	if err != nil {
		w.log.Error("DeleteOldTextChunksActivity: Failed to delete text chunk records",
			zap.String("fileUID", param.FileUID.String()),
			zap.Error(err))
		return temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to delete old text chunk records: %s", errorsx.MessageOrErr(err)),
			deleteOldTextChunksActivityError,
			err,
		)
	}

	w.log.Info("DeleteOldTextChunksActivity: Successfully deleted old text chunk records",
		zap.String("fileUID", param.FileUID.String()))

	return nil
}

// SaveTextChunksActivityParam for saving text chunks to database and MinIO
type SaveTextChunksActivityParam struct {
	FileUID          types.FileUIDType          // File unique identifier
	KBUID            types.KBUIDType            // Knowledge base unique identifier
	TextChunks       []types.TextChunk          // Text chunks to save
	ConvertedFileUID types.ConvertedFileUIDType // Converted file UID (content or summary)
}

// SaveTextChunksActivityResult contains saved text chunk UIDs from SaveTextChunksActivity
type SaveTextChunksActivityResult struct {
	TextChunkUIDs []types.TextChunkUIDType // Saved text chunk unique identifiers
}

// SaveTextChunksActivity saves text chunks to database and MinIO storage
// Note: Old data cleanup is handled at workflow level before this activity:
//   - Chunk blobs: DeleteOldConvertedFilesActivity (when converted files are deleted)
//   - Chunk DB records: DeleteOldTextChunksActivity
//
// This activity only creates new chunks without performing any deletion
func (w *Worker) SaveTextChunksActivity(ctx context.Context, param *SaveTextChunksActivityParam) (*SaveTextChunksActivityResult, error) {
	w.log.Info("SaveTextChunksActivity: Saving text chunks to database and MinIO",
		zap.String("fileUID", param.FileUID.String()),
		zap.Int("chunkCount", len(param.TextChunks)))

	// Use the provided converted file UID (either content or summary converted_file)
	convertedFileUID := param.ConvertedFileUID

	w.log.Info("SaveTextChunksActivity: Using converted file UID",
		zap.String("convertedFileUID", convertedFileUID.String()))

	// Chunks already have page references from ChunkContentActivity
	// No need to add them again
	chunksWithReferences := param.TextChunks

	// Log how many chunks already have references
	chunksWithRefs := 0
	for _, chunk := range chunksWithReferences {
		if chunk.Reference != nil {
			chunksWithRefs++
		}
	}
	w.log.Info("SaveTextChunksActivity: Chunks with references",
		zap.Int("totalChunks", len(chunksWithReferences)),
		zap.Int("chunksWithRefs", chunksWithRefs))

	// IMPORTANT: Save chunks to database and upload to MinIO
	// Step 1: Create text chunk models
	textChunks := make([]*repository.TextChunkModel, len(chunksWithReferences))
	texts := make([]string, len(chunksWithReferences))
	for i, c := range chunksWithReferences {
		// Convert protobuf Chunk.Type enum to string for database storage using .String() method
		if c.Type == artifactpb.Chunk_TYPE_UNSPECIFIED {
			w.log.Warn("SaveTextChunksActivity: Type is UNSPECIFIED, defaulting to content",
				zap.Int("chunkIndex", i),
				zap.String("fileUID", param.FileUID.String()))
		}

		textChunks[i] = &repository.TextChunkModel{
			SourceUID:   convertedFileUID, // Use the provided converted file UID (content or summary)
			SourceTable: repository.ConvertedFileTableName,
			StartPos:    c.Start,
			EndPos:      c.End,
			Reference:   c.Reference,
			ContentDest: "pending", // Placeholder, will be updated after MinIO save
			Tokens:      c.Tokens,
			Retrievable: true,
			InOrder:     i,
			KBUID:       param.KBUID,
			FileUID:     param.FileUID,
			ContentType: "text/markdown",
			ChunkType:   c.Type.String(),
		}
		texts[i] = c.Text
	}

	// Step 2: Save text chunks to database with placeholder destinations
	// Note: Old chunks are deleted by DeleteOldTextChunksActivity at workflow level
	// This activity only creates new chunks
	err := w.repository.CreateTextChunks(ctx, textChunks)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to save text chunks to database: %s", errorsx.MessageOrErr(err)),
			saveTextChunksActivityError,
			err,
		)
	}
	createdChunks := textChunks

	// Step 3: Upload text chunks to MinIO after DB transaction commits
	destinations := make(map[string]string, len(createdChunks))
	for i, chunk := range createdChunks {
		chunkUID := chunk.UID.String()

		// Construct the MinIO path using the format: kb-{kbUID}/file-{fileUID}/chunk/{chunkUID}.md
		basePath := fmt.Sprintf("kb-%s/file-%s/chunk", param.KBUID.String(), param.FileUID.String())
		path := fmt.Sprintf("%s/%s.md", basePath, chunkUID)

		// Encode text chunk content to base64
		base64Content := base64.StdEncoding.EncodeToString([]byte(texts[i]))

		// Save text chunk content to MinIO
		err := w.repository.GetMinIOStorage().UploadBase64File(
			ctx,
			config.Config.Minio.BucketName,
			path,
			base64Content,
			"text/markdown",
		)
		if err != nil {
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to upload text chunk (ID: %s) to MinIO: %s", chunkUID, errorsx.MessageOrErr(err)),
				saveTextChunksActivityError,
				err,
			)
		}

		destinations[chunkUID] = path
	}

	// Step 4: Update text chunk destinations in database
	err = w.repository.UpdateTextChunkDestinations(ctx, destinations)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to update text chunk destinations: %s", errorsx.MessageOrErr(err)),
			saveTextChunksActivityError,
			err,
		)
	}

	w.log.Info("SaveTextChunksActivity: Successfully saved text chunks",
		zap.String("fileUID", param.FileUID.String()),
		zap.Int("chunkCount", len(createdChunks)))

	return &SaveTextChunksActivityResult{
		TextChunkUIDs: nil,
	}, nil
}

// ===== FILE TYPE CONVERSION ACTIVITY =====

// StandardizeFileTypeActivityParam defines the parameters for StandardizeFileTypeActivity
type StandardizeFileTypeActivityParam struct {
	FileUID     types.FileUIDType    // File unique identifier
	KBUID       types.KBUIDType      // Knowledge base unique identifier
	Bucket      string               // MinIO bucket containing the file
	Destination string               // MinIO path to the file
	FileType    artifactpb.File_Type // Original file type to convert from
	Filename    string               // Filename for identification
	Pipelines   []pipeline.Release   // indexing-convert-file-type pipeline
	Metadata    *structpb.Struct     // Request metadata for authentication
}

// StandardizeFileTypeActivityResult defines the result of StandardizeFileTypeActivity
type StandardizeFileTypeActivityResult struct {
	ConvertedDestination string               // MinIO path to converted file (empty if no conversion)
	ConvertedBucket      string               // MinIO bucket for converted file (empty if no conversion)
	ConvertedType        artifactpb.File_Type // New file type after conversion
	OriginalType         artifactpb.File_Type // Original file type
	Converted            bool                 // Whether conversion was performed
	PipelineRelease      pipeline.Release     // Pipeline used for conversion
	ConvertedFileUID     types.FileUIDType    // UUID of created converted_file record (nil if not saved as converted_file)
	PositionData         *types.PositionData  // Position data from conversion (for PDFs)
}

// StandardizeFileTypeActivity standardizes non-AI-native file types to AI-supported formats
// Following format mappings defined in the AI component
func (w *Worker) StandardizeFileTypeActivity(ctx context.Context, param *StandardizeFileTypeActivityParam) (*StandardizeFileTypeActivityResult, error) {
	w.log.Info("StandardizeFileTypeActivity: Checking if file needs standardization",
		zap.String("fileType", param.FileType.String()),
		zap.String("filename", param.Filename))

	// Check if file needs conversion
	needsConversion, targetFormat, _ := filetype.NeedsFileTypeConversion(param.FileType)
	if !needsConversion {
		w.log.Info("StandardizeFileTypeActivity: File type is AI-native, no standardization needed",
			zap.String("fileType", param.FileType.String()))

		// Original blob remains in place for consistency with other file types
		// Converted-file folder copy provides unified VIEW_STANDARD_FILE_TYPE access
		// For AI-native files (PDF, PNG, OGG, MP4), copy to converted-file folder
		convertedFileTypeEnum, fileExtension, contentType := filetype.GetConvertedFileTypeInfo(param.FileType)
		shouldCopy := convertedFileTypeEnum != artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_UNSPECIFIED

		if shouldCopy {
			w.log.Info("StandardizeFileTypeActivity: Copying AI-native file to converted-file folder for VIEW_STANDARD_FILE_TYPE support",
				zap.String("fileType", param.FileType.String()),
				zap.String("extension", fileExtension))

			// Create authenticated context if metadata provided
			authCtx := ctx
			if param.Metadata != nil {
				var err error
				authCtx, err = CreateAuthenticatedContext(ctx, param.Metadata)
				if err != nil {
					return nil, temporal.NewApplicationErrorWithCause(
						fmt.Sprintf("Failed to create authenticated context: %s", errorsx.MessageOrErr(err)),
						standardizeFileTypeActivityError,
						err,
					)
				}
			}

			// Fetch original file content from MinIO blob location
			fileContent, err := w.repository.GetMinIOStorage().GetFile(authCtx, param.Bucket, param.Destination)
			if err != nil {
				return nil, temporal.NewApplicationErrorWithCause(
					fmt.Sprintf("Failed to retrieve original file from storage: %s", errorsx.MessageOrErr(err)),
					standardizeFileTypeActivityError,
					err,
				)
			}

			// Generate UUID for the converted file
			convertedFileUID := types.FileUIDType(uuid.Must(uuid.NewV4()))

			// Upload to MinIO FIRST (before creating DB record)
			// This makes the activity idempotent - retries will generate new UUIDs and upload new files
			convertedDestination, err := w.repository.GetMinIOStorage().SaveConvertedFile(
				authCtx,
				param.KBUID,
				param.FileUID,
				convertedFileUID,
				fileExtension,
				fileContent,
			)
			if err != nil {
				return nil, temporal.NewApplicationErrorWithCause(
					fmt.Sprintf("Failed to copy original file to converted-file folder: %s", errorsx.MessageOrErr(err)),
					standardizeFileTypeActivityError,
					err,
				)
			}

			// Create converted_file DB record with actual destination (after successful upload)
			convertedFileRecord := repository.ConvertedFileModel{
				UID:           convertedFileUID,
				KBUID:         param.KBUID,
				FileUID:       param.FileUID,
				ContentType:   contentType,
				ConvertedType: convertedFileTypeEnum.String(),
				Destination:   convertedDestination, // Use actual destination, not placeholder
				PositionData:  nil,
			}

			_, err = w.repository.CreateConvertedFileWithDestination(authCtx, convertedFileRecord)
			if err != nil {
				// Compensate: delete the uploaded MinIO file
				_ = w.repository.GetMinIOStorage().DeleteFile(authCtx, config.Config.Minio.BucketName, convertedDestination)
				return nil, temporal.NewApplicationErrorWithCause(
					fmt.Sprintf("Failed to create converted_file record: %s", errorsx.MessageOrErr(err)),
					standardizeFileTypeActivityError,
					err,
				)
			}

			w.log.Info("StandardizeFileTypeActivity: AI-native file copied to converted-file folder",
				zap.String("fileType", param.FileType.String()),
				zap.String("originalDestination", param.Destination),
				zap.String("convertedFileUID", convertedFileUID.String()),
				zap.String("convertedDestination", convertedDestination))

			// Return original location for processing (ProcessContentActivity needs original blob)
			// but also include converted file info for VIEW_STANDARD_FILE_TYPE access
			return &StandardizeFileTypeActivityResult{
				ConvertedDestination: param.Destination, // Use original blob for processing
				ConvertedBucket:      param.Bucket,      // Use original blob bucket
				ConvertedType:        param.FileType,
				OriginalType:         param.FileType,
				Converted:            false,            // Not converted, just copied
				ConvertedFileUID:     convertedFileUID, // Track the converted_file record
				PositionData:         nil,
			}, nil
		}

		// For AI-native files that don't need conversion and no standard file type support
		return &StandardizeFileTypeActivityResult{
			ConvertedDestination: "", // No conversion, use original file
			ConvertedBucket:      "",
			ConvertedType:        param.FileType,
			OriginalType:         param.FileType,
			Converted:            false,
			ConvertedFileUID:     types.FileUIDType(uuid.Nil),
			PositionData:         nil,
		}, nil
	}

	w.log.Info("StandardizeFileTypeActivity: File type needs standardization",
		zap.String("originalType", param.FileType.String()),
		zap.String("targetFormat", targetFormat))

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = CreateAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to create authenticated context: %s", errorsx.MessageOrErr(err)),
				standardizeFileTypeActivityError,
				err,
			)
		}
	}

	// Fetch original file content from MinIO
	content, err := w.repository.GetMinIOStorage().GetFile(authCtx, param.Bucket, param.Destination)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to retrieve file from storage: %s", errorsx.MessageOrErr(err)),
			standardizeFileTypeActivityError,
			err,
		)
	}

	w.log.Info("StandardizeFileTypeActivity: File content retrieved from MinIO",
		zap.Int("contentSize", len(content)))

	// Convert using indexing-convert-file-type pipeline (handles both data URI and blob URL outputs)
	mimeType := filetype.FileTypeToMimeType(param.FileType)
	convertedContent, err := pipeline.ConvertFileTypePipe(authCtx, w.pipelineClient, content, param.FileType, mimeType)
	if err != nil {
		w.log.Warn("StandardizeFileTypeActivity: Pipeline standardization failed",
			zap.Error(err),
			zap.String("pipeline", pipeline.ConvertFileTypePipeline.Name()))
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to convert file: %s", errorsx.MessageOrErr(err)),
			standardizeFileTypeActivityError,
			err,
		)
	}

	w.log.Info("StandardizeFileTypeActivity: File standardization successful",
		zap.String("originalType", param.FileType.String()),
		zap.String("convertedType", targetFormat),
		zap.Int("originalSize", len(content)),
		zap.Int("convertedSize", len(convertedContent)))

	// Map target format string to FileType enum
	convertedFileType := filetype.FormatToFileType(targetFormat)

	// Get MIME type for the converted file type
	convertedMimeType := filetype.FileTypeToMimeType(convertedFileType)

	// Map target format to ConvertedFileType enum
	var convertedFileTypeEnum artifactpb.ConvertedFileType
	var fileExtension string
	switch targetFormat {
	case "pdf":
		convertedFileTypeEnum = artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_DOCUMENT
		fileExtension = "pdf"
	case "png":
		convertedFileTypeEnum = artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_IMAGE
		fileExtension = "png"
	case "ogg":
		convertedFileTypeEnum = artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_AUDIO
		fileExtension = "ogg"
	case "mp4":
		convertedFileTypeEnum = artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_VIDEO
		fileExtension = "mp4"
	default:
		return nil, temporal.NewApplicationError(
			fmt.Sprintf("Unsupported target format: %s", targetFormat),
			standardizeFileTypeActivityError,
		)
	}

	// Save ALL standardized files (PDF, PNG, OGG, MP4) directly to converted-file folder
	// This provides unified VIEW_STANDARD_FILE_TYPE support for all media types:
	// - Documents → PDF
	// - Images → PNG
	// - Audio → OGG
	// - Video → MP4
	w.log.Info("StandardizeFileTypeActivity: Saving standardized file directly to converted-file folder",
		zap.String("targetFormat", targetFormat),
		zap.String("convertedFileType", convertedFileTypeEnum.String()))

	// Generate UUID for the converted file
	convertedFileUID := types.FileUIDType(uuid.Must(uuid.NewV4()))

	// Upload to MinIO FIRST (before creating DB record)
	// This makes the activity idempotent - retries will generate new UUIDs and upload new files
	convertedDestination, err := w.repository.GetMinIOStorage().SaveConvertedFile(
		authCtx,
		param.KBUID,
		param.FileUID,
		convertedFileUID,
		fileExtension,
		convertedContent,
	)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to upload standardized file to MinIO: %s", errorsx.MessageOrErr(err)),
			standardizeFileTypeActivityError,
			err,
		)
	}

	w.log.Info("StandardizeFileTypeActivity: Standardized file uploaded to MinIO",
		zap.String("destination", convertedDestination),
		zap.String("format", targetFormat))

	// Create converted_file DB record with actual destination (after successful upload)
	convertedFileRecord := repository.ConvertedFileModel{
		UID:           convertedFileUID,
		KBUID:         param.KBUID,
		FileUID:       param.FileUID,
		ContentType:   convertedMimeType,
		ConvertedType: convertedFileTypeEnum.String(),
		Destination:   convertedDestination, // Use actual destination, not placeholder
		PositionData:  nil,                  // Position data extraction not yet implemented for converted files
	}

	_, err = w.repository.CreateConvertedFileWithDestination(authCtx, convertedFileRecord)
	if err != nil {
		// Compensate: delete the uploaded MinIO file
		_ = w.repository.GetMinIOStorage().DeleteFile(authCtx, config.Config.Minio.BucketName, convertedDestination)
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to create converted_file record: %s", errorsx.MessageOrErr(err)),
			standardizeFileTypeActivityError,
			err,
		)
	}

	w.log.Info("StandardizeFileTypeActivity: Created converted_file DB record",
		zap.String("convertedFileUID", convertedFileUID.String()),
		zap.String("convertedType", convertedFileTypeEnum.String()))

	w.log.Info("StandardizeFileTypeActivity: Standardized file saved to converted-file folder",
		zap.String("convertedFileUID", convertedFileUID.String()),
		zap.String("destination", convertedDestination),
		zap.String("format", targetFormat),
		zap.String("convertedType", convertedFileTypeEnum.String()))

	return &StandardizeFileTypeActivityResult{
		ConvertedDestination: convertedDestination,
		ConvertedBucket:      config.Config.Minio.BucketName, // Converted files go to artifact bucket
		ConvertedType:        convertedFileType,
		OriginalType:         param.FileType,
		Converted:            true,
		PipelineRelease:      pipeline.ConvertFileTypePipeline,
		ConvertedFileUID:     convertedFileUID, // Return the created converted_file UID
		PositionData:         nil,              // Position data extraction not yet implemented
	}, nil
}

// ===== CACHE FILE CONTEXT ACTIVITY =====

// CacheFileContextActivityParam defines the parameters for CacheFileContextActivity
type CacheFileContextActivityParam struct {
	FileUID     types.FileUIDType    // File unique identifier
	KBUID       types.KBUIDType      // Knowledge base unique identifier
	Bucket      string               // MinIO bucket (original or converted file)
	Destination string               // MinIO path (original or converted file)
	FileType    artifactpb.File_Type // File type for content type determination
	Filename    string               // Filename for cache display name
	Metadata    *structpb.Struct     // Request metadata for authentication
}

// CacheFileContextActivityResult defines the result of CacheFileContextActivity
type CacheFileContextActivityResult struct {
	CacheName            string    // AI cache name
	Model                string    // Model used for cache
	CreateTime           time.Time // When cache was created
	ExpireTime           time.Time // When cache will expire
	CachedContextEnabled bool      // Flag indicating if cache was created (false means caching is disabled)
	UsageMetadata        any       // Token usage metadata from AI client (nil if cache was not created)
}

// CacheFileContextActivity creates a cached context for the input file
// This enables efficient subsequent operations like conversion, analysis, etc.
// Supports: Documents (PDF, DOCX, DOC, PPTX, PPT), Images, Audio, Video
func (w *Worker) CacheFileContextActivity(ctx context.Context, param *CacheFileContextActivityParam) (*CacheFileContextActivityResult, error) {
	w.log.Info("CacheFileContextActivity: Creating cache for input content",
		zap.String("fileUID", param.FileUID.String()),
		zap.String("fileType", param.FileType.String()),
		zap.String("bucket", param.Bucket),
		zap.String("destination", param.Destination))

	// Check if AI client is available
	if w.aiClient == nil {
		w.log.Info("CacheFileContextActivity: AI client not available, skipping cache creation")
		return &CacheFileContextActivityResult{
			CachedContextEnabled: false,
		}, nil
	}

	// Check if file type is supported for caching
	if !w.aiClient.SupportsFileType(param.FileType) {
		w.log.Info("CacheFileContextActivity: File type not supported for caching",
			zap.String("fileType", param.FileType.String()))
		return &CacheFileContextActivityResult{
			CachedContextEnabled: false,
		}, nil
	}

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = CreateAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			w.log.Warn("CacheFileContextActivity: Failed to create authenticated context, proceeding without auth",
				zap.Error(err))
		}
	}

	// Fetch file content from MinIO (either original or converted file)
	content, err := w.repository.GetMinIOStorage().GetFile(authCtx, param.Bucket, param.Destination)
	if err != nil {
		// If file doesn't exist (e.g., original blob deleted or never uploaded),
		// gracefully skip caching instead of failing the workflow
		// Caching is a performance optimization, not a requirement
		w.log.Warn("CacheFileContextActivity: File not found in storage, skipping cache creation",
			zap.String("bucket", param.Bucket),
			zap.String("destination", param.Destination),
			zap.Error(err))
		return &CacheFileContextActivityResult{
			CachedContextEnabled: false,
		}, nil
	}

	w.log.Info("CacheFileContextActivity: File content retrieved from MinIO",
		zap.String("bucket", param.Bucket),
		zap.String("destination", param.Destination),
		zap.Int("contentSize", len(content)))

	// Set cache TTL (5 minutes default)
	cacheTTL := 5 * time.Minute

	// Create the cache using AI client
	// Note: Cache creation is optional - if it fails, we continue without cache
	cacheOutput, err := w.aiClient.CreateCache(authCtx, []ai.FileContent{
		{
			Content:  content,
			FileType: param.FileType,
			Filename: param.Filename,
		},
	}, cacheTTL)
	if err != nil {
		// Log the error but don't fail the activity
		// Common reasons for cache failure:
		// - Content too small (< 1024 tokens minimum)
		// - Content too large (> maximum cache size)
		// - API quota exceeded
		// - Network issues
		w.log.Warn("CacheFileContextActivity: Cache creation failed, continuing without cache",
			zap.Error(err),
			zap.String("fileUID", param.FileUID.String()),
			zap.String("fileType", param.FileType.String()))

		return &CacheFileContextActivityResult{
			CachedContextEnabled: false,
		}, nil
	}

	return &CacheFileContextActivityResult{
		CacheName:            cacheOutput.CacheName,
		Model:                cacheOutput.Model,
		CreateTime:           cacheOutput.CreateTime,
		ExpireTime:           cacheOutput.ExpireTime,
		CachedContextEnabled: true,
		UsageMetadata:        cacheOutput.UsageMetadata,
	}, nil
}

// DeleteCacheActivityParam defines the parameters for DeleteCacheActivity
type DeleteCacheActivityParam struct {
	CacheName string // AI cache name to delete
}

// DeleteCacheActivity deletes a cached context
func (w *Worker) DeleteCacheActivity(ctx context.Context, param *DeleteCacheActivityParam) error {
	if param.CacheName == "" {
		w.log.Info("DeleteCacheActivity: No cache name provided, skipping deletion")
		return nil
	}

	w.log.Info("DeleteCacheActivity: Deleting cache",
		zap.String("cacheName", param.CacheName))

	// Check if AI client is available
	if w.aiClient == nil {
		w.log.Warn("DeleteCacheActivity: AI client not available, cannot delete cache")
		return nil
	}

	err := w.aiClient.DeleteCache(ctx, param.CacheName)
	if err != nil {
		// Log error but don't fail the activity - cache will expire automatically
		w.log.Warn("DeleteCacheActivity: Failed to delete cache (will expire automatically)",
			zap.String("cacheName", param.CacheName),
			zap.Error(err))
		return nil
	}

	return nil
}

// ===== CONTENT PROCESSING ACTIVITIES =====

// ProcessContentActivityParam defines input for ProcessContentActivity
type ProcessContentActivityParam struct {
	FileUID     types.FileUIDType    // File unique identifier
	KBUID       types.KBUIDType      // Knowledge base unique identifier
	Bucket      string               // MinIO bucket
	Destination string               // MinIO path
	FileType    artifactpb.File_Type // File type
	Filename    string               // File name
	Metadata    *structpb.Struct     // Request metadata
	CacheName   string               // AI cache name
}

// ProcessContentActivityResult defines output from ProcessContentActivity
type ProcessContentActivityResult struct {
	Content          string                     // Converted markdown content
	Length           []uint32                   // Length information
	PositionData     *types.PositionData        // Position data for PDF
	OriginalType     artifactpb.File_Type       // Original file type
	ConvertedType    artifactpb.File_Type       // Converted file type
	UsageMetadata    any                        // AI token usage
	ConvertedFileUID types.ConvertedFileUIDType // Content converted file UID
	Pipeline         string                     // Pipeline used (e.g., "instill-ai/indexing-generate-content/v1.4.0" for OpenAI route, empty for AI client)
}

// ProcessContentActivity handles the entire content processing pipeline:
// 1. Cleanup old converted files
// 2. Markdown conversion
// 3. Save to database and MinIO
//
// This is a composite activity that replaces ProcessContentWorkflow for simplified architecture.
func (w *Worker) ProcessContentActivity(ctx context.Context, param *ProcessContentActivityParam) (*ProcessContentActivityResult, error) {
	logger := w.log.With(
		zap.String("fileUID", param.FileUID.String()),
		zap.String("fileType", param.FileType.String()),
		zap.Bool("hasCache", param.CacheName != ""))

	logger.Info("ProcessContentActivity started")

	result := &ProcessContentActivityResult{
		OriginalType: param.FileType,
	}

	// Phase 1: Markdown conversion
	// Note: Cleanup of old converted files is now handled in the workflow BEFORE parallel execution
	// Note: Format conversion (DOCX→PDF, GIF→PNG, etc.) has already been done at the workflow level
	// The param.Bucket, param.Destination, param.FileType already point to the converted file

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = CreateAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			logger.Error("Failed to create authenticated context", zap.Error(err))
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to create authenticated context: %s", errorsx.MessageOrErr(err)),
				processContentActivityError,
				err,
			)
		}
	}

	// Fetch file rawFileContent from MinIO
	rawFileContent, err := w.repository.GetMinIOStorage().GetFile(authCtx, param.Bucket, param.Destination)
	if err != nil {
		// Check if a converted file already exists (reprocessing scenario)
		existingConverted, checkErr := w.repository.GetConvertedFileByFileUIDAndType(ctx, param.FileUID, artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_CONTENT)
		if checkErr == nil && existingConverted != nil {
			// Original file is missing but converted content exists - skip reprocessing
			// This can happen during KB updates when original blob files are no longer available
			logger.Warn("Original file not found in storage, but converted content exists - skipping reprocessing",
				zap.String("bucket", param.Bucket),
				zap.String("destination", param.Destination),
				zap.String("existingConvertedUID", existingConverted.UID.String()),
				zap.Error(err))
			// Return success with existing content metadata
			return &ProcessContentActivityResult{
				OriginalType:     param.FileType,
				ConvertedType:    param.FileType,
				ConvertedFileUID: existingConverted.UID,
				Content:          "(using existing converted content)",
			}, nil
		}

		// No existing converted content - this is a real error
		logger.Error("Failed to retrieve file from storage", zap.Error(err))
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to retrieve file from storage: %s", errorsx.MessageOrErr(err)),
			processContentActivityError,
			err,
		)
	}

	logger.Info("File rawFileContent retrieved",
		zap.Int("rawFileContentSize", len(rawFileContent)))

	var contentInMarkdown string
	var positionData *types.PositionData
	var length []uint32
	var usageMetadata any

	// Fetch KB with config to check embedding model family for routing
	kb, err := w.repository.GetKnowledgeBaseByUIDWithConfig(authCtx, param.KBUID)
	if err != nil {
		logger.Error("Failed to get KB for routing", zap.Error(err))
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to get knowledge base: %s", errorsx.MessageOrErr(err)),
			processContentActivityError, err)
	}

	// For text-based files (TEXT/MARKDOWN/CSV/HTML), no AI conversion needed - content is already in usable format
	// These file types are directly readable as text and don't require AI processing
	if param.FileType == artifactpb.File_TYPE_TEXT ||
		param.FileType == artifactpb.File_TYPE_MARKDOWN ||
		param.FileType == artifactpb.File_TYPE_CSV ||
		param.FileType == artifactpb.File_TYPE_HTML {
		logger.Info("Text-based file - using content as-is (no AI conversion needed)",
			zap.String("fileType", param.FileType.String()))
		contentInMarkdown = string(rawFileContent)
		length = []uint32{uint32(len(contentInMarkdown))}
	} else {
		// Check for UNSPECIFIED or unsupported file types
		if param.FileType == artifactpb.File_TYPE_UNSPECIFIED {
			logger.Error("File type is UNSPECIFIED")
			return nil, temporal.NewApplicationErrorWithCause(
				"File type could not be determined. Please ensure the file has a valid extension and format.",
				processContentActivityError,
				fmt.Errorf("file type is UNSPECIFIED"),
			)
		}

		// Route based on embedding config model family
		if kb.SystemConfig.RAG.Embedding.ModelFamily == "openai" {
			logger.Info("Using OpenAI pipeline route for content conversion")

			if w.pipelineClient == nil {
				return nil, temporal.NewApplicationErrorWithCause(
					"Pipeline client not configured for OpenAI route",
					processContentActivityError, fmt.Errorf("pipeline client is nil"))
			}

			// Base64 encode file content
			base64Content := base64.StdEncoding.EncodeToString(rawFileContent)

			// Call OpenAI pipeline
			pipelineResult, err := pipeline.GenerateContentPipe(authCtx, w.pipelineClient, pipeline.GenerateContentParams{
				Base64Content: base64Content,
				Type:          param.FileType,
			})
			if err != nil {
				logger.Error("OpenAI pipeline conversion failed", zap.Error(err))

				// Check for specific OpenAI API issues that indicate non-retryable problems
				errMsg := err.Error()
				isReferenceError := strings.Contains(errMsg, "Couldn't resolve reference") &&
					strings.Contains(errMsg, "output.texts[0]")
				isEmptyOutput := strings.Contains(errMsg, "fields in the output are nil") ||
					strings.Contains(errMsg, "response is nil or has no outputs")

				if isReferenceError || isEmptyOutput {
					// OpenAI returned empty response - likely content filtering or model issue
					// Mark as end-user error (non-retryable) with helpful message
					// Note: Pipeline debug information (failed components, error details) is included in the error message
					return nil, temporal.NewNonRetryableApplicationError(
						fmt.Sprintf("AI service failed to generate content. This may be due to content filtering, document complexity, or AI service issues. Please try with a different document or contact support if the problem persists. Details: %s", err.Error()),
						processContentActivityError, err)
				}

				// Other errors (network, timeout, etc.) should retry
				// Note: Pipeline debug information (failed components, error details) is included in the error message
				return nil, temporal.NewApplicationErrorWithCause(
					fmt.Sprintf("OpenAI pipeline failed: %s", errorsx.MessageOrErr(err)),
					processContentActivityError, err)
			}

			// Record pipeline used (e.g., "instill-ai/indexing-generate-content@v1.4.0")
			result.Pipeline = pipelineResult.PipelineRelease.Name()

			// Process pipeline result (extracts [Page: X] tags and creates PageDelimiters)
			contentInMarkdown, positionData, length = ExtractPageDelimiters(
				pipelineResult.Markdown, w.log, map[string]any{"route": "openai-pipeline"})

			logger.Info("OpenAI pipeline conversion successful",
				zap.Int("markdownLength", len(contentInMarkdown)),
				zap.Int("pageCount", func() int {
					if positionData != nil {
						return len(positionData.PageDelimiters)
					}
					return 0
				}()))
		} else {
			logger.Info("Using Gemini AI client route for content conversion")

			// Check if AI client is configured
			if w.aiClient == nil {
				logger.Error("AI client not configured")
				return nil, temporal.NewApplicationErrorWithCause(
					"AI client is required for content conversion. Please configure Gemini API key in the server configuration.",
					processContentActivityError,
					fmt.Errorf("AI client not configured"),
				)
			}

			// Check if AI client supports this file type
			clientName := "unknown"
			if w.aiClient != nil {
				clientName = w.aiClient.Name()
			}

			supportsFileType := w.aiClient.SupportsFileType(param.FileType)
			logger.Info("Checking AI client file type support",
				zap.String("fileType", param.FileType.String()),
				zap.String("clientName", clientName),
				zap.Bool("supported", supportsFileType))

			if !supportsFileType {
				logger.Error("File type not supported by AI client",
					zap.String("fileType", param.FileType.String()),
					zap.String("clientName", clientName),
					zap.String("hint", "File should have been converted by StandardizeFileTypeActivity"))
				return nil, temporal.NewApplicationErrorWithCause(
					fmt.Sprintf("File type %s is not supported. The file may need to be converted to a supported format first.", param.FileType.String()),
					processContentActivityError,
					fmt.Errorf("unsupported file type: %s", param.FileType.String()),
				)
			}

			var conversionErr error

			// For other file types: Try AI client with cache first (if available)
			if param.CacheName != "" {
				logger.Info("Attempting AI conversion with cache",
					zap.String("cacheName", param.CacheName),
					zap.String("client", w.aiClient.Name()))

				conversion, err := w.aiClient.ConvertToMarkdownWithCache(
					authCtx,
					param.CacheName,
					gemini.GetGenerateContentPrompt(),
				)
				if err != nil {
					logger.Warn("Cached AI conversion failed, will try without cache", zap.Error(err))
					conversionErr = err
				} else {
					logger.Info("Cached AI conversion successful",
						zap.Int("markdownLength", len(conversion.Markdown)))

					// Process AI conversion result
					contentInMarkdown, positionData, length = ExtractPageDelimiters(
						conversion.Markdown,
						w.log,
						map[string]any{"cacheName": param.CacheName},
					)
					usageMetadata = conversion.UsageMetadata
				}
			}

			// Try AI client without cache if cached attempt failed or wasn't available
			if contentInMarkdown == "" {
				logger.Info("Attempting AI conversion without cache",
					zap.String("fileType", param.FileType.String()),
					zap.String("client", w.aiClient.Name()))

				conversion, err := w.aiClient.ConvertToMarkdownWithoutCache(
					authCtx,
					rawFileContent,
					param.FileType,
					param.Filename,
					gemini.GetGenerateContentPrompt(),
				)
				if err != nil {
					logger.Error("AI conversion failed", zap.Error(err))
					conversionErr = err
				} else {
					logger.Info("AI conversion successful",
						zap.Int("markdownLength", len(conversion.Markdown)))

					// Process AI conversion result
					contentInMarkdown, positionData, length = ExtractPageDelimiters(
						conversion.Markdown,
						w.log,
						map[string]any{"fileType": param.FileType.String()},
					)
					usageMetadata = conversion.UsageMetadata
				}
			}

			// If conversion still failed, return the actual error
			if contentInMarkdown == "" {
				logger.Error("AI conversion failed to produce content")
				if conversionErr != nil {
					return nil, temporal.NewApplicationErrorWithCause(
						fmt.Sprintf("AI client failed to convert file: %s", errorsx.MessageOrErr(conversionErr)),
						processContentActivityError,
						conversionErr,
					)
				}
				return nil, temporal.NewApplicationErrorWithCause(
					"AI client failed to convert file: conversion produced empty content",
					processContentActivityError,
					fmt.Errorf("empty conversion result"),
				)
			}
		} // End of Gemini route
	}

	result.Content = contentInMarkdown
	result.Length = length
	result.PositionData = positionData
	result.UsageMetadata = usageMetadata
	result.ConvertedType = param.FileType // Already converted at workflow level

	logger.Info("Content in Markdown conversion completed", zap.Int("contentInMarkdownLength", len(contentInMarkdown)))

	// Phase 3: Save converted file to DB and MinIO
	// All file types now follow the same 3-step SAGA: create record → upload → update destination
	convertedFileUID, _ := uuid.NewV4()

	// Step 1: Create DB record with placeholder
	_, err = w.CreateConvertedFileRecordActivity(ctx, &CreateConvertedFileRecordActivityParam{
		KBUID:            param.KBUID,
		FileUID:          param.FileUID,
		ConvertedFileUID: convertedFileUID,
		ConvertedType:    artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_CONTENT,
		Destination:      fmt.Sprintf("placeholder-pending-upload-%s", convertedFileUID.String()),
		PositionData:     positionData,
	})
	if err != nil {
		logger.Error("Failed to create converted file record", zap.Error(err))
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to create converted file record: %s", errorsx.MessageOrErr(err)),
			processContentActivityError,
			err,
		)
	}

	// Step 2: Upload to MinIO
	uploadResult, err := w.UploadConvertedFileToMinIOActivity(ctx, &UploadConvertedFileToMinIOActivityParam{
		KBUID:            param.KBUID,
		FileUID:          param.FileUID,
		ConvertedFileUID: convertedFileUID,
		Content:          contentInMarkdown,
	})
	if err != nil {
		// Compensating transaction: delete DB record
		logger.Warn("MinIO upload failed, deleting DB record", zap.Error(err))
		_ = w.DeleteConvertedFileRecordActivity(ctx, &DeleteConvertedFileRecordActivityParam{
			ConvertedFileUID: convertedFileUID,
		})
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to upload converted file to MinIO: %s", errorsx.MessageOrErr(err)),
			processContentActivityError,
			err,
		)
	}

	// Step 3: Update DB record with actual destination
	if err := w.UpdateConvertedFileDestinationActivity(ctx, &UpdateConvertedFileDestinationActivityParam{
		ConvertedFileUID: convertedFileUID,
		Destination:      uploadResult.Destination,
	}); err != nil {
		// Compensating transactions: delete MinIO file and DB record
		logger.Warn("Failed to update destination, cleaning up", zap.Error(err))
		_ = w.DeleteConvertedFileFromMinIOActivity(ctx, &DeleteConvertedFileFromMinIOActivityParam{
			Bucket:      config.Config.Minio.BucketName,
			Destination: uploadResult.Destination,
		})
		_ = w.DeleteConvertedFileRecordActivity(ctx, &DeleteConvertedFileRecordActivityParam{
			ConvertedFileUID: convertedFileUID,
		})
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to update converted file destination: %s", errorsx.MessageOrErr(err)),
			processContentActivityError,
			err,
		)
	}

	logger.Info("Converted file saved successfully", zap.String("convertedFileUID", convertedFileUID.String()))

	result.ConvertedFileUID = convertedFileUID
	return result, nil
}

// ProcessSummaryActivityParam defines input for ProcessSummaryActivity
type ProcessSummaryActivityParam struct {
	FileUID         types.FileUIDType    // File unique identifier
	KBUID           types.KBUIDType      // Knowledge base unique identifier
	Bucket          string               // MinIO bucket
	Destination     string               // MinIO path
	Filename        string               // File name
	FileType        artifactpb.File_Type // File type
	Metadata        *structpb.Struct     // Request metadata
	CacheName       string               // Optional: AI cache name (used for Gemini route to reuse cached file context; empty for OpenAI route which doesn't support caching)
	ContentMarkdown string               // Optional: Pre-processed markdown content (used for OpenAI route where summary depends on content generation output)
}

// ProcessSummaryActivityResult defines output from ProcessSummaryActivity
type ProcessSummaryActivityResult struct {
	Summary          string                     // Generated summary content
	Length           []uint32                   // Length information (summary length)
	PositionData     *types.PositionData        // Position data (single page with delimiter at end)
	OriginalType     artifactpb.File_Type       // Original file type
	ConvertedType    artifactpb.File_Type       // Converted file type (same as original)
	UsageMetadata    any                        // AI token usage
	ConvertedFileUID types.ConvertedFileUIDType // Summary converted file UID (zero if no summary)
	Pipeline         string                     // Pipeline used (e.g., "instill-ai/indexing-generate-summary/v1.0.0" for OpenAI route, empty for AI client)
}

// ProcessSummaryActivity handles the entire summary generation:
// 1. Generate summary using AI (with or without cache)
// 2. Save summary to database
// 3. Create converted_file record for summary (if summary was generated)
func (w *Worker) ProcessSummaryActivity(ctx context.Context, param *ProcessSummaryActivityParam) (*ProcessSummaryActivityResult, error) {
	logger := w.log.With(
		zap.String("fileUID", param.FileUID.String()),
		zap.Bool("hasCache", param.CacheName != ""))

	logger.Info("ProcessSummaryActivity started")

	result := &ProcessSummaryActivityResult{}

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = CreateAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			logger.Error("Failed to create authenticated context", zap.Error(err))
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to create authenticated context: %s", errorsx.MessageOrErr(err)),
				processSummaryActivityError,
				err,
			)
		}
	}

	// Phase 1: Prepare content for summary generation
	var content []byte
	var contentStr string

	if param.ContentMarkdown != "" {
		// OpenAI route: Use pre-processed markdown content from content generation
		// This is necessary because OpenAI summary pipeline expects markdown input, not raw file
		logger.Info("Using pre-processed markdown content (OpenAI route)",
			zap.Int("contentLength", len(param.ContentMarkdown)))
		contentStr = param.ContentMarkdown
	} else {
		// Gemini route: Read raw file content and process independently
		// Fetch content from MinIO
		var err error
		content, err = w.repository.GetMinIOStorage().GetFile(authCtx, param.Bucket, param.Destination)
		if err != nil {
			// Check if a summary file already exists (reprocessing scenario)
			existingConverted, checkErr := w.repository.GetConvertedFileByFileUIDAndType(ctx, param.FileUID, artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_SUMMARY)
			if checkErr == nil && existingConverted != nil {
				// Original file is missing but summary exists - skip reprocessing
				logger.Warn("Original file not found in storage, but summary exists - skipping reprocessing",
					zap.String("bucket", param.Bucket),
					zap.String("destination", param.Destination),
					zap.String("existingSummaryUID", existingConverted.UID.String()),
					zap.Error(err))
				// Return success with existing summary metadata
				return &ProcessSummaryActivityResult{
					OriginalType:     param.FileType,
					ConvertedType:    param.FileType,
					ConvertedFileUID: existingConverted.UID,
					Summary:          "(using existing summary)",
				}, nil
			}

			// No existing summary - this is a real error
			logger.Error("Failed to retrieve file from storage", zap.Error(err))
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to retrieve file from storage: %s", errorsx.MessageOrErr(err)),
				processSummaryActivityError,
				err,
			)
		}

		logger.Info("Content retrieved from storage (Gemini route)",
			zap.Int("contentBytes", len(content)))

		// Convert bytes to valid UTF-8 string, replacing any invalid sequences
		// This prevents gRPC marshaling errors when content contains invalid UTF-8
		contentStr = strings.ToValidUTF8(string(content), "�")
	}

	// Verify content is not empty
	if len(strings.TrimSpace(contentStr)) == 0 {
		return nil, temporal.NewApplicationError(
			fmt.Sprintf("content is empty for file %s", param.FileUID.String()),
			processSummaryActivityError,
		)
	}

	// Fetch KB with config to check embedding model family for routing
	kb, err := w.repository.GetKnowledgeBaseByUIDWithConfig(authCtx, param.KBUID)
	if err != nil {
		logger.Error("Failed to get KB for routing", zap.Error(err))
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to get KB: %s", errorsx.MessageOrErr(err)),
			processSummaryActivityError, err)
	}

	var summary string
	var usageMetadata any

	// Route based on model family
	if kb.SystemConfig.RAG.Embedding.ModelFamily == "openai" {
		logger.Info("Using OpenAI pipeline route for summary generation")

		if w.pipelineClient == nil {
			return nil, temporal.NewApplicationErrorWithCause(
				"Pipeline client not configured for OpenAI route",
				processSummaryActivityError, fmt.Errorf("pipeline client is nil"))
		}

		// Call OpenAI summary pipeline with sanitized UTF-8 content
		summary, err = pipeline.GenerateSummaryPipe(authCtx, w.pipelineClient, contentStr, param.FileType)
		if err != nil {
			logger.Error("OpenAI pipeline summarization failed", zap.Error(err))
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("OpenAI pipeline failed: %s", errorsx.MessageOrErr(err)),
				processSummaryActivityError, err)
		}

		// Record pipeline used (e.g., "instill-ai/indexing-generate-summary@v1.0.0")
		result.Pipeline = pipeline.GenerateSummaryPipeline.Name()

		logger.Info("OpenAI pipeline summarization successful", zap.Int("summaryLength", len(summary)))

	} else {
		logger.Info("Using Gemini AI client route for summary generation")

		// Check if AI client is configured
		if w.aiClient == nil {
			logger.Error("AI client not configured")
			return nil, temporal.NewApplicationErrorWithCause(
				"AI client is required for summary generation. Please configure Gemini API key in the server configuration.",
				processSummaryActivityError,
				fmt.Errorf("AI client not configured"),
			)
		}

		var summarizationErr error

		// Build prompt with filename context
		summaryPrompt := gemini.GetGenerateSummaryPrompt()
		if param.Filename != "" {
			summaryPrompt = strings.ReplaceAll(gemini.GetGenerateSummaryPrompt(), "[filename]", param.Filename)
		}

		// Use the file type from parameter (already converted in workflow)
		fileType := param.FileType

		// Try AI client with cache first (if available)
		if param.CacheName != "" && w.aiClient.SupportsFileType(fileType) {
			logger.Info("Attempting AI summarization with cache",
				zap.String("cacheName", param.CacheName),
				zap.String("client", w.aiClient.Name()))

			conversion, err := w.aiClient.ConvertToMarkdownWithCache(authCtx, param.CacheName, summaryPrompt)
			if err != nil {
				logger.Warn("Cached AI summarization failed, will try without cache", zap.Error(err))
				summarizationErr = err
			} else {
				summary = conversion.Markdown
				usageMetadata = conversion.UsageMetadata
				logger.Info("AI summarization with cache succeeded",
					zap.Int("summaryLength", len(summary)))
			}
		}

		// Try AI client without cache if cached attempt failed or wasn't available
		if summary == "" && w.aiClient.SupportsFileType(fileType) {
			logger.Info("Attempting AI summarization without cache",
				zap.String("fileType", fileType.String()),
				zap.String("client", w.aiClient.Name()))

			conversion, err := w.aiClient.ConvertToMarkdownWithoutCache(
				authCtx,
				content,
				fileType,
				param.Filename,
				summaryPrompt,
			)
			if err != nil {
				logger.Error("AI summarization failed", zap.Error(err))
				summarizationErr = err
			} else {
				summary = conversion.Markdown
				usageMetadata = conversion.UsageMetadata
				logger.Info("AI summarization without cache succeeded",
					zap.Int("summaryLength", len(summary)))
			}
		}

		// If summarization failed, return the actual error
		if summary == "" {
			logger.Error("AI summarization failed to produce content")
			if summarizationErr != nil {
				return nil, temporal.NewApplicationErrorWithCause(
					fmt.Sprintf("AI client failed to generate summary: %s", errorsx.MessageOrErr(summarizationErr)),
					processSummaryActivityError,
					summarizationErr,
				)
			}
			return nil, temporal.NewApplicationErrorWithCause(
				"AI client failed to generate summary: summarization produced empty content",
				processSummaryActivityError,
				fmt.Errorf("empty summarization result"),
			)
		}
	} // End of Gemini route

	// Set result fields (symmetric with ProcessContentActivityResult)
	summaryLength := uint32(len([]rune(summary))) // Use rune count for consistency with content
	result.Summary = summary
	result.Length = []uint32{summaryLength}
	result.UsageMetadata = usageMetadata
	result.OriginalType = param.FileType  // File type being summarized
	result.ConvertedType = param.FileType // Same as original (no conversion)

	// Create PositionData: single page with delimiter at end of summary
	if len(summary) > 0 {
		result.PositionData = &types.PositionData{
			PageDelimiters: []uint32{summaryLength}, // Single page, delimiter at end
		}
	}

	logger.Info("Summary generated successfully",
		zap.Int("summaryLength", len(summary)))

	// Phase 2: Create converted_file record for summary (same pattern as content)
	if len(summary) > 0 {
		logger.Info("Creating converted_file record for summary")

		// Generate UUID for summary converted file
		convertedFileUID, _ := uuid.NewV4()

		// Step 1: Create DB record with placeholder destination (must be unique due to idx_unique_destination constraint)
		_, err = w.CreateConvertedFileRecordActivity(ctx, &CreateConvertedFileRecordActivityParam{
			KBUID:            param.KBUID,
			FileUID:          param.FileUID,
			ConvertedFileUID: convertedFileUID,
			ConvertedType:    artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_SUMMARY,
			Destination:      fmt.Sprintf("placeholder-pending-upload-%s", convertedFileUID.String()),
			PositionData:     result.PositionData,
		})
		if err != nil {
			logger.Error("Failed to create summary converted file record", zap.Error(err))
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to create summary converted file record: %s", errorsx.MessageOrErr(err)),
				processSummaryActivityError,
				err,
			)
		}

		// Step 2: Upload to MinIO (same path pattern as content)
		uploadResult, err := w.UploadConvertedFileToMinIOActivity(ctx, &UploadConvertedFileToMinIOActivityParam{
			KBUID:            param.KBUID,
			FileUID:          param.FileUID,
			ConvertedFileUID: convertedFileUID,
			Content:          summary,
		})
		if err != nil {
			// Compensating transaction: delete DB record
			logger.Warn("MinIO upload failed, deleting DB record", zap.Error(err))
			_ = w.DeleteConvertedFileRecordActivity(ctx, &DeleteConvertedFileRecordActivityParam{
				ConvertedFileUID: convertedFileUID,
			})
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to upload summary to MinIO: %s", errorsx.MessageOrErr(err)),
				processSummaryActivityError,
				err,
			)
		}

		// Step 3: Update DB record with actual destination
		if err := w.UpdateConvertedFileDestinationActivity(ctx, &UpdateConvertedFileDestinationActivityParam{
			ConvertedFileUID: convertedFileUID,
			Destination:      uploadResult.Destination,
		}); err != nil {
			// Compensating transactions: delete MinIO file and DB record
			logger.Warn("Failed to update destination, cleaning up", zap.Error(err))
			_ = w.DeleteConvertedFileFromMinIOActivity(ctx, &DeleteConvertedFileFromMinIOActivityParam{
				Bucket:      config.Config.Minio.BucketName,
				Destination: uploadResult.Destination,
			})
			_ = w.DeleteConvertedFileRecordActivity(ctx, &DeleteConvertedFileRecordActivityParam{
				ConvertedFileUID: convertedFileUID,
			})
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to update summary converted file destination: %s", errorsx.MessageOrErr(err)),
				processSummaryActivityError,
				err,
			)
		}

		logger.Info("Summary converted file created successfully",
			zap.String("summaryConvertedFileUID", convertedFileUID.String()),
			zap.String("destination", uploadResult.Destination))

		result.ConvertedFileUID = convertedFileUID
	}

	return result, nil
}

// DEPRECATED: SavePDFAsConvertedFileActivity
// This activity has been merged into StandardizeFileTypeActivity for better efficiency:
// - PDFs (converted or original) are now saved directly to converted-file folder during standardization
// - No intermediate tmp/ storage needed for PDFs
// - This refactoring fixes the dual-processing bug where original blobs were being deleted
//   before rollback KB could process them.

// DeleteTemporaryConvertedFileActivityParam defines the parameters for DeleteTemporaryConvertedFileActivity
type DeleteTemporaryConvertedFileActivityParam struct {
	Bucket      string // MinIO bucket (usually blob)
	Destination string // MinIO path to temporary converted file
}

// ===== MARKDOWN CONVERSION HELPERS =====

// ExtractPageDelimiters parses [Page: X] tags from AI-generated markdown and extracts page delimiter positions
// Returns markdown WITH page tags preserved, position data for visual grounding, and length array
// (Exported for EE worker overrides)
func ExtractPageDelimiters(markdown string, logger *zap.Logger, logContext map[string]any) (string, *types.PositionData, []uint32) {
	// Parse pages from AI-generated Markdown
	// Expected format: [Page: 1] ... [Page: n] ...
	// The page tags are KEPT in the stored markdown for robust extraction
	// Position data is calculated for visual grounding (mapping chunks to pages)
	markdownWithPageTags, pages, positionData := parseMarkdownPages(markdown)

	pageCount := len(pages)

	// Log if multi-page document detected
	if pageCount > 1 {
		logFields := []zap.Field{
			zap.Int("pageCount", pageCount),
		}
		for k, v := range logContext {
			logFields = append(logFields, zap.Any(k, v))
		}
		logger.Info("AI conversion: Multi-page document detected with [Page: X] tags", logFields...)
	}

	// Calculate length: page count for multi-page, character count for single-page
	var length []uint32
	if pageCount > 1 {
		length = []uint32{uint32(pageCount)} // Number of pages
	} else {
		length = []uint32{uint32(len(markdownWithPageTags))} // Character length
	}

	return markdownWithPageTags, positionData, length
}

// ============================================================================
// FindTargetFileByNameActivity - used for dual-processing coordination
// ============================================================================

// FindTargetFileByNameActivityParam contains parameters for finding a file by name in a target KB
type FindTargetFileByNameActivityParam struct {
	TargetKBUID    types.KBUIDType // Target KB UID to search in
	TargetOwnerUID string          // Target KB owner UID
	Filename       string          // File name to search for
}

// FindTargetFileByNameActivityResult contains the found file information
type FindTargetFileByNameActivityResult struct {
	Found   bool              // Whether the file was found
	FileUID types.FileUIDType // File UID if found
}

const findTargetFileByNameActivityError = "findTargetFileByNameActivityError"

// FindTargetFileByNameActivity finds a file by name in the target KB.
// Used during dual-processing coordination to locate target files that correspond to production files.
// This is a simple wrapper around ListKnowledgeBaseFiles repository method with post-filtering by name.
func (w *Worker) FindTargetFileByNameActivity(ctx context.Context, param *FindTargetFileByNameActivityParam) (*FindTargetFileByNameActivityResult, error) {
	w.log.Info("FindTargetFileByNameActivity: Searching for file",
		zap.String("targetKBUID", param.TargetKBUID.String()),
		zap.String("filename", param.Filename))

	// List all files in the target KB and filter by name
	// Note: We don't paginate since dual-processing typically involves a small number of files
	files, err := w.repository.ListKnowledgeBaseFiles(ctx, repository.KnowledgeBaseFileListParams{
		OwnerUID: param.TargetOwnerUID,
		KBUID:    param.TargetKBUID.String(),
		PageSize: 1000, // Large enough to cover typical KB update scenarios
	})
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to list files: %s", errorsx.MessageOrErr(err)),
			findTargetFileByNameActivityError,
			err,
		)
	}

	result := &FindTargetFileByNameActivityResult{
		Found: false,
	}

	// Find the file with matching name
	for _, file := range files.Files {
		if file.Filename == param.Filename {
			result.Found = true
			result.FileUID = file.UID
			w.log.Info("FindTargetFileByNameActivity: File found",
				zap.String("fileUID", result.FileUID.String()))
			break
		}
	}

	if !result.Found {
		w.log.Info("FindTargetFileByNameActivity: File not found in target KB")
	}

	return result, nil
}
