package worker

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gofrs/uuid"
	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"
	"gorm.io/gorm"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/internal/ai"
	"github.com/instill-ai/artifact-backend/internal/ai/gemini"
	"github.com/instill-ai/artifact-backend/pkg/pipeline"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
	errorsx "github.com/instill-ai/x/errors"
)

// This file contains file processing activities used by ProcessFileWorkflow and its child workflows:
//
// Main Workflow Activities:
// - GetFileMetadataActivity - Retrieves file information and pipeline configuration
// - CleanupOldConvertedFileActivity - Removes old converted files before creating new ones
// - CreateConvertedFileRecordActivity - Creates DB record for converted markdown file
// - UploadConvertedFileToMinIOActivity - Uploads converted content to MinIO
// - UpdateConvertedFileDestinationActivity - Updates DB with actual MinIO destination
// - DeleteConvertedFileRecordActivity - Cleanup activity for DB record
// - DeleteConvertedFileFromMinIOActivity - Cleanup activity for MinIO file
// - UpdateConversionMetadataActivity - Updates file metadata after conversion
// - GetConvertedFileForChunkingActivity - Retrieves converted file for chunking
// - ChunkContentActivity - Splits document content into manageable text chunks
// - SaveTextChunksToDBActivity - Persists text chunks and their metadata to database
// - UpdateChunkingMetadataActivity - Updates text chunking statistics and pipeline info
//
// Child Workflow Activities (ProcessContentWorkflow):
// - ConvertFileTypeActivity - Converts file formats (GIF→PNG, MKV→MP4, DOC→PDF, etc.)
// - CacheContextActivity - Creates AI cache for efficient processing
// - DeleteCacheActivity - Cleans up AI cache
// - DeleteTemporaryConvertedFileActivity - Cleans up temporary converted files from MinIO
// - ConvertToFileActivity - Converts files to Markdown using AI or pipelines
//
// Child Workflow Activities (ProcessSummaryWorkflow):
// - GenerateSummaryActivity - Generates document summaries using AI
// - SaveSummaryActivity - Saves summary to PostgreSQL database

// extractPageReferences extracts the location of a text chunk (defined by its start
// and end byte positions) in a document (defined by the byte delimiters of its
// pages). The function handles edge cases where text chunk boundaries are exactly at
// page delimiters or extend beyond the last page.
func extractPageReferences(textChunkStart, textChunkEnd uint32, pageDelimiters []uint32) (pageStart, pageEnd uint32) {
	if len(pageDelimiters) == 0 {
		return 0, 0
	}

	// delimiter is the first byte of the next page.
	for i, delimiter := range pageDelimiters {
		if textChunkStart < delimiter && pageStart == 0 {
			pageStart = uint32(i + 1)
		}

		// Use <= to handle chunks that end exactly at a page delimiter
		if textChunkEnd <= delimiter && pageEnd == 0 {
			pageEnd = uint32(i + 1)
		}

		if pageStart != 0 && pageEnd != 0 {
			break
		}
	}

	// Handle edge case: if textChunkEnd extends beyond all delimiters,
	// it belongs to the last page
	if pageEnd == 0 {
		pageEnd = uint32(len(pageDelimiters))
	}

	// Handle edge case: if textChunkStart is beyond all delimiters,
	// it belongs to the last page (shouldn't happen in normal cases)
	if pageStart == 0 {
		pageStart = uint32(len(pageDelimiters))
	}

	return
}

// sanitizeUTF8 ensures the content is valid UTF-8 by replacing invalid sequences
// with the Unicode replacement character (�). This prevents gRPC marshaling errors
// when content contains invalid UTF-8 sequences (common with certain PDF conversions).
func sanitizeUTF8(content []byte) string {
	// Fast path: if already valid UTF-8, return as-is
	if utf8.Valid(content) {
		return string(content)
	}

	// Slow path: replace invalid UTF-8 sequences
	// The � character (U+FFFD) is the standard replacement for invalid UTF-8
	return strings.ToValidUTF8(string(content), "�")
}

// ===== METADATA & CONTENT ACTIVITIES =====

// GetFileMetadataActivityParam for retrieving file and KB metadata
type GetFileMetadataActivityParam struct {
	FileUID types.FileUIDType // File unique identifier
	KBUID   types.KBUIDType   // Knowledge base unique identifier
}

// GetFileMetadataActivityResult contains file and KB configuration
type GetFileMetadataActivityResult struct {
	File                 *repository.KnowledgeBaseFileModel // File metadata from database
	ConvertingPipelines  []pipeline.PipelineRelease         // Pipelines for file conversion
	SummarizingPipelines []pipeline.PipelineRelease         // Pipelines for summarization
	ChunkingPipelines    []pipeline.PipelineRelease         // Pipelines for chunking
	EmbeddingPipelines   []pipeline.PipelineRelease         // Pipelines for embedding generation
	ExternalMetadata     *structpb.Struct                   // External metadata from request
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
		// File was deleted during processing - this is OK in scenarios like:
		// - Catalog/file deletion triggered while workflow is running
		// - Test cleanup happening concurrently with processing
		// Return a non-retryable error to exit the workflow gracefully
		w.log.Info("GetFileMetadataActivity: File not found (may have been deleted during processing)",
			zap.String("fileUID", param.FileUID.String()))
		err := fmt.Errorf("file not found: %s", param.FileUID.String())
		return nil, temporal.NewApplicationErrorWithCause(
			"File not found",
			getFileMetadataActivityError,
			err,
		)
	}
	file := files[0]

	// Get knowledge base configuration
	kb, err := w.repository.GetKnowledgeBaseByUID(ctx, param.KBUID)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			getFileMetadataActivityError,
			err,
		)
	}

	// Build converting pipelines list
	convertingPipelines := make([]pipeline.PipelineRelease, 0, len(kb.ConvertingPipelines)+1)

	// Add file-specific pipeline if exists
	if file.ExtraMetaDataUnmarshal != nil && file.ExtraMetaDataUnmarshal.ConvertingPipe != "" {
		pipeline, err := pipeline.PipelineReleaseFromName(file.ExtraMetaDataUnmarshal.ConvertingPipe)
		if err != nil {
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Invalid file pipeline: %s", errorsx.MessageOrErr(err)),
				getFileMetadataActivityError,
				err,
			)
		}
		convertingPipelines = append(convertingPipelines, pipeline)
	}

	// Add catalog pipelines
	for _, pipelineName := range kb.ConvertingPipelines {
		if len(pipelineName) == 0 {
			continue
		}
		// Skip if already added as file pipeline
		if file.ExtraMetaDataUnmarshal != nil && pipelineName == file.ExtraMetaDataUnmarshal.ConvertingPipe {
			continue
		}
		pipeline, err := pipeline.PipelineReleaseFromName(pipelineName)
		if err != nil {
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Invalid catalog pipeline: %s", errorsx.MessageOrErr(err)),
				getFileMetadataActivityError,
				err,
			)
		}
		convertingPipelines = append(convertingPipelines, pipeline)
	}

	// If no converting pipelines configured (neither file-specific nor catalog-level),
	// fall back to default markdown conversion pipelines for backward compatibility
	if len(convertingPipelines) == 0 {
		convertingPipelines = pipeline.DefaultConversionPipelines
	}

	return &GetFileMetadataActivityResult{
		File:                &file,
		ConvertingPipelines: convertingPipelines,
		ExternalMetadata:    file.ExternalMetadataUnmarshal,
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
		authCtx, err = createAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to create authenticated context: %s", errorsx.MessageOrErr(err)),
				getFileContentActivityError,
				err,
			)
		}
	}

	content, err := w.repository.GetFile(authCtx, param.Bucket, param.Destination)
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

// CleanupOldConvertedFileActivityParam for removing old converted files
type CleanupOldConvertedFileActivityParam struct {
	FileUID types.FileUIDType // File unique identifier to clean up old conversions for
}

// CreateConvertedFileRecordActivityParam for creating a converted file DB record
type CreateConvertedFileRecordActivityParam struct {
	KBUID            types.KBUIDType          // Knowledge base unique identifier
	FileUID          types.FileUIDType        // Original file unique identifier
	ConvertedFileUID types.FileUIDType        // Converted file unique identifier
	FileName         string                   // Converted file name
	Destination      string                   // MinIO destination path
	PositionData     *repository.PositionData // Position data from conversion (e.g., page mappings)
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
	FileUID  types.FileUIDType // File unique identifier
	Length   []uint32          // Length of markdown sections
	Pipeline string            // Pipeline used for conversion (empty if AI was used)
}

// CleanupOldConvertedFileActivity removes old converted file from MinIO if it exists
// This is a single MinIO delete operation - idempotent (delete is naturally idempotent)
func (w *Worker) CleanupOldConvertedFileActivity(ctx context.Context, param *CleanupOldConvertedFileActivityParam) error {
	w.log.Info("CleanupOldConvertedFileActivity: Checking for old converted file",
		zap.String("fileUID", param.FileUID.String()))

	// Check if old converted file exists
	oldConvertedFile, err := w.repository.GetConvertedFileByFileUID(ctx, param.FileUID)
	if err != nil || oldConvertedFile == nil || oldConvertedFile.Destination == "" {
		// No old file to clean up - this is fine
		return nil
	}

	w.log.Info("CleanupOldConvertedFileActivity: Deleting old converted file",
		zap.String("oldConvertedFileUID", oldConvertedFile.UID.String()),
		zap.String("destination", oldConvertedFile.Destination))

	// Delete from MinIO
	// Note: MinIO DeleteFile is idempotent - if file doesn't exist, it succeeds.
	// Any error returned here is a real error (network, permissions, etc.) that should be retried.
	err = w.repository.DeleteFile(ctx, config.Config.Minio.BucketName, oldConvertedFile.Destination)
	if err != nil {
		w.log.Error("CleanupOldConvertedFileActivity: Failed to delete old converted file from MinIO",
			zap.String("destination", oldConvertedFile.Destination),
			zap.Error(err))
		return temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to delete old converted file: %s", errorsx.MessageOrErr(err)),
			cleanupOldConvertedFileActivityError,
			err,
		)
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
		UID:          param.ConvertedFileUID,
		KBUID:        param.KBUID,
		FileUID:      param.FileUID,
		Name:         param.FileName,
		Type:         "text/markdown",
		Destination:  param.Destination,
		PositionData: param.PositionData,
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
	destination, err := blobStorage.SaveConvertedFile(
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
	err := blobStorage.DeleteFile(ctx, param.Bucket, param.Destination)
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
		zap.String("fileUID", param.FileUID.String()))

	mdUpdate := repository.ExtraMetaData{
		Length:         param.Length,
		ConvertingPipe: param.Pipeline,
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

// ===== CHUNKING ACTIVITIES =====

// GetConvertedFileForChunkingActivityParam retrieves converted file for text chunking
type GetConvertedFileForChunkingActivityParam struct {
	FileUID types.FileUIDType // File unique identifier
	KBUID   types.KBUIDType   // Knowledge base unique identifier
}

// GetConvertedFileForChunkingActivityResult contains file metadata and content
type GetConvertedFileForChunkingActivityResult struct {
	Content            string                     // Converted markdown content
	ChunkingPipelines  []pipeline.PipelineRelease // Pipelines for text chunking
	EmbeddingPipelines []pipeline.PipelineRelease // Pipelines for embedding generation
	ExternalMetadata   *structpb.Struct           // External metadata from request
}

// ChunkContentActivityParam for external text chunking pipeline call
type ChunkContentActivityParam struct {
	FileUID   types.FileUIDType          // File unique identifier
	KBUID     types.KBUIDType            // Knowledge base unique identifier
	Content   string                     // Content to chunk into text chunks
	Pipelines []pipeline.PipelineRelease // Pipelines for text chunking
	Metadata  *structpb.Struct           // Request metadata for authentication
}

// ChunkContentActivityResult contains chunked content
type ChunkContentActivityResult struct {
	Chunks          []pipeline.TextChunk     // Generated text chunks
	PipelineRelease pipeline.PipelineRelease // Pipeline used for text chunking
}

// SaveTextChunksToDBActivityParam for saving text chunks to database
type SaveTextChunksToDBActivityParam struct {
	FileUID    types.FileUIDType    // File unique identifier
	KBUID      types.KBUIDType      // Knowledge base unique identifier
	TextChunks []pipeline.TextChunk // Text chunks to save
}

// SaveChunksToDBActivityResult contains saved text chunk UIDs from SaveTextChunksToDBActivity
type SaveChunksToDBActivityResult struct {
	TextChunkUIDs []types.TextChunkUIDType // Saved text chunk unique identifiers
}

// UpdateChunkingMetadataActivityParam for updating metadata after text chunking
type UpdateChunkingMetadataActivityParam struct {
	FileUID          types.FileUIDType // File unique identifier
	KBUID            types.KBUIDType   // Knowledge base unique identifier
	ChunkingPipeline string            // Pipeline used for text chunking
	TextChunkCount   uint32            // Number of text chunks created
}

// GetConvertedFileForChunkingActivity retrieves converted file content for text chunking
func (w *Worker) GetConvertedFileForChunkingActivity(ctx context.Context, param *GetConvertedFileForChunkingActivityParam) (*GetConvertedFileForChunkingActivityResult, error) {
	w.log.Info("GetConvertedFileForChunkingActivity: Fetching converted file",
		zap.String("fileUID", param.FileUID.String()))

	// Get converted file from DB
	convertedFile, err := w.repository.GetConvertedFileByFileUID(ctx, param.FileUID)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to get converted file: %s", errorsx.MessageOrErr(err)),
			getConvertedFileForChunkingActivityError,
			err,
		)
	}

	// Get content from MinIO
	bucket := repository.BucketFromDestination(convertedFile.Destination)
	content, err := w.repository.GetFile(ctx, bucket, convertedFile.Destination)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to get content from storage: %s", errorsx.MessageOrErr(err)),
			getConvertedFileForChunkingActivityError,
			err,
		)
	}

	// Get file for external metadata
	file, err := getFileByUID(ctx, w.repository, param.FileUID)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to get file metadata: %s", errorsx.MessageOrErr(err)),
			getConvertedFileForChunkingActivityError,
			err,
		)
	}

	// TODO: Build chunking and embedding pipelines from KB configuration
	// For now, these will be passed from the workflow
	chunkingPipelines := make([]pipeline.PipelineRelease, 0)
	embeddingPipelines := make([]pipeline.PipelineRelease, 0)

	return &GetConvertedFileForChunkingActivityResult{
		Content:            string(content),
		ChunkingPipelines:  chunkingPipelines,
		EmbeddingPipelines: embeddingPipelines,
		ExternalMetadata:   file.ExternalMetadataUnmarshal,
	}, nil
}

// ChunkContentActivity calls external pipeline to chunk content into text chunks
func (w *Worker) ChunkContentActivity(ctx context.Context, param *ChunkContentActivityParam) (*ChunkContentActivityResult, error) {
	w.log.Info("ChunkContentActivity: Chunking content into text chunks",
		zap.String("fileUID", param.FileUID.String()))

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = createAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to create authenticated context: %s", errorsx.MessageOrErr(err)),
				chunkContentActivityError,
				err,
			)
		}
	}

	// Call the chunking pipeline
	chunkingResult, err := pipeline.ChunkMarkdownPipe(authCtx, w.pipelineClient, param.Content)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Content chunking failed: %s", errorsx.MessageOrErr(err)),
			chunkContentActivityError,
			err,
		)
	}

	w.log.Info("ChunkContentActivity: Chunking successful",
		zap.Int("chunkCount", len(chunkingResult.Chunks)))

	return &ChunkContentActivityResult{
		Chunks:          chunkingResult.Chunks,
		PipelineRelease: chunkingResult.PipelineRelease,
	}, nil
}

// SaveTextChunksToDBActivity saves text chunks to database
func (w *Worker) SaveTextChunksToDBActivity(ctx context.Context, param *SaveTextChunksToDBActivityParam) (*SaveChunksToDBActivityResult, error) {
	w.log.Info("SaveChunksToDBActivity: Saving text chunks to database",
		zap.String("fileUID", param.FileUID.String()),
		zap.Int("chunkCount", len(param.TextChunks)))

	// Delete old text chunks (for reprocessing)
	err := w.deleteTextChunksByFileUIDSync(ctx, param.KBUID, param.FileUID)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to delete old text chunks: %s", errorsx.MessageOrErr(err)),
			saveTextChunksToDBActivityError,
			err,
		)
	}

	// Get converted file for source information
	convertedFile, err := w.repository.GetConvertedFileByFileUID(ctx, param.FileUID)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to get converted file: %s", errorsx.MessageOrErr(err)),
			saveTextChunksToDBActivityError,
			err,
		)
	}

	// Add page references to text chunks if position data is available
	chunksWithReferences := param.TextChunks
	if convertedFile.PositionData != nil && len(convertedFile.PositionData.PageDelimiters) > 0 {
		w.log.Info("SaveChunksToDBActivity: Adding page references to text chunks",
			zap.Int("delimiterCount", len(convertedFile.PositionData.PageDelimiters)))

		chunksWithReferences = make([]pipeline.TextChunk, len(param.TextChunks))
		copy(chunksWithReferences, param.TextChunks)

		for i, chunk := range chunksWithReferences {
			pageStart, pageEnd := extractPageReferences(
				uint32(chunk.Start),
				uint32(chunk.End),
				convertedFile.PositionData.PageDelimiters,
			)

			if pageStart != 0 && pageEnd != 0 {
				chunksWithReferences[i].Reference = &repository.TextChunkReference{
					PageRange: [2]uint32{pageStart, pageEnd},
				}
				w.log.Debug("Added page reference",
					zap.Int("chunkIndex", i),
					zap.Uint32("pageStart", pageStart),
					zap.Uint32("pageEnd", pageEnd))
			}
		}
	}

	// Save text chunks using helper function
	_, err = saveChunksToDBOnly(
		ctx,
		w.repository,
		w.repository,
		param.KBUID,
		param.FileUID,
		convertedFile.UID,
		repository.ConvertedFileTableName,
		nil, // No summary chunks
		chunksWithReferences,
		"text/markdown",
	)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to save text chunks: %s", errorsx.MessageOrErr(err)),
			saveTextChunksToDBActivityError,
			err,
		)
	}

	return &SaveChunksToDBActivityResult{
		TextChunkUIDs: nil,
	}, nil
}

// UpdateChunkingMetadataActivity updates file metadata after text chunking
func (w *Worker) UpdateChunkingMetadataActivity(ctx context.Context, param *UpdateChunkingMetadataActivityParam) error {
	w.log.Info("UpdateChunkingMetadataActivity: Updating file metadata",
		zap.String("fileUID", param.FileUID.String()))

	mdUpdate := repository.ExtraMetaData{
		ChunkingPipe: param.ChunkingPipeline,
	}

	err := w.repository.UpdateKnowledgeFileMetadata(ctx, param.FileUID, mdUpdate)
	if err != nil {
		// If file not found, it may have been deleted during processing - this is OK
		if errors.Is(err, gorm.ErrRecordNotFound) {
			w.log.Info("UpdateChunkingMetadataActivity: File not found (may have been deleted), skipping metadata update",
				zap.String("fileUID", param.FileUID.String()))
			return nil
		}
		return temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to update file metadata: %s", errorsx.MessageOrErr(err)),
			updateChunkingMetadataActivityError,
			err,
		)
	}

	return nil
}

// ===== SUMMARY ACTIVITIES =====

// GenerateSummaryActivityParam for AI-based summarization
type GenerateSummaryActivityParam struct {
	FileUID     types.FileUIDType // File unique identifier
	KBUID       types.KBUIDType   // Knowledge base unique identifier (unused, kept for consistency)
	Bucket      string            // MinIO bucket containing the file
	Destination string            // MinIO path to the file
	FileName    string            // File name for identification
	FileType    string            // File type for summarization
	Metadata    *structpb.Struct  // Request metadata for authentication
	CacheName   string            // Optional: AI cache name for efficient summary generation
}

// GenerateSummaryActivityResult contains the generated summary
type GenerateSummaryActivityResult struct {
	Summary       string // Generated summary text
	Pipeline      string // Pipeline used for summary generation (empty if AI was used)
	UsageMetadata any    // Token usage metadata from AI provider (nil if pipeline was used)
}

// SaveSummaryActivityParam saves summary to database
type SaveSummaryActivityParam struct {
	FileUID  types.FileUIDType // File unique identifier
	Summary  string            // Summary text to save
	Pipeline string            // Pipeline used for summary generation (empty if AI was used)
}

// GenerateSummaryActivity generates document summary using AI (with cache or pipeline fallback)
func (w *Worker) GenerateSummaryActivity(ctx context.Context, param *GenerateSummaryActivityParam) (*GenerateSummaryActivityResult, error) {
	w.log.Info("GenerateSummaryActivity: Generating summary",
		zap.String("fileUID", param.FileUID.String()))

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = createAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to create authenticated context: %s", errorsx.MessageOrErr(err)),
				generateSummaryActivityError,
				err,
			)
		}
	}

	// Fetch content from MinIO
	content, err := w.repository.GetFile(authCtx, param.Bucket, param.Destination)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to retrieve file from storage: %s", errorsx.MessageOrErr(err)),
			generateSummaryActivityError,
			err,
		)
	}

	// Sanitize content to ensure valid UTF-8
	// This prevents gRPC marshaling errors when content contains invalid UTF-8 sequences
	// (common with certain PDF conversions or binary data)
	contentStr := sanitizeUTF8(content)

	// Log content stats for debugging
	w.log.Info("GenerateSummaryActivity: Content prepared",
		zap.String("fileUID", param.FileUID.String()),
		zap.Int("originalBytes", len(content)),
		zap.Int("sanitizedBytes", len(contentStr)),
		zap.Int("sanitizedRunes", len([]rune(contentStr))),
		zap.Bool("isEmpty", len(strings.TrimSpace(contentStr)) == 0))

	// Verify content is not empty after sanitization
	if len(strings.TrimSpace(contentStr)) == 0 {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Content is empty after UTF-8 sanitization for file %s", param.FileUID.String()),
			generateSummaryActivityError,
			fmt.Errorf("empty content after sanitization"),
		)
	}

	var summary string

	// Build prompt with filename context by replacing [filename] placeholder
	summaryPrompt := gemini.DefaultSummaryPrompt
	if param.FileName != "" {
		summaryPrompt = strings.ReplaceAll(gemini.DefaultSummaryPrompt, "[filename]", param.FileName)
	}

	// Determine file type for AI - parse from string parameter (used by both cached and non-cached routes)
	fileType := artifactpb.FileType_FILE_TYPE_PDF // Default to PDF for summarization
	if param.FileType != "" {
		// Parse the file type string to enum value (e.g., "FILE_TYPE_TEXT" -> FileType_FILE_TYPE_TEXT)
		if enumVal, ok := artifactpb.FileType_value[param.FileType]; ok {
			fileType = artifactpb.FileType(enumVal)
		}
	}

	// Try AI provider with cache first (if available, cache is provided, and file type is supported)
	if w.aiProvider != nil && param.CacheName != "" && w.aiProvider.SupportsFileType(fileType) {
		w.log.Info("GenerateSummaryActivity: Attempting AI summarization with cache",
			zap.String("cacheName", param.CacheName),
			zap.String("fileName", param.FileName),
			zap.String("fileType", fileType.String()),
			zap.String("provider", w.aiProvider.Name()))

		conversion, err := w.aiProvider.ConvertToMarkdownWithCache(authCtx, param.CacheName, summaryPrompt)
		if err != nil {
			// Log the error but continue to try AI without cache
			w.log.Warn("GenerateSummaryActivity: Cached AI summarization failed, will try without cache",
				zap.Error(err))
		} else {
			w.log.Info("GenerateSummaryActivity: Cached AI summarization successful",
				zap.Int("summaryLength", len(conversion.Markdown)),
				zap.String("provider", conversion.Provider))

			return &GenerateSummaryActivityResult{
				Summary:       conversion.Markdown,
				Pipeline:      "", // No pipeline used (AI direct)
				UsageMetadata: conversion.UsageMetadata,
			}, nil
		}
	}

	// Try AI provider without cache (if AI provider is available and file type is supported)
	if w.aiProvider != nil && w.aiProvider.SupportsFileType(fileType) {
		w.log.Info("GenerateSummaryActivity: Attempting AI summarization without cache",
			zap.String("provider", w.aiProvider.Name()),
			zap.String("fileName", param.FileName),
			zap.String("fileType", fileType.String()))

		conversion, err := w.aiProvider.ConvertToMarkdown(authCtx, content, fileType, param.FileName, summaryPrompt)
		if err != nil {
			// Log the error but fall back to pipeline
			w.log.Warn("GenerateSummaryActivity: AI summarization failed, falling back to pipeline",
				zap.Error(err))
		} else {
			w.log.Info("GenerateSummaryActivity: AI summarization successful",
				zap.Int("summaryLength", len(conversion.Markdown)),
				zap.String("provider", conversion.Provider))

			return &GenerateSummaryActivityResult{
				Summary:       conversion.Markdown,
				Pipeline:      "", // No pipeline used (AI direct)
				UsageMetadata: conversion.UsageMetadata,
			}, nil
		}
	}

	// Fall back to pipeline-based summarization
	w.log.Info("GenerateSummaryActivity: Using pipeline for summarization")
	summary, err = pipeline.GenerateSummary(authCtx, w.pipelineClient, contentStr, param.FileType)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Summary generation failed: %s", errorsx.MessageOrErr(err)),
			generateSummaryActivityError,
			err,
		)
	}

	return &GenerateSummaryActivityResult{
		Summary:  summary,
		Pipeline: pipeline.GenerateSummaryPipeline.Name(),
	}, nil
}

// SaveSummaryActivity saves summary to database
func (w *Worker) SaveSummaryActivity(ctx context.Context, param *SaveSummaryActivityParam) error {
	w.log.Info("SaveSummaryActivity: Saving summary to database",
		zap.String("fileUID", param.FileUID.String()))

	// Update summary in KB file
	updateMap := map[string]any{
		repository.KnowledgeBaseFileColumn.Summary: param.Summary,
	}

	_, err := w.repository.UpdateKnowledgeBaseFile(ctx, param.FileUID.String(), updateMap)
	if err != nil {
		// If file not found, it may have been deleted during processing - this is OK
		if errors.Is(err, gorm.ErrRecordNotFound) {
			w.log.Info("SaveSummaryActivity: File not found (may have been deleted), skipping summary save",
				zap.String("fileUID", param.FileUID.String()))
			return nil
		}
		return temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to save summary: %s", errorsx.MessageOrErr(err)),
			saveSummaryActivityError,
			err,
		)
	}

	// Update metadata
	mdUpdate := repository.ExtraMetaData{
		SummarizingPipe: param.Pipeline,
	}
	err = w.repository.UpdateKnowledgeFileMetadata(ctx, param.FileUID, mdUpdate)
	if err != nil {
		// If file not found, it may have been deleted during processing - this is OK
		if errors.Is(err, gorm.ErrRecordNotFound) {
			w.log.Info("SaveSummaryActivity: File not found (may have been deleted), skipping metadata update",
				zap.String("fileUID", param.FileUID.String()))
			return nil
		}
		return temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to update file metadata: %s", errorsx.MessageOrErr(err)),
			saveSummaryActivityError,
			err,
		)
	}

	return nil
}

// ===== CHILD WORKFLOW ACTIVITIES =====
// Activities used by ProcessContentWorkflow and ProcessSummaryWorkflow

// ===== FILE TYPE CONVERSION ACTIVITY =====

// ConvertFileTypeActivityParam defines the parameters for ConvertFileTypeActivity
type ConvertFileTypeActivityParam struct {
	FileUID     types.FileUIDType          // File unique identifier
	KBUID       types.KBUIDType            // Knowledge base unique identifier
	Bucket      string                     // MinIO bucket containing the file
	Destination string                     // MinIO path to the file
	FileType    artifactpb.FileType        // Original file type to convert from
	Filename    string                     // Filename for identification
	Pipelines   []pipeline.PipelineRelease // convert-file-type pipeline
	Metadata    *structpb.Struct           // Request metadata for authentication
}

// ConvertFileTypeActivityResult defines the result of ConvertFileTypeActivity
type ConvertFileTypeActivityResult struct {
	ConvertedDestination string                   // MinIO path to converted file (empty if no conversion)
	ConvertedBucket      string                   // MinIO bucket for converted file (empty if no conversion)
	ConvertedType        artifactpb.FileType      // New file type after conversion
	OriginalType         artifactpb.FileType      // Original file type
	Converted            bool                     // Whether conversion was performed
	PipelineRelease      pipeline.PipelineRelease // Pipeline used for conversion
}

// ConvertFileTypeActivity converts non-AI-native file types to AI-supported formats
// Following format mappings defined in the AI component
func (w *Worker) ConvertFileTypeActivity(ctx context.Context, param *ConvertFileTypeActivityParam) (*ConvertFileTypeActivityResult, error) {
	w.log.Info("ConvertFileTypeActivity: Checking if file needs conversion",
		zap.String("fileType", param.FileType.String()),
		zap.String("filename", param.Filename))

	// Check if file needs conversion
	needsConversion, targetFormat := needsFileConversion(param.FileType)
	if !needsConversion {
		w.log.Info("ConvertFileTypeActivity: File type is AI-native, no conversion needed",
			zap.String("fileType", param.FileType.String()))
		return &ConvertFileTypeActivityResult{
			ConvertedDestination: "", // No conversion, use original file
			ConvertedBucket:      "",
			ConvertedType:        param.FileType,
			OriginalType:         param.FileType,
			Converted:            false,
		}, nil
	}

	w.log.Info("ConvertFileTypeActivity: File type needs conversion",
		zap.String("originalType", param.FileType.String()),
		zap.String("targetFormat", targetFormat))

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = createAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to create authenticated context: %s", errorsx.MessageOrErr(err)),
				convertFileTypeActivityError,
				err,
			)
		}
	}

	// Fetch original file content from MinIO
	content, err := w.repository.GetFile(authCtx, param.Bucket, param.Destination)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to retrieve file from storage: %s", errorsx.MessageOrErr(err)),
			convertFileTypeActivityError,
			err,
		)
	}

	w.log.Info("ConvertFileTypeActivity: File content retrieved from MinIO",
		zap.Int("contentSize", len(content)))

	// Determine which pipeline to use and prepare input
	var convertedContent []byte
	var usedPipeline pipeline.PipelineRelease

	// Try convert-file-type pipeline
	if len(param.Pipelines) > 0 {
		convertedContent, usedPipeline, err = w.convertUsingPipeline(authCtx, content, param.FileType, targetFormat, param.Pipelines)
		if err != nil {
			w.log.Warn("ConvertFileTypeActivity: Pipeline conversion failed",
				zap.Error(err),
				zap.String("pipeline", usedPipeline.Name()))
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to convert file: %s", errorsx.MessageOrErr(err)),
				convertFileTypeActivityError,
				err,
			)
		}
	} else {
		return nil, temporal.NewApplicationError(
			"No conversion pipeline available",
			convertFileTypeActivityError,
		)
	}

	w.log.Info("ConvertFileTypeActivity: File conversion successful",
		zap.String("originalType", param.FileType.String()),
		zap.String("convertedType", targetFormat),
		zap.Int("originalSize", len(content)),
		zap.Int("convertedSize", len(convertedContent)))

	// Map target format string to FileType enum
	convertedFileType := mapFormatToFileType(targetFormat)

	// Upload converted content to MinIO (avoid passing large blobs through Temporal)
	// Use a temporary path in the blob bucket for intermediate conversion results
	// Generate unique destination for converted file in tmp directory
	convertedDestination := fmt.Sprintf("tmp/%s/%s.%s",
		param.FileUID.String(),
		uuid.Must(uuid.NewV4()).String(),
		targetFormat)

	// Encode content to base64 for MinIO upload
	base64Content := base64.StdEncoding.EncodeToString(convertedContent)

	// Get MIME type for the converted file type
	mimeType := ai.FileTypeToMIME(convertedFileType)

	err = w.repository.UploadBase64File(authCtx, repository.BlobBucketName, convertedDestination, base64Content, mimeType)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to upload converted file to MinIO: %s", errorsx.MessageOrErr(err)),
			convertFileTypeActivityError,
			err,
		)
	}

	w.log.Info("ConvertFileTypeActivity: Converted file uploaded to MinIO",
		zap.String("destination", convertedDestination),
		zap.String("bucket", repository.BlobBucketName),
		zap.String("mimeType", mimeType))

	return &ConvertFileTypeActivityResult{
		ConvertedDestination: convertedDestination,
		ConvertedBucket:      repository.BlobBucketName,
		ConvertedType:        convertedFileType,
		OriginalType:         param.FileType,
		Converted:            true,
		PipelineRelease:      usedPipeline,
	}, nil
}

// ===== CACHE CONTEXT ACTIVITY =====

// CacheContextActivityParam defines the parameters for CacheContextActivity
type CacheContextActivityParam struct {
	FileUID     types.FileUIDType   // File unique identifier
	KBUID       types.KBUIDType     // Knowledge base unique identifier
	Bucket      string              // MinIO bucket (original or converted file)
	Destination string              // MinIO path (original or converted file)
	FileType    artifactpb.FileType // File type for content type determination
	Filename    string              // Filename for cache display name
	Metadata    *structpb.Struct    // Request metadata for authentication
}

// CacheContextActivityResult defines the result of CacheContextActivity
type CacheContextActivityResult struct {
	CacheName     string    // AI cache name
	Model         string    // Model used for cache
	CreateTime    time.Time // When cache was created
	ExpireTime    time.Time // When cache will expire
	CacheEnabled  bool      // Flag indicating if cache was created (false means caching is disabled)
	UsageMetadata any       // Token usage metadata from AI provider (nil if cache was not created)
}

// CacheContextActivity creates a cached context for the input file
// This enables efficient subsequent operations like conversion, analysis, etc.
// Supports: Documents (PDF, DOCX, DOC, PPTX, PPT), Images, Audio, Video
func (w *Worker) CacheContextActivity(ctx context.Context, param *CacheContextActivityParam) (*CacheContextActivityResult, error) {
	w.log.Info("CacheContextActivity: Creating cache for input content",
		zap.String("fileUID", param.FileUID.String()),
		zap.String("fileType", param.FileType.String()),
		zap.String("destination", param.Destination))

	// Check if AI provider is available
	if w.aiProvider == nil {
		w.log.Info("CacheContextActivity: AI provider not available, skipping cache creation")
		return &CacheContextActivityResult{
			CacheEnabled: false,
		}, nil
	}

	// Check if file type is supported for caching
	if !w.aiProvider.SupportsFileType(param.FileType) {
		w.log.Info("CacheContextActivity: File type not supported for caching",
			zap.String("fileType", param.FileType.String()))
		return &CacheContextActivityResult{
			CacheEnabled: false,
		}, nil
	}

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = createAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			w.log.Warn("CacheContextActivity: Failed to create authenticated context, proceeding without auth",
				zap.Error(err))
		}
	}

	// Fetch file content from MinIO (either original or converted file)
	content, err := w.repository.GetFile(authCtx, param.Bucket, param.Destination)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to retrieve file from storage: %s", errorsx.MessageOrErr(err)),
			cacheContextActivityError,
			err,
		)
	}

	w.log.Info("CacheContextActivity: File content retrieved from MinIO",
		zap.String("bucket", param.Bucket),
		zap.String("destination", param.Destination),
		zap.Int("contentSize", len(content)))

	// Set cache TTL (5 minutes default)
	cacheTTL := 5 * time.Minute

	// Create the cache using AI provider
	// Note: Cache creation is optional - if it fails, we continue without cache
	cacheOutput, err := w.aiProvider.CreateCache(authCtx, content, param.FileType, param.Filename, cacheTTL)
	if err != nil {
		// Log the error but don't fail the activity
		// Common reasons for cache failure:
		// - Content too small (< 1024 tokens minimum)
		// - Content too large (> maximum cache size)
		// - API quota exceeded
		// - Network issues
		w.log.Warn("CacheContextActivity: Cache creation failed, continuing without cache",
			zap.Error(err),
			zap.String("fileUID", param.FileUID.String()),
			zap.String("fileType", param.FileType.String()))

		return &CacheContextActivityResult{
			CacheEnabled: false,
		}, nil
	}

	return &CacheContextActivityResult{
		CacheName:     cacheOutput.CacheName,
		Model:         cacheOutput.Model,
		CreateTime:    cacheOutput.CreateTime,
		ExpireTime:    cacheOutput.ExpireTime,
		CacheEnabled:  true,
		UsageMetadata: cacheOutput.UsageMetadata,
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

	// Check if AI provider is available
	if w.aiProvider == nil {
		w.log.Warn("DeleteCacheActivity: AI provider not available, cannot delete cache")
		return nil
	}

	err := w.aiProvider.DeleteCache(ctx, param.CacheName)
	if err != nil {
		// Log error but don't fail the activity - cache will expire automatically
		w.log.Warn("DeleteCacheActivity: Failed to delete cache (will expire automatically)",
			zap.String("cacheName", param.CacheName),
			zap.Error(err))
		return nil
	}

	return nil
}

// DeleteTemporaryConvertedFileActivityParam defines the parameters for DeleteTemporaryConvertedFileActivity
type DeleteTemporaryConvertedFileActivityParam struct {
	Bucket      string // MinIO bucket (usually blob)
	Destination string // MinIO path to temporary converted file
}

// DeleteTemporaryConvertedFileActivity deletes a temporary converted file from MinIO
// This cleans up intermediate conversion results (e.g., core-blob/tmp/fileUID/uuid.pdf) after they've been used
// Note: This is different from DeleteConvertedFileActivity which deletes the final markdown from DB
func (w *Worker) DeleteTemporaryConvertedFileActivity(ctx context.Context, param *DeleteTemporaryConvertedFileActivityParam) error {
	if param.Destination == "" {
		w.log.Info("DeleteTemporaryConvertedFileActivity: No destination provided, skipping deletion")
		return nil
	}

	w.log.Info("DeleteTemporaryConvertedFileActivity: Deleting temporary converted file",
		zap.String("bucket", param.Bucket),
		zap.String("destination", param.Destination))

	err := w.repository.DeleteFile(ctx, param.Bucket, param.Destination)
	if err != nil {
		// Log error but don't fail the activity - temporary files can accumulate but won't break functionality
		w.log.Warn("DeleteTemporaryConvertedFileActivity: Failed to delete temporary file (will remain in MinIO)",
			zap.String("destination", param.Destination),
			zap.Error(err))
		return nil
	}

	return nil
}

// ===== MARKDOWN CONVERSION ACTIVITY =====

// ConvertToFileActivityParam for external pipeline conversion call
type ConvertToFileActivityParam struct {
	Bucket      string                     // MinIO bucket containing the file
	Destination string                     // MinIO file path
	FileType    artifactpb.FileType        // File type to convert
	Pipelines   []pipeline.PipelineRelease // Pipeline releases for fallback conversion
	Metadata    *structpb.Struct           // Request metadata for authentication context
	CacheName   string                     // Optional: AI cache name for efficient conversion
}

// ConvertToFileActivityResult contains the conversion result
type ConvertToFileActivityResult struct {
	Markdown        string                   // Converted markdown content
	PositionData    *repository.PositionData // Position metadata (e.g., page mappings)
	Length          []uint32                 // Length of markdown sections
	PipelineRelease pipeline.PipelineRelease // Pipeline used (empty if AI was used)
	UsageMetadata   any                      // Token usage metadata from AI provider (nil if pipeline was used)
}

// ConvertToFileActivity calls external pipeline to convert file to markdown
// For TEXT/MARKDOWN files, it extracts the file length without calling the pipeline
// This is a single external API call - idempotent (pipeline should be idempotent)
// Note: This activity fetches content directly from MinIO to avoid passing large files through Temporal
func (w *Worker) ConvertToFileActivity(ctx context.Context, param *ConvertToFileActivityParam) (*ConvertToFileActivityResult, error) {
	w.log.Info("ConvertToFileActivity: Converting file to markdown",
		zap.String("fileType", param.FileType.String()),
		zap.String("destination", param.Destination),
		zap.Int("pipelineCount", len(param.Pipelines)))

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = createAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to create authenticated context: %s", errorsx.MessageOrErr(err)),
				convertToFileActivityError,
				err,
			)
		}
	}

	// Fetch file content directly from MinIO (avoids passing large files through Temporal)
	content, err := w.repository.GetFile(authCtx, param.Bucket, param.Destination)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to retrieve file from storage: %s", errorsx.MessageOrErr(err)),
			convertToFileActivityError,
			err,
		)
	}

	w.log.Info("ConvertToFileActivity: File content retrieved from MinIO",
		zap.Int("contentSize", len(content)))

	// For TEXT/MARKDOWN files, no conversion needed - return content as-is
	if param.FileType == artifactpb.FileType_FILE_TYPE_TEXT || param.FileType == artifactpb.FileType_FILE_TYPE_MARKDOWN {
		w.log.Info("ConvertToFileActivity: TEXT/MARKDOWN file - returning content as-is",
			zap.String("fileType", param.FileType.String()))

		markdown := string(content)
		return &ConvertToFileActivityResult{
			Markdown:        markdown,
			Length:          []uint32{uint32(len(markdown))},
			PositionData:    nil,
			PipelineRelease: pipeline.PipelineRelease{}, // No conversion needed
		}, nil
	}

	// Try AI provider conversion first for supported file types (faster and more efficient)
	if w.aiProvider != nil && w.aiProvider.SupportsFileType(param.FileType) {
		// If cache name is provided, use cached context for much faster conversion
		if param.CacheName != "" {
			w.log.Info("ConvertToFileActivity: Attempting AI conversion with cache",
				zap.String("fileType", param.FileType.String()),
				zap.String("cacheName", param.CacheName),
				zap.String("provider", w.aiProvider.Name()))

			conversion, err := w.aiProvider.ConvertToMarkdownWithCache(
				authCtx,
				param.CacheName,
				gemini.DefaultConvertToMarkdownPromptTemplate,
			)
			if err != nil {
				// Log the error but fall back to non-cached conversion
				w.log.Warn("ConvertToFileActivity: Cached AI conversion failed, trying direct conversion",
					zap.Error(err))
			} else {
				w.log.Info("ConvertToFileActivity: Cached AI conversion successful",
					zap.Int("markdownLength", len(conversion.Markdown)),
					zap.String("provider", conversion.Provider))

				return &ConvertToFileActivityResult{
					Markdown:        conversion.Markdown,
					PositionData:    conversion.PositionData,
					Length:          conversion.Length,
					PipelineRelease: pipeline.PipelineRelease{}, // No pipeline used (AI cached)
					UsageMetadata:   conversion.UsageMetadata,
				}, nil
			}
		}

		// Try direct AI conversion (without cache)
		w.log.Info("ConvertToFileActivity: Attempting direct AI conversion",
			zap.String("fileType", param.FileType.String()),
			zap.String("provider", w.aiProvider.Name()))

		// Extract filename from destination if not provided
		filename := param.Destination
		if idx := strings.LastIndex(filename, "/"); idx >= 0 {
			filename = filename[idx+1:]
		}

		conversion, err := w.aiProvider.ConvertToMarkdown(authCtx, content, param.FileType, filename, gemini.DefaultConvertToMarkdownPromptTemplate)
		if err != nil {
			// Log the error but fall back to pipeline conversion
			w.log.Warn("ConvertToFileActivity: AI conversion failed, falling back to pipeline",
				zap.Error(err))
		} else {
			w.log.Info("ConvertToFileActivity: AI conversion successful",
				zap.Int("markdownLength", len(conversion.Markdown)),
				zap.String("provider", conversion.Provider))

			return &ConvertToFileActivityResult{
				Markdown:        conversion.Markdown,
				PositionData:    conversion.PositionData,
				Length:          conversion.Length,
				PipelineRelease: pipeline.PipelineRelease{}, // No pipeline used (AI direct)
				UsageMetadata:   conversion.UsageMetadata,
			}, nil
		}
	}

	// Fall back to pipeline conversion
	w.log.Info("ConvertToFileActivity: Using pipeline conversion",
		zap.String("fileType", param.FileType.String()))

	// Call the conversion pipeline - THIS IS THE KEY EXTERNAL CALL
	// Note: Use authCtx to pass authentication credentials to the pipeline service
	conversion, err := pipeline.ConvertToMarkdownPipe(authCtx, w.pipelineClient, w.repository, pipeline.MarkdownConversionParams{
		Base64Content: base64.StdEncoding.EncodeToString(content),
		Type:          param.FileType,
		Pipelines:     param.Pipelines,
	})
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("File conversion failed: %s", errorsx.MessageOrErr(err)),
			convertToFileActivityError,
			err,
		)
	}

	w.log.Info("ConvertToFileActivity: Pipeline conversion successful",
		zap.Int("markdownLength", len(conversion.Markdown)))

	return &ConvertToFileActivityResult{
		Markdown:        conversion.Markdown,
		PositionData:    conversion.PositionData,
		Length:          conversion.Length,
		PipelineRelease: conversion.PipelineRelease,
	}, nil
}

// ===== HELPER FUNCTIONS FOR FILE TYPE CONVERSION =====

// needsFileConversion checks if a file type needs conversion to AI-supported format
// Returns (needsConversion bool, targetFormat string)
// Based on format definitions in the AI component
func needsFileConversion(fileType artifactpb.FileType) (bool, string) {
	switch fileType {
	// AI-native image formats - no conversion needed
	case artifactpb.FileType_FILE_TYPE_PNG,
		artifactpb.FileType_FILE_TYPE_JPEG,
		artifactpb.FileType_FILE_TYPE_JPG,
		artifactpb.FileType_FILE_TYPE_WEBP,
		artifactpb.FileType_FILE_TYPE_HEIC,
		artifactpb.FileType_FILE_TYPE_HEIF:
		return false, ""

	// Convertible image formats - convert to PNG (widely supported)
	case artifactpb.FileType_FILE_TYPE_GIF,
		artifactpb.FileType_FILE_TYPE_BMP,
		artifactpb.FileType_FILE_TYPE_TIFF,
		artifactpb.FileType_FILE_TYPE_AVIF:
		return true, "png"

	// AI-native audio formats - no conversion needed
	case artifactpb.FileType_FILE_TYPE_MP3,
		artifactpb.FileType_FILE_TYPE_WAV,
		artifactpb.FileType_FILE_TYPE_AAC,
		artifactpb.FileType_FILE_TYPE_OGG,
		artifactpb.FileType_FILE_TYPE_FLAC,
		artifactpb.FileType_FILE_TYPE_AIFF:
		return false, ""

	// Convertible audio formats - convert to OGG (widely supported)
	case artifactpb.FileType_FILE_TYPE_M4A,
		artifactpb.FileType_FILE_TYPE_WMA:
		return true, "ogg"

	// AI-native video formats - no conversion needed
	case artifactpb.FileType_FILE_TYPE_MP4,
		artifactpb.FileType_FILE_TYPE_MPEG,
		artifactpb.FileType_FILE_TYPE_MOV,
		artifactpb.FileType_FILE_TYPE_AVI,
		artifactpb.FileType_FILE_TYPE_FLV,
		artifactpb.FileType_FILE_TYPE_WEBM_VIDEO,
		artifactpb.FileType_FILE_TYPE_WMV:
		return false, ""

	// Convertible video formats - convert to MP4 (widely supported)
	case artifactpb.FileType_FILE_TYPE_MKV:
		return true, "mp4"

	// AI-native document format - no conversion needed
	case artifactpb.FileType_FILE_TYPE_PDF:
		return false, ""

	// Convertible document formats - convert to PDF (widely supported visual document format)
	case artifactpb.FileType_FILE_TYPE_DOC,
		artifactpb.FileType_FILE_TYPE_DOCX,
		artifactpb.FileType_FILE_TYPE_PPT,
		artifactpb.FileType_FILE_TYPE_PPTX,
		artifactpb.FileType_FILE_TYPE_XLS,
		artifactpb.FileType_FILE_TYPE_XLSX:
		return true, "pdf"

	// Text-based documents - no conversion needed (will be used as text)
	case artifactpb.FileType_FILE_TYPE_HTML,
		artifactpb.FileType_FILE_TYPE_TEXT,
		artifactpb.FileType_FILE_TYPE_MARKDOWN,
		artifactpb.FileType_FILE_TYPE_CSV:
		return false, ""

	default:
		return false, ""
	}
}

// convertUsingPipeline converts a file using the convert-file-type pipeline
func (w *Worker) convertUsingPipeline(ctx context.Context, content []byte, sourceType artifactpb.FileType, targetFormat string, pipelines []pipeline.PipelineRelease) ([]byte, pipeline.PipelineRelease, error) {
	if len(pipelines) == 0 {
		return nil, pipeline.PipelineRelease{}, fmt.Errorf("no pipelines provided")
	}

	// Use the first available pipeline (convert-file-type)
	pipeline := pipelines[0]

	// Determine input variable name based on source type
	inputVarName := getInputVariableName(sourceType)
	if inputVarName == "" {
		return nil, pipeline, fmt.Errorf("unsupported source file type: %s", sourceType.String())
	}

	// Prepare pipeline input
	base64Content := base64.StdEncoding.EncodeToString(content)
	mimeType := ai.FileTypeToMIME(sourceType)
	dataURI := fmt.Sprintf("data:%s;base64,%s", mimeType, base64Content)

	inputs := []*structpb.Struct{
		{
			Fields: map[string]*structpb.Value{
				inputVarName: {
					Kind: &structpb.Value_StringValue{
						StringValue: dataURI,
					},
				},
			},
		},
	}

	// Import the pipeline protobuf package
	pipelineClient := w.pipelineClient

	// Create pipeline request
	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: pipeline.Namespace,
		PipelineId:  pipeline.ID,
		ReleaseId:   pipeline.Version,
		Inputs:      inputs,
	}

	// Trigger pipeline with timeout
	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

	resp, err := pipelineClient.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return nil, pipeline, fmt.Errorf("failed to trigger conversion pipeline: %w", err)
	}

	if resp == nil || len(resp.Outputs) == 0 {
		return nil, pipeline, fmt.Errorf("conversion pipeline returned no outputs")
	}

	// Extract converted content from output
	outputFieldName := getOutputFieldName(targetFormat)
	outputValue := resp.Outputs[0].Fields[outputFieldName]
	if outputValue == nil {
		return nil, pipeline, fmt.Errorf("conversion pipeline output missing field: %s", outputFieldName)
	}

	outputURL := outputValue.GetStringValue()
	if outputURL == "" {
		return nil, pipeline, fmt.Errorf("conversion pipeline output is empty")
	}

	// The pipeline returns a MinIO blob URL (not a data URI) for efficient large file handling
	// Format: http://localhost:8080/v1alpha/blob-urls/base64_encoded_presigned_url
	convertedContent, err := w.fetchFromBlobURL(ctx, outputURL)
	if err != nil {
		return nil, pipeline, fmt.Errorf("failed to fetch converted content from blob URL: %w", err)
	}

	return convertedContent, pipeline, nil
}

// fetchFromBlobURL fetches file content from a MinIO blob URL
// The blob URL format is: scheme://host/v1alpha/blob-urls/base64_encoded_presigned_url
// Uses the shared DecodeBlobURL function from the service layer
func (w *Worker) fetchFromBlobURL(ctx context.Context, blobURL string) ([]byte, error) {
	// Check if it's a blob URL or data URI
	if !strings.Contains(blobURL, "/v1alpha/blob-urls/") {
		// If it's a regular data URI, decode it directly
		if strings.HasPrefix(blobURL, "data:") {
			parts := strings.SplitN(blobURL, ",", 2)
			if len(parts) != 2 {
				return nil, fmt.Errorf("invalid data URI format")
			}
			return base64.StdEncoding.DecodeString(parts[1])
		}
		return nil, fmt.Errorf("unsupported URL format: %s", blobURL)
	}

	// Decode the blob URL to get the presigned URL using shared service function
	presignedURL, err := decodeBlobURL(blobURL)
	if err != nil {
		return nil, fmt.Errorf("failed to decode blob URL: %w", err)
	}

	// Fetch the file content from the presigned URL using HTTP client
	return w.fetchFromPresignedURL(ctx, presignedURL)
}

// fetchFromPresignedURL fetches file content from a presigned MinIO URL using HTTP client
func (w *Worker) fetchFromPresignedURL(ctx context.Context, presignedURL string) ([]byte, error) {
	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 300 * time.Second,
	}

	req, err := http.NewRequestWithContext(ctx, "GET", presignedURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch from presigned URL: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("presigned URL returned non-200 status: %d", resp.StatusCode)
	}

	// Read the response body
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	return content, nil
}

// getInputVariableName returns the pipeline input variable name based on file type
func getInputVariableName(fileType artifactpb.FileType) string {
	switch fileType {
	case artifactpb.FileType_FILE_TYPE_GIF,
		artifactpb.FileType_FILE_TYPE_BMP,
		artifactpb.FileType_FILE_TYPE_TIFF,
		artifactpb.FileType_FILE_TYPE_AVIF:
		return "image"
	case artifactpb.FileType_FILE_TYPE_M4A,
		artifactpb.FileType_FILE_TYPE_WMA:
		return "audio"
	case artifactpb.FileType_FILE_TYPE_MKV:
		return "video"
	case artifactpb.FileType_FILE_TYPE_DOC,
		artifactpb.FileType_FILE_TYPE_DOCX,
		artifactpb.FileType_FILE_TYPE_PPT,
		artifactpb.FileType_FILE_TYPE_PPTX,
		artifactpb.FileType_FILE_TYPE_XLS,
		artifactpb.FileType_FILE_TYPE_XLSX:
		return "document"
	default:
		return ""
	}
}

// getOutputFieldName returns the pipeline output field name based on target format
func getOutputFieldName(targetFormat string) string {
	switch targetFormat {
	case "png":
		return "image"
	case "ogg":
		return "audio"
	case "mp4":
		return "video"
	case "pdf":
		return "document"
	default:
		return ""
	}
}

// mapFormatToFileType maps target format string to FileType enum
func mapFormatToFileType(format string) artifactpb.FileType {
	switch format {
	case "png":
		return artifactpb.FileType_FILE_TYPE_PNG
	case "ogg":
		return artifactpb.FileType_FILE_TYPE_OGG
	case "mp4":
		return artifactpb.FileType_FILE_TYPE_MP4
	case "pdf":
		return artifactpb.FileType_FILE_TYPE_PDF
	default:
		return artifactpb.FileType_FILE_TYPE_UNSPECIFIED
	}
}

// ===== ERROR CONSTANTS =====

const (
	getFileMetadataActivityError                = "GetFileMetadataActivity"
	getFileContentActivityError                 = "GetFileContentActivity"
	cleanupOldConvertedFileActivityError        = "CleanupOldConvertedFileActivity"
	createConvertedFileRecordActivityError      = "CreateConvertedFileRecordActivity"
	uploadConvertedFileToMinIOActivityError     = "UploadConvertedFileToMinIOActivity"
	deleteConvertedFileRecordActivityError      = "DeleteConvertedFileRecordActivity"
	updateConvertedFileDestinationActivityError = "UpdateConvertedFileDestinationActivity"
	deleteConvertedFileFromMinIOActivityError   = "DeleteConvertedFileFromMinIOActivity"
	updateConversionMetadataActivityError       = "UpdateConversionMetadataActivity"
	getConvertedFileForChunkingActivityError    = "GetConvertedFileForChunkingActivity"
	chunkContentActivityError                   = "ChunkContentActivity"
	saveTextChunksToDBActivityError             = "SaveTextChunksToDBActivity"
	updateChunkingMetadataActivityError         = "UpdateChunkingMetadataActivity"
	generateSummaryActivityError                = "GenerateSummaryActivity"
	saveSummaryActivityError                    = "SaveSummaryActivity"
	convertFileTypeActivityError                = "ConvertFileTypeActivity"
	cacheContextActivityError                   = "CacheContextActivity"
	convertToFileActivityError                  = "ConvertToFileActivity"
)

// decodeBlobURL decodes a blob URL to extract the presigned URL
// This is a helper function copied from service package to avoid circular imports
func decodeBlobURL(blobURL string) (string, error) {
	// Check if it's a blob URL
	if !strings.Contains(blobURL, "/v1alpha/blob-urls/") {
		return "", fmt.Errorf("not a valid blob URL format")
	}

	// Parse the blob URL and extract the base64-encoded presigned URL
	urlParts := strings.Split(blobURL, "/")
	if len(urlParts) < 4 {
		return "", fmt.Errorf("invalid blob URL format")
	}

	// Find the "blob-urls" segment and get the next segment
	for i, part := range urlParts {
		if part == "blob-urls" && i+1 < len(urlParts) {
			base64EncodedURL := urlParts[i+1]

			// Decode the base64-encoded presigned URL
			decodedURL, err := base64.URLEncoding.DecodeString(base64EncodedURL)
			if err != nil {
				// Try standard encoding if URL encoding fails
				decodedURL, err = base64.StdEncoding.DecodeString(base64EncodedURL)
				if err != nil {
					return "", fmt.Errorf("failed to decode presigned URL: %w", err)
				}
			}

			return string(decodedURL), nil
		}
	}

	return "", fmt.Errorf("blob URL segment not found in URL")
}
