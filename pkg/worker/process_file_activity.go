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
// - StandardizeFileTypeActivity - Standardizes file formats to AI-native types (GIF→PNG, MKV→MP4, DOC→PDF, etc.)
// - CacheFileContextActivity - Creates AI cache for efficient file processing
// - DeleteCacheActivity - Cleans up AI cache
// - DeleteTemporaryConvertedFileActivity - Cleans up temporary converted files from MinIO
// - ConvertToFileActivity - Converts files to Markdown using AI or pipelines
//
// Child Workflow Activities (ProcessSummaryWorkflow):
// - GenerateSummaryActivity - Generates document summaries using AI
// - SaveSummaryActivity - Saves summary to PostgreSQL database

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
	File                    *repository.KnowledgeBaseFileModel // File metadata from database
	GenerateContentPipeline pipeline.PipelineRelease           // Pipeline for content generation
	GenerateSummaryPipeline pipeline.PipelineRelease           // Pipeline for summary generation
	ChunkMarkdownPipeline   pipeline.PipelineRelease           // Pipeline for chunking markdown (the converted documents)
	ChunkTextPipeline       pipeline.PipelineRelease           // Pipeline for chunking plain text
	EmbedPipeline           pipeline.PipelineRelease           // Pipeline for embedding generation
	ExternalMetadata        *structpb.Struct                   // External metadata from request
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

	// Use first configured pipeline or empty
	var generateContentPipeline pipeline.PipelineRelease
	if len(convertingPipelines) > 0 {
		generateContentPipeline = convertingPipelines[0]
	}

	return &GetFileMetadataActivityResult{
		File:                    &file,
		GenerateContentPipeline: generateContentPipeline,
		ExternalMetadata:        file.ExternalMetadataUnmarshal,
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
	KBUID            types.KBUIDType     // Knowledge base unique identifier
	FileUID          types.FileUIDType   // Original file unique identifier
	ConvertedFileUID types.FileUIDType   // Converted file unique identifier
	FileName         string              // Converted file name
	Destination      string              // MinIO destination path
	PositionData     *types.PositionData // Position data from conversion (e.g., page mappings)
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
	FileUID           types.FileUIDType        // File unique identifier
	KBUID             types.KBUIDType          // Knowledge base unique identifier
	ChunkTextPipeline pipeline.PipelineRelease // Pipeline for text chunking
	EmbedTextPipeline pipeline.PipelineRelease // Pipeline for text embedding
}

// GetConvertedFileForChunkingActivityResult contains file metadata and content
type GetConvertedFileForChunkingActivityResult struct {
	Content           string                   // Converted markdown content
	ChunkTextPipeline pipeline.PipelineRelease // Pipeline for text chunking
	EmbedTextPipeline pipeline.PipelineRelease // Pipeline for text embedding generation
	ExternalMetadata  *structpb.Struct         // External metadata from request
}

// ChunkContentActivityParam for external text chunking pipeline call
type ChunkContentActivityParam struct {
	FileUID           types.FileUIDType        // File unique identifier
	KBUID             types.KBUIDType          // Knowledge base unique identifier
	Content           string                   // Content to chunk into text chunks
	ChunkTextPipeline pipeline.PipelineRelease // Pipeline for text chunking
	Metadata          *structpb.Struct         // Request metadata for authentication
}

// ChunkContentActivityResult contains chunked content
type ChunkContentActivityResult struct {
	TextChunks      []types.TextChunk        // Generated text chunks
	PipelineRelease pipeline.PipelineRelease // Pipeline used for text chunking
}

// SaveTextChunksToDBActivityParam for saving text chunks to database
type SaveTextChunksToDBActivityParam struct {
	FileUID    types.FileUIDType // File unique identifier
	KBUID      types.KBUIDType   // Knowledge base unique identifier
	TextChunks []types.TextChunk // Text chunks to save
}

// SaveChunksToDBActivityResult contains saved text chunk UIDs from SaveTextChunksToDBActivity
type SaveChunksToDBActivityResult struct {
	TextChunkUIDs []types.TextChunkUIDType // Saved text chunk unique identifiers
}

// UpdateChunkingMetadataActivityParam for updating metadata after text chunking
type UpdateChunkingMetadataActivityParam struct {
	FileUID        types.FileUIDType // File unique identifier
	KBUID          types.KBUIDType   // Knowledge base unique identifier
	Pipeline       string            // Pipeline used for text chunking
	TextChunkCount uint32            // Number of text chunks created
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

	// Pass through pipeline from workflow parameter
	return &GetConvertedFileForChunkingActivityResult{
		Content:           string(content),
		ChunkTextPipeline: param.ChunkTextPipeline,
		EmbedTextPipeline: param.EmbedTextPipeline,
		ExternalMetadata:  file.ExternalMetadataUnmarshal,
	}, nil
}

// ChunkContentActivity chunks content by pages using position data
// Each page becomes a separate chunk for accurate page-level citations
// For non-paginated content (like summaries), returns empty to skip chunking
func (w *Worker) ChunkContentActivity(ctx context.Context, param *ChunkContentActivityParam) (*ChunkContentActivityResult, error) {
	w.log.Info("ChunkContentActivity: Chunking content by pages",
		zap.String("fileUID", param.FileUID.String()))

	// Get converted file to access position data
	convertedFile, err := w.repository.GetConvertedFileByFileUID(ctx, param.FileUID)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to get converted file: %s", errorsx.MessageOrErr(err)),
			chunkContentActivityError,
			err,
		)
	}

	// Check if we have position data
	if convertedFile.PositionData == nil || len(convertedFile.PositionData.PageDelimiters) == 0 {
		w.log.Info("ChunkContentActivity: No page delimiters, treating entire file as single chunk")
		// For non-paginated files (TXT, MD, CSV, HTML), treat entire content as one chunk
		tokens := ai.EstimateTokenCount(param.Content)
		chunk := types.TextChunk{
			Text:   param.Content,
			Start:  0,
			End:    len(param.Content),
			Tokens: tokens,
			Reference: &types.TextChunkReference{
				PageRange: [2]uint32{1, 1}, // Treat as single-page file
			},
		}
		w.log.Info("ChunkContentActivity: Created single text chunk",
			zap.Int("contentLength", len(param.Content)),
			zap.Int("tokens", tokens))
		return &ChunkContentActivityResult{
			TextChunks:      []types.TextChunk{chunk},
			PipelineRelease: pipeline.PipelineRelease{}, // No pipeline used
		}, nil
	}

	contentRunes := []rune(param.Content)
	lastDelimiter := convertedFile.PositionData.PageDelimiters[len(convertedFile.PositionData.PageDelimiters)-1]

	// Check if content length matches page delimiters (i.e., it's the converted markdown, not summary)
	// If content is much shorter/longer than expected, it's probably summary or other text
	contentLen := uint32(len(contentRunes))
	if contentLen < lastDelimiter/2 || contentLen > lastDelimiter*2 {
		w.log.Info("ChunkContentActivity: Content length mismatch with page delimiters, treating as single chunk",
			zap.Uint32("contentLen", contentLen),
			zap.Uint32("expectedLen", lastDelimiter))
		// Fall back to single chunk for summaries or mismatched content
		tokens := ai.EstimateTokenCount(param.Content)
		chunk := types.TextChunk{
			Text:   param.Content,
			Start:  0,
			End:    len(param.Content),
			Tokens: tokens,
			Reference: &types.TextChunkReference{
				PageRange: [2]uint32{1, 1}, // Treat as single-page file
			},
		}
		w.log.Info("ChunkContentActivity: Created single text chunk (fallback)",
			zap.Int("contentLength", len(param.Content)),
			zap.Int("tokens", tokens))
		return &ChunkContentActivityResult{
			TextChunks:      []types.TextChunk{chunk},
			PipelineRelease: pipeline.PipelineRelease{}, // No pipeline used
		}, nil
	}

	// Chunk by pages using position data
	w.log.Info("ChunkContentActivity: Chunking by page delimiters",
		zap.Int("pageCount", len(convertedFile.PositionData.PageDelimiters)))

	chunks := make([]types.TextChunk, 0, len(convertedFile.PositionData.PageDelimiters))
	var startPos uint32

	for pageNum, endPos := range convertedFile.PositionData.PageDelimiters {
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
		tokens := ai.EstimateTokenCount(pageText)
		chunk := types.TextChunk{
			Text:   pageText,
			Start:  int(startPos),
			End:    int(endPos),
			Tokens: tokens,
			Reference: &types.TextChunkReference{
				PageRange: [2]uint32{uint32(pageNum + 1), uint32(pageNum + 1)}, // Single page
			},
		}
		chunks = append(chunks, chunk)

		startPos = endPos
	}

	w.log.Info("ChunkContentActivity: Page-based chunking successful",
		zap.Int("chunkCount", len(chunks)),
		zap.Int("pageCount", len(convertedFile.PositionData.PageDelimiters)))

	return &ChunkContentActivityResult{
		TextChunks:      chunks,
		PipelineRelease: pipeline.PipelineRelease{}, // No pipeline used
	}, nil
}

// SaveTextChunksToDBActivity saves text chunks to database
func (w *Worker) SaveTextChunksToDBActivity(ctx context.Context, param *SaveTextChunksToDBActivityParam) (*SaveChunksToDBActivityResult, error) {
	w.log.Info("SaveChunksToDBActivity: Saving text chunks to database",
		zap.String("fileUID", param.FileUID.String()),
		zap.Int("chunkCount", len(param.TextChunks)))

	// Get converted file for source information
	convertedFile, err := w.repository.GetConvertedFileByFileUID(ctx, param.FileUID)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to get converted file: %s", errorsx.MessageOrErr(err)),
			saveTextChunksToDBActivityError,
			err,
		)
	}

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
	w.log.Info("SaveChunksToDBActivity: Chunks with references",
		zap.Int("totalChunks", len(chunksWithReferences)),
		zap.Int("chunksWithRefs", chunksWithRefs))

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
		ChunkingPipe: param.Pipeline,
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

// SaveSummaryActivityParam saves summary to database
type SaveSummaryActivityParam struct {
	FileUID  types.FileUIDType // File unique identifier
	Summary  string            // Summary text to save
	Pipeline string            // Pipeline used for summary generation (empty if AI was used)
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

// ===== FILE TYPE CONVERSION ACTIVITY =====

// StandardizeFileTypeActivityParam defines the parameters for StandardizeFileTypeActivity
type StandardizeFileTypeActivityParam struct {
	FileUID     types.FileUIDType          // File unique identifier
	KBUID       types.KBUIDType            // Knowledge base unique identifier
	Bucket      string                     // MinIO bucket containing the file
	Destination string                     // MinIO path to the file
	FileType    artifactpb.FileType        // Original file type to convert from
	Filename    string                     // Filename for identification
	Pipelines   []pipeline.PipelineRelease // indexing-convert-file-type pipeline
	Metadata    *structpb.Struct           // Request metadata for authentication
}

// StandardizeFileTypeActivityResult defines the result of StandardizeFileTypeActivity
type StandardizeFileTypeActivityResult struct {
	ConvertedDestination string                   // MinIO path to converted file (empty if no conversion)
	ConvertedBucket      string                   // MinIO bucket for converted file (empty if no conversion)
	ConvertedType        artifactpb.FileType      // New file type after conversion
	OriginalType         artifactpb.FileType      // Original file type
	Converted            bool                     // Whether conversion was performed
	PipelineRelease      pipeline.PipelineRelease // Pipeline used for conversion
}

// StandardizeFileTypeActivity standardizes non-AI-native file types to AI-supported formats
// Following format mappings defined in the AI component
func (w *Worker) StandardizeFileTypeActivity(ctx context.Context, param *StandardizeFileTypeActivityParam) (*StandardizeFileTypeActivityResult, error) {
	w.log.Info("StandardizeFileTypeActivity: Checking if file needs standardization",
		zap.String("fileType", param.FileType.String()),
		zap.String("filename", param.Filename))

	// Check if file needs conversion
	needsConversion, targetFormat := needsFileConversion(param.FileType)
	if !needsConversion {
		w.log.Info("StandardizeFileTypeActivity: File type is AI-native, no standardization needed",
			zap.String("fileType", param.FileType.String()))
		return &StandardizeFileTypeActivityResult{
			ConvertedDestination: "", // No conversion, use original file
			ConvertedBucket:      "",
			ConvertedType:        param.FileType,
			OriginalType:         param.FileType,
			Converted:            false,
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
	content, err := w.repository.GetFile(authCtx, param.Bucket, param.Destination)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to retrieve file from storage: %s", errorsx.MessageOrErr(err)),
			standardizeFileTypeActivityError,
			err,
		)
	}

	w.log.Info("StandardizeFileTypeActivity: File content retrieved from MinIO",
		zap.Int("contentSize", len(content)))

	// Determine which pipeline to use and prepare input
	var convertedContent []byte
	var usedPipeline pipeline.PipelineRelease

	// Try indexing-convert-file-type pipeline
	if len(param.Pipelines) > 0 {
		convertedContent, usedPipeline, err = w.convertUsingPipeline(authCtx, content, param.FileType, targetFormat, param.Pipelines)
		if err != nil {
			w.log.Warn("StandardizeFileTypeActivity: Pipeline standardization failed",
				zap.Error(err),
				zap.String("pipeline", usedPipeline.Name()))
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to convert file: %s", errorsx.MessageOrErr(err)),
				standardizeFileTypeActivityError,
				err,
			)
		}
	} else {
		return nil, temporal.NewApplicationError(
			"No conversion pipeline available",
			standardizeFileTypeActivityError,
		)
	}

	w.log.Info("StandardizeFileTypeActivity: File standardization successful",
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
			standardizeFileTypeActivityError,
			err,
		)
	}

	w.log.Info("StandardizeFileTypeActivity: Standardized file uploaded to MinIO",
		zap.String("destination", convertedDestination),
		zap.String("bucket", repository.BlobBucketName),
		zap.String("mimeType", mimeType))

	return &StandardizeFileTypeActivityResult{
		ConvertedDestination: convertedDestination,
		ConvertedBucket:      repository.BlobBucketName,
		ConvertedType:        convertedFileType,
		OriginalType:         param.FileType,
		Converted:            true,
		PipelineRelease:      usedPipeline,
	}, nil
}

// ===== CACHE FILE CONTEXT ACTIVITY =====

// CacheFileContextActivityParam defines the parameters for CacheFileContextActivity
type CacheFileContextActivityParam struct {
	FileUID     types.FileUIDType   // File unique identifier
	KBUID       types.KBUIDType     // Knowledge base unique identifier
	Bucket      string              // MinIO bucket (original or converted file)
	Destination string              // MinIO path (original or converted file)
	FileType    artifactpb.FileType // File type for content type determination
	Filename    string              // Filename for cache display name
	Metadata    *structpb.Struct    // Request metadata for authentication
}

// CacheFileContextActivityResult defines the result of CacheFileContextActivity
type CacheFileContextActivityResult struct {
	CacheName            string    // AI cache name
	Model                string    // Model used for cache
	CreateTime           time.Time // When cache was created
	ExpireTime           time.Time // When cache will expire
	CachedContextEnabled bool      // Flag indicating if cache was created (false means caching is disabled)
	UsageMetadata        any       // Token usage metadata from AI provider (nil if cache was not created)
}

// CacheFileContextActivity creates a cached context for the input file
// This enables efficient subsequent operations like conversion, analysis, etc.
// Supports: Documents (PDF, DOCX, DOC, PPTX, PPT), Images, Audio, Video
func (w *Worker) CacheFileContextActivity(ctx context.Context, param *CacheFileContextActivityParam) (*CacheFileContextActivityResult, error) {
	w.log.Info("CacheFileContextActivity: Creating cache for input content",
		zap.String("fileUID", param.FileUID.String()),
		zap.String("fileType", param.FileType.String()),
		zap.String("destination", param.Destination))

	// Check if AI provider is available
	if w.aiProvider == nil {
		w.log.Info("CacheFileContextActivity: AI provider not available, skipping cache creation")
		return &CacheFileContextActivityResult{
			CachedContextEnabled: false,
		}, nil
	}

	// Check if file type is supported for caching
	if !w.aiProvider.SupportsFileType(param.FileType) {
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
	content, err := w.repository.GetFile(authCtx, param.Bucket, param.Destination)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to retrieve file from storage: %s", errorsx.MessageOrErr(err)),
			cacheContextActivityError,
			err,
		)
	}

	w.log.Info("CacheFileContextActivity: File content retrieved from MinIO",
		zap.String("bucket", param.Bucket),
		zap.String("destination", param.Destination),
		zap.Int("contentSize", len(content)))

	// Set cache TTL (5 minutes default)
	cacheTTL := 5 * time.Minute

	// Create the cache using AI provider
	// Note: Cache creation is optional - if it fails, we continue without cache
	// Use RAG system instruction for conversion/summarization operations
	cacheOutput, err := w.aiProvider.CreateCache(authCtx, []ai.FileContent{
		{
			Content:  content,
			FileType: param.FileType,
			Filename: param.Filename,
		},
	}, cacheTTL, ai.SystemInstructionRAG)
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

// CacheChatContextActivityParam defines the parameters for CacheChatContextActivity
type CacheChatContextActivityParam struct {
	FileUIDs []types.FileUIDType // File unique identifiers to cache together
	KBUID    types.KBUIDType     // Knowledge base unique identifier
	Metadata *structpb.Struct    // Request metadata for authentication
}

// CacheChatContextActivityResult defines the output from CacheChatContextActivity
// This has the same structure as CacheFileContextActivityResult but is a separate type for clarity
// Supports two modes:
// 1. Cached mode: CachedContextEnabled=true, CacheName set, FileRefs empty (large files)
// 2. Uncached mode: CachedContextEnabled=false, CacheName empty, FileRefs set (small files)
type CacheChatContextActivityResult struct {
	CacheName            string                      // AI cache name (empty if cache creation failed)
	Model                string                      // Model used for cache
	CreateTime           time.Time                   // When cache was created
	ExpireTime           time.Time                   // When cache will expire
	CachedContextEnabled bool                        // Flag indicating if cached context was created (false means caching is disabled)
	UsageMetadata        any                         // Token usage metadata from AI provider (nil if cache was not created)
	FileRefs             []repository.FileContentRef // File references for uncached small files
}

// CacheChatContextActivity creates a chat cached context for multiple files
// This enables efficient multi-file operations like question answering across multiple documents
// The chat cache contains all files together, optimized for cross-file queries
func (w *Worker) CacheChatContextActivity(ctx context.Context, param *CacheChatContextActivityParam) (*CacheChatContextActivityResult, error) {
	w.log.Info("CacheChatContextActivity: Creating chat cache for multiple files",
		zap.Int("fileCount", len(param.FileUIDs)))

	// Check if AI provider is available
	if w.aiProvider == nil {
		w.log.Info("CacheChatContextActivity: AI provider not available, skipping cache creation")
		return &CacheChatContextActivityResult{
			CachedContextEnabled: false,
		}, nil
	}

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = CreateAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			w.log.Warn("CacheChatContextActivity: Failed to create authenticated context, proceeding without auth",
				zap.Error(err))
		}
	}

	// Fetch metadata and content for all files
	fileContents := make([]ai.FileContent, 0, len(param.FileUIDs))

	for _, fileUID := range param.FileUIDs {
		// Get file metadata
		kbFiles, err := w.repository.GetKnowledgeBaseFilesByFileUIDs(authCtx, []uuid.UUID{fileUID})
		if err != nil || len(kbFiles) == 0 {
			w.log.Warn("CacheChatContextActivity: Failed to get file metadata, skipping file",
				zap.String("fileUID", fileUID.String()),
				zap.Error(err))
			continue
		}
		kbFile := kbFiles[0]

		// Fetch file content from MinIO
		bucket := repository.BucketFromDestination(kbFile.Destination)
		content, err := w.repository.GetFile(authCtx, bucket, kbFile.Destination)
		if err != nil {
			w.log.Warn("CacheChatContextActivity: Failed to retrieve file content, skipping file",
				zap.String("fileUID", fileUID.String()),
				zap.String("destination", kbFile.Destination),
				zap.Error(err))
			continue
		}

		fileType := artifactpb.FileType(artifactpb.FileType_value[kbFile.Type])

		// Check if file type is supported for caching
		if !w.aiProvider.SupportsFileType(fileType) {
			w.log.Info("CacheChatContextActivity: File type not supported for caching, skipping file",
				zap.String("fileUID", fileUID.String()),
				zap.String("fileType", fileType.String()))
			continue
		}

		fileContents = append(fileContents, ai.FileContent{
			FileUID:  fileUID,
			Content:  content,
			FileType: fileType,
			Filename: kbFile.Name,
		})

		w.log.Info("CacheChatContextActivity: File content retrieved",
			zap.String("fileUID", fileUID.String()),
			zap.String("filename", kbFile.Name),
			zap.Int("contentSize", len(content)))
	}

	// Check if we have any files to cache
	if len(fileContents) == 0 {
		w.log.Warn("CacheChatContextActivity: No valid files to cache")
		return &CacheChatContextActivityResult{
			CachedContextEnabled: false,
		}, nil
	}

	// Set cache TTL (5 minutes default)
	cacheTTL := 5 * time.Minute

	// Proactively check if total content meets minimum cache size (1024 tokens)
	// This avoids unnecessary API calls for small files
	estimatedTokens, err := ai.EstimateTotalTokens(fileContents)
	if err != nil {
		w.log.Warn("CacheChatContextActivity: Failed to estimate tokens, will attempt cache creation anyway",
			zap.Error(err))
		estimatedTokens = ai.MinCacheTokens // Assume sufficient tokens if estimation fails
	}

	var cacheOutput *ai.CacheResult
	var cacheErr error

	if estimatedTokens < ai.MinCacheTokens {
		// Content is too small for AI cache (expected, not an error)
		// Skip cache creation and directly store content for fallback chat
		w.log.Info("CacheChatContextActivity: Content too small for AI cache, storing directly in Redis",
			zap.Int("estimatedTokens", estimatedTokens),
			zap.Int("minRequired", ai.MinCacheTokens),
			zap.Int("fileCount", len(fileContents)))
	} else {
		// Content is large enough, attempt cache creation
		w.log.Info("CacheChatContextActivity: Attempting to create AI cache",
			zap.Int("estimatedTokens", estimatedTokens),
			zap.Int("fileCount", len(fileContents)))

		// Use Chat system instruction for Q&A operations
		cacheOutput, cacheErr = w.aiProvider.CreateCache(authCtx, fileContents, cacheTTL, ai.SystemInstructionChat)
		if cacheErr != nil {
			// Real cache creation error (network, quota, API error, etc.)
			// This is unexpected since we verified token count
			w.log.Error("CacheChatContextActivity: Cache creation failed unexpectedly",
				zap.Error(cacheErr),
				zap.Int("estimatedTokens", estimatedTokens),
				zap.Int("fileCount", len(fileContents)))
		} else {
			// Cache created successfully!
			w.log.Info("CacheChatContextActivity: Chat cache created successfully",
				zap.String("cacheName", cacheOutput.CacheName),
				zap.String("model", cacheOutput.Model),
				zap.Int("fileCount", len(fileContents)))

			return &CacheChatContextActivityResult{
				CacheName:            cacheOutput.CacheName,
				Model:                cacheOutput.Model,
				CreateTime:           cacheOutput.CreateTime,
				ExpireTime:           cacheOutput.ExpireTime,
				CachedContextEnabled: true,
				UsageMetadata:        cacheOutput.UsageMetadata,
			}, nil
		}
	}

	// If we reach here, either:
	// 1. Content is too small (< 1024 tokens) - expected
	// 2. Cache creation failed for other reasons - unexpected but we continue
	// In both cases, store file content directly in Redis for fallback chat
	fileRefs := make([]repository.FileContentRef, 0, len(fileContents))
	for _, fc := range fileContents {
		// Get file metadata for type information
		kbFiles, err := w.repository.GetKnowledgeBaseFilesByFileUIDs(authCtx, []uuid.UUID{fc.FileUID})
		if err != nil || len(kbFiles) == 0 {
			continue
		}
		kbFile := kbFiles[0]

		// Store actual content in Redis
		fileRefs = append(fileRefs, repository.FileContentRef{
			FileUID:  fc.FileUID,
			Content:  fc.Content, // Store content directly
			FileType: kbFile.Type,
			Filename: kbFile.Name,
		})
	}

	w.log.Info("CacheChatContextActivity: Stored file content in Redis for uncached chat",
		zap.Int("fileCount", len(fileRefs)),
		zap.Int("totalBytes", func() int {
			total := 0
			for _, ref := range fileRefs {
				total += len(ref.Content)
			}
			return total
		}()))

	return &CacheChatContextActivityResult{
		CachedContextEnabled: false,
		FileRefs:             fileRefs,
		Model:                "gemini-2.5-flash", // Default model for uncached chat
		ExpireTime:           time.Now().Add(cacheTTL),
	}, nil
}

// ===== CONTENT PROCESSING ACTIVITIES =====

// ProcessContentActivityParam defines input for ProcessContentActivity
type ProcessContentActivityParam struct {
	FileUID          types.FileUIDType        // File unique identifier
	KBUID            types.KBUIDType          // Knowledge base unique identifier
	Bucket           string                   // MinIO bucket
	Destination      string                   // MinIO path
	FileType         artifactpb.FileType      // File type
	Filename         string                   // File name
	FallbackPipeline pipeline.PipelineRelease // Pipeline for conversion fallback
	Metadata         *structpb.Struct         // Request metadata
	CacheName        string                   // AI cache name
}

// ProcessContentActivityResult defines output from ProcessContentActivity
type ProcessContentActivityResult struct {
	Markdown           string                   // Converted markdown content
	Length             []uint32                 // Length information
	PositionData       *types.PositionData      // Position data for PDF
	ConversionPipeline pipeline.PipelineRelease // Pipeline used
	FormatConverted    bool                     // Whether format conversion occurred
	OriginalType       artifactpb.FileType      // Original file type
	ConvertedType      artifactpb.FileType      // Converted file type
	UsageMetadata      any                      // AI token usage
}

// ProcessContentActivity handles the entire content processing pipeline:
// 1. Cleanup old converted files
// 2. Markdown conversion (AI or pipeline)
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

	// Phase 1: Cleanup old converted file
	if err := w.CleanupOldConvertedFileActivity(ctx, &CleanupOldConvertedFileActivityParam{
		FileUID: param.FileUID,
	}); err != nil {
		logger.Error("Failed to cleanup old converted file", zap.Error(err))
		return nil, err
	}

	// Phase 2: Markdown conversion
	// Note: Format conversion (DOCX→PDF, GIF→PNG, etc.) has already been done at the workflow level
	// The param.Bucket, param.Destination, param.FileType already point to the converted file

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = CreateAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			logger.Error("Failed to create authenticated context", zap.Error(err))
			return nil, err
		}
	}

	// Fetch file content from MinIO
	content, err := w.repository.GetFile(authCtx, param.Bucket, param.Destination)
	if err != nil {
		logger.Error("Failed to retrieve file from storage", zap.Error(err))
		return nil, err
	}

	logger.Info("File content retrieved",
		zap.Int("contentSize", len(content)))

	var markdown string
	var positionData *types.PositionData
	var length []uint32
	var pipelineRelease pipeline.PipelineRelease
	var usageMetadata any

	// For TEXT/MARKDOWN files, no AI/pipeline conversion needed - content is already in usable format
	if param.FileType == artifactpb.FileType_FILE_TYPE_TEXT || param.FileType == artifactpb.FileType_FILE_TYPE_MARKDOWN {
		logger.Info("TEXT/MARKDOWN file - using content as-is (no conversion needed)")
		markdown = string(content)
		length = []uint32{uint32(len(markdown))}
	} else {
		// Check if AI provider supports this file type before attempting AI conversion
		aiSupported := w.aiProvider != nil && w.aiProvider.SupportsFileType(param.FileType)
		if !aiSupported && param.FileType != artifactpb.FileType_FILE_TYPE_PDF {
			logger.Warn("File type not supported by AI provider and should have been converted",
				zap.String("fileType", param.FileType.String()),
				zap.String("hint", "StandardizeFileTypeActivity may have failed"))
		}

		// For other file types: Try AI provider with cache first (if available)
		if aiSupported && param.CacheName != "" {
			logger.Info("Attempting AI conversion with cache",
				zap.String("cacheName", param.CacheName),
				zap.String("provider", w.aiProvider.Name()))

			conversion, err := w.aiProvider.ConvertToMarkdownWithCache(
				authCtx,
				param.CacheName,
				gemini.GetRAGGenerateContentPrompt(),
			)
			if err != nil {
				logger.Warn("Cached AI conversion failed, will try without cache", zap.Error(err))
			} else {
				logger.Info("Cached AI conversion successful",
					zap.Int("markdownLength", len(conversion.Markdown)))

				// Process AI conversion result
				markdown, positionData, length = ProcessAIConversionResult(
					conversion.Markdown,
					w.log,
					map[string]any{"cacheName": param.CacheName},
				)
				usageMetadata = conversion.UsageMetadata
			}
		}

		// Try AI provider without cache if cached attempt failed or wasn't available
		if markdown == "" && aiSupported {
			logger.Info("Attempting AI conversion without cache",
				zap.String("fileType", param.FileType.String()),
				zap.String("provider", w.aiProvider.Name()))

			conversion, err := w.aiProvider.ConvertToMarkdownWithoutCache(
				authCtx,
				content,
				param.FileType,
				param.Filename,
				gemini.GetRAGGenerateContentPrompt(),
			)
			if err != nil {
				logger.Warn("AI conversion failed, will try pipeline fallback", zap.Error(err))
			} else {
				logger.Info("AI conversion successful",
					zap.Int("markdownLength", len(conversion.Markdown)))

				// Process AI conversion result
				markdown, positionData, length = ProcessAIConversionResult(
					conversion.Markdown,
					w.log,
					map[string]any{"fileType": param.FileType.String()},
				)
				usageMetadata = conversion.UsageMetadata
			}
		}

		// Pipeline fallback if AI failed or not available
		if markdown == "" {
			logger.Info("Using pipeline for markdown conversion")

			// Use configured pipeline or empty to use defaults
			var pipelines []pipeline.PipelineRelease
			if param.FallbackPipeline.ID != "" {
				pipelines = []pipeline.PipelineRelease{param.FallbackPipeline}
			}

			conversion, err := pipeline.GenerateContentPipe(authCtx, w.pipelineClient, w.repository, pipeline.GenerateContentParams{
				Base64Content: base64.StdEncoding.EncodeToString(content),
				Type:          param.FileType,
				Pipelines:     pipelines,
			})
			if err != nil {
				logger.Error("Pipeline conversion failed", zap.Error(err))
				return nil, err
			}

			markdown = conversion.Markdown
			positionData = conversion.PositionData
			length = conversion.Length
			pipelineRelease = conversion.PipelineRelease
			logger.Info("Pipeline conversion successful",
				zap.Int("markdownLength", len(markdown)))
		}
	}

	result.Markdown = markdown
	result.Length = length
	result.PositionData = positionData
	result.ConversionPipeline = pipelineRelease
	result.UsageMetadata = usageMetadata
	result.ConvertedType = param.FileType // Already converted at workflow level
	result.FormatConverted = false        // Not converted here (already done at workflow level)

	logger.Info("Markdown conversion completed", zap.Int("markdownLength", len(markdown)))

	// Phase 3: Save converted file to DB and MinIO
	// All file types now follow the same 3-step SAGA: create record → upload → update destination
	convertedFileUID, _ := uuid.NewV4()

	// Step 1: Create DB record with placeholder
	_, err = w.CreateConvertedFileRecordActivity(ctx, &CreateConvertedFileRecordActivityParam{
		KBUID:            param.KBUID,
		FileUID:          param.FileUID,
		ConvertedFileUID: convertedFileUID,
		FileName:         "converted_" + param.Filename,
		Destination:      fmt.Sprintf("placeholder-pending-upload-%s", convertedFileUID.String()),
		PositionData:     positionData,
	})
	if err != nil {
		logger.Error("Failed to create converted file record", zap.Error(err))
		return nil, err
	}

	// Step 2: Upload to MinIO
	uploadResult, err := w.UploadConvertedFileToMinIOActivity(ctx, &UploadConvertedFileToMinIOActivityParam{
		KBUID:            param.KBUID,
		FileUID:          param.FileUID,
		ConvertedFileUID: convertedFileUID,
		Content:          markdown,
	})
	if err != nil {
		// Compensating transaction: delete DB record
		logger.Warn("MinIO upload failed, deleting DB record", zap.Error(err))
		_ = w.DeleteConvertedFileRecordActivity(ctx, &DeleteConvertedFileRecordActivityParam{
			ConvertedFileUID: convertedFileUID,
		})
		return nil, err
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
		return nil, err
	}

	logger.Info("Converted file saved successfully", zap.String("convertedFileUID", convertedFileUID.String()))
	return result, nil
}

// ProcessSummaryActivityParam defines input for ProcessSummaryActivity
type ProcessSummaryActivityParam struct {
	FileUID          types.FileUIDType        // File unique identifier
	KBUID            types.KBUIDType          // Knowledge base unique identifier
	Bucket           string                   // MinIO bucket
	Destination      string                   // MinIO path
	FileName         string                   // File name
	FileType         artifactpb.FileType      // File type
	Metadata         *structpb.Struct         // Request metadata
	CacheName        string                   // AI cache name
	FallbackPipeline pipeline.PipelineRelease // Pipeline for summary generation fallback
}

// ProcessSummaryActivityResult defines output from ProcessSummaryActivity
type ProcessSummaryActivityResult struct {
	Summary       string // Generated summary
	Pipeline      string // Pipeline used (empty if AI)
	UsageMetadata any    // AI token usage
}

// ProcessSummaryActivity handles the entire summary generation pipeline:
// 1. Generate summary (using AI cache or pipeline)
// 2. Save summary to database
//
// This is a composite activity that replaces ProcessSummaryWorkflow for simplified architecture.
func (w *Worker) ProcessSummaryActivity(ctx context.Context, param *ProcessSummaryActivityParam) (*ProcessSummaryActivityResult, error) {
	logger := w.log.With(
		zap.String("fileUID", param.FileUID.String()),
		zap.Bool("hasCache", param.CacheName != ""))

	logger.Info("ProcessSummaryActivity started")

	result := &ProcessSummaryActivityResult{}

	// Phase 1: Generate summary from file content
	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = CreateAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			logger.Error("Failed to create authenticated context", zap.Error(err))
			return nil, err
		}
	}

	// Fetch content from MinIO
	content, err := w.repository.GetFile(authCtx, param.Bucket, param.Destination)
	if err != nil {
		logger.Error("Failed to retrieve file from storage", zap.Error(err))
		return nil, err
	}

	// Sanitize content to ensure valid UTF-8
	contentStr := sanitizeUTF8(content)

	logger.Info("Content prepared for summarization",
		zap.Int("originalBytes", len(content)),
		zap.Int("sanitizedBytes", len(contentStr)))

	// Verify content is not empty after sanitization
	if len(strings.TrimSpace(contentStr)) == 0 {
		return nil, fmt.Errorf("content is empty after UTF-8 sanitization for file %s", param.FileUID.String())
	}

	var summary string
	var pipelineName string
	var usageMetadata any

	// Build prompt with filename context
	summaryPrompt := gemini.GetRAGGenerateSummaryPrompt()
	if param.FileName != "" {
		summaryPrompt = strings.ReplaceAll(gemini.GetRAGGenerateSummaryPrompt(), "[filename]", param.FileName)
	}

	// Use the file type from parameter (already converted in workflow)
	fileType := param.FileType

	// Try AI provider with cache first (if available)
	if w.aiProvider != nil && param.CacheName != "" && w.aiProvider.SupportsFileType(fileType) {
		logger.Info("Attempting AI summarization with cache",
			zap.String("cacheName", param.CacheName),
			zap.String("provider", w.aiProvider.Name()))

		conversion, err := w.aiProvider.ConvertToMarkdownWithCache(authCtx, param.CacheName, summaryPrompt)
		if err != nil {
			logger.Warn("Cached AI summarization failed, will try without cache", zap.Error(err))
		} else {
			summary = conversion.Markdown
			usageMetadata = conversion.UsageMetadata
			logger.Info("AI summarization with cache succeeded",
				zap.Int("summaryLength", len(summary)))
		}
	}

	// Try AI provider without cache if cached attempt failed or wasn't available
	if summary == "" && w.aiProvider != nil && w.aiProvider.SupportsFileType(fileType) {
		logger.Info("Attempting AI summarization without cache",
			zap.String("fileType", fileType.String()),
			zap.String("provider", w.aiProvider.Name()))

		conversion, err := w.aiProvider.ConvertToMarkdownWithoutCache(
			authCtx,
			content,
			fileType,
			param.FileName,
			summaryPrompt,
		)
		if err != nil {
			logger.Warn("AI summarization failed, will try pipeline fallback", zap.Error(err))
		} else {
			summary = conversion.Markdown
			usageMetadata = conversion.UsageMetadata
			logger.Info("AI summarization without cache succeeded",
				zap.Int("summaryLength", len(summary)))
		}
	}

	// Pipeline fallback if AI failed or not available
	if summary == "" {
		logger.Info("Using pipeline for summary generation")

		// Use configured pipeline or fall back to default
		usePipeline := param.FallbackPipeline
		if usePipeline.ID == "" {
			usePipeline = pipeline.GenerateSummaryPipeline
		}

		pipelineSummary, err := pipeline.GenerateSummaryPipe(authCtx, w.pipelineClient, contentStr, param.FileType)
		if err != nil {
			logger.Error("Pipeline summary generation failed", zap.Error(err))
			return nil, err
		}
		summary = pipelineSummary
		pipelineName = usePipeline.Name()
	}

	result.Summary = summary
	result.Pipeline = pipelineName
	result.UsageMetadata = usageMetadata

	logger.Info("Summary generated successfully",
		zap.Int("summaryLength", len(summary)),
		zap.String("pipeline", pipelineName))

	// Phase 2: Save summary to database
	if err := w.SaveSummaryActivity(ctx, &SaveSummaryActivityParam{
		FileUID:  param.FileUID,
		Summary:  summary,
		Pipeline: pipelineName,
	}); err != nil {
		logger.Error("Failed to save summary to database", zap.Error(err))
		return nil, err
	}

	return result, nil
}

// ===== STORE CHAT CACHE METADATA ACTIVITY =====

// StoreChatCacheMetadataActivityParam defines parameters for storing chat cache metadata in Redis
type StoreChatCacheMetadataActivityParam struct {
	KBUID                types.KBUIDType             // Knowledge base UID
	FileUIDs             []types.FileUIDType         // File UIDs included in the chat cache
	CacheName            string                      // AI cache name (empty if cache creation failed)
	Model                string                      // Model used for caching
	FileCount            int                         // Number of files in cache
	CreateTime           time.Time                   // When cache was created
	ExpireTime           time.Time                   // When cache will expire
	TTL                  time.Duration               // Time-to-live for Redis key
	CachedContextEnabled bool                        // Whether AI cache exists
	FileRefs             []repository.FileContentRef // File references for uncached small files
}

// StoreChatCacheMetadataActivity stores chat cache metadata in Redis
// This enables the Chat API to quickly lookup and use cached contexts
// for files that are still being processed (before embeddings are ready)
func (w *Worker) StoreChatCacheMetadataActivity(
	ctx context.Context,
	param *StoreChatCacheMetadataActivityParam,
) error {
	w.log.Info("StoreChatCacheMetadataActivity: Storing chat cache metadata in Redis",
		zap.String("cacheName", param.CacheName),
		zap.Int("fileCount", param.FileCount),
		zap.Duration("ttl", param.TTL))

	// Convert types.FileUIDType to uuid.UUID for repository layer
	fileUIDs := make([]uuid.UUID, len(param.FileUIDs))
	for i, fuid := range param.FileUIDs {
		fileUIDs[i] = uuid.UUID(fuid)
	}

	metadata := &repository.ChatCacheMetadata{
		CacheName:            param.CacheName,
		Model:                param.Model,
		FileUIDs:             fileUIDs,
		FileCount:            param.FileCount,
		CreateTime:           param.CreateTime,
		ExpireTime:           param.ExpireTime,
		CachedContextEnabled: param.CachedContextEnabled,
		FileContents:         param.FileRefs,
	}

	kbUID := uuid.UUID(param.KBUID)

	if err := w.repository.SetChatCacheMetadata(ctx, kbUID, fileUIDs, metadata, param.TTL); err != nil {
		w.log.Error("StoreChatCacheMetadataActivity: Failed to store metadata",
			zap.Error(err),
			zap.String("cacheName", param.CacheName))
		return err
	}

	w.log.Info("StoreChatCacheMetadataActivity: Successfully stored chat cache metadata",
		zap.String("cacheName", param.CacheName),
		zap.String("kbUID", kbUID.String()),
		zap.Int("fileCount", param.FileCount))

	return nil
}

// DeleteTemporaryConvertedFileActivityParam defines the parameters for DeleteTemporaryConvertedFileActivity
type DeleteTemporaryConvertedFileActivityParam struct {
	Bucket      string // MinIO bucket (usually blob)
	Destination string // MinIO path to temporary converted file
}

// DeleteTemporaryConvertedFileActivity deletes a temporary format-converted file from MinIO
// after it has been used for AI caching and markdown conversion.
//
// Purpose: When processing files like DOCX, GIF, or MKV, we first convert them to AI-friendly
// formats (DOCX→PDF, GIF→PNG, MKV→MP4) using the indexing-convert-file-type pipeline.
// These converted files are temporarily stored in MinIO (e.g., tmp/fileUID/uuid.pdf) and used
// for AI caching and markdown conversion. Once processing is complete, these intermediate files
// are no longer needed and should be cleaned up to avoid storage waste.
//
// Lifecycle:
//  1. Original file: kb-123/file-456/example.docx (kept permanently)
//  2. Temporary converted: tmp/file-456/abc-123.pdf (deleted by this activity)
//  3. Final markdown: kb-123/file-456/converted_example.md (kept permanently)
//
// Note: This is different from DeleteConvertedFileActivity, which deletes the final markdown
// output from both DB and MinIO when a file is removed from the knowledge base.
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
	PositionData    *types.PositionData      // Position metadata (e.g., page mappings)
	Length          []uint32                 // Length of markdown sections
	PipelineRelease pipeline.PipelineRelease // Pipeline used (empty if AI was used)
	UsageMetadata   any                      // Token usage metadata from AI provider (nil if pipeline was used)
}

// ProcessAIConversionResult processes AI conversion output by parsing page tags and preparing the result
// Returns markdown WITH page tags preserved, position data for visual grounding, and length array
// (Exported for EE worker overrides)
func ProcessAIConversionResult(markdown string, logger *zap.Logger, logContext map[string]any) (string, *types.PositionData, []uint32) {
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
		logger.Info("ConvertToFileActivity: Multi-page document detected with [Page: X] tags", logFields...)
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

// convertUsingPipeline converts a file using the indexing-convert-file-type pipeline
func (w *Worker) convertUsingPipeline(ctx context.Context, content []byte, sourceType artifactpb.FileType, targetFormat string, pipelines []pipeline.PipelineRelease) ([]byte, pipeline.PipelineRelease, error) {
	if len(pipelines) == 0 {
		return nil, pipeline.PipelineRelease{}, fmt.Errorf("no pipelines provided")
	}

	// Use the first available pipeline (indexing-convert-file-type)
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
	standardizeFileTypeActivityError            = "StandardizeFileTypeActivity"
	cacheContextActivityError                   = "CacheFileContextActivity"
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
