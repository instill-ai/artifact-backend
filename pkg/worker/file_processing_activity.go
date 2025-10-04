package worker

import (
	"context"
	"encoding/base64"
	"fmt"
	"unicode/utf8"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/minio"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// extractPageReferences extracts the location of a chunk (defined by its start
// and end byte positions) in a document (defined by the byte delimiters of its
// pages). The function handles edge cases where chunk boundaries are exactly at
// page delimiters or extend beyond the last page.
func extractPageReferences(chunkStart, chunkEnd uint32, pageDelimiters []uint32) (pageStart, pageEnd uint32) {
	if len(pageDelimiters) == 0 {
		return 0, 0
	}

	// delimiter is the first byte of the next page.
	for i, delimiter := range pageDelimiters {
		if chunkStart < delimiter && pageStart == 0 {
			pageStart = uint32(i + 1)
		}

		// Use <= to handle chunks that end exactly at a page delimiter
		if chunkEnd <= delimiter && pageEnd == 0 {
			pageEnd = uint32(i + 1)
		}

		if pageStart != 0 && pageEnd != 0 {
			break
		}
	}

	// Handle edge case: if chunkEnd extends beyond all delimiters,
	// it belongs to the last page
	if pageEnd == 0 {
		pageEnd = uint32(len(pageDelimiters))
	}

	// Handle edge case: if chunkStart is beyond all delimiters,
	// it belongs to the last page (shouldn't happen in normal cases)
	if pageStart == 0 {
		pageStart = uint32(len(pageDelimiters))
	}

	return
}

// ===== CONVERSION ACTIVITIES =====

// ProcessWaitingFileActivityParam defines the parameters for the ProcessWaitingFileActivity
type ProcessWaitingFileActivityParam struct {
	FileUID          uuid.UUID
	KnowledgeBaseUID uuid.UUID
	UserUID          uuid.UUID
	RequesterUID     uuid.UUID
}

// GetFileMetadataActivityParam for retrieving file and KB metadata
type GetFileMetadataActivityParam struct {
	FileUID          uuid.UUID
	KnowledgeBaseUID uuid.UUID
}

// GetFileMetadataActivityResult contains file and KB configuration
type GetFileMetadataActivityResult struct {
	File                *repository.KnowledgeBaseFile
	ConvertingPipelines []service.PipelineRelease
	ExternalMetadata    *structpb.Struct
}

// GetFileContentActivityParam for retrieving file content from MinIO
type GetFileContentActivityParam struct {
	Bucket      string
	Destination string
	Metadata    *structpb.Struct // For authentication context
}

// ConvertToMarkdownActivityParam for external pipeline conversion call
type ConvertToMarkdownActivityParam struct {
	Content   []byte
	FileType  artifactpb.FileType
	Pipelines []service.PipelineRelease
	Metadata  *structpb.Struct // For authentication context
}

// ConvertToMarkdownActivityResult contains the conversion result
type ConvertToMarkdownActivityResult struct {
	Markdown        string
	PositionData    *repository.PositionData
	Length          []uint32
	PipelineRelease service.PipelineRelease
}

// CleanupOldConvertedFileActivityParam for removing old converted files
type CleanupOldConvertedFileActivityParam struct {
	FileUID uuid.UUID
}

// SaveConvertedFileActivityParam for saving converted file to DB + MinIO
type SaveConvertedFileActivityParam struct {
	KnowledgeBaseUID uuid.UUID
	FileUID          uuid.UUID
	FileName         string
	ConversionResult *ConvertToMarkdownActivityResult
}

// UpdateConversionMetadataActivityParam for updating file metadata after conversion
type UpdateConversionMetadataActivityParam struct {
	FileUID  uuid.UUID
	Length   []uint32
	Pipeline string
}

// GetFileMetadataActivity retrieves file and knowledge base metadata from DB
// This is a single DB read operation - idempotent
func (w *Worker) GetFileMetadataActivity(ctx context.Context, param *GetFileMetadataActivityParam) (*GetFileMetadataActivityResult, error) {
	w.log.Info("GetFileMetadataActivity: Fetching file and KB metadata",
		zap.String("fileUID", param.FileUID.String()))

	// Get file metadata
	files, err := w.repository.GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{param.FileUID})
	if err != nil {
		return nil, fmt.Errorf("failed to get file: %w", err)
	}
	if len(files) == 0 {
		// File was deleted during processing - this is OK in scenarios like:
		// - Catalog/file deletion triggered while workflow is running
		// - Test cleanup happening concurrently with processing
		// Return a non-retryable error to exit the workflow gracefully
		w.log.Info("GetFileMetadataActivity: File not found (may have been deleted during processing)",
			zap.String("fileUID", param.FileUID.String()))
		return nil, fmt.Errorf("file not found: %s", param.FileUID.String())
	}
	file := files[0]

	// Get knowledge base configuration
	kb, err := w.repository.GetKnowledgeBaseByUID(ctx, param.KnowledgeBaseUID)
	if err != nil {
		return nil, fmt.Errorf("failed to get knowledge base: %w", err)
	}

	// Build converting pipelines list
	convertingPipelines := make([]service.PipelineRelease, 0, len(kb.ConvertingPipelines)+1)

	// Add file-specific pipeline if exists
	if file.ExtraMetaDataUnmarshal != nil && file.ExtraMetaDataUnmarshal.ConvertingPipe != "" {
		pipeline, err := service.PipelineReleaseFromName(file.ExtraMetaDataUnmarshal.ConvertingPipe)
		if err != nil {
			return nil, fmt.Errorf("failed to parse file pipeline: %w", err)
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
		pipeline, err := service.PipelineReleaseFromName(pipelineName)
		if err != nil {
			return nil, fmt.Errorf("failed to parse catalog pipeline: %w", err)
		}
		convertingPipelines = append(convertingPipelines, pipeline)
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
			w.log.Warn("Failed to create authenticated context, using original", zap.Error(err))
			authCtx = ctx
		}
	}

	content, err := w.service.MinIO().GetFile(authCtx, param.Bucket, param.Destination)
	if err != nil {
		return nil, fmt.Errorf("failed to get file from MinIO: %w", err)
	}

	w.log.Info("GetFileContentActivity: File retrieved successfully",
		zap.Int("contentSize", len(content)))
	return content, nil
}

// ConvertToMarkdownActivity calls external pipeline to convert file to markdown
// This is a single external API call - idempotent (pipeline should be idempotent)
func (w *Worker) ConvertToMarkdownActivity(ctx context.Context, param *ConvertToMarkdownActivityParam) (*ConvertToMarkdownActivityResult, error) {
	w.log.Info("ConvertToMarkdownActivity: Converting file to markdown",
		zap.String("fileType", param.FileType.String()),
		zap.Int("pipelineCount", len(param.Pipelines)))

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = createAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			w.log.Warn("Failed to create authenticated context, using original", zap.Error(err))
			authCtx = ctx
		}
	}

	// Call the conversion pipeline - THIS IS THE KEY EXTERNAL CALL
	conversion, err := w.service.ConvertToMDPipe(authCtx, service.MDConversionParams{
		Base64Content: base64.StdEncoding.EncodeToString(param.Content),
		Type:          param.FileType,
		Pipelines:     param.Pipelines,
	})
	if err != nil {
		return nil, fmt.Errorf("pipeline conversion failed: %w", err)
	}

	w.log.Info("ConvertToMarkdownActivity: Conversion successful",
		zap.Int("markdownLength", len(conversion.Markdown)))

	return &ConvertToMarkdownActivityResult{
		Markdown:        conversion.Markdown,
		PositionData:    conversion.PositionData,
		Length:          conversion.Length,
		PipelineRelease: conversion.PipelineRelease,
	}, nil
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
		w.log.Info("CleanupOldConvertedFileActivity: No old converted file found")
		return nil
	}

	w.log.Info("CleanupOldConvertedFileActivity: Deleting old converted file",
		zap.String("oldConvertedFileUID", oldConvertedFile.UID.String()),
		zap.String("destination", oldConvertedFile.Destination))

	// Delete from MinIO
	err = w.service.MinIO().DeleteFile(ctx, config.Config.Minio.BucketName, oldConvertedFile.Destination)
	if err != nil {
		// Log warning but don't fail - orphaned files are acceptable
		w.log.Warn("Failed to delete old converted file from MinIO (continuing)",
			zap.String("destination", oldConvertedFile.Destination),
			zap.Error(err))
		return nil // Don't fail the activity
	}

	w.log.Info("CleanupOldConvertedFileActivity: Old file deleted successfully")
	return nil
}

// SaveConvertedFileActivity saves converted file to DB and MinIO
// This is a single transactional operation (DB + MinIO save is handled atomically by repository)
func (w *Worker) SaveConvertedFileActivity(ctx context.Context, param *SaveConvertedFileActivityParam) error {
	w.log.Info("SaveConvertedFileActivity: Saving converted file",
		zap.String("fileUID", param.FileUID.String()))

	// Use the helper function from common.go which handles DB + MinIO save atomically
	err := saveConvertedFile(ctx, w.service, param.KnowledgeBaseUID, param.FileUID, param.FileName, &service.MDConversionResult{
		Markdown:        param.ConversionResult.Markdown,
		PositionData:    param.ConversionResult.PositionData,
		Length:          param.ConversionResult.Length,
		PipelineRelease: param.ConversionResult.PipelineRelease,
	})
	if err != nil {
		return fmt.Errorf("failed to save converted file: %w", err)
	}

	w.log.Info("SaveConvertedFileActivity: Converted file saved successfully")
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

	err := w.repository.UpdateKBFileMetadata(ctx, param.FileUID, mdUpdate)
	if err != nil {
		// If file not found, it may have been deleted during processing - this is OK
		if err.Error() == "record not found" || err.Error() == "fetching file: record not found" {
			w.log.Info("UpdateConversionMetadataActivity: File not found (may have been deleted), skipping metadata update",
				zap.String("fileUID", param.FileUID.String()))
			return nil
		}
		return fmt.Errorf("failed to update file metadata: %w", err)
	}

	w.log.Info("UpdateConversionMetadataActivity: Metadata updated successfully")
	return nil
}

// ===== CHUNKING ACTIVITIES =====

// GetConvertedFileForChunkingActivityParam retrieves converted file for chunking
type GetConvertedFileForChunkingActivityParam struct {
	FileUID uuid.UUID
}

// GetConvertedFileForChunkingActivityResult contains file data and metadata
type GetConvertedFileForChunkingActivityResult struct {
	Content      []byte
	SourceTable  string
	SourceUID    uuid.UUID
	PositionData *repository.PositionData
	Metadata     *structpb.Struct
}

// GetOriginalFileForChunkingActivityParam retrieves original file for chunking (text/markdown)
type GetOriginalFileForChunkingActivityParam struct {
	FileUID     uuid.UUID
	Bucket      string
	Destination string
	Metadata    *structpb.Struct
}

// GetOriginalFileForChunkingActivityResult contains file data
type GetOriginalFileForChunkingActivityResult struct {
	Content     []byte
	SourceTable string
	SourceUID   uuid.UUID
	FileType    string
	Metadata    *structpb.Struct
}

// ChunkContentActivityParam for external chunking pipeline call
type ChunkContentActivityParam struct {
	Content      []byte
	IsMarkdown   bool
	ChunkSize    int
	ChunkOverlap int
	Metadata     *structpb.Struct
}

// ChunkContentActivityResult contains chunked content
type ChunkContentActivityResult struct {
	Chunks []service.Chunk
}

// SaveChunksToDBActivityParam for saving chunks to database with placeholders
type SaveChunksToDBActivityParam struct {
	KnowledgeBaseUID uuid.UUID
	FileUID          uuid.UUID
	SourceUID        uuid.UUID
	SourceTable      string
	SummaryChunks    []service.Chunk
	ContentChunks    []service.Chunk
	FileType         string
}

// SaveChunksToDBActivityResult contains chunks that need MinIO save
type SaveChunksToDBActivityResult struct {
	ChunksToSave map[string][]byte
}

// UpdateChunkingMetadataActivityParam for updating metadata after chunking
type UpdateChunkingMetadataActivityParam struct {
	FileUID  uuid.UUID
	Pipeline string
}

// GetConvertedFileForChunkingActivity retrieves converted file for chunking
// This is a DB read + MinIO read operation - idempotent
func (w *Worker) GetConvertedFileForChunkingActivity(ctx context.Context, param *GetConvertedFileForChunkingActivityParam) (*GetConvertedFileForChunkingActivityResult, error) {
	w.log.Info("GetConvertedFileForChunkingActivity: Fetching converted file",
		zap.String("fileUID", param.FileUID.String()))

	// Get converted file metadata from DB
	convertedFile, err := w.repository.GetConvertedFileByFileUID(ctx, param.FileUID)
	if err != nil {
		return nil, fmt.Errorf("failed to get converted file metadata: %w", err)
	}

	// Get file content from MinIO
	fileData, err := w.service.MinIO().GetFile(ctx, config.Config.Minio.BucketName, convertedFile.Destination)
	if err != nil {
		return nil, fmt.Errorf("failed to get converted file from MinIO: %w", err)
	}

	w.log.Info("GetConvertedFileForChunkingActivity: Converted file retrieved",
		zap.Int("contentSize", len(fileData)))

	return &GetConvertedFileForChunkingActivityResult{
		Content:      fileData,
		SourceTable:  w.repository.ConvertedFileTableName(),
		SourceUID:    convertedFile.UID,
		PositionData: convertedFile.PositionData,
	}, nil
}

// GetOriginalFileForChunkingActivity retrieves original file for chunking (text/markdown files)
// This is a single MinIO read operation
func (w *Worker) GetOriginalFileForChunkingActivity(ctx context.Context, param *GetOriginalFileForChunkingActivityParam) (*GetOriginalFileForChunkingActivityResult, error) {
	w.log.Info("GetOriginalFileForChunkingActivity: Fetching original file",
		zap.String("fileUID", param.FileUID.String()))

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = createAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			w.log.Warn("Failed to create authenticated context, using original", zap.Error(err))
			authCtx = ctx
		}
	}

	// Get file content from MinIO
	fileData, err := w.service.MinIO().GetFile(authCtx, param.Bucket, param.Destination)
	if err != nil {
		return nil, fmt.Errorf("failed to get original file from MinIO: %w", err)
	}

	// Get file to determine source UID
	files, err := w.repository.GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{param.FileUID})
	if err != nil || len(files) == 0 {
		return nil, fmt.Errorf("failed to get file metadata: %w", err)
	}
	file := files[0]

	w.log.Info("GetOriginalFileForChunkingActivity: Original file retrieved",
		zap.Int("contentSize", len(fileData)))

	return &GetOriginalFileForChunkingActivityResult{
		Content:     fileData,
		SourceTable: w.repository.KnowledgeBaseFileTableName(),
		SourceUID:   file.UID,
		FileType:    file.Type,
		Metadata:    file.ExternalMetadataUnmarshal,
	}, nil
}

// ChunkContentActivity calls external pipeline to chunk content
// This is a single external API call - idempotent (pipeline should be deterministic)
func (w *Worker) ChunkContentActivity(ctx context.Context, param *ChunkContentActivityParam) (*ChunkContentActivityResult, error) {
	w.log.Info("ChunkContentActivity: Chunking content",
		zap.Bool("isMarkdown", param.IsMarkdown),
		zap.Int("contentSize", len(param.Content)))

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = createAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			w.log.Warn("Failed to create authenticated context, using original", zap.Error(err))
			authCtx = ctx
		}
	}

	// Call the appropriate chunking pipeline
	var chunkingResult *service.ChunkingResult
	var err error
	if param.IsMarkdown {
		chunkingResult, err = w.service.ChunkMarkdownPipe(authCtx, string(param.Content))
	} else {
		chunkingResult, err = w.service.ChunkTextPipe(authCtx, string(param.Content))
	}
	if err != nil {
		return nil, fmt.Errorf("chunking pipeline failed: %w", err)
	}

	w.log.Info("ChunkContentActivity: Chunking successful",
		zap.Int("chunkCount", len(chunkingResult.Chunks)))

	return &ChunkContentActivityResult{
		Chunks: chunkingResult.Chunks,
	}, nil
}

// SaveChunksToDBActivity saves chunks to database with placeholder destinations
// This is a single DB transaction - atomic operation
func (w *Worker) SaveChunksToDBActivity(ctx context.Context, param *SaveChunksToDBActivityParam) (*SaveChunksToDBActivityResult, error) {
	w.log.Info("SaveChunksToDBActivity: Saving chunks to database",
		zap.String("fileUID", param.FileUID.String()),
		zap.Int("summaryChunkCount", len(param.SummaryChunks)),
		zap.Int("contentChunkCount", len(param.ContentChunks)))

	// First, delete old chunks from MinIO (for reprocessing)
	// This is idempotent - if no chunks exist, it's a no-op
	err := w.service.DeleteTextChunksByFileUID(ctx, param.KnowledgeBaseUID, param.FileUID)
	if err != nil {
		// Log warning but don't fail - old chunks might not exist
		w.log.Warn("SaveChunksToDBActivity: Failed to delete old chunks from MinIO (continuing)",
			zap.String("fileUID", param.FileUID.String()),
			zap.Error(err))
	}

	// Use helper function from common.go
	chunksToSave, err := saveChunksToDBOnly(ctx, w.service, w.repository,
		param.KnowledgeBaseUID, param.FileUID, param.SourceUID, param.SourceTable,
		param.SummaryChunks, param.ContentChunks, param.FileType)
	if err != nil {
		return nil, fmt.Errorf("failed to save chunks to DB: %w", err)
	}

	w.log.Info("SaveChunksToDBActivity: Chunks saved to database",
		zap.Int("chunksForMinIO", len(chunksToSave)))

	return &SaveChunksToDBActivityResult{
		ChunksToSave: chunksToSave,
	}, nil
}

// UpdateChunkingMetadataActivity updates file metadata after chunking
// This is a single DB write operation - idempotent
func (w *Worker) UpdateChunkingMetadataActivity(ctx context.Context, param *UpdateChunkingMetadataActivityParam) error {
	w.log.Info("UpdateChunkingMetadataActivity: Updating file metadata",
		zap.String("fileUID", param.FileUID.String()))

	mdUpdate := repository.ExtraMetaData{
		ChunkingPipe: param.Pipeline,
	}

	err := w.repository.UpdateKBFileMetadata(ctx, param.FileUID, mdUpdate)
	if err != nil {
		// If file not found, it may have been deleted during processing - this is OK
		if err.Error() == "record not found" || err.Error() == "fetching file: record not found" {
			w.log.Info("UpdateChunkingMetadataActivity: File not found (may have been deleted), skipping metadata update",
				zap.String("fileUID", param.FileUID.String()))
			return nil
		}
		return fmt.Errorf("failed to update file metadata: %w", err)
	}

	w.log.Info("UpdateChunkingMetadataActivity: Metadata updated successfully")
	return nil
}

// ===== SUMMARY ACTIVITIES =====

// GetFileContentForSummaryActivityParam retrieves content for summarization
type GetFileContentForSummaryActivityParam struct {
	FileUID     uuid.UUID
	Bucket      string
	Destination string
	FileType    string
	Metadata    *structpb.Struct
}

// GetFileContentForSummaryActivityResult contains content to summarize
type GetFileContentForSummaryActivityResult struct {
	Content  []byte
	Metadata *structpb.Struct
}

// GenerateSummaryFromPipelineActivityParam for external summarization pipeline call
type GenerateSummaryFromPipelineActivityParam struct {
	Content     []byte
	FileName    string
	RequesterID string
	Metadata    *structpb.Struct
}

// GenerateSummaryFromPipelineActivityResult contains the generated summary
type GenerateSummaryFromPipelineActivityResult struct {
	Summary  string
	Pipeline string
}

// SaveSummaryActivityParam saves summary to database
type SaveSummaryActivityParam struct {
	FileUID  uuid.UUID
	Summary  string
	Pipeline string
}

// GetFileContentForSummaryActivity retrieves content for summarization
// This is a DB read + MinIO read operation - idempotent
func (w *Worker) GetFileContentForSummaryActivity(ctx context.Context, param *GetFileContentForSummaryActivityParam) (*GetFileContentForSummaryActivityResult, error) {
	w.log.Info("GetFileContentForSummaryActivity: Fetching content",
		zap.String("fileUID", param.FileUID.String()),
		zap.String("fileType", param.FileType))

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = createAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			w.log.Warn("Failed to create authenticated context, using original", zap.Error(err))
			authCtx = ctx
		}
	}

	// For document types, get converted file; for text/markdown, get original
	var content []byte
	var err error

	switch param.FileType {
	case artifactpb.FileType_FILE_TYPE_PDF.String(),
		artifactpb.FileType_FILE_TYPE_DOC.String(),
		artifactpb.FileType_FILE_TYPE_DOCX.String(),
		artifactpb.FileType_FILE_TYPE_PPT.String(),
		artifactpb.FileType_FILE_TYPE_PPTX.String(),
		artifactpb.FileType_FILE_TYPE_HTML.String(),
		artifactpb.FileType_FILE_TYPE_XLSX.String(),
		artifactpb.FileType_FILE_TYPE_XLS.String(),
		artifactpb.FileType_FILE_TYPE_CSV.String():

		// Get converted file
		convertedFile, err := w.repository.GetConvertedFileByFileUID(ctx, param.FileUID)
		if err != nil {
			return nil, fmt.Errorf("failed to get converted file: %w", err)
		}
		content, err = w.service.MinIO().GetFile(authCtx, config.Config.Minio.BucketName, convertedFile.Destination)
		if err != nil {
			return nil, fmt.Errorf("failed to get converted file from MinIO: %w", err)
		}

	default:
		// Get original file
		content, err = w.service.MinIO().GetFile(authCtx, param.Bucket, param.Destination)
		if err != nil {
			return nil, fmt.Errorf("failed to get original file from MinIO: %w", err)
		}
	}

	w.log.Info("GetFileContentForSummaryActivity: Content retrieved",
		zap.Int("contentSize", len(content)))

	return &GetFileContentForSummaryActivityResult{
		Content:  content,
		Metadata: param.Metadata,
	}, nil
}

// GenerateSummaryFromPipelineActivity calls external pipeline to generate summary
// This is a single external API call - idempotent (pipeline should be idempotent)
func (w *Worker) GenerateSummaryFromPipelineActivity(ctx context.Context, param *GenerateSummaryFromPipelineActivityParam) (*GenerateSummaryFromPipelineActivityResult, error) {
	w.log.Info("GenerateSummaryFromPipelineActivity: Generating summary",
		zap.String("fileName", param.FileName))

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = createAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			w.log.Warn("Failed to create authenticated context, using original", zap.Error(err))
			authCtx = ctx
		}
	}

	// Call the summarization pipeline - THIS IS THE KEY EXTERNAL CALL
	summary, err := w.service.GenerateSummary(authCtx, string(param.Content), param.RequesterID)
	if err != nil {
		return nil, fmt.Errorf("summarization pipeline failed: %w", err)
	}

	w.log.Info("GenerateSummaryFromPipelineActivity: Summary generated successfully",
		zap.Int("summaryLength", len(summary)))

	return &GenerateSummaryFromPipelineActivityResult{
		Summary:  summary,
		Pipeline: service.GenerateSummaryPipeline.Name(),
	}, nil
}

// SaveSummaryActivity saves summary to database
// This is a single DB write operation - idempotent
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
		if err.Error() == "record not found" {
			w.log.Info("SaveSummaryActivity: File not found (may have been deleted), skipping summary save",
				zap.String("fileUID", param.FileUID.String()))
			return nil
		}
		return fmt.Errorf("failed to update summary: %w", err)
	}

	// Update metadata
	mdUpdate := repository.ExtraMetaData{
		SummarizingPipe: param.Pipeline,
	}
	err = w.repository.UpdateKBFileMetadata(ctx, param.FileUID, mdUpdate)
	if err != nil {
		// If file not found, it may have been deleted during processing - this is OK
		if err.Error() == "record not found" || err.Error() == "fetching file: record not found" {
			w.log.Info("SaveSummaryActivity: File not found (may have been deleted), skipping metadata update",
				zap.String("fileUID", param.FileUID.String()))
			return nil
		}
		return fmt.Errorf("failed to update metadata: %w", err)
	}

	w.log.Info("SaveSummaryActivity: Summary saved successfully")
	return nil
}

// ProcessWaitingFileActivity determines the next status based on file type
func (w *Worker) ProcessWaitingFileActivity(ctx context.Context, param *ProcessWaitingFileActivityParam) (artifactpb.FileProcessStatus, error) {
	w.log.Info("Processing waiting file", zap.String("fileUID", param.FileUID.String()))

	file, err := getFileByUID(ctx, w.repository, param.FileUID)
	if err != nil {
		return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, err
	}

	var nextStatus artifactpb.FileProcessStatus

	switch file.Type {
	case artifactpb.FileType_FILE_TYPE_PDF.String(),
		artifactpb.FileType_FILE_TYPE_DOC.String(),
		artifactpb.FileType_FILE_TYPE_DOCX.String(),
		artifactpb.FileType_FILE_TYPE_PPT.String(),
		artifactpb.FileType_FILE_TYPE_PPTX.String(),
		artifactpb.FileType_FILE_TYPE_HTML.String(),
		artifactpb.FileType_FILE_TYPE_XLSX.String(),
		artifactpb.FileType_FILE_TYPE_XLS.String(),
		artifactpb.FileType_FILE_TYPE_CSV.String():
		nextStatus = artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CONVERTING

	case artifactpb.FileType_FILE_TYPE_TEXT.String(),
		artifactpb.FileType_FILE_TYPE_MARKDOWN.String():

		bucket := minio.BucketFromDestination(file.Destination)
		data, err := w.service.MinIO().GetFile(ctx, bucket, file.Destination)
		if err != nil {
			return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, fmt.Errorf("fetching file from MinIO: %w", err)
		}
		charCount := utf8.RuneCount(data)
		mdUpdate := repository.ExtraMetaData{
			Length: []uint32{uint32(charCount)},
		}
		if err := w.repository.UpdateKBFileMetadata(ctx, param.FileUID, mdUpdate); err != nil {
			return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, fmt.Errorf("saving length metadata in file record: %w", err)
		}

		nextStatus = artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_SUMMARIZING

	default:
		return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, fmt.Errorf("unsupported file type in ProcessWaitingFileActivity: %v", file.Type)
	}

	w.log.Info("Waiting file processed", zap.String("fileUID", param.FileUID.String()), zap.String("nextStatus", nextStatus.String()))
	return nextStatus, nil
}
