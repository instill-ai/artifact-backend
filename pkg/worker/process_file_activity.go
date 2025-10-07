package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/gofrs/uuid"
	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"
	"gorm.io/gorm"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/minio"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"

	errorsx "github.com/instill-ai/x/errors"
)

// This file contains file processing activities used by ProcessFileWorkflow:
// - GetFileMetadataActivity - Retrieves file information and pipeline configuration
// - GenerateSummaryFromPipelineActivity - Generates document summaries using AI pipelines
// - ChunkContentActivity - Splits document content into manageable chunks
// - SaveChunksToDBActivity - Persists chunks and their metadata to database
// - UpdateChunkingMetadataActivity - Updates chunking statistics and pipeline info

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
	FileUID          uuid.UUID
	KnowledgeBaseUID uuid.UUID
}

// GetFileMetadataActivityResult contains file and KB configuration
type GetFileMetadataActivityResult struct {
	File                 *repository.KnowledgeBaseFile
	ConvertingPipelines  []service.PipelineRelease
	SummarizingPipelines []service.PipelineRelease
	ChunkingPipelines    []service.PipelineRelease
	EmbeddingPipelines   []service.PipelineRelease
	ExternalMetadata     *structpb.Struct
}

// GetFileContentActivityParam for retrieving file content from MinIO
type GetFileContentActivityParam struct {
	Bucket      string
	Destination string
	Metadata    *structpb.Struct // For authentication context
}

// GetFileMetadataActivity retrieves file and knowledge base metadata from DB
// This is a single DB read operation - idempotent
func (w *Worker) GetFileMetadataActivity(ctx context.Context, param *GetFileMetadataActivityParam) (*GetFileMetadataActivityResult, error) {
	w.log.Info("GetFileMetadataActivity: Fetching file and KB metadata",
		zap.String("fileUID", param.FileUID.String()))

	// Get file metadata
	files, err := w.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{param.FileUID})
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
	kb, err := w.service.Repository().GetKnowledgeBaseByUID(ctx, param.KnowledgeBaseUID)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			getFileMetadataActivityError,
			err,
		)
	}

	// Build converting pipelines list
	convertingPipelines := make([]service.PipelineRelease, 0, len(kb.ConvertingPipelines)+1)

	// Add file-specific pipeline if exists
	if file.ExtraMetaDataUnmarshal != nil && file.ExtraMetaDataUnmarshal.ConvertingPipe != "" {
		pipeline, err := service.PipelineReleaseFromName(file.ExtraMetaDataUnmarshal.ConvertingPipe)
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
		pipeline, err := service.PipelineReleaseFromName(pipelineName)
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
		convertingPipelines = service.DefaultConversionPipelines
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

	content, err := w.service.MinIO().GetFile(authCtx, param.Bucket, param.Destination)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to retrieve file from storage: %s", errorsx.MessageOrErr(err)),
			getFileContentActivityError,
			err,
		)
	}

	w.log.Info("GetFileContentActivity: File retrieved successfully",
		zap.Int("contentSize", len(content)))
	return content, nil
}

// ===== FILE MANAGEMENT ACTIVITIES =====

// CleanupOldConvertedFileActivityParam for removing old converted files
type CleanupOldConvertedFileActivityParam struct {
	FileUID uuid.UUID
}

// CreateConvertedFileRecordActivityParam for creating a converted file DB record
type CreateConvertedFileRecordActivityParam struct {
	KnowledgeBaseUID uuid.UUID
	FileUID          uuid.UUID
	ConvertedFileUID uuid.UUID
	FileName         string
	Destination      string                   // Known destination (for TEXT/MARKDOWN: original file, for others: determined MinIO path)
	PositionData     *repository.PositionData // Position data from conversion
}

// CreateConvertedFileRecordActivityResult returns the created converted file UID
type CreateConvertedFileRecordActivityResult struct {
	ConvertedFileUID uuid.UUID
}

// UploadConvertedFileToMinIOActivityParam for uploading converted file to MinIO
type UploadConvertedFileToMinIOActivityParam struct {
	KnowledgeBaseUID uuid.UUID
	FileUID          uuid.UUID
	ConvertedFileUID uuid.UUID
	Content          string // Markdown content to upload
}

// UploadConvertedFileToMinIOActivityResult returns the actual MinIO destination
type UploadConvertedFileToMinIOActivityResult struct {
	Destination string
}

// DeleteConvertedFileRecordActivityParam for deleting a converted file DB record (compensating transaction)
type DeleteConvertedFileRecordActivityParam struct {
	ConvertedFileUID uuid.UUID
}

// DeleteConvertedFileFromMinIOActivityParam for deleting a converted file from MinIO (compensating transaction)
type DeleteConvertedFileFromMinIOActivityParam struct {
	Bucket      string
	Destination string
}

// UpdateConvertedFileDestinationActivityParam for updating the converted file destination in DB
type UpdateConvertedFileDestinationActivityParam struct {
	ConvertedFileUID uuid.UUID
	Destination      string
}

// UpdateConversionMetadataActivityParam for updating file metadata after conversion
type UpdateConversionMetadataActivityParam struct {
	FileUID  uuid.UUID
	Length   []uint32
	Pipeline string
}

// CleanupOldConvertedFileActivity removes old converted file from MinIO if it exists
// This is a single MinIO delete operation - idempotent (delete is naturally idempotent)
func (w *Worker) CleanupOldConvertedFileActivity(ctx context.Context, param *CleanupOldConvertedFileActivityParam) error {
	w.log.Info("CleanupOldConvertedFileActivity: Checking for old converted file",
		zap.String("fileUID", param.FileUID.String()))

	// Check if old converted file exists
	oldConvertedFile, err := w.service.Repository().GetConvertedFileByFileUID(ctx, param.FileUID)
	if err != nil || oldConvertedFile == nil || oldConvertedFile.Destination == "" {
		// No old file to clean up - this is fine
		w.log.Info("CleanupOldConvertedFileActivity: No old converted file found")
		return nil
	}

	w.log.Info("CleanupOldConvertedFileActivity: Deleting old converted file",
		zap.String("oldConvertedFileUID", oldConvertedFile.UID.String()),
		zap.String("destination", oldConvertedFile.Destination))

	// Delete from MinIO
	// Note: MinIO DeleteFile is idempotent - if file doesn't exist, it succeeds.
	// Any error returned here is a real error (network, permissions, etc.) that should be retried.
	err = w.service.MinIO().DeleteFile(ctx, config.Config.Minio.BucketName, oldConvertedFile.Destination)
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

	w.log.Info("CleanupOldConvertedFileActivity: Old file deleted successfully")
	return nil
}

// CreateConvertedFileRecordActivity creates a DB record for the converted file
// This is a separate activity from the MinIO upload to properly decouple DB and storage operations
// Returns the created converted file UID
func (w *Worker) CreateConvertedFileRecordActivity(ctx context.Context, param *CreateConvertedFileRecordActivityParam) (*CreateConvertedFileRecordActivityResult, error) {
	w.log.Info("CreateConvertedFileRecordActivity: Creating DB record",
		zap.String("fileUID", param.FileUID.String()),
		zap.String("convertedFileUID", param.ConvertedFileUID.String()))

	convertedFile := repository.ConvertedFile{
		UID:          param.ConvertedFileUID,
		KbUID:        param.KnowledgeBaseUID,
		FileUID:      param.FileUID,
		Name:         param.FileName,
		Type:         "text/markdown",
		Destination:  param.Destination,
		PositionData: param.PositionData,
	}

	createdFile, err := w.service.Repository().CreateConvertedFileWithDestination(ctx, convertedFile)
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

	w.log.Info("CreateConvertedFileRecordActivity: DB record created successfully",
		zap.String("convertedFileUID", createdFile.UID.String()))

	return &CreateConvertedFileRecordActivityResult{
		ConvertedFileUID: createdFile.UID,
	}, nil
}

// UploadConvertedFileToMinIOActivity uploads the converted file content to MinIO
// This is a separate activity from the DB record creation to properly decouple operations
func (w *Worker) UploadConvertedFileToMinIOActivity(ctx context.Context, param *UploadConvertedFileToMinIOActivityParam) (*UploadConvertedFileToMinIOActivityResult, error) {
	w.log.Info("UploadConvertedFileToMinIOActivity: Uploading to MinIO",
		zap.String("convertedFileUID", param.ConvertedFileUID.String()))

	blobStorage := w.service.MinIO()
	destination, err := blobStorage.SaveConvertedFile(
		ctx,
		param.KnowledgeBaseUID,
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

	w.log.Info("UploadConvertedFileToMinIOActivity: Upload successful",
		zap.String("destination", destination))

	return &UploadConvertedFileToMinIOActivityResult{
		Destination: destination,
	}, nil
}

// DeleteConvertedFileRecordActivity deletes a converted file DB record
// This is used as a compensating transaction when MinIO upload fails
func (w *Worker) DeleteConvertedFileRecordActivity(ctx context.Context, param *DeleteConvertedFileRecordActivityParam) error {
	w.log.Info("DeleteConvertedFileRecordActivity: Deleting DB record",
		zap.String("convertedFileUID", param.ConvertedFileUID.String()))

	err := w.service.Repository().DeleteConvertedFile(ctx, param.ConvertedFileUID)
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

	w.log.Info("DeleteConvertedFileRecordActivity: DB record deleted successfully")
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
	err := w.service.Repository().UpdateConvertedFile(ctx, param.ConvertedFileUID, update)
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

	w.log.Info("UpdateConvertedFileDestinationActivity: Destination updated successfully")
	return nil
}

// DeleteConvertedFileFromMinIOActivity deletes a converted file from MinIO
// This is used as a compensating transaction when DB operations fail after successful upload
func (w *Worker) DeleteConvertedFileFromMinIOActivity(ctx context.Context, param *DeleteConvertedFileFromMinIOActivityParam) error {
	w.log.Info("DeleteConvertedFileFromMinIOActivity: Deleting file from MinIO",
		zap.String("destination", param.Destination))

	blobStorage := w.service.MinIO()
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

	w.log.Info("DeleteConvertedFileFromMinIOActivity: File deleted successfully from MinIO")
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

	err := w.service.Repository().UpdateKBFileMetadata(ctx, param.FileUID, mdUpdate)
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

	w.log.Info("UpdateConversionMetadataActivity: Metadata updated successfully")
	return nil
}

// ===== CHUNKING ACTIVITIES =====

// GetConvertedFileForChunkingActivityParam retrieves converted file for chunking
type GetConvertedFileForChunkingActivityParam struct {
	FileUID          uuid.UUID
	KnowledgeBaseUID uuid.UUID
}

// GetConvertedFileForChunkingActivityResult contains file metadata and content
type GetConvertedFileForChunkingActivityResult struct {
	Content            string
	ChunkingPipelines  []service.PipelineRelease
	EmbeddingPipelines []service.PipelineRelease
	ExternalMetadata   *structpb.Struct
}

// ChunkContentActivityParam for external chunking pipeline call
type ChunkContentActivityParam struct {
	FileUID          uuid.UUID
	KnowledgeBaseUID uuid.UUID
	Content          string
	Pipelines        []service.PipelineRelease
	Metadata         *structpb.Struct
}

// ChunkContentActivityResult contains chunked content
type ChunkContentActivityResult struct {
	Chunks          []service.Chunk
	PipelineRelease service.PipelineRelease
}

// SaveChunksToDBActivityParam for saving chunks to database
type SaveChunksToDBActivityParam struct {
	FileUID          uuid.UUID
	KnowledgeBaseUID uuid.UUID
	Chunks           []service.Chunk
}

// SaveChunksToDBActivityResult contains saved chunk UIDs
type SaveChunksToDBActivityResult struct {
	ChunkUIDs []uuid.UUID
}

// UpdateChunkingMetadataActivityParam for updating metadata after chunking
type UpdateChunkingMetadataActivityParam struct {
	FileUID          uuid.UUID
	KnowledgeBaseUID uuid.UUID
	ChunkingPipeline string
	ChunkCount       uint32
}

// GetConvertedFileForChunkingActivity retrieves converted file content for chunking
func (w *Worker) GetConvertedFileForChunkingActivity(ctx context.Context, param *GetConvertedFileForChunkingActivityParam) (*GetConvertedFileForChunkingActivityResult, error) {
	w.log.Info("GetConvertedFileForChunkingActivity: Fetching converted file",
		zap.String("fileUID", param.FileUID.String()))

	// Get converted file from DB
	convertedFile, err := w.service.Repository().GetConvertedFileByFileUID(ctx, param.FileUID)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to get converted file: %s", errorsx.MessageOrErr(err)),
			getConvertedFileForChunkingActivityError,
			err,
		)
	}

	// Get content from MinIO
	bucket := minio.BucketFromDestination(convertedFile.Destination)
	content, err := w.service.MinIO().GetFile(ctx, bucket, convertedFile.Destination)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to get content from storage: %s", errorsx.MessageOrErr(err)),
			getConvertedFileForChunkingActivityError,
			err,
		)
	}

	// Get file for external metadata
	file, err := getFileByUID(ctx, w.service.Repository(), param.FileUID)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to get file metadata: %s", errorsx.MessageOrErr(err)),
			getConvertedFileForChunkingActivityError,
			err,
		)
	}

	// TODO: Build chunking and embedding pipelines from KB configuration
	// For now, these will be passed from the workflow
	chunkingPipelines := make([]service.PipelineRelease, 0)
	embeddingPipelines := make([]service.PipelineRelease, 0)

	w.log.Info("GetConvertedFileForChunkingActivity: Content retrieved successfully",
		zap.Int("contentSize", len(content)))

	return &GetConvertedFileForChunkingActivityResult{
		Content:            string(content),
		ChunkingPipelines:  chunkingPipelines,
		EmbeddingPipelines: embeddingPipelines,
		ExternalMetadata:   file.ExternalMetadataUnmarshal,
	}, nil
}

// ChunkContentActivity calls external pipeline to chunk content
func (w *Worker) ChunkContentActivity(ctx context.Context, param *ChunkContentActivityParam) (*ChunkContentActivityResult, error) {
	w.log.Info("ChunkContentActivity: Chunking content",
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
	chunkingResult, err := w.service.ChunkMarkdownPipe(authCtx, param.Content)
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

// SaveChunksToDBActivity saves chunks to database
func (w *Worker) SaveChunksToDBActivity(ctx context.Context, param *SaveChunksToDBActivityParam) (*SaveChunksToDBActivityResult, error) {
	w.log.Info("SaveChunksToDBActivity: Saving chunks to database",
		zap.String("fileUID", param.FileUID.String()),
		zap.Int("chunkCount", len(param.Chunks)))

	// Delete old chunks (for reprocessing)
	err := w.service.DeleteTextChunksByFileUID(ctx, param.KnowledgeBaseUID, param.FileUID)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to delete old chunks: %s", errorsx.MessageOrErr(err)),
			saveChunksToDBActivityError,
			err,
		)
	}

	// Get converted file for source information
	convertedFile, err := w.service.Repository().GetConvertedFileByFileUID(ctx, param.FileUID)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to get converted file: %s", errorsx.MessageOrErr(err)),
			saveChunksToDBActivityError,
			err,
		)
	}

	// Add page references to chunks if position data is available
	chunksWithReferences := param.Chunks
	if convertedFile.PositionData != nil && len(convertedFile.PositionData.PageDelimiters) > 0 {
		w.log.Info("SaveChunksToDBActivity: Adding page references to chunks",
			zap.Int("delimiterCount", len(convertedFile.PositionData.PageDelimiters)))

		chunksWithReferences = make([]service.Chunk, len(param.Chunks))
		copy(chunksWithReferences, param.Chunks)

		for i, chunk := range chunksWithReferences {
			pageStart, pageEnd := extractPageReferences(
				uint32(chunk.Start),
				uint32(chunk.End),
				convertedFile.PositionData.PageDelimiters,
			)

			if pageStart != 0 && pageEnd != 0 {
				chunksWithReferences[i].Reference = &repository.ChunkReference{
					PageRange: [2]uint32{pageStart, pageEnd},
				}
				w.log.Debug("Added page reference",
					zap.Int("chunkIndex", i),
					zap.Uint32("pageStart", pageStart),
					zap.Uint32("pageEnd", pageEnd))
			}
		}
	}

	// Save chunks using helper function
	chunksToSave, err := saveChunksToDBOnly(
		ctx,
		w.service.Repository(),
		w.service.MinIO(),
		param.KnowledgeBaseUID,
		param.FileUID,
		convertedFile.UID,
		w.service.Repository().ConvertedFileTableName(),
		nil, // No summary chunks
		chunksWithReferences,
		"text/markdown",
	)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to save chunks: %s", errorsx.MessageOrErr(err)),
			saveChunksToDBActivityError,
			err,
		)
	}

	w.log.Info("SaveChunksToDBActivity: Chunks saved successfully",
		zap.Int("chunksToSave", len(chunksToSave)))

	return &SaveChunksToDBActivityResult{
		ChunkUIDs: nil,
	}, nil
}

// UpdateChunkingMetadataActivity updates file metadata after chunking
func (w *Worker) UpdateChunkingMetadataActivity(ctx context.Context, param *UpdateChunkingMetadataActivityParam) error {
	w.log.Info("UpdateChunkingMetadataActivity: Updating file metadata",
		zap.String("fileUID", param.FileUID.String()))

	mdUpdate := repository.ExtraMetaData{
		ChunkingPipe: param.ChunkingPipeline,
	}

	err := w.service.Repository().UpdateKBFileMetadata(ctx, param.FileUID, mdUpdate)
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

	w.log.Info("UpdateChunkingMetadataActivity: Metadata updated successfully")
	return nil
}

// ===== SUMMARY ACTIVITIES =====

// GenerateSummaryFromPipelineActivityParam for external summarization pipeline call
type GenerateSummaryFromPipelineActivityParam struct {
	FileUID          uuid.UUID
	KnowledgeBaseUID uuid.UUID
	Bucket           string
	Destination      string
	FileName         string
	FileType         string
	Metadata         *structpb.Struct
}

// GenerateSummaryFromPipelineActivityResult contains the generated summary
type GenerateSummaryFromPipelineActivityResult struct {
	Summary  string
	Pipeline string
}

// SaveSummaryActivityParam saves summary to database
type SaveSummaryActivityParam struct {
	FileUID          uuid.UUID
	KnowledgeBaseUID uuid.UUID
	Summary          string
	Pipeline         string
}

// GenerateSummaryFromPipelineActivity calls external pipeline to generate summary
func (w *Worker) GenerateSummaryFromPipelineActivity(ctx context.Context, param *GenerateSummaryFromPipelineActivityParam) (*GenerateSummaryFromPipelineActivityResult, error) {
	w.log.Info("GenerateSummaryFromPipelineActivity: Generating summary",
		zap.String("fileUID", param.FileUID.String()))

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = createAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			return nil, temporal.NewApplicationErrorWithCause(
				fmt.Sprintf("Failed to create authenticated context: %s", errorsx.MessageOrErr(err)),
				generateSummaryFromPipelineActivityError,
				err,
			)
		}
	}

	// Fetch content from MinIO
	content, err := w.service.MinIO().GetFile(authCtx, param.Bucket, param.Destination)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to retrieve file from storage: %s", errorsx.MessageOrErr(err)),
			generateSummaryFromPipelineActivityError,
			err,
		)
	}

	// Sanitize content to ensure valid UTF-8
	// This prevents gRPC marshaling errors when content contains invalid UTF-8 sequences
	// (common with certain PDF conversions or binary data)
	contentStr := sanitizeUTF8(content)

	// Log content stats for debugging
	w.log.Info("GenerateSummaryFromPipelineActivity: Content prepared",
		zap.String("fileUID", param.FileUID.String()),
		zap.Int("originalBytes", len(content)),
		zap.Int("sanitizedBytes", len(contentStr)),
		zap.Int("sanitizedRunes", len([]rune(contentStr))),
		zap.Bool("isEmpty", len(strings.TrimSpace(contentStr)) == 0))

	// Verify content is not empty after sanitization
	if len(strings.TrimSpace(contentStr)) == 0 {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Content is empty after UTF-8 sanitization for file %s", param.FileUID.String()),
			generateSummaryFromPipelineActivityError,
			fmt.Errorf("empty content after sanitization"),
		)
	}

	// Call the summarization pipeline
	summary, err := w.service.GenerateSummary(authCtx, contentStr, param.FileType)
	if err != nil {
		return nil, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Summary generation failed: %s", errorsx.MessageOrErr(err)),
			generateSummaryFromPipelineActivityError,
			err,
		)
	}

	w.log.Info("GenerateSummaryFromPipelineActivity: Summary generated successfully",
		zap.Int("summaryLength", len(summary)))

	return &GenerateSummaryFromPipelineActivityResult{
		Summary:  summary,
		Pipeline: service.GenerateSummaryPipeline.Name(),
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

	_, err := w.service.Repository().UpdateKnowledgeBaseFile(ctx, param.FileUID.String(), updateMap)
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
	err = w.service.Repository().UpdateKBFileMetadata(ctx, param.FileUID, mdUpdate)
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

	w.log.Info("SaveSummaryActivity: Summary saved successfully")
	return nil
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
	saveChunksToDBActivityError                 = "SaveChunksToDBActivity"
	updateChunkingMetadataActivityError         = "UpdateChunkingMetadataActivity"
	embedChunksActivityError                    = "EmbedChunksActivity"
	saveEmbeddingsToMilvusActivityError         = "SaveEmbeddingsToMilvusActivity"
	generateSummaryFromPipelineActivityError    = "GenerateSummaryFromPipelineActivity"
	saveSummaryActivityError                    = "SaveSummaryActivity"
)
