package worker

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/constant"
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

// ConvertFileActivity handles file conversion operations
func (w *worker) ConvertFileActivity(ctx context.Context, param *ConvertFileActivityParam) error {
	w.log.Info("Starting ConvertFileActivity",
		zap.String("fileUID", param.FileUID.String()),
		zap.String("conversionType", param.ConversionType))

	file, err := getFileByUID(ctx, w.repository, param.FileUID)
	if err != nil {
		return err
	}

	authCtx, err := createAuthenticatedContext(ctx, file.ExternalMetadataUnmarshal)
	if err != nil {
		w.log.Warn("Failed to create authenticated context, using original context", zap.Error(err))
		authCtx = ctx
	}

	kb, err := w.repository.GetKnowledgeBaseByUID(ctx, param.KnowledgeBaseUID)
	if err != nil {
		return fmt.Errorf("fetching parent catalog: %w", err)
	}

	// Build converting pipelines list
	convertingPipelines := make([]service.PipelineRelease, 0, len(kb.ConvertingPipelines)+1)

	fileConvertingPipeName := file.ExtraMetaDataUnmarshal.ConvertingPipe
	if fileConvertingPipeName != "" {
		fileConvertingPipeline, err := service.PipelineReleaseFromName(fileConvertingPipeName)
		if err != nil {
			return fmt.Errorf("parsing pipeline name: %w", err)
		}
		convertingPipelines = append(convertingPipelines, fileConvertingPipeline)
	}

	for _, pipelineName := range kb.ConvertingPipelines {
		if len(pipelineName) == 0 || pipelineName == fileConvertingPipeName {
			continue
		}
		catalogConvertingPipeline, err := service.PipelineReleaseFromName(pipelineName)
		if err != nil {
			return fmt.Errorf("parsing pipeline name: %w", err)
		}
		convertingPipelines = append(convertingPipelines, catalogConvertingPipeline)
	}

	bucket := minio.BucketFromDestination(file.Destination)
	data, err := w.service.MinIO().GetFile(authCtx, bucket, file.Destination)
	if err != nil {
		return fmt.Errorf("fetching file from MinIO: %w", err)
	}

	conversion, err := w.service.ConvertToMDPipe(authCtx, service.MDConversionParams{
		Base64Content: base64.StdEncoding.EncodeToString(data),
		Type:          artifactpb.FileType(artifactpb.FileType_value[file.Type]),
		Pipelines:     convertingPipelines,
	})
	if err != nil {
		return fmt.Errorf("converting file to Markdown: %w", err)
	}

	// Save converted file first before updating metadata
	err = saveConvertedFile(ctx, w.service, param.KnowledgeBaseUID, param.FileUID, "converted_"+file.Name, conversion)
	if err != nil {
		return fmt.Errorf("saving converted data: %w", err)
	}

	// Update metadata and process status together after file is saved
	mdUpdate := repository.ExtraMetaData{
		Length:         conversion.Length,
		ConvertingPipe: conversion.PipelineRelease.Name(),
	}
	if err := w.repository.UpdateKBFileMetadata(ctx, param.FileUID, mdUpdate); err != nil {
		return fmt.Errorf("saving conversion metadata in file record: %w", err)
	}

	updateMap := map[string]any{
		repository.KnowledgeBaseFileColumn.ProcessStatus: artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_SUMMARIZING.String(),
	}
	_, err = w.repository.UpdateKnowledgeBaseFile(ctx, param.FileUID.String(), updateMap)
	if err != nil {
		return fmt.Errorf("updating file process status: %w", err)
	}

	w.log.Info("File conversion completed", zap.String("fileUID", param.FileUID.String()))
	return nil
}

// ChunkFileActivity handles file chunking operations
func (w *worker) ChunkFileActivity(ctx context.Context, param *ChunkFileActivityParam) error {
	w.log.Info("Starting ChunkFileActivity",
		zap.String("fileUID", param.FileUID.String()),
		zap.Int("chunkSize", param.ChunkSize),
		zap.Int("chunkOverlap", param.ChunkOverlap))

	file, err := getFileByUID(ctx, w.repository, param.FileUID)
	if err != nil {
		return err
	}

	authCtx, err := createAuthenticatedContext(ctx, file.ExternalMetadataUnmarshal)
	if err != nil {
		w.log.Warn("Failed to create authenticated context, using original context", zap.Error(err))
		authCtx = ctx
	}

	var fileData []byte
	var sourceTable string
	var sourceUID uuid.UUID
	var contentChunks []service.Chunk

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

		convertedFile, err := w.repository.GetConvertedFileByFileUID(ctx, param.FileUID)
		if err != nil {
			return fmt.Errorf("fetching converted file record: %w", err)
		}

		fileData, err = w.service.MinIO().GetFile(authCtx, config.Config.Minio.BucketName, convertedFile.Destination)
		if err != nil {
			return fmt.Errorf("fetching converted file blob: %w", err)
		}

		sourceTable = w.repository.ConvertedFileTableName()
		sourceUID = convertedFile.UID

		chunkingResult, err := w.service.ChunkMarkdownPipe(authCtx, string(fileData))
		if err != nil {
			return fmt.Errorf("chunking converted file: %w", err)
		}
		contentChunks = chunkingResult.Chunks

		if convertedFile.PositionData != nil {
			for i, chunk := range contentChunks {
				pageStart, pageEnd := extractPageReferences(
					uint32(chunk.Start),
					uint32(chunk.End),
					convertedFile.PositionData.PageDelimiters,
				)

				if pageStart != 0 && pageEnd != 0 {
					contentChunks[i].Reference = &repository.ChunkReference{
						PageRange: [2]uint32{pageStart, pageEnd},
					}
				}
			}
		}

	case artifactpb.FileType_FILE_TYPE_MARKDOWN.String():
		bucket := minio.BucketFromDestination(file.Destination)
		fileData, err = w.service.MinIO().GetFile(authCtx, bucket, file.Destination)
		if err != nil {
			return fmt.Errorf("fetching original file blob: %w", err)
		}
		sourceTable = w.repository.KnowledgeBaseFileTableName()
		sourceUID = file.UID

		chunkingResult, err := w.service.ChunkMarkdownPipe(authCtx, string(fileData))
		if err != nil {
			return fmt.Errorf("chunking original file: %w", err)
		}
		contentChunks = chunkingResult.Chunks

	case artifactpb.FileType_FILE_TYPE_TEXT.String():
		bucket := minio.BucketFromDestination(file.Destination)
		fileData, err = w.service.MinIO().GetFile(authCtx, bucket, file.Destination)
		if err != nil {
			return fmt.Errorf("fetching original file blob: %w", err)
		}
		sourceTable = w.repository.KnowledgeBaseFileTableName()
		sourceUID = file.UID

		chunkingResult, err := w.service.ChunkTextPipe(authCtx, string(fileData))
		if err != nil {
			return fmt.Errorf("chunking original file: %w", err)
		}
		contentChunks = chunkingResult.Chunks

	default:
		return fmt.Errorf("unsupported file type in ChunkFileActivity: %v", file.Type)
	}

	summaryChunkingResult, err := w.service.ChunkTextPipe(authCtx, string(file.Summary))
	if err != nil {
		return fmt.Errorf("chunking summary: %w", err)
	}

	err = saveChunks(ctx, w.service, param.KnowledgeBaseUID, param.FileUID, sourceUID, sourceTable, summaryChunkingResult.Chunks, contentChunks, string(constant.DocumentFileType))
	if err != nil {
		return fmt.Errorf("storing chunks: %w", err)
	}

	mdUpdate := repository.ExtraMetaData{
		ChunkingPipe: service.ChunkTextPipeline.Name(),
	}
	if err = w.repository.UpdateKBFileMetadata(ctx, param.FileUID, mdUpdate); err != nil {
		return fmt.Errorf("saving chunking metadata in file record: %w", err)
	}

	// Update status to EMBEDDING
	updateMap := map[string]any{
		repository.KnowledgeBaseFileColumn.ProcessStatus: artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING.String(),
	}
	_, err = w.repository.UpdateKnowledgeBaseFile(ctx, param.FileUID.String(), updateMap)
	if err != nil {
		return fmt.Errorf("updating file process status: %w", err)
	}

	w.log.Info("File chunking completed", zap.String("fileUID", param.FileUID.String()))
	return nil
}

// EmbedFileActivity handles file embedding operations
func (w *worker) EmbedFileActivity(ctx context.Context, param *EmbedFileActivityParam) error {
	w.log.Info("Starting EmbedFileActivity",
		zap.String("fileUID", param.FileUID.String()),
		zap.String("embeddingModel", param.EmbeddingModel))

	file, err := getFileByUID(ctx, w.repository, param.FileUID)
	if err != nil {
		return err
	}

	sourceTable, sourceUID, chunks, _, texts, err := w.service.GetChunksByFile(ctx, &file)
	if err != nil {
		w.log.Error("Failed to get chunks from database first time.", zap.String("SourceUID", sourceUID.String()))
		time.Sleep(1 * time.Second)
		w.log.Info("Retrying to get chunks from database.", zap.String("SourceUID", sourceUID.String()))
		sourceTable, sourceUID, chunks, _, texts, err = w.service.GetChunksByFile(ctx, &file)
		if err != nil {
			w.log.Error("Failed to get chunks from database second time.", zap.String("SourceUID", sourceUID.String()))
			return fmt.Errorf("failed to get chunks: %w", err)
		}
	}

	mdUpdate := repository.ExtraMetaData{
		EmbeddingPipe: service.EmbedTextPipeline.Name(),
	}
	if err = w.repository.UpdateKBFileMetadata(ctx, param.FileUID, mdUpdate); err != nil {
		w.log.Error("Failed to save embedding pipeline metadata.", zap.String("File uid:", param.FileUID.String()))
		return fmt.Errorf("failed to save embedding pipeline metadata: %w", err)
	}

	authCtx, err := createAuthenticatedContext(ctx, file.ExternalMetadataUnmarshal)
	if err != nil {
		w.log.Warn("Failed to create authenticated context, using original context", zap.Error(err))
		authCtx = ctx
	}

	vectors, err := w.service.EmbeddingTextPipe(authCtx, texts)
	if err != nil {
		w.log.Error("Failed to get embeddings from chunks using embedding pipeline", zap.String("SourceTable", sourceTable), zap.String("SourceUID", sourceUID.String()))
		return fmt.Errorf("failed to generate embeddings: %w", err)
	}

	collection := service.KBCollectionName(param.KnowledgeBaseUID)
	embeddings := make([]repository.Embedding, len(vectors))
	for i, v := range vectors {
		embeddings[i] = repository.Embedding{
			SourceTable: w.repository.TextChunkTableName(),
			SourceUID:   chunks[i].UID,
			Vector:      v,
			Collection:  collection,
			KbUID:       param.KnowledgeBaseUID,
			KbFileUID:   param.FileUID,
			FileType:    chunks[i].FileType,
			ContentType: chunks[i].ContentType,
		}
	}

	err = saveEmbeddings(ctx, w.service, param.KnowledgeBaseUID, param.FileUID, embeddings, file.Name)
	if err != nil {
		w.log.Error("Failed to save embeddings into vector database and metadata into database.", zap.String("SourceUID", sourceUID.String()))
		return fmt.Errorf("failed to save embeddings: %w", err)
	}

	w.log.Info("File embedding completed", zap.String("fileUID", param.FileUID.String()))
	return nil
}

// GenerateSummaryActivity generates a summary for the file
func (w *worker) GenerateSummaryActivity(ctx context.Context, param *GenerateSummaryActivityParam) error {
	w.log.Info("Generating summary", zap.String("fileUID", param.FileUID.String()))

	file, err := getFileByUID(ctx, w.repository, param.FileUID)
	if err != nil {
		return err
	}

	authCtx, err := createAuthenticatedContext(ctx, file.ExternalMetadataUnmarshal)
	if err != nil {
		w.log.Warn("Failed to create authenticated context, using original context", zap.Error(err))
		authCtx = ctx
	}

	var fileData []byte

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
		convertedFile, err := w.repository.GetConvertedFileByFileUID(ctx, param.FileUID)
		if err != nil {
			w.log.Error("Failed to get converted file metadata.", zap.String("File uid", param.FileUID.String()))
			return fmt.Errorf("failed to get converted file: %w", err)
		}
		fileData, err = w.service.MinIO().GetFile(ctx, config.Config.Minio.BucketName, convertedFile.Destination)
		if err != nil {
			w.log.Error("Failed to get converted file from minIO.", zap.String("Converted file uid", convertedFile.UID.String()))
			return fmt.Errorf("failed to get converted file from MinIO: %w", err)
		}

	case artifactpb.FileType_FILE_TYPE_TEXT.String(),
		artifactpb.FileType_FILE_TYPE_MARKDOWN.String():
		bucket := minio.BucketFromDestination(file.Destination)
		fileData, err = w.service.MinIO().GetFile(ctx, bucket, file.Destination)
		if err != nil {
			w.log.Error("Failed to get file from minIO.", zap.String("File uid", param.FileUID.String()))
			return fmt.Errorf("failed to get file from MinIO: %w", err)
		}

	default:
		return fmt.Errorf("unsupported file type in GenerateSummaryActivity: %v", file.Type)
	}

	summary, err := w.service.GenerateSummary(authCtx, string(fileData), string(constant.DocumentFileType))
	if err != nil {
		w.log.Error("Failed to generate summary from file.", zap.String("File uid", param.FileUID.String()))
		return fmt.Errorf("failed to generate summary: %w", err)
	}

	mdUpdate := repository.ExtraMetaData{
		SummarizingPipe: service.GenerateSummaryPipeline.Name(),
	}
	if err = w.repository.UpdateKBFileMetadata(ctx, param.FileUID, mdUpdate); err != nil {
		w.log.Error("Failed to save summarizing pipeline metadata.", zap.String("File uid:", param.FileUID.String()))
		return fmt.Errorf("failed to save summarizing pipeline metadata: %w", err)
	}

	updateMap := map[string]any{
		repository.KnowledgeBaseFileColumn.ProcessStatus: artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING.String(),
		repository.KnowledgeBaseFileColumn.Summary:       []byte(summary),
	}
	_, err = w.repository.UpdateKnowledgeBaseFile(ctx, param.FileUID.String(), updateMap)
	if err != nil {
		w.log.Error("Failed to update file status.", zap.String("File uid", param.FileUID.String()))
		return fmt.Errorf("failed to update file status: %w", err)
	}

	w.log.Info("Summary generation completed", zap.String("fileUID", param.FileUID.String()))
	return nil
}

// ProcessWaitingFileActivity determines the next status based on file type
func (w *worker) ProcessWaitingFileActivity(ctx context.Context, param *ProcessWaitingFileActivityParam) (artifactpb.FileProcessStatus, error) {
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

	updateMap := map[string]any{
		repository.KnowledgeBaseFileColumn.ProcessStatus: nextStatus.String(),
	}
	_, err = w.repository.UpdateKnowledgeBaseFile(ctx, param.FileUID.String(), updateMap)
	if err != nil {
		return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, fmt.Errorf("failed to update file status: %w", err)
	}

	w.log.Info("Waiting file processed", zap.String("fileUID", param.FileUID.String()), zap.String("nextStatus", nextStatus.String()))
	return nextStatus, nil
}
