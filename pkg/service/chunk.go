package service

import (
	"context"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/repository"

	logx "github.com/instill-ai/x/log"
)

type ChunkUIDType = uuid.UUID
type ContentType = string
type SourceTableType = string
type SourceIDType = uuid.UUID

// GetChunksByFile returns the chunks of a file
func (s *service) GetChunksByFile(ctx context.Context, file *repository.KnowledgeBaseFile) (
	SourceTableType,
	SourceIDType,
	[]repository.TextChunk,
	map[ChunkUIDType]ContentType,
	[]string,
	error,
) {

	logger, _ := logx.GetZapLogger(ctx)
	// After the refactoring, ALL file types (including TEXT/MARKDOWN) now use converted_file as source
	// TEXT/MARKDOWN files have converted file records pointing to the original file
	var sourceTable string
	var sourceUID uuid.UUID

	// Get converted file for all types
	convertedFile, err := s.repository.GetConvertedFileByFileUID(ctx, file.UID)
	if err != nil {
		logger.Error("Failed to get converted file metadata.", zap.String("File uid", file.UID.String()))
		return sourceTable, sourceUID, nil, nil, nil, err
	}
	sourceTable = s.repository.ConvertedFileTableName()
	sourceUID = convertedFile.UID
	// get the chunks's path
	chunks, err := s.repository.GetTextChunksBySource(ctx, sourceTable, sourceUID)
	if err != nil {
		logger.Error("Failed to get chunks from database.", zap.String("SourceUID", sourceUID.String()))
		return sourceTable, sourceUID, nil, nil, nil, err
	}
	// get the chunks from minIO
	chunksPaths := make([]string, len(chunks))
	for i, c := range chunks {
		chunksPaths[i] = c.ContentDest
	}
	chunkFiles, err := s.GetFilesByPaths(ctx, config.Config.Minio.BucketName, chunksPaths)
	if err != nil {
		// log error source table and source UID
		logger.Error("Failed to get chunks from minIO.", zap.String("SourceTable", sourceTable), zap.String("SourceUID", sourceUID.String()))
		return sourceTable, sourceUID, nil, nil, nil, err
	}
	texts := make([]string, len(chunkFiles))
	for i, f := range chunkFiles {
		texts[i] = string(f.Content)
	}
	// return the chunks and texts
	chunkUIDToContents := make(map[ChunkUIDType]ContentType, len(chunks))
	for i, c := range chunks {
		chunkUIDToContents[c.UID] = texts[i]
	}

	return sourceTable, sourceUID, chunks, chunkUIDToContents, texts, nil

}
