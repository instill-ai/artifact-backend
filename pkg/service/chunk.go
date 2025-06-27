package service

import (
	"context"
	"fmt"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/minio"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/x/log"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

type ChunkUIDType = uuid.UUID
type ContentType = string
type SourceTableType = string
type SourceIDType = uuid.UUID

// GetChunksByFile returns the chunks of a file
func (s *Service) GetChunksByFile(ctx context.Context, file *repository.KnowledgeBaseFile) (
	SourceTableType, SourceIDType, []repository.TextChunk, map[ChunkUIDType]ContentType, []string, error) {
	logger, _ := log.GetZapLogger(ctx)
	// check the file type
	// if it is pdf, sourceTable is converted file table, sourceUID is converted file UID
	// if it is txt or md, sourceTable is knowledge base file table, sourceUID is knowledge base file UID
	var sourceTable string
	var sourceUID uuid.UUID
	switch file.Type {
	case artifactpb.FileType_FILE_TYPE_PDF.String(),
		artifactpb.FileType_FILE_TYPE_HTML.String(),
		artifactpb.FileType_FILE_TYPE_DOC.String(),
		artifactpb.FileType_FILE_TYPE_DOCX.String(),
		artifactpb.FileType_FILE_TYPE_PPT.String(),
		artifactpb.FileType_FILE_TYPE_PPTX.String(),
		artifactpb.FileType_FILE_TYPE_XLSX.String(),
		artifactpb.FileType_FILE_TYPE_XLS.String(),
		artifactpb.FileType_FILE_TYPE_CSV.String():
		// set the sourceTable and sourceUID
		convertedFile, err := s.Repository.GetConvertedFileByFileUID(ctx, file.UID)
		if err != nil {
			logger.Error("Failed to get converted file metadata.", zap.String("File uid", file.UID.String()))
			return sourceTable, sourceUID, nil, nil, nil, err
		}
		sourceTable = s.Repository.ConvertedFileTableName()
		sourceUID = convertedFile.UID
	case artifactpb.FileType_name[int32(artifactpb.FileType_FILE_TYPE_TEXT)],
		artifactpb.FileType_name[int32(artifactpb.FileType_FILE_TYPE_MARKDOWN)]:
		// set the sourceTable and sourceUID
		sourceTable = s.Repository.KnowledgeBaseFileTableName()
		sourceUID = file.UID
	default:
		return sourceTable, sourceUID, nil, nil, nil, fmt.Errorf("unsupported file type: %s", file.Type)
	}
	// get the chunks's path
	chunks, err := s.Repository.GetTextChunksBySource(ctx, sourceTable, sourceUID)
	if err != nil {
		logger.Error("Failed to get chunks from database.", zap.String("SourceUID", sourceUID.String()))
		return sourceTable, sourceUID, nil, nil, nil, err
	}
	// get the chunks from minIO
	chunksPaths := make([]string, len(chunks))
	for i, c := range chunks {
		chunksPaths[i] = c.ContentDest
	}
	chunkFiles, err := s.MinIO.GetFilesByPaths(ctx, minio.KnowledgeBaseBucketName, chunksPaths)
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
