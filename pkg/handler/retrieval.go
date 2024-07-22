package handler

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/instill-ai/artifact-backend/pkg/customerror"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	artifactv1alpha "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	"go.uber.org/zap"
)

func (ph *PublicHandler) SimilarityChunksSearch(
	ctx context.Context, req *artifactv1alpha.SimilarityChunksSearchRequest) (
	*artifactv1alpha.SimilarityChunksSearchResponse,
	error) {

	log, _ := logger.GetZapLogger(ctx)

	uid, err := getUserUIDFromContext(ctx)
	if err != nil {
		log.Error("failed to get user id from header", zap.Error(err))
		return nil, fmt.Errorf("failed to get user id from header: %v. err: %w", err, customerror.ErrUnauthenticated)
	}
	// turn uid to uuid
	uidUUID, err := uuid.Parse(uid)
	if err != nil {
		log.Error("failed to parse user id", zap.Error(err))
		return nil, fmt.Errorf("failed to parse user id: %v. err: %w", err, customerror.ErrUnauthenticated)
	}
	// TODO ACL : check user has access to the knowledge base
	_ = req.KbId
	_ = uid

	// retrieve the chunks based on the similarity
	simChunksScroes, err := ph.service.SimilarityChunksSearch(ctx, uidUUID, req)
	if err != nil {
		log.Error("failed to get similarity chunks", zap.Error(err))
		return nil, fmt.Errorf("failed to get similarity chunks. err: %w", err)
	}
	var chunkUIDs []uuid.UUID
	for _, simChunk := range simChunksScroes {
		chunkUIDs = append(chunkUIDs, simChunk.ChunkUID)
	}
	// fetch the chunks metadata
	chunks, err := ph.service.Repository.GetChunksByUIDs(ctx, chunkUIDs)
	if err != nil {
		log.Error("failed to get chunks by uids", zap.Error(err))
		return nil, fmt.Errorf("failed to get chunks by uids. err: %w", err)
	}
	// chunks content
	chunkFilePaths := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		chunkFilePaths = append(chunkFilePaths, chunk.ContentDest)
	}
	// fetch the chunks content from minio
	chunkContents, err := ph.service.MinIO.GetFilesByPaths(ctx, chunkFilePaths)
	if err != nil {
		log.Error("failed to get chunks content", zap.Error(err))
		return nil, fmt.Errorf("failed to get chunks content. err: %w", err)
	}

	// fetch the file names
	fileUIDMapName := make(map[uuid.UUID]string)
	for _, chunk := range chunks {
		fileUIDMapName[chunk.KbFileUID] = ""
	}
	fileUids := make([]uuid.UUID, 0, len(fileUIDMapName))
	for fileUID := range fileUIDMapName {
		fileUids = append(fileUids, fileUID)
	}
	files, err := ph.service.Repository.GetKnowledgeBaseFilesByFileUIDs(
		ctx, fileUids, repository.ConvertedFileColumn.FileUID, repository.KnowledgeBaseFileColumn.Name)
	if err != nil {
		log.Error("failed to get knowledge base files by file uids", zap.Error(err))
		return nil, fmt.Errorf("failed to get knowledge base files by file uids. err: %w", err)
	}
	for _, file := range files {
		fileUIDMapName[file.UID] = file.Name
	}

	// prepare the response
	simChunks := make([]*artifactv1alpha.SimilarityChunk, 0, len(chunks))
	for i, chunk := range chunks {
		simChunks = append(simChunks, &artifactv1alpha.SimilarityChunk{
			ChunkUid:        chunk.UID.String(),
			SimilarityScore: float32(simChunksScroes[i].Score),
			TextContent:     string(chunkContents[i].Content),
			SourceFile:      fileUIDMapName[chunk.KbFileUID],
		})
	}
	return &artifactv1alpha.SimilarityChunksSearchResponse{SimilarChunks: simChunks}, nil
}
