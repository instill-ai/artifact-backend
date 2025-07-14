package handler

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/minio"
	"github.com/instill-ai/artifact-backend/pkg/repository"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
)

func (ph *PublicHandler) SimilarityChunksSearch(
	ctx context.Context,
	req *artifactpb.SimilarityChunksSearchRequest,
) (*artifactpb.SimilarityChunksSearchResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	t := time.Now()
	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		logger.Error("failed to get namespace by ns id", zap.Error(err))
		return nil, fmt.Errorf("failed to get namespace by ns id. err: %w", err)
	}
	logger.Info("get namespace by ns id", zap.Duration("duration", time.Since(t)))
	t = time.Now()
	ownerUID := ns.NsUID
	kb, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ownerUID, req.CatalogId)
	if err != nil {
		logger.Error("failed to get catalog by owner and kb id", zap.Error(err))
		return nil, fmt.Errorf("failed to get catalog by owner and kb id. err: %w", err)
	}
	logger.Info("get catalog by owner and kb id", zap.Duration("duration", time.Since(t)))
	t = time.Now()
	// ACL : check user has access to the catalog
	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", kb.UID, "reader")
	if err != nil {
		logger.Error("failed to check permission", zap.Error(err))
		return nil, fmt.Errorf("failed to check permission. err: %w", err)
	}
	if !granted {
		return nil, fmt.Errorf("SimilarityChunksSearch permission denied. err: %w", errorsx.ErrUnauthenticated)
	}
	logger.Info("check permission", zap.Duration("duration", time.Since(t)))
	t = time.Now()
	// check auth user has access to the requester
	err = ph.service.ACLClient().CheckRequesterPermission(ctx)
	if err != nil {
		logger.Error("failed to check requester permission", zap.Error(err))
		return nil, fmt.Errorf("failed to check requester permission. err: %w", err)
	}
	// retrieve the chunks based on the similarity
	simChunksScores, err := ph.service.SimilarityChunksSearch(ctx, ownerUID, req)
	if err != nil {
		logger.Error("failed to get similarity chunks", zap.Error(err))
		return nil, fmt.Errorf("failed to get similarity chunks. err: %w", err)
	}
	var chunkUIDs []uuid.UUID
	for _, simChunk := range simChunksScores {
		chunkUIDs = append(chunkUIDs, simChunk.ChunkUID)
	}
	logger.Info("get similarity chunks", zap.Duration("duration", time.Since(t)))
	t = time.Now()
	// fetch the chunks metadata
	chunks, err := ph.service.Repository().GetChunksByUIDs(ctx, chunkUIDs)
	if err != nil {
		logger.Error("failed to get chunks by uids", zap.Error(err))
		return nil, fmt.Errorf("failed to get chunks by uids. err: %w", err)
	}
	// chunks content
	chunkFilePaths := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		chunkFilePaths = append(chunkFilePaths, chunk.ContentDest)
	}
	logger.Info("get chunks by uids", zap.Duration("duration", time.Since(t)))
	t = time.Now()
	// fetch the chunks content from minio
	chunkContents, err := ph.service.MinIO().GetFilesByPaths(ctx, minio.KnowledgeBaseBucketName, chunkFilePaths)
	if err != nil {
		logger.Error("failed to get chunks content", zap.Error(err))
		return nil, fmt.Errorf("failed to get chunks content. err: %w", err)
	}
	logger.Info("get chunks content from minIO", zap.Duration("duration", time.Since(t)))

	// fetch the file names
	fileUIDMapName := make(map[uuid.UUID]string)
	for _, chunk := range chunks {
		fileUIDMapName[chunk.KbFileUID] = ""
	}
	fileUids := make([]uuid.UUID, 0, len(fileUIDMapName))
	for fileUID := range fileUIDMapName {
		fileUids = append(fileUids, fileUID)
	}
	t = time.Now()
	files, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(
		ctx, fileUids, repository.KnowledgeBaseFileColumn.UID, repository.KnowledgeBaseFileColumn.Name)
	if err != nil {
		logger.Error("failed to get catalog files by file uids", zap.Error(err))
		return nil, fmt.Errorf("failed to get catalog files by file uids. err: %w", err)
	}
	for _, file := range files {
		fileUIDMapName[file.UID] = file.Name
	}
	logger.Info("get catalog files by file uids", zap.Duration("duration", time.Since(t)))

	// prepare the response
	simChunks := make([]*artifactpb.SimilarityChunk, 0, len(chunks))
	for i, chunk := range chunks {
		if !chunk.Retrievable {
			continue
		}
		var contentType artifactpb.ContentType
		switch chunk.ContentType {
		case string(constant.ChunkContentType):
			contentType = artifactpb.ContentType_CONTENT_TYPE_CHUNK
		case string(constant.SummaryContentType):
			contentType = artifactpb.ContentType_CONTENT_TYPE_SUMMARY
		case string(constant.AugmentedContentType):
			contentType = artifactpb.ContentType_CONTENT_TYPE_AUGMENTED
		default:
			contentType = artifactpb.ContentType_CONTENT_TYPE_UNSPECIFIED

		}
		simChunks = append(simChunks, &artifactpb.SimilarityChunk{
			ChunkUid:        chunk.UID.String(),
			SimilarityScore: float32(simChunksScores[i].Score),
			TextContent:     string(chunkContents[i].Content),
			SourceFile:      fileUIDMapName[chunk.KbFileUID],
			ChunkMetadata: &artifactpb.Chunk{
				ChunkUid:        chunk.UID.String(),
				Retrievable:     chunk.Retrievable,
				StartPos:        uint32(chunk.StartPos),
				EndPos:          uint32(chunk.EndPos),
				Tokens:          uint32(chunk.Tokens),
				CreateTime:      timestamppb.New(*chunk.CreateTime),
				OriginalFileUid: chunk.KbFileUID.String(),
				ContentType:     contentType,
			},
		})
	}
	return &artifactpb.SimilarityChunksSearchResponse{SimilarChunks: simChunks}, nil
}
