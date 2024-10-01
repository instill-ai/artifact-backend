package handler

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/customerror"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/resource"
	"github.com/instill-ai/artifact-backend/pkg/service"
	artifactPb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (ph *PublicHandler) SimilarityChunksSearch(
	ctx context.Context, req *artifactPb.SimilarityChunksSearchRequest) (
	*artifactPb.SimilarityChunksSearchResponse,
	error) {

	log, _ := logger.GetZapLogger(ctx)

	uid, err := getUserUIDFromContext(ctx)
	if err != nil {
		log.Error("failed to get user id from header", zap.Error(err))
		return nil, fmt.Errorf("failed to get user id from header: %v. err: %w", err, customerror.ErrUnauthenticated)
	}
	// turn uid to uuid
	uidUUID, err := uuid.FromString(uid)
	if err != nil {
		log.Error("failed to parse user id", zap.Error(err))
		return nil, fmt.Errorf("failed to parse user id: %v. err: %w", err, customerror.ErrUnauthenticated)
	}
	t := time.Now()
	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		log.Error("failed to get namespace by ns id", zap.Error(err))
		return nil, fmt.Errorf("failed to get namespace by ns id. err: %w", err)
	}
	log.Info("get namespace by ns id", zap.Duration("duration", time.Since(t)))
	t = time.Now()
	ownerUID := ns.NsUID
	kb, err := ph.service.Repository.GetKnowledgeBaseByOwnerAndKbID(ctx, ownerUID, req.CatalogId)
	if err != nil {
		log.Error("failed to get catalog by owner and kb id", zap.Error(err))
		return nil, fmt.Errorf("failed to get catalog by owner and kb id. err: %w", err)
	}
	log.Info("get catalog by owner and kb id", zap.Duration("duration", time.Since(t)))
	t = time.Now()
	// ACL : check user has access to the catalog
	granted, err := ph.service.ACLClient.CheckPermission(ctx, "knowledgebase", kb.UID, "reader")
	if err != nil {
		log.Error("failed to check permission", zap.Error(err))
		return nil, fmt.Errorf("failed to check permission. err: %w", err)
	}
	if !granted {
		log.Error("permission denied", zap.String("user_id", uid), zap.String("kb_id", kb.UID.String()))
		return nil, fmt.Errorf("SimilarityChunksSearch permission denied. err: %w", service.ErrNoPermission)
	}
	log.Info("check permission", zap.Duration("duration", time.Since(t)))
	t = time.Now()
	// check auth user has access to the requester
	err = ph.service.ACLClient.CheckRequesterPermission(ctx)
	if err != nil {
		log.Error("failed to check requester permission", zap.Error(err))
		return nil, fmt.Errorf("failed to check requester permission. err: %w", err)
	}
	reqUIDString := resource.GetRequestSingleHeader(ctx, constant.HeaderRequesterUIDKey)
	requesterUID := uuid.FromStringOrNil(reqUIDString)
	// retrieve the chunks based on the similarity
	simChunksScores, err := ph.service.SimilarityChunksSearch(ctx, uidUUID, requesterUID, ownerUID, req)
	if err != nil {
		log.Error("failed to get similarity chunks", zap.Error(err))
		return nil, fmt.Errorf("failed to get similarity chunks. err: %w", err)
	}
	var chunkUIDs []uuid.UUID
	for _, simChunk := range simChunksScores {
		chunkUIDs = append(chunkUIDs, simChunk.ChunkUID)
	}
	log.Info("get similarity chunks", zap.Duration("duration", time.Since(t)))
	t = time.Now()
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
	log.Info("get chunks by uids", zap.Duration("duration", time.Since(t)))
	t = time.Now()
	// fetch the chunks content from minio
	chunkContents, err := ph.service.MinIO.GetFilesByPaths(ctx, chunkFilePaths)
	if err != nil {
		log.Error("failed to get chunks content", zap.Error(err))
		return nil, fmt.Errorf("failed to get chunks content. err: %w", err)
	}
	log.Info("get chunks content from minIO", zap.Duration("duration", time.Since(t)))

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
	files, err := ph.service.Repository.GetKnowledgeBaseFilesByFileUIDs(
		ctx, fileUids, repository.KnowledgeBaseFileColumn.UID, repository.KnowledgeBaseFileColumn.Name)
	if err != nil {
		log.Error("failed to get catalog files by file uids", zap.Error(err))
		return nil, fmt.Errorf("failed to get catalog files by file uids. err: %w", err)
	}
	for _, file := range files {
		fileUIDMapName[file.UID] = file.Name
	}
	log.Info("get catalog files by file uids", zap.Duration("duration", time.Since(t)))

	// prepare the response
	simChunks := make([]*artifactPb.SimilarityChunk, 0, len(chunks))
	for i, chunk := range chunks {
		if !chunk.Retrievable {
			continue
		}
		simChunks = append(simChunks, &artifactPb.SimilarityChunk{
			ChunkUid:        chunk.UID.String(),
			SimilarityScore: float32(simChunksScores[i].Score),
			TextContent:     string(chunkContents[i].Content),
			SourceFile:      fileUIDMapName[chunk.KbFileUID],
			ChunkMetadata: &artifactPb.Chunk{
				ChunkUid:        chunk.UID.String(),
				Retrievable:     chunk.Retrievable,
				StartPos:        uint32(chunk.StartPos),
				EndPos:          uint32(chunk.EndPos),
				Tokens:          uint32(chunk.Tokens),
				CreateTime:      timestamppb.New(*chunk.CreateTime),
				OriginalFileUid: chunk.KbFileUID.String(),
			},
		})
	}
	return &artifactPb.SimilarityChunksSearchResponse{SimilarChunks: simChunks}, nil
}
