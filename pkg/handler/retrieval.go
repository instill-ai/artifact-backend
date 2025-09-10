package handler

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/repository"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
)

// SimilarityChunksSearch rturns the top-K most similar chunks to a text
// prompt.
func (ph *PublicHandler) SimilarityChunksSearch(
	ctx context.Context,
	req *artifactpb.SimilarityChunksSearchRequest,
) (*artifactpb.SimilarityChunksSearchResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	logger = logger.With(
		zap.String("namespace", req.GetNamespaceId()),
		zap.String("catalog", req.GetCatalogId()),
	)

	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		return nil, fmt.Errorf("fetching namespace: %w", err)
	}

	ownerUID := ns.NsUID
	kb, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ownerUID, req.CatalogId)
	if err != nil {
		return nil, fmt.Errorf("fetching catalog: %w", err)
	}

	// ACL : check user has access to the catalog
	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", kb.UID, "reader")
	if err != nil {
		return nil, fmt.Errorf("checking permissions: %w", err)
	}
	if !granted {
		return nil, errorsx.ErrUnauthenticated
	}

	// check auth user has access to the requester
	err = ph.service.ACLClient().CheckRequesterPermission(ctx)
	if err != nil {
		return nil, fmt.Errorf("checking requester permission: %w", err)
	}

	// retrieve the chunks based on the similarity
	t := time.Now()
	simChunksScores, err := ph.service.SimilarityChunksSearch(ctx, ownerUID, req)
	if err != nil {
		return nil, fmt.Errorf("searching similar chunks: %w", err)
	}
	var chunkUIDs []uuid.UUID
	for _, simChunk := range simChunksScores {
		chunkUIDs = append(chunkUIDs, simChunk.ChunkUID)
	}
	logger.Info("get similarity chunks", zap.Duration("duration", time.Since(t)))

	// fetch the chunks metadata
	chunks, err := ph.service.Repository().GetChunksByUIDs(ctx, chunkUIDs)
	if err != nil {
		return nil, fmt.Errorf("fetching chunk metadata: %w", err)
	}

	// chunks content
	chunkFilePaths := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		chunkFilePaths = append(chunkFilePaths, chunk.ContentDest)
	}

	// fetch the chunks content from blob storage
	chunkContents, err := ph.service.MinIO().GetFilesByPaths(ctx, config.Config.Minio.BucketName, chunkFilePaths)
	if err != nil {
		return nil, fmt.Errorf("fetching chunk contents: %w", err)
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

	files, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(
		ctx,
		fileUids,
		repository.KnowledgeBaseFileColumn.UID,
		repository.KnowledgeBaseFileColumn.Name,
	)
	if err != nil {
		return nil, fmt.Errorf("fetching file info: %w", err)
	}

	for _, file := range files {
		fileUIDMapName[file.UID] = file.Name
	}

	// build response
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
		chunk := &artifactpb.SimilarityChunk{
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
		}

		if contentType == artifactpb.ContentType_CONTENT_TYPE_CHUNK && strings.HasSuffix(chunk.SourceFile, ".pdf") {
			chunk.ChunkMetadata.Reference = &artifactpb.Chunk_Reference{
				Start: &artifactpb.File_Position{
					Unit:        artifactpb.File_Position_UNIT_PAGE,
					Coordinates: []uint32{2},
				},
				End: &artifactpb.File_Position{
					Unit:        artifactpb.File_Position_UNIT_PAGE,
					Coordinates: []uint32{2},
				},
			}
		}

		simChunks = append(simChunks, chunk)
	}

	return &artifactpb.SimilarityChunksSearchResponse{SimilarChunks: simChunks}, nil
}
