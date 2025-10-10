package handler

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

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

	// Extract authentication metadata from context to pass to worker
	var requestMetadata map[string][]string
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		md, ok = metadata.FromOutgoingContext(ctx)
	}
	if ok {
		requestMetadata = md
	}

	// Embed text using service
	textVector, err := ph.service.EmbedTexts(ctx, []string{req.TextPrompt}, 32, requestMetadata)
	if err != nil {
		return nil, fmt.Errorf("vectorizing text: %w", err)
	}

	// Now call service to do vector search
	simChunksScores, err := ph.service.SimilarityChunksSearch(ctx, ownerUID, req, textVector)
	if err != nil {
		return nil, fmt.Errorf("searching similar chunks: %w", err)
	}
	var chunkUIDs []types.TextChunkUIDType
	for _, simChunk := range simChunksScores {
		chunkUIDs = append(chunkUIDs, simChunk.ChunkUID)
	}
	logger.Info("get similarity chunks", zap.Duration("duration", time.Since(t)))

	// fetch the chunks metadata
	chunks, err := ph.service.Repository().GetTextChunksByUIDs(ctx, chunkUIDs)
	if err != nil {
		return nil, fmt.Errorf("fetching chunk metadata: %w", err)
	}

	// chunks content
	chunkFilePaths := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		chunkFilePaths = append(chunkFilePaths, chunk.ContentDest)
	}

	// Get file contents using service
	chunkContents, err := ph.service.GetFilesByPaths(ctx, config.Config.Minio.BucketName, chunkFilePaths)
	if err != nil {
		return nil, fmt.Errorf("fetching chunk contents: %w", err)
	}

	// fetch the file names
	fileUIDMapName := make(map[types.FileUIDType]string)
	for _, chunk := range chunks {
		fileUIDMapName[chunk.KBFileUID] = ""
	}

	fileUids := make([]types.FileUIDType, 0, len(fileUIDMapName))
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

		pbChunk := &artifactpb.SimilarityChunk{
			ChunkUid:        chunk.UID.String(),
			SimilarityScore: float32(simChunksScores[i].Score),
			TextContent:     string(chunkContents[i].Content),
			SourceFile:      fileUIDMapName[chunk.KBFileUID],
			ChunkMetadata:   convertToProtoChunk(chunk),
		}

		simChunks = append(simChunks, pbChunk)
	}

	return &artifactpb.SimilarityChunksSearchResponse{SimilarChunks: simChunks}, nil
}
