package handler

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
)

// SimilarityChunksSearch returns the top-K most similar chunks to a text
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
		return nil, errorsx.AddMessage(
			fmt.Errorf("fetching namespace: %w", err),
			"Unable to access the specified namespace. Please check the namespace ID and try again.",
		)
	}

	ownerUID := ns.NsUID
	kb, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ownerUID, req.CatalogId)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("fetching catalog: %w", err),
			"Unable to access the specified catalog. Please check the catalog ID and try again.",
		)
	}

	// ACL : check user has access to the catalog
	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", kb.UID, "reader")
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("checking permissions: %w", err),
			"Unable to verify access permissions. Please try again.",
		)
	}
	if !granted {
		return nil, errorsx.AddMessage(
			errorsx.ErrUnauthenticated,
			"You don't have permission to access this catalog. Please contact the owner for access.",
		)
	}

	// check auth user has access to the requester
	err = ph.service.ACLClient().CheckRequesterPermission(ctx)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("checking requester permission: %w", err),
			"Unable to verify your authentication. Please log in again and try again.",
		)
	}

	// retrieve the chunks based on the similarity
	t := time.Now()

	// Embed query using appropriate client based on KB's embedding config
	// Note: For similarity search, we use RETRIEVAL_QUERY task type which optimizes
	// for finding relevant documents similar to the query
	textVector, err := ph.service.EmbedTexts(ctx, &kb.UID, []string{req.TextPrompt}, "RETRIEVAL_QUERY")
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("vectorizing text: %w", err),
			"Unable to process your search query. Please try again.",
		)
	}

	// Now call service to do vector search
	simChunksScores, err := ph.service.SimilarityChunksSearch(ctx, ownerUID, req, textVector)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("searching similar chunks: %w", err),
			"Unable to search for similar content. Please try again.",
		)
	}
	var chunkUIDs []types.TextChunkUIDType
	for _, simChunk := range simChunksScores {
		chunkUIDs = append(chunkUIDs, simChunk.ChunkUID)
	}
	logger.Info("get similarity chunks", zap.Duration("duration", time.Since(t)))

	// fetch the chunks metadata
	chunks, err := ph.service.Repository().GetTextChunksByUIDs(ctx, chunkUIDs)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("fetching chunk metadata: %w", err),
			"Unable to retrieve search results. Please try again.",
		)
	}

	// chunks content
	chunkFilePaths := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		chunkFilePaths = append(chunkFilePaths, chunk.ContentDest)
	}

	// Get file contents using service
	chunkContents, err := ph.service.GetFilesByPaths(ctx, config.Config.Minio.BucketName, chunkFilePaths)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("fetching chunk contents: %w", err),
			"Unable to load search result contents. Please try again.",
		)
	}

	// fetch the file names
	fileUIDMapName := make(map[types.FileUIDType]string)
	for _, chunk := range chunks {
		fileUIDMapName[chunk.FileUID] = ""
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
		return nil, errorsx.AddMessage(
			fmt.Errorf("fetching file info: %w", err),
			"Unable to load file information for search results. Please try again.",
		)
	}

	for _, file := range files {
		fileUIDMapName[file.UID] = file.Name
	}

	// build response
	simChunks := make([]*artifactpb.SimilarityChunk, 0, len(chunks))
	for i, chunk := range chunks {
		if !chunk.Retrievable {
			logger.Warn("Skipping non-retrievable chunk", zap.String("chunkUID", chunk.UID.String()))
			continue
		}

		pbChunk := &artifactpb.SimilarityChunk{
			ChunkUid:        chunk.UID.String(),
			SimilarityScore: float32(simChunksScores[i].Score),
			TextContent:     string(chunkContents[i].Content),
			SourceFile:      fileUIDMapName[chunk.FileUID],
			ChunkMetadata:   convertToProtoChunk(chunk),
		}

		simChunks = append(simChunks, pbChunk)
	}

	logger.Info("SimilarityChunksSearch response",
		zap.Int("totalChunks", len(chunks)),
		zap.Int("returnedChunks", len(simChunks)),
		zap.Int("vectorSearchResults", len(simChunksScores)))

	return &artifactpb.SimilarityChunksSearchResponse{SimilarChunks: simChunks}, nil
}
