package handler

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
)

// ========================================================================
// NEW AIP-COMPLIANT CHUNK/RETRIEVAL HANDLERS
// Following Google AIP-121 (Resource-oriented design) and AIP-122 (Resource names)
// ========================================================================

// GetChunk retrieves a specific chunk from a knowledge base.
// Returns chunk metadata only. For chunk content, use SearchChunks which returns
// text content along with similarity scores.
func (ph *PublicHandler) GetChunk(ctx context.Context, req *artifactpb.GetChunkRequest) (*artifactpb.GetChunkResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Get namespace
	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("fetching namespace: %w", err),
			"Unable to access the specified namespace. Please check the namespace ID and try again.",
		)
	}

	// Get knowledge base
	kb, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, req.GetKnowledgeBaseId())
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("fetching knowledge base: %w", err),
			"Unable to access the specified knowledge base. Please check the knowledge base ID and try again.",
		)
	}

	// ACL - check user has access to the knowledge base
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
			"You don't have permission to access this knowledge base. Please contact the owner for access.",
		)
	}

	// Get chunk metadata from repository
	chunkUUID, err := uuid.FromString(req.GetChunkId())
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("invalid chunk ID: %w", err),
			"Invalid chunk ID format. Please provide a valid UUID.",
		)
	}

	chunks, err := ph.service.Repository().GetTextChunksByUIDs(ctx, []types.TextChunkUIDType{types.TextChunkUIDType(chunkUUID)})
	if err != nil || len(chunks) == 0 {
		logger.Error("chunk not found", zap.Error(err), zap.String("chunkID", req.GetChunkId()))
		return nil, errorsx.AddMessage(
			fmt.Errorf("chunk not found: %w", errorsx.ErrNotFound),
			"Chunk not found. Please check the chunk ID and try again.",
		)
	}

	chunk := chunks[0]

	// Convert to protobuf format
	// Note: The Chunk message only contains metadata. For chunk content, use SearchChunks
	// which returns SimilarityChunk with text_content field, or add a content field to
	// the Chunk proto message if direct content retrieval is needed.
	pbChunk := convertToProtoChunk(chunk)

	return &artifactpb.GetChunkResponse{
		Chunk: pbChunk,
	}, nil
}

// SearchChunks performs vector similarity search across chunks in a knowledge base (AIP-compliant version of SearchChunks).
// Returns the top-K most similar chunks to a text prompt.
func (ph *PublicHandler) SearchChunks(
	ctx context.Context,
	req *artifactpb.SearchChunksRequest,
) (*artifactpb.SearchChunksResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	logger = logger.With(
		zap.String("namespace", req.GetNamespaceId()),
		zap.String("knowledge_base", req.GetKnowledgeBaseId()),
	)

	// Get namespace
	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("fetching namespace: %w", err),
			"Unable to access the specified namespace. Please check the namespace ID and try again.",
		)
	}

	// Get knowledge base
	ownerUID := ns.NsUID
	kb, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ownerUID, req.KnowledgeBaseId)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("fetching knowledge base: %w", err),
			"Unable to access the specified knowledge base. Please check the knowledge base ID and try again.",
		)
	}

	// ACL - check user has access to the knowledge base
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
			"You don't have permission to access this knowledge base. Please contact the owner for access.",
		)
	}

	// Check auth user has access to the requester
	err = ph.service.ACLClient().CheckRequesterPermission(ctx)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("checking requester permission: %w", err),
			"Unable to verify your authentication. Please log in again and try again.",
		)
	}

	// Retrieve the chunks based on the similarity
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

	// Perform vector similarity search
	simChunksScores, err := ph.service.SearchChunks(ctx, ownerUID, req, textVector)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("searching similar chunks: %w", err),
			"Unable to search for similar content. Please try again.",
		)
	}

	// Extract chunk UIDs
	var chunkUIDs []types.TextChunkUIDType
	for _, simChunk := range simChunksScores {
		chunkUIDs = append(chunkUIDs, simChunk.ChunkUID)
	}
	logger.Info("get similarity chunks", zap.Duration("duration", time.Since(t)))

	// Fetch chunk metadata
	chunks, err := ph.service.Repository().GetTextChunksByUIDs(ctx, chunkUIDs)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("fetching chunk metadata: %w", err),
			"Unable to retrieve search results. Please try again.",
		)
	}

	// Get chunk contents from MinIO
	chunkFilePaths := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		chunkFilePaths = append(chunkFilePaths, chunk.ContentDest)
	}

	chunkContents, err := ph.service.GetFilesByPaths(ctx, config.Config.Minio.BucketName, chunkFilePaths)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("fetching chunk contents: %w", err),
			"Unable to load search result contents. Please try again.",
		)
	}

	// Fetch file names
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
		repository.KnowledgeBaseFileColumn.Filename,
	)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("fetching file info: %w", err),
			"Unable to load file information for search results. Please try again.",
		)
	}

	for _, file := range files {
		fileUIDMapName[file.UID] = file.Filename
	}

	// Build response with new protobuf format
	simChunks := make([]*artifactpb.SimilarityChunk, 0, len(chunks))
	for i, chunk := range chunks {
		if !chunk.Retrievable {
			logger.Warn("Skipping non-retrievable chunk", zap.String("chunkUID", chunk.UID.String()))
			continue
		}

		pbChunk := &artifactpb.SimilarityChunk{
			ChunkId:         chunk.UID.String(), // Use ChunkId (new field name)
			SimilarityScore: float32(simChunksScores[i].Score),
			TextContent:     string(chunkContents[i].Content),
			SourceFile:      fileUIDMapName[chunk.FileUID],
			ChunkMetadata:   convertToProtoChunk(chunk),
		}
		simChunks = append(simChunks, pbChunk)
	}

	logger.Info("SearchChunks response",
		zap.Int("totalChunks", len(chunks)),
		zap.Int("returnedChunks", len(simChunks)),
		zap.Int("vectorSearchResults", len(simChunksScores)))

	return &artifactpb.SearchChunksResponse{SimilarChunks: simChunks}, nil
}
