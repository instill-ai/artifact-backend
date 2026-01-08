package handler

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
)

// convertToProtoChunk converts a TextChunkModel to a protobuf Chunk
// namespaceId, kbId, and fileId are used to construct the resource name
func convertToProtoChunk(textChunk repository.TextChunkModel, namespaceID, kbID, fileID string) *artifactpb.Chunk {
	var chunkType artifactpb.Chunk_Type

	// Convert database string to protobuf enum
	switch textChunk.ChunkType {
	case "TYPE_CONTENT":
		chunkType = artifactpb.Chunk_TYPE_CONTENT
	case "TYPE_SUMMARY":
		chunkType = artifactpb.Chunk_TYPE_SUMMARY
	case "TYPE_AUGMENTED":
		chunkType = artifactpb.Chunk_TYPE_AUGMENTED
	default:
		chunkType = artifactpb.Chunk_TYPE_CONTENT // Default to content
	}

	chunkID := textChunk.UID.String()
	resourceName := fmt.Sprintf("namespaces/%s/knowledge-bases/%s/files/%s/chunks/%s",
		namespaceID, kbID, fileID, chunkID)

	pbChunk := &artifactpb.Chunk{
		Uid:            chunkID,
		Id:             chunkID,
		Name:           resourceName,
		Retrievable:    textChunk.Retrievable,
		Tokens:         uint32(textChunk.Tokens),
		CreateTime:     timestamppb.New(*textChunk.CreateTime),
		OriginalFileId: textChunk.FileUID.String(),
		Type:           chunkType,
	}

	// Set markdown reference for all chunk types (content, summary, augmented)
	pbChunk.MarkdownReference = &artifactpb.Chunk_Reference{
		Start: &artifactpb.File_Position{
			Unit:        artifactpb.File_Position_UNIT_CHARACTER,
			Coordinates: []uint32{uint32(textChunk.StartPos)},
		},
		End: &artifactpb.File_Position{
			Unit:        artifactpb.File_Position_UNIT_CHARACTER,
			Coordinates: []uint32{uint32(textChunk.EndPos)},
		},
	}

	// Add page reference if available
	if textChunk.Reference != nil && textChunk.Reference.PageRange[0] != 0 && textChunk.Reference.PageRange[1] != 0 {
		// We only extract one kind of reference for now. When more file
		// types are supported, we'll need to inspect the reference object
		// to build the response correctly.
		pbChunk.Reference = &artifactpb.Chunk_Reference{
			Start: &artifactpb.File_Position{
				Unit:        artifactpb.File_Position_UNIT_PAGE,
				Coordinates: []uint32{textChunk.Reference.PageRange[0]},
			},
			End: &artifactpb.File_Position{
				Unit:        artifactpb.File_Position_UNIT_PAGE,
				Coordinates: []uint32{textChunk.Reference.PageRange[1]},
			},
		}
	}

	return pbChunk
}

// ========================================================================
// CHUNK API HANDLERS
// ========================================================================

// GetChunk retrieves a specific chunk from a knowledge base.
// Returns chunk metadata including markdown_reference for extracting content.
// If chunk_type parameter is provided, returns a chunk of that type from the same file.
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

	// If chunk_type filter is specified, find a chunk of that type from the same file
	if req.ChunkType != nil && *req.ChunkType != artifactpb.Chunk_TYPE_UNSPECIFIED {
		// Get all chunks from the same file
		fileChunks, err := ph.service.Repository().ListTextChunksByKBFileUID(ctx, chunk.FileUID)
		if err != nil {
			logger.Warn("failed to get chunks for file", zap.Error(err), zap.String("fileUID", chunk.FileUID.String()))
		} else {
			// Find a chunk matching the requested type
			requestedType := req.ChunkType.String()
			for _, fc := range fileChunks {
				if fc.ChunkType == requestedType {
					chunk = fc
					break
				}
			}
		}
	}

	// Convert to protobuf format
	pbChunk := convertToProtoChunk(chunk, req.GetNamespaceId(), req.GetKnowledgeBaseId(), req.GetFileId())

	return &artifactpb.GetChunkResponse{
		Chunk: pbChunk,
	}, nil
}

// ListChunks lists the chunks of a file
func (ph *PublicHandler) ListChunks(ctx context.Context, req *artifactpb.ListChunksRequest) (*artifactpb.ListChunksResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	_, err := getUserUIDFromContext(ctx)
	if err != nil {
		err := fmt.Errorf("failed to get user id from header: %v. err: %w", err, errorsx.ErrUnauthenticated)
		return nil, err
	}

	fileUID, err := uuid.FromString(req.FileId)
	if err != nil {
		logger.Error("failed to parse file uid", zap.Error(err))
		return nil, fmt.Errorf("failed to parse file uid: %v. err: %w", err, errorsx.ErrInvalidArgument)
	}

	fileUIDs := []types.FileUIDType{types.FileUIDType(fileUID)}
	kbfs, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, fileUIDs)
	if err != nil {
		logger.Error("failed to get knowledge base files by file uids", zap.Error(err))
		return nil, fmt.Errorf("failed to get knowledge base files by file uids")
	} else if len(kbfs) == 0 {
		logger.Error("no files found for the given file uids")
		return nil, fmt.Errorf("no files found for the given file uids: %v. err: %w", fileUIDs, errorsx.ErrNotFound)
	}
	kbf := kbfs[0]
	// ACL - check user's permission to read knowledge base
	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", kbf.KBUID, "reader")
	if err != nil {
		logger.Error("failed to check permission", zap.Error(err))
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err)
	}
	if !granted {
		return nil, fmt.Errorf("%w: no permission over knowledge base", errorsx.ErrUnauthorized)
	}
	// Get ALL text chunks for this file (content + summary + augmented)
	// Note: A file can have multiple converted_files (e.g., content converted_file + summary converted_file)
	// We need to fetch chunks from ALL sources, not just one
	chunks, err := ph.service.Repository().ListTextChunksByKBFileUID(ctx, fileUID)
	if err != nil {
		logger.Error("failed to list text chunks by file uid", zap.Error(err))
		return nil, fmt.Errorf("failed to list text chunks by file uid: %w", err)
	}
	// Sort chunks: chunk type first (content, then summary, then augmented), then by order within each type
	// This ensures content chunks appear before summary chunks in the UI
	sort.Slice(chunks, func(i, j int) bool {
		// Define sort priority for chunk types
		typePriority := map[string]int{
			"content":   1,
			"summary":   2,
			"augmented": 3,
		}

		iPriority, iOk := typePriority[chunks[i].ChunkType]
		jPriority, jOk := typePriority[chunks[j].ChunkType]

		// Unknown types go last
		if !iOk {
			iPriority = 999
		}
		if !jOk {
			jPriority = 999
		}

		// First sort by content type priority
		if iPriority != jPriority {
			return iPriority < jPriority
		}

		// Within same content type, sort by in_order
		return chunks[i].InOrder < chunks[j].InOrder
	})

	res := make([]*artifactpb.Chunk, 0, len(chunks))
	for _, chunk := range chunks {
		res = append(res, convertToProtoChunk(chunk, req.NamespaceId, req.KnowledgeBaseId, req.FileId))
	}

	return &artifactpb.ListChunksResponse{
		Chunks: res,
	}, nil
}

// UpdateChunk updates the retrievable of a chunk
func (ph *PublicHandler) UpdateChunk(ctx context.Context, req *artifactpb.UpdateChunkRequest) (*artifactpb.UpdateChunkResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	chunks, err := ph.service.Repository().GetTextChunksByUIDs(ctx, []types.TextChunkUIDType{types.TextChunkUIDType(uuid.FromStringOrNil(req.ChunkId))})
	if err != nil {
		logger.Error("failed to get chunks by uids", zap.Error(err))
		return nil, fmt.Errorf("failed to get chunks by uids: %w", err)
	}
	if len(chunks) == 0 {
		logger.Error("no chunks found for the given chunk uids")
		return nil, fmt.Errorf("no chunks found for the given chunk uids: %v. err: %w", req.ChunkId, errorsx.ErrNotFound)
	}
	chunk := &chunks[0]
	// ACL - check user's permission to write knowledge base of chunks
	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", chunk.KBUID, "writer")
	if err != nil {
		logger.Error("failed to check permission", zap.Error(err))
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err)
	}
	if !granted {
		return nil, fmt.Errorf("%w: no permission over knowledge base", errorsx.ErrUnauthorized)
	}

	retrievable := req.Retrievable
	update := map[string]any{
		repository.TextChunkColumn.Retrievable: retrievable,
	}

	chunk, err = ph.service.Repository().UpdateTextChunk(ctx, req.ChunkId, update)
	if err != nil {
		logger.Error("failed to update text chunk", zap.Error(err))
		return nil, fmt.Errorf("failed to update text chunk: %w", err)
	}

	// Get file to retrieve file ID for resource name
	files, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []types.FileUIDType{chunk.FileUID})
	if err != nil || len(files) == 0 {
		logger.Error("failed to get file for chunk", zap.Error(err))
		// Fallback to using FileUID as file ID
		return &artifactpb.UpdateChunkResponse{
			Chunk: convertToProtoChunk(*chunk, req.NamespaceId, req.KnowledgeBaseId, chunk.FileUID.String()),
		}, nil
	}

	return &artifactpb.UpdateChunkResponse{
		// Populate the response fields appropriately
		Chunk: convertToProtoChunk(*chunk, req.NamespaceId, req.KnowledgeBaseId, files[0].UID.String()),
	}, nil
}

// SearchChunks performs vector similarity search across chunks in a knowledge base.
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
	fileUIDMapDisplayName := make(map[types.FileUIDType]string)
	for _, chunk := range chunks {
		fileUIDMapDisplayName[chunk.FileUID] = ""
	}

	fileUids := make([]types.FileUIDType, 0, len(fileUIDMapDisplayName))
	for fileUID := range fileUIDMapDisplayName {
		fileUids = append(fileUids, fileUID)
	}

	files, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(
		ctx,
		fileUids,
		repository.KnowledgeBaseFileColumn.UID,
		repository.KnowledgeBaseFileColumn.DisplayName,
	)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("fetching file info: %w", err),
			"Unable to load file information for search results. Please try again.",
		)
	}

	for _, file := range files {
		fileUIDMapDisplayName[file.UID] = file.DisplayName
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
			SourceFile:      fileUIDMapDisplayName[chunk.FileUID],
			ChunkMetadata:   convertToProtoChunk(chunk, req.GetNamespaceId(), req.GetKnowledgeBaseId(), chunk.FileUID.String()),
		}
		simChunks = append(simChunks, pbChunk)
	}

	logger.Info("SearchChunks response",
		zap.Int("totalChunks", len(chunks)),
		zap.Int("returnedChunks", len(simChunks)),
		zap.Int("vectorSearchResults", len(simChunksScores)))

	return &artifactpb.SearchChunksResponse{SimilarChunks: simChunks}, nil
}
