package handler

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
	"github.com/instill-ai/x/resource"
)

// parseChunkFromName parses a resource name of format:
// "namespaces/{namespace}/knowledge-bases/{kb}/files/{file}/chunks/{chunk}"
// and returns the namespace_id, kb_id, file_id, and chunk_id
func parseChunkFromName(name string) (namespaceID, kbID, fileID, chunkID string, err error) {
	parts := strings.Split(name, "/")
	if len(parts) == 8 && parts[0] == "namespaces" && parts[2] == "knowledge-bases" && parts[4] == "files" && parts[6] == "chunks" {
		return parts[1], parts[3], parts[5], parts[7], nil
	}
	return "", "", "", "", fmt.Errorf("invalid chunk name format, expected namespaces/{namespace}/knowledge-bases/{kb}/files/{file}/chunks/{chunk}")
}

// parseFileFromParent parses a parent resource name of format:
// "namespaces/{namespace}/knowledge-bases/{kb}/files/{file}"
// and returns the namespace_id, kb_id, and file_id
func parseFileFromParent(parent string) (namespaceID, kbID, fileID string, err error) {
	parts := strings.Split(parent, "/")
	if len(parts) == 6 && parts[0] == "namespaces" && parts[2] == "knowledge-bases" && parts[4] == "files" {
		return parts[1], parts[3], parts[5], nil
	}
	return "", "", "", fmt.Errorf("invalid parent format, expected namespaces/{namespace}/knowledge-bases/{kb}/files/{file}")
}

// parseNamespaceFromParent parses a parent resource name of format "namespaces/{namespace}"
// and returns the namespace_id
func parseNamespaceFromParent(parent string) (namespaceID string, err error) {
	parts := strings.Split(parent, "/")
	if len(parts) != 2 || parts[0] != "namespaces" {
		return "", fmt.Errorf("invalid parent format, expected namespaces/{namespace}")
	}
	return parts[1], nil
}

// convertToProtoChunk converts a TextChunkModel to a protobuf Chunk
// namespaceID, kbID and fileID are used to construct the resource name
func convertToProtoChunk(chunk repository.ChunkModel, namespaceID, kbID, fileID string) *artifactpb.Chunk {
	var chunkType artifactpb.Chunk_Type

	// Convert database string to protobuf enum
	switch chunk.ChunkType {
	case "TYPE_CONTENT":
		chunkType = artifactpb.Chunk_TYPE_CONTENT
	case "TYPE_SUMMARY":
		chunkType = artifactpb.Chunk_TYPE_SUMMARY
	case "TYPE_AUGMENTED":
		chunkType = artifactpb.Chunk_TYPE_AUGMENTED
	default:
		chunkType = artifactpb.Chunk_TYPE_CONTENT // Default to content
	}

	resourceName := fmt.Sprintf("namespaces/%s/knowledge-bases/%s/files/%s/chunks/%s",
		namespaceID, kbID, fileID, chunk.ID)
	originalFile := fmt.Sprintf("namespaces/%s/knowledge-bases/%s/files/%s", namespaceID, kbID, fileID)

	pbChunk := &artifactpb.Chunk{
		Name:         resourceName,
		Id:           chunk.ID,
		Retrievable:  chunk.Retrievable,
		Tokens:       uint32(chunk.Tokens),
		CreateTime:   timestamppb.New(*chunk.CreateTime),
		OriginalFile: originalFile,
		Type:         chunkType,
	}

	// Set markdown reference for all chunk types (content, summary, augmented)
	pbChunk.MarkdownReference = &artifactpb.Chunk_Reference{
		Start: &artifactpb.File_Position{
			Unit:        artifactpb.File_Position_UNIT_CHARACTER,
			Coordinates: []uint32{uint32(chunk.StartPos)},
		},
		End: &artifactpb.File_Position{
			Unit:        artifactpb.File_Position_UNIT_CHARACTER,
			Coordinates: []uint32{uint32(chunk.EndPos)},
		},
	}

	// Add page reference if available
	if chunk.Reference != nil && chunk.Reference.PageRange[0] != 0 && chunk.Reference.PageRange[1] != 0 {
		// We only extract one kind of reference for now. When more file
		// types are supported, we'll need to inspect the reference object
		// to build the response correctly.
		pbChunk.Reference = &artifactpb.Chunk_Reference{
			Start: &artifactpb.File_Position{
				Unit:        artifactpb.File_Position_UNIT_PAGE,
				Coordinates: []uint32{chunk.Reference.PageRange[0]},
			},
			End: &artifactpb.File_Position{
				Unit:        artifactpb.File_Position_UNIT_PAGE,
				Coordinates: []uint32{chunk.Reference.PageRange[1]},
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

	// Parse resource name to get namespace_id, kb_id, file_id, and chunk_id
	namespaceID, kbID, fileID, chunkID, err := parseChunkFromName(req.GetName())
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("parsing chunk name: %w", err),
			"Invalid chunk name format. Expected: namespaces/{namespace}/knowledge-bases/{knowledge_base}/files/{file}/chunks/{chunk}",
		)
	}

	// Validate namespace exists
	if _, err := ph.service.GetNamespaceByNsID(ctx, namespaceID); err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("fetching namespace: %w", err),
			"Unable to access the specified namespace. Please check the namespace ID and try again.",
		)
	}

	// Get file to get knowledge base UID for ACL check
	kbfs, err := ph.service.Repository().GetFilesByFileIDs(ctx, []string{fileID})
	if err != nil || len(kbfs) == 0 {
		return nil, errorsx.AddMessage(
			fmt.Errorf("fetching file: %w", err),
			"Unable to access the specified file. Please check the file ID and try again.",
		)
	}
	kbf := &kbfs[0]

	// ACL - check user has access to any of the knowledge bases associated with this file
	kbUIDs, err := ph.service.Repository().GetKnowledgeBaseUIDsForFile(ctx, kbf.UID)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("getting file KB associations: %w", err),
			"Unable to verify file ownership. Please try again.",
		)
	}
	if len(kbUIDs) == 0 {
		return nil, errorsx.AddMessage(
			errorsx.ErrNotFound,
			"File is not associated with any knowledge base.",
		)
	}

	// Check permission on any associated KB
	granted := false
	for _, kbUID := range kbUIDs {
		hasPermission, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", kbUID, "reader")
		if err != nil {
			logger.Warn("failed to check permission for KB", zap.Error(err), zap.String("kbUID", kbUID.String()))
			continue
		}
		if hasPermission {
			granted = true
			break
		}
	}
	if !granted {
		return nil, errorsx.AddMessage(
			errorsx.ErrUnauthenticated,
			"You don't have permission to access this knowledge base. Please contact the owner for access.",
		)
	}

	// Get chunk metadata from repository
	chunkUUID, err := uuid.FromString(chunkID)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("invalid chunk ID: %w", err),
			"Invalid chunk ID format. Please provide a valid UUID.",
		)
	}

	chunks, err := ph.service.Repository().GetTextChunksByUIDs(ctx, []types.ChunkUIDType{types.ChunkUIDType(chunkUUID)})
	if err != nil || len(chunks) == 0 {
		logger.Error("chunk not found", zap.Error(err), zap.String("chunkID", chunkID))
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

	// Convert to protobuf format - use namespace ID from name parsing
	pbChunk := convertToProtoChunk(chunk, namespaceID, kbID, fileID)

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

	// Parse parent to get namespace_id, kb_id, and file_id
	namespaceID, kbID, fileID, err := parseFileFromParent(req.GetParent())
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("parsing parent: %w", err),
			"Invalid parent format. Expected: namespaces/{namespace}/knowledge-bases/{knowledge_base}/files/{file}",
		)
	}

	// Look up file by hash-based ID (also supports UUID lookup internally)
	kbfs, err := ph.service.Repository().GetFilesByFileIDs(ctx, []string{fileID})
	if err != nil {
		logger.Error("failed to get knowledge base files by file ids", zap.Error(err))
		return nil, fmt.Errorf("failed to get knowledge base files: %w", err)
	}
	if len(kbfs) == 0 {
		logger.Error("no files found for the given file id", zap.String("fileId", fileID))
		return nil, fmt.Errorf("file not found: %s. err: %w", fileID, errorsx.ErrNotFound)
	}

	kbf := &kbfs[0]
	fileUID := uuid.UUID(kbf.UID)
	_ = namespaceID // namespaceID can be used for validation if needed

	// ACL - check user's permission to read any of the knowledge bases associated with this file
	kbUIDs, err := ph.service.Repository().GetKnowledgeBaseUIDsForFile(ctx, kbf.UID)
	if err != nil {
		logger.Error("failed to get KB associations for file", zap.Error(err))
		return nil, fmt.Errorf("getting file KB associations: %w", err)
	}
	if len(kbUIDs) == 0 {
		return nil, fmt.Errorf("%w: file not associated with any knowledge base", errorsx.ErrNotFound)
	}
	granted := false
	for _, kbUID := range kbUIDs {
		hasPermission, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", kbUID, "reader")
		if err != nil {
			logger.Warn("failed to check permission for KB", zap.Error(err), zap.String("kbUID", kbUID.String()))
			continue
		}
		if hasPermission {
			granted = true
			break
		}
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
		res = append(res, convertToProtoChunk(chunk, namespaceID, kbID, fileID))
	}

	return &artifactpb.ListChunksResponse{
		Chunks: res,
	}, nil
}

// UpdateChunk updates the retrievable of a chunk
func (ph *PublicHandler) UpdateChunk(ctx context.Context, req *artifactpb.UpdateChunkRequest) (*artifactpb.UpdateChunkResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Parse resource name to get namespace_id, kb_id, file_id, and chunk_id
	namespaceID, kbID, fileID, chunkID, err := parseChunkFromName(req.GetName())
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("parsing chunk name: %w", err),
			"Invalid chunk name format. Expected: namespaces/{namespace}/knowledge-bases/{knowledge_base}/files/{file}/chunks/{chunk}",
		)
	}

	chunks, err := ph.service.Repository().GetTextChunksByUIDs(ctx, []types.ChunkUIDType{types.ChunkUIDType(uuid.FromStringOrNil(chunkID))})
	if err != nil {
		logger.Error("failed to get chunks by uids", zap.Error(err))
		return nil, fmt.Errorf("failed to get chunks by uids: %w", err)
	}
	if len(chunks) == 0 {
		logger.Error("no chunks found for the given chunk uids")
		return nil, fmt.Errorf("no chunks found for the given chunk uids: %v. err: %w", chunkID, errorsx.ErrNotFound)
	}
	chunk := &chunks[0]
	// ACL - check user's permission to write knowledge base of chunks
	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", chunk.KnowledgeBaseUID, "writer")
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

	chunk, err = ph.service.Repository().UpdateTextChunk(ctx, chunkID, update)
	if err != nil {
		logger.Error("failed to update text chunk", zap.Error(err))
		return nil, fmt.Errorf("failed to update text chunk: %w", err)
	}

	return &artifactpb.UpdateChunkResponse{
		Chunk: convertToProtoChunk(*chunk, namespaceID, kbID, fileID),
	}, nil
}

// SearchChunks performs vector similarity search across chunks in a knowledge base.
// Returns the top-K most similar chunks to a text prompt.
func (ph *PublicHandler) SearchChunks(
	ctx context.Context,
	req *artifactpb.SearchChunksRequest,
) (*artifactpb.SearchChunksResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Parse parent to get namespace_id
	namespaceID, err := parseNamespaceFromParent(req.GetParent())
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("parsing parent: %w", err),
			"Invalid parent format. Expected: namespaces/{namespace}",
		)
	}

	// Extract KB ID from resource name: namespaces/{namespace}/knowledge-bases/{kb}
	kbID := resource.ExtractResourceID(req.GetKnowledgeBase())
	if kbID == "" {
		return nil, errorsx.AddMessage(
			fmt.Errorf("missing knowledge base"),
			"Knowledge base resource name is required in format: namespaces/{namespace}/knowledge-bases/{knowledge_base}",
		)
	}
	logger = logger.With(
		zap.String("namespace", namespaceID),
		zap.String("knowledge_base", kbID),
	)

	// Get namespace
	ns, err := ph.service.GetNamespaceByNsID(ctx, namespaceID)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("fetching namespace: %w", err),
			"Unable to access the specified namespace. Please check the namespace ID and try again.",
		)
	}

	// Get knowledge base
	ownerUID := ns.NsUID
	kb, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ownerUID, kbID)
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
	var chunkUIDs []types.ChunkUIDType
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
		chunkFilePaths = append(chunkFilePaths, chunk.StoragePath)
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

	files, err := ph.service.Repository().GetFilesByFileUIDs(
		ctx,
		fileUids,
		repository.FileColumn.UID,
		repository.FileColumn.DisplayName,
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

		// Build full resource names for chunk and file
		chunkName := fmt.Sprintf("namespaces/%s/knowledge-bases/%s/files/%s/chunks/%s", namespaceID, kbID, chunk.FileUID.String(), chunk.ID)
		fileName := fmt.Sprintf("namespaces/%s/knowledge-bases/%s/files/%s", namespaceID, kbID, chunk.FileUID.String())

		pbChunk := &artifactpb.SimilarityChunk{
			Chunk:           chunkName,
			SimilarityScore: float32(simChunksScores[i].Score),
			TextContent:     string(chunkContents[i].Content),
			File:            fileName,
			ChunkMetadata:   convertToProtoChunk(chunk, namespaceID, kbID, chunk.FileUID.String()),
		}
		simChunks = append(simChunks, pbChunk)
	}

	logger.Info("SearchChunks response",
		zap.Int("totalChunks", len(chunks)),
		zap.Int("returnedChunks", len(simChunks)),
		zap.Int("vectorSearchResults", len(simChunksScores)))

	return &artifactpb.SearchChunksResponse{SimilarChunks: simChunks}, nil
}
