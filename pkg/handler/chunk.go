package handler

import (
	"context"
	"fmt"
	"sort"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
)

// convertToProtoChunk
func convertToProtoChunk(textChunk repository.TextChunkModel) *artifactpb.Chunk {
	var chunkType artifactpb.Chunk_Type

	// Convert database string to protobuf enum
	switch textChunk.ChunkType {
	case "content":
		chunkType = artifactpb.Chunk_TYPE_CONTENT
	case "summary":
		chunkType = artifactpb.Chunk_TYPE_SUMMARY
	case "augmented":
		chunkType = artifactpb.Chunk_TYPE_AUGMENTED
	default:
		chunkType = artifactpb.Chunk_TYPE_CONTENT // Default to content
	}

	pbChunk := &artifactpb.Chunk{
		Uid:            textChunk.UID.String(),
		Id:             textChunk.UID.String(),
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

	// StartPos and EndPos are deprecated in favor of MarkdownReference.
	pbChunk.StartPos = uint32(textChunk.StartPos)
	pbChunk.EndPos = uint32(textChunk.EndPos)

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
		res = append(res, convertToProtoChunk(chunk))
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

	return &artifactpb.UpdateChunkResponse{
		// Populate the response fields appropriately
		Chunk: convertToProtoChunk(*chunk),
	}, nil
}
