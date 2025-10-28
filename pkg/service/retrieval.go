package service

import (
	"context"
	"fmt"

	"github.com/gofrs/uuid"

	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// SimChunk represents a similarity chunk with its UID and score.
type SimChunk struct {
	ChunkUID types.TextChunkUIDType
	Score    float32
}

func (s *service) SearchChunks(ctx context.Context, ownerUID types.OwnerUIDType, req *artifactpb.SearchChunksRequest, textVector [][]float32) ([]SimChunk, error) {
	if req.TextPrompt == "" {
		return nil, fmt.Errorf("empty text prompt")
	}

	kb, err := s.repository.GetKnowledgeBaseByOwnerAndKbID(ctx, ownerUID, req.CatalogId)
	if err != nil {
		return nil, fmt.Errorf("fetching knowledge base: %w", err)
	}

	// CRITICAL FIX: Removed dual-mode routing logic that was unnecessary.
	// The atomic swap now properly renames KBs, so catalog_id always points to the correct KB:
	// - Before swap: catalog_id="my-catalog" → old production KB
	// - After swap: catalog_id="my-catalog" → new production KB (atomic rename)
	// No need to query multiple KBs - the swap is instant and users always get the right data.

	// Search similar embeddings in KB.
	var fileType types.FileType
	switch req.GetFileMediaType() {
	case artifactpb.File_FILE_MEDIA_TYPE_DOCUMENT:
		fileType = types.DocumentFileType
	case artifactpb.File_FILE_MEDIA_TYPE_UNSPECIFIED:
		fileType = ""
	default:
		return nil, fmt.Errorf("unsupported file type: %v", req.GetFileMediaType())
	}

	// Convert protobuf Chunk.Type enum to database string
	var chunkType string
	switch req.GetType() {
	case artifactpb.Chunk_TYPE_CONTENT:
		chunkType = "content"
	case artifactpb.Chunk_TYPE_SUMMARY:
		chunkType = "summary"
	case artifactpb.Chunk_TYPE_AUGMENTED:
		chunkType = "augmented"
	case artifactpb.Chunk_TYPE_UNSPECIFIED:
		chunkType = ""
	default:
		return nil, fmt.Errorf("unsupported chunk type: %v", req.GetType())
	}

	fileUIDs := make([]types.FileUIDType, 0, len(req.GetFileIds()))
	for _, uid := range req.GetFileIds() {
		fileUIDs = append(fileUIDs, types.FileUIDType(uuid.FromStringOrNil(uid)))
	}

	topK := req.GetTopK()
	if topK == 0 {
		topK = 5
	}

	// Single KB query - no dual-mode routing needed
	sp := repository.SearchVectorParam{
		CollectionID: constant.KBCollectionName(kb.UID),
		Vectors:      textVector,
		TopK:         topK,
		FileUIDs:     fileUIDs,
		ContentType:  string(fileType),
		ChunkType:    string(chunkType),
	}

	// Check file UID metadata availability
	hasFileUID, err := s.repository.CheckFileUIDMetadata(ctx, sp.CollectionID)
	if err != nil {
		return nil, fmt.Errorf("check in collection metadata: %w", err)
	}

	if !hasFileUID {
		files, err := s.repository.GetKnowledgeBaseFilesByFileUIDs(ctx, fileUIDs)
		if err != nil {
			return nil, fmt.Errorf("fetching files: %w", err)
		}

		sp.Filenames = make([]string, 0, len(files))
		for _, file := range files {
			sp.Filenames = append(sp.Filenames, file.Filename)
		}
	}

	simEmbeddings, err := s.repository.SearchVectorsInCollection(ctx, sp)
	if err != nil {
		return nil, fmt.Errorf("searching similar embeddings: %w", err)
	}

	// Process results
	var results []SimChunk
	if len(simEmbeddings) > 0 {
		for _, simEmb := range simEmbeddings[0] {
			if simEmb.SourceTable != repository.TextChunkTableName {
				continue
			}
			simChunkUID, err := uuid.FromString(simEmb.SourceUID)
			if err != nil {
				return nil, fmt.Errorf("invalid chunk uid %s", simEmb.SourceUID)
			}
			results = append(results, SimChunk{
				ChunkUID: types.TextChunkUIDType(simChunkUID),
				Score:    simEmb.Score,
			})
		}
	}

	return results, nil
}
