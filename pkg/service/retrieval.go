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

type SimChunk struct {
	ChunkUID types.TextChunkUIDType
	Score    float32
}

func (s *service) SimilarityChunksSearch(ctx context.Context, ownerUID types.OwnerUIDType, req *artifactpb.SimilarityChunksSearchRequest, textVector [][]float32) ([]SimChunk, error) {
	if req.TextPrompt == "" {
		return nil, fmt.Errorf("empty text prompt")
	}

	kb, err := s.repository.GetKnowledgeBaseByOwnerAndKbID(ctx, ownerUID, req.CatalogId)
	if err != nil {
		return nil, fmt.Errorf("fetching knowledge base: %w", err)
	}

	// Search similar embeddings in KB.
	var fileType types.FileType
	switch req.GetFileMediaType() {
	case artifactpb.FileMediaType_FILE_MEDIA_TYPE_DOCUMENT:
		fileType = types.DocumentFileType
	case artifactpb.FileMediaType_FILE_MEDIA_TYPE_UNSPECIFIED:
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

	fileUIDs := make([]types.FileUIDType, 0, len(req.GetFileUids()))
	for _, uid := range req.GetFileUids() {
		fileUIDs = append(fileUIDs, types.FileUIDType(uuid.FromStringOrNil(uid)))
	}

	topK := req.GetTopK()
	if topK == 0 {
		topK = 5
	}
	sp := repository.SimilarVectorSearchParam{
		CollectionID: constant.KBCollectionName(kb.UID),
		Vectors:      textVector,
		TopK:         topK,
		FileUIDs:     fileUIDs,
		ContentType:  string(fileType),
		ChunkType:    string(chunkType),
	}

	// By default we'll filter the chunk search with the file UID metadata.
	// However, certain legacy collections lack this field. In that case, we'll
	// filter by filename. This field isn't indexed, so performance might be
	// affected.
	hasFileUID, err := s.repository.CheckFileUIDMetadata(ctx, sp.CollectionID)
	if err != nil {
		return nil, fmt.Errorf("check in collection metadata: %w", err)
	}

	if !hasFileUID {
		files, err := s.repository.GetKnowledgeBaseFilesByFileUIDs(ctx, fileUIDs)
		if err != nil {
			return nil, fmt.Errorf("fetching files: %w", err)
		}

		sp.FileNames = make([]string, 0, len(files))
		for _, file := range files {
			sp.FileNames = append(sp.FileNames, file.Name)
		}
	}

	simEmbeddings, err := s.repository.SimilarVectorsInCollection(ctx, sp)
	if err != nil {
		return nil, fmt.Errorf("searching similar embeddings in KB: %w", err)
	}

	// fetch chunks by their UIDs
	res := make([]SimChunk, 0, len(simEmbeddings))
	if len(simEmbeddings) == 0 {
		return []SimChunk{}, nil
	}

	for _, simEmb := range simEmbeddings[0] {
		if simEmb.SourceTable != repository.TextChunkTableName {
			continue
		}
		simChunkUID, err := uuid.FromString(simEmb.SourceUID)
		if err != nil {
			return nil, fmt.Errorf("invalid chunk uid %s", simEmb.SourceUID)
		}
		res = append(res, SimChunk{
			ChunkUID: types.TextChunkUIDType(simChunkUID),
			Score:    simEmb.Score,
		})
	}

	return res, nil
}
