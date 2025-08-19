package service

import (
	"context"
	"fmt"

	"github.com/gofrs/uuid"

	"github.com/instill-ai/artifact-backend/pkg/constant"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

type SimChunk struct {
	ChunkUID uuid.UUID
	Score    float32
}

func (s *service) SimilarityChunksSearch(ctx context.Context, ownerUID uuid.UUID, req *artifactpb.SimilarityChunksSearchRequest) ([]SimChunk, error) {
	if req.TextPrompt == "" {
		return nil, fmt.Errorf("empty text prompt")
	}

	textVector, err := s.EmbeddingTextPipe(ctx, []string{req.TextPrompt})
	if err != nil {
		return nil, fmt.Errorf("vectorizing text: %w", err)
	}

	kb, err := s.repository.GetKnowledgeBaseByOwnerAndKbID(ctx, ownerUID, req.CatalogId)
	if err != nil {
		return nil, fmt.Errorf("fetching knowledge base: %w", err)
	}

	// Search similar embeddings in KB.
	var fileType constant.FileType
	switch req.GetFileMediaType() {
	case artifactpb.FileMediaType_FILE_MEDIA_TYPE_DOCUMENT:
		fileType = constant.DocumentFileType
	case artifactpb.FileMediaType_FILE_MEDIA_TYPE_UNSPECIFIED:
		fileType = ""
	default:
		return nil, fmt.Errorf("unsupported file type: %v", req.GetFileMediaType())
	}

	var contentType constant.ContentType
	switch req.GetContentType() {
	case artifactpb.ContentType_CONTENT_TYPE_CHUNK:
		contentType = constant.ChunkContentType
	case artifactpb.ContentType_CONTENT_TYPE_SUMMARY:
		contentType = constant.SummaryContentType
	case artifactpb.ContentType_CONTENT_TYPE_AUGMENTED:
		contentType = constant.AugmentedContentType
	case artifactpb.ContentType_CONTENT_TYPE_UNSPECIFIED:
		contentType = ""
	default:
		return nil, fmt.Errorf("unsupported content type: %v", req.GetContentType())
	}

	fileUIDs := make([]uuid.UUID, 0, len(req.GetFileUids()))
	for _, uid := range req.GetFileUids() {
		fileUIDs = append(fileUIDs, uuid.FromStringOrNil(uid))
	}

	// The FileUid field is deprecated and used only when FileUids aren't
	// present.
	if len(fileUIDs) == 0 && req.GetFileUid() != "" {
		fileUIDs = append(fileUIDs, uuid.FromStringOrNil(req.GetFileUid()))
	}

	// The FileName field is deprecated and used only in absence of the file UID
	// params.
	if len(fileUIDs) == 0 && req.GetFileName() != "" {
		file, err := s.repository.GetKnowledgebaseFileByKbUIDAndFileID(ctx, kb.UID, req.GetFileName())
		if err != nil {
			return nil, fmt.Errorf("fetching kb file: %w", err)
		}

		fileUIDs = append(fileUIDs, file.UID)
	}

	topK := req.GetTopK()
	if topK == 0 {
		topK = 5
	}
	sp := SimilarVectorSearchParam{
		CollectionID: KBCollectionName(kb.UID),
		Vectors:      textVector,
		TopK:         topK,
		FileUIDs:     fileUIDs,
		FileType:     string(fileType),
		ContentType:  string(contentType),
	}

	// By default we'll filter the chunk search with the file UID metadata.
	// However, certain legacy collections lack this field. In that case, we'll
	// filter by filename. This field isn't indexed, so performance might be
	// affected.
	hasFileUID, err := s.vectorDB.CheckFileUIDMetadata(ctx, sp.CollectionID)
	if err != nil {
		return nil, fmt.Errorf("checkin collection metadata: %w", err)
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

	simEmbeddings, err := s.vectorDB.SimilarVectorsInCollection(ctx, sp)
	if err != nil {
		return nil, fmt.Errorf("searching similar embeddings in KB: %w", err)
	}

	// fetch chunks by their UIDs
	res := make([]SimChunk, 0, len(simEmbeddings))
	if len(simEmbeddings) == 0 {
		return []SimChunk{}, nil
	}
	for _, simEmb := range simEmbeddings[0] {
		if simEmb.SourceTable != s.repository.TextChunkTableName() {
			continue
		}
		simChunkUID, err := uuid.FromString(simEmb.SourceUID)
		if err != nil {
			return nil, fmt.Errorf("invalid chunk uid %s", simEmb.SourceUID)
		}
		res = append(res, SimChunk{
			ChunkUID: simChunkUID,
			Score:    simEmb.Score,
		})
	}

	return res, nil
}
