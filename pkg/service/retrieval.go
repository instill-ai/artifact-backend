package service

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/errors"
	"github.com/instill-ai/x/log"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

type SimChunk struct {
	ChunkUID uuid.UUID
	Score    float32
}

// SimilarityChunksSearch ...
func (s *service) SimilarityChunksSearch(ctx context.Context, ownerUID uuid.UUID, req *artifactpb.SimilarityChunksSearchRequest) ([]SimChunk, error) {
	log, _ := log.GetZapLogger(ctx)
	t := time.Now()
	// check if text prompt is empty
	if req.TextPrompt == "" {
		return nil, fmt.Errorf("text prompt is empty in SimilarityChunksSearch")
	}
	textVector, err := s.EmbeddingTextPipe(ctx, []string{req.TextPrompt})
	if err != nil {
		log.Error("failed to vectorize text", zap.Error(err))
		return nil, fmt.Errorf("failed to vectorize text. err: %w", err)
	}
	log.Info("vectorize text", zap.Duration("duration", time.Since(t)))
	t = time.Now()
	// get kb by kb_id and owner uid
	kb, err := s.repository.GetKnowledgeBaseByOwnerAndKbID(ctx, ownerUID, req.CatalogId)
	if err != nil {
		log.Error("failed to get knowledge base by owner and id", zap.Error(err))
		return nil, fmt.Errorf("failed to get knowledge base by owner and id. err: %w", err)
	}
	log.Info("get knowledge base by owner and id", zap.Duration("duration", time.Since(t)))

	// Search similar embeddings in KB.
	t = time.Now()

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

	var fileName string
	var fileUID uuid.UUID
	if req.GetFileUid() != "" {
		fileUID = uuid.FromStringOrNil(req.GetFileUid())
		kbfs, err := s.repository.GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{fileUID})
		switch {
		case err != nil:
			return nil, fmt.Errorf("fetching file from repository: %w", err)
		case len(kbfs) == 0:
			return nil, fmt.Errorf("fetching file from repository: %w", errors.ErrNotFound)
		}

		fileName = kbfs[0].Name
	} else if req.GetFileName() != "" {
		fileName = req.GetFileName()
		kbf, err := s.repository.GetKnowledgebaseFileByKbUIDAndFileID(ctx, kb.UID, fileName)
		if err != nil {
			return nil, fmt.Errorf("fetching kb file: %w", err)
		}

		fileUID = kbf.UID
	}

	topK := req.GetTopK()
	if topK == 0 {
		topK = 5
	}
	searchParam := SimilarVectorSearchParam{
		CollectionID: KBCollectionName(kb.UID),
		Vectors:      textVector,
		TopK:         topK,
		FileUID:      fileUID,
		FileName:     fileName,
		FileType:     string(fileType),
		ContentType:  string(contentType),
	}

	simEmbeddings, err := s.vectorDB.SimilarVectorsInCollection(ctx, searchParam)
	if err != nil {
		return nil, fmt.Errorf("searching similar embeddings in KB: %w", err)
	}

	log.Info("Search similar embeddings in KB", zap.Duration("duration", time.Since(t)))

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
			log.Error("failed to parse chunk uid", zap.Error(err))
			return nil, fmt.Errorf("failed to parse chunk uid: %v. err: %w", simEmb.SourceUID, err)
		}
		res = append(res, SimChunk{
			ChunkUID: simChunkUID,
			Score:    simEmb.Score,
		})
	}

	return res, nil
}
