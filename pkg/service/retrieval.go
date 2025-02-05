package service

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/logger"

	artifactPb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

type SimChunk struct {
	ChunkUID uuid.UUID
	Score    float32
}

func (s *Service) SimilarityChunksSearch(ctx context.Context, caller uuid.UUID, requester uuid.UUID, ownerUID uuid.UUID, req *artifactPb.SimilarityChunksSearchRequest) ([]SimChunk, error) {
	log, _ := logger.GetZapLogger(ctx)
	t := time.Now()
	// check if text prompt is empty
	if req.TextPrompt == "" {
		return nil, fmt.Errorf("text prompt is empty in SimilarityChunksSearch")
	}
	textVector, err := s.EmbeddingTextPipe(ctx, caller, requester, []string{req.TextPrompt})
	if err != nil {
		log.Error("failed to vectorize text", zap.Error(err))
		return nil, fmt.Errorf("failed to vectorize text. err: %w", err)
	}
	log.Info("vectorize text", zap.Duration("duration", time.Since(t)))
	t = time.Now()
	// get kb by kb_id and owner uid
	kb, err := s.Repository.GetKnowledgeBaseByOwnerAndKbID(ctx, ownerUID, req.CatalogId)
	if err != nil {
		log.Error("failed to get knowledge base by owner and id", zap.Error(err))
		return nil, fmt.Errorf("failed to get knowledge base by owner and id. err: %w", err)
	}
	log.Info("get knowledge base by owner and id", zap.Duration("duration", time.Since(t)))
	t = time.Now()

	// search similar embeddings in kb
	simEmbeddings, err := s.MilvusClient.SearchSimilarEmbeddingsInKB(ctx, kb.UID.String(), textVector, int(req.TopK))
	if err != nil {
		log.Error("failed to search similar embeddings in kb", zap.Error(err))
		return nil, fmt.Errorf("failed to search similar embeddings in kb. err: %w", err)
	}
	log.Info("search similar embeddings in kb", zap.Duration("duration", time.Since(t)))

	// fetch chunks by their UIDs
	res := make([]SimChunk, 0, len(simEmbeddings))
	if len(simEmbeddings) == 0 {
		return []SimChunk{}, nil
	}
	for _, simEmb := range simEmbeddings[0] {
		if simEmb.SourceTable != s.Repository.TextChunkTableName() {
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
