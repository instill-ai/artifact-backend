package service

import (
	"context"
	"fmt"

	"github.com/gofrs/uuid"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	artifactv1alpha "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	"go.uber.org/zap"
)

type SimChunk struct {
	ChunkUID uuid.UUID
	Score    float32
}

func (s *Service) SimilarityChunksSearch(ctx context.Context, caller uuid.UUID, ownerUID string, req *artifactv1alpha.SimilarityChunksSearchRequest) ([]SimChunk, error) {
	log, _ := logger.GetZapLogger(ctx)
	log.Info("SimilarityChunksSearch")
	textVector, err := s.VectorizeText(ctx, caller, []string{req.TextPrompt})
	fmt.Println("textVector", textVector[0][:10])
	if err != nil {
		log.Error("failed to vectorize text", zap.Error(err))
		return nil, fmt.Errorf("failed to vectorize text. err: %w", err)
	}

	// get kb by kb_id and owner uid
	kb, err := s.Repository.GetKnowledgeBaseByOwnerAndKbID(ctx, ownerUID, req.KbId)
	if err != nil {
		log.Error("failed to get knowledge base by owner and id", zap.Error(err))
		return nil, fmt.Errorf("failed to get knowledge base by owner and id. err: %w", err)

	}

	// search similar embeddings in kb
	simEmbeddings, err := s.MilvusClient.SearchSimilarEmbeddingsInKB(ctx, kb.UID.String(), textVector, int(req.Topk))
	if err != nil {
		log.Error("failed to search similar embeddings in kb", zap.Error(err))
		return nil, fmt.Errorf("failed to search similar embeddings in kb. err: %w", err)
	}

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
