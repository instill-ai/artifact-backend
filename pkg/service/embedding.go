package service

import (
	"context"
	"fmt"

	"github.com/instill-ai/artifact-backend/pkg/ai"
	"github.com/instill-ai/artifact-backend/pkg/types"

	errorsx "github.com/instill-ai/x/errors"
)

// EmbedTexts embeds texts with a specific task type optimization
// This method uses the AI client's routing capabilities to automatically select the correct
// implementation based on the knowledge base's embedding configuration.
// kbUID is required and used to fetch the KB's model family configuration.
// taskType specifies the optimization (e.g., "RETRIEVAL_DOCUMENT", "RETRIEVAL_QUERY", "QUESTION_ANSWERING")
//
// RAG Phase: RETRIEVAL - This is primarily used for synchronous user requests during the
// retrieval phase (e.g., similarity search, Q&A). Typical task type is "RETRIEVAL_QUERY"
// which optimizes embeddings for matching against stored document embeddings.
//
// For RAG INDEXING phase (embedding document chunks during ingestion), see
// Worker.EmbedTextsActivity which uses task type "RETRIEVAL_DOCUMENT".
func (s *service) EmbedTexts(ctx context.Context, kbUID *types.KBUIDType, texts []string, taskType string) ([][]float32, error) {
	if kbUID == nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("KB UID is required for embedding"),
			"Knowledge base context is required for query processing.",
		)
	}

	// Fetch KB with system configuration for embedding
	kb, err := s.repository.GetKnowledgeBaseByUIDWithConfig(ctx, *kbUID)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to fetch KB embedding config: %w", err),
			"Unable to retrieve knowledge base configuration. Please try again.",
		)
	}

	// Use AI client directly (no type assertion needed)
	return ai.EmbedTexts(
		ctx,
		s.aiClient,
		kb.SystemConfig.RAG.Embedding.ModelFamily,
		int32(kb.SystemConfig.RAG.Embedding.Dimensionality),
		texts,
		taskType,
	)
}
