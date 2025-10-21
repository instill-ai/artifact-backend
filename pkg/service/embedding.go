package service

import (
	"context"
	"fmt"

	"github.com/instill-ai/artifact-backend/internal/ai"
	"github.com/instill-ai/artifact-backend/pkg/types"

	errorsx "github.com/instill-ai/x/errors"
)

// EmbedTexts embeds texts with a specific task type optimization
// This method uses the AI client's routing capabilities to automatically select the correct
// implementation based on the knowledge base's embedding configuration.
// kbUID is required and used to fetch the KB's model family configuration.
// taskType specifies the optimization (e.g., "RETRIEVAL_DOCUMENT", "RETRIEVAL_QUERY", "QUESTION_ANSWERING")
//
// This is used for synchronous user requests (retrieval/QA). For workflow embedding, see
// Worker.EmbedTextsActivity in process_file_activity.go.
func (s *service) EmbedTexts(ctx context.Context, kbUID *types.KBUIDType, texts []string, taskType string) ([][]float32, error) {
	if kbUID == nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("KB UID is required for embedding"),
			"Knowledge base context is required for query processing.",
		)
	}

	// Fetch KB's embedding configuration
	kb, err := s.repository.GetKnowledgeBaseByUID(ctx, *kbUID)
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
		kb.EmbeddingConfig.ModelFamily,
		int32(kb.EmbeddingConfig.Dimensionality),
		texts,
		taskType,
	)
}
