package vertexai

import (
	"context"
	"fmt"

	"google.golang.org/genai"

	"github.com/instill-ai/artifact-backend/pkg/ai"
	"github.com/instill-ai/artifact-backend/pkg/ai/gemini"

	errorsx "github.com/instill-ai/x/errors"
)

// EmbedTexts implements ai.Client
// Generates embeddings using VertexAI embedding models
func (c *Client) EmbedTexts(ctx context.Context, texts []string, taskType string, dimensionality int32) (*ai.EmbedResult, error) {
	if len(texts) == 0 {
		return nil, errorsx.AddMessage(errorsx.ErrInvalidArgument, "No texts provided for embedding")
	}

	// Use default embedding model
	embeddingModel := GetEmbeddingModel()

	// Convert task type to VertexAI format
	vertexAITaskType := convertTaskType(taskType)

	// Generate embeddings for all texts
	vectors := make([][]float32, 0, len(texts))

	for _, text := range texts {
		content := &genai.Content{
			Parts: []*genai.Part{
				{Text: text},
			},
		}

		config := &genai.EmbedContentConfig{
			TaskType: vertexAITaskType,
		}

		// Set output dimensionality if specified
		if dimensionality > 0 {
			dim := dimensionality
			config.OutputDimensionality = &dim
		}

		// Generate embedding for this text
		resp, err := c.client.Models.EmbedContent(ctx, embeddingModel, []*genai.Content{content}, config)
		if err != nil {
			return nil, errorsx.AddMessage(
				fmt.Errorf("failed to generate embedding: %w", err),
				"Unable to generate embeddings. Please try again.",
			)
		}

		// Extract embedding vectors
		if resp != nil && len(resp.Embeddings) > 0 {
			for _, emb := range resp.Embeddings {
				if len(emb.Values) > 0 {
					vectors = append(vectors, emb.Values)
				}
			}
		}
	}

	// Determine actual dimensionality from first vector
	actualDim := int32(0)
	if len(vectors) > 0 && len(vectors[0]) > 0 {
		actualDim = int32(len(vectors[0]))
	}

	return &ai.EmbedResult{
		Vectors:        vectors,
		Model:          embeddingModel,
		Dimensionality: actualDim,
	}, nil
}

// convertTaskType converts CE task type to VertexAI TaskType string
func convertTaskType(taskType string) string {
	switch taskType {
	case gemini.TaskTypeRetrievalDocument:
		return "RETRIEVAL_DOCUMENT"
	case gemini.TaskTypeRetrievalQuery:
		return "RETRIEVAL_QUERY"
	case gemini.TaskTypeQuestionAnswering:
		// VertexAI doesn't have a separate QA task type, use retrieval query
		return "RETRIEVAL_QUERY"
	default:
		// Default to retrieval document
		return "RETRIEVAL_DOCUMENT"
	}
}
