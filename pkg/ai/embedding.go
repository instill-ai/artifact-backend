package ai

import (
	"context"
	"fmt"

	errorsx "github.com/instill-ai/x/errors"
)

// EmbedTexts generates embeddings using the specified client, model family, and dimensionality.
// This is a shared helper used by both Worker activities and Service layer methods.
//
// Parameters:
//   - client: The AI client with routing capabilities
//   - modelFamily: The model family to use (e.g., "gemini", "openai")
//   - dimensionality: The desired embedding vector size (e.g., 1536, 3072)
//   - texts: The texts to embed
//   - taskType: The embedding task type (RETRIEVAL_DOCUMENT, RETRIEVAL_QUERY, QUESTION_ANSWERING)
//
// Returns:
//   - The embedding vectors
//   - Error if any step fails
func EmbedTexts(
	ctx context.Context,
	client Client,
	modelFamily string,
	dimensionality int32,
	texts []string,
	taskType string,
) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}

	if client == nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("AI client not initialized"),
			"AI client is not configured. Please configure Gemini or OpenAI API key in your settings.",
		)
	}

	// Use client's routing capability to get the appropriate implementation
	selectedClient, err := client.GetModelFamily(modelFamily)
	if err != nil {
		return nil, err // Error already has user-friendly message
	}

	// Generate embeddings using selected client with specified dimensionality
	result, err := selectedClient.EmbedTexts(ctx, texts, taskType, dimensionality)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to generate embeddings: %w", err),
			"Unable to generate embeddings. Please try again.",
		)
	}

	return result.Vectors, nil
}
