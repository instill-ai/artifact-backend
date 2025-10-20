package openai

import (
	"context"
	"fmt"
	"sync"

	"github.com/openai/openai-go/v3"

	"github.com/instill-ai/artifact-backend/internal/ai"

	errorsx "github.com/instill-ai/x/errors"
)

// EmbedTexts generates embeddings for a batch of texts using OpenAI API
// Note: OpenAI doesn't support task-specific embeddings like Gemini, so taskType is ignored
// Note: OpenAI's text-embedding-3-small only supports 1536 dimensions
func (c *Client) EmbedTexts(ctx context.Context, texts []string, taskType string, dimensionality int32) (*ai.EmbedResult, error) {
	// Validate dimensionality - OpenAI only supports 1536
	if dimensionality != ai.OpenAIEmbeddingDim {
		return nil, errorsx.AddMessage(
			fmt.Errorf("openai text-embedding-3-small only supports %d dimensions, got %d", ai.OpenAIEmbeddingDim, dimensionality),
			fmt.Sprintf("OpenAI embeddings only support %d dimensions. Please update your knowledge base configuration to use dimensionality %d, or switch to Gemini which supports 768, 1536, or 3072 dimensions.", ai.OpenAIEmbeddingDim, ai.OpenAIEmbeddingDim),
		)
	}

	if len(texts) == 0 {
		return &ai.EmbedResult{
			Vectors:        [][]float32{},
			Model:          c.embeddingModel,
			Dimensionality: ai.OpenAIEmbeddingDim,
		}, nil
	}

	// Validate inputs
	for i, text := range texts {
		if text == "" {
			return nil, errorsx.AddMessage(
				fmt.Errorf("text at index %d is empty", i),
				"Cannot generate embeddings for empty text",
			)
		}
	}

	// Process texts concurrently for better performance
	vectors := make([][]float32, len(texts))
	var wg sync.WaitGroup
	var mu sync.Mutex
	var embeddingErr error

	const maxRetries = 3

	for i, text := range texts {
		wg.Add(1)
		go func(idx int, txt string) {
			defer wg.Done()

			// Retry with exponential backoff for transient failures
			var embedding []float32
			var err error

			for attempt := 0; attempt < maxRetries; attempt++ {
				// Call OpenAI API for embedding
				response, apiErr := c.client.Embeddings.New(ctx, openai.EmbeddingNewParams{
					Input: openai.EmbeddingNewParamsInputUnion{
						OfArrayOfStrings: []string{txt},
					},
					Model: c.embeddingModel,
				})

				if apiErr != nil {
					err = fmt.Errorf("openai API call failed for text %d: %w", idx, apiErr)
					if attempt < maxRetries-1 {
						continue
					}
					break
				}

				// Validate response
				if len(response.Data) == 0 {
					err = fmt.Errorf("no embeddings returned for text %d", idx)
					if attempt < maxRetries-1 {
						continue
					}
					break
				}

				emb := response.Data[0]
				if len(emb.Embedding) == 0 {
					err = fmt.Errorf("empty embedding vector for text %d", idx)
					if attempt < maxRetries-1 {
						continue
					}
					break
				}

				// Convert float64 to float32
				embedding = make([]float32, len(emb.Embedding))
				for j, val := range emb.Embedding {
					embedding[j] = float32(val)
				}

				err = nil
				break
			}

			// Handle errors after all retries exhausted
			if err != nil {
				mu.Lock()
				if embeddingErr == nil {
					embeddingErr = errorsx.AddMessage(
						fmt.Errorf("openai embedding failed for text %d after %d attempts: %w", idx, maxRetries, err),
						"Unable to generate embeddings. Please try again.",
					)
				}
				mu.Unlock()
				return
			}

			// Store result at correct index to preserve order
			mu.Lock()
			vectors[idx] = embedding
			mu.Unlock()
		}(i, text)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Check if any errors occurred
	if embeddingErr != nil {
		return nil, embeddingErr
	}

	return &ai.EmbedResult{
		Vectors:        vectors,
		Model:          c.embeddingModel,
		Dimensionality: ai.OpenAIEmbeddingDim,
	}, nil
}
