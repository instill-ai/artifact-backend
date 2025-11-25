package gemini

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/genai"

	"github.com/instill-ai/artifact-backend/pkg/ai"

	errorsx "github.com/instill-ai/x/errors"
)

// EmbedTexts generates embeddings for a batch of texts using Gemini API directly
//
// taskType specifies the optimization:
// - TaskTypeRetrievalDocument: For text chunks being stored in vector DB
// - TaskTypeRetrievalQuery: For search queries finding similar chunks
// - TaskTypeQuestionAnswering: For questions that need answers from documents
//
// dimensionality specifies the desired embedding vector size:
// - Gemini supports 768, 1536, or 3072 dimensions
// - This parameter allows dynamic sizing based on KB configuration
//
// Best practices from https://ai.google.dev/gemini-api/docs/embeddings:
// 1. Use task-specific embeddings for better retrieval quality
// 2. Use consistent dimensionality across all embeddings in a system
// 3. Batch multiple texts together for better efficiency
func (c *Client) EmbedTexts(ctx context.Context, texts []string, taskType string, dimensionality int32) (*ai.EmbedResult, error) {
	// Validate dimensionality - Gemini supports 768, 1536, or 3072
	validDims := map[int32]bool{768: true, 1536: true, 3072: true}
	if !validDims[dimensionality] {
		return nil, errorsx.AddMessage(
			fmt.Errorf("gemini embeddings only support 768, 1536, or 3072 dimensions, got %d", dimensionality),
			"Gemini embeddings only support 768, 1536, or 3072 dimensions. Please update your knowledge base configuration to use one of these supported dimensionalities.",
		)
	}

	if len(texts) == 0 {
		return &ai.EmbedResult{
			Vectors:        [][]float32{},
			Model:          DefaultEmbeddingModel,
			Dimensionality: dimensionality,
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

	// Process texts concurrently with retry logic for better performance and reliability
	// Note: Gemini API doesn't have a batch endpoint, so we call EmbedContent for each text
	vectors := make([][]float32, len(texts))
	var wg sync.WaitGroup
	var mu sync.Mutex
	var embeddingErr error

	const maxRetries = 3
	// Limit concurrent API calls to avoid rate limiting from Gemini
	// Gemini has quotas (~60-100 requests/min depending on tier)
	const maxConcurrent = 10
	semaphore := make(chan struct{}, maxConcurrent)

	for i, text := range texts {
		wg.Add(1)
		go func(idx int, txt string) {
			defer wg.Done()

			// Acquire semaphore slot with context cancellation support
			// This prevents goroutines from blocking indefinitely if context times out
			select {
			case semaphore <- struct{}{}:
				// Got a slot, continue
			case <-ctx.Done():
				// Context cancelled, exit early
				mu.Lock()
				if embeddingErr == nil {
					embeddingErr = errorsx.AddMessage(
						ctx.Err(),
						"Embedding operation cancelled or timed out.",
					)
				}
				mu.Unlock()
				return
			}
			defer func() { <-semaphore }()

			// Retry with exponential backoff for transient failures
			var embedding []float32
			var err error
			var lastErrorType string // Track error type for better user message

			for attempt := range maxRetries {
				// Check context before each attempt
				if ctx.Err() != nil {
					err = ctx.Err()
					lastErrorType = "context_cancelled"
					break
				}

				// Create content for this text
				contents := []*genai.Content{
					genai.NewContentFromText(txt, genai.RoleUser),
				}

				// Call Gemini API for embedding with task-specific optimization
				result, apiErr := c.client.Models.EmbedContent(ctx, DefaultEmbeddingModel, contents, &genai.EmbedContentConfig{
					TaskType:             taskType,
					OutputDimensionality: genai.Ptr(dimensionality),
				})

				if apiErr != nil {
					err = fmt.Errorf("gemini API call failed for text %d: %w", idx, apiErr)
					lastErrorType = "api_error"
					// Don't retry on last attempt
					if attempt < maxRetries-1 {
						// Exponential backoff with context-aware sleep
						backoff := time.Duration(1<<uint(attempt)) * time.Second
						select {
						case <-time.After(backoff):
							// Continue to retry
						case <-ctx.Done():
							err = ctx.Err()
							lastErrorType = "context_cancelled"
							break
						}
						continue
					}
					break
				}

				// Validate response
				if len(result.Embeddings) == 0 {
					err = fmt.Errorf("no embeddings returned for text %d", idx)
					lastErrorType = "empty_response"
					if attempt < maxRetries-1 {
						backoff := time.Duration(1<<uint(attempt)) * time.Second
						select {
						case <-time.After(backoff):
						case <-ctx.Done():
							err = ctx.Err()
							lastErrorType = "context_cancelled"
							break
						}
						continue
					}
					break
				}

				emb := result.Embeddings[0]
				if len(emb.Values) == 0 {
					err = fmt.Errorf("empty embedding vector for text %d", idx)
					lastErrorType = "invalid_embedding"
					if attempt < maxRetries-1 {
						backoff := time.Duration(1<<uint(attempt)) * time.Second
						select {
						case <-time.After(backoff):
						case <-ctx.Done():
							err = ctx.Err()
							lastErrorType = "context_cancelled"
							break
						}
						continue
					}
					break
				}

				// Success! Store the embedding
				embedding = emb.Values
				err = nil
				break
			}

			// Handle errors after all retries exhausted
			if err != nil {
				mu.Lock()
				if embeddingErr == nil {
					// Provide context-specific user-friendly message
					var userMessage string
					switch lastErrorType {
					case "api_error":
						userMessage = "Unable to connect to AI service. Please check your connection and try again."
					case "empty_response":
						userMessage = "AI service returned empty response. Please try again."
					case "invalid_embedding":
						userMessage = "AI service returned invalid embeddings. Please try again."
					case "context_cancelled":
						userMessage = "Embedding operation cancelled or timed out."
					default:
						userMessage = "Unable to generate embeddings. Please try again."
					}

					embeddingErr = errorsx.AddMessage(
						fmt.Errorf("gemini embedding failed for text %d after %d attempts: %w", idx, maxRetries, err),
						userMessage,
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

	// Check context error first (timeout/cancellation takes priority)
	if ctx.Err() != nil {
		return nil, errorsx.AddMessage(
			ctx.Err(),
			"Embedding operation cancelled or timed out.",
		)
	}

	// Check if any errors occurred
	if embeddingErr != nil {
		return nil, embeddingErr
	}

	return &ai.EmbedResult{
		Vectors:        vectors,
		Model:          DefaultEmbeddingModel,
		Dimensionality: dimensionality,
	}, nil
}
