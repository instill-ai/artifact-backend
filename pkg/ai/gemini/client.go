package gemini

import (
	"context"
	"fmt"

	"google.golang.org/genai"

	"github.com/instill-ai/artifact-backend/pkg/ai"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

// Client implements the ai.Client interface for Gemini
type Client struct {
	client *genai.Client
}

// NewClient creates a new Gemini AI client
func NewClient(ctx context.Context, apiKey string) (*Client, error) {
	if apiKey == "" {
		err := errorsx.ErrInvalidArgument
		return nil, errorsx.AddMessage(err, "AI client configuration is missing. Please contact your administrator.")
	}

	client, err := genai.NewClient(ctx, &genai.ClientConfig{
		APIKey:  apiKey,
		Backend: genai.BackendGeminiAPI,
	})
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to create Gemini client: %w", err),
			"Unable to connect to AI service. Please try again later.",
		)
	}

	return &Client{
		client: client,
	}, nil
}

// Name returns the client name
func (c *Client) Name() string {
	return "gemini"
}

// GetEmbeddingDimensionality returns the Gemini embedding vector dimensionality (3072)
func (c *Client) GetEmbeddingDimensionality() int32 {
	return DefaultEmbeddingDimension
}

// SupportsFileType returns true if Gemini can handle the file type
func (c *Client) SupportsFileType(fileType artifactpb.File_Type) bool {
	return ai.SupportsFileType(fileType)
}

// CountTokens counts the total tokens for the given content without processing it
// Uses the Gemini CountTokens API: https://cloud.google.com/vertex-ai/generative-ai/docs/multimodal/get-token-count
func (c *Client) CountTokens(ctx context.Context, content []byte, fileType artifactpb.File_Type, filename string) (int, any, error) {
	// Get MIME type for the file
	mimeType := ai.FileTypeToMIME(fileType)

	// Build file part from content (using File API for large files or inline for small files)
	filePart, uploadedFileName, err := c.createContentPartWithFileAPI(ctx, content, mimeType)
	if err != nil {
		return 0, nil, errorsx.AddMessage(
			fmt.Errorf("failed to build file part for token counting: %w", err),
			"Unable to prepare content for token counting.",
		)
	}

	// Defer cleanup of uploaded file if File API was used
	if uploadedFileName != "" {
		defer func() {
			_, _ = c.client.Files.Delete(ctx, uploadedFileName, nil)
		}()
	}

	// Create contents for token counting
	contents := []*genai.Content{
		{
			Parts: []*genai.Part{filePart},
			Role:  "user",
		},
	}

	// Count tokens using the Gemini API
	resp, err := c.client.Models.CountTokens(ctx, DefaultModel, contents, nil)
	if err != nil {
		return 0, nil, errorsx.AddMessage(
			fmt.Errorf("failed to count tokens: %w", err),
			"Unable to count tokens. Please try again.",
		)
	}

	// Return total tokens and the full response as usage metadata
	return int(resp.TotalTokens), resp, nil
}

// GetModelFamily returns this client if the model family matches, otherwise error
// For single clients, this returns self only for matching model family
func (c *Client) GetModelFamily(modelFamily string) (ai.Client, error) {
	if modelFamily == ai.ModelFamilyGemini {
		return c, nil
	}
	return nil, fmt.Errorf("model family %s not supported by Gemini client", modelFamily)
}

// Close releases client resources
func (c *Client) Close() error {
	// The genai.Client doesn't need explicit closing in the current API
	return nil
}
