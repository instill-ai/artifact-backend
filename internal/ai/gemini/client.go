package gemini

import (
	"context"
	"fmt"

	"google.golang.org/genai"

	"github.com/instill-ai/artifact-backend/internal/ai"

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
	return ai.GeminiEmbeddingDimDefault
}

// SupportsFileType returns true if Gemini can handle the file type
func (c *Client) SupportsFileType(fileType artifactpb.File_Type) bool {
	return ai.SupportsFileType(fileType)
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
