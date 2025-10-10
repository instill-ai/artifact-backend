package gemini

import (
	"context"
	"fmt"

	"google.golang.org/genai"

	"github.com/instill-ai/artifact-backend/internal/ai"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

// Provider implements the ai.Provider interface for Gemini
type Provider struct {
	client *genai.Client
}

// NewProvider creates a new Gemini AI provider
func NewProvider(ctx context.Context, apiKey string) (*Provider, error) {
	if apiKey == "" {
		err := errorsx.ErrInvalidArgument
		return nil, errorsx.AddMessage(err, "AI provider configuration is missing. Please contact your administrator.")
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

	return &Provider{
		client: client,
	}, nil
}

// Name returns the provider name
func (p *Provider) Name() string {
	return "gemini"
}

// SupportsFileType returns true if Gemini can handle the file type
func (p *Provider) SupportsFileType(fileType artifactpb.FileType) bool {
	return ai.SupportsFileType(fileType)
}

// Close releases provider resources
func (p *Provider) Close() error {
	// The genai.Client doesn't need explicit closing in the current API
	return nil
}
