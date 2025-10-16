package openai

import (
	"context"
	"fmt"
	"time"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"

	"github.com/instill-ai/artifact-backend/internal/ai"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

// Provider implements the ai.Provider interface for OpenAI
// Note: This provider is designed for LEGACY EMBEDDING SUPPORT ONLY
// It only supports embedding generation (1536-dimensional vectors)
// Content conversion, caching, and chat are NOT supported - use Gemini for these
type Provider struct {
	client         *openai.Client
	embeddingModel string
}

// NewProvider creates a new OpenAI AI provider
func NewProvider(ctx context.Context, apiKey string) (*Provider, error) {
	if apiKey == "" {
		err := errorsx.ErrInvalidArgument
		return nil, errorsx.AddMessage(err, "AI provider configuration is missing. Please contact your administrator.")
	}

	client := openai.NewClient(
		option.WithAPIKey(apiKey),
	)

	return &Provider{
		client:         &client,
		embeddingModel: ai.OpenAIEmbeddingModelDefault,
	}, nil
}

// Name returns the provider name
func (p *Provider) Name() string {
	return ai.ModelFamilyOpenAI
}

// GetEmbeddingDimensionality returns the OpenAI embedding vector dimensionality (1536)
func (p *Provider) GetEmbeddingDimensionality() int32 {
	return ai.OpenAIEmbeddingDim
}

// SupportsFileType returns false - OpenAI provider doesn't process files
// This provider is for legacy embedding generation only (1536-dim vectors)
// Use Gemini provider for content conversion and file processing
func (p *Provider) SupportsFileType(fileType artifactpb.File_Type) bool {
	return false // OpenAI provider doesn't process files, only generates embeddings
}

// Close releases provider resources
func (p *Provider) Close() error {
	return nil
}

// ConvertToMarkdownWithoutCache is not supported by OpenAI provider
func (p *Provider) ConvertToMarkdownWithoutCache(ctx context.Context, content []byte, fileType artifactpb.File_Type, filename string, prompt string) (*ai.ConversionResult, error) {
	return nil, errorsx.AddMessage(
		fmt.Errorf("content conversion not supported"),
		"Content conversion is not supported by OpenAI provider. Please configure Gemini for file processing.",
	)
}

// ConvertToMarkdownWithCache is not supported by OpenAI provider
func (p *Provider) ConvertToMarkdownWithCache(ctx context.Context, cacheName, prompt string) (*ai.ConversionResult, error) {
	return nil, errorsx.AddMessage(
		fmt.Errorf("content conversion not supported"),
		"Content conversion is not supported by OpenAI provider. Please configure Gemini for file processing.",
	)
}

// CreateCache is not supported by OpenAI provider
func (p *Provider) CreateCache(ctx context.Context, files []ai.FileContent, ttl time.Duration, systemInstruction string) (*ai.CacheResult, error) {
	return nil, errorsx.AddMessage(
		fmt.Errorf("caching not supported"),
		"Caching is not supported by OpenAI provider. Please configure Gemini for file processing.",
	)
}

// ListCaches is not supported by OpenAI provider
func (p *Provider) ListCaches(ctx context.Context, options *ai.CacheListOptions) (*ai.CacheListResult, error) {
	return nil, errorsx.AddMessage(
		fmt.Errorf("caching not supported"),
		"Caching is not supported by OpenAI provider.",
	)
}

// GetCache is not supported by OpenAI provider
func (p *Provider) GetCache(ctx context.Context, cacheName string) (*ai.CacheResult, error) {
	return nil, errorsx.AddMessage(
		fmt.Errorf("caching not supported"),
		"Caching is not supported by OpenAI provider.",
	)
}

// UpdateCache is not supported by OpenAI provider
func (p *Provider) UpdateCache(ctx context.Context, cacheName string, options *ai.CacheUpdateOptions) (*ai.CacheResult, error) {
	return nil, errorsx.AddMessage(
		fmt.Errorf("caching not supported"),
		"Caching is not supported by OpenAI provider.",
	)
}

// DeleteCache is not supported by OpenAI provider
func (p *Provider) DeleteCache(ctx context.Context, cacheName string) error {
	return nil // No-op, nothing to delete
}

// ChatWithCache is not supported by OpenAI provider
func (p *Provider) ChatWithCache(ctx context.Context, cacheName, prompt string) (*ai.ChatResult, error) {
	return nil, errorsx.AddMessage(
		fmt.Errorf("chat not supported"),
		"Chat functionality is not supported by OpenAI provider.",
	)
}

// ChatWithFiles is not supported by OpenAI provider
func (p *Provider) ChatWithFiles(ctx context.Context, files []ai.FileContent, prompt string) (*ai.ChatResult, error) {
	return nil, errorsx.AddMessage(
		fmt.Errorf("chat not supported"),
		"Chat functionality is not supported by OpenAI provider.",
	)
}
