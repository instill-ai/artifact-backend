package openai

import (
	"context"
	"fmt"
	"time"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"

	"github.com/instill-ai/artifact-backend/pkg/ai"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

// Client implements the ai.Client interface for OpenAI
// Note: This client is designed for LEGACY EMBEDDING SUPPORT ONLY
// It only supports embedding generation (1536-dimensional vectors)
// Content conversion, caching, and chat are NOT supported - use Gemini for these
type Client struct {
	client         *openai.Client
	embeddingModel string
}

// NewClient creates a new OpenAI AI client
func NewClient(ctx context.Context, apiKey string) (*Client, error) {
	if apiKey == "" {
		err := errorsx.ErrInvalidArgument
		return nil, errorsx.AddMessage(err, "AI client configuration is missing. Please contact your administrator.")
	}

	client := openai.NewClient(
		option.WithAPIKey(apiKey),
	)

	return &Client{
		client:         &client,
		embeddingModel: DefaultEmbeddingModel,
	}, nil
}

// Name returns the client name
func (c *Client) Name() string {
	return ai.ModelFamilyOpenAI
}

// GetEmbeddingDimensionality returns the OpenAI embedding vector dimensionality (1536)
func (c *Client) GetEmbeddingDimensionality() int32 {
	return DefaultEmbeddingDimension
}

// SupportsFileType returns false - OpenAI client doesn't process files
// This client is for legacy embedding generation only (1536-dim vectors)
// Use Gemini client for content conversion and file processing
func (c *Client) SupportsFileType(fileType artifactpb.File_Type) bool {
	return false // OpenAI client doesn't process files, only generates embeddings
}

// CountTokens is not fully supported by OpenAI client for multimodal content
// OpenAI only supports text tokenization via tiktoken: https://github.com/openai/tiktoken
// For multimodal files (images, audio, video, documents), this returns an error
// Text tokenization could be implemented using tiktoken library in the future
func (c *Client) CountTokens(ctx context.Context, content []byte, fileType artifactpb.File_Type, filename string) (int, any, error) {
	return 0, nil, errorsx.AddMessage(
		fmt.Errorf("token counting not supported for multimodal content"),
		"OpenAI only supports text tokenization via tiktoken. Use Gemini/VertexAI for multimodal token counting.",
	)
}

// GetModelFamily returns this client if the model family matches, otherwise error
// For single clients, this returns self only for matching model family
func (c *Client) GetModelFamily(modelFamily string) (ai.Client, error) {
	if modelFamily == ai.ModelFamilyOpenAI {
		return c, nil
	}
	return nil, fmt.Errorf("model family %s not supported by OpenAI client", modelFamily)
}

// Close releases client resources
func (c *Client) Close() error {
	return nil
}

// ConvertToMarkdownWithoutCache is not supported by OpenAI client
func (c *Client) ConvertToMarkdownWithoutCache(ctx context.Context, content []byte, fileType artifactpb.File_Type, filename string, prompt string) (*ai.ConversionResult, error) {
	return nil, errorsx.AddMessage(
		fmt.Errorf("content conversion not supported"),
		"Content conversion is not supported by OpenAI client. Please configure Gemini for file processing.",
	)
}

// ConvertToMarkdownWithCache is not supported by OpenAI client
func (c *Client) ConvertToMarkdownWithCache(ctx context.Context, cacheName, prompt string) (*ai.ConversionResult, error) {
	return nil, errorsx.AddMessage(
		fmt.Errorf("content conversion not supported"),
		"Content conversion is not supported by OpenAI client. Please configure Gemini for file processing.",
	)
}

// CreateCache is not supported by OpenAI client
func (c *Client) CreateCache(ctx context.Context, files []ai.FileContent, ttl time.Duration) (*ai.CacheResult, error) {
	return nil, errorsx.AddMessage(
		fmt.Errorf("caching not supported"),
		"Caching is not supported by OpenAI client. Please configure Gemini for file processing.",
	)
}

// ListCaches is not supported by OpenAI client
func (c *Client) ListCaches(ctx context.Context, options *ai.CacheListOptions) (*ai.CacheListResult, error) {
	return nil, errorsx.AddMessage(
		fmt.Errorf("caching not supported"),
		"Caching is not supported by OpenAI client.",
	)
}

// GetCache is not supported by OpenAI client
func (c *Client) GetCache(ctx context.Context, cacheName string) (*ai.CacheResult, error) {
	return nil, errorsx.AddMessage(
		fmt.Errorf("caching not supported"),
		"Caching is not supported by OpenAI client.",
	)
}

// UpdateCache is not supported by OpenAI client
func (c *Client) UpdateCache(ctx context.Context, cacheName string, options *ai.CacheUpdateOptions) (*ai.CacheResult, error) {
	return nil, errorsx.AddMessage(
		fmt.Errorf("caching not supported"),
		"Caching is not supported by OpenAI client.",
	)
}

// DeleteCache is not supported by OpenAI client
func (c *Client) DeleteCache(ctx context.Context, cacheName string) error {
	return nil // No-op, nothing to delete
}
