package ai

import (
	"context"
	"fmt"
	"time"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

// compositeClient wraps multiple clients and routes requests based on model family
type compositeClient struct {
	clients       map[string]Client
	defaultClient Client
}

// NewCompositeClient creates a composite client from a map of clients
// This allows external callers to initialize individual clients and compose them
func NewCompositeClient(clients map[string]Client, defaultModelFamily string) (Client, error) {
	if len(clients) == 0 {
		return nil, fmt.Errorf("at least one client must be provided")
	}

	// If only one client, return it directly (no need for composite wrapper)
	if len(clients) == 1 {
		for _, client := range clients {
			return client, nil
		}
	}

	// Determine default client
	defaultClient, ok := clients[defaultModelFamily]
	if !ok {
		// Use first available client
		for _, client := range clients {
			defaultClient = client
			break
		}
	}

	// Multiple clients: return composite
	return &compositeClient{
		clients:       clients,
		defaultClient: defaultClient,
	}, nil
}

// Name returns "composite" to indicate this is a multi-client
func (c *compositeClient) Name() string {
	return "composite"
}

// GetModelFamily returns the client for a specific model family
func (c *compositeClient) GetModelFamily(modelFamily string) (Client, error) {
	client, ok := c.clients[modelFamily]
	if !ok {
		return nil, errorsx.AddMessage(
			fmt.Errorf("unsupported model family: %s", modelFamily),
			fmt.Sprintf("Model family %s is not configured. Please contact your administrator.", modelFamily),
		)
	}
	if client == nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("client for model family %s is not initialized", modelFamily),
			fmt.Sprintf("%s client is not configured. Please configure the API key in your settings.", modelFamily),
		)
	}
	return client, nil
}

// ConvertToMarkdownWithoutCache delegates to the default client
func (c *compositeClient) ConvertToMarkdownWithoutCache(ctx context.Context, content []byte, fileType artifactpb.File_Type, filename string, prompt string) (*ConversionResult, error) {
	return c.defaultClient.ConvertToMarkdownWithoutCache(ctx, content, fileType, filename, prompt)
}

// ConvertToMarkdownWithCache delegates to the default client
func (c *compositeClient) ConvertToMarkdownWithCache(ctx context.Context, cacheName, prompt string) (*ConversionResult, error) {
	return c.defaultClient.ConvertToMarkdownWithCache(ctx, cacheName, prompt)
}

// CreateCache delegates to the default client
func (c *compositeClient) CreateCache(ctx context.Context, files []FileContent, ttl time.Duration, systemInstruction string) (*CacheResult, error) {
	return c.defaultClient.CreateCache(ctx, files, ttl, systemInstruction)
}

// ListCaches delegates to the default client
func (c *compositeClient) ListCaches(ctx context.Context, options *CacheListOptions) (*CacheListResult, error) {
	return c.defaultClient.ListCaches(ctx, options)
}

// GetCache delegates to the default client
func (c *compositeClient) GetCache(ctx context.Context, cacheName string) (*CacheResult, error) {
	return c.defaultClient.GetCache(ctx, cacheName)
}

// UpdateCache delegates to the default client
func (c *compositeClient) UpdateCache(ctx context.Context, cacheName string, options *CacheUpdateOptions) (*CacheResult, error) {
	return c.defaultClient.UpdateCache(ctx, cacheName, options)
}

// DeleteCache delegates to the default client
func (c *compositeClient) DeleteCache(ctx context.Context, cacheName string) error {
	return c.defaultClient.DeleteCache(ctx, cacheName)
}

// EmbedTexts generates embeddings using the default client
// For model-family-specific embedding, use GetClientForModelFamily first
func (c *compositeClient) EmbedTexts(ctx context.Context, texts []string, taskType string, dimensionality int32) (*EmbedResult, error) {
	return c.defaultClient.EmbedTexts(ctx, texts, taskType, dimensionality)
}

// GetEmbeddingDimensionality returns the default client's dimensionality
func (c *compositeClient) GetEmbeddingDimensionality() int32 {
	return c.defaultClient.GetEmbeddingDimensionality()
}

// SupportsFileType checks if the default client supports the file type
func (c *compositeClient) SupportsFileType(fileType artifactpb.File_Type) bool {
	return c.defaultClient.SupportsFileType(fileType)
}

// Close releases all client resources
func (c *compositeClient) Close() error {
	var errors []error
	for family, client := range c.clients {
		if client != nil {
			if err := client.Close(); err != nil {
				errors = append(errors, fmt.Errorf("failed to close %s client: %w", family, err))
			}
		}
	}

	if len(errors) > 0 {
		// Return first error
		return errors[0]
	}

	return nil
}
