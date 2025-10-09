package gemini

import (
	"context"
	"fmt"
	"time"

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

// ConvertToMarkdown implements ai.Provider
// Note: This does direct conversion WITHOUT creating a cache.
// For cached conversion, use ConvertToMarkdownWithCache() with a pre-existing cache.
func (p *Provider) ConvertToMarkdown(ctx context.Context, content []byte, fileType artifactpb.FileType, filename string, prompt string) (*ai.ConversionResult, error) {
	mimeType := ai.FileTypeToMIME(fileType)
	input := &ConversionInput{
		Content:      content,
		ContentType:  mimeType,
		Filename:     filename,
		Model:        DefaultConversionModel,
		CustomPrompt: &prompt,
	}

	output, err := p.convertToMarkdownWithRetry(ctx, input, 2)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("gemini conversion failed: %w", err),
			"Failed to convert file to markdown. Please try again or contact support if the problem persists.",
		)
	}

	return &ai.ConversionResult{
		Markdown:      output.Markdown,
		Length:        []uint32{uint32(len(output.Markdown))},
		Provider:      "gemini",
		PositionData:  nil,
		UsageMetadata: output.UsageMetadata,
	}, nil
}

// ConvertToMarkdownWithCache implements ai.Provider
func (p *Provider) ConvertToMarkdownWithCache(ctx context.Context, cacheName, prompt string) (*ai.ConversionResult, error) {
	output, err := p.convertToMarkdownWithCache(ctx, cacheName, prompt)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("gemini conversion with cache failed: %w", err),
			"Failed to convert file to markdown. Please try again or contact support if the problem persists.",
		)
	}

	return &ai.ConversionResult{
		Markdown:      output.Markdown,
		Length:        []uint32{uint32(len(output.Markdown))},
		Provider:      "gemini",
		PositionData:  nil,
		UsageMetadata: output.UsageMetadata,
	}, nil
}

// CreateCache implements ai.Provider
func (p *Provider) CreateCache(ctx context.Context, content []byte, fileType artifactpb.FileType, filename string, ttl time.Duration) (*ai.CacheResult, error) {
	mimeType := ai.FileTypeToMIME(fileType)
	displayName := fmt.Sprintf("cache-%s", filename)

	input := &CacheInput{
		Model:       DefaultConversionModel,
		Content:     content,
		ContentType: mimeType,
		Filename:    filename,
		TTL:         &ttl,
		DisplayName: &displayName,
	}

	output, err := p.createCache(ctx, input)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to create cache: %w", err),
			"Unable to optimize file processing. The file will be processed without optimization.",
		)
	}

	return &ai.CacheResult{
		CacheName:     output.CacheName,
		Model:         output.Model,
		CreateTime:    output.CreateTime,
		ExpireTime:    output.ExpireTime,
		UsageMetadata: output.UsageMetadata,
	}, nil
}

// DeleteCache implements ai.Provider
func (p *Provider) DeleteCache(ctx context.Context, cacheName string) error {
	if cacheName == "" {
		return nil
	}
	return p.deleteCache(ctx, cacheName)
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
