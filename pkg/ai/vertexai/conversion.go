package vertexai

import (
	"context"
	"encoding/base64"
	"fmt"
	"path"
	"strings"

	"github.com/gofrs/uuid"
	"google.golang.org/genai"

	"github.com/instill-ai/artifact-backend/pkg/ai"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
	filetype "github.com/instill-ai/x/file"
)

// ConvertToMarkdownWithoutCache implements ai.Client
// Converts content to markdown using VertexAI without using a cache
func (c *Client) ConvertToMarkdownWithoutCache(ctx context.Context, content []byte, fileType artifactpb.File_Type, filename string, prompt string) (*ai.ConversionResult, error) {
	if len(content) == 0 {
		return nil, errorsx.AddMessage(errorsx.ErrInvalidArgument, "Content is empty")
	}

	mimeType := filetype.FileTypeToMimeType(fileType)

	// Upload file to object storage (GCS)
	// Generate unique path for file
	fileUID := uuid.Must(uuid.NewV4())
	objectPath := path.Join("vertexai-conversion", fileUID.String(), filename)

	// Convert to base64 for object.Storage interface
	base64Content := base64.StdEncoding.EncodeToString(content)
	err := c.storage.UploadBase64File(ctx, "", objectPath, base64Content, mimeType)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to upload file to object storage for conversion: %w", err),
			"Unable to upload file for processing. Please try again.",
		)
	}

	// Construct GCS URI
	// Get the bucket name from the storage configuration
	bucketName := c.storage.GetBucket()
	gsURI := fmt.Sprintf("gs://%s/%s", bucketName, objectPath)

	// Clean up file after conversion (defer)
	defer func() {
		_ = c.storage.DeleteFile(context.Background(), "", objectPath)
	}()

	// Create FileData part with GCS URI
	part := &genai.Part{
		FileData: &genai.FileData{
			FileURI:  gsURI,
			MIMEType: mimeType,
		},
	}

	// Create prompt content
	promptContent := []*genai.Content{
		{
			Role: genai.RoleUser,
			Parts: []*genai.Part{
				part,
				{Text: prompt},
			},
		},
	}

	// Generate response using VertexAI Models API
	resp, err := c.client.Models.GenerateContent(ctx, GetModel(), promptContent, &genai.GenerateContentConfig{
		SystemInstruction: &genai.Content{
			Parts: []*genai.Part{
				{Text: getSystemInstruction()},
			},
		},
	})
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to generate content: %w", err),
			"Unable to process file content. Please try again.",
		)
	}

	// Extract markdown from response
	markdown := extractMarkdownFromResponse(resp)

	return &ai.ConversionResult{
		Markdown:      markdown,
		PositionData:  nil, // VertexAI doesn't provide position data
		Length:        nil,
		Client:        "vertexai",
		UsageMetadata: resp.UsageMetadata,
	}, nil
}

// ConvertToMarkdownWithCache implements ai.Client
// Uses a pre-existing VertexAI cached context for content conversion
func (c *Client) ConvertToMarkdownWithCache(ctx context.Context, cacheName, prompt string) (*ai.ConversionResult, error) {
	if cacheName == "" {
		return nil, errorsx.AddMessage(errorsx.ErrInvalidArgument, "Cache name is required")
	}

	// Create prompt content
	promptContent := []*genai.Content{
		{
			Role: genai.RoleUser,
			Parts: []*genai.Part{
				{Text: prompt},
			},
		},
	}

	// Generate response using cached content
	// The cache name is passed via CachedContent field in config
	resp, err := c.client.Models.GenerateContent(ctx, GetModel(), promptContent, &genai.GenerateContentConfig{
		CachedContent: cacheName,
	})
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to generate content from cache: %w", err),
			"Unable to process content using cache. Please try again.",
		)
	}

	// Extract markdown from response
	markdown := extractMarkdownFromResponse(resp)

	return &ai.ConversionResult{
		Markdown:      markdown,
		PositionData:  nil,
		Length:        nil,
		Client:        "vertexai",
		UsageMetadata: resp.UsageMetadata,
	}, nil
}

// extractMarkdownFromResponse extracts markdown text from VertexAI response
func extractMarkdownFromResponse(resp *genai.GenerateContentResponse) string {
	if resp == nil || len(resp.Candidates) == 0 {
		return ""
	}

	var markdown strings.Builder
	for _, candidate := range resp.Candidates {
		if candidate.Content == nil {
			continue
		}
		for _, part := range candidate.Content.Parts {
			if part.Text != "" {
				markdown.WriteString(part.Text)
			}
		}
	}

	return markdown.String()
}
