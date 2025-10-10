package gemini

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/genai"

	"github.com/instill-ai/artifact-backend/internal/ai"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

// conversionOutput defines the output of content-to-markdown conversion
type conversionOutput struct {
	// Markdown is the converted/extracted markdown content
	Markdown string
	// UsageMetadata tracks the number of tokens consumed
	UsageMetadata *genai.GenerateContentResponseUsageMetadata
	// CacheName is the name of the cached content (if caching was enabled)
	CacheName *string
	// Model is the model that was used for conversion
	Model string
}

// ConvertToMarkdownWithoutCache implements ai.Provider
// This does direct conversion WITHOUT using a cached context.
// For cached conversion, use ConvertToMarkdownWithCache() with a pre-existing cache.
func (p *Provider) ConvertToMarkdownWithoutCache(ctx context.Context, content []byte, fileType artifactpb.FileType, filename string, prompt string) (*ai.ConversionResult, error) {
	mimeType := ai.FileTypeToMIME(fileType)

	// Retry logic for non-cached conversion
	output, err := retryConversion(ctx, 2, func() (*conversionOutput, error) {
		return p.convertToMarkdownWithoutCache(ctx, content, mimeType, prompt)
	})
	return buildConversionResult(output, err)
}

// ConvertToMarkdownWithCache implements ai.Provider
// Uses a pre-existing cached context for efficient conversion
func (p *Provider) ConvertToMarkdownWithCache(ctx context.Context, cacheName, prompt string) (*ai.ConversionResult, error) {
	// Retry logic for cached conversion
	output, err := retryConversion(ctx, 2, func() (*conversionOutput, error) {
		return p.convertToMarkdownWithCache(ctx, cacheName, prompt)
	})
	return buildConversionResult(output, err)
}

// convertToMarkdownWithoutCache converts unstructured data (documents, images, audio, video) to Markdown
// using Gemini's multimodal understanding capabilities WITHOUT using a cached context
// Uses File API for large files (>20MB) or videos for better performance and reliability
func (p *Provider) convertToMarkdownWithoutCache(ctx context.Context, content []byte, contentType, prompt string) (*conversionOutput, error) {
	// Validate input
	if len(content) == 0 {
		return nil, errorsx.AddMessage(errorsx.ErrInvalidArgument, "The file appears to be empty. Please upload a valid file.")
	}
	if contentType == "" {
		return nil, errorsx.AddMessage(errorsx.ErrInvalidArgument, "Unsupported file type. Please upload a supported file format.")
	}

	// Validate prompt
	if err := validatePrompt(prompt); err != nil {
		return nil, err
	}

	// Prepare content part using File API for large files
	contentPart, uploadedFileName, err := p.createContentPartWithFileAPI(ctx, content, contentType)
	if err != nil {
		return nil, err
	}

	// Clean up uploaded file after conversion
	if uploadedFileName != "" {
		defer func() {
			_, _ = p.client.Files.Delete(ctx, uploadedFileName, nil)
		}()
	}

	// Build request with content + prompt
	contents := []*genai.Content{
		{
			Role: genai.RoleUser,
			Parts: []*genai.Part{
				contentPart,
				{Text: prompt},
			},
		},
	}

	// Generate content without cache
	config := createGenerateContentConfig("")
	return p.generateContentAndExtractMarkdown(ctx, GetModel(), contents, config, nil)
}

// convertToMarkdownWithCache converts unstructured data to Markdown using a pre-existing cached context
// This is much more efficient than direct conversion when processing the same content multiple times
func (p *Provider) convertToMarkdownWithCache(ctx context.Context, cacheName, prompt string) (*conversionOutput, error) {
	// Validate cache name
	if cacheName == "" {
		return nil, errorsx.AddMessage(errorsx.ErrInvalidArgument, "Internal processing error. Please try again.")
	}

	// Validate prompt
	if err := validatePrompt(prompt); err != nil {
		return nil, err
	}

	// Build request with just the prompt (data is already cached)
	contents := []*genai.Content{
		{
			Role: genai.RoleUser,
			Parts: []*genai.Part{
				{Text: prompt},
			},
		},
	}

	// Generate content using cached context
	config := createGenerateContentConfig(cacheName)
	return p.generateContentAndExtractMarkdown(ctx, GetModel(), contents, config, &cacheName)
}

// generateContentAndExtractMarkdown is a common helper that calls Gemini API and extracts markdown
// This consolidates the common code between cached and non-cached conversion
func (p *Provider) generateContentAndExtractMarkdown(ctx context.Context, model string, contents []*genai.Content, config *genai.GenerateContentConfig, cacheName *string) (*conversionOutput, error) {
	// === Call Gemini API ===
	result, err := p.client.Models.GenerateContent(ctx, model, contents, config)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("gemini API call failed: %w", err),
			"AI service is temporarily unavailable. Please try again in a few moments.",
		)
	}

	// === Extract Response ===
	markdown, err := p.extractMarkdownFromResponse(result)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to extract markdown from response: %w", err),
			"Unable to process the AI response. Please try again or contact support.",
		)
	}

	// === Build Output ===
	output := &conversionOutput{
		Markdown:      markdown,
		UsageMetadata: p.extractTokenUsage(result),
		CacheName:     cacheName, // nil for non-cached, set for cached
		Model:         model,
	}

	return output, nil
}

// createContentPartWithFileAPI creates a genai.Part from the input content using File API for large files or videos
// Returns the part and the uploaded file name (empty string if inline data was used)
// Text-based content (text/plain, text/html, text/markdown, text/csv) is sent as text parts instead of blobs
func (p *Provider) createContentPartWithFileAPI(ctx context.Context, content []byte, contentType string) (*genai.Part, string, error) {
	// Text-based content (plain text, HTML, Markdown, CSV) should be sent as text, not as blobs
	// Gemini API expects text content to be in Text parts for proper understanding
	if isTextBasedContent(contentType) {
		textContent := string(content)
		part := &genai.Part{
			Text: textContent,
		}
		return part, "", nil
	}

	fileSize := len(content)
	isVideo := isVideoType(contentType)

	// Use File API if:
	// 1. File is larger than 20MB, or
	// 2. File is a video (videos always use File API for better reliability)
	useFileAPI := fileSize > MaxInlineSize || isVideo

	if useFileAPI {
		// Upload file and wait for it to become active (using shared upload logic)
		part, uploadedFileName, err := p.uploadAndWaitForFile(ctx, content, contentType)
		if err != nil {
			return nil, "", errorsx.AddMessage(
				fmt.Errorf("failed to upload file for conversion: %w", err),
				"Unable to upload file to AI service. The file may be too large or the service is busy. Please try again later.",
			)
		}
		return part, uploadedFileName, nil
	}

	// Use inline data for small binary files (PDFs, images, audio, video)
	part := &genai.Part{
		InlineData: &genai.Blob{
			MIMEType: contentType,
			Data:     content,
		},
	}

	return part, "", nil
}

// createContentPart creates a genai.Part from the input content using inline data (for testing and small files)
func (p *Provider) createContentPart(content []byte, contentType string) (*genai.Part, error) {
	// ContentType should always be set by the caller (via ai.FileTypeToMIME)
	if contentType == "" {
		err := errorsx.ErrInvalidArgument
		return nil, errorsx.AddMessage(err, "Unsupported file type. Please upload a supported file format.")
	}

	// Create inline data part for any content type
	// Gemini's multimodal API accepts documents, images, audio, and video
	part := &genai.Part{
		InlineData: &genai.Blob{
			MIMEType: contentType,
			Data:     content,
		},
	}

	return part, nil
}

// extractMarkdownFromResponse extracts the markdown text from Gemini's response
func (p *Provider) extractMarkdownFromResponse(response *genai.GenerateContentResponse) (string, error) {
	if response == nil {
		err := fmt.Errorf("response is nil")
		return "", errorsx.AddMessage(err, "AI service returned an invalid response. Please try again.")
	}

	if len(response.Candidates) == 0 {
		err := fmt.Errorf("no candidates in response")
		return "", errorsx.AddMessage(err, "AI service could not generate a response. The file may be corrupted or unsupported.")
	}

	candidate := response.Candidates[0]
	if candidate.Content == nil || len(candidate.Content.Parts) == 0 {
		err := fmt.Errorf("no content in candidate")
		return "", errorsx.AddMessage(err, "AI service returned an empty response. Please try again.")
	}

	// Concatenate all text parts
	var markdown strings.Builder
	for _, part := range candidate.Content.Parts {
		if part.Text != "" {
			markdown.WriteString(part.Text)
		}
	}

	result := markdown.String()
	if result == "" {
		err := fmt.Errorf("empty markdown result")
		return "", errorsx.AddMessage(err, "AI service could not extract any content from the file. The file may be empty or corrupted.")
	}

	// Clean up the markdown (remove code block markers if present)
	result = cleanMarkdown(result)

	return result, nil
}

// extractTokenUsage extracts token usage information from the response
func (p *Provider) extractTokenUsage(response *genai.GenerateContentResponse) *genai.GenerateContentResponseUsageMetadata {
	if response == nil || response.UsageMetadata == nil {
		return nil
	}

	return response.UsageMetadata
}

// cleanMarkdown removes unnecessary code block markers and cleans up the output
func cleanMarkdown(text string) string {
	// Remove leading/trailing whitespace
	text = strings.TrimSpace(text)

	// Remove markdown code block markers if the model accidentally included them
	if after, found := strings.CutPrefix(text, "```markdown"); found {
		text = strings.TrimPrefix(after, "\n")
	} else if after, found := strings.CutPrefix(text, "```"); found {
		text = strings.TrimPrefix(after, "\n")
	}

	if after, found := strings.CutSuffix(text, "```"); found {
		text = strings.TrimSuffix(after, "\n")
	}

	return strings.TrimSpace(text)
}

// retryConversion is a generic retry wrapper for conversion operations
func retryConversion(ctx context.Context, maxRetries int, fn func() (*conversionOutput, error)) (*conversionOutput, error) {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			backoff := time.Duration(attempt*attempt) * time.Second
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}

		output, err := fn()
		if err == nil {
			return output, nil
		}

		lastErr = err

		// Don't retry on certain errors (validation or unsupported formats)
		if strings.Contains(err.Error(), "invalid") || strings.Contains(err.Error(), "unsupported") {
			break
		}
	}

	return nil, errorsx.AddMessage(
		fmt.Errorf("conversion failed after %d attempts: %w", maxRetries+1, lastErr),
		"Failed to convert file after multiple attempts. The file may be corrupted or in an unsupported format.",
	)
}

// buildConversionResult constructs the final conversion result from internal output
func buildConversionResult(output *conversionOutput, err error) (*ai.ConversionResult, error) {
	if err != nil {
		return nil, errorsx.AddMessage(
			err,
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

// validatePrompt validates a prompt string
func validatePrompt(prompt string) error {
	if prompt == "" {
		return errorsx.AddMessage(
			errorsx.ErrInvalidArgument,
			"Internal processing error: prompt not specified. Please try again.",
		)
	}
	return nil
}

// createGenerateContentConfig creates a standard GenerateContentConfig with common settings
func createGenerateContentConfig(cacheName string) *genai.GenerateContentConfig {
	config := &genai.GenerateContentConfig{
		Temperature: genai.Ptr(float32(1.0)),
		TopP:        genai.Ptr(float32(0.95)),
	}

	if cacheName != "" {
		// Using cached content - system instruction is already baked into the cache
		config.CachedContent = cacheName
	} else {
		// Not using cache - set system instruction directly
		config.SystemInstruction = &genai.Content{
			Parts: []*genai.Part{
				{Text: GetRAGSystemInstruction()},
			},
		}
	}

	return config
}
