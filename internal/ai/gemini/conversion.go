package gemini

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/genai"

	errorsx "github.com/instill-ai/x/errors"
)

// convertToMarkdownWithoutCache converts unstructured data (documents, images, audio, video) to Markdown
// using Gemini's multimodal understanding capabilities WITHOUT using a cached context
// Uses File API for large files (>20MB) or videos for better performance and reliability
func (p *Provider) convertToMarkdownWithoutCache(ctx context.Context, input *ConversionInput) (*ConversionOutput, error) {
	// === Validate Input ===
	if input == nil {
		err := errorsx.ErrInvalidArgument
		return nil, errorsx.AddMessage(err, "Invalid request. Please ensure the file is properly uploaded.")
	}
	if len(input.Content) == 0 {
		err := errorsx.ErrInvalidArgument
		return nil, errorsx.AddMessage(err, "The file appears to be empty. Please upload a valid file.")
	}

	// === Set Defaults ===
	model := input.Model
	if model == "" {
		model = DefaultConversionModel
	}

	// Prompt must be provided by caller (no default assumption)
	if input.CustomPrompt == nil || *input.CustomPrompt == "" {
		err := errorsx.ErrInvalidArgument
		return nil, errorsx.AddMessage(err, "Internal processing error: prompt not specified. Please try again.")
	}
	prompt := *input.CustomPrompt

	// === Prepare Content ===
	// Create content part (document, image, audio, or video) using File API for large files
	contentPart, uploadedFileName, err := p.createContentPartWithFileAPI(ctx, input)
	if err != nil {
		return nil, err
	}

	// Clean up uploaded file after conversion (if File API was used)
	if uploadedFileName != "" {
		defer func() {
			// Best effort cleanup - ignore errors
			// Files will be automatically deleted after 48 hours anyway
			_, _ = p.client.Files.Delete(ctx, uploadedFileName, nil)
		}()
	}

	// Build user content with data + prompt
	contents := []*genai.Content{
		{
			Role: genai.RoleUser,
			Parts: []*genai.Part{
				contentPart,
				{Text: prompt},
			},
		},
	}

	// === Configure API Call ===
	config := &genai.GenerateContentConfig{
		Temperature: genai.Ptr(float32(1.0)),
		TopP:        genai.Ptr(float32(0.95)),
	}

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
	output := &ConversionOutput{
		Markdown:   markdown,
		TokensUsed: p.extractTokenUsage(result),
		CacheName:  nil, // No cache used
		Model:      model,
	}

	return output, nil
}

// convertToMarkdownWithCache converts unstructured data to Markdown using a pre-existing cached context
// This is much more efficient than direct conversion when processing the same content multiple times
func (p *Provider) convertToMarkdownWithCache(ctx context.Context, cacheName, prompt string) (*ConversionOutput, error) {
	// === Validate Input ===
	if cacheName == "" {
		err := errorsx.ErrInvalidArgument
		return nil, errorsx.AddMessage(err, "Internal processing error. Please try again.")
	}

	// === Set Defaults ===
	model := DefaultConversionModel

	// Prompt must be provided by caller (no default assumption)
	if prompt == "" {
		err := errorsx.ErrInvalidArgument
		return nil, errorsx.AddMessage(err, "Internal processing error: prompt not specified. Please try again.")
	}

	// === Prepare Content ===
	// Build user content with just the prompt (data is already cached)
	contents := []*genai.Content{
		{
			Role: genai.RoleUser,
			Parts: []*genai.Part{
				{Text: prompt},
			},
		},
	}

	// === Configure API Call ===
	config := &genai.GenerateContentConfig{
		Temperature:   genai.Ptr(float32(1.0)), // Deterministic output
		TopP:          genai.Ptr(float32(0.95)),
		CachedContent: cacheName, // Use cached context
	}

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
	output := &ConversionOutput{
		Markdown:   markdown,
		TokensUsed: p.extractTokenUsage(result),
		CacheName:  &cacheName, // Cache was used
		Model:      model,
	}

	return output, nil
}

// createContentPartWithFileAPI creates a genai.Part from the input content using File API for large files or videos
// Returns the part and the uploaded file name (empty string if inline data was used)
// Text-based content (text/plain, text/html, text/markdown, text/csv) is sent as text parts instead of blobs
func (p *Provider) createContentPartWithFileAPI(ctx context.Context, input *ConversionInput) (*genai.Part, string, error) {
	// ContentType should always be set by the caller (via ai.FileTypeToMIME)
	if input.ContentType == "" {
		err := errorsx.ErrInvalidArgument
		return nil, "", errorsx.AddMessage(err, "Unsupported file type. Please upload a supported file format.")
	}

	// Text-based content (plain text, HTML, Markdown, CSV) should be sent as text, not as blobs
	// Gemini API expects text content to be in Text parts for proper understanding
	if isTextBasedContent(input.ContentType) {
		textContent := string(input.Content)
		part := &genai.Part{
			Text: textContent,
		}
		return part, "", nil
	}

	fileSize := len(input.Content)
	isVideo := isVideoType(input.ContentType)

	// Use File API if:
	// 1. File is larger than 20MB, or
	// 2. File is a video (videos always use File API for better reliability)
	useFileAPI := fileSize > MaxInlineSize || isVideo

	if useFileAPI {
		// Upload file and wait for it to become active
		uploadedFile, err := p.uploadFileAndWait(ctx, input.Content, input.ContentType)
		if err != nil {
			return nil, "", errorsx.AddMessage(
				fmt.Errorf("failed to upload file for conversion: %w", err),
				"Unable to upload file to AI service. The file may be too large or the service is busy. Please try again later.",
			)
		}

		// Create FileData part
		part := &genai.Part{
			FileData: &genai.FileData{
				FileURI:  uploadedFile.uri,
				MIMEType: uploadedFile.mimeType,
			},
		}

		return part, uploadedFile.name, nil
	}

	// Use inline data for small binary files (PDFs, images, audio, video)
	part := &genai.Part{
		InlineData: &genai.Blob{
			MIMEType: input.ContentType,
			Data:     input.Content,
		},
	}

	return part, "", nil
}

// createContentPart creates a genai.Part from the input content using inline data (for testing and small files)
func (p *Provider) createContentPart(input *ConversionInput) (*genai.Part, error) {
	// ContentType should always be set by the caller (via ai.FileTypeToMIME)
	if input.ContentType == "" {
		err := errorsx.ErrInvalidArgument
		return nil, errorsx.AddMessage(err, "Unsupported file type. Please upload a supported file format.")
	}

	// Create inline data part for any content type
	// Gemini's multimodal API accepts documents, images, audio, and video
	part := &genai.Part{
		InlineData: &genai.Blob{
			MIMEType: input.ContentType,
			Data:     input.Content,
		},
	}

	return part, nil
}

// uploadedFileInfo represents a file that was uploaded via File API
type uploadedFileInfo struct {
	name     string
	uri      string
	mimeType string
}

// uploadFileAndWait uploads a file to File API and waits for it to become ACTIVE
func (p *Provider) uploadFileAndWait(ctx context.Context, data []byte, mimeType string) (*uploadedFileInfo, error) {
	// Upload file
	file, err := p.client.Files.Upload(ctx, bytes.NewReader(data), &genai.UploadFileConfig{
		MIMEType: mimeType,
	})
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to upload file: %w", err),
			"Unable to upload file to AI service. Please try again.",
		)
	}

	fileName := file.Name

	// Wait for file to become ACTIVE with timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, FileUploadTimeout)
	defer cancel()

	if err := p.waitForFileActive(timeoutCtx, fileName); err != nil {
		// Clean up the uploaded file on timeout/failure
		_, _ = p.client.Files.Delete(ctx, fileName, nil)
		return nil, err
	}

	return &uploadedFileInfo{
		name:     fileName,
		uri:      file.URI,
		mimeType: mimeType,
	}, nil
}

// waitForFileActive waits for an uploaded file to become ACTIVE state before using it
func (p *Provider) waitForFileActive(ctx context.Context, fileName string) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Check immediately first (file might already be active)
	if fileInfo, err := p.client.Files.Get(ctx, fileName, nil); err == nil {
		if fileInfo.State == genai.FileStateActive {
			return nil
		}
		if fileInfo.State == genai.FileStateFailed {
			err := fmt.Errorf("file processing failed")
			return errorsx.AddMessage(err, "AI service failed to process the uploaded file. The file may be corrupted or in an unsupported format.")
		}
	}

	// Wait for file to become active
	for {
		select {
		case <-ctx.Done():
			return errorsx.AddMessage(
				fmt.Errorf("timeout waiting for file to become active: %w", ctx.Err()),
				"File upload timed out. The file may be too large or the service is busy. Please try again later.",
			)
		case <-ticker.C:
			fileInfo, err := p.client.Files.Get(ctx, fileName, nil)
			if err != nil {
				return errorsx.AddMessage(
					fmt.Errorf("failed to get file status: %w", err),
					"Unable to check file processing status. Please try again.",
				)
			}

			switch fileInfo.State {
			case genai.FileStateActive:
				return nil
			case genai.FileStateFailed:
				err := fmt.Errorf("file processing failed")
				return errorsx.AddMessage(err, "AI service failed to process the uploaded file. The file may be corrupted or in an unsupported format.")
			case genai.FileStateProcessing:
				continue
			default:
				return errorsx.AddMessage(
					fmt.Errorf("file in unexpected state: %s", fileInfo.State),
					"File processing encountered an unexpected error. Please try again.",
				)
			}
		}
	}
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
func (p *Provider) extractTokenUsage(response *genai.GenerateContentResponse) *TokenUsage {
	if response == nil || response.UsageMetadata == nil {
		return nil
	}

	usage := &TokenUsage{
		InputTokens:       int32(response.UsageMetadata.PromptTokenCount),
		OutputTokens:      int32(response.UsageMetadata.CandidatesTokenCount),
		TotalTokens:       int32(response.UsageMetadata.TotalTokenCount),
		CachedInputTokens: int32(response.UsageMetadata.CachedContentTokenCount),
	}

	return usage
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

// deleteCache deletes a cached content by name
func (p *Provider) deleteCache(ctx context.Context, cacheName string) error {
	if cacheName == "" {
		err := errorsx.ErrInvalidArgument
		return errorsx.AddMessage(err, "Internal error during cache cleanup.")
	}

	_, err := p.client.Caches.Delete(ctx, cacheName, &genai.DeleteCachedContentConfig{})
	if err != nil {
		return errorsx.AddMessage(
			fmt.Errorf("failed to delete cache: %w", err),
			"Failed to clean up processing cache. This may affect future operations.",
		)
	}

	return nil
}

// convertToMarkdownWithRetry converts any content (document, image, audio, video) with automatic retry on transient failures
func (p *Provider) convertToMarkdownWithRetry(ctx context.Context, input *ConversionInput, maxRetries int) (*ConversionOutput, error) {
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

		output, err := p.convertToMarkdownWithoutCache(ctx, input)
		if err == nil {
			return output, nil
		}

		lastErr = err

		// Don't retry on certain errors
		if strings.Contains(err.Error(), "invalid") || strings.Contains(err.Error(), "unsupported") {
			break
		}
	}

	return nil, errorsx.AddMessage(
		fmt.Errorf("conversion failed after %d attempts: %w", maxRetries+1, lastErr),
		"Failed to convert file after multiple attempts. The file may be corrupted or in an unsupported format.",
	)
}
