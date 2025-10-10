package gemini

import (
	"context"
	"fmt"

	"google.golang.org/genai"

	"github.com/instill-ai/artifact-backend/internal/ai"

	errorsx "github.com/instill-ai/x/errors"
)

// ChatWithCache responds to a prompt using cached context via Gemini API
// This leverages Gemini's cached content for instant responses without needing vector search
//
// The cached context contains the full file content(s), allowing the AI to chat about
// files that are still being processed (before embeddings are ready)
func (p *Provider) ChatWithCache(ctx context.Context, cacheName, prompt string) (*ai.ChatResult, error) {
	if cacheName == "" {
		return nil, errorsx.AddMessage(
			errorsx.ErrInvalidArgument,
			"Cache name is required for chat",
		)
	}
	if prompt == "" {
		return nil, errorsx.AddMessage(
			errorsx.ErrInvalidArgument,
			"Prompt cannot be empty",
		)
	}

	// Verify cache exists before attempting to use it
	_, err := p.client.Caches.Get(ctx, cacheName, &genai.GetCachedContentConfig{})
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get cache: %w", err),
			"The cached content is no longer available. Please try uploading the files again.",
		)
	}

	// Build the conversation with the prompt
	contents := []*genai.Content{
		{
			Role: genai.RoleUser,
			Parts: []*genai.Part{
				{Text: prompt},
			},
		},
	}

	// Configure generation for chat with cached context
	// Note: SystemInstruction is already baked into the cached content and cannot be overridden
	config := &genai.GenerateContentConfig{
		CachedContent: cacheName,
		Temperature:   genai.Ptr(float32(1.0)),
		TopP:          genai.Ptr(float32(0.95)),
		TopK:          genai.Ptr(float32(40)),
	}

	// Generate response using the cached context
	// Use the default model for chat (Gemini 1.5 Pro is recommended for cached content)
	resp, err := p.client.Models.GenerateContent(ctx, DefaultConversionModel, contents, config)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to generate response: %w", err),
			"Unable to generate response. Please try again later.",
		)
	}

	// Check if we got a valid response
	if len(resp.Candidates) == 0 {
		return nil, errorsx.AddMessage(
			fmt.Errorf("no response candidates generated"),
			"No response was generated. Please try rephrasing your prompt.",
		)
	}

	candidate := resp.Candidates[0]
	if candidate.Content == nil || len(candidate.Content.Parts) == 0 {
		return nil, errorsx.AddMessage(
			fmt.Errorf("empty response content"),
			"Generated response is empty. Please try rephrasing your prompt.",
		)
	}

	// Extract response text from all parts
	var answer string
	for i, part := range candidate.Content.Parts {
		if i > 0 {
			answer += "\n\n"
		}
		answer += part.Text
	}

	return &ai.ChatResult{
		Answer:        answer,
		Model:         DefaultConversionModel,
		UsageMetadata: resp.UsageMetadata,
	}, nil
}

// ChatWithFiles sends files + prompt directly to AI without caching
// Used for small files that couldn't be cached by Gemini (< 1024 tokens minimum)
// This provides chat capability during processing phase without embeddings
func (p *Provider) ChatWithFiles(ctx context.Context, files []ai.FileContent, prompt string) (*ai.ChatResult, error) {
	if len(files) == 0 {
		return nil, errorsx.AddMessage(
			errorsx.ErrInvalidArgument,
			"At least one file is required for chat",
		)
	}
	if prompt == "" {
		return nil, errorsx.AddMessage(
			errorsx.ErrInvalidArgument,
			"Prompt cannot be empty",
		)
	}

	// Upload files to Gemini's File API
	var parts []*genai.Part
	for _, file := range files {
		// Check if file type is supported
		if !p.SupportsFileType(file.FileType) {
			continue // Skip unsupported files
		}

		// Get MIME type for this file
		mimeType := ai.FileTypeToMIME(file.FileType)

		// Upload file to Gemini File API (required for multimodal)
		part, _, err := p.uploadAndWaitForFile(ctx, file.Content, mimeType)
		if err != nil {
			// Log but continue with other files
			continue
		}

		// Add file as a part
		parts = append(parts, part)
	}

	// Check if we have any valid files
	if len(parts) == 0 {
		return nil, errorsx.AddMessage(
			errorsx.ErrInvalidArgument,
			"No valid files could be processed for chat",
		)
	}

	// Add prompt as the last part
	parts = append(parts, &genai.Part{Text: prompt})

	// Build the conversation
	contents := []*genai.Content{
		{
			Role:  genai.RoleUser,
			Parts: parts,
		},
	}

	// Configure generation for chat with files (no cache)
	config := &genai.GenerateContentConfig{
		Temperature: genai.Ptr(float32(1.0)),
		TopP:        genai.Ptr(float32(0.95)),
		TopK:        genai.Ptr(float32(40)),
		SystemInstruction: &genai.Content{
			Parts: []*genai.Part{
				{Text: DefaultChatSystemInstruction},
			},
		},
	}

	// Generate response with files
	resp, err := p.client.Models.GenerateContent(ctx, DefaultConversionModel, contents, config)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to generate response: %w", err),
			"Unable to generate response. Please try again later.",
		)
	}

	// Check if we got a valid response
	if len(resp.Candidates) == 0 {
		return nil, errorsx.AddMessage(
			fmt.Errorf("no response candidates generated"),
			"No response was generated. Please try rephrasing your prompt.",
		)
	}

	candidate := resp.Candidates[0]
	if candidate.Content == nil || len(candidate.Content.Parts) == 0 {
		return nil, errorsx.AddMessage(
			fmt.Errorf("empty response content"),
			"Generated response is empty. Please try rephrasing your prompt.",
		)
	}

	// Extract response text from all parts
	var answer string
	for i, part := range candidate.Content.Parts {
		if i > 0 {
			answer += "\n\n"
		}
		answer += part.Text
	}

	return &ai.ChatResult{
		Answer:        answer,
		Model:         DefaultConversionModel,
		UsageMetadata: resp.UsageMetadata,
	}, nil
}
