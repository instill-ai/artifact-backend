package gemini

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/internal/ai"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

func TestNewProvider(t *testing.T) {
	c := qt.New(t)

	t.Run("empty API key returns error", func(t *testing.T) {
		provider, err := NewProvider(context.Background(), "")
		c.Assert(err, qt.Not(qt.IsNil))
		msg := errorsx.Message(err)
		c.Assert(msg, qt.Contains, "AI provider configuration is missing")
		c.Assert(provider, qt.IsNil)
	})

	t.Run("invalid API key format", func(t *testing.T) {
		// Note: This test will actually try to connect, so we expect a connection error
		// We're testing that the function handles errors gracefully
		provider, err := NewProvider(context.Background(), "invalid-key-123")
		// Either we get an error (expected) or somehow it succeeds (unlikely)
		// We just verify the function doesn't panic and handles the case
		if err != nil {
			c.Assert(provider, qt.IsNil)
		}
	})
}

func TestProvider_Name(t *testing.T) {
	c := qt.New(t)

	provider := &Provider{}
	name := provider.Name()
	c.Assert(name, qt.Equals, "gemini")
}

func TestProvider_SupportsFileType(t *testing.T) {
	c := qt.New(t)

	provider := &Provider{}

	t.Run("supports PDF", func(t *testing.T) {
		result := provider.SupportsFileType(artifactpb.FileType_FILE_TYPE_PDF)
		c.Assert(result, qt.IsTrue)
	})

	t.Run("supports DOCX", func(t *testing.T) {
		result := provider.SupportsFileType(artifactpb.FileType_FILE_TYPE_DOCX)
		c.Assert(result, qt.IsTrue)
	})

	t.Run("supports images", func(t *testing.T) {
		c.Assert(provider.SupportsFileType(artifactpb.FileType_FILE_TYPE_PNG), qt.IsTrue)
		c.Assert(provider.SupportsFileType(artifactpb.FileType_FILE_TYPE_JPEG), qt.IsTrue)
		c.Assert(provider.SupportsFileType(artifactpb.FileType_FILE_TYPE_JPG), qt.IsTrue)
		c.Assert(provider.SupportsFileType(artifactpb.FileType_FILE_TYPE_WEBP), qt.IsTrue)
		c.Assert(provider.SupportsFileType(artifactpb.FileType_FILE_TYPE_GIF), qt.IsTrue)
		c.Assert(provider.SupportsFileType(artifactpb.FileType_FILE_TYPE_AVIF), qt.IsTrue)
	})

	t.Run("supports audio", func(t *testing.T) {
		c.Assert(provider.SupportsFileType(artifactpb.FileType_FILE_TYPE_MP3), qt.IsTrue)
		c.Assert(provider.SupportsFileType(artifactpb.FileType_FILE_TYPE_WAV), qt.IsTrue)
		c.Assert(provider.SupportsFileType(artifactpb.FileType_FILE_TYPE_AAC), qt.IsTrue)
		c.Assert(provider.SupportsFileType(artifactpb.FileType_FILE_TYPE_FLAC), qt.IsTrue)
	})

	t.Run("supports video", func(t *testing.T) {
		c.Assert(provider.SupportsFileType(artifactpb.FileType_FILE_TYPE_MP4), qt.IsTrue)
		c.Assert(provider.SupportsFileType(artifactpb.FileType_FILE_TYPE_MOV), qt.IsTrue)
		c.Assert(provider.SupportsFileType(artifactpb.FileType_FILE_TYPE_WEBM_VIDEO), qt.IsTrue)
		c.Assert(provider.SupportsFileType(artifactpb.FileType_FILE_TYPE_AVI), qt.IsTrue)
	})

	t.Run("unsupported file type", func(t *testing.T) {
		result := provider.SupportsFileType(artifactpb.FileType_FILE_TYPE_UNSPECIFIED)
		c.Assert(result, qt.IsFalse)
	})
}

func TestProvider_Close(t *testing.T) {
	c := qt.New(t)

	provider := &Provider{}
	err := provider.Close()
	c.Assert(err, qt.IsNil, qt.Commentf("Close should not return an error"))
}

func TestProvider_DeleteCache_EmptyCacheName(t *testing.T) {
	c := qt.New(t)

	provider := &Provider{
		client: nil, // Not needed for this test
	}

	err := provider.DeleteCache(context.Background(), "")
	c.Assert(err, qt.IsNil, qt.Commentf("Empty cache name should be handled gracefully"))
}

func TestProvider_InterfaceImplementation(t *testing.T) {
	c := qt.New(t)

	// Verify Provider implements ai.Provider interface
	var _ ai.Provider = (*Provider)(nil)
	c.Assert(true, qt.IsTrue, qt.Commentf("Provider should implement ai.Provider interface"))
}

func TestProvider_ConvertToMarkdown_UsesCorrectMIMEType(t *testing.T) {
	c := qt.New(t)

	// Test that the provider correctly maps file types to MIME types
	testCases := []struct {
		fileType     artifactpb.FileType
		expectedMIME string
	}{
		{artifactpb.FileType_FILE_TYPE_PDF, "application/pdf"},
		{artifactpb.FileType_FILE_TYPE_PNG, "image/png"},
		{artifactpb.FileType_FILE_TYPE_JPEG, "image/jpeg"},
		{artifactpb.FileType_FILE_TYPE_MP3, "audio/mpeg"},
		{artifactpb.FileType_FILE_TYPE_MP4, "video/mp4"},
	}

	for _, tc := range testCases {
		t.Run(tc.fileType.String(), func(t *testing.T) {
			mimeType := ai.FileTypeToMIME(tc.fileType)
			c.Assert(mimeType, qt.Equals, tc.expectedMIME)
		})
	}
}

func TestProvider_CreateCache_ValidatesInput(t *testing.T) {
	c := qt.New(t)

	provider := &Provider{
		client: nil,
	}

	t.Run("returns error on empty content", func(t *testing.T) {
		result, err := provider.CreateCache(
			context.Background(),
			[]ai.FileContent{
				{
					Content:  []byte{},
					FileType: artifactpb.FileType_FILE_TYPE_PDF,
					Filename: "test.pdf",
				},
			},
			0,
			ai.SystemInstructionRAG,
		)
		c.Assert(err, qt.Not(qt.IsNil))
		c.Assert(result, qt.IsNil)
	})
}

func TestProvider_ConvertToMarkdownWithCache_ValidatesInput(t *testing.T) {
	c := qt.New(t)

	provider := &Provider{
		client: nil,
	}

	t.Run("returns error on empty cache name", func(t *testing.T) {
		result, err := provider.ConvertToMarkdownWithCache(
			context.Background(),
			"",
			"test prompt",
		)
		c.Assert(err, qt.Not(qt.IsNil))
		msg := errorsx.Message(err)
		c.Assert(msg, qt.Contains, "Failed to convert file to markdown")
		c.Assert(result, qt.IsNil)
	})
}

func TestProvider_MethodSignatures(t *testing.T) {
	c := qt.New(t)

	// Verify method signatures match ai.Provider interface
	provider := &Provider{}

	t.Run("Name returns string", func(t *testing.T) {
		name := provider.Name()
		c.Assert(name, qt.Not(qt.Equals), "")
	})

	t.Run("SupportsFileType accepts FileType", func(t *testing.T) {
		// Just verify the method can be called
		_ = provider.SupportsFileType(artifactpb.FileType_FILE_TYPE_PDF)
	})

	t.Run("Close returns error", func(t *testing.T) {
		err := provider.Close()
		c.Assert(err, qt.IsNil)
	})
}

func TestProvider_StructFields(t *testing.T) {
	c := qt.New(t)

	t.Run("Provider has client field", func(t *testing.T) {
		provider := &Provider{
			client: nil,
		}
		c.Assert(provider.client, qt.IsNil)
		c.Assert(provider.GetEmbeddingDimensionality(), qt.Equals, int32(3072))
	})
}

func TestProvider_ErrorMessages(t *testing.T) {
	c := qt.New(t)

	provider := &Provider{
		client: nil,
	}

	t.Run("CreateCache error includes context", func(t *testing.T) {
		_, err := provider.CreateCache(
			context.Background(),
			[]ai.FileContent{
				{
					Content:  []byte{},
					FileType: artifactpb.FileType_FILE_TYPE_PDF,
					Filename: "test.pdf",
				},
			},
			0,
			ai.SystemInstructionRAG,
		)
		c.Assert(err, qt.Not(qt.IsNil))
		// The error should be descriptive
		c.Assert(err.Error(), qt.Not(qt.Equals), "")
	})

	t.Run("ConvertToMarkdownWithCache error includes context", func(t *testing.T) {
		_, err := provider.ConvertToMarkdownWithCache(
			context.Background(),
			"",
			"prompt",
		)
		c.Assert(err, qt.Not(qt.IsNil))
		msg := errorsx.Message(err)
		c.Assert(msg, qt.Not(qt.Equals), "")
	})
}

func TestPromptLoading(t *testing.T) {
	c := qt.New(t)

	t.Run("chat system instruction is loaded", func(t *testing.T) {
		prompt := GetChatSystemInstruction()
		c.Assert(prompt, qt.Not(qt.Equals), "", qt.Commentf("Chat system instruction should be loaded from embedded file"))
		c.Assert(prompt, qt.Contains, "AI assistant", qt.Commentf("Prompt should contain expected content"))
	})

	t.Run("RAG system instruction is loaded", func(t *testing.T) {
		prompt := GetRAGSystemInstruction()
		c.Assert(prompt, qt.Not(qt.Equals), "", qt.Commentf("RAG system instruction should be loaded from embedded file"))
		c.Assert(prompt, qt.Contains, "Retrieval Augmented Generation", qt.Commentf("Prompt should contain expected content"))
	})

	t.Run("generate content prompt is loaded", func(t *testing.T) {
		prompt := GetRAGGenerateContentPrompt()
		c.Assert(prompt, qt.Not(qt.Equals), "", qt.Commentf("Generate content prompt should be loaded from embedded file"))
		c.Assert(prompt, qt.Contains, "High-Fidelity", qt.Commentf("Prompt should contain expected content"))
	})

	t.Run("generate summary prompt is loaded", func(t *testing.T) {
		prompt := GetRAGGenerateSummaryPrompt()
		c.Assert(prompt, qt.Not(qt.Equals), "", qt.Commentf("Generate summary prompt should be loaded from embedded file"))
		c.Assert(prompt, qt.Contains, "analyst", qt.Commentf("Prompt should contain expected content"))
	})
}
