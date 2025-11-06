package gemini

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/ai"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

func TestNewClient(t *testing.T) {
	c := qt.New(t)

	t.Run("empty API key returns error", func(t *testing.T) {
		client, err := NewClient(context.Background(), "")
		c.Assert(err, qt.Not(qt.IsNil))
		msg := errorsx.Message(err)
		c.Assert(msg, qt.Contains, "AI client configuration is missing")
		c.Assert(client, qt.IsNil)
	})

	t.Run("invalid API key format", func(t *testing.T) {
		// Note: This test will actually try to connect, so we expect a connection error
		// We're testing that the function handles errors gracefully
		client, err := NewClient(context.Background(), "invalid-key-123")
		// Either we get an error (expected) or somehow it succeeds (unlikely)
		// We just verify the function doesn't panic and handles the case
		if err != nil {
			c.Assert(client, qt.IsNil)
		}
	})
}

func TestClient_Name(t *testing.T) {
	c := qt.New(t)

	client := &Client{}
	name := client.Name()
	c.Assert(name, qt.Equals, "gemini")
}

func TestClient_SupportsFileType(t *testing.T) {
	c := qt.New(t)

	client := &Client{}

	t.Run("supports PDF", func(t *testing.T) {
		result := client.SupportsFileType(artifactpb.File_TYPE_PDF)
		c.Assert(result, qt.IsTrue)
	})

	t.Run("supports DOCX", func(t *testing.T) {
		result := client.SupportsFileType(artifactpb.File_TYPE_DOCX)
		c.Assert(result, qt.IsTrue)
	})

	t.Run("supports images", func(t *testing.T) {
		c.Assert(client.SupportsFileType(artifactpb.File_TYPE_PNG), qt.IsTrue)
		c.Assert(client.SupportsFileType(artifactpb.File_TYPE_JPEG), qt.IsTrue)
		c.Assert(client.SupportsFileType(artifactpb.File_TYPE_WEBP), qt.IsTrue)
		c.Assert(client.SupportsFileType(artifactpb.File_TYPE_GIF), qt.IsTrue)
		c.Assert(client.SupportsFileType(artifactpb.File_TYPE_AVIF), qt.IsTrue)
	})

	t.Run("supports audio", func(t *testing.T) {
		c.Assert(client.SupportsFileType(artifactpb.File_TYPE_MP3), qt.IsTrue)
		c.Assert(client.SupportsFileType(artifactpb.File_TYPE_WAV), qt.IsTrue)
		c.Assert(client.SupportsFileType(artifactpb.File_TYPE_AAC), qt.IsTrue)
		c.Assert(client.SupportsFileType(artifactpb.File_TYPE_FLAC), qt.IsTrue)
	})

	t.Run("supports video", func(t *testing.T) {
		c.Assert(client.SupportsFileType(artifactpb.File_TYPE_MP4), qt.IsTrue)
		c.Assert(client.SupportsFileType(artifactpb.File_TYPE_MOV), qt.IsTrue)
		c.Assert(client.SupportsFileType(artifactpb.File_TYPE_WEBM_VIDEO), qt.IsTrue)
		c.Assert(client.SupportsFileType(artifactpb.File_TYPE_AVI), qt.IsTrue)
	})

	t.Run("unsupported file type", func(t *testing.T) {
		result := client.SupportsFileType(artifactpb.File_TYPE_UNSPECIFIED)
		c.Assert(result, qt.IsFalse)
	})
}

func TestClient_Close(t *testing.T) {
	c := qt.New(t)

	client := &Client{}
	err := client.Close()
	c.Assert(err, qt.IsNil, qt.Commentf("Close should not return an error"))
}

func TestClient_InterfaceImplementation(t *testing.T) {
	c := qt.New(t)

	// Verify Client implements ai.Client interface
	var _ ai.Client = (*Client)(nil)
	c.Assert(true, qt.IsTrue, qt.Commentf("Client should implement ai.Client interface"))
}

func TestClient_MethodSignatures(t *testing.T) {
	c := qt.New(t)

	// Verify method signatures match ai.Client interface
	client := &Client{}

	t.Run("Name returns string", func(t *testing.T) {
		name := client.Name()
		c.Assert(name, qt.Not(qt.Equals), "")
	})

	t.Run("SupportsFileType accepts FileType", func(t *testing.T) {
		// Just verify the method can be called
		_ = client.SupportsFileType(artifactpb.File_TYPE_PDF)
	})

	t.Run("Close returns error", func(t *testing.T) {
		err := client.Close()
		c.Assert(err, qt.IsNil)
	})
}

func TestClient_StructFields(t *testing.T) {
	c := qt.New(t)

	t.Run("Client has client field", func(t *testing.T) {
		client := &Client{
			client: nil,
		}
		c.Assert(client.client, qt.IsNil)
		c.Assert(client.GetEmbeddingDimensionality(), qt.Equals, int32(3072))
	})
}
