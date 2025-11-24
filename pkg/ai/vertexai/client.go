package vertexai

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"google.golang.org/genai"

	"github.com/instill-ai/artifact-backend/pkg/ai"
	"github.com/instill-ai/artifact-backend/pkg/repository/object"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
	filetype "github.com/instill-ai/x/file"
)

// Client implements the ai.Client interface for VertexAI
type Client struct {
	client  *genai.Client
	storage object.Storage
	config  Config
}

// NewClient creates a new VertexAI client with object storage support (GCS)
func NewClient(ctx context.Context, config Config, storage object.Storage) (*Client, error) {
	if config.SAKey == "" {
		return nil, errorsx.AddMessage(
			errorsx.ErrInvalidArgument,
			"VertexAI configuration is missing service account key.",
		)
	}
	if config.ProjectID == "" {
		return nil, errorsx.AddMessage(
			errorsx.ErrInvalidArgument,
			"VertexAI configuration is missing project ID.",
		)
	}
	if config.Region == "" {
		return nil, errorsx.AddMessage(
			errorsx.ErrInvalidArgument,
			"VertexAI configuration is missing region.",
		)
	}
	if storage == nil {
		return nil, errorsx.AddMessage(
			errorsx.ErrInvalidArgument,
			"Object storage is required for VertexAI client.",
		)
	}

	// Create VertexAI client with service account authentication
	// genai.NewClient uses Application Default Credentials (ADC) via GOOGLE_APPLICATION_CREDENTIALS env var
	// We need to write the service account key to a temp file and set the environment variable

	// The service account key might be wrapped in a Vault response structure
	// We need to extract the actual credentials from data.data if present
	saKey := config.SAKey

	// Try to parse the key to see if it's a Vault response
	var keyData map[string]interface{}
	if err := json.Unmarshal([]byte(saKey), &keyData); err == nil {
		// Check if this is a Vault response (has data.data structure)
		if data, ok := keyData["data"].(map[string]interface{}); ok {
			if innerData, ok := data["data"].(map[string]interface{}); ok {
				// Extract the actual service account key from data.data
				actualKey, err := json.Marshal(innerData)
				if err != nil {
					return nil, errorsx.AddMessage(
						fmt.Errorf("failed to marshal service account key: %w", err),
						"Unable to process service account credentials.",
					)
				}
				saKey = string(actualKey)
			}
		}
	}

	// Create a temporary file for the service account key
	// Note: We don't delete this file because the client needs it for the lifetime of the process
	tmpFile, err := os.CreateTemp("", "vertexai-sa-key-*.json")
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to create temp file for service account key: %w", err),
			"Unable to set up VertexAI authentication. Please check your system permissions.",
		)
	}

	// Write the unwrapped service account key to the temp file
	if _, err := tmpFile.WriteString(saKey); err != nil {
		tmpFile.Close()
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to write service account key to temp file: %w", err),
			"Unable to set up VertexAI authentication. Please check your system permissions.",
		)
	}
	tmpFile.Close()

	// Set the GOOGLE_APPLICATION_CREDENTIALS environment variable
	// This tells genai.NewClient where to find the service account credentials
	if err := os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", tmpFile.Name()); err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to set GOOGLE_APPLICATION_CREDENTIALS: %w", err),
			"Unable to set up VertexAI authentication. Please check your system permissions.",
		)
	}

	genaiClient, err := genai.NewClient(ctx, &genai.ClientConfig{
		Backend:  genai.BackendVertexAI,
		Project:  config.ProjectID,
		Location: config.Region,
	})
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to create VertexAI client: %w", err),
			"Unable to connect to VertexAI service. Please check your service account configuration.",
		)
	}

	return &Client{
		client:  genaiClient,
		storage: storage,
		config:  config,
	}, nil
}

// Name returns the client name
func (c *Client) Name() string {
	return "vertexai"
}

// GetEmbeddingDimensionality returns the VertexAI embedding vector dimensionality
func (c *Client) GetEmbeddingDimensionality() int32 {
	return GetEmbeddingDimension()
}

// CountTokens counts the total tokens for the given content without processing it
// Uses the VertexAI CountTokens API: https://cloud.google.com/vertex-ai/generative-ai/docs/multimodal/get-token-count
func (c *Client) CountTokens(ctx context.Context, content []byte, fileType artifactpb.File_Type, filename string) (int, any, error) {
	// For VertexAI, we need to upload the file to GCS first and create a gs:// URI
	// This is similar to how we handle caching
	namespaceUID := "system" // Use system namespace for token counting
	objectUID := fmt.Sprintf("token-count-%d", time.Now().UnixNano())
	objectPath := fmt.Sprintf("ns-%s/obj-%s", namespaceUID, objectUID)

	// Determine MIME type
	mimeType := filetype.FileTypeToMimeType(fileType)

	// Convert to base64 for object.Storage interface
	base64Content := base64.StdEncoding.EncodeToString(content)

	// Upload file to GCS (empty bucket means use default)
	if err := c.storage.UploadBase64File(ctx, "", objectPath, base64Content, mimeType); err != nil {
		return 0, nil, errorsx.AddMessage(
			fmt.Errorf("failed to upload file to GCS for token counting: %w", err),
			"Unable to prepare content for token counting.",
		)
	}

	// Defer cleanup of the temporary file
	defer func() {
		_ = c.storage.DeleteFile(ctx, "", objectPath)
	}()

	// Build gs:// URI
	bucketName := c.storage.GetBucket()
	gsURI := fmt.Sprintf("gs://%s/%s", bucketName, objectPath)

	// Create file part with gs:// URI
	filePart := &genai.Part{
		FileData: &genai.FileData{
			FileURI:  gsURI,
			MIMEType: mimeType,
		},
	}

	// Create contents for token counting
	contents := []*genai.Content{
		{
			Parts: []*genai.Part{filePart},
			Role:  "user",
		},
	}

	// Count tokens using the VertexAI API
	resp, err := c.client.Models.CountTokens(ctx, GetModel(), contents, nil)
	if err != nil {
		return 0, nil, errorsx.AddMessage(
			fmt.Errorf("failed to count tokens: %w", err),
			"Unable to count tokens. Please try again.",
		)
	}

	// Return total tokens and the full response as usage metadata
	return int(resp.TotalTokens), resp, nil
}

// GetModelFamily returns this client if the model family matches, otherwise error
func (c *Client) GetModelFamily(modelFamily string) (ai.Client, error) {
	// VertexAI is compatible with "gemini" model family requests
	// Also accept "vertexai" as a specific identifier
	if modelFamily == ai.ModelFamilyGemini || modelFamily == "vertexai" {
		return c, nil
	}
	return nil, fmt.Errorf("model family %s not supported by VertexAI client", modelFamily)
}

// Close releases client resources
func (c *Client) Close() error {
	// genai.Client doesn't have a Close method in this version
	return nil
}
