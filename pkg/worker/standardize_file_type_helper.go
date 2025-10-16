package worker

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/internal/ai"
	"github.com/instill-ai/artifact-backend/pkg/pipeline"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
)

// needsFileConversion checks if a file type needs conversion to AI-supported format
// Returns (needsConversion bool, targetFormat string)
// Based on format definitions in the AI component
func needsFileConversion(fileType artifactpb.File_Type) (bool, string) {
	switch fileType {
	// AI-native image formats - no conversion needed
	case artifactpb.File_TYPE_PNG,
		artifactpb.File_TYPE_JPEG,
		artifactpb.File_TYPE_JPG,
		artifactpb.File_TYPE_WEBP,
		artifactpb.File_TYPE_HEIC,
		artifactpb.File_TYPE_HEIF:
		return false, ""

	// Convertible image formats - convert to PNG (widely supported)
	case artifactpb.File_TYPE_GIF,
		artifactpb.File_TYPE_BMP,
		artifactpb.File_TYPE_TIFF,
		artifactpb.File_TYPE_AVIF:
		return true, "png"

	// AI-native audio formats - no conversion needed
	case artifactpb.File_TYPE_MP3,
		artifactpb.File_TYPE_WAV,
		artifactpb.File_TYPE_AAC,
		artifactpb.File_TYPE_OGG,
		artifactpb.File_TYPE_FLAC,
		artifactpb.File_TYPE_AIFF:
		return false, ""

	// Convertible audio formats - convert to OGG (widely supported)
	case artifactpb.File_TYPE_M4A,
		artifactpb.File_TYPE_WMA:
		return true, "ogg"

	// AI-native video formats - no conversion needed
	case artifactpb.File_TYPE_MP4,
		artifactpb.File_TYPE_MPEG,
		artifactpb.File_TYPE_MOV,
		artifactpb.File_TYPE_AVI,
		artifactpb.File_TYPE_FLV,
		artifactpb.File_TYPE_WEBM_VIDEO,
		artifactpb.File_TYPE_WMV:
		return false, ""

	// Convertible video formats - convert to MP4 (widely supported)
	case artifactpb.File_TYPE_MKV:
		return true, "mp4"

	// AI-native document format - no conversion needed
	case artifactpb.File_TYPE_PDF:
		return false, ""

	// Convertible document formats - convert to PDF (widely supported visual document format)
	case artifactpb.File_TYPE_DOC,
		artifactpb.File_TYPE_DOCX,
		artifactpb.File_TYPE_PPT,
		artifactpb.File_TYPE_PPTX,
		artifactpb.File_TYPE_XLS,
		artifactpb.File_TYPE_XLSX:
		return true, "pdf"

	// Text-based documents - no conversion needed (will be used as text)
	case artifactpb.File_TYPE_HTML,
		artifactpb.File_TYPE_TEXT,
		artifactpb.File_TYPE_MARKDOWN,
		artifactpb.File_TYPE_CSV:
		return false, ""

	default:
		return false, ""
	}
}

// convertUsingPipeline converts a file using the indexing-convert-file-type pipeline
func (w *Worker) convertUsingPipeline(ctx context.Context, content []byte, sourceType artifactpb.File_Type, targetFormat string, pipelines []pipeline.PipelineRelease) ([]byte, pipeline.PipelineRelease, error) {
	if len(pipelines) == 0 {
		return nil, pipeline.PipelineRelease{}, fmt.Errorf("no pipelines provided")
	}

	// Use the first available pipeline (indexing-convert-file-type)
	pipeline := pipelines[0]

	// Determine input variable name based on source type
	inputVarName := getInputVariableName(sourceType)
	if inputVarName == "" {
		return nil, pipeline, fmt.Errorf("unsupported source file type: %s", sourceType.String())
	}

	// Prepare pipeline input
	base64Content := base64.StdEncoding.EncodeToString(content)
	mimeType := ai.FileTypeToMIME(sourceType)
	dataURI := fmt.Sprintf("data:%s;base64,%s", mimeType, base64Content)

	inputs := []*structpb.Struct{
		{
			Fields: map[string]*structpb.Value{
				inputVarName: {
					Kind: &structpb.Value_StringValue{
						StringValue: dataURI,
					},
				},
			},
		},
	}

	// Import the pipeline protobuf package
	pipelineClient := w.pipelineClient

	// Create pipeline request
	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: pipeline.Namespace,
		PipelineId:  pipeline.ID,
		ReleaseId:   pipeline.Version,
		Inputs:      inputs,
	}

	// Trigger pipeline with timeout
	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

	resp, err := pipelineClient.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return nil, pipeline, fmt.Errorf("failed to trigger conversion pipeline: %w", err)
	}

	if resp == nil || len(resp.Outputs) == 0 {
		return nil, pipeline, fmt.Errorf("conversion pipeline returned no outputs")
	}

	// Extract converted content from output
	outputFieldName := getOutputFieldName(targetFormat)
	outputValue := resp.Outputs[0].Fields[outputFieldName]
	if outputValue == nil {
		return nil, pipeline, fmt.Errorf("conversion pipeline output missing field: %s", outputFieldName)
	}

	outputURL := outputValue.GetStringValue()
	if outputURL == "" {
		return nil, pipeline, fmt.Errorf("conversion pipeline output is empty")
	}

	// The pipeline returns a MinIO blob URL (not a data URI) for efficient large file handling
	// Format: http://localhost:8080/v1alpha/blob-urls/base64_encoded_presigned_url
	convertedContent, err := w.fetchFromBlobURL(ctx, outputURL)
	if err != nil {
		return nil, pipeline, fmt.Errorf("failed to fetch converted content from blob URL: %w", err)
	}

	return convertedContent, pipeline, nil
}

// fetchFromBlobURL fetches file content from a MinIO blob URL
// The blob URL format is: scheme://host/v1alpha/blob-urls/base64_encoded_presigned_url
// Uses the shared DecodeBlobURL function from the service layer
func (w *Worker) fetchFromBlobURL(ctx context.Context, blobURL string) ([]byte, error) {
	// Check if it's a blob URL or data URI
	if !strings.Contains(blobURL, "/v1alpha/blob-urls/") {
		// If it's a regular data URI, decode it directly
		if strings.HasPrefix(blobURL, "data:") {
			parts := strings.SplitN(blobURL, ",", 2)
			if len(parts) != 2 {
				return nil, fmt.Errorf("invalid data URI format")
			}
			return base64.StdEncoding.DecodeString(parts[1])
		}
		return nil, fmt.Errorf("unsupported URL format: %s", blobURL)
	}

	// Decode the blob URL to get the presigned URL using shared service function
	presignedURL, err := decodeBlobURL(blobURL)
	if err != nil {
		return nil, fmt.Errorf("failed to decode blob URL: %w", err)
	}

	// Fetch the file content from the presigned URL using HTTP client
	return w.fetchFromPresignedURL(ctx, presignedURL)
}

// fetchFromPresignedURL fetches file content from a presigned MinIO URL using HTTP client
func (w *Worker) fetchFromPresignedURL(ctx context.Context, presignedURL string) ([]byte, error) {
	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 300 * time.Second,
	}

	req, err := http.NewRequestWithContext(ctx, "GET", presignedURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch from presigned URL: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("presigned URL returned non-200 status: %d", resp.StatusCode)
	}

	// Read the response body
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	return content, nil
}

// getInputVariableName returns the pipeline input variable name based on file type
func getInputVariableName(fileType artifactpb.File_Type) string {
	switch fileType {
	case artifactpb.File_TYPE_GIF,
		artifactpb.File_TYPE_BMP,
		artifactpb.File_TYPE_TIFF,
		artifactpb.File_TYPE_AVIF:
		return "image"
	case artifactpb.File_TYPE_M4A,
		artifactpb.File_TYPE_WMA:
		return "audio"
	case artifactpb.File_TYPE_MKV:
		return "video"
	case artifactpb.File_TYPE_DOC,
		artifactpb.File_TYPE_DOCX,
		artifactpb.File_TYPE_PPT,
		artifactpb.File_TYPE_PPTX,
		artifactpb.File_TYPE_XLS,
		artifactpb.File_TYPE_XLSX:
		return "document"
	default:
		return ""
	}
}

// getOutputFieldName returns the pipeline output field name based on target format
func getOutputFieldName(targetFormat string) string {
	switch targetFormat {
	case "png":
		return "image"
	case "ogg":
		return "audio"
	case "mp4":
		return "video"
	case "pdf":
		return "document"
	default:
		return ""
	}
}

// mapFormatToFileType maps target format string to FileType enum
func mapFormatToFileType(format string) artifactpb.File_Type {
	switch format {
	case "png":
		return artifactpb.File_TYPE_PNG
	case "ogg":
		return artifactpb.File_TYPE_OGG
	case "mp4":
		return artifactpb.File_TYPE_MP4
	case "pdf":
		return artifactpb.File_TYPE_PDF
	default:
		return artifactpb.File_TYPE_UNSPECIFIED
	}
}
