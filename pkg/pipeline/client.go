package pipeline

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/v1beta"
)

// pipelineIDCache caches pipeline IDs to avoid repeated lookups.
// Key: "namespace/slug", Value: pipeline ID
var (
	pipelineIDCache   = make(map[string]string)
	pipelineIDCacheMu sync.RWMutex
)

// withServiceHeader adds the Instill-Service header and user context to the context.
// This is required for inter-service communication to access preset pipelines.
// NOTE: We create a new outgoing context to ensure the headers are properly set,
// as Temporal activity contexts may not properly propagate gRPC metadata.
// The requesterUID parameter specifies who is making the request - this is the actual
// user/namespace that owns the resources being operated on (e.g., KB owner).
func withServiceHeader(ctx context.Context, requesterUID string) context.Context {
	// Extract existing outgoing metadata if any
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	}
	// Create a copy and add the required headers
	md = md.Copy()
	md.Set("instill-service", "instill")
	// Add user context headers for internal calls
	// Both instill-user-uid and instill-requester-uid should be the actual requester
	// (the user/namespace that owns the resources being operated on)
	md.Set("instill-auth-type", "user")
	if requesterUID != "" {
		md.Set("instill-user-uid", requesterUID)
		md.Set("instill-requester-uid", requesterUID)
	}
	return metadata.NewOutgoingContext(ctx, md)
}

// getPipelineIDBySlug looks up a pipeline by its slug and returns the pipeline ID.
// TriggerPipelineRelease requires the pipeline ID (e.g., pip-xxx), not the slug.
// The result is cached to avoid repeated lookups.
// requesterUID is the UID of the user/namespace making the request (for permission checks).
func getPipelineIDBySlug(ctx context.Context, pipelineClient pipelinepb.PipelinePublicServiceClient, release Release, requesterUID string) (string, error) {
	cacheKey := fmt.Sprintf("%s/%s", release.Namespace, release.Slug())

	// Check cache first
	pipelineIDCacheMu.RLock()
	if id, ok := pipelineIDCache[cacheKey]; ok {
		pipelineIDCacheMu.RUnlock()
		return id, nil
	}
	pipelineIDCacheMu.RUnlock()

	// Add service header to access preset pipelines
	// This is required for inter-service communication to access public preset pipelines
	ctx = withServiceHeader(ctx, requesterUID)

	// Look up pipeline by slug
	pipelineName := fmt.Sprintf("namespaces/%s/pipelines/%s", release.Namespace, release.Slug())
	getResp, err := pipelineClient.GetPipeline(ctx, &pipelinepb.GetPipelineRequest{
		Name: pipelineName,
	})
	if err != nil {
		return "", fmt.Errorf("looking up pipeline %s: %w", pipelineName, err)
	}

	pipelineID := getResp.Pipeline.GetId()
	if pipelineID == "" {
		return "", fmt.Errorf("pipeline %s has no ID", pipelineName)
	}

	// Cache the result
	pipelineIDCacheMu.Lock()
	pipelineIDCache[cacheKey] = pipelineID
	pipelineIDCacheMu.Unlock()

	return pipelineID, nil
}

// GenerateContentPipe converts a file to Markdown using OpenAI pipeline
func GenerateContentPipe(ctx context.Context, pipelineClient pipelinepb.PipelinePublicServiceClient, params GenerateContentParams) (*GenerateContentResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()
	ctx = withServiceHeader(ctx, "")

	prefix := GetFileTypePrefix(params.Type)
	input := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"document_input": structpb.NewStringValue(prefix + params.Base64Content),
		},
	}

	// Look up pipeline by slug to get the pipeline ID
	// TriggerPipelineRelease requires the pipeline ID (e.g., pip-xxx), not the slug
	pipelineID, err := getPipelineIDBySlug(ctx, pipelineClient, GenerateContentPipeline, "")
	if err != nil {
		return nil, fmt.Errorf("looking up pipeline ID: %w", err)
	}

	// Build full resource name using pipeline ID: namespaces/{namespace}/pipelines/{pipeline_id}/releases/{release}
	name := fmt.Sprintf("namespaces/%s/pipelines/%s/releases/%s",
		GenerateContentPipeline.Namespace, pipelineID, GenerateContentPipeline.Version)

	req := &pipelinepb.TriggerPipelineReleaseRequest{
		Name:   name,
		Inputs: []*structpb.Struct{input},
	}

	resp, err := pipelineClient.TriggerPipelineRelease(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("triggering pipeline: %w", err)
	}

	result, err := convertResultParser(resp)
	if err != nil {
		// Enhance error with pipeline execution metadata for debugging
		enhancedErr := enhanceErrorWithPipelineMetadata(err, resp, &GenerateContentPipeline)
		return nil, enhancedErr
	}
	result.PipelineRelease = GenerateContentPipeline
	return result, nil
}

// GenerateSummaryPipe generates summary using OpenAI pipeline
func GenerateSummaryPipe(ctx context.Context, pipelineClient pipelinepb.PipelinePublicServiceClient, content string, fileType artifactpb.File_Type) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()
	ctx = withServiceHeader(ctx, "")

	// Look up pipeline by slug to get the pipeline ID
	// TriggerPipelineRelease requires the pipeline ID (e.g., pip-xxx), not the slug
	pipelineID, err := getPipelineIDBySlug(ctx, pipelineClient, GenerateSummaryPipeline, "")
	if err != nil {
		return "", fmt.Errorf("looking up pipeline ID: %w", err)
	}

	// Build full resource name using pipeline ID: namespaces/{namespace}/pipelines/{pipeline_id}/releases/{release}
	summaryName := fmt.Sprintf("namespaces/%s/pipelines/%s/releases/%s",
		GenerateSummaryPipeline.Namespace, pipelineID, GenerateSummaryPipeline.Version)

	req := &pipelinepb.TriggerPipelineReleaseRequest{
		Name: summaryName,
		Data: []*pipelinepb.TriggerData{
			{
				Variable: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"file_type": structpb.NewStringValue(fileType.String()),
						"context":   structpb.NewStringValue(content),
						"llm_model": structpb.NewStringValue("gpt-4o-mini"),
					},
				},
			},
		},
	}

	resp, err := pipelineClient.TriggerPipelineRelease(ctx, req)
	if err != nil {
		return "", fmt.Errorf("triggering pipeline: %w", err)
	}

	summary, err := getGenerateSummaryResult(resp)
	if err != nil {
		// Enhance error with pipeline execution metadata for debugging
		enhancedErr := enhanceErrorWithPipelineMetadata(err, resp, &GenerateSummaryPipeline)
		return "", enhancedErr
	}
	return summary, nil
}

// ConvertFileTypePipe converts files to standardized formats using the indexing-convert-file-type pipeline
// Handles conversions like PPTX→PDF, HEIC→PNG, MKV→MP4, etc.
// requesterUID parameter is kept for API compatibility but not used - service-to-service auth is sufficient.
// Using empty requesterUID avoids "namespace error" when the original file owner's namespace was deleted.
func ConvertFileTypePipe(ctx context.Context, pipelineClient pipelinepb.PipelinePublicServiceClient, content []byte, sourceType artifactpb.File_Type, mimeType string, requesterUID string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()
	// Use empty requesterUID for service-to-service calls, consistent with other pipeline calls
	// (GenerateContentPipe, GenerateSummaryPipe, EmbedPipe all use empty requesterUID)
	// This prevents "fetching requester namespace: namespace error" when the KB owner's
	// user/organization namespace was deleted but files still need processing during migration
	ctx = withServiceHeader(ctx, "")

	// Determine input variable name based on source type
	inputVarName := getInputVariableName(sourceType)
	if inputVarName == "" {
		return nil, fmt.Errorf("unsupported source file type: %s", sourceType.String())
	}

	// Prepare pipeline input as data URI
	base64Content := base64.StdEncoding.EncodeToString(content)
	dataURI := fmt.Sprintf("data:%s;base64,%s", mimeType, base64Content)

	// Look up pipeline by slug to get the pipeline ID
	// TriggerPipelineRelease requires the pipeline ID (e.g., pip-xxx), not the slug
	// Use empty requesterUID consistent with service-to-service calls
	pipelineID, err := getPipelineIDBySlug(ctx, pipelineClient, ConvertFileTypePipeline, "")
	if err != nil {
		return nil, fmt.Errorf("looking up pipeline ID: %w", err)
	}

	// Build full resource name using pipeline ID: namespaces/{namespace}/pipelines/{pipeline_id}/releases/{release}
	convertName := fmt.Sprintf("namespaces/%s/pipelines/%s/releases/%s",
		ConvertFileTypePipeline.Namespace, pipelineID, ConvertFileTypePipeline.Version)

	req := &pipelinepb.TriggerPipelineReleaseRequest{
		Name: convertName,
		Inputs: []*structpb.Struct{
			{
				Fields: map[string]*structpb.Value{
					inputVarName: structpb.NewStringValue(dataURI),
				},
			},
		},
	}

	resp, err := pipelineClient.TriggerPipelineRelease(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("triggering file type conversion pipeline: %w", err)
	}

	// Parse conversion result from response
	if resp == nil || len(resp.Outputs) == 0 {
		err := fmt.Errorf("response is nil or has no outputs")
		return nil, enhanceErrorWithPipelineMetadata(err, resp, &ConvertFileTypePipeline)
	}

	fields := resp.Outputs[0].GetFields()
	if fields == nil {
		err := fmt.Errorf("fields in the output are nil")
		return nil, enhanceErrorWithPipelineMetadata(err, resp, &ConvertFileTypePipeline)
	}

	// The output field name matches the input variable name (document→document, image→image, etc.)
	outputValue := fields[inputVarName]
	if outputValue == nil {
		err := fmt.Errorf("output field '%s' not found in response", inputVarName)
		return nil, enhanceErrorWithPipelineMetadata(err, resp, &ConvertFileTypePipeline)
	}

	// Extract the output value
	outputResult := outputValue.GetStringValue()
	if outputResult == "" {
		err := fmt.Errorf("output field '%s' is empty", inputVarName)
		return nil, enhanceErrorWithPipelineMetadata(err, resp, &ConvertFileTypePipeline)
	}

	// Pipeline-backend returns either:
	// 1. Data URI: data:mime/type;base64,<base64data> (when input is data URI or small file)
	// 2. Blob URL: http://host/v1alpha/blob-urls/<base64-encoded-presigned-url> (when input is URL)
	convertedContent, err := fetchFromBlobURLOrDataURI(ctx, outputResult)
	if err != nil {
		return nil, enhanceErrorWithPipelineMetadata(
			fmt.Errorf("failed to fetch converted content: %w", err),
			resp,
			&ConvertFileTypePipeline,
		)
	}

	return convertedContent, nil
}

// getInputVariableName returns the pipeline input variable name based on file type
func getInputVariableName(fileType artifactpb.File_Type) string {
	switch fileType {
	case artifactpb.File_TYPE_GIF,
		artifactpb.File_TYPE_BMP,
		artifactpb.File_TYPE_TIFF,
		artifactpb.File_TYPE_AVIF,
		artifactpb.File_TYPE_JPEG,
		artifactpb.File_TYPE_WEBP,
		artifactpb.File_TYPE_HEIC,
		artifactpb.File_TYPE_HEIF,
		artifactpb.File_TYPE_PNG:
		return "image"
	case artifactpb.File_TYPE_M4A,
		artifactpb.File_TYPE_WMA,
		artifactpb.File_TYPE_MP3,
		artifactpb.File_TYPE_WAV,
		artifactpb.File_TYPE_AAC,
		artifactpb.File_TYPE_OGG,
		artifactpb.File_TYPE_FLAC,
		artifactpb.File_TYPE_AIFF,
		artifactpb.File_TYPE_WEBM_AUDIO:
		return "audio"
	case artifactpb.File_TYPE_MKV,
		artifactpb.File_TYPE_MPEG,
		artifactpb.File_TYPE_MOV,
		artifactpb.File_TYPE_AVI,
		artifactpb.File_TYPE_FLV,
		artifactpb.File_TYPE_WEBM_VIDEO,
		artifactpb.File_TYPE_WMV,
		artifactpb.File_TYPE_MP4:
		return "video"
	case artifactpb.File_TYPE_DOC,
		artifactpb.File_TYPE_DOCX,
		artifactpb.File_TYPE_PPT,
		artifactpb.File_TYPE_PPTX,
		artifactpb.File_TYPE_XLS,
		artifactpb.File_TYPE_XLSX,
		artifactpb.File_TYPE_HTML,
		artifactpb.File_TYPE_TEXT,
		artifactpb.File_TYPE_MARKDOWN,
		artifactpb.File_TYPE_CSV:
		return "document"
	default:
		return ""
	}
}

// fetchFromBlobURLOrDataURI fetches file content from either a blob URL or data URI
func fetchFromBlobURLOrDataURI(ctx context.Context, output string) ([]byte, error) {
	// Check if it's a blob URL
	if strings.Contains(output, "/v1alpha/blob-urls/") {
		// Decode the blob URL to get the presigned URL
		presignedURL, err := decodeBlobURL(output)
		if err != nil {
			return nil, fmt.Errorf("failed to decode blob URL: %w", err)
		}

		// Fetch the file content from the presigned URL using HTTP client
		return fetchFromPresignedURL(ctx, presignedURL)
	}

	// Handle data URI format
	if strings.HasPrefix(output, "data:") {
		parts := strings.SplitN(output, ",", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid data URI format")
		}
		return base64.StdEncoding.DecodeString(parts[1])
	}

	return nil, fmt.Errorf("unsupported output format (expected data URI or blob URL): %s", output[:min(50, len(output))])
}

// fetchFromPresignedURL fetches file content from a presigned MinIO URL using HTTP client
func fetchFromPresignedURL(ctx context.Context, presignedURL string) ([]byte, error) {
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

// decodeBlobURL decodes a blob URL to extract the presigned URL
// Blob URL format: http://host/v1alpha/blob-urls/<base64-encoded-presigned-url>
func decodeBlobURL(blobURL string) (string, error) {
	// Extract the base64 part after /blob-urls/
	parts := strings.Split(blobURL, "/v1alpha/blob-urls/")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid blob URL format: %s", blobURL)
	}

	encoded := parts[1]
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("failed to decode base64 blob URL: %w", err)
	}

	return string(decoded), nil
}

// EmbedPipe generates embeddings using OpenAI pipeline
// Returns embedding vectors for a batch of texts
func EmbedPipe(ctx context.Context, pipelineClient pipelinepb.PipelinePublicServiceClient, texts []string) ([][]float32, error) {
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	ctx = withServiceHeader(ctx, "")

	if len(texts) == 0 {
		return [][]float32{}, nil
	}

	// Pipeline batch size limit
	const maxBatchSize = 32

	var allEmbeddings [][]float32

	// Look up pipeline by slug to get the pipeline ID (do this once before the loop)
	// TriggerPipelineRelease requires the pipeline ID (e.g., pip-xxx), not the slug
	pipelineID, err := getPipelineIDBySlug(ctx, pipelineClient, EmbedPipeline, "")
	if err != nil {
		return nil, fmt.Errorf("looking up pipeline ID: %w", err)
	}

	// Build full resource name using pipeline ID: namespaces/{namespace}/pipelines/{pipeline_id}/releases/{release}
	embedName := fmt.Sprintf("namespaces/%s/pipelines/%s/releases/%s",
		EmbedPipeline.Namespace, pipelineID, EmbedPipeline.Version)

	// Process texts in batches of 32
	for i := 0; i < len(texts); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(texts) {
			end = len(texts)
		}
		batchTexts := texts[i:end]

		// Build inputs for this batch
		inputs := make([]*structpb.Struct, len(batchTexts))
		for j, text := range batchTexts {
			inputs[j] = &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"text": structpb.NewStringValue(text),
				},
			}
		}

		req := &pipelinepb.TriggerPipelineReleaseRequest{
			Name:   embedName,
			Inputs: inputs,
		}

		resp, err := pipelineClient.TriggerPipelineRelease(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("triggering embedding pipeline (batch %d-%d): %w", i, end, err)
		}

		// Parse embeddings from response for this batch
		for idx, output := range resp.Outputs {
			fields := output.GetFields()
			if fields == nil {
				err := fmt.Errorf("output %d has no fields", idx)
				return nil, enhanceErrorWithPipelineMetadata(err, resp, &EmbedPipeline)
			}

			embeddingValue := fields["embedding"]
			if embeddingValue == nil {
				err := fmt.Errorf("output %d missing 'embedding' field", idx)
				return nil, enhanceErrorWithPipelineMetadata(err, resp, &EmbedPipeline)
			}

			// Extract float array from list value
			listValue := embeddingValue.GetListValue()
			if listValue == nil {
				err := fmt.Errorf("output %d embedding is not a list", idx)
				return nil, enhanceErrorWithPipelineMetadata(err, resp, &EmbedPipeline)
			}

			values := listValue.GetValues()
			embedding := make([]float32, len(values))
			for k, v := range values {
				embedding[k] = float32(v.GetNumberValue())
			}

			allEmbeddings = append(allEmbeddings, embedding)
		}
	}

	return allEmbeddings, nil
}
