package pipeline

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
)

// GenerateContentPipe converts a file to Markdown using OpenAI pipeline
func GenerateContentPipe(ctx context.Context, pipelineClient pipelinepb.PipelinePublicServiceClient, params GenerateContentParams) (*GenerateContentResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

	prefix := GetFileTypePrefix(params.Type)
	input := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"document_input": structpb.NewStringValue(prefix + params.Base64Content),
		},
	}

	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: GenerateContentPipeline.Namespace,
		PipelineId:  GenerateContentPipeline.ID,
		ReleaseId:   GenerateContentPipeline.Version,
		Inputs:      []*structpb.Struct{input},
	}

	resp, err := pipelineClient.TriggerNamespacePipelineRelease(ctx, req)
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

	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: GenerateSummaryPipeline.Namespace,
		PipelineId:  GenerateSummaryPipeline.ID,
		ReleaseId:   GenerateSummaryPipeline.Version,
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

	resp, err := pipelineClient.TriggerNamespacePipelineRelease(ctx, req)
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
func ConvertFileTypePipe(ctx context.Context, pipelineClient pipelinepb.PipelinePublicServiceClient, content []byte, sourceType artifactpb.File_Type, mimeType string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

	// Determine input variable name based on source type
	inputVarName := getInputVariableName(sourceType)
	if inputVarName == "" {
		return nil, fmt.Errorf("unsupported source file type: %s", sourceType.String())
	}

	// Prepare pipeline input as data URI
	base64Content := base64.StdEncoding.EncodeToString(content)
	dataURI := fmt.Sprintf("data:%s;base64,%s", mimeType, base64Content)

	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: ConvertFileTypePipeline.Namespace,
		PipelineId:  ConvertFileTypePipeline.ID,
		ReleaseId:   ConvertFileTypePipeline.Version,
		Inputs: []*structpb.Struct{
			{
				Fields: map[string]*structpb.Value{
					inputVarName: structpb.NewStringValue(dataURI),
				},
			},
		},
	}

	resp, err := pipelineClient.TriggerNamespacePipelineRelease(ctx, req)
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

	if len(texts) == 0 {
		return [][]float32{}, nil
	}

	// Pipeline batch size limit
	const maxBatchSize = 32

	var allEmbeddings [][]float32

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

		req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
			NamespaceId: EmbedPipeline.Namespace,
			PipelineId:  EmbedPipeline.ID,
			ReleaseId:   EmbedPipeline.Version,
			Inputs:      inputs,
		}

		resp, err := pipelineClient.TriggerNamespacePipelineRelease(ctx, req)
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
