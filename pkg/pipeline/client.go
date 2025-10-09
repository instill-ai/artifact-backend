package pipeline

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/pkg/repository"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
)

// ChunkMarkdownPipe triggers the markdown splitting pipeline
func ChunkMarkdownPipe(ctx context.Context, pipelineClient pipelinepb.PipelinePublicServiceClient, markdown string) (*TextChunkingResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()
	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: ChunkMarkdownPipeline.Namespace,
		PipelineId:  ChunkMarkdownPipeline.ID,
		ReleaseId:   ChunkMarkdownPipeline.Version,
		Inputs: []*structpb.Struct{
			{
				Fields: map[string]*structpb.Value{
					"md_input":         structpb.NewStringValue(markdown),
					"max_chunk_length": structpb.NewNumberValue(MaxChunkLengthForMarkdownCatalog),
					"chunk_overlap":    structpb.NewNumberValue(ChunkOverlapForMarkdownCatalog),
				},
			},
		},
	}
	res, err := pipelineClient.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to trigger %s pipeline. err:%w", ChunkMarkdownPipeline.ID, err)
	}
	result, err := GetChunksFromResponse(res)
	if err != nil {
		return nil, err
	}
	// remove the empty chunk.
	// note: this is a workaround for the pipeline bug that sometimes returns empty chunks.
	var filteredResult []TextChunk
	for _, chunk := range result {
		if chunk.Text != "" {
			filteredResult = append(filteredResult, chunk)
		}
	}
	return &TextChunkingResult{
		Chunks:          filteredResult,
		PipelineRelease: ChunkMarkdownPipeline,
	}, nil
}

// GenerateSummary triggers the generate summary pipeline
func GenerateSummary(ctx context.Context, pipelineClient pipelinepb.PipelinePublicServiceClient, content, fileType string) (string, error) {
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
						"file_type": structpb.NewStringValue(fileType),
						"context":   structpb.NewStringValue(content),
						"llm_model": structpb.NewStringValue("gpt-4o-mini"),
					},
				},
			},
		},
	}

	resp, err := pipelineClient.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to trigger %s pipeline. err:%w", GenerateSummaryPipeline.ID, err)
	}
	result, err := getGenerateSummaryResult(resp)
	if err != nil {
		return "", err
	}

	return result, nil
}

// ConvertToMarkdownPipe converts a file into Markdown by triggering a converting pipeline
func ConvertToMarkdownPipe(ctx context.Context, pipelineClient pipelinepb.PipelinePublicServiceClient, repo repository.Repository, params MarkdownConversionParams) (*MarkdownConversionResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

	// Get the appropriate prefix for the file type
	prefix := GetFileTypePrefix(params.Type)

	input := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"document_input": structpb.NewStringValue(prefix + params.Base64Content),
		},
	}

	// Determine which pipeline and version to use based on file type
	// Text-based files: no conversion needed, return as-is
	switch params.Type {
	case artifactpb.FileType_FILE_TYPE_TEXT,
		artifactpb.FileType_FILE_TYPE_MARKDOWN:
		// Decode base64 content
		content, err := base64.StdEncoding.DecodeString(params.Base64Content)
		if err != nil {
			return nil, fmt.Errorf("failed to decode text content: %w", err)
		}
		// Return text content as-is (it's already markdown-compatible)
		return &MarkdownConversionResult{
			Markdown:        string(content),
			PipelineRelease: PipelineRelease{}, // No pipeline used
		}, nil

	// Spreadsheet types and others use the standard pipeline
	case artifactpb.FileType_FILE_TYPE_XLSX,
		artifactpb.FileType_FILE_TYPE_XLS,
		artifactpb.FileType_FILE_TYPE_CSV,
		artifactpb.FileType_FILE_TYPE_HTML:
		params.Pipelines = []PipelineRelease{ConvertDocToMarkdownStandardPipeline}

	// Document types use the conversion pipeline configured in the catalog
	case artifactpb.FileType_FILE_TYPE_PDF,
		artifactpb.FileType_FILE_TYPE_DOCX,
		artifactpb.FileType_FILE_TYPE_DOC,
		artifactpb.FileType_FILE_TYPE_PPT,
		artifactpb.FileType_FILE_TYPE_PPTX:
		// If this is a reprocessing scenario with the default pipeline (same
		// namespace and ID, but potentially different version), reprocess with
		// the newest version of the default pipeline
		reprocessWithDefaultPipeline := len(params.Pipelines) == 1 &&
			params.Pipelines[0].Namespace == ConvertDocToMarkdownPipeline.Namespace &&
			params.Pipelines[0].ID == ConvertDocToMarkdownPipeline.ID

		if len(params.Pipelines) == 0 || reprocessWithDefaultPipeline {
			params.Pipelines = DefaultConversionPipelines
		}
	default:
		return nil, fmt.Errorf("unsupported file type: %v", params.Type)
	}

	for _, pipeline := range params.Pipelines {
		req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
			NamespaceId: pipeline.Namespace,
			PipelineId:  pipeline.ID,
			ReleaseId:   pipeline.Version,
			Inputs:      []*structpb.Struct{input},
		}

		resp, err := pipelineClient.TriggerNamespacePipelineRelease(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("triggering %s pipeline: %w", pipeline.ID, err)
		}

		result, err := convertResultParser(resp)
		if err != nil {
			return nil, fmt.Errorf("getting conversion result: %w", err)
		}

		if result == nil || result.Markdown == "" {
			continue
		}

		// Set the pipeline release used for this conversion
		result.PipelineRelease = pipeline

		// Add position data for page-based conversions if result has pages
		if result.PositionData == nil && len(result.Length) > 0 && result.Length[0] > 0 {
			// Extract pages from markdown for position data
			pages := []string{result.Markdown} // Single page for now
			result.PositionData = PositionDataFromPages(pages)
		}

		return result, nil
	}

	return nil, fmt.Errorf("conversion pipelines didn't produce any result")
}

// EmbedTextPipe converts texts into vector embeddings using pipeline
func EmbedTextPipe(ctx context.Context, pipelineClient pipelinepb.PipelinePublicServiceClient, texts []string, requestMetadata metadata.MD) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}

	// Prepare the inputs for the pipeline request
	inputs := make([]*structpb.Struct, 0, len(texts))
	for _, text := range texts {
		inputs = append(inputs, &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"chunk_input": structpb.NewStringValue(text),
			},
		})
	}

	// Trigger the embedding pipeline
	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: EmbedTextPipeline.Namespace,
		PipelineId:  EmbedTextPipeline.ID,
		ReleaseId:   EmbedTextPipeline.Version,
		Inputs:      inputs,
	}

	cctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

	res, err := pipelineClient.TriggerNamespacePipelineRelease(cctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to trigger %s pipeline: %w", EmbedTextPipeline.ID, err)
	}

	// Extract vectors from the response
	vectors, err := GetVectorsFromResponse(res)
	if err != nil {
		return nil, fmt.Errorf("failed to get vectors from response: %w", err)
	}

	return vectors, nil
}

// ChunkTextPipe splits the input text into chunks using the splitting pipeline
func ChunkTextPipe(ctx context.Context, pipelineClient pipelinepb.PipelinePublicServiceClient, text string) (*TextChunkingResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: ChunkTextPipeline.Namespace,
		PipelineId:  ChunkTextPipeline.ID,
		ReleaseId:   ChunkTextPipeline.Version,

		Inputs: []*structpb.Struct{
			{
				Fields: map[string]*structpb.Value{
					"text_input":       structpb.NewStringValue(text),
					"max_chunk_length": structpb.NewNumberValue(MaxChunkLengthForTextCatalog),
					"chunk_overlap":    structpb.NewNumberValue(ChunkOverlapForTextCatalog),
				},
			},
		},
	}
	res, err := pipelineClient.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to trigger %s pipeline. err:%w", ChunkTextPipeline.ID, err)
	}
	result, err := GetChunksFromResponse(res)
	if err != nil {
		return nil, fmt.Errorf("failed to get chunks from response: %w", err)
	}
	// remove the empty chunk.
	// note: this is a workaround for the pipeline bug that sometimes returns empty chunks.
	var filteredResult []TextChunk
	for _, chunk := range result {
		if chunk.Text != "" {
			filteredResult = append(filteredResult, chunk)
		}
	}
	return &TextChunkingResult{
		Chunks:          filteredResult,
		PipelineRelease: ChunkTextPipeline,
	}, nil
}

// QuestionAnsweringPipe processes a question with retrieved chunks using the QA pipeline
func QuestionAnsweringPipe(ctx context.Context, pipelineClient pipelinepb.PipelinePublicServiceClient, question string, simChunks []string) (string, error) {
	// create a retired chunk var that combines all the chunks by /n/n
	retrievedChunk := ""
	for _, chunk := range simChunks {
		retrievedChunk += chunk + "\n\n"
	}
	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: QAPipeline.Namespace,
		PipelineId:  QAPipeline.ID,
		ReleaseId:   QAPipeline.Version,
		Inputs: []*structpb.Struct{
			{
				Fields: map[string]*structpb.Value{
					"retrieved_chunk": structpb.NewStringValue(retrievedChunk),
					"user_question":   structpb.NewStringValue(question),
				},
			},
		},
	}
	res, err := pipelineClient.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to trigger %s pipeline. err:%w", QAPipeline.ID, err)
	}
	reply := res.Outputs[0].GetFields()["assistant_reply"].GetStringValue()
	return reply, nil
}
