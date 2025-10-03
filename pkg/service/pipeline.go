package service

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/pkg/repository"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
)

const maxChunkLengthForMarkdownCatalog = 1024
const maxChunkLengthForTextCatalog = 7000

const chunkOverlapForMarkdownCatalog = 200
const chunkOverlapForTextCatalog = 700

// GetFileTypePrefix returns the appropriate prefix for the given file type
func GetFileTypePrefix(fileType artifactpb.FileType) string {
	switch fileType {
	case artifactpb.FileType_FILE_TYPE_PDF:
		return "data:application/pdf;base64,"
	case artifactpb.FileType_FILE_TYPE_DOCX:
		return "data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,"
	case artifactpb.FileType_FILE_TYPE_DOC:
		return "data:application/msword;base64,"
	case artifactpb.FileType_FILE_TYPE_PPT:
		return "data:application/vnd.ms-powerpoint;base64,"
	case artifactpb.FileType_FILE_TYPE_PPTX:
		return "data:application/vnd.openxmlformats-officedocument.presentationml.presentation;base64,"
	case artifactpb.FileType_FILE_TYPE_HTML:
		return "data:text/html;base64,"
	case artifactpb.FileType_FILE_TYPE_TEXT:
		return "data:text/plain;base64,"
	case artifactpb.FileType_FILE_TYPE_XLSX:
		return "data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,"
	case artifactpb.FileType_FILE_TYPE_XLS:
		return "data:application/vnd.ms-excel;base64,"
	case artifactpb.FileType_FILE_TYPE_CSV:
		return "data:text/csv;base64,"
	default:
		return ""
	}
}

// Chunk is a struct that represents a chunk of text.
type Chunk struct {
	// Start and end contain the start and end positions of the chunk within
	// the converted file.
	Start     int
	End       int
	Text      string
	Tokens    int
	Reference *repository.ChunkReference
}

// GenerateSummary triggers the generate summary pipeline, processes markdown/text, and deducts credits from the caller's account.
// It generate summary from content.
func (s *service) GenerateSummary(ctx context.Context, content, fileType string) (string, error) {
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

	resp, err := s.pipelinePub.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to trigger %s pipeline. err:%w", GenerateSummaryPipeline.ID, err)
	}
	result, err := getGenerateSummaryResult(resp)
	if err != nil {
		return "", err
	}

	return result, nil
}

// getGenerateSummaryResult extracts the conversion result from the pipeline response.
// It first checks for a non-empty "convert_result" field, then falls back to "convert_result2".
// Returns an error if neither field contains valid data or if the response structure is invalid.
func getGenerateSummaryResult(resp *pipelinepb.TriggerNamespacePipelineReleaseResponse) (string, error) {
	if resp == nil || len(resp.Outputs) == 0 {
		return "", fmt.Errorf("response is nil or has no outputs. resp: %v", resp)
	}
	fields := resp.Outputs[0].GetFields()
	if fields == nil {
		return "", fmt.Errorf("fields in the output are nil. resp: %v", resp)
	}
	convertResult, ok := fields["summary_from_long_text"]
	if ok && convertResult.GetStringValue() != "" {
		return convertResult.GetStringValue(), nil
	}
	convertResult2, ok2 := fields["summary_from_short_text"]
	if ok2 && convertResult2.GetStringValue() != "" {
		return convertResult2.GetStringValue(), nil
	}
	return "", fmt.Errorf("summary_from_short_text and summary_from_long_text not found in the output fields. resp: %v", resp)
}

// ChunkingResult contains the information extracted from chunking a text.
type ChunkingResult struct {
	Chunks []Chunk

	// PipelineRelease is the pipeline used for chunking
	PipelineRelease PipelineRelease
}

// ChunkMarkdownPipe triggers the markdown splitting pipeline, processes the
// markdown text, and deducts credits from the caller's account. It sets up the
// necessary metadata, triggers the pipeline, and processes the response to
// return the non-empty chunks.
func (s *service) ChunkMarkdownPipe(ctx context.Context, markdown string) (*ChunkingResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()
	req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
		NamespaceId: ChunkMDPipeline.Namespace,
		PipelineId:  ChunkMDPipeline.ID,
		ReleaseId:   ChunkMDPipeline.Version,
		Inputs: []*structpb.Struct{
			{
				Fields: map[string]*structpb.Value{
					"md_input":         structpb.NewStringValue(markdown),
					"max_chunk_length": structpb.NewNumberValue(maxChunkLengthForMarkdownCatalog),
					"chunk_overlap":    structpb.NewNumberValue(chunkOverlapForMarkdownCatalog),
				},
			},
		},
	}
	res, err := s.pipelinePub.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to trigger %s pipeline. err:%w", ChunkMDPipeline.ID, err)
	}
	result, err := getChunksFromResponse(res)
	if err != nil {
		return nil, err
	}
	// remove the empty chunk.
	// note: this is a workaround for the pipeline bug that sometimes returns empty chunks.
	var filteredResult []Chunk
	for _, chunk := range result {
		if chunk.Text != "" {
			filteredResult = append(filteredResult, chunk)
		}
	}
	return &ChunkingResult{
		Chunks:          filteredResult,
		PipelineRelease: ChunkMDPipeline,
	}, nil
}

func getChunksFromResponse(resp *pipelinepb.TriggerNamespacePipelineReleaseResponse) ([]Chunk, error) {
	if resp == nil || len(resp.Outputs) == 0 {
		return nil, fmt.Errorf("response is nil or has no outputs. resp: %v", resp)
	}
	splitResult, ok := resp.Outputs[0].GetFields()["split_result"]

	if !ok {
		return nil, fmt.Errorf("split_result not found in the output fields. resp: %v", resp)
	}
	listValue := splitResult.GetListValue()
	if listValue == nil {
		return nil, fmt.Errorf("split_result is not a list. resp: %v", resp)
	}

	var chunks []Chunk
	for _, v := range listValue.GetValues() {
		endPos := int(v.GetStructValue().Fields["end-position"].GetNumberValue())
		startPos := int(v.GetStructValue().Fields["start-position"].GetNumberValue())
		text := v.GetStructValue().Fields["text"].GetStringValue()
		tokenCount := int(v.GetStructValue().Fields["token-count"].GetNumberValue())
		chunks = append(chunks, Chunk{
			End:    endPos,
			Start:  startPos,
			Text:   text,
			Tokens: tokenCount,
		})
	}

	return chunks, nil
}

// ChunkTextPipe splits the input text into chunks using the splitting pipeline
// and consumes the caller's credits. It sets up the necessary metadata,
// triggers the pipeline, and processes the response to return the non-empty
// chunks.
func (s *service) ChunkTextPipe(ctx context.Context, text string) (*ChunkingResult, error) {
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
					"max_chunk_length": structpb.NewNumberValue(maxChunkLengthForTextCatalog),
					"chunk_overlap":    structpb.NewNumberValue(chunkOverlapForTextCatalog),
				},
			},
		},
	}
	res, err := s.pipelinePub.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to trigger %s pipeline. err:%w", ChunkTextPipeline.ID, err)
	}
	result, err := getChunksFromResponse(res)
	if err != nil {
		return nil, fmt.Errorf("failed to get chunks from response: %w", err)
	}
	// remove the empty chunk.
	// note: this is a workaround for the pipeline bug that sometimes returns empty chunks.
	var filteredResult []Chunk
	for _, chunk := range result {
		if chunk.Text != "" {
			filteredResult = append(filteredResult, chunk)
		}
	}
	return &ChunkingResult{
		Chunks:          filteredResult,
		PipelineRelease: ChunkTextPipeline,
	}, nil
}

// EmbeddingTextBatch converts a single batch of text inputs into vector embeddings using a pipeline service.
// This method is designed to be called by the EmbedTextsActivity in Temporal workflows.
//
// Parameters:
//   - ctx: Context for the operation
//   - texts: Slice of strings to be converted to embeddings (typically a batch of 32 or fewer)
//
// Returns:
//   - [][]float32: 2D slice where each inner slice is a vector embedding
//   - error: Any error encountered during processing
func (s *service) EmbeddingTextBatch(ctx context.Context, texts []string) ([][]float32, error) {
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

	res, err := s.pipelinePub.TriggerNamespacePipelineRelease(cctx, req)
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

// EmbeddingTextPipe converts multiple text inputs into vector embeddings using Temporal workflows.
// It processes texts in parallel batches for efficiency while managing resource usage.
//
// Parameters:
//   - ctx: Context for the operation
//   - texts: Slice of strings to be converted to embeddings
//
// Returns:
//   - [][]float32: 2D slice where each inner slice is a vector embedding
//   - error: Any error encountered during processing
//
// The function:
//   - Uses Temporal workflow for batch processing
//   - Batches are processed in chunks of 32 texts
//   - Concurrency is controlled at the Temporal worker level (not per-workflow)
//   - Maintains input order in the output
//   - Automatic retries via Temporal's retry policy
func (s *service) EmbeddingTextPipe(ctx context.Context, texts []string) ([][]float32, error) {
	// Extract authentication metadata from context to pass to activities
	var requestMetadata map[string][]string
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		requestMetadata = md
	} else if md, ok := metadata.FromOutgoingContext(ctx); ok {
		requestMetadata = md
	}

	param := EmbedTextsWorkflowParam{
		Texts:           texts,
		BatchSize:       32,
		RequestMetadata: requestMetadata,
	}

	return s.embedTextsWorkflow.Execute(ctx, param)
}

// GetVectorsFromResponse converts the pipeline response into a slice of float32.
func GetVectorsFromResponse(resp *pipelinepb.TriggerNamespacePipelineReleaseResponse) ([][]float32, error) {
	if resp == nil || len(resp.Outputs) == 0 {
		return nil, fmt.Errorf("response is nil or has no outputs. resp: %v", resp)
	}

	vectors := make([][]float32, 0, len(resp.Outputs))
	for _, output := range resp.Outputs {
		embedResult, ok := output.GetFields()["embed_result"]
		if !ok {
			return nil, fmt.Errorf("embed_result not found in the output fields. pipeline's output: %v. pipeline's metadata: %v", output, resp.Metadata)
		}
		listValue := embedResult.GetListValue()
		if listValue == nil {
			return nil, fmt.Errorf("embed_result is not a list. pipeline's output: %v. pipeline's metadata: %v", output, resp.Metadata)
		}

		vector := make([]float32, 0, len(listValue.GetValues()))
		for _, v := range listValue.GetValues() {
			vector = append(vector, float32(v.GetNumberValue()))
		}
		vectors = append(vectors, vector)
	}

	return vectors, nil
}

// VectoringText using embedding pipeline to vector text and consume caller's credits
func (s *service) QuestionAnsweringPipe(ctx context.Context, question string, simChunks []string) (string, error) {
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
	res, err := s.pipelinePub.TriggerNamespacePipelineRelease(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to trigger %s pipeline. err:%w", QAPipeline.ID, err)
	}
	reply := res.Outputs[0].GetFields()["assistant_reply"].GetStringValue()
	return reply, nil
}
