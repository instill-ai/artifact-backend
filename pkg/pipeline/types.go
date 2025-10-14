package pipeline

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
)

const (
	// MaxChunkLengthForMarkdownCatalog is the default maximum chunk length for markdown files
	MaxChunkLengthForMarkdownCatalog = 1024
	// MaxChunkLengthForTextCatalog is the default maximum chunk length for text files
	MaxChunkLengthForTextCatalog = 7000

	// ChunkOverlapForMarkdownCatalog is the default chunk overlap for markdown files
	ChunkOverlapForMarkdownCatalog = 200
	// ChunkOverlapForTextCatalog is the default chunk overlap for text files
	ChunkOverlapForTextCatalog = 700
)

// TextChunkResult contains the chunking result and metadata
type TextChunkResult struct {
	TextChunks []types.TextChunk

	// PipelineRelease is the pipeline used for chunking
	PipelineRelease PipelineRelease
}

// GenerateContentParams defines parameters for markdown conversion
type GenerateContentParams struct {
	Base64Content string
	Type          artifactpb.FileType
	Pipelines     []PipelineRelease
}

// GenerateContentResult contains the information extracted from the conversion step.
type GenerateContentResult struct {
	Markdown     string
	PositionData *types.PositionData

	// Length of the file. The unit and dimensions will depend on the filetype
	// (e.g. pages, milliseconds, pixels).
	Length []uint32

	// PipelineRelease is the pipeline used for conversion.
	PipelineRelease PipelineRelease
}

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

// getGenerateSummaryResult extracts the conversion result from the pipeline response.
// It first checks for a non-empty "summary_from_long_text" field, then falls back to "summary_from_short_text".
// Returns an error if neither field contains valid data or if the response structure is invalid.
func getGenerateSummaryResult(resp *pipelinepb.TriggerNamespacePipelineReleaseResponse) (string, error) {
	if resp == nil || len(resp.Outputs) == 0 {
		// Extract trace errors if available
		traceErrors := extractPipelineTraceErrors(resp)
		if traceErrors != "" {
			return "", fmt.Errorf("pipeline execution failed: %s", traceErrors)
		}
		return "", fmt.Errorf("response is nil or has no outputs. resp: %v", resp)
	}
	fields := resp.Outputs[0].GetFields()
	if fields == nil {
		// Extract trace errors if available
		traceErrors := extractPipelineTraceErrors(resp)
		if traceErrors != "" {
			return "", fmt.Errorf("pipeline execution failed: %s", traceErrors)
		}
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

	// Extract trace errors for better debugging
	traceErrors := extractPipelineTraceErrors(resp)
	if traceErrors != "" {
		return "", fmt.Errorf("summary fields not found in output. Pipeline errors: %s", traceErrors)
	}

	return "", fmt.Errorf("summary fields not found in output: %v", fields)
}

// extractPipelineTraceErrors extracts error messages from pipeline trace metadata
func extractPipelineTraceErrors(resp *pipelinepb.TriggerNamespacePipelineReleaseResponse) string {
	if resp == nil || resp.Metadata == nil {
		return ""
	}

	traces := resp.Metadata.GetTraces()
	if len(traces) == 0 {
		return ""
	}

	var errors []string
	for componentName, trace := range traces {
		// Check for STATUS_ERROR
		if len(trace.Statuses) > 0 && trace.Statuses[0] == pipelinepb.Trace_STATUS_ERROR {
			if trace.Error != nil && trace.Error.Fields != nil {
				if msgField := trace.Error.Fields["message"]; msgField != nil {
					if msg := msgField.GetStringValue(); msg != "" {
						errors = append(errors, fmt.Sprintf("%s: %s", componentName, msg))
					}
				}
			}
		}
	}

	if len(errors) == 0 {
		return ""
	}

	return strings.Join(errors, "; ")
}

// GetChunksFromResponse extracts chunks from the pipeline response
func GetChunksFromResponse(resp *pipelinepb.TriggerNamespacePipelineReleaseResponse) ([]types.TextChunk, error) {
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

	var chunks []types.TextChunk
	for _, v := range listValue.GetValues() {
		endPos := int(v.GetStructValue().Fields["end-position"].GetNumberValue())
		startPos := int(v.GetStructValue().Fields["start-position"].GetNumberValue())
		text := v.GetStructValue().Fields["text"].GetStringValue()
		tokenCount := int(v.GetStructValue().Fields["token-count"].GetNumberValue())
		chunks = append(chunks, types.TextChunk{
			End:    endPos,
			Start:  startPos,
			Text:   text,
			Tokens: tokenCount,
			Reference: &types.TextChunkReference{
				PageRange: [2]uint32{1, 1}, // Pipeline-based chunks default to page 1
			},
		})
	}

	return chunks, nil
}

// GetVectorsFromResponse extracts vector embeddings from the pipeline response
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

// convertResultParser extracts the markdown conversion result from pipeline response
func convertResultParser(resp *pipelinepb.TriggerNamespacePipelineReleaseResponse) (*GenerateContentResult, error) {
	if resp == nil || len(resp.Outputs) == 0 {
		return nil, fmt.Errorf("response is nil or has no outputs. resp: %v", resp)
	}
	fields := resp.Outputs[0].GetFields()
	if fields == nil {
		return nil, fmt.Errorf("fields in the output are nil. resp: %v", resp)
	}

	// Try convert_result first, then convert_result2 as fallback
	suffix := "\n"
	convertResult, ok := fields["convert_result"]
	if !ok {
		convertResult, ok = fields["convert_result2"]
		if !ok {
			return nil, fmt.Errorf("no conversion result fields found in response")
		}

		suffix = ""
	}

	// Check if it's a list (page-based files)
	if list := convertResult.GetListValue(); list != nil {
		pages := ProtoListToStrings(list, suffix)
		if len(pages) == 0 {
			return nil, fmt.Errorf("empty page list in conversion result")
		}

		return &GenerateContentResult{
			Markdown: strings.Join(pages, ""),
			Length:   []uint32{uint32(len(pages))},
		}, nil
	}

	// Otherwise it's a simple string value
	markdown := convertResult.GetStringValue()
	if markdown == "" {
		return nil, fmt.Errorf("conversion result is empty")
	}

	return &GenerateContentResult{
		Markdown: markdown,
	}, nil
}

// ProtoListToStrings converts a protobuf list value to a string slice
func ProtoListToStrings(list *structpb.ListValue, suffix string) []string {
	values := list.GetValues()
	asStrings := make([]string, 0, len(values))
	for i, v := range values {
		s := v.GetStringValue()
		if s == "" {
			continue
		}

		if len(suffix) > 0 && !strings.HasSuffix(s, suffix) && i < len(values)-1 {
			s = s + suffix
		}

		asStrings = append(asStrings, s)
	}

	return asStrings
}

// PositionDataFromPages extracts the page delimiters from a list of pages.
func PositionDataFromPages(pages []string) *types.PositionData {
	if len(pages) == 0 {
		return nil
	}

	var offset uint32
	positionData := &types.PositionData{
		PageDelimiters: make([]uint32, len(pages)),
	}

	for i, page := range pages {
		offset += uint32(len([]rune(page)))
		positionData.PageDelimiters[i] = offset
	}

	return positionData
}
