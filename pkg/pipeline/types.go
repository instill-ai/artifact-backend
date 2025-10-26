package pipeline

import (
	"fmt"
	"strings"

	"github.com/instill-ai/artifact-backend/pkg/types"
	"google.golang.org/protobuf/types/known/structpb"

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
	PipelineRelease Release
}

// GenerateContentParams defines parameters for markdown conversion
type GenerateContentParams struct {
	Base64Content string
	Type          artifactpb.File_Type
	Pipelines     []Release
}

// GenerateContentResult contains the information extracted from the conversion step.
type GenerateContentResult struct {
	Markdown     string
	PositionData *types.PositionData

	// Length of the file. The unit and dimensions will depend on the filetype
	// (e.g. pages, milliseconds, pixels).
	Length []uint32

	// PipelineRelease is the pipeline used for conversion.
	PipelineRelease Release
}

// GetFileTypePrefix returns the appropriate prefix for the given file type
func GetFileTypePrefix(fileType artifactpb.File_Type) string {
	switch fileType {
	case artifactpb.File_TYPE_PDF:
		return "data:application/pdf;base64,"
	case artifactpb.File_TYPE_DOCX:
		return "data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,"
	case artifactpb.File_TYPE_DOC:
		return "data:application/msword;base64,"
	case artifactpb.File_TYPE_PPT:
		return "data:application/vnd.ms-powerpoint;base64,"
	case artifactpb.File_TYPE_PPTX:
		return "data:application/vnd.openxmlformats-officedocument.presentationml.presentation;base64,"
	case artifactpb.File_TYPE_HTML:
		return "data:text/html;base64,"
	case artifactpb.File_TYPE_TEXT:
		return "data:text/plain;base64,"
	case artifactpb.File_TYPE_XLSX:
		return "data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,"
	case artifactpb.File_TYPE_XLS:
		return "data:application/vnd.ms-excel;base64,"
	case artifactpb.File_TYPE_CSV:
		return "data:text/csv;base64,"
	default:
		return ""
	}
}

// ConvertPagesToTaggedMarkdown converts a slice of page strings to [Page: X] tagged format
// This makes the OpenAI pipeline output compatible with the current page delimiter parsing logic
func ConvertPagesToTaggedMarkdown(pages []string) string {
	var builder strings.Builder
	for i, page := range pages {
		if i > 0 {
			builder.WriteString("\n")
		}
		// Inject [Page: X] tag at start of each page (same format as Gemini)
		builder.WriteString(fmt.Sprintf("[Page: %d]\n", i+1))
		builder.WriteString(page)
	}
	return builder.String()
}

// ProtoListToStrings converts a protobuf ListValue to a slice of strings
func ProtoListToStrings(list *structpb.ListValue, suffix string) []string {
	values := list.GetValues()
	asStrings := make([]string, 0, len(values))
	for i, v := range values {
		s := v.GetStringValue()
		if s == "" {
			continue
		}
		// Add suffix if needed (e.g., newline between pages)
		if len(suffix) > 0 && !strings.HasSuffix(s, suffix) && i < len(values)-1 {
			s = s + suffix
		}
		asStrings = append(asStrings, s)
	}
	return asStrings
}

// convertResultParser extracts markdown content from pipeline response
func convertResultParser(resp *pipelinepb.TriggerNamespacePipelineReleaseResponse) (*GenerateContentResult, error) {
	if resp == nil || len(resp.Outputs) == 0 {
		traceErrors := extractPipelineTraceErrors(resp)
		if traceErrors != "" {
			return nil, fmt.Errorf("response is nil or has no outputs. Pipeline errors: %s", traceErrors)
		}
		return nil, fmt.Errorf("response is nil or has no outputs")
	}
	fields := resp.Outputs[0].GetFields()
	if fields == nil {
		traceErrors := extractPipelineTraceErrors(resp)
		if traceErrors != "" {
			return nil, fmt.Errorf("fields in the output are nil. Pipeline errors: %s", traceErrors)
		}
		return nil, fmt.Errorf("fields in the output are nil. This usually means the pipeline executed but produced no output fields")
	}

	suffix := "\n"
	convertResult, ok := fields["convert_result"]
	if !ok {
		convertResult, ok = fields["convert_result2"]
		if !ok {
			// Include available field names in error message for debugging
			availableFields := make([]string, 0, len(fields))
			for fieldName := range fields {
				availableFields = append(availableFields, fieldName)
			}

			traceErrors := extractPipelineTraceErrors(resp)
			if traceErrors != "" {
				return nil, fmt.Errorf("no conversion result fields found (expected 'convert_result' or 'convert_result2', got fields: %v). Pipeline errors: %s", availableFields, traceErrors)
			}
			return nil, fmt.Errorf("no conversion result fields found (expected 'convert_result' or 'convert_result2', got fields: %v)", availableFields)
		}
		suffix = ""
	}

	// Check if it's a list (page-based files)
	if list := convertResult.GetListValue(); list != nil {
		pages := ProtoListToStrings(list, suffix)
		if len(pages) == 0 {
			return nil, fmt.Errorf("empty page list")
		}

		// Convert to [Page: X] tagged format for compatibility with current page delimiter logic
		markdownWithTags := ConvertPagesToTaggedMarkdown(pages)

		return &GenerateContentResult{
			Markdown: markdownWithTags,
			Length:   []uint32{uint32(len(pages))},
		}, nil
	}

	// Single-page string result
	markdown := convertResult.GetStringValue()
	if markdown == "" {
		return nil, fmt.Errorf("conversion result is empty")
	}
	return &GenerateContentResult{Markdown: markdown}, nil
}

// getGenerateSummaryResult extracts summary text from pipeline response
func getGenerateSummaryResult(resp *pipelinepb.TriggerNamespacePipelineReleaseResponse) (string, error) {
	if resp == nil || len(resp.Outputs) == 0 {
		traceErrors := extractPipelineTraceErrors(resp)
		if traceErrors != "" {
			return "", fmt.Errorf("response is nil or has no outputs. Pipeline errors: %s", traceErrors)
		}
		return "", fmt.Errorf("response is nil or has no outputs")
	}
	fields := resp.Outputs[0].GetFields()
	if fields == nil {
		traceErrors := extractPipelineTraceErrors(resp)
		if traceErrors != "" {
			return "", fmt.Errorf("fields in the output are nil. Pipeline errors: %s", traceErrors)
		}
		return "", fmt.Errorf("fields in the output are nil. This usually means the pipeline executed but produced no output fields")
	}

	// Try summary_from_long_text first, then summary_from_short_text
	if summary, ok := fields["summary_from_long_text"]; ok && summary.GetStringValue() != "" {
		return summary.GetStringValue(), nil
	}
	if summary, ok := fields["summary_from_short_text"]; ok && summary.GetStringValue() != "" {
		return summary.GetStringValue(), nil
	}

	// Include available field names in error message for debugging
	availableFields := make([]string, 0, len(fields))
	for fieldName := range fields {
		availableFields = append(availableFields, fieldName)
	}

	traceErrors := extractPipelineTraceErrors(resp)
	if traceErrors != "" {
		return "", fmt.Errorf("no summary found in pipeline response (expected 'summary_from_long_text' or 'summary_from_short_text', got fields: %v). Pipeline errors: %s", availableFields, traceErrors)
	}
	return "", fmt.Errorf("no summary found in pipeline response (expected 'summary_from_long_text' or 'summary_from_short_text', got fields: %v)", availableFields)
}

// extractPipelineTraceErrors extracts error messages from pipeline traces
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
