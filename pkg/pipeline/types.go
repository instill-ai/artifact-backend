package pipeline

import (
	"fmt"
	"strings"

	"github.com/instill-ai/artifact-backend/pkg/types"
	"google.golang.org/protobuf/types/known/structpb"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/v1beta"
	filetype "github.com/instill-ai/x/file"
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
	TextChunks []types.Chunk

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
// Deprecated: Use filetype.GetDataURIPrefix from github.com/instill-ai/x/file instead
func GetFileTypePrefix(fileType artifactpb.File_Type) string {
	return filetype.GetDataURIPrefix(fileType)
}

// ProtoListToStrings converts a protobuf ListValue to a slice of strings
// This is used to extract pages from multi-page documents
func ProtoListToStrings(list *structpb.ListValue, suffix string) []string {
	values := list.GetValues()
	pages := make([]string, len(values))

	for i, val := range values {
		if str := val.GetStringValue(); str != "" {
			pages[i] = str + suffix
		}
	}

	return pages
}

// ConvertPagesToTaggedMarkdown converts page array to [Page: X] tagged format
// This preserves page information for position data extraction
func ConvertPagesToTaggedMarkdown(pages []string) string {
	if len(pages) == 0 {
		return ""
	}

	var sb strings.Builder
	for i, page := range pages {
		// Add page tag before each page (starting from page 1)
		sb.WriteString(fmt.Sprintf("[Page: %d]\n", i+1))
		sb.WriteString(page)
		if i < len(pages)-1 {
			sb.WriteString("\n")
		}
	}
	return sb.String()
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

// enhanceErrorWithPipelineMetadata enhances error messages with pipeline execution metadata
// This adds component trace information and other debugging details to help trace pipeline failures
// If pipelineRelease is provided, it adds the pipeline identity to help locate the execution
func enhanceErrorWithPipelineMetadata(err error, resp *pipelinepb.TriggerNamespacePipelineReleaseResponse, pipelineRelease *Release) error {
	if err == nil {
		return nil
	}

	// Extract metadata if available
	var metadataInfo []string

	// Add pipeline identity first for context
	if pipelineRelease != nil {
		pipelineID := fmt.Sprintf("pipeline=%s/%s@%s", pipelineRelease.Namespace, pipelineRelease.Slug(), pipelineRelease.Version)
		metadataInfo = append(metadataInfo, pipelineID)
	}

	if resp != nil && resp.Metadata != nil {
		metadata := resp.Metadata
		traces := metadata.GetTraces()

		// Add component trace information for failed components
		if len(traces) > 0 {
			var failedComponents []string
			var componentDetails []string

			for componentName, trace := range traces {
				// Check if component has error status
				if len(trace.Statuses) > 0 && trace.Statuses[0] == pipelinepb.Trace_STATUS_ERROR {
					failedComponents = append(failedComponents, componentName)

					// Extract detailed error information from the error struct
					if trace.Error != nil && trace.Error.Fields != nil {
						if errTypeField := trace.Error.Fields["type"]; errTypeField != nil {
							if errType := errTypeField.GetStringValue(); errType != "" {
								componentDetails = append(componentDetails, fmt.Sprintf("%s:%s", componentName, errType))
							}
						}
					}
				}
			}

			if len(failedComponents) > 0 {
				metadataInfo = append(metadataInfo, fmt.Sprintf("failedComponents=%s", strings.Join(failedComponents, ",")))
			}

			if len(componentDetails) > 0 {
				metadataInfo = append(metadataInfo, fmt.Sprintf("errorDetails=%s", strings.Join(componentDetails, ";")))
			}

			// Add total component count for context
			metadataInfo = append(metadataInfo, fmt.Sprintf("totalComponents=%d", len(traces)))
		}
	}

	// If we have metadata, enhance the error message
	if len(metadataInfo) > 0 {
		return fmt.Errorf("%w [Pipeline debug: %s]", err, strings.Join(metadataInfo, ", "))
	}

	// No metadata available, return original error
	return err
}
