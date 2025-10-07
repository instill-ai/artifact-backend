package service

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/pkg/repository"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
	logx "github.com/instill-ai/x/log"
)

// MarkdownConversionParams contains the information required to convert a file to
// markdown.
type MarkdownConversionParams struct {
	Base64Content string
	Type          artifactpb.FileType
	Pipelines     []PipelineRelease
}

// MarkdownConversionResult contains the information extracted from the conversion
// step.
type MarkdownConversionResult struct {
	Markdown     string
	PositionData *repository.PositionData

	// Length of the file. The unit and dimensions will depend on the filetype
	// (e.g. pages, milliseconds, pixels).
	Length []uint32

	// PipelineRelease is the pipeline used for conversion.
	PipelineRelease PipelineRelease
}

// convertResultParser extracts the conversion result from the pipeline
// response. It first checks for a non-empty "convert_result" field, then falls
// back to "convert_result2". It handles both single strings (for non-page-based
// files) and arrays of strings (for page-based files). Returns an error if
// neither field contains valid data or if the response structure is invalid.
func convertResultParser(resp *pipelinepb.TriggerNamespacePipelineReleaseResponse) (*MarkdownConversionResult, error) {
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

		return &MarkdownConversionResult{
			Markdown:     strings.Join(pages, ""),
			Length:       []uint32{uint32(len(pages))},
			PositionData: PositionDataFromPages(pages),
		}, nil
	}

	// Check if it's a string (non-page-based files)
	md := convertResult.GetStringValue()
	if md == "" {
		return nil, fmt.Errorf("empty markdown string in conversion result")
	}
	return &MarkdownConversionResult{
		Markdown: md,
		// No length or position data for non-page-based files
	}, nil
}

// ConvertToMarkdownPipe converts a file into Markdown by triggering a converting
// pipeline. If conversion succeeds, the catalog file record will be updated
// with the pipeline used to produce the results.
//   - If the file has a document type extension (pdf, doc[x], ppt[x]) the client
//     may specify a slice of pipelines, which will be triggered in order until a
//     successful trigger produces a non-empty result.
//   - If no pipelines are specified, ConvertDocToMarkdownPipeline will be used by
//     default.
//   - Non-document files will use ConvertDocToMarkdownStandardPipeline, as these types
//     tend to be trivial to convert and can use a deterministic pipeline instead
//     of a custom one that improves the conversion performance.
func (s *service) ConvertToMarkdownPipe(ctx context.Context, p MarkdownConversionParams) (*MarkdownConversionResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

	logger, _ := logx.GetZapLogger(ctx)

	// Get the appropriate prefix for the file type
	prefix := GetFileTypePrefix(p.Type)

	input := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"document_input": structpb.NewStringValue(prefix + p.Base64Content),
		},
	}

	// Determine which pipeline and version to use based on file type
	switch p.Type {
	// Text-based files: no conversion needed, return as-is
	// These are typically handled by AI providers, but this is a fallback
	case artifactpb.FileType_FILE_TYPE_TEXT,
		artifactpb.FileType_FILE_TYPE_MARKDOWN:

		// Decode base64 content
		content, err := base64.StdEncoding.DecodeString(p.Base64Content)
		if err != nil {
			return nil, fmt.Errorf("failed to decode text content: %w", err)
		}

		// Return text content as-is (it's already markdown-compatible)
		return &MarkdownConversionResult{
			Markdown:        string(content),
			PipelineRelease: PipelineRelease{}, // No pipeline used
		}, nil

	// Spreadsheet types and others use the original pipeline
	case artifactpb.FileType_FILE_TYPE_XLSX,
		artifactpb.FileType_FILE_TYPE_XLS,
		artifactpb.FileType_FILE_TYPE_CSV,
		artifactpb.FileType_FILE_TYPE_HTML:

		p.Pipelines = []PipelineRelease{ConvertDocToMarkdownStandardPipeline}

	// Document types use the conversion pipeline configured in the catalog, if
	// present, or the default one for documents (parsing-router if the request
	// comes from Instill Agent, the advanced conversion pipeline otherwise).
	case artifactpb.FileType_FILE_TYPE_PDF,
		artifactpb.FileType_FILE_TYPE_DOCX,
		artifactpb.FileType_FILE_TYPE_DOC,
		artifactpb.FileType_FILE_TYPE_PPT,
		artifactpb.FileType_FILE_TYPE_PPTX:

		// If this is a reprocessing scenario with the default pipeline (same
		// namespace and ID, but potentially different version), reprocess with
		// the newest version of the default pipeline
		reprocessWithDefaultPipeline := len(p.Pipelines) == 1 &&
			p.Pipelines[0].Namespace == ConvertDocToMarkdownPipeline.Namespace &&
			p.Pipelines[0].ID == ConvertDocToMarkdownPipeline.ID

		//
		if len(p.Pipelines) == 0 || reprocessWithDefaultPipeline {
			p.Pipelines = DefaultConversionPipelines
		}
	default:
		return nil, fmt.Errorf("unsupported file type: %v", p.Type)
	}

	for _, pipeline := range p.Pipelines {
		req := &pipelinepb.TriggerNamespacePipelineReleaseRequest{
			NamespaceId: pipeline.Namespace,
			PipelineId:  pipeline.ID,
			ReleaseId:   pipeline.Version,
			Inputs:      []*structpb.Struct{input},
		}

		resp, err := s.pipelinePub.TriggerNamespacePipelineRelease(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("triggering %s pipeline: %w", pipeline.ID, err)
		}

		result, err := convertResultParser(resp)
		if err != nil {
			return nil, fmt.Errorf("getting conversion result: %w", err)
		}

		if result == nil || result.Markdown == "" {
			logger.Info("Conversion pipeline didn't yield results", zap.String("pipeline", pipeline.Name()))
			continue
		}

		// Set the pipeline release used for this conversion
		result.PipelineRelease = pipeline
		return result, nil
	}

	return nil, fmt.Errorf("conversion pipelines didn't produce any result")
}

// ProtoListToStrings returns a proto list of strings as a string slice. The empty
// elements will be removed. A suffix can be passed, which will be appended to
// all the elements but the last one. This will produce the same effect than
// strings.Join(asStrings, suffix) in upstream code, but allows for page
// delimiter extraction before that step.
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
func PositionDataFromPages(pages []string) *repository.PositionData {
	if len(pages) == 0 {
		return nil
	}

	var offset uint32
	positionData := &repository.PositionData{
		PageDelimiters: make([]uint32, len(pages)),
	}

	for i, page := range pages {
		offset += uint32(len([]rune(page)))
		positionData.PageDelimiters[i] = offset
	}

	return positionData
}
