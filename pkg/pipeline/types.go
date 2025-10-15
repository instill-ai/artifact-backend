package pipeline

import (
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
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

// NOTE: GetChunksFromResponse, GetVectorsFromResponse, ProtoListToStrings, and PositionDataFromPages
// have been removed as they were only used by retired pipeline-based chunking and embedding.
