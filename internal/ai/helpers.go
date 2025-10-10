package ai

import (
	"github.com/pkoukk/tiktoken-go"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// MinCacheTokens is the minimum token count for cache creation
// This is a common requirement across AI providers (e.g., Gemini requires 1024 tokens)
const MinCacheTokens = 1024

// Cache system instruction types
const (
	// SystemInstructionRAG indicates RAG-optimized system instruction for content/summarization generation
	SystemInstructionRAG = "rag"
	// SystemInstructionChat indicates chat-optimized system instruction for chat
	SystemInstructionChat = "chat"
)

// SupportsFileType returns true if AI providers can directly process this file type
// Aligned with pipeline-backend/pkg/data supported formats
func SupportsFileType(fileType artifactpb.FileType) bool {
	switch fileType {
	// Documents - supported by pipeline-backend/pkg/data/document.go
	case artifactpb.FileType_FILE_TYPE_PDF,
		artifactpb.FileType_FILE_TYPE_DOCX,
		artifactpb.FileType_FILE_TYPE_DOC,
		artifactpb.FileType_FILE_TYPE_PPTX,
		artifactpb.FileType_FILE_TYPE_PPT,
		artifactpb.FileType_FILE_TYPE_XLSX,
		artifactpb.FileType_FILE_TYPE_XLS,
		artifactpb.FileType_FILE_TYPE_HTML,
		artifactpb.FileType_FILE_TYPE_TEXT,
		artifactpb.FileType_FILE_TYPE_MARKDOWN,
		artifactpb.FileType_FILE_TYPE_CSV:
		return true

	// Images - supported by pipeline-backend/pkg/data/image.go
	case artifactpb.FileType_FILE_TYPE_PNG,
		artifactpb.FileType_FILE_TYPE_JPEG,
		artifactpb.FileType_FILE_TYPE_JPG,
		artifactpb.FileType_FILE_TYPE_WEBP,
		artifactpb.FileType_FILE_TYPE_HEIC,
		artifactpb.FileType_FILE_TYPE_HEIF,
		artifactpb.FileType_FILE_TYPE_GIF,
		artifactpb.FileType_FILE_TYPE_BMP,
		artifactpb.FileType_FILE_TYPE_TIFF,
		artifactpb.FileType_FILE_TYPE_AVIF:
		return true

	// Audio - supported by pipeline-backend/pkg/data/audio.go
	case artifactpb.FileType_FILE_TYPE_MP3,
		artifactpb.FileType_FILE_TYPE_WAV,
		artifactpb.FileType_FILE_TYPE_AAC,
		artifactpb.FileType_FILE_TYPE_OGG,
		artifactpb.FileType_FILE_TYPE_FLAC,
		artifactpb.FileType_FILE_TYPE_AIFF,
		artifactpb.FileType_FILE_TYPE_M4A,
		artifactpb.FileType_FILE_TYPE_WMA:
		return true

	// Video - supported by pipeline-backend/pkg/data/video.go
	case artifactpb.FileType_FILE_TYPE_MP4,
		artifactpb.FileType_FILE_TYPE_MPEG,
		artifactpb.FileType_FILE_TYPE_MOV,
		artifactpb.FileType_FILE_TYPE_AVI,
		artifactpb.FileType_FILE_TYPE_FLV,
		artifactpb.FileType_FILE_TYPE_WEBM_VIDEO,
		artifactpb.FileType_FILE_TYPE_WMV,
		artifactpb.FileType_FILE_TYPE_MKV:
		return true

	default:
		return false
	}
}

// FileTypeToMIME converts artifact file type to MIME type
// Aligned with pipeline-backend/pkg/data MIME type constants
func FileTypeToMIME(fileType artifactpb.FileType) string {
	switch fileType {
	// Documents (from pipeline-backend/pkg/data/document.go)
	case artifactpb.FileType_FILE_TYPE_PDF:
		return "application/pdf"
	case artifactpb.FileType_FILE_TYPE_DOCX:
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	case artifactpb.FileType_FILE_TYPE_DOC:
		return "application/msword"
	case artifactpb.FileType_FILE_TYPE_PPTX:
		return "application/vnd.openxmlformats-officedocument.presentationml.presentation"
	case artifactpb.FileType_FILE_TYPE_PPT:
		return "application/vnd.ms-powerpoint"
	case artifactpb.FileType_FILE_TYPE_XLSX:
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case artifactpb.FileType_FILE_TYPE_XLS:
		return "application/vnd.ms-excel"
	case artifactpb.FileType_FILE_TYPE_HTML:
		return "text/html"
	case artifactpb.FileType_FILE_TYPE_TEXT:
		return "text/plain"
	case artifactpb.FileType_FILE_TYPE_MARKDOWN:
		return "text/markdown"
	case artifactpb.FileType_FILE_TYPE_CSV:
		return "text/csv"

	// Images (from pipeline-backend/pkg/data/image.go)
	case artifactpb.FileType_FILE_TYPE_PNG:
		return "image/png"
	case artifactpb.FileType_FILE_TYPE_JPEG, artifactpb.FileType_FILE_TYPE_JPG:
		return "image/jpeg"
	case artifactpb.FileType_FILE_TYPE_GIF:
		return "image/gif"
	case artifactpb.FileType_FILE_TYPE_WEBP:
		return "image/webp"
	case artifactpb.FileType_FILE_TYPE_TIFF:
		return "image/tiff"
	case artifactpb.FileType_FILE_TYPE_BMP:
		return "image/bmp"
	case artifactpb.FileType_FILE_TYPE_HEIC:
		return "image/heic"
	case artifactpb.FileType_FILE_TYPE_HEIF:
		return "image/heif"
	case artifactpb.FileType_FILE_TYPE_AVIF:
		return "image/avif"

	// Audio (from pipeline-backend/pkg/data/audio.go)
	case artifactpb.FileType_FILE_TYPE_MP3:
		return "audio/mpeg"
	case artifactpb.FileType_FILE_TYPE_WAV:
		return "audio/wav"
	case artifactpb.FileType_FILE_TYPE_AAC:
		return "audio/aac"
	case artifactpb.FileType_FILE_TYPE_OGG:
		return "audio/ogg"
	case artifactpb.FileType_FILE_TYPE_FLAC:
		return "audio/flac"
	case artifactpb.FileType_FILE_TYPE_M4A:
		return "audio/mp4"
	case artifactpb.FileType_FILE_TYPE_WMA:
		return "audio/x-ms-wma"
	case artifactpb.FileType_FILE_TYPE_AIFF:
		return "audio/aiff"

	// Video (from pipeline-backend/pkg/data/video.go)
	case artifactpb.FileType_FILE_TYPE_MP4:
		return "video/mp4"
	case artifactpb.FileType_FILE_TYPE_AVI:
		return "video/x-msvideo"
	case artifactpb.FileType_FILE_TYPE_MOV:
		return "video/quicktime"
	case artifactpb.FileType_FILE_TYPE_WEBM_VIDEO:
		return "video/webm"
	case artifactpb.FileType_FILE_TYPE_MKV:
		return "video/x-matroska"
	case artifactpb.FileType_FILE_TYPE_FLV:
		return "video/x-flv"
	case artifactpb.FileType_FILE_TYPE_WMV:
		return "video/x-ms-wmv"
	case artifactpb.FileType_FILE_TYPE_MPEG:
		return "video/mpeg"

	default:
		return "application/octet-stream"
	}
}

// EstimateTokenCount estimates the token count for a single text string
// Returns the estimated token count (0 if estimation fails)
// Note: This is an approximation using GPT-4 tokenizer (actual AI provider may count differently)
func EstimateTokenCount(text string) int {
	tkm, err := tiktoken.EncodingForModel("gpt-4")
	if err != nil {
		// If we can't get the tokenizer, use a rough estimate: ~4 chars per token
		return len(text) / 4
	}

	return len(tkm.Encode(text, nil, nil))
}

// EstimateTotalTokens estimates the total token count across all files
// Returns (estimatedTokens, error)
// Note: This is an approximation using GPT-4 tokenizer (actual AI provider may count differently)
func EstimateTotalTokens(files []FileContent) (int, error) {
	totalTokens := 0
	for _, file := range files {
		tokens := EstimateTokenCount(string(file.Content))
		totalTokens += tokens
	}

	return totalTokens, nil
}
