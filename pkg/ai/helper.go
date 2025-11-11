package ai

import (
	"github.com/pkoukk/tiktoken-go"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// MapFormatToFileType maps target format string to FileType enum
func MapFormatToFileType(format string) artifactpb.File_Type {
	switch format {
	case "png":
		return artifactpb.File_TYPE_PNG
	case "ogg":
		return artifactpb.File_TYPE_OGG
	case "mp4":
		return artifactpb.File_TYPE_MP4
	case "pdf":
		return artifactpb.File_TYPE_PDF
	default:
		return artifactpb.File_TYPE_UNSPECIFIED
	}
}

// NeedsFileTypeConversion checks if a file type needs conversion to AI-supported format
// Returns (needsConversion bool, targetFormat string)
// Based on format definitions in the AI component
func NeedsFileTypeConversion(fileType artifactpb.File_Type) (bool, string) {
	switch fileType {
	// Standard image format - no conversion needed
	case artifactpb.File_TYPE_PNG:
		return false, ""

	// Convertible image formats - convert to PNG
	case artifactpb.File_TYPE_GIF,
		artifactpb.File_TYPE_BMP,
		artifactpb.File_TYPE_TIFF,
		artifactpb.File_TYPE_AVIF,
		artifactpb.File_TYPE_JPEG,
		artifactpb.File_TYPE_WEBP,
		artifactpb.File_TYPE_HEIC,
		artifactpb.File_TYPE_HEIF:
		return true, "png"

	// Standard audio format - no conversion needed
	case artifactpb.File_TYPE_OGG:
		return false, ""

	// Convertible audio formats - convert to OGG
	case artifactpb.File_TYPE_MP3,
		artifactpb.File_TYPE_WAV,
		artifactpb.File_TYPE_AAC,
		artifactpb.File_TYPE_M4A,
		artifactpb.File_TYPE_WMA,
		artifactpb.File_TYPE_FLAC,
		artifactpb.File_TYPE_AIFF,
		artifactpb.File_TYPE_WEBM_AUDIO:
		return true, "ogg"

	// Standard video format - no conversion needed
	case artifactpb.File_TYPE_MP4:
		return false, ""

	// Convertible video formats - convert to MP4
	case artifactpb.File_TYPE_MKV,
		artifactpb.File_TYPE_MPEG,
		artifactpb.File_TYPE_MOV,
		artifactpb.File_TYPE_AVI,
		artifactpb.File_TYPE_FLV,
		artifactpb.File_TYPE_WMV,
		artifactpb.File_TYPE_WEBM_VIDEO:
		return true, "mp4"

	// Standard document format - no conversion needed
	case artifactpb.File_TYPE_PDF:
		return false, ""

	// Convertible document formats - convert to PDF
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
		return true, "pdf"

	default:
		return false, ""
	}
}

// SupportsFileType returns true if AI clients can directly process this file type
// Aligned with pipeline-backend/pkg/data supported formats
func SupportsFileType(fileType artifactpb.File_Type) bool {
	switch fileType {
	// Documents - supported by pipeline-backend/pkg/data/document.go
	case artifactpb.File_TYPE_PDF,
		artifactpb.File_TYPE_DOCX,
		artifactpb.File_TYPE_DOC,
		artifactpb.File_TYPE_PPTX,
		artifactpb.File_TYPE_PPT,
		artifactpb.File_TYPE_XLSX,
		artifactpb.File_TYPE_XLS,
		artifactpb.File_TYPE_HTML,
		artifactpb.File_TYPE_TEXT,
		artifactpb.File_TYPE_MARKDOWN,
		artifactpb.File_TYPE_CSV:
		return true

	// Images - supported by pipeline-backend/pkg/data/image.go
	case artifactpb.File_TYPE_PNG,
		artifactpb.File_TYPE_JPEG,
		artifactpb.File_TYPE_WEBP,
		artifactpb.File_TYPE_HEIC,
		artifactpb.File_TYPE_HEIF,
		artifactpb.File_TYPE_GIF,
		artifactpb.File_TYPE_BMP,
		artifactpb.File_TYPE_TIFF,
		artifactpb.File_TYPE_AVIF:
		return true

	// Audio - supported by pipeline-backend/pkg/data/audio.go
	case artifactpb.File_TYPE_MP3,
		artifactpb.File_TYPE_WAV,
		artifactpb.File_TYPE_AAC,
		artifactpb.File_TYPE_OGG,
		artifactpb.File_TYPE_FLAC,
		artifactpb.File_TYPE_AIFF,
		artifactpb.File_TYPE_M4A,
		artifactpb.File_TYPE_WMA,
		artifactpb.File_TYPE_WEBM_AUDIO:
		return true

	// Video - supported by pipeline-backend/pkg/data/video.go
	case artifactpb.File_TYPE_MP4,
		artifactpb.File_TYPE_MPEG,
		artifactpb.File_TYPE_MOV,
		artifactpb.File_TYPE_AVI,
		artifactpb.File_TYPE_FLV,
		artifactpb.File_TYPE_WMV,
		artifactpb.File_TYPE_MKV,
		artifactpb.File_TYPE_WEBM_VIDEO:
		return true

	default:
		return false
	}
}

// FileTypeToMIME converts artifact file type to MIME type
// Aligned with pipeline-backend/pkg/data MIME type constants
func FileTypeToMIME(fileType artifactpb.File_Type) string {
	switch fileType {
	// Documents
	case artifactpb.File_TYPE_PDF:
		return "application/pdf"
	case artifactpb.File_TYPE_DOC:
		return "application/msword"
	case artifactpb.File_TYPE_DOCX:
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	case artifactpb.File_TYPE_PPT:
		return "application/vnd.ms-powerpoint"
	case artifactpb.File_TYPE_PPTX:
		return "application/vnd.openxmlformats-officedocument.presentationml.presentation"
	case artifactpb.File_TYPE_XLS:
		return "application/vnd.ms-excel"
	case artifactpb.File_TYPE_XLSX:
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case artifactpb.File_TYPE_HTML:
		return "text/html"
	case artifactpb.File_TYPE_TEXT:
		return "text/plain"
	case artifactpb.File_TYPE_MARKDOWN:
		return "text/markdown"
	case artifactpb.File_TYPE_CSV:
		return "text/csv"

	// Images
	case artifactpb.File_TYPE_PNG:
		return "image/png"
	case artifactpb.File_TYPE_JPEG:
		return "image/jpeg"
	case artifactpb.File_TYPE_WEBP:
		return "image/webp"
	case artifactpb.File_TYPE_HEIC:
		return "image/heic"
	case artifactpb.File_TYPE_HEIF:
		return "image/heif"
	case artifactpb.File_TYPE_GIF:
		return "image/gif"
	case artifactpb.File_TYPE_BMP:
		return "image/bmp"
	case artifactpb.File_TYPE_TIFF:
		return "image/tiff"
	case artifactpb.File_TYPE_AVIF:
		return "image/avif"

	// Audio
	case artifactpb.File_TYPE_MP3:
		return "audio/mpeg"
	case artifactpb.File_TYPE_WAV:
		return "audio/wav"
	case artifactpb.File_TYPE_AAC:
		return "audio/aac"
	case artifactpb.File_TYPE_OGG:
		return "audio/ogg"
	case artifactpb.File_TYPE_FLAC:
		return "audio/flac"
	case artifactpb.File_TYPE_AIFF:
		return "audio/aiff"
	case artifactpb.File_TYPE_M4A:
		return "audio/mp4"
	case artifactpb.File_TYPE_WMA:
		return "audio/x-ms-wma"
	case artifactpb.File_TYPE_WEBM_AUDIO:
		return "audio/webm"

	// Video
	case artifactpb.File_TYPE_MP4:
		return "video/mp4"
	case artifactpb.File_TYPE_MPEG:
		return "video/mpeg"
	case artifactpb.File_TYPE_MOV:
		return "video/quicktime"
	case artifactpb.File_TYPE_AVI:
		return "video/x-msvideo"
	case artifactpb.File_TYPE_FLV:
		return "video/x-flv"
	case artifactpb.File_TYPE_WMV:
		return "video/x-ms-wmv"
	case artifactpb.File_TYPE_MKV:
		return "video/x-matroska"
	case artifactpb.File_TYPE_WEBM_VIDEO:
		return "video/webm"

	default:
		return "application/octet-stream"
	}
}

// EstimateTokenCount estimates the token count for a single text string
// Returns the estimated token count (0 if estimation fails)
// Note: This is an approximation using GPT-4 tokenizer (actual AI client may count differently)
func EstimateTokenCount(text string) int {
	tkm, err := tiktoken.EncodingForModel("gpt-4")
	if err != nil {
		// If we can't get the tokenizer, use a rough estimate: ~4 chars per token
		return len(text) / 4
	}

	return len(tkm.Encode(text, nil, nil))
}
