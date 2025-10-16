package worker

import (
	"testing"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

func TestNeedsFileConversion(t *testing.T) {
	tests := []struct {
		name             string
		fileType         artifactpb.File_Type
		wantNeedsConvert bool
		wantTargetFormat string
		description      string
	}{
		// AI-native image formats
		{
			name:             "PNG - no conversion",
			fileType:         artifactpb.File_TYPE_PNG,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "PNG is AI-native, no conversion needed",
		},
		{
			name:             "JPEG - no conversion",
			fileType:         artifactpb.File_TYPE_JPEG,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "JPEG is AI-native, no conversion needed",
		},
		{
			name:             "JPG - no conversion",
			fileType:         artifactpb.File_TYPE_JPG,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "JPG is AI-native, no conversion needed",
		},
		{
			name:             "WEBP - no conversion",
			fileType:         artifactpb.File_TYPE_WEBP,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "WEBP is AI-native, no conversion needed",
		},
		{
			name:             "HEIC - no conversion",
			fileType:         artifactpb.File_TYPE_HEIC,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "HEIC is AI-native, no conversion needed",
		},
		{
			name:             "HEIF - no conversion",
			fileType:         artifactpb.File_TYPE_HEIF,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "HEIF is AI-native, no conversion needed",
		},

		// Convertible image formats
		{
			name:             "GIF - convert to PNG",
			fileType:         artifactpb.File_TYPE_GIF,
			wantNeedsConvert: true,
			wantTargetFormat: "png",
			description:      "GIF should be converted to PNG",
		},
		{
			name:             "BMP - convert to PNG",
			fileType:         artifactpb.File_TYPE_BMP,
			wantNeedsConvert: true,
			wantTargetFormat: "png",
			description:      "BMP should be converted to PNG",
		},
		{
			name:             "TIFF - convert to PNG",
			fileType:         artifactpb.File_TYPE_TIFF,
			wantNeedsConvert: true,
			wantTargetFormat: "png",
			description:      "TIFF should be converted to PNG",
		},
		{
			name:             "AVIF - convert to PNG",
			fileType:         artifactpb.File_TYPE_AVIF,
			wantNeedsConvert: true,
			wantTargetFormat: "png",
			description:      "AVIF should be converted to PNG",
		},

		// AI-native audio formats
		{
			name:             "MP3 - no conversion",
			fileType:         artifactpb.File_TYPE_MP3,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "MP3 is AI-native, no conversion needed",
		},
		{
			name:             "WAV - no conversion",
			fileType:         artifactpb.File_TYPE_WAV,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "WAV is AI-native, no conversion needed",
		},
		{
			name:             "AAC - no conversion",
			fileType:         artifactpb.File_TYPE_AAC,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "AAC is AI-native, no conversion needed",
		},
		{
			name:             "OGG - no conversion",
			fileType:         artifactpb.File_TYPE_OGG,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "OGG is AI-native, no conversion needed",
		},
		{
			name:             "FLAC - no conversion",
			fileType:         artifactpb.File_TYPE_FLAC,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "FLAC is AI-native, no conversion needed",
		},
		{
			name:             "AIFF - no conversion",
			fileType:         artifactpb.File_TYPE_AIFF,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "AIFF is AI-native, no conversion needed",
		},

		// Convertible audio formats
		{
			name:             "M4A - convert to OGG",
			fileType:         artifactpb.File_TYPE_M4A,
			wantNeedsConvert: true,
			wantTargetFormat: "ogg",
			description:      "M4A should be converted to OGG",
		},
		{
			name:             "WMA - convert to OGG",
			fileType:         artifactpb.File_TYPE_WMA,
			wantNeedsConvert: true,
			wantTargetFormat: "ogg",
			description:      "WMA should be converted to OGG",
		},

		// AI-native video formats
		{
			name:             "MP4 - no conversion",
			fileType:         artifactpb.File_TYPE_MP4,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "MP4 is AI-native, no conversion needed",
		},
		{
			name:             "MPEG - no conversion",
			fileType:         artifactpb.File_TYPE_MPEG,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "MPEG is AI-native, no conversion needed",
		},
		{
			name:             "MOV - no conversion",
			fileType:         artifactpb.File_TYPE_MOV,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "MOV is AI-native, no conversion needed",
		},
		{
			name:             "AVI - no conversion",
			fileType:         artifactpb.File_TYPE_AVI,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "AVI is AI-native, no conversion needed",
		},
		{
			name:             "FLV - no conversion",
			fileType:         artifactpb.File_TYPE_FLV,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "FLV is AI-native, no conversion needed",
		},
		{
			name:             "WEBM_VIDEO - no conversion",
			fileType:         artifactpb.File_TYPE_WEBM_VIDEO,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "WEBM is AI-native, no conversion needed",
		},
		{
			name:             "WMV - no conversion",
			fileType:         artifactpb.File_TYPE_WMV,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "WMV is AI-native, no conversion needed",
		},

		// Convertible video formats
		{
			name:             "MKV - convert to MP4",
			fileType:         artifactpb.File_TYPE_MKV,
			wantNeedsConvert: true,
			wantTargetFormat: "mp4",
			description:      "MKV should be converted to MP4",
		},

		// AI-native document format
		{
			name:             "PDF - no conversion",
			fileType:         artifactpb.File_TYPE_PDF,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "PDF is AI-native, no conversion needed",
		},

		// Convertible document formats
		{
			name:             "DOC - convert to PDF",
			fileType:         artifactpb.File_TYPE_DOC,
			wantNeedsConvert: true,
			wantTargetFormat: "pdf",
			description:      "DOC should be converted to PDF",
		},
		{
			name:             "DOCX - convert to PDF",
			fileType:         artifactpb.File_TYPE_DOCX,
			wantNeedsConvert: true,
			wantTargetFormat: "pdf",
			description:      "DOCX should be converted to PDF",
		},
		{
			name:             "PPT - convert to PDF",
			fileType:         artifactpb.File_TYPE_PPT,
			wantNeedsConvert: true,
			wantTargetFormat: "pdf",
			description:      "PPT should be converted to PDF",
		},
		{
			name:             "PPTX - convert to PDF",
			fileType:         artifactpb.File_TYPE_PPTX,
			wantNeedsConvert: true,
			wantTargetFormat: "pdf",
			description:      "PPTX should be converted to PDF",
		},
		{
			name:             "XLS - convert to PDF",
			fileType:         artifactpb.File_TYPE_XLS,
			wantNeedsConvert: true,
			wantTargetFormat: "pdf",
			description:      "XLS should be converted to PDF",
		},
		{
			name:             "XLSX - convert to PDF",
			fileType:         artifactpb.File_TYPE_XLSX,
			wantNeedsConvert: true,
			wantTargetFormat: "pdf",
			description:      "XLSX should be converted to PDF",
		},

		// Text-based documents
		{
			name:             "HTML - no conversion",
			fileType:         artifactpb.File_TYPE_HTML,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "HTML is text-based, no conversion needed",
		},
		{
			name:             "TEXT - no conversion",
			fileType:         artifactpb.File_TYPE_TEXT,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "TEXT is text-based, no conversion needed",
		},
		{
			name:             "MARKDOWN - no conversion",
			fileType:         artifactpb.File_TYPE_MARKDOWN,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "MARKDOWN is text-based, no conversion needed",
		},
		{
			name:             "CSV - no conversion",
			fileType:         artifactpb.File_TYPE_CSV,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "CSV is text-based, no conversion needed",
		},

		// Unknown/default
		{
			name:             "UNSPECIFIED - no conversion",
			fileType:         artifactpb.File_TYPE_UNSPECIFIED,
			wantNeedsConvert: false,
			wantTargetFormat: "",
			description:      "Unspecified type returns false",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotNeedsConvert, gotTargetFormat := needsFileConversion(tt.fileType)
			if gotNeedsConvert != tt.wantNeedsConvert {
				t.Errorf("needsFileConversion(%v) needsConvert = %v, want %v (%s)",
					tt.fileType, gotNeedsConvert, tt.wantNeedsConvert, tt.description)
			}
			if gotTargetFormat != tt.wantTargetFormat {
				t.Errorf("needsFileConversion(%v) targetFormat = %v, want %v (%s)",
					tt.fileType, gotTargetFormat, tt.wantTargetFormat, tt.description)
			}
		})
	}
}

func TestGetInputVariableName(t *testing.T) {
	tests := []struct {
		name     string
		fileType artifactpb.File_Type
		want     string
	}{
		// Image formats
		{name: "GIF", fileType: artifactpb.File_TYPE_GIF, want: "image"},
		{name: "BMP", fileType: artifactpb.File_TYPE_BMP, want: "image"},
		{name: "TIFF", fileType: artifactpb.File_TYPE_TIFF, want: "image"},
		{name: "AVIF", fileType: artifactpb.File_TYPE_AVIF, want: "image"},

		// Audio formats
		{name: "M4A", fileType: artifactpb.File_TYPE_M4A, want: "audio"},
		{name: "WMA", fileType: artifactpb.File_TYPE_WMA, want: "audio"},

		// Video formats
		{name: "MKV", fileType: artifactpb.File_TYPE_MKV, want: "video"},

		// Document formats
		{name: "DOC", fileType: artifactpb.File_TYPE_DOC, want: "document"},
		{name: "DOCX", fileType: artifactpb.File_TYPE_DOCX, want: "document"},
		{name: "PPT", fileType: artifactpb.File_TYPE_PPT, want: "document"},
		{name: "PPTX", fileType: artifactpb.File_TYPE_PPTX, want: "document"},
		{name: "XLS", fileType: artifactpb.File_TYPE_XLS, want: "document"},
		{name: "XLSX", fileType: artifactpb.File_TYPE_XLSX, want: "document"},

		// Unsupported formats
		{name: "PNG (AI-native, not convertible)", fileType: artifactpb.File_TYPE_PNG, want: ""},
		{name: "PDF (AI-native, not convertible)", fileType: artifactpb.File_TYPE_PDF, want: ""},
		{name: "UNSPECIFIED", fileType: artifactpb.File_TYPE_UNSPECIFIED, want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getInputVariableName(tt.fileType)
			if got != tt.want {
				t.Errorf("getInputVariableName(%v) = %v, want %v", tt.fileType, got, tt.want)
			}
		})
	}
}

func TestGetOutputFieldName(t *testing.T) {
	tests := []struct {
		name         string
		targetFormat string
		want         string
	}{
		{name: "PNG format", targetFormat: "png", want: "image"},
		{name: "OGG format", targetFormat: "ogg", want: "audio"},
		{name: "MP4 format", targetFormat: "mp4", want: "video"},
		{name: "PDF format", targetFormat: "pdf", want: "document"},
		{name: "Unknown format", targetFormat: "unknown", want: ""},
		{name: "Empty string", targetFormat: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getOutputFieldName(tt.targetFormat)
			if got != tt.want {
				t.Errorf("getOutputFieldName(%v) = %v, want %v", tt.targetFormat, got, tt.want)
			}
		})
	}
}

func TestMapFormatToFileType(t *testing.T) {
	tests := []struct {
		name   string
		format string
		want   artifactpb.File_Type
	}{
		{
			name:   "PNG format",
			format: "png",
			want:   artifactpb.File_TYPE_PNG,
		},
		{
			name:   "OGG format",
			format: "ogg",
			want:   artifactpb.File_TYPE_OGG,
		},
		{
			name:   "MP4 format",
			format: "mp4",
			want:   artifactpb.File_TYPE_MP4,
		},
		{
			name:   "PDF format",
			format: "pdf",
			want:   artifactpb.File_TYPE_PDF,
		},
		{
			name:   "Unknown format",
			format: "unknown",
			want:   artifactpb.File_TYPE_UNSPECIFIED,
		},
		{
			name:   "Empty string",
			format: "",
			want:   artifactpb.File_TYPE_UNSPECIFIED,
		},
		{
			name:   "Case sensitive - uppercase PNG",
			format: "PNG",
			want:   artifactpb.File_TYPE_UNSPECIFIED,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapFormatToFileType(tt.format)
			if got != tt.want {
				t.Errorf("mapFormatToFileType(%v) = %v, want %v", tt.format, got, tt.want)
			}
		})
	}
}

// TestFileConversionConsistency ensures that all convertible file types have corresponding
// input variable names and that target formats map correctly to output field names
func TestFileConversionConsistency(t *testing.T) {
	allFileTypes := []artifactpb.File_Type{
		artifactpb.File_TYPE_GIF,
		artifactpb.File_TYPE_BMP,
		artifactpb.File_TYPE_TIFF,
		artifactpb.File_TYPE_AVIF,
		artifactpb.File_TYPE_M4A,
		artifactpb.File_TYPE_WMA,
		artifactpb.File_TYPE_MKV,
		artifactpb.File_TYPE_DOC,
		artifactpb.File_TYPE_DOCX,
		artifactpb.File_TYPE_PPT,
		artifactpb.File_TYPE_PPTX,
		artifactpb.File_TYPE_XLS,
		artifactpb.File_TYPE_XLSX,
	}

	for _, fileType := range allFileTypes {
		t.Run(fileType.String(), func(t *testing.T) {
			needsConvert, targetFormat := needsFileConversion(fileType)
			if !needsConvert {
				t.Errorf("%v should need conversion but returned false", fileType)
				return
			}

			inputVar := getInputVariableName(fileType)
			if inputVar == "" {
				t.Errorf("%v needs conversion but has no input variable name", fileType)
			}

			outputField := getOutputFieldName(targetFormat)
			if outputField == "" {
				t.Errorf("%v target format %q has no output field name", fileType, targetFormat)
			}

			convertedType := mapFormatToFileType(targetFormat)
			if convertedType == artifactpb.File_TYPE_UNSPECIFIED {
				t.Errorf("%v target format %q maps to UNSPECIFIED type", fileType, targetFormat)
			}

			// Verify input and output match
			if inputVar != outputField {
				t.Errorf("%v: input variable %q != output field %q", fileType, inputVar, outputField)
			}
		})
	}
}
