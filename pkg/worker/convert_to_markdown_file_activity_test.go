package worker

import (
	"context"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/gojuno/minimock/v3"
	"go.uber.org/zap"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/internal/ai"
	"github.com/instill-ai/artifact-backend/pkg/mock"
	"github.com/instill-ai/artifact-backend/pkg/service"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

func TestNeedsFileConversion(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name         string
		fileType     artifactpb.FileType
		needsConvert bool
		targetFormat string
	}{
		{
			name:         "PNG is Gemini-native",
			fileType:     artifactpb.FileType_FILE_TYPE_PNG,
			needsConvert: false,
			targetFormat: "",
		},
		{
			name:         "GIF needs conversion to PNG",
			fileType:     artifactpb.FileType_FILE_TYPE_GIF,
			needsConvert: true,
			targetFormat: "png",
		},
		{
			name:         "AVIF needs conversion to PNG",
			fileType:     artifactpb.FileType_FILE_TYPE_AVIF,
			needsConvert: true,
			targetFormat: "png",
		},
		{
			name:         "MP3 is Gemini-native",
			fileType:     artifactpb.FileType_FILE_TYPE_MP3,
			needsConvert: false,
			targetFormat: "",
		},
		{
			name:         "M4A needs conversion to OGG",
			fileType:     artifactpb.FileType_FILE_TYPE_M4A,
			needsConvert: true,
			targetFormat: "ogg",
		},
		{
			name:         "MP4 is Gemini-native",
			fileType:     artifactpb.FileType_FILE_TYPE_MP4,
			needsConvert: false,
			targetFormat: "",
		},
		{
			name:         "MKV needs conversion to MP4",
			fileType:     artifactpb.FileType_FILE_TYPE_MKV,
			needsConvert: true,
			targetFormat: "mp4",
		},
		{
			name:         "PDF is Gemini-native",
			fileType:     artifactpb.FileType_FILE_TYPE_PDF,
			needsConvert: false,
			targetFormat: "",
		},
		{
			name:         "DOCX needs conversion to PDF",
			fileType:     artifactpb.FileType_FILE_TYPE_DOCX,
			needsConvert: true,
			targetFormat: "pdf",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			needsConvert, targetFormat := needsFileConversion(tt.fileType)
			c.Assert(needsConvert, qt.Equals, tt.needsConvert)
			c.Assert(targetFormat, qt.Equals, tt.targetFormat)
		})
	}
}

func TestGetInputVariableName(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		fileType artifactpb.FileType
		expected string
	}{
		{
			name:     "GIF maps to image",
			fileType: artifactpb.FileType_FILE_TYPE_GIF,
			expected: "image",
		},
		{
			name:     "M4A maps to audio",
			fileType: artifactpb.FileType_FILE_TYPE_M4A,
			expected: "audio",
		},
		{
			name:     "MKV maps to video",
			fileType: artifactpb.FileType_FILE_TYPE_MKV,
			expected: "video",
		},
		{
			name:     "DOCX maps to document",
			fileType: artifactpb.FileType_FILE_TYPE_DOCX,
			expected: "document",
		},
		{
			name:     "PNG returns empty (no conversion needed)",
			fileType: artifactpb.FileType_FILE_TYPE_PNG,
			expected: "",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			result := getInputVariableName(tt.fileType)
			c.Assert(result, qt.Equals, tt.expected)
		})
	}
}

func TestGetOutputFieldName(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name         string
		targetFormat string
		expected     string
	}{
		{
			name:         "PNG maps to image",
			targetFormat: "png",
			expected:     "image",
		},
		{
			name:         "OGG maps to audio",
			targetFormat: "ogg",
			expected:     "audio",
		},
		{
			name:         "MP4 maps to video",
			targetFormat: "mp4",
			expected:     "video",
		},
		{
			name:         "PDF maps to document",
			targetFormat: "pdf",
			expected:     "document",
		},
		{
			name:         "Unknown format returns empty",
			targetFormat: "unknown",
			expected:     "",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			result := getOutputFieldName(tt.targetFormat)
			c.Assert(result, qt.Equals, tt.expected)
		})
	}
}

func TestMapFormatToFileType(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		format   string
		expected artifactpb.FileType
	}{
		{
			name:     "PNG maps to FILE_TYPE_PNG",
			format:   "png",
			expected: artifactpb.FileType_FILE_TYPE_PNG,
		},
		{
			name:     "OGG maps to FILE_TYPE_OGG",
			format:   "ogg",
			expected: artifactpb.FileType_FILE_TYPE_OGG,
		},
		{
			name:     "MP4 maps to FILE_TYPE_MP4",
			format:   "mp4",
			expected: artifactpb.FileType_FILE_TYPE_MP4,
		},
		{
			name:     "PDF maps to FILE_TYPE_PDF",
			format:   "pdf",
			expected: artifactpb.FileType_FILE_TYPE_PDF,
		},
		{
			name:     "Unknown format maps to FILE_TYPE_UNSPECIFIED",
			format:   "unknown",
			expected: artifactpb.FileType_FILE_TYPE_UNSPECIFIED,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			result := mapFormatToFileType(tt.format)
			c.Assert(result, qt.Equals, tt.expected)
		})
	}
}

func TestCacheContextActivity_NoAIProvider(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	mockService := mock.NewServiceMock(mc)
	logger := zap.NewNop()

	worker := &Worker{
		service:    mockService,
		log:        logger,
		aiProvider: nil, // No AI provider
	}

	param := &CacheContextActivityParam{
		FileUID:          uuid.Must(uuid.NewV4()),
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
		Bucket:           "test-bucket",
		Destination:      "test/file.pdf",
		FileType:         artifactpb.FileType_FILE_TYPE_PDF,
		Filename:         "file.pdf",
	}

	result, err := worker.CacheContextActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	c.Assert(result.CacheEnabled, qt.IsFalse)
}

func TestCacheContextActivity_UnsupportedFileType(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	mockService := mock.NewServiceMock(mc)
	logger := zap.NewNop()

	mockAIProvider := mock.NewProviderMock(mc)
	mockAIProvider.SupportsFileTypeMock.Return(false)

	worker := &Worker{
		service:    mockService,
		log:        logger,
		aiProvider: mockAIProvider,
	}

	param := &CacheContextActivityParam{
		FileUID:          uuid.Must(uuid.NewV4()),
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
		Bucket:           "test-bucket",
		Destination:      "test/file.xyz",
		FileType:         artifactpb.FileType_FILE_TYPE_UNSPECIFIED,
		Filename:         "file.xyz",
	}

	result, err := worker.CacheContextActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	c.Assert(result.CacheEnabled, qt.IsFalse)
}

func TestCacheContextActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	mockService := mock.NewServiceMock(mc)
	mockMinio := mock.NewMinioIMock(mc)
	logger := zap.NewNop()

	fileContent := []byte("test file content")
	cacheName := "test-cache-123"
	now := time.Now()

	mockService.MinIOMock.Return(mockMinio)
	mockMinio.GetFileMock.
		When(minimock.AnyContext, "test-bucket", "test/file.pdf").
		Then(fileContent, nil)

	mockAIProvider := mock.NewProviderMock(mc)
	mockAIProvider.SupportsFileTypeMock.Return(true)
	mockAIProvider.CreateCacheMock.Return(&ai.CacheResult{
		CacheName:  cacheName,
		Model:      "gemini-2.0-flash",
		CreateTime: now,
		ExpireTime: now.Add(5 * time.Minute),
	}, nil)

	worker := &Worker{
		service:    mockService,
		log:        logger,
		aiProvider: mockAIProvider,
	}

	param := &CacheContextActivityParam{
		FileUID:          uuid.Must(uuid.NewV4()),
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
		Bucket:           "test-bucket",
		Destination:      "test/file.pdf",
		FileType:         artifactpb.FileType_FILE_TYPE_PDF,
		Filename:         "file.pdf",
	}

	result, err := worker.CacheContextActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	c.Assert(result.CacheEnabled, qt.IsTrue)
	c.Assert(result.CacheName, qt.Equals, cacheName)
	c.Assert(result.Model, qt.Equals, "gemini-2.0-flash")
}

func TestDeleteCacheActivity_NoAIProvider(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	mockService := mock.NewServiceMock(mc)
	logger := zap.NewNop()

	worker := &Worker{
		service:    mockService,
		log:        logger,
		aiProvider: nil,
	}

	param := &DeleteCacheActivityParam{
		CacheName: "test-cache",
	}

	err := worker.DeleteCacheActivity(ctx, param)
	c.Assert(err, qt.IsNil) // Should not fail
}

func TestDeleteCacheActivity_EmptyCacheName(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	mockService := mock.NewServiceMock(mc)
	logger := zap.NewNop()

	worker := &Worker{
		service: mockService,
		log:     logger,
	}

	param := &DeleteCacheActivityParam{
		CacheName: "",
	}

	err := worker.DeleteCacheActivity(ctx, param)
	c.Assert(err, qt.IsNil)
}

func TestDeleteCacheActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	mockService := mock.NewServiceMock(mc)
	logger := zap.NewNop()

	mockAIProvider := mock.NewProviderMock(mc)
	mockAIProvider.DeleteCacheMock.
		When(minimock.AnyContext, "test-cache").
		Then(nil)

	worker := &Worker{
		service:    mockService,
		log:        logger,
		aiProvider: mockAIProvider,
	}

	param := &DeleteCacheActivityParam{
		CacheName: "test-cache",
	}

	err := worker.DeleteCacheActivity(ctx, param)
	c.Assert(err, qt.IsNil)
}

func TestConvertToMarkdownFileActivity_WithCache(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	mockService := mock.NewServiceMock(mc)
	mockMinio := mock.NewMinioIMock(mc)
	logger := zap.NewNop()

	fileContent := []byte("test content")
	markdown := "# Test Markdown"
	cacheName := "test-cache"

	mockService.MinIOMock.Return(mockMinio)
	mockMinio.GetFileMock.
		When(minimock.AnyContext, "test-bucket", "test/file.pdf").
		Then(fileContent, nil)

	mockAIProvider := mock.NewProviderMock(mc)
	mockAIProvider.NameMock.Return("gemini")
	mockAIProvider.SupportsFileTypeMock.Return(true)
	mockAIProvider.ConvertToMarkdownWithCacheMock.Return(&ai.ConversionResult{
		Markdown: markdown,
		Length:   []uint32{uint32(len(markdown))},
		Provider: "gemini",
	}, nil)

	worker := &Worker{
		service:    mockService,
		log:        logger,
		aiProvider: mockAIProvider,
	}

	param := &ConvertToMarkdownFileActivityParam{
		Bucket:      "test-bucket",
		Destination: "test/file.pdf",
		FileType:    artifactpb.FileType_FILE_TYPE_PDF,
		CacheName:   cacheName,
	}

	result, err := worker.ConvertToMarkdownFileActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Markdown, qt.Equals, markdown)
	c.Assert(result.Length, qt.HasLen, 1)
}

func TestConvertToMarkdownFileActivity_DirectAI(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	mockService := mock.NewServiceMock(mc)
	mockMinio := mock.NewMinioIMock(mc)
	logger := zap.NewNop()

	fileContent := []byte("test content")
	markdown := "# Direct AI Conversion"

	mockService.MinIOMock.Return(mockMinio)
	mockMinio.GetFileMock.
		When(minimock.AnyContext, "test-bucket", "test/file.pdf").
		Then(fileContent, nil)

	mockAIProvider := mock.NewProviderMock(mc)
	mockAIProvider.NameMock.Return("gemini")
	mockAIProvider.SupportsFileTypeMock.Return(true)
	mockAIProvider.ConvertToMarkdownMock.
		When(minimock.AnyContext, fileContent, artifactpb.FileType_FILE_TYPE_PDF, "file.pdf").
		Then(&ai.ConversionResult{
			Markdown: markdown,
			Length:   []uint32{uint32(len(markdown))},
			Provider: "gemini",
		}, nil)

	worker := &Worker{
		service:    mockService,
		log:        logger,
		aiProvider: mockAIProvider,
	}

	param := &ConvertToMarkdownFileActivityParam{
		Bucket:      "test-bucket",
		Destination: "test/file.pdf",
		FileType:    artifactpb.FileType_FILE_TYPE_PDF,
		CacheName:   "", // No cache
	}

	result, err := worker.ConvertToMarkdownFileActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Markdown, qt.Equals, markdown)
}

func TestConvertToMarkdownFileActivity_PipelineFallback(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	mockService := mock.NewServiceMock(mc)
	mockMinio := mock.NewMinioIMock(mc)
	logger := zap.NewNop()

	fileContent := []byte("test content")
	markdown := "# Pipeline Conversion"

	mockService.MinIOMock.Return(mockMinio)
	mockMinio.GetFileMock.
		When(minimock.AnyContext, "test-bucket", "test/file.pdf").
		Then(fileContent, nil)

	mockService.ConvertToMarkdownPipeMock.Return(&service.MarkdownConversionResult{
		Markdown: markdown,
		Length:   []uint32{uint32(len(markdown))},
	}, nil)

	worker := &Worker{
		service:    mockService,
		log:        logger,
		aiProvider: nil, // No AI provider, should fallback to pipeline
	}

	param := &ConvertToMarkdownFileActivityParam{
		Bucket:      "test-bucket",
		Destination: "test/file.pdf",
		FileType:    artifactpb.FileType_FILE_TYPE_PDF,
		Pipelines:   []service.PipelineRelease{{}},
	}

	result, err := worker.ConvertToMarkdownFileActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Markdown, qt.Equals, markdown)
}
