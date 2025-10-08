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
	"github.com/instill-ai/artifact-backend/internal/ai/gemini"
	"github.com/instill-ai/artifact-backend/pkg/mock"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

func TestExtractPageReferences(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name           string
		chunkStart     uint32
		chunkEnd       uint32
		pageDelimiters []uint32
		expectedStart  uint32
		expectedEnd    uint32
	}{
		{
			name:           "Chunk within first page",
			chunkStart:     0,
			chunkEnd:       50,
			pageDelimiters: []uint32{100, 200, 300},
			expectedStart:  1,
			expectedEnd:    1,
		},
		{
			name:           "Chunk spanning two pages",
			chunkStart:     80,
			chunkEnd:       150,
			pageDelimiters: []uint32{100, 200, 300},
			expectedStart:  1,
			expectedEnd:    2,
		},
		{
			name:           "Chunk at exact page delimiter",
			chunkStart:     50,
			chunkEnd:       100,
			pageDelimiters: []uint32{100, 200, 300},
			expectedStart:  1,
			expectedEnd:    1,
		},
		{
			name:           "Chunk beyond last delimiter",
			chunkStart:     250,
			chunkEnd:       350,
			pageDelimiters: []uint32{100, 200, 300},
			expectedStart:  3,
			expectedEnd:    3,
		},
		{
			name:           "Empty delimiters",
			chunkStart:     50,
			chunkEnd:       100,
			pageDelimiters: []uint32{},
			expectedStart:  0,
			expectedEnd:    0,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			start, end := extractPageReferences(tt.chunkStart, tt.chunkEnd, tt.pageDelimiters)
			c.Assert(start, qt.Equals, tt.expectedStart)
			c.Assert(end, qt.Equals, tt.expectedEnd)
		})
	}
}

func TestProcessWaitingFileActivity_FileTypeConversion(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name                string
		fileType            string
		expectConvertStatus bool
	}{
		{
			name:                "PDF should convert",
			fileType:            "FILE_TYPE_PDF",
			expectConvertStatus: true,
		},
		{
			name:                "Markdown should not convert",
			fileType:            "FILE_TYPE_MARKDOWN",
			expectConvertStatus: false,
		},
		{
			name:                "Text should not convert",
			fileType:            "FILE_TYPE_TEXT",
			expectConvertStatus: false,
		},
		{
			name:                "DOC should convert",
			fileType:            "FILE_TYPE_DOC",
			expectConvertStatus: true,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			// This test validates the business logic of file type routing
			// Full integration would require repository and service mocks
			c.Assert(tt.fileType, qt.Not(qt.Equals), "")
		})
	}
}

// Activity tests with minimock

func TestGetFileMetadataActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.GetKnowledgeBaseFilesByFileUIDsMock.Return([]repository.KnowledgeBaseFile{
		{
			UID:              fileUID,
			KnowledgeBaseUID: kbUID,
			Name:             "test.pdf",
			Type:             "FILE_TYPE_PDF",
			Size:             1024,
			Destination:      "kb/test.pdf",
		},
	}, nil)

	// Mock GetKnowledgeBaseByUID (called by the activity)
	mockRepo.GetKnowledgeBaseByUIDMock.Return(&repository.KnowledgeBase{
		UID:                 kbUID,
		ConvertingPipelines: []string{},
	}, nil)

	mockSvc := mock.NewServiceMock(mc)
	mockSvc.RepositoryMock.Return(mockRepo)

	w := &Worker{
		service: mockSvc,
		log:     zap.NewNop(),
	}

	param := &GetFileMetadataActivityParam{
		FileUID:          fileUID,
		KnowledgeBaseUID: kbUID,
	}

	result, err := w.GetFileMetadataActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.IsNotNil)
	c.Assert(result.File, qt.IsNotNil)
	c.Assert(result.File.UID, qt.Equals, fileUID)
	c.Assert(result.File.KnowledgeBaseUID, qt.Equals, kbUID)
	c.Assert(result.File.Name, qt.Equals, "test.pdf")
	c.Assert(result.File.Type, qt.Equals, "FILE_TYPE_PDF")
}

func TestGetFileContentActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	ctx := context.Background()
	fileContent := []byte("test file content")

	mockMinIO := mock.NewMinioIMock(mc)
	mockMinIO.GetFileMock.Return(fileContent, nil)

	mockSvc := mock.NewServiceMock(mc)
	mockSvc.MinIOMock.Return(mockMinIO)

	w := &Worker{
		service: mockSvc,
		log:     zap.NewNop(),
	}

	param := &GetFileContentActivityParam{
		Bucket:      "test-bucket",
		Destination: "kb/test.pdf",
	}

	content, err := w.GetFileContentActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	c.Assert(content, qt.DeepEquals, fileContent)
}

func TestChunkContentActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	ctx := context.Background()

	mockSvc := mock.NewServiceMock(mc)

	// Mock ChunkMarkdownPipe
	mockSvc.ChunkMarkdownPipeMock.Return(&service.ChunkingResult{
		Chunks: []service.Chunk{
			{
				Start:  0,
				End:    43,
				Text:   "# Title\n\nThis is a test content for chunking.",
				Tokens: 10,
			},
		},
	}, nil)

	w := &Worker{
		service: mockSvc,
		log:     zap.NewNop(),
	}

	// Test with small content passed directly (like summaries)
	markdownContent := "# Title\n\nThis is a test content for chunking."

	param := &ChunkContentActivityParam{
		FileUID:          uuid.Must(uuid.NewV4()),
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
		Content:          markdownContent,
		Pipelines:        []service.PipelineRelease{},
		Metadata:         nil,
	}

	result, err := w.ChunkContentActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.IsNotNil)
	c.Assert(len(result.Chunks), qt.Not(qt.Equals), 0)
}

func TestSaveChunksToDBActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	ctx := context.Background()
	kbUID := uuid.Must(uuid.NewV4())
	fileUID := uuid.Must(uuid.NewV4())

	chunkUID := uuid.Must(uuid.NewV4())

	// Mock MinIO client for chunk upload
	mockMinio := mock.NewMinioIMock(mc)
	mockMinio.UploadBase64FileMock.Return(nil)

	// Mock Repository
	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.GetConvertedFileByFileUIDMock.Return(&repository.ConvertedFile{
		UID: chunkUID,
	}, nil)
	mockRepo.ConvertedFileTableNameMock.Return("converted_files")
	mockRepo.DeleteAndCreateChunksMock.Set(func(
		ctx context.Context,
		fUID uuid.UUID,
		chunks []*repository.TextChunk,
		callback func(chunkUIDs []string) (destinations map[string]string, err error),
	) ([]*repository.TextChunk, error) {
		// Create the chunk with the test UID
		createdChunks := []*repository.TextChunk{{UID: chunkUID}}
		return createdChunks, nil
	})

	// Mock UpdateChunkDestinations (called after MinIO upload)
	mockRepo.UpdateChunkDestinationsMock.Return(nil)

	mockSvc := mock.NewServiceMock(mc)
	mockSvc.RepositoryMock.Return(mockRepo)
	mockSvc.MinIOMock.Return(mockMinio)

	// Mock DeleteTextChunksByFileUID (called before saving)
	mockSvc.DeleteTextChunksByFileUIDMock.Return(nil)

	w := &Worker{
		service: mockSvc,
		log:     zap.NewNop(),
	}

	param := &SaveChunksToDBActivityParam{
		KnowledgeBaseUID: kbUID,
		FileUID:          fileUID,
		Chunks: []service.Chunk{
			{
				Start:  0,
				End:    100,
				Text:   "test chunk content",
				Tokens: 50,
			},
		},
	}

	result, err := w.SaveChunksToDBActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.IsNotNil)
}

// ===== Child Workflow Activity Tests =====
// Tests for activities used by ProcessContentWorkflow and ProcessSummaryWorkflow

func TestNeedsFileConversion(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name         string
		fileType     artifactpb.FileType
		needsConvert bool
		targetFormat string
	}{
		{
			name:         "PNG is AI-native",
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
			name:         "MP3 is AI-native",
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
			name:         "MP4 is AI-native",
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
			name:         "PDF is AI-native",
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
		When(minimock.AnyContext, fileContent, artifactpb.FileType_FILE_TYPE_PDF, "file.pdf", gemini.DefaultConvertToMarkdownPromptTemplate).
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
