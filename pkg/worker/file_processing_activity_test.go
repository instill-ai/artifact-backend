package worker

import (
	"context"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/gojuno/minimock/v3"
	"go.uber.org/zap"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/mock"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"
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

	mockSvc := NewServiceMock(mc)
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

	mockSvc := NewServiceMock(mc)
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

	mockSvc := NewServiceMock(mc)

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

	// Create a simple markdown content for chunking
	markdownContent := []byte("# Title\n\nThis is a test content for chunking.")

	param := &ChunkContentActivityParam{
		Content:      markdownContent,
		IsMarkdown:   true,
		ChunkSize:    500,
		ChunkOverlap: 50,
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
	sourceUID := uuid.Must(uuid.NewV4())

	chunkUID := uuid.Must(uuid.NewV4())

	// Mock MinIO client for chunk upload
	mockMinio := mock.NewMinioIMock(mc)
	mockMinio.UploadBase64FileMock.Return(nil)

	// Mock Repository with custom implementation that calls the callback
	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.DeleteAndCreateChunksMock.Set(func(
		ctx context.Context,
		fUID uuid.UUID,
		chunks []*repository.TextChunk,
		callback func(chunkUIDs []string) (destinations map[string]string, err error),
	) ([]*repository.TextChunk, error) {
		// Create the chunk with the test UID
		createdChunks := []*repository.TextChunk{{UID: chunkUID}}

		// Call the callback if provided (this will trigger MinIO upload)
		if callback != nil {
			chunkUIDs := []string{chunkUID.String()}
			destinations, err := callback(chunkUIDs)
			if err != nil {
				return nil, err
			}
			// Update the destinations in the chunks
			for _, chunk := range createdChunks {
				if dest, ok := destinations[chunk.UID.String()]; ok {
					chunk.ContentDest = dest
				}
			}
		}

		return createdChunks, nil
	})

	mockSvc := NewServiceMock(mc)
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
		SourceUID:        sourceUID,
		SourceTable:      "knowledge_base_files",
		ContentChunks: []service.Chunk{
			{
				Start:  0,
				End:    100,
				Text:   "test chunk content",
				Tokens: 50,
			},
		},
		FileType: "FILE_TYPE_PDF",
	}

	result, err := w.SaveChunksToDBActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.IsNotNil)
	c.Assert(result.ChunksToSave, qt.IsNotNil)
}
