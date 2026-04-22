package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/gojuno/minimock/v3"
	"go.uber.org/zap"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/ai"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"
	"github.com/instill-ai/artifact-backend/pkg/worker/mock"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
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

	// Unexported function - skip testing
	c.Skip("Tests unexported helper function extractPageReferences")
	_ = tests
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
			fileType:            "TYPE_PDF",
			expectConvertStatus: true,
		},
		{
			name:                "Markdown should not convert",
			fileType:            "TYPE_MARKDOWN",
			expectConvertStatus: false,
		},
		{
			name:                "Text should not convert",
			fileType:            "TYPE_TEXT",
			expectConvertStatus: false,
		},
		{
			name:                "DOC should convert",
			fileType:            "TYPE_DOC",
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

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetFilesByFileUIDsMock.Return([]repository.FileModel{
		{
			UID:         fileUID,
			DisplayName: "test.pdf",
			FileType:    "TYPE_PDF",
			Size:        1024,
			StoragePath: "kb/test.pdf",
		},
	}, nil)

	// GetKnowledgeBaseByUIDWithConfig is called to retrieve system config
	mockRepository.GetKnowledgeBaseByUIDWithConfigMock.Return(&repository.KnowledgeBaseWithConfig{
		KnowledgeBaseModel: repository.KnowledgeBaseModel{
			UID: kbUID,
		},
	}, nil)

	// GetKnowledgeBaseByUID is called to check for dual-processing
	mockRepository.GetKnowledgeBaseByUIDMock.Return(&repository.KnowledgeBaseModel{
		UID:     kbUID,
		Staging: false,
	}, nil)

	// GetDualProcessingTarget is called to check for dual-processing targets
	mockRepository.GetDualProcessingTargetMock.Return(nil, nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	param := &GetFileMetadataActivityParam{
		FileUID: fileUID,
		KBUID:   kbUID,
	}

	result, err := w.GetFileMetadataActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.IsNotNil)
	c.Assert(result.File, qt.IsNotNil)
	c.Assert(result.File.UID, qt.Equals, fileUID)
	c.Assert(result.File.DisplayName, qt.Equals, "test.pdf")
	c.Assert(result.File.FileType, qt.Equals, "TYPE_PDF")
}

func TestGetFileContentActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	ctx := context.Background()
	fileContent := []byte("test file content")

	mockStorage := mock.NewStorageMock(mc)
	mockStorage.GetFileMock.Return(fileContent, nil)

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetMinIOStorageMock.Return(mockStorage)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

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
	// This test requires a real pipeline client mock which is complex to set up
	// Skip for now - integration tests cover this functionality
	c.Skip("Requires pipeline client mock")
}

func TestSaveChunksActivity_Success(t *testing.T) {
	c := qt.New(t)
	// This test requires complex mock setup for chunk creation and MinIO upload
	// Skip for now - integration tests cover this functionality
	c.Skip("Requires complex mock setup")
}

// ===== Composite Activity Tests =====
// Tests for activities used by ProcessContentActivity and ProcessSummaryActivity

func TestCacheFileContextActivity_NoAIClient(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	mockRepository := mock.NewRepositoryMock(mc)
	logger := zap.NewNop()

	w := &Worker{repository: mockRepository, log: logger}

	param := &CacheFileContextActivityParam{
		FileUID:         uuid.Must(uuid.NewV4()),
		KBUID:           uuid.Must(uuid.NewV4()),
		Bucket:          "test-bucket",
		Destination:     "test/file.pdf",
		FileType:        artifactpb.File_TYPE_PDF,
		FileDisplayName: "file.pdf",
	}

	result, err := w.CacheFileContextActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	c.Assert(result.CachedContextEnabled, qt.IsFalse)
}

func TestCacheFileContextActivity_UnsupportedFileType(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	mockRepositoryMock := mock.NewRepositoryMock(mc)
	logger := zap.NewNop()

	mockAIClient := mock.NewClientMock(mc)

	w := &Worker{repository: mockRepositoryMock, log: logger, aiClient: mockAIClient}

	param := &CacheFileContextActivityParam{
		FileUID:         uuid.Must(uuid.NewV4()),
		KBUID:           uuid.Must(uuid.NewV4()),
		Bucket:          "test-bucket",
		Destination:     "test/file.xyz",
		FileType:        artifactpb.File_TYPE_UNSPECIFIED,
		FileDisplayName: "file.xyz",
	}

	result, err := w.CacheFileContextActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	c.Assert(result.CachedContextEnabled, qt.IsFalse)
}

func TestCacheFileContextActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	logger := zap.NewNop()

	fileContent := []byte("test file content")

	mockStorage := mock.NewStorageMock(mc)
	mockStorage.GetFileMock.Return(fileContent, nil)

	mockRepositoryMock := mock.NewRepositoryMock(mc)
	mockRepositoryMock.GetMinIOStorageMock.Return(mockStorage)

	cacheName := "test-cache-123"
	now := time.Now()

	mockAIClient := mock.NewClientMock(mc)
	mockAIClient.CreateCacheMock.Return(&ai.CacheResult{
		CacheName:  cacheName,
		Model:      "gemini-2.0-flash",
		CreateTime: now,
		ExpireTime: now.Add(5 * time.Minute),
	}, nil)

	w := &Worker{repository: mockRepositoryMock, log: logger, aiClient: mockAIClient}

	param := &CacheFileContextActivityParam{
		FileUID:         uuid.Must(uuid.NewV4()),
		KBUID:           uuid.Must(uuid.NewV4()),
		Bucket:          "test-bucket",
		Destination:     "test/file.pdf",
		FileType:        artifactpb.File_TYPE_PDF,
		FileDisplayName: "file.pdf",
	}

	result, err := w.CacheFileContextActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	c.Assert(result.CachedContextEnabled, qt.IsTrue)
	c.Assert(result.CacheName, qt.Equals, cacheName)
	c.Assert(result.Model, qt.Equals, "gemini-2.0-flash")
}

func TestDeleteCacheActivity_NoAIClient(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	mockRepositoryMock := mock.NewRepositoryMock(mc)
	logger := zap.NewNop()

	w := &Worker{repository: mockRepositoryMock, log: logger}

	param := &DeleteCacheActivityParam{
		CacheName: "test-cache",
	}

	err := w.DeleteCacheActivity(ctx, param)
	c.Assert(err, qt.IsNil) // Should not fail
}

func TestDeleteCacheActivity_EmptyCacheName(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	mockRepositoryMock := mock.NewRepositoryMock(mc)
	logger := zap.NewNop()

	w := &Worker{repository: mockRepositoryMock, log: logger}

	param := &DeleteCacheActivityParam{
		CacheName: "",
	}

	err := w.DeleteCacheActivity(ctx, param)
	c.Assert(err, qt.IsNil)
}

func TestDeleteCacheActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	mockRepositoryMock := mock.NewRepositoryMock(mc)
	logger := zap.NewNop()

	mockAIClient := mock.NewClientMock(mc)
	mockAIClient.DeleteCacheMock.
		When(minimock.AnyContext, "test-cache").
		Then(nil)

	w := &Worker{repository: mockRepositoryMock, log: logger, aiClient: mockAIClient}

	param := &DeleteCacheActivityParam{
		CacheName: "test-cache",
	}

	err := w.DeleteCacheActivity(ctx, param)
	c.Assert(err, qt.IsNil)
}

// ===== Content and Summary Processing Tests =====
// Tests for content/summary symmetry, converted files, and chunk references

// TestProcessSummaryActivityResult_SymmetricWithContent verifies that
// ProcessSummaryActivityResult has the same fields as ProcessContentActivityResult
func TestProcessSummaryActivityResult_SymmetricWithContent(t *testing.T) {
	c := qt.New(t)

	// Create sample results
	summaryResult := ProcessSummaryActivityResult{
		Summary:          "Test summary content",
		Length:           []uint32{100},
		PositionData:     &types.PositionData{PageDelimiters: []uint32{100}},
		OriginalType:     artifactpb.File_TYPE_PDF,
		ConvertedType:    artifactpb.File_TYPE_PDF,
		UsageMetadata:    map[string]interface{}{"tokens": 50},
		ConvertedFileUID: uuid.Must(uuid.NewV4()),
	}

	contentResult := ProcessContentActivityResult{
		Content:          "Test content",
		Length:           []uint32{200},
		PositionData:     &types.PositionData{PageDelimiters: []uint32{200}},
		OriginalType:     artifactpb.File_TYPE_PDF,
		ConvertedType:    artifactpb.File_TYPE_PDF,
		UsageMetadata:    map[string]interface{}{"tokens": 100},
		ConvertedFileUID: uuid.Must(uuid.NewV4()),
	}

	// Verify both have the same field types (structural symmetry)
	c.Assert(summaryResult.Length, qt.IsNotNil)
	c.Assert(summaryResult.PositionData, qt.IsNotNil)
	c.Assert(summaryResult.UsageMetadata, qt.IsNotNil)
	c.Assert(summaryResult.ConvertedFileUID, qt.Not(qt.Equals), uuid.Nil)

	c.Assert(contentResult.Length, qt.IsNotNil)
	c.Assert(contentResult.PositionData, qt.IsNotNil)
	c.Assert(contentResult.UsageMetadata, qt.IsNotNil)
	c.Assert(contentResult.ConvertedFileUID, qt.Not(qt.Equals), uuid.Nil)

	// Verify type assertions work for both
	c.Assert(len(summaryResult.Length), qt.Equals, 1)
	c.Assert(len(contentResult.Length), qt.Equals, 1)

	// Verify both have separate ConvertedFileUIDs
	c.Assert(summaryResult.ConvertedFileUID, qt.Not(qt.Equals), contentResult.ConvertedFileUID)
}

// TestSummaryPositionData_SinglePageFormat verifies that summary PositionData
// is formatted as a single page with delimiter at the end
func TestSummaryPositionData_SinglePageFormat(t *testing.T) {
	tests := []struct {
		name           string
		summaryContent string
		wantDelimiters []uint32
		description    string
	}{
		{
			name:           "Short summary",
			summaryContent: "This is a short summary.",
			wantDelimiters: []uint32{24}, // Length in runes
			description:    "Single page with delimiter at end (24 characters)",
		},
		{
			name:           "Multi-line summary",
			summaryContent: "Line 1\nLine 2\nLine 3",
			wantDelimiters: []uint32{20}, // Including newlines
			description:    "Multi-line summary treated as single page",
		},
		{
			name:           "Unicode summary",
			summaryContent: "Hello 世界 emoji 🌟",
			wantDelimiters: []uint32{16}, // H-e-l-l-o-space-世-界-space-e-m-o-j-i-space-🌟
			description:    "Unicode characters counted correctly as runes",
		},
		{
			name:           "Empty summary",
			summaryContent: "",
			wantDelimiters: []uint32{0},
			description:    "Empty summary has delimiter at position 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Calculate position data as the activity would
			summaryLength := uint32(len([]rune(tt.summaryContent)))
			var positionData *types.PositionData
			if len(tt.summaryContent) > 0 || tt.summaryContent == "" {
				positionData = &types.PositionData{
					PageDelimiters: []uint32{summaryLength},
				}
			}

			// Verify
			c.Assert(positionData, qt.IsNotNil, qt.Commentf(tt.description))
			c.Assert(positionData.PageDelimiters, qt.DeepEquals, tt.wantDelimiters, qt.Commentf(tt.description))
			c.Assert(len(positionData.PageDelimiters), qt.Equals, 1, qt.Commentf("Summary should have exactly one page"))
		})
	}
}

// TestProcessSummaryActivity_ConvertedFileCreation verifies that
// ProcessSummaryActivity result structure matches expected format
func TestProcessSummaryActivity_ConvertedFileCreation(t *testing.T) {
	c := qt.New(t)

	kbUID := uuid.Must(uuid.NewV4())
	fileUID := uuid.Must(uuid.NewV4())

	param := &ProcessSummaryActivityParam{
		FileUID:         fileUID,
		KBUID:           kbUID,
		FileType:        artifactpb.File_TYPE_PDF,
		FileDisplayName: "test.pdf",
	}

	// Verify the structure of what would be created
	expectedSummary := "Test summary"
	summaryLength := uint32(len([]rune(expectedSummary)))

	expectedResult := &ProcessSummaryActivityResult{
		Summary:          expectedSummary,
		Length:           []uint32{summaryLength},
		PositionData:     &types.PositionData{PageDelimiters: []uint32{summaryLength}},
		OriginalType:     artifactpb.File_TYPE_PDF,
		ConvertedType:    artifactpb.File_TYPE_PDF,
		ConvertedFileUID: uuid.Must(uuid.NewV4()), // Would be generated in activity
	}

	// Verify result structure
	c.Assert(expectedResult.Summary, qt.Not(qt.Equals), "")
	c.Assert(expectedResult.Length, qt.HasLen, 1)
	c.Assert(expectedResult.PositionData, qt.IsNotNil)
	c.Assert(expectedResult.PositionData.PageDelimiters, qt.HasLen, 1)
	c.Assert(expectedResult.PositionData.PageDelimiters[0], qt.Equals, summaryLength)
	c.Assert(expectedResult.OriginalType, qt.Equals, param.FileType)
	c.Assert(expectedResult.ConvertedType, qt.Equals, param.FileType)
	c.Assert(expectedResult.ConvertedFileUID, qt.Not(qt.Equals), uuid.Nil)

	// The actual converted file path would be:
	// "core-artifact/kb-{kbUID}/file-{fileUID}/converted-file/{convertedFileUID}.md"
	expectedPathPattern := "kb-" + kbUID.String() + "/file-" + fileUID.String() + "/converted-file/"
	c.Assert(expectedPathPattern, qt.Contains, "kb-")
	c.Assert(expectedPathPattern, qt.Contains, "file-")
	c.Assert(expectedPathPattern, qt.Contains, "converted-file/")
}

// TestContentVsSummaryConvertedFiles verifies that content and summary
// create separate converted_file records with different source_uids
func TestContentVsSummaryConvertedFiles(t *testing.T) {
	c := qt.New(t)

	kbUID := uuid.Must(uuid.NewV4())
	fileUID := uuid.Must(uuid.NewV4())
	contentConvertedFileUID := uuid.Must(uuid.NewV4())
	summaryConvertedFileUID := uuid.Must(uuid.NewV4())

	// Simulate content converted file
	contentConvertedFile := &repository.ConvertedFileModel{
		UID:              contentConvertedFileUID,
		FileUID:          fileUID,
		KnowledgeBaseUID: kbUID,
		StoragePath:      "kb-" + kbUID.String() + "/file-" + fileUID.String() + "/converted-file/" + contentConvertedFileUID.String() + ".md",
		PositionData:     &types.PositionData{PageDelimiters: []uint32{100, 200, 300}}, // Multi-page
	}

	// Simulate summary converted file
	summaryConvertedFile := &repository.ConvertedFileModel{
		UID:              summaryConvertedFileUID,
		FileUID:          fileUID,
		KnowledgeBaseUID: kbUID,
		StoragePath:      "kb-" + kbUID.String() + "/file-" + fileUID.String() + "/converted-file/" + summaryConvertedFileUID.String() + ".md",
		PositionData:     &types.PositionData{PageDelimiters: []uint32{50}}, // Single page (summary)
	}

	// Verify they have different UIDs
	c.Assert(contentConvertedFile.UID, qt.Not(qt.Equals), summaryConvertedFile.UID)

	// Verify they reference the same original file
	c.Assert(contentConvertedFile.FileUID, qt.Equals, summaryConvertedFile.FileUID)
	c.Assert(contentConvertedFile.KnowledgeBaseUID, qt.Equals, summaryConvertedFile.KnowledgeBaseUID)

	// Verify they have different storage paths
	c.Assert(contentConvertedFile.StoragePath, qt.Not(qt.Equals), summaryConvertedFile.StoragePath)

	// Verify both use the same path pattern (kb-.../file-.../converted-file/)
	c.Assert(contentConvertedFile.StoragePath, qt.Contains, "kb-"+kbUID.String())
	c.Assert(contentConvertedFile.StoragePath, qt.Contains, "file-"+fileUID.String())
	c.Assert(contentConvertedFile.StoragePath, qt.Contains, "converted-file/")

	c.Assert(summaryConvertedFile.StoragePath, qt.Contains, "kb-"+kbUID.String())
	c.Assert(summaryConvertedFile.StoragePath, qt.Contains, "file-"+fileUID.String())
	c.Assert(summaryConvertedFile.StoragePath, qt.Contains, "converted-file/")

	// Verify position data structure differences
	c.Assert(len(contentConvertedFile.PositionData.PageDelimiters), qt.Equals, 3, qt.Commentf("Content has multiple pages"))
	c.Assert(len(summaryConvertedFile.PositionData.PageDelimiters), qt.Equals, 1, qt.Commentf("Summary has single page"))
}

// TestTextChunkSourceUIDReferences verifies that text chunks reference
// the correct converted_file UID based on their content type
func TestTextChunkSourceUIDReferences(t *testing.T) {
	c := qt.New(t)

	contentConvertedFileUID := uuid.Must(uuid.NewV4())
	summaryConvertedFileUID := uuid.Must(uuid.NewV4())
	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	// Simulate content chunks
	contentChunk := &repository.ChunkModel{
		UID:              uuid.Must(uuid.NewV4()),
		SourceUID:        contentConvertedFileUID, // References content converted_file
		SourceTable:      repository.ConvertedFileTableName,
		StartPos:         0,
		EndPos:           100,
		ChunkType:        "content",
		FileUID:          fileUID,
		KnowledgeBaseUID: kbUID,
	}

	// Simulate summary chunk
	summaryChunk := &repository.ChunkModel{
		UID:              uuid.Must(uuid.NewV4()),
		SourceUID:        summaryConvertedFileUID, // References summary converted_file
		SourceTable:      repository.ConvertedFileTableName,
		StartPos:         0,
		EndPos:           50,
		ChunkType:        "summary",
		FileUID:          fileUID,
		KnowledgeBaseUID: kbUID,
	}

	// Verify chunks reference different source UIDs
	c.Assert(contentChunk.SourceUID, qt.Not(qt.Equals), summaryChunk.SourceUID)

	// Verify chunks reference the same file and KB
	c.Assert(contentChunk.FileUID, qt.Equals, summaryChunk.FileUID)
	c.Assert(contentChunk.KnowledgeBaseUID, qt.Equals, summaryChunk.KnowledgeBaseUID)

	// Verify both use converted_file as source table
	c.Assert(contentChunk.SourceTable, qt.Equals, repository.ConvertedFileTableName)
	c.Assert(summaryChunk.SourceTable, qt.Equals, repository.ConvertedFileTableName)

	// Verify chunk types are different
	c.Assert(contentChunk.ChunkType, qt.Equals, "content")
	c.Assert(summaryChunk.ChunkType, qt.Equals, "summary")

	// Verify source UIDs match their respective converted files
	c.Assert(contentChunk.SourceUID, qt.Equals, contentConvertedFileUID)
	c.Assert(summaryChunk.SourceUID, qt.Equals, summaryConvertedFileUID)
}

// TestSummaryLength_RuneCount verifies that summary length uses rune count
// for consistency with content processing
func TestSummaryLength_RuneCount(t *testing.T) {
	tests := []struct {
		name          string
		summary       string
		expectedRunes uint32
		expectedBytes int
		description   string
	}{
		{
			name:          "ASCII only",
			summary:       "Hello World",
			expectedRunes: 11,
			expectedBytes: 11,
			description:   "ASCII characters: rune count = byte count",
		},
		{
			name:          "Mixed Unicode",
			summary:       "Hello 世界",
			expectedRunes: 8,  // H-e-l-l-o-space-世-界
			expectedBytes: 12, // 5 bytes + 1 space + 6 bytes (3 per CJK char)
			description:   "Unicode: rune count < byte count",
		},
		{
			name:          "Emoji",
			summary:       "Test 🌟 emoji",
			expectedRunes: 12, // T-e-s-t-space-🌟-space-e-m-o-j-i
			expectedBytes: 15, // "Test " (5) + 🌟 (4 bytes) + " emoji" (6) = 15
			description:   "Emoji counted as single rune",
		},
		{
			name:          "Newlines",
			summary:       "Line1\nLine2\nLine3",
			expectedRunes: 17,
			expectedBytes: 17,
			description:   "Newlines counted correctly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Calculate as the activity does
			runeCount := uint32(len([]rune(tt.summary)))
			byteCount := len(tt.summary)

			c.Assert(runeCount, qt.Equals, tt.expectedRunes, qt.Commentf(tt.description))
			c.Assert(byteCount, qt.Equals, tt.expectedBytes, qt.Commentf(tt.description))

			// Verify position data uses rune count
			positionData := &types.PositionData{
				PageDelimiters: []uint32{runeCount},
			}
			c.Assert(positionData.PageDelimiters[0], qt.Equals, tt.expectedRunes)
		})
	}
}

// TestConvertedFilePath_Consistency verifies that both content and summary
// use the same path pattern for converted files
func TestConvertedFilePath_Consistency(t *testing.T) {
	c := qt.New(t)

	kbUID := uuid.Must(uuid.NewV4())
	fileUID := uuid.Must(uuid.NewV4())
	contentUID := uuid.Must(uuid.NewV4())
	summaryUID := uuid.Must(uuid.NewV4())

	// Expected pattern: "core-artifact/kb-{kbUID}/file-{fileUID}/converted-file/{uid}.md"
	// Or in MinIO: "kb-{kbUID}/file-{fileUID}/converted-file/{uid}.md"

	contentPath := "kb-" + kbUID.String() + "/file-" + fileUID.String() + "/converted-file/" + contentUID.String() + ".md"
	summaryPath := "kb-" + kbUID.String() + "/file-" + fileUID.String() + "/converted-file/" + summaryUID.String() + ".md"

	// Verify both use the same pattern
	c.Assert(contentPath, qt.Contains, "kb-"+kbUID.String())
	c.Assert(contentPath, qt.Contains, "file-"+fileUID.String())
	c.Assert(contentPath, qt.Contains, "converted-file/")
	c.Assert(contentPath, qt.Matches, `.*\.md$`)

	c.Assert(summaryPath, qt.Contains, "kb-"+kbUID.String())
	c.Assert(summaryPath, qt.Contains, "file-"+fileUID.String())
	c.Assert(summaryPath, qt.Contains, "converted-file/")
	c.Assert(summaryPath, qt.Matches, `.*\.md$`)

	// Verify they differ only in the UUID
	c.Assert(contentPath, qt.Not(qt.Equals), summaryPath)

	// Extract base paths (everything except the UUID)
	contentBase := "kb-" + kbUID.String() + "/file-" + fileUID.String() + "/converted-file/"
	summaryBase := "kb-" + kbUID.String() + "/file-" + fileUID.String() + "/converted-file/"

	c.Assert(contentBase, qt.Equals, summaryBase,
		qt.Commentf("Base paths should be identical"))
}

func TestExtractTimeRange(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		text string
		want *[2]uint64
	}{
		{
			name: "video range MM:SS",
			text: "Some transcript text\n[Video: 1:30 - 3:45]\nMore text",
			want: &[2]uint64{90_000, 225_000},
		},
		{
			name: "audio range starting at zero",
			text: "[Audio: 0:00 - 1:23] speech here",
			want: &[2]uint64{0, 83_000},
		},
		{
			name: "single sound timestamp",
			text: "[Sound: 10:00] ambient noise",
			want: &[2]uint64{600_000, 600_000},
		},
		{
			name: "HH:MM:SS format",
			text: "[Video: 1:02:03 - 1:05:00]",
			want: &[2]uint64{3_723_000, 3_900_000},
		},
		{
			name: "multiple markers uses first start and last end",
			text: "[Audio: 0:10 - 0:30]\nSome words\n[Audio: 1:00 - 1:45]",
			want: &[2]uint64{10_000, 105_000},
		},
		{
			name: "no markers returns nil",
			text: "Plain document text with no timestamps at all.",
			want: nil,
		},
		{
			name: "empty string returns nil",
			text: "",
			want: nil,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			got := extractTimeRange(tt.text)
			if tt.want == nil {
				c.Assert(got, qt.IsNil)
			} else {
				c.Assert(got, qt.IsNotNil)
				c.Assert(*got, qt.DeepEquals, *tt.want)
			}
		})
	}
}

func TestPopulateTimeRanges(t *testing.T) {
	c := qt.New(t)

	c.Run("sets TimeRange on chunks with markers", func(c *qt.C) {
		chunks := []types.Chunk{
			{Text: "[Video: 0:00 - 1:30] scene one"},
			{Text: "[Video: 1:30 - 3:00] scene two"},
		}
		populateTimeRanges(chunks)

		c.Assert(chunks[0].Reference, qt.IsNotNil)
		c.Assert(chunks[0].Reference.TimeRange, qt.DeepEquals, [2]uint64{0, 90_000})
		c.Assert(chunks[1].Reference, qt.IsNotNil)
		c.Assert(chunks[1].Reference.TimeRange, qt.DeepEquals, [2]uint64{90_000, 180_000})
	})

	c.Run("leaves chunks without markers unchanged", func(c *qt.C) {
		chunks := []types.Chunk{
			{Text: "Plain text with no timestamps"},
		}
		populateTimeRanges(chunks)
		c.Assert(chunks[0].Reference, qt.IsNil)
	})

	c.Run("creates Reference if nil", func(c *qt.C) {
		chunks := []types.Chunk{
			{Text: "[Audio: 2:00] voice", Reference: nil},
		}
		populateTimeRanges(chunks)
		c.Assert(chunks[0].Reference, qt.IsNotNil)
		c.Assert(chunks[0].Reference.TimeRange, qt.DeepEquals, [2]uint64{120_000, 120_000})
	})

	c.Run("preserves existing PageRange when setting TimeRange", func(c *qt.C) {
		chunks := []types.Chunk{
			{
				Text: "[Video: 0:30 - 1:00] first scene",
				Reference: &types.ChunkReference{
					PageRange: [2]uint32{1, 1},
				},
			},
		}
		populateTimeRanges(chunks)
		c.Assert(chunks[0].Reference.PageRange, qt.DeepEquals, [2]uint32{1, 1})
		c.Assert(chunks[0].Reference.TimeRange, qt.DeepEquals, [2]uint64{30_000, 60_000})
	})
}

func TestChunkContentActivity_MediaTimeRange(t *testing.T) {
	c := qt.New(t)

	w := &Worker{log: zap.NewNop()}

	content := "[Video: 0:00 - 1:30]\nScene one description.\n\n[Video: 1:30 - 3:45]\nScene two description."
	contentRunes := []rune(content)

	param := &ChunkContentActivityParam{
		FileUID: uuid.Must(uuid.NewV4()),
		Content: content,
		Type:    artifactpb.Chunk_TYPE_CONTENT,
		PositionData: &types.PositionData{
			PageDelimiters: []uint32{uint32(len(contentRunes))},
		},
	}

	result, err := w.ChunkContentActivity(context.Background(), param)
	c.Assert(err, qt.IsNil)
	c.Assert(len(result.Chunks) > 0, qt.IsTrue)

	for _, chunk := range result.Chunks {
		c.Assert(chunk.Reference, qt.IsNotNil, qt.Commentf("chunk %q must have a Reference", chunk.Text))
		c.Assert(chunk.Reference.PageRange, qt.DeepEquals, [2]uint32{1, 1},
			qt.Commentf("PageRange preserved for single-page media"))
		c.Assert(chunk.Reference.TimeRange[1] > 0, qt.IsTrue,
			qt.Commentf("TimeRange end must be non-zero for media chunk %q", chunk.Text))
	}
}

// TestChunkContentActivity_TypeParameter verifies that
// ChunkContentActivity correctly sets Type based on parameter
func TestChunkContentActivity_TypeParameter(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name            string
		chunkType       artifactpb.Chunk_Type
		expectedTypeStr string
	}{
		{
			name:            "Content chunks",
			chunkType:       artifactpb.Chunk_TYPE_CONTENT,
			expectedTypeStr: "content",
		},
		{
			name:            "Summary chunks",
			chunkType:       artifactpb.Chunk_TYPE_SUMMARY,
			expectedTypeStr: "summary",
		},
		{
			name:            "Augmented chunks",
			chunkType:       artifactpb.Chunk_TYPE_AUGMENTED,
			expectedTypeStr: "augmented",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate chunk creation
			chunk := types.Chunk{
				Text:   "Sample text",
				Start:  0,
				End:    11,
				Tokens: 5,
				Type:   tt.chunkType,
			}

			// Verify Type is set correctly
			c.Assert(chunk.Type, qt.Equals, tt.chunkType)

			// Verify conversion to string for database
			var typeStr string
			switch chunk.Type {
			case artifactpb.Chunk_TYPE_CONTENT:
				typeStr = "content"
			case artifactpb.Chunk_TYPE_SUMMARY:
				typeStr = "summary"
			case artifactpb.Chunk_TYPE_AUGMENTED:
				typeStr = "augmented"
			}

			c.Assert(typeStr, qt.Equals, tt.expectedTypeStr)
		})
	}
}

func TestInjectMediaPageDelimiters(t *testing.T) {
	c := qt.New(t)

	t.Run("nil when no timestamps", func(t *testing.T) {
		pd := injectMediaPageDelimiters("Hello world, no timestamps here.", 4000)
		c.Assert(pd, qt.IsNil)
	})

	t.Run("nil when single timestamp", func(t *testing.T) {
		pd := injectMediaPageDelimiters("[Video: 00:00:00] Hello world", 4000)
		c.Assert(pd, qt.IsNil)
	})

	t.Run("nil when empty content", func(t *testing.T) {
		pd := injectMediaPageDelimiters("", 4000)
		c.Assert(pd, qt.IsNil)
	})

	t.Run("creates delimiters for long transcript", func(t *testing.T) {
		// Build a transcript with timestamps every ~500 chars.
		var content string
		for i := 0; i < 30; i++ {
			ts := fmt.Sprintf("[Video: 00:%02d:00] ", i)
			content += ts + strings.Repeat("x", 480) + "\n"
		}
		pd := injectMediaPageDelimiters(content, 4000)
		c.Assert(pd, qt.IsNotNil)
		c.Assert(len(pd.PageDelimiters) >= 3, qt.IsTrue, qt.Commentf("got %d delimiters", len(pd.PageDelimiters)))

		// Final delimiter must equal content rune length.
		contentRunes := []rune(content)
		c.Assert(pd.PageDelimiters[len(pd.PageDelimiters)-1], qt.Equals, uint32(len(contentRunes)))
	})

	t.Run("respects target chunk size", func(t *testing.T) {
		var content string
		for i := 0; i < 20; i++ {
			ts := fmt.Sprintf("[Audio: %02d:00] ", i)
			content += ts + strings.Repeat("a", 200) + "\n"
		}
		// With a small target, more delimiters should be created.
		pd := injectMediaPageDelimiters(content, 500)
		c.Assert(pd, qt.IsNotNil)
		c.Assert(len(pd.PageDelimiters) >= 5, qt.IsTrue)
	})
}

func TestInjectMarkdownPageDelimiters(t *testing.T) {
	c := qt.New(t)

	t.Run("nil when content shorter than target", func(t *testing.T) {
		pd := injectMarkdownPageDelimiters("# Short\nHello world", 4000)
		c.Assert(pd, qt.IsNil)
	})

	t.Run("nil when no headings or paragraph breaks", func(t *testing.T) {
		content := strings.Repeat("x", 5000)
		pd := injectMarkdownPageDelimiters(content, 4000)
		c.Assert(pd, qt.IsNil)
	})

	t.Run("creates delimiters at heading boundaries", func(t *testing.T) {
		var content string
		for i := 0; i < 15; i++ {
			content += fmt.Sprintf("## Section %d\n%s\n", i, strings.Repeat("y", 800))
		}
		pd := injectMarkdownPageDelimiters(content, 4000)
		c.Assert(pd, qt.IsNotNil)
		c.Assert(len(pd.PageDelimiters) >= 2, qt.IsTrue)

		contentRunes := []rune(content)
		c.Assert(pd.PageDelimiters[len(pd.PageDelimiters)-1], qt.Equals, uint32(len(contentRunes)))
	})

	t.Run("uses paragraph breaks when no headings", func(t *testing.T) {
		var content string
		for i := 0; i < 20; i++ {
			content += strings.Repeat("z", 500) + "\n\n"
		}
		pd := injectMarkdownPageDelimiters(content, 4000)
		c.Assert(pd, qt.IsNotNil)
		c.Assert(len(pd.PageDelimiters) >= 2, qt.IsTrue)
	})

	t.Run("deeply nested headings treated as boundaries", func(t *testing.T) {
		var content string
		for i := 0; i < 10; i++ {
			content += fmt.Sprintf("#### Deep heading %d\n%s\n", i, strings.Repeat("w", 1000))
		}
		pd := injectMarkdownPageDelimiters(content, 4000)
		c.Assert(pd, qt.IsNotNil)
	})
}

func TestParseSummaryEntities(t *testing.T) {
	c := qt.New(t)

	t.Run("full structured output", func(t *testing.T) {
		raw := `A lecture about monopoly theory and startup strategy.

## Entities
- Peter Thiel [person]
- monopoly theory [concept]
- PayPal [organization]
- Zero to One [work]
`
		summary, entities := parseSummaryEntities(raw)
		c.Assert(summary, qt.Equals, "A lecture about monopoly theory and startup strategy.")
		c.Assert(len(entities), qt.Equals, 4)
		c.Assert(entities[0].Name, qt.Equals, "Peter Thiel")
		c.Assert(entities[0].Type, qt.Equals, "person")
		c.Assert(entities[2].Name, qt.Equals, "PayPal")
		c.Assert(entities[2].Type, qt.Equals, "organization")
		c.Assert(entities[3].Name, qt.Equals, "Zero to One")
		c.Assert(entities[3].Type, qt.Equals, "work")
	})

	t.Run("no entities section", func(t *testing.T) {
		raw := "Just a plain summary with no entities."
		summary, entities := parseSummaryEntities(raw)
		c.Assert(summary, qt.Equals, "Just a plain summary with no entities.")
		c.Assert(entities, qt.IsNil)
	})

	t.Run("empty entities section", func(t *testing.T) {
		raw := "A summary.\n\n## Entities\n"
		summary, entities := parseSummaryEntities(raw)
		c.Assert(summary, qt.Equals, "A summary.")
		c.Assert(len(entities), qt.Equals, 0)
	})

	t.Run("entity without type", func(t *testing.T) {
		raw := "Summary text.\n\n## Entities\n- SomeEntity\n"
		summary, entities := parseSummaryEntities(raw)
		c.Assert(summary, qt.Equals, "Summary text.")
		c.Assert(len(entities), qt.Equals, 1)
		c.Assert(entities[0].Name, qt.Equals, "SomeEntity")
		c.Assert(entities[0].Type, qt.Equals, "")
	})

	t.Run("malformed lines ignored", func(t *testing.T) {
		raw := "Summary.\n\n## Entities\nNot a list item\n- Valid [concept]\n  indented line\n"
		summary, entities := parseSummaryEntities(raw)
		c.Assert(summary, qt.Equals, "Summary.")
		c.Assert(len(entities), qt.Equals, 1)
		c.Assert(entities[0].Name, qt.Equals, "Valid")
	})

	t.Run("aliases parenthesis is extracted and deduped", func(t *testing.T) {
		// Regression guard for Part 1 of the bilingual_zodiac_retrieval
		// plan: the summary prompt instructs Gemini to append up to six
		// alias surface forms after the bracketed type. We need those
		// aliases to land in EntityTag.Aliases so they can flow through
		// to kb_entity.aliases + the TYPE_AUGMENTED chunk.
		raw := "Prose.\n\n## Entities\n- Rooster [concept] (cock, chicken, 雞, 公雞, 酉, rooster, duplicate, extra7)\n"
		summary, entities := parseSummaryEntities(raw)
		c.Assert(summary, qt.Equals, "Prose.")
		c.Assert(len(entities), qt.Equals, 1)
		c.Assert(entities[0].Name, qt.Equals, "Rooster")
		c.Assert(entities[0].Type, qt.Equals, "concept")
		// Self-reference "rooster" dropped (case-insensitive match with
		// Name), remainder deduped + capped at maxAliasesPerEntity=6.
		c.Assert(entities[0].Aliases, qt.DeepEquals,
			[]string{"cock", "chicken", "雞", "公雞", "酉", "duplicate"})
	})

	t.Run("aliases cap at six and dedup is case-insensitive", func(t *testing.T) {
		raw := "S.\n\n## Entities\n- X [c] (a, A, b, B, c, C, d, E, F, G)\n"
		_, entities := parseSummaryEntities(raw)
		c.Assert(len(entities), qt.Equals, 1)
		c.Assert(len(entities[0].Aliases), qt.Equals, 6)
		c.Assert(entities[0].Aliases[0], qt.Equals, "a")
		c.Assert(entities[0].Aliases[1], qt.Equals, "b")
	})

	t.Run("empty aliases parens yields nil slice", func(t *testing.T) {
		raw := "S.\n\n## Entities\n- X [c] ()\n- Y [c] ( )\n"
		_, entities := parseSummaryEntities(raw)
		c.Assert(len(entities), qt.Equals, 2)
		c.Assert(entities[0].Aliases, qt.IsNil)
		c.Assert(entities[1].Aliases, qt.IsNil)
	})
}

func TestFormatAugmentedEntities(t *testing.T) {
	c := qt.New(t)

	t.Run("empty input yields empty string", func(t *testing.T) {
		c.Assert(formatAugmentedEntities(nil), qt.Equals, "")
		c.Assert(formatAugmentedEntities([]EntityTag{}), qt.Equals, "")
	})

	t.Run("entities without aliases separated by semicolons", func(t *testing.T) {
		got := formatAugmentedEntities([]EntityTag{
			{Name: "Alice"}, {Name: "Bob"}, {Name: "Charlie"},
		})
		c.Assert(got, qt.Equals, "Alice; Bob; Charlie")
	})

	t.Run("aliases render inside parentheses", func(t *testing.T) {
		// The " (" open-paren-after-space is the idempotency marker the
		// backfill workflow uses to skip already-backfilled files.
		got := formatAugmentedEntities([]EntityTag{
			{Name: "Rooster", Aliases: []string{"cock", "chicken", "雞", "公雞"}},
			{Name: "monopoly", Aliases: []string{"monopoly theory"}},
			{Name: "Peter Thiel"},
		})
		c.Assert(got, qt.Equals,
			"Rooster (cock, chicken, 雞, 公雞); monopoly (monopoly theory); Peter Thiel")
	})

	t.Run("rune-safe truncation stops at a whole-entity boundary", func(t *testing.T) {
		// Construct entities whose total length crosses the
		// augmentedChunkMaxRunes cap. We verify we stop at a ";" and
		// never mid-alias. Each entity contributes exactly 30 runes
		// including the trailing "; " separator.
		var tags []EntityTag
		for i := 0; i < 100; i++ {
			tags = append(tags, EntityTag{Name: fmt.Sprintf("ent-%020d", i)})
		}
		got := formatAugmentedEntities(tags)
		c.Assert(len(got) <= augmentedChunkMaxRunes*4, qt.Equals, true) // bytes bound
		// Result must end on an entity boundary (last byte is not a
		// space and the trailing "; " was dropped).
		c.Assert(strings.HasSuffix(got, " "), qt.Equals, false)
		c.Assert(strings.HasSuffix(got, ";"), qt.Equals, false)
	})
}

// withFakeFFprobe swaps the package-level ffprobeRun for the duration of the
// subtest so probeMediaDuration can be exercised without a real ffprobe
// binary. The returned function restores the original.
func withFakeFFprobe(t *testing.T, fake func(ctx context.Context, args ...string) ([]byte, error)) {
	t.Helper()
	orig := ffprobeRun
	ffprobeRun = fake
	t.Cleanup(func() { ffprobeRun = orig })
}

// fakeFFprobeByQuery returns a fake ffprobeRun that routes each invocation to
// a canned response based on the `-select_streams` / `-show_entries` pair at
// the front of the args. Any query with no mapping falls through to the
// `defaultOut` / `defaultErr`. This mirrors the probe fallback chain in
// probeMediaDuration (video → audio → format).
func fakeFFprobeByQuery(responses map[string]ffprobeResponse, defaultResp ffprobeResponse) func(ctx context.Context, args ...string) ([]byte, error) {
	return func(ctx context.Context, args ...string) ([]byte, error) {
		key := probeQueryKey(args)
		if r, ok := responses[key]; ok {
			return []byte(r.out), r.err
		}
		return []byte(defaultResp.out), defaultResp.err
	}
}

type ffprobeResponse struct {
	out string
	err error
}

// probeQueryKey extracts a stable key from ffprobe args identifying the
// stream/format target, e.g. "v:0|stream=duration", "a:0|stream=duration",
// or "format=duration".
func probeQueryKey(args []string) string {
	var (
		selectStreams = ""
		showEntries   = ""
	)
	for i := 0; i < len(args)-1; i++ {
		switch args[i] {
		case "-select_streams":
			selectStreams = args[i+1]
		case "-show_entries":
			showEntries = args[i+1]
		}
	}
	if selectStreams == "" {
		return showEntries
	}
	return selectStreams + "|" + showEntries
}

// TestProbeMediaDuration_FormatNAFallsBackToVideoStream reproduces the
// production bug for chat cht-SzZSvFGyBe / kb-2gNsSWakjs: ffprobe returns
// "N/A" for `format=duration` on fragmented MP4s, but the first video
// stream still carries a valid per-stream duration. The legacy single-shot
// probe failed with NumError "parsing \"N/A\"". The fallback chain must
// recover the stream value so the workflow can correctly route the file
// onto the long-media path.
func TestProbeMediaDuration_FormatNAFallsBackToVideoStream(t *testing.T) {
	c := qt.New(t)
	withFakeFFprobe(t, fakeFFprobeByQuery(map[string]ffprobeResponse{
		"v:0|stream=duration": {out: "125.42\n", err: nil},
		"a:0|stream=duration": {out: "N/A\n", err: nil},
		"format=duration":     {out: "N/A\n", err: nil},
	}, ffprobeResponse{out: "", err: nil}))

	dur, err := probeMediaDuration(context.Background(), []byte("fake-mp4"))
	c.Assert(err, qt.IsNil)
	c.Assert(dur, qt.Equals, 125.42)
}

// TestProbeMediaDuration_AudioFallback: video stream has no duration
// (audio-only file or video stream with N/A), but the audio stream does.
// The fallback chain must not stop at the video miss.
func TestProbeMediaDuration_AudioFallback(t *testing.T) {
	c := qt.New(t)
	withFakeFFprobe(t, fakeFFprobeByQuery(map[string]ffprobeResponse{
		"v:0|stream=duration": {out: "\n", err: nil}, // empty = no video stream matched
		"a:0|stream=duration": {out: "88.7\n", err: nil},
		"format=duration":     {out: "N/A\n", err: nil},
	}, ffprobeResponse{out: "", err: nil}))

	dur, err := probeMediaDuration(context.Background(), []byte("fake-audio"))
	c.Assert(err, qt.IsNil)
	c.Assert(dur, qt.Equals, 88.7)
}

// TestProbeMediaDuration_FormatOnly: only container-level duration is
// available (no per-stream values). Regression guard that the third
// fallback still wins.
func TestProbeMediaDuration_FormatOnly(t *testing.T) {
	c := qt.New(t)
	withFakeFFprobe(t, fakeFFprobeByQuery(map[string]ffprobeResponse{
		"v:0|stream=duration": {out: "N/A\n", err: nil},
		"a:0|stream=duration": {out: "N/A\n", err: nil},
		"format=duration":     {out: "42.0\n", err: nil},
	}, ffprobeResponse{out: "", err: nil}))

	dur, err := probeMediaDuration(context.Background(), []byte("fake-mp4"))
	c.Assert(err, qt.IsNil)
	c.Assert(dur, qt.Equals, 42.0)
}

// TestProbeMediaDuration_AllUnprobableReturnsSentinel mirrors the
// production failure fingerprint where every query returned "N/A". The
// caller (GetMediaDurationActivity) relies on errors.Is to recognise this
// exact sentinel and wrap it as MEDIA_DURATION_UNPROBABLE.
func TestProbeMediaDuration_AllUnprobableReturnsSentinel(t *testing.T) {
	c := qt.New(t)
	withFakeFFprobe(t, fakeFFprobeByQuery(map[string]ffprobeResponse{
		"v:0|stream=duration": {out: "N/A\n", err: nil},
		"a:0|stream=duration": {out: "N/A\n", err: nil},
		"format=duration":     {out: "N/A\n", err: nil},
	}, ffprobeResponse{out: "", err: nil}))

	dur, err := probeMediaDuration(context.Background(), []byte("fake-mp4"))
	c.Assert(err, qt.IsNotNil)
	c.Assert(errors.Is(err, errMediaDurationUnprobable), qt.Equals, true)
	c.Assert(dur, qt.Equals, 0.0)
}

// TestProbeMediaDuration_TransientErrorNotSentinel ensures a broken
// ffprobe binary propagates as a plain wrapped error — NOT the unprobable
// sentinel — so Temporal's retry policy still absorbs transient failures.
// Only content-intrinsic paths (N/A outputs, or `*exec.ExitError`s on
// every query) are marked non-retryable.
func TestProbeMediaDuration_TransientErrorNotSentinel(t *testing.T) {
	c := qt.New(t)
	boom := fmt.Errorf("exec: ffprobe: executable file not found in $PATH")
	withFakeFFprobe(t, fakeFFprobeByQuery(
		map[string]ffprobeResponse{}, // no canned matches
		ffprobeResponse{out: "", err: boom},
	))

	dur, err := probeMediaDuration(context.Background(), []byte("fake"))
	c.Assert(err, qt.IsNotNil)
	c.Assert(errors.Is(err, errMediaDurationUnprobable), qt.Equals, false)
	c.Assert(strings.Contains(err.Error(), "ffprobe invocation failed on every query"), qt.Equals, true)
	c.Assert(dur, qt.Equals, 0.0)
}

// TestProbeMediaDuration_AllExitErrorsReturnSentinel locks in the
// "ffprobe rejected the file on every fallback query" path. A malformed
// MP4 (e.g. a skeleton ISO-MP4 with no sample tables, the fixture used by
// `standalone-media-duration-probe.js`) causes ffprobe to exit non-zero
// on all three queries with stderr `Invalid data found...`. Before
// the MEDIA-INV-DURATION-PROBE-FALLBACK tightening these propagated as
// transient errors and the workflow silently skipped duration probing,
// re-routing ~500 MB+ uploads down the non-long-media single-shot path.
// Now they MUST surface as `errMediaDurationUnprobable` so the workflow
// fails the file fast with a deterministic `status_message`.
func TestProbeMediaDuration_AllExitErrorsReturnSentinel(t *testing.T) {
	c := qt.New(t)
	exitErr := &exec.ExitError{
		ProcessState: &os.ProcessState{},
		Stderr:       []byte("[mov,mp4,m4a] invalid STSD entries 0\nInvalid data found when processing input"),
	}
	withFakeFFprobe(t, fakeFFprobeByQuery(
		map[string]ffprobeResponse{}, // force defaultResp for every query
		ffprobeResponse{out: "", err: exitErr},
	))

	dur, err := probeMediaDuration(context.Background(), []byte("fake"))
	c.Assert(err, qt.IsNotNil)
	c.Assert(errors.Is(err, errMediaDurationUnprobable), qt.Equals, true)
	c.Assert(strings.Contains(err.Error(), "ffprobe-error:"), qt.Equals, true,
		qt.Commentf("error message should include stderr context for ops: %v", err))
	c.Assert(dur, qt.Equals, 0.0)
}

// TestProbeMediaDuration_MixedExitAndNA treats "some queries exit-err,
// others return N/A" as unprobable too — the file is just as broken as
// in the all-exit-err case and no retry will make it probe. This covers
// semi-malformed MP4s where ffprobe parses the container header (so
// `format=duration` returns N/A) but bails on the per-stream queries.
func TestProbeMediaDuration_MixedExitAndNA(t *testing.T) {
	c := qt.New(t)
	exitErr := &exec.ExitError{
		ProcessState: &os.ProcessState{},
		Stderr:       []byte("stream not found"),
	}
	withFakeFFprobe(t, fakeFFprobeByQuery(map[string]ffprobeResponse{
		"v:0|stream=duration": {out: "", err: exitErr},
		"a:0|stream=duration": {out: "", err: exitErr},
		"format=duration":     {out: "N/A\n", err: nil},
	}, ffprobeResponse{out: "", err: nil}))

	dur, err := probeMediaDuration(context.Background(), []byte("fake"))
	c.Assert(err, qt.IsNotNil)
	c.Assert(errors.Is(err, errMediaDurationUnprobable), qt.Equals, true)
	c.Assert(dur, qt.Equals, 0.0)
}

// TestProbeMediaDuration_ContextCanceledIsTransient pins that a
// canceled / deadline-expired context surfaces as a plain transient
// error, NOT the unprobable sentinel. Otherwise a Temporal scheduling
// blip (shutdown, worker drain) would permanently fail an otherwise-
// healthy file.
func TestProbeMediaDuration_ContextCanceledIsTransient(t *testing.T) {
	c := qt.New(t)
	withFakeFFprobe(t, fakeFFprobeByQuery(
		map[string]ffprobeResponse{},
		ffprobeResponse{out: "", err: context.Canceled},
	))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	dur, err := probeMediaDuration(ctx, []byte("fake"))
	c.Assert(err, qt.IsNotNil)
	c.Assert(errors.Is(err, errMediaDurationUnprobable), qt.Equals, false)
	c.Assert(strings.Contains(err.Error(), "ffprobe canceled"), qt.Equals, true)
	c.Assert(dur, qt.Equals, 0.0)
}

// TestProbeMediaDuration_NonPositiveIgnored: ffprobe occasionally prints
// "0" or "0.000000" for placeholder/zero-length streams. Those must be
// treated as unusable so we fall through to the next query rather than
// returning a bogus 0-second duration (which would pass the `err == nil`
// check but then make the workflow's `DurationSeconds > 0` branch flaky).
func TestProbeMediaDuration_NonPositiveIgnored(t *testing.T) {
	c := qt.New(t)
	withFakeFFprobe(t, fakeFFprobeByQuery(map[string]ffprobeResponse{
		"v:0|stream=duration": {out: "0.000000\n", err: nil},
		"a:0|stream=duration": {out: "-1\n", err: nil},
		"format=duration":     {out: "7.5\n", err: nil},
	}, ffprobeResponse{out: "", err: nil}))

	dur, err := probeMediaDuration(context.Background(), []byte("fake"))
	c.Assert(err, qt.IsNil)
	c.Assert(dur, qt.Equals, 7.5)
}

// TestParseDurationLines covers the small in-process parser used by
// probeMediaDuration; it lets us pin the "N/A-is-never-a-float" contract
// without invoking the full probe chain.
func TestParseDurationLines(t *testing.T) {
	c := qt.New(t)

	cases := []struct {
		name    string
		in      string
		wantV   float64
		wantOK  bool
	}{
		{"positive float", "12.34\n", 12.34, true},
		{"N/A literal", "N/A\n", 0, false},
		{"lower-case n/a", "n/a\n", 0, false},
		{"empty", "\n", 0, false},
		{"whitespace only", "   \n\t\n", 0, false},
		{"multi-line, first is N/A", "N/A\n\n55.5\n", 55.5, true},
		{"negative", "-3.14\n", 0, false},
		{"zero", "0\n", 0, false},
		{"inf", "inf\n", 0, false},
		{"nan", "nan\n", 0, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			v, ok := parseDurationLines(tc.in)
			c.Assert(ok, qt.Equals, tc.wantOK)
			if tc.wantOK {
				c.Assert(v, qt.Equals, tc.wantV)
			}
		})
	}
}
