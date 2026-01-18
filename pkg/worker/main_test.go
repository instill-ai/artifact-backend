package worker

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/gojuno/minimock/v3"
	"go.uber.org/zap"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"
	"github.com/instill-ai/artifact-backend/pkg/worker/mock"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
)

func TestGetRepository(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	mockRepository := mock.NewRepositoryMock(mc)
	w := &Worker{
		repository: mockRepository,
		log:        zap.NewNop(),
	}

	repo := w.GetRepository()
	c.Assert(repo, qt.Not(qt.IsNil))
	c.Assert(repo, qt.Equals, mockRepository)
}

func TestGetLogger(t *testing.T) {
	c := qt.New(t)

	logger := zap.NewNop()
	w := &Worker{
		log: logger,
	}

	gotLogger := w.GetLogger()
	c.Assert(gotLogger, qt.Not(qt.IsNil))
	c.Assert(gotLogger, qt.Equals, logger)
}

func TestDeleteFilesBatchActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	bucket := "test-bucket"
	filePaths := []string{
		"file1.txt",
		"file2.txt",
		"file3.txt",
	}

	mockStorage := mock.NewStorageMock(mc)
	mockStorage.DeleteFileMock.Return(nil)

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetMinIOStorageMock.Return(mockStorage)

	w := &Worker{
		repository: mockRepository,
		log:        zap.NewNop(),
	}

	param := &DeleteFilesBatchActivityParam{
		Bucket:    bucket,
		FilePaths: filePaths,
	}

	err := w.DeleteFilesBatchActivity(ctx, param)
	c.Assert(err, qt.IsNil)
}

func TestDeleteFilesBatchActivity_EmptyList(t *testing.T) {
	c := qt.New(t)

	ctx := context.Background()
	w := &Worker{
		log: zap.NewNop(),
	}

	param := &DeleteFilesBatchActivityParam{
		Bucket:    "test-bucket",
		FilePaths: []string{},
	}

	err := w.DeleteFilesBatchActivity(ctx, param)
	c.Assert(err, qt.IsNil)
}

func TestDeleteFilesBatchActivity_PartialFailure(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	bucket := "test-bucket"
	filePaths := []string{
		"file1.txt",
		"file2.txt",
	}

	mockStorage := mock.NewStorageMock(mc)
	// First file succeeds, second file fails
	var callCount atomic.Int32
	mockStorage.DeleteFileMock.Set(func(ctx context.Context, bucket string, path string) error {
		count := callCount.Add(1)
		if count == 1 {
			return nil
		}
		return fmt.Errorf("delete failed")
	})

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetMinIOStorageMock.Return(mockStorage)

	w := &Worker{
		repository: mockRepository,
		log:        zap.NewNop(),
	}

	param := &DeleteFilesBatchActivityParam{
		Bucket:    bucket,
		FilePaths: filePaths,
	}

	err := w.DeleteFilesBatchActivity(ctx, param)
	c.Assert(err, qt.Not(qt.IsNil))
}

func TestGetFilesBatchActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	bucket := "test-bucket"
	filePaths := []string{
		"file1.txt",
		"file2.txt",
	}

	mockStorage := mock.NewStorageMock(mc)
	mockStorage.GetFileMock.Set(func(ctx context.Context, bucket string, path string) ([]byte, error) {
		return []byte("content of " + path), nil
	})

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetMinIOStorageMock.Return(mockStorage)

	w := &Worker{
		repository: mockRepository,
		log:        zap.NewNop(),
	}

	param := &GetFilesBatchActivityParam{
		Bucket:    bucket,
		FilePaths: filePaths,
		Metadata:  nil,
	}

	result, err := w.GetFilesBatchActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.Not(qt.IsNil))
	c.Assert(len(result.Files), qt.Equals, 2)
}

func TestGetFilesBatchActivity_EmptyList(t *testing.T) {
	c := qt.New(t)

	ctx := context.Background()
	w := &Worker{
		log: zap.NewNop(),
	}

	param := &GetFilesBatchActivityParam{
		Bucket:    "test-bucket",
		FilePaths: []string{},
	}

	result, err := w.GetFilesBatchActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.Not(qt.IsNil))
	c.Assert(len(result.Files), qt.Equals, 0)
}

func TestExecuteKnowledgeBaseUpdate_NoEligibleKBs(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	knowledgeBaseID := "test-knowledge-base"

	mockRepository := mock.NewRepositoryMock(mc)
	// Return KB that's already updating (not eligible)
	mockRepository.GetKnowledgeBaseByIDMock.
		When(minimock.AnyContext, knowledgeBaseID).
		Then(&repository.KnowledgeBaseModel{
			UID:          types.KBUIDType(uuid.Must(uuid.NewV4())),
			ID:         knowledgeBaseID,
			Staging:      false,
			UpdateStatus: artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_UPDATING.String(),
		}, nil)

	w := &Worker{
		repository: mockRepository,
		log:        zap.NewNop(),
	}

	result, err := w.ExecuteKnowledgeBaseUpdate(ctx, []string{knowledgeBaseID}, "")

	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.Not(qt.IsNil))
	c.Assert(result.Started, qt.IsFalse)
	c.Assert(result.Message, qt.Contains, "No eligible")
}

func TestExecuteKnowledgeBaseUpdate_ListAllEligible(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()

	mockRepository := mock.NewRepositoryMock(mc)
	// List returns no eligible KBs
	mockRepository.ListKnowledgeBasesForUpdateMock.
		When(minimock.AnyContext, nil, nil).
		Then([]repository.KnowledgeBaseModel{}, nil)

	w := &Worker{
		repository: mockRepository,
		log:        zap.NewNop(),
	}

	result, err := w.ExecuteKnowledgeBaseUpdate(ctx, []string{}, "")

	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.Not(qt.IsNil))
	c.Assert(result.Started, qt.IsFalse)
}

func TestAbortKnowledgeBaseUpdate_NoInProgressKBs(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	knowledgeBaseID := "test-knowledge-base"

	mockRepository := mock.NewRepositoryMock(mc)
	// Return KB that's not updating
	mockRepository.GetKnowledgeBaseByIDMock.
		When(minimock.AnyContext, knowledgeBaseID).
		Then(&repository.KnowledgeBaseModel{
			UID:          types.KBUIDType(uuid.Must(uuid.NewV4())),
			ID:         knowledgeBaseID,
			UpdateStatus: artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED.String(),
		}, nil)

	w := &Worker{
		repository: mockRepository,
		log:        zap.NewNop(),
	}

	result, err := w.AbortKnowledgeBaseUpdate(ctx, []string{knowledgeBaseID})

	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.Not(qt.IsNil))
	c.Assert(result.Success, qt.IsTrue)
	c.Assert(result.Message, qt.Contains, "No knowledge bases")
}

func TestAbortKnowledgeBaseUpdate_ListAll(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()

	mockRepository := mock.NewRepositoryMock(mc)
	// List returns no in-progress KBs
	mockRepository.ListKnowledgeBasesByUpdateStatusMock.Return([]repository.KnowledgeBaseModel{}, nil)

	w := &Worker{
		repository: mockRepository,
		log:        zap.NewNop(),
	}

	result, err := w.AbortKnowledgeBaseUpdate(ctx, []string{})

	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.Not(qt.IsNil))
	c.Assert(result.Success, qt.IsTrue)
}

func TestProcessFileDualMode_ProductionFailure(t *testing.T) {
	// This test requires a temporal client which is complex to mock
	// The underlying ProcessFile call is tested separately
	// So we skip this integration test for now
	t.Skip("ProcessFileDualMode requires temporal client setup - tested in integration tests")
}

func TestCleanupFile_Success(t *testing.T) {
	// This test requires a temporal client which is complex to mock
	// The cleanup activities are tested separately
	// So we skip this integration test for now
	t.Skip("CleanupFile requires temporal client setup - tested in integration tests")
}

func TestUpdateRAGIndexResult(t *testing.T) {
	c := qt.New(t)

	result := &UpdateRAGIndexResult{
		Started: true,
		Message: "Update started successfully",
	}

	c.Assert(result.Started, qt.IsTrue)
	c.Assert(result.Message, qt.Not(qt.Equals), "")
}

func TestAbortKBUpdateResult(t *testing.T) {
	c := qt.New(t)

	result := &AbortKBUpdateResult{
		Success:      true,
		Message:      "Aborted successfully",
		AbortedCount: 2,
		KnowledgeBaseStatus: []KnowledgeBaseAbortStatus{
			{
				KnowledgeBaseID:  "knowledge-base-1",
				KnowledgeBaseUID: uuid.Must(uuid.NewV4()).String(),
				WorkflowID:       "workflow-1",
				Status:           artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_ABORTED.String(),
			},
		},
	}

	c.Assert(result.Success, qt.IsTrue)
	c.Assert(result.AbortedCount, qt.Equals, 2)
	c.Assert(len(result.KnowledgeBaseStatus), qt.Equals, 1)
}

func TestFileContent(t *testing.T) {
	c := qt.New(t)

	content := FileContent{
		Index:   0,
		Name:    "test.txt",
		Content: []byte("test content"),
	}

	c.Assert(content.Index, qt.Equals, 0)
	c.Assert(content.Name, qt.Equals, "test.txt")
	c.Assert(string(content.Content), qt.Equals, "test content")
}

func TestGetFilesByPaths_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	bucket := "test-bucket"
	filePaths := []string{"file1.txt"}

	mockStorage := mock.NewStorageMock(mc)
	mockStorage.GetFileMock.
		When(minimock.AnyContext, bucket, "file1.txt").
		Then([]byte("content"), nil)

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetMinIOStorageMock.Return(mockStorage)

	w := &Worker{
		repository: mockRepository,
		log:        zap.NewNop(),
	}

	files, err := w.GetFilesByPaths(ctx, bucket, filePaths)

	c.Assert(err, qt.IsNil)
	c.Assert(len(files), qt.Equals, 1)
	c.Assert(files[0].Name, qt.Equals, "file1.txt")
}

func TestDeleteFiles_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	bucket := "test-bucket"
	filePaths := []string{"file1.txt"}

	mockStorage := mock.NewStorageMock(mc)
	mockStorage.DeleteFileMock.Return(nil)

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetMinIOStorageMock.Return(mockStorage)

	w := &Worker{
		repository: mockRepository,
		log:        zap.NewNop(),
	}

	err := w.DeleteFiles(ctx, bucket, filePaths)

	c.Assert(err, qt.IsNil)
}
