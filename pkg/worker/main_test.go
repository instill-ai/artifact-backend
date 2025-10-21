package worker

import (
	"context"
	"fmt"
	"testing"

	"github.com/gojuno/minimock/v3"
	"go.uber.org/zap"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/worker/mock"
)

// ===== MinIO Batch Activity Tests =====

func TestDeleteFilesBatchActivityParam_Validation(t *testing.T) {
	c := qt.New(t)

	param := DeleteFilesBatchActivityParam{
		Bucket:    "test-bucket",
		FilePaths: []string{"file1.txt", "file2.txt"},
	}

	c.Assert(param.Bucket, qt.Not(qt.Equals), "")
	c.Assert(param.FilePaths, qt.HasLen, 2)
}

func TestGetFilesBatchActivityParam_Validation(t *testing.T) {
	c := qt.New(t)

	filePaths := []string{
		"path/to/file1.txt",
		"path/to/file2.txt",
	}

	param := GetFilesBatchActivityParam{
		Bucket:    "test-bucket",
		FilePaths: filePaths,
	}

	c.Assert(param.Bucket, qt.Not(qt.Equals), "")
	c.Assert(param.FilePaths, qt.HasLen, 2)
}

func TestDeleteFilesBatchActivity_EmptyPaths(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepo := mock.NewRepositoryMock(mc)
	w := &Worker{repository: mockRepo, log: zap.NewNop()}

	param := &DeleteFilesBatchActivityParam{
		Bucket:    "test-bucket",
		FilePaths: []string{},
	}

	err := w.DeleteFilesBatchActivity(context.Background(), param)
	c.Assert(err, qt.IsNil)
}

func TestDeleteFilesBatchActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepo := mock.NewRepositoryMock(mc)
	mockRepo.DeleteFileMock.Return(nil)

	w := &Worker{repository: mockRepo, log: zap.NewNop()}

	param := &DeleteFilesBatchActivityParam{
		Bucket:    "test-bucket",
		FilePaths: []string{"file1.txt", "file2.txt"},
	}

	err := w.DeleteFilesBatchActivity(context.Background(), param)
	c.Assert(err, qt.IsNil)
	// Verify DeleteFile was called 2 times
	c.Assert(mockRepo.DeleteFileMock.Calls(), qt.HasLen, 2)
}

func TestDeleteFilesBatchActivity_PartialFailure(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepo := mock.NewRepositoryMock(mc)
	// Set up mock to fail for any call
	mockRepo.DeleteFileMock.Return(fmt.Errorf("delete failed"))

	w := &Worker{repository: mockRepo, log: zap.NewNop()}

	param := &DeleteFilesBatchActivityParam{
		Bucket:    "test-bucket",
		FilePaths: []string{"file1.txt", "file2.txt"},
	}

	err := w.DeleteFilesBatchActivity(context.Background(), param)
	c.Assert(err, qt.Not(qt.IsNil))
	c.Assert(err.Error(), qt.Contains, "Failed to delete")
}

func TestGetFilesBatchActivity_EmptyPaths(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepo := mock.NewRepositoryMock(mc)
	w := &Worker{repository: mockRepo, log: zap.NewNop()}

	param := &GetFilesBatchActivityParam{
		Bucket:    "test-bucket",
		FilePaths: []string{},
	}

	result, err := w.GetFilesBatchActivity(context.Background(), param)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Files, qt.HasLen, 0)
}

func TestGetFilesBatchActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepo := mock.NewRepositoryMock(mc)
	mockRepo.GetFileMock.Return([]byte("test content"), nil)

	w := &Worker{repository: mockRepo, log: zap.NewNop()}

	param := &GetFilesBatchActivityParam{
		Bucket:    "test-bucket",
		FilePaths: []string{"file1.txt", "file2.txt"},
	}

	result, err := w.GetFilesBatchActivity(context.Background(), param)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Files, qt.HasLen, 2)
	// Verify GetFile was called 2 times
	c.Assert(mockRepo.GetFileMock.Calls(), qt.HasLen, 2)
	// Verify result ordering
	c.Assert(result.Files[0].Index, qt.Equals, 0)
	c.Assert(result.Files[1].Index, qt.Equals, 1)
	c.Assert(result.Files[0].Name, qt.Equals, "file1.txt")
	c.Assert(result.Files[1].Name, qt.Equals, "file2.txt")
}

func TestGetFilesBatchActivity_Failure(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepo := mock.NewRepositoryMock(mc)
	mockRepo.GetFileMock.Return(nil, fmt.Errorf("get file failed"))

	w := &Worker{repository: mockRepo, log: zap.NewNop()}

	param := &GetFilesBatchActivityParam{
		Bucket:    "test-bucket",
		FilePaths: []string{"file1.txt"},
	}

	result, err := w.GetFilesBatchActivity(context.Background(), param)
	c.Assert(err, qt.Not(qt.IsNil))
	c.Assert(result, qt.IsNil)
	c.Assert(err.Error(), qt.Contains, "Failed to retrieve file")
}
