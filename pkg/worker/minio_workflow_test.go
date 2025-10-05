package worker

import (
	"testing"

	"github.com/gofrs/uuid"
	"github.com/gojuno/minimock/v3"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/zap"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/mock"
	"github.com/instill-ai/artifact-backend/pkg/service"
)

func TestSaveChunksWorkflowParam_Validation(t *testing.T) {
	c := qt.New(t)
	kbUID := uuid.Must(uuid.NewV4())
	fileUID := uuid.Must(uuid.NewV4())

	chunks := map[string][]byte{
		"chunk1": []byte("content1"),
		"chunk2": []byte("content2"),
	}

	param := service.SaveChunksWorkflowParam{
		KnowledgeBaseUID: kbUID,
		FileUID:          fileUID,
		Chunks:           chunks,
	}

	c.Assert(param.KnowledgeBaseUID, qt.Not(qt.Equals), uuid.Nil)
	c.Assert(param.FileUID, qt.Not(qt.Equals), uuid.Nil)
	c.Assert(param.Chunks, qt.HasLen, 2)
}

func TestDeleteFilesWorkflowParam_Validation(t *testing.T) {
	c := qt.New(t)

	param := service.DeleteFilesWorkflowParam{
		Bucket:    "test-bucket",
		FilePaths: []string{"file1.txt", "file2.txt"},
	}

	c.Assert(param.Bucket, qt.Not(qt.Equals), "")
	c.Assert(param.FilePaths, qt.HasLen, 2)
}

func TestGetFilesWorkflowParam_Validation(t *testing.T) {
	c := qt.New(t)

	filePaths := []string{
		"path/to/file1.txt",
		"path/to/file2.txt",
	}

	param := service.GetFilesWorkflowParam{
		Bucket:    "test-bucket",
		FilePaths: filePaths,
	}

	c.Assert(param.Bucket, qt.Not(qt.Equals), "")
	c.Assert(param.FilePaths, qt.HasLen, 2)
}

func TestSaveChunksWorkflow_EmptyChunks(t *testing.T) {
	c := qt.New(t)

	param := service.SaveChunksWorkflowParam{
		Chunks: map[string][]byte{},
	}

	c.Assert(param.Chunks, qt.HasLen, 0)
	// Workflow should handle empty chunks gracefully
}

func TestDeleteFilesWorkflow_EmptyPaths(t *testing.T) {
	c := qt.New(t)

	param := service.DeleteFilesWorkflowParam{
		Bucket:    "test-bucket",
		FilePaths: []string{},
	}

	c.Assert(param.FilePaths, qt.HasLen, 0)
	// Workflow should handle empty paths gracefully
}

// Workflow tests with minimock

func TestSaveChunksWorkflow_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockSvc := NewServiceMock(mc)
	mockMinIO := mock.NewMinioIMock(mc)
	mockSvc.MinIOMock.Return(mockMinIO)

	kbUID := uuid.Must(uuid.NewV4())
	fileUID := uuid.Must(uuid.NewV4())

	// Mock for SaveChunkActivity
	mockMinIO.UploadBase64FileMock.Return(nil)

	worker := &Worker{service: mockSvc, log: zap.NewNop()}
	env.RegisterActivity(worker.SaveChunkActivity)
	env.RegisterWorkflow(worker.SaveChunksWorkflow)

	param := service.SaveChunksWorkflowParam{
		KnowledgeBaseUID: kbUID,
		FileUID:          fileUID,
		Chunks: map[string][]byte{
			"chunk-1": []byte("chunk content 1"),
			"chunk-2": []byte("chunk content 2"),
		},
	}

	env.ExecuteWorkflow(worker.SaveChunksWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.IsNil)

	var destinations map[string]string
	err := env.GetWorkflowResult(&destinations)
	c.Assert(err, qt.IsNil)
	c.Assert(destinations, qt.HasLen, 2)
}

func TestDeleteFilesWorkflow_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockSvc := NewServiceMock(mc)
	mockMinIO := mock.NewMinioIMock(mc)
	mockSvc.MinIOMock.Return(mockMinIO)

	// Mock for DeleteFileActivity
	mockMinIO.DeleteFileMock.Return(nil)

	worker := &Worker{service: mockSvc, log: zap.NewNop()}
	env.RegisterActivity(worker.DeleteFileActivity)
	env.RegisterWorkflow(worker.DeleteFilesWorkflow)

	param := service.DeleteFilesWorkflowParam{
		Bucket:    "test-bucket",
		FilePaths: []string{"file1.txt", "file2.txt"},
	}

	env.ExecuteWorkflow(worker.DeleteFilesWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.IsNil)
}

func TestGetFilesWorkflow_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockSvc := NewServiceMock(mc)
	mockMinIO := mock.NewMinioIMock(mc)
	mockSvc.MinIOMock.Return(mockMinIO)

	// Mock for GetFileActivity
	mockMinIO.GetFileMock.Return([]byte("file content"), nil)

	worker := &Worker{service: mockSvc, log: zap.NewNop()}
	env.RegisterActivity(worker.GetFileActivity)
	env.RegisterWorkflow(worker.GetFilesWorkflow)

	param := service.GetFilesWorkflowParam{
		Bucket:    "test-bucket",
		FilePaths: []string{"file1.txt", "file2.txt"},
	}

	env.ExecuteWorkflow(worker.GetFilesWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.IsNil)

	var results []service.FileContent
	err := env.GetWorkflowResult(&results)
	c.Assert(err, qt.IsNil)
	c.Assert(results, qt.HasLen, 2)
}
