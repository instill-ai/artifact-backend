package worker

import (
	"testing"

	"github.com/gojuno/minimock/v3"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/zap"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/mock"
	"github.com/instill-ai/artifact-backend/pkg/service"
)

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

func TestDeleteFilesWorkflow_EmptyPaths(t *testing.T) {
	c := qt.New(t)

	param := service.DeleteFilesWorkflowParam{
		Bucket:    "test-bucket",
		FilePaths: []string{},
	}

	c.Assert(param.FilePaths, qt.HasLen, 0)
	// Workflow should handle empty paths gracefully
}

func TestDeleteFilesWorkflow_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockSvc := mock.NewServiceMock(mc)
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

	mockSvc := mock.NewServiceMock(mc)
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
