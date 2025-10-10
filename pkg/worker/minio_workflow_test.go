package worker

import (
	"testing"

	"github.com/gojuno/minimock/v3"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/zap"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/worker/mock"
)

func TestDeleteFilesWorkflowParam_Validation(t *testing.T) {
	c := qt.New(t)

	param := DeleteFilesWorkflowParam{
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

	param := GetFilesWorkflowParam{
		Bucket:    "test-bucket",
		FilePaths: filePaths,
	}

	c.Assert(param.Bucket, qt.Not(qt.Equals), "")
	c.Assert(param.FilePaths, qt.HasLen, 2)
}

func TestDeleteFilesWorkflow_EmptyPaths(t *testing.T) {
	c := qt.New(t)

	param := DeleteFilesWorkflowParam{
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

	mockRepo := mock.NewRepositoryMock(mc)
	mockRepo.DeleteFileMock.Return(nil)

	w := &Worker{repository: mockRepo, log: zap.NewNop()}
	env.RegisterActivity(w.DeleteFileActivity)
	env.RegisterWorkflow(w.DeleteFilesWorkflow)

	param := DeleteFilesWorkflowParam{
		Bucket:    "test-bucket",
		FilePaths: []string{"file1.txt", "file2.txt"},
	}

	env.ExecuteWorkflow(w.DeleteFilesWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.IsNil)
}

func TestGetFilesWorkflow_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockRepo := mock.NewRepositoryMock(mc)
	mockRepo.GetFileMock.Return([]byte("test content"), nil)

	w := &Worker{repository: mockRepo, log: zap.NewNop()}
	env.RegisterActivity(w.GetFileActivity)
	env.RegisterWorkflow(w.GetFilesWorkflow)

	param := GetFilesWorkflowParam{
		Bucket:    "test-bucket",
		FilePaths: []string{"file1.txt", "file2.txt"},
	}

	env.ExecuteWorkflow(w.GetFilesWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.IsNil)

	var results []FileContent
	err := env.GetWorkflowResult(&results)
	c.Assert(err, qt.IsNil)
	c.Assert(results, qt.HasLen, 2)
}
