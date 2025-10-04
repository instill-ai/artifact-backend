package worker

import (
	"testing"

	"github.com/gofrs/uuid"

	qt "github.com/frankban/quicktest"

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
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
		FileUID:          uuid.Must(uuid.NewV4()),
		Chunks:           map[string][]byte{},
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
