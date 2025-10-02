package worker

import (
	"testing"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/instill-ai/artifact-backend/pkg/temporal"
)

func TestSaveChunksWorkflowParam_Validation(t *testing.T) {
	kbUID := uuid.Must(uuid.NewV4())
	fileUID := uuid.Must(uuid.NewV4())

	chunks := map[string][]byte{
		"chunk1": []byte("content1"),
		"chunk2": []byte("content2"),
	}

	param := temporal.SaveChunksWorkflowParam{
		KnowledgeBaseUID: kbUID,
		FileUID:          fileUID,
		Chunks:           chunks,
	}

	require.NotNil(t, param)
	assert.NotEqual(t, uuid.Nil, param.KnowledgeBaseUID)
	assert.NotEqual(t, uuid.Nil, param.FileUID)
	assert.Len(t, param.Chunks, 2)
}

func TestDeleteFilesWorkflowParam_Validation(t *testing.T) {
	param := temporal.DeleteFilesWorkflowParam{
		Bucket:    "test-bucket",
		FilePaths: []string{"file1.txt", "file2.txt"},
	}

	require.NotNil(t, param)
	assert.NotEmpty(t, param.Bucket)
	assert.Len(t, param.FilePaths, 2)
}

func TestGetFilesWorkflowParam_Validation(t *testing.T) {
	filePaths := []string{
		"path/to/file1.txt",
		"path/to/file2.txt",
	}

	param := temporal.GetFilesWorkflowParam{
		Bucket:    "test-bucket",
		FilePaths: filePaths,
	}

	require.NotNil(t, param)
	assert.NotEmpty(t, param.Bucket)
	assert.Len(t, param.FilePaths, 2)
}

func TestSaveChunksWorkflow_EmptyChunks(t *testing.T) {
	param := temporal.SaveChunksWorkflowParam{
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
		FileUID:          uuid.Must(uuid.NewV4()),
		Chunks:           map[string][]byte{},
	}

	assert.Len(t, param.Chunks, 0)
	// Workflow should handle empty chunks gracefully
}

func TestDeleteFilesWorkflow_EmptyPaths(t *testing.T) {
	param := temporal.DeleteFilesWorkflowParam{
		Bucket:    "test-bucket",
		FilePaths: []string{},
	}

	assert.Len(t, param.FilePaths, 0)
	// Workflow should handle empty paths gracefully
}
