package worker

import (
	"testing"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSaveChunkActivityParam_Validation(t *testing.T) {
	kbUID := uuid.Must(uuid.NewV4())
	fileUID := uuid.Must(uuid.NewV4())
	chunkUID := "test-chunk-uid"

	param := &SaveChunkActivityParam{
		KnowledgeBaseUID: kbUID,
		FileUID:          fileUID,
		ChunkUID:         chunkUID,
		ChunkContent:     []byte("test content"),
	}

	require.NotNil(t, param)
	assert.NotEqual(t, uuid.Nil, param.KnowledgeBaseUID)
	assert.NotEqual(t, uuid.Nil, param.FileUID)
	assert.NotEmpty(t, param.ChunkUID)
	assert.NotEmpty(t, param.ChunkContent)
}

func TestDeleteFileActivityParam_Validation(t *testing.T) {
	param := &DeleteFileActivityParam{
		Bucket: "test-bucket",
		Path:   "path/to/file",
	}

	require.NotNil(t, param)
	assert.NotEmpty(t, param.Bucket)
	assert.NotEmpty(t, param.Path)
}

func TestGetFileActivityParam_Validation(t *testing.T) {
	tests := []struct {
		name  string
		param *GetFileActivityParam
		valid bool
	}{
		{
			name: "Valid param",
			param: &GetFileActivityParam{
				Bucket: "test-bucket",
				Path:   "path/to/file",
				Index:  0,
			},
			valid: true,
		},
		{
			name: "With index",
			param: &GetFileActivityParam{
				Bucket: "test-bucket",
				Path:   "path/to/file",
				Index:  5,
			},
			valid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NotNil(t, tt.param)
			assert.NotEmpty(t, tt.param.Bucket)
			assert.NotEmpty(t, tt.param.Path)
			assert.GreaterOrEqual(t, tt.param.Index, 0)
		})
	}
}

func TestSaveChunkActivityResult_Validation(t *testing.T) {
	result := &SaveChunkActivityResult{
		ChunkUID:    "test-chunk-uid",
		Destination: "path/to/chunk",
	}

	require.NotNil(t, result)
	assert.NotEmpty(t, result.ChunkUID)
	assert.NotEmpty(t, result.Destination)
}

func TestGetFileActivityResult_Validation(t *testing.T) {
	result := &GetFileActivityResult{
		Index:   0,
		Name:    "test-file.txt",
		Content: []byte("test content"),
	}

	require.NotNil(t, result)
	assert.GreaterOrEqual(t, result.Index, 0)
	assert.NotEmpty(t, result.Name)
	assert.NotEmpty(t, result.Content)
}
