package worker

import (
	"testing"

	"github.com/gofrs/uuid"

	qt "github.com/frankban/quicktest"
)

func TestSaveChunkActivityParam_Validation(t *testing.T) {
	c := qt.New(t)
	kbUID := uuid.Must(uuid.NewV4())
	fileUID := uuid.Must(uuid.NewV4())
	chunkUID := "test-chunk-uid"

	param := &SaveChunkActivityParam{
		KnowledgeBaseUID: kbUID,
		FileUID:          fileUID,
		ChunkUID:         chunkUID,
		ChunkContent:     []byte("test content"),
	}

	c.Assert(param.KnowledgeBaseUID, qt.Not(qt.Equals), uuid.Nil)
	c.Assert(param.FileUID, qt.Not(qt.Equals), uuid.Nil)
	c.Assert(param.ChunkUID, qt.Not(qt.Equals), "")
	c.Assert(param.ChunkContent, qt.Not(qt.HasLen), 0)
}

func TestDeleteFileActivityParam_Validation(t *testing.T) {
	c := qt.New(t)

	param := &DeleteFileActivityParam{
		Bucket: "test-bucket",
		Path:   "path/to/file",
	}

	c.Assert(param.Bucket, qt.Not(qt.Equals), "")
	c.Assert(param.Path, qt.Not(qt.Equals), "")
}

func TestGetFileActivityParam_Validation(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		param *GetFileActivityParam
	}{
		{
			name: "Valid param",
			param: &GetFileActivityParam{
				Bucket: "test-bucket",
				Path:   "path/to/file",
				Index:  0,
			},
		},
		{
			name: "With index",
			param: &GetFileActivityParam{
				Bucket: "test-bucket",
				Path:   "path/to/file",
				Index:  5,
			},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(tt.param.Bucket, qt.Not(qt.Equals), "")
			c.Assert(tt.param.Path, qt.Not(qt.Equals), "")
			c.Assert(tt.param.Index >= 0, qt.IsTrue)
		})
	}
}

func TestSaveChunkActivityResult_Validation(t *testing.T) {
	c := qt.New(t)

	result := &SaveChunkActivityResult{
		ChunkUID:    "test-chunk-uid",
		Destination: "path/to/chunk",
	}

	c.Assert(result.ChunkUID, qt.Not(qt.Equals), "")
	c.Assert(result.Destination, qt.Not(qt.Equals), "")
}

func TestGetFileActivityResult_Validation(t *testing.T) {
	c := qt.New(t)

	result := &GetFileActivityResult{
		Index:   0,
		Name:    "test-file.txt",
		Content: []byte("test content"),
	}

	c.Assert(result.Index >= 0, qt.IsTrue)
	c.Assert(result.Name, qt.Not(qt.Equals), "")
	c.Assert(result.Content, qt.Not(qt.HasLen), 0)
}
