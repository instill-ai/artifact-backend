package minio

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"

	"github.com/instill-ai/artifact-backend/pkg/utils"
)

// KnowledgeBaseI is the interface for knowledge base related operations.
type KnowledgeBaseI interface {
	// SaveConvertedFile saves a converted file to MinIO with the appropriate MIME type.
	SaveConvertedFile(ctx context.Context, kbUID, convertedFileUID, fileExt string, content []byte) error
	// SaveTextChunks saves batch of chunks(text files) to MinIO.
	SaveTextChunks(ctx context.Context, kbUID string, chunks map[ChunkUIDType]ChunkContentType) error
	// GetUploadedFilePathInKnowledgeBase returns the path of the uploaded file in MinIO.
	GetUploadedFilePathInKnowledgeBase(kbUID, dest string) string
	// GetConvertedFilePathInKnowledgeBase returns the path of the converted file in MinIO.
	GetConvertedFilePathInKnowledgeBase(kbUID, ConvertedFileUID, fileExt string) string
	// GetChunkPathInKnowledgeBase returns the path of the chunk in MinIO.
	GetChunkPathInKnowledgeBase(kbUID, chunkUID string) string
	// DeleteKnowledgeBase deletes all files in the knowledge base.
	DeleteKnowledgeBase(ctx context.Context, kbUID string) chan error
	// DeleteAllConvertedFilesInKb deletes converted files in the knowledge base.
	DeleteAllConvertedFilesInKb(ctx context.Context, kbUID string) chan error
	// DeleteAllUploadedFilesInKb deletes uploaded files in the knowledge base.
	DeleteAllUploadedFilesInKb(ctx context.Context, kbUID string) chan error
	// DeleteAllChunksInKb deletes chunks in the knowledge base.
	DeleteAllChunksInKb(ctx context.Context, kbUID string) chan error
}

// prefix
const uploadedFilePrefix = "/uploaded-file/"
const convertedFilePrefix = "/converted-file/"
const chunkPrefix = "/chunk/"

// SaveConvertedFile saves a converted file to MinIO with the appropriate MIME type.
func (m *Minio) SaveConvertedFile(ctx context.Context, kbUID, convertedFileUID, fileExt string, content []byte) error {
	filePathName := m.GetConvertedFilePathInKnowledgeBase(kbUID, convertedFileUID, fileExt)
	mimeType := "application/octet-stream"
	if fileExt == "md" {
		mimeType = "text/markdown"
	}

	err := m.UploadBase64File(ctx, filePathName, base64.StdEncoding.EncodeToString(content), mimeType)
	if err != nil {
		return err
	}
	return nil
}

type ChunkUIDType string
type ChunkContentType []byte

// SaveTextChunks saves batch of chunks(text files) to MinIO.
func (m *Minio) SaveTextChunks(ctx context.Context, kbUID string, chunks map[ChunkUIDType]ChunkContentType) error {
	var wg sync.WaitGroup
	errorUIDChan := make(chan string, len(chunks))
	for chunkUID, chunkContent := range chunks {
		wg.Add(1)
		go utils.GoRecover(func() {
			func(chunkUID ChunkUIDType, chunkContent ChunkContentType) {
				defer wg.Done()
				filePathName := m.GetChunkPathInKnowledgeBase(kbUID, string(chunkUID))

				err := m.UploadBase64File(ctx, filePathName, base64.StdEncoding.EncodeToString(chunkContent), "text/plain")
				if err != nil {
					errorUIDChan <- string(chunkUID)
					return
				}
			}(chunkUID, chunkContent)
		}, fmt.Sprintf("SaveTextChunks %s", chunkUID))
	}
	wg.Wait()
	close(errorUIDChan)
	var errStr []string
	for err := range errorUIDChan {
		errStr = append(errStr, err)
	}
	if len(errStr) > 0 {
		return fmt.Errorf("failed to upload chunks: %v", errStr)
	}
	return nil
}

// Delete all files in the knowledge base
func (m *Minio) DeleteKnowledgeBase(ctx context.Context, kbUID string) chan error {
	// List all objects in the knowledge base
	err := m.DeleteFilesWithPrefix(ctx, kbUID)
	return err
}

// Delete converted files in the knowledge base
func (m *Minio) DeleteAllConvertedFilesInKb(ctx context.Context, kbUID string) chan error {
	// List all objects in the knowledge base
	err := m.DeleteFilesWithPrefix(ctx, kbUID+convertedFilePrefix)

	return err
}

// Delete uploaded files in the knowledge base
func (m *Minio) DeleteAllUploadedFilesInKb(ctx context.Context, kbUID string) chan error {
	// List all objects in the knowledge base
	err := m.DeleteFilesWithPrefix(ctx, kbUID+uploadedFilePrefix)

	return err
}

// Delete chunks in the knowledge base
func (m *Minio) DeleteAllChunksInKb(ctx context.Context, kbUID string) chan error {
	// List all objects in the knowledge base
	err := m.DeleteFilesWithPrefix(ctx, kbUID+chunkPrefix)

	return err
}

func (m *Minio) GetUploadedFilePathInKnowledgeBase(kbUID, dest string) string {
	return kbUID + uploadedFilePrefix + dest
}

func (m *Minio) GetConvertedFilePathInKnowledgeBase(kbUID, ConvertedFileUID, fileExt string) string {
	return kbUID + convertedFilePrefix + ConvertedFileUID + "." + fileExt
}

func (m *Minio) GetChunkPathInKnowledgeBase(kbUID, chunkUID string) string {
	return kbUID + chunkPrefix + chunkUID + ".txt"
}
