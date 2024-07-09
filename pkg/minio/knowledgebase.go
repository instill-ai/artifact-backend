package minio

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"
)

// KnowledgeBaseI is the interface for knowledge base related operations.
type KnowledgeBaseI interface {
	// SaveConvertedFile saves a converted file to MinIO with the appropriate MIME type.
	SaveConvertedFile(ctx context.Context, kbUID, convertedFileUID, fileExt string, content []byte) error
	// SaveChunks saves batch of chunks(text files) to MinIO.
	SaveChunks(ctx context.Context, kbUID string, chunks map[ChunkUIDType]ChunkContentType) error
	// GetUploadedFilePathInKnowledgeBase returns the path of the uploaded file in MinIO.
	GetUploadedFilePathInKnowledgeBase(kbUID, dest string) string
	// GetConvertedFilePathInKnowledgeBase returns the path of the converted file in MinIO.
	GetConvertedFilePathInKnowledgeBase(kbUID, ConvertedFileUID, fileExt string) string
	// GetChunkPathInKnowledgeBase returns the path of the chunk in MinIO.
	GetChunkPathInKnowledgeBase(kbUID, chunkUID string) string
}

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

// SaveChunks saves batch of chunks(text files) to MinIO.
func (m *Minio) SaveChunks(ctx context.Context, kbUID string, chunks map[ChunkUIDType]ChunkContentType) error {
	var wg sync.WaitGroup
	errorUIDChan := make(chan string, len(chunks))
	for chunkUID, chunkContent := range chunks {
		wg.Add(1)
		go func(chunkUID ChunkUIDType, chunkContent ChunkContentType) {
			defer wg.Done()
			filePathName := m.GetChunkPathInKnowledgeBase(kbUID, string(chunkUID))

			err := m.UploadBase64File(ctx, filePathName, base64.StdEncoding.EncodeToString(chunkContent), "text/plain")
			if err != nil {
				errorUIDChan <- string(chunkUID)
				return
			}
		}(chunkUID, chunkContent)
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

func (m *Minio) GetUploadedFilePathInKnowledgeBase(kbUID, dest string) string {
	return kbUID + "/uploaded-file/" + dest
}

func (m *Minio) GetConvertedFilePathInKnowledgeBase(kbUID, ConvertedFileUID, fileExt string) string {
	return kbUID + "/converted-file/" + ConvertedFileUID + "." + fileExt
}

func (m *Minio) GetChunkPathInKnowledgeBase(kbUID, chunkUID string) string {
	return kbUID + "/chunk/" + chunkUID + ".txt"
}
