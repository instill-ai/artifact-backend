package service

import (
	"context"

	"github.com/instill-ai/artifact-backend/pkg/types"
)

func (s *service) GetConvertedFilePathsByFileUID(ctx context.Context, kbUID types.KBUIDType, fileUID types.FileUIDType) ([]string, error) {
	return s.repository.ListConvertedFilesByFileUID(ctx, kbUID, fileUID)
}

func (s *service) GetTextChunkFilePathsByFileUID(ctx context.Context, kbUID types.KBUIDType, fileUID types.FileUIDType) ([]string, error) {
	return s.repository.ListTextChunksByFileUID(ctx, kbUID, fileUID)
}
