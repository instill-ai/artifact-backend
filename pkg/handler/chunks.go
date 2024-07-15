package handler

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/instill-ai/artifact-backend/pkg/customerror"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (ph *PublicHandler) ListChunks(ctx context.Context, req *artifactpb.ListChunksRequest) (*artifactpb.ListChunksResponse, error) {
	log, _ := logger.GetZapLogger(ctx)
	uid, err := getUserUIDFromContext(ctx)
	if err != nil {
		log.Error("failed to get user id from header", zap.Error(err))
		return nil, fmt.Errorf("failed to get user id from header: %v. err: %w", err, customerror.ErrUnauthenticated)
	}
	fileUID, err := uuid.Parse(req.FileUid)
	if err != nil {
		log.Error("failed to parse file uid", zap.Error(err))
		return nil, fmt.Errorf("failed to parse file uid: %v. err: %w", err, customerror.ErrInvalidArgument)
	}
	// TODO: ACL - check if the user(uid from context) has access to the chunk. get knowledge base id and owner id and check if the user has access to the knowledge base
	{
		_ = uid
		_ = req.OwnerId
		_ = req.KbId
	}

	fileUIDs := []uuid.UUID{fileUID}
	files, err := ph.service.Repository.GetKnowledgeBaseFilesByFileUIDs(ctx, fileUIDs)
	if err != nil {
		log.Error("failed to get knowledge base files by file uids", zap.Error(err))
		return nil, fmt.Errorf("failed to get knowledge base files by file uids")
	}
	if len(files) == 0 {
		log.Error("no files found for the given file uids")
		return nil, fmt.Errorf("no files found for the given file uids: %v. err: %w", fileUIDs, customerror.ErrNotFound)
	}
	file := files[0]

	sources, err := ph.service.Repository.GetSourceTableAndUIDByFileUIDs(ctx, []repository.KnowledgeBaseFile{file})
	if err != nil {
		log.Error("failed to get source table and uid by file uids", zap.Error(err))
		return nil, fmt.Errorf("failed to get source table and uid by file uids ")
	}

	source, ok := sources[fileUID]
	if !ok {
		// source not found. if some files(e.g. pdf) don't have been converted yet. there is not source file for chunks.
		return nil, nil
	}

	chunks, err := ph.service.Repository.GetTextChunksBySource(ctx, source.SourceTable, source.SourceUID)
	if err != nil {
		log.Error("failed to get text chunks by source", zap.Error(err))
		return nil, fmt.Errorf("failed to get text chunks by source: %w ", err)

	}

	res := make([]*artifactpb.Chunk, 0, len(chunks))
	for _, chunk := range chunks {
		res = append(res, &artifactpb.Chunk{
			ChunkUid:        chunk.UID.String(),
			Retrievable:     chunk.Retrievable,
			StartPos:        uint32(chunk.StartPos),
			EndPos:          uint32(chunk.EndPos),
			Tokens:          uint32(chunk.Tokens),
			CreateTime:      timestamppb.New(*chunk.CreateTime),
			OriginalFileUid: file.UID.String(),
		})
	}

	return &artifactpb.ListChunksResponse{
		Chunks: res,
	}, nil
}

func (ph *PublicHandler) UpdateChunk(ctx context.Context, req *artifactpb.UpdateChunkRequest) (*artifactpb.UpdateChunkResponse, error) {
	log, _ := logger.GetZapLogger(ctx)
	uid, err := getUserUIDFromContext(ctx)
	if err != nil {
		log.Error("failed to get user id from header", zap.Error(err))
		return nil, fmt.Errorf("failed to get user id from header: %v. err: %w", err, customerror.ErrUnauthenticated)
	}

	// TODO: ACL - check if the user(uid from context) has access to the chunk. get chunk's kb uid and check if the uid has access to the knowledge base
	{
		_ = uid
		_ = req.ChunkUid
		// use chunk uid to get the knowledge base id
		// ....
		// check ACL
	}

	retrievable := req.Retrievable
	update := map[string]interface{}{
		repository.TextChunkColumn.Retrievable: retrievable,
	}

	chunk, err := ph.service.Repository.UpdateChunk(ctx, req.ChunkUid, update)
	if err != nil {
		log.Error("failed to update text chunk", zap.Error(err))
		return nil, fmt.Errorf("failed to update text chunk: %w", err)
	}

	return &artifactpb.UpdateChunkResponse{
		// Populate the response fields appropriately
		Chunk: &artifactpb.Chunk{
			ChunkUid:    chunk.UID.String(),
			Retrievable: chunk.Retrievable,
			StartPos:    uint32(chunk.StartPos),
			EndPos:      uint32(chunk.EndPos),
			Tokens:      uint32(chunk.Tokens),
			CreateTime:  timestamppb.New(*chunk.CreateTime),
			// OriginalFileUid: chunk.FileUID.String(),
		},
	}, nil
}

func (ph *PublicHandler) GetSourceFile(ctx context.Context, req *artifactpb.GetSourceFileRequest) (*artifactpb.GetSourceFileResponse, error) {
	log, _ := logger.GetZapLogger(ctx)
	uid, err := getUserUIDFromContext(ctx)
	if err != nil {
		log.Error("failed to get user id from header", zap.Error(err))
		return nil, fmt.Errorf("failed to get user id from header: %v. err: %w", err, customerror.ErrUnauthenticated)
	}
	fileUID, err := uuid.Parse(req.FileUid)
	if err != nil {
		log.Error("failed to parse file uid", zap.Error(err))
		return nil, fmt.Errorf("failed to parse file uid: %v. err: %w", err, customerror.ErrInvalidArgument)
	}

	// TODO ACL - check if the user(uid from context) has access to the source file.
	{
		_ = uid
		_ = req.FileUid
		// use file uid to get the knowledge base id
		// ....
		// check ACL
		// ...
	}
	source, err := ph.service.Repository.GetTruthSourceByFileUID(ctx, fileUID)
	if err != nil {
		log.Error("failed to get truth source by file uid", zap.Error(err))
		return nil, fmt.Errorf("failed to get truth source by file uid. err: %w", err)
	}
	// get the source file content from minIO using dest of source
	content, err := ph.service.MinIO.GetFile(ctx, source.Dest)
	if err != nil {
		log.Error("failed to get file from minio", zap.Error(err))
		return nil, fmt.Errorf("failed to get file from minio. err: %w", err)
	}

	return &artifactpb.GetSourceFileResponse{
		// Populate the response fields appropriately
		SourceFile: &artifactpb.SourceFile{
			Content:    string(content),
			CreateTime: timestamppb.New(source.CreateTime),
			UpdateTime: timestamppb.New(source.CreateTime),
		},
	}, nil
}
