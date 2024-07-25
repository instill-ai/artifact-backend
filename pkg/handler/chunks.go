package handler

import (
	"context"
	"fmt"

	"github.com/gofrs/uuid"
	"github.com/instill-ai/artifact-backend/pkg/customerror"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (ph *PublicHandler) ListChunks(ctx context.Context, req *artifactpb.ListChunksRequest) (*artifactpb.ListChunksResponse, error) {
	log, _ := logger.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		log.Error("failed to get user id from header", zap.Error(err))
		return nil, fmt.Errorf("failed to get user id from header: %v. err: %w", err, customerror.ErrUnauthenticated)
	}
	fileUID, err := uuid.FromString(req.FileUid)
	if err != nil {
		log.Error("failed to parse file uid", zap.Error(err))
		return nil, fmt.Errorf("failed to parse file uid: %v. err: %w", err, customerror.ErrInvalidArgument)
	}
	// ACL - check if the user(uid from context) has access to the chunk. get knowledge base id and owner id and check if the user has access to the knowledge base
	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		log.Error(
			"failed to get namespace ",
			zap.Error(err),
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
			zap.String("auth_uid", authUID))
		return nil, fmt.Errorf("failed to get namespace. err: %w", err)
	}
	// ACL - check user's permission to write knowledge base
	kb, err := ph.service.Repository.GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID.String(), req.KbId)
	if err != nil {
		log.Error("failed to get knowledge base", zap.Error(err))
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}
	granted, err := ph.service.ACLClient.CheckPermission(ctx, "knowledgebase", kb.UID, "writer")
	if err != nil {
		log.Error("failed to check permission", zap.Error(err))
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err)
	}
	if !granted {
		log.Error("no permission list chunks", zap.String("user_id", authUID), zap.String("kb_id", kb.UID.String()))
		return nil, fmt.Errorf(ErrorDeleteKnowledgeBaseMsg, customerror.ErrNoPermission)
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
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		log.Error("failed to get user id from header", zap.Error(err))
		return nil, fmt.Errorf("failed to get user id from header: %v. err: %w", err, customerror.ErrUnauthenticated)
	}

	// ACL - check if the user(uid from context) has access to the chunk. get chunk's kb uid and check if the uid has access to the knowledge base
	chunks, err := ph.service.Repository.GetChunksByUIDs(ctx, []uuid.UUID{uuid.FromStringOrNil(req.ChunkUid)})
	if err != nil {
		log.Error("failed to get chunks by uids", zap.Error(err))
		return nil, fmt.Errorf("failed to get chunks by uids: %w", err)
	}
	if len(chunks) == 0 {
		log.Error("no chunks found for the given chunk uids")
		return nil, fmt.Errorf("no chunks found for the given chunk uids: %v. err: %w", req.ChunkUid, customerror.ErrNotFound)
	}
	// ACL - check user's permission to write knowledge base
	granted, err := ph.service.ACLClient.CheckPermission(ctx, "knowledgebase", chunks[0].KbUID, "writer")
	if err != nil {
		log.Error("failed to check permission", zap.Error(err))
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err)
	}
	if !granted {
		log.Error("no permission update chunks", zap.String("user_id", authUID), zap.String("kb_id", chunks[0].KbUID.String()))
		return nil, fmt.Errorf(ErrorDeleteKnowledgeBaseMsg, customerror.ErrNoPermission)
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
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		log.Error("failed to get user id from header", zap.Error(err))
		return nil, fmt.Errorf("failed to get user id from header: %v. err: %w", err, customerror.ErrUnauthenticated)
	}
	fileUID, err := uuid.FromString(req.FileUid)
	if err != nil {
		log.Error("failed to parse file uid", zap.Error(err))
		return nil, fmt.Errorf("failed to parse file uid: %v. err: %w", err, customerror.ErrInvalidArgument)
	}

	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		log.Error(
			"failed to get namespace ",
			zap.Error(err),
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
			zap.String("auth_uid", authUID))
		return nil, fmt.Errorf("failed to get namespace in GetSourceFile. err: %w", err)
	}
	// ACL - check user's permission to write knowledge base
	kb, err := ph.service.Repository.GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID.String(), req.KbId)
	if err != nil {
		log.Error("failed to get knowledge base in GetSourceFile", zap.Error(err))
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}
	// ACL - check if the user(uid from context) has access to the source file.
	granted, err := ph.service.ACLClient.CheckPermission(ctx, "knowledgebase", kb.UID, "writer")
	if err != nil {
		log.Error("failed to check permission in GetSourceFile", zap.Error(err))
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err)
	}
	if !granted {
		log.Error("no permission get source file in GetSourceFile", zap.String("user_id", authUID), zap.String("kb_id", kb.UID.String()))
		return nil, fmt.Errorf(ErrorDeleteKnowledgeBaseMsg, customerror.ErrNoPermission)
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
