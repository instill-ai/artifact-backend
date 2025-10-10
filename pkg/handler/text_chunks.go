package handler

import (
	"context"
	"fmt"
	"sort"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
)

// convertToProtoChunk
func convertToProtoChunk(textChunk repository.TextChunkModel) *artifactpb.Chunk {
	var contentType artifactpb.ContentType

	switch textChunk.ContentType {
	case string(types.SummaryContentType):
		contentType = artifactpb.ContentType_CONTENT_TYPE_SUMMARY
	case string(types.ChunkContentType):
		contentType = artifactpb.ContentType_CONTENT_TYPE_CHUNK
	case string(types.AugmentedContentType):
		contentType = artifactpb.ContentType_CONTENT_TYPE_AUGMENTED
	}

	pbChunk := &artifactpb.Chunk{
		ChunkUid:        textChunk.UID.String(),
		Retrievable:     textChunk.Retrievable,
		Tokens:          uint32(textChunk.Tokens),
		CreateTime:      timestamppb.New(*textChunk.CreateTime),
		OriginalFileUid: textChunk.KBFileUID.String(),
		ContentType:     contentType,
	}

	if pbChunk.ContentType != artifactpb.ContentType_CONTENT_TYPE_CHUNK {
		return pbChunk
	}

	pbChunk.MarkdownReference = &artifactpb.Chunk_Reference{
		Start: &artifactpb.File_Position{
			Unit:        artifactpb.File_Position_UNIT_CHARACTER,
			Coordinates: []uint32{uint32(textChunk.StartPos)},
		},
		End: &artifactpb.File_Position{
			Unit:        artifactpb.File_Position_UNIT_CHARACTER,
			Coordinates: []uint32{uint32(textChunk.EndPos)},
		},
	}

	// StartPos and EndPos are deprecated in favor of MarkdownReference.
	pbChunk.StartPos = uint32(textChunk.StartPos)
	pbChunk.EndPos = uint32(textChunk.EndPos)

	if textChunk.Reference != nil && textChunk.Reference.PageRange[0] != 0 && textChunk.Reference.PageRange[1] != 0 {
		// We only extract one kind of reference for now. When more file
		// types are supported, we'll need to inspect the reference object
		// to build the response correctly.
		pbChunk.Reference = &artifactpb.Chunk_Reference{
			Start: &artifactpb.File_Position{
				Unit:        artifactpb.File_Position_UNIT_PAGE,
				Coordinates: []uint32{textChunk.Reference.PageRange[0]},
			},
			End: &artifactpb.File_Position{
				Unit:        artifactpb.File_Position_UNIT_PAGE,
				Coordinates: []uint32{textChunk.Reference.PageRange[1]},
			},
		}
	}

	return pbChunk
}

// ListChunks lists the chunks of a file
func (ph *PublicHandler) ListChunks(ctx context.Context, req *artifactpb.ListChunksRequest) (*artifactpb.ListChunksResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	_, err := getUserUIDFromContext(ctx)
	if err != nil {
		err := fmt.Errorf("failed to get user id from header: %v. err: %w", err, errorsx.ErrUnauthenticated)
		return nil, err
	}

	fileUID, err := uuid.FromString(req.FileUid)
	if err != nil {
		logger.Error("failed to parse file uid", zap.Error(err))
		return nil, fmt.Errorf("failed to parse file uid: %v. err: %w", err, errorsx.ErrInvalidArgument)
	}

	fileUIDs := []uuid.UUID{fileUID}
	kbfs, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, fileUIDs)
	if err != nil {
		logger.Error("failed to get knowledge base files by file uids", zap.Error(err))
		return nil, fmt.Errorf("failed to get knowledge base files by file uids")
	} else if len(kbfs) == 0 {
		logger.Error("no files found for the given file uids")
		return nil, fmt.Errorf("no files found for the given file uids: %v. err: %w", fileUIDs, errorsx.ErrNotFound)
	}
	kbf := kbfs[0]
	// ACL - check user's permission to read knowledge base
	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", kbf.KBUID, "reader")
	if err != nil {
		logger.Error("failed to check permission", zap.Error(err))
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err)
	}
	if !granted {
		return nil, fmt.Errorf("%w: no permission over catalog", errorsx.ErrUnauthorized)
	}
	sources, err := ph.service.Repository().GetSourceTableAndUIDByFileUIDs(ctx, []repository.KnowledgeBaseFileModel{kbf})
	if err != nil {
		logger.Error("failed to get source table and uid by file uids", zap.Error(err))
		return nil, fmt.Errorf("failed to get source table and uid by file uids ")
	}

	source, ok := sources[fileUID]
	if !ok {
		// source not found. if some files(e.g. pdf) don't have been converted yet. there is not source file for chunks.
		return nil, nil
	}

	chunks, err := ph.service.Repository().GetTextChunksBySource(ctx, source.SourceTable, source.SourceUID)
	if err != nil {
		logger.Error("failed to get text chunks by source", zap.Error(err))
		return nil, fmt.Errorf("failed to get text chunks by source: %w ", err)

	}
	// reorder the chunks by order
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].InOrder < chunks[j].InOrder
	})

	res := make([]*artifactpb.Chunk, 0, len(chunks))
	for _, chunk := range chunks {
		res = append(res, convertToProtoChunk(chunk))
	}

	return &artifactpb.ListChunksResponse{
		Chunks: res,
	}, nil
}

func (ph *PublicHandler) SearchChunks(ctx context.Context, req *artifactpb.SearchChunksRequest) (*artifactpb.SearchChunksResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	_, err := getUserUIDFromContext(ctx)
	if err != nil {
		logger.Error("failed to get user id from header", zap.Error(err))
		return nil, fmt.Errorf("failed to get user id from header: %v. err: %w", err, errorsx.ErrUnauthenticated)
	}
	// check if user can access the namespace
	ns, err := ph.service.GetNamespaceAndCheckPermission(ctx, req.NamespaceId)
	if err != nil {
		logger.Error("failed to get namespace and check permission", zap.Error(err))
		return nil, fmt.Errorf("failed to get namespace and check permission: %w", err)
	}

	chunkUIDs := make([]uuid.UUID, 0, len(req.ChunkUids))
	for _, chunkUID := range req.ChunkUids {
		chunkUID, err := uuid.FromString(chunkUID)
		if err != nil {
			logger.Error("failed to parse chunk uid", zap.Error(err))
			return nil, fmt.Errorf("failed to parse chunk uid: %w", err)
		}
		chunkUIDs = append(chunkUIDs, chunkUID)
	}
	// check if the chunkUIs is more than 20
	if len(chunkUIDs) > 25 {
		logger.Error("chunk uids is more than 20", zap.Int("chunk_uids_count", len(chunkUIDs)))
		return nil, fmt.Errorf("chunk uids is more than 20")
	}
	chunks, err := ph.service.Repository().GetTextChunksByUIDs(ctx, chunkUIDs)
	if err != nil {
		logger.Error("failed to get chunks by uids", zap.Error(err))
		return nil, fmt.Errorf("failed to get chunks by uids: %w", err)
	}

	// get the kbUIDs from chunks
	kbUIDs := make([]uuid.UUID, 0, len(chunks))
	for _, chunk := range chunks {
		kbUIDs = append(kbUIDs, chunk.KBUID)
	}
	// use kbUIDs to get the knowledge bases
	knowledgeBases, err := ph.service.Repository().GetKnowledgeBasesByUIDs(ctx, kbUIDs)
	if err != nil {
		logger.Error("failed to get knowledge bases by uids", zap.Error(err))
		return nil, fmt.Errorf("failed to get knowledge bases by uids: %w", err)
	}
	// check if the chunks's knowledge base's owner(namespace uid) is the same as namespace uuid in path
	for _, knowledgeBase := range knowledgeBases {
		if knowledgeBase.Owner != ns.NsUID.String() {
			logger.Error("chunks's namespace is not the same as namespace in path", zap.String("namespace_id_in_path", ns.NsUID.String()), zap.String("namespace_id_in_chunks", knowledgeBase.Owner))
			return nil, fmt.Errorf("chunks's namespace is not the same as namespace in path")
		}
	}

	// populate the response
	protoChunks := make([]*artifactpb.Chunk, 0, len(chunks))
	for _, chunk := range chunks {
		protoChunks = append(protoChunks, convertToProtoChunk(chunk))
	}
	return &artifactpb.SearchChunksResponse{
		Chunks: protoChunks,
	}, nil
}

// UpdateChunk updates the retrievable of a chunk
func (ph *PublicHandler) UpdateChunk(ctx context.Context, req *artifactpb.UpdateChunkRequest) (*artifactpb.UpdateChunkResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	chunks, err := ph.service.Repository().GetTextChunksByUIDs(ctx, []uuid.UUID{uuid.FromStringOrNil(req.ChunkUid)})
	if err != nil {
		logger.Error("failed to get chunks by uids", zap.Error(err))
		return nil, fmt.Errorf("failed to get chunks by uids: %w", err)
	}
	if len(chunks) == 0 {
		logger.Error("no chunks found for the given chunk uids")
		return nil, fmt.Errorf("no chunks found for the given chunk uids: %v. err: %w", req.ChunkUid, errorsx.ErrNotFound)
	}
	chunk := &chunks[0]
	// ACL - check user's permission to write knowledge base of chunks
	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", chunk.KBUID, "writer")
	if err != nil {
		logger.Error("failed to check permission", zap.Error(err))
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err)
	}
	if !granted {
		return nil, fmt.Errorf("%w: no permission over catalog", errorsx.ErrUnauthorized)
	}

	retrievable := req.Retrievable
	update := map[string]any{
		repository.TextChunkColumn.Retrievable: retrievable,
	}

	chunk, err = ph.service.Repository().UpdateTextChunk(ctx, req.ChunkUid, update)
	if err != nil {
		logger.Error("failed to update text chunk", zap.Error(err))
		return nil, fmt.Errorf("failed to update text chunk: %w", err)
	}

	return &artifactpb.UpdateChunkResponse{
		// Populate the response fields appropriately
		Chunk: convertToProtoChunk(*chunk),
	}, nil
}

// GetSourceFile gets the source file of a chunk
func (ph *PublicHandler) GetSourceFile(ctx context.Context, req *artifactpb.GetSourceFileRequest) (*artifactpb.GetSourceFileResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	fileUID, err := uuid.FromString(req.FileUid)
	if err != nil {
		logger.Error("failed to parse file uid", zap.Error(err))
		return nil, fmt.Errorf("failed to parse file uid: %v. err: %w", err, errorsx.ErrInvalidArgument)
	}

	// Fetch file to get knowledge base UID for ACL check
	files, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{fileUID})
	if err != nil || len(files) == 0 {
		logger.Error("failed to fetch file", zap.Error(err))
		return nil, fmt.Errorf("file not found. err: %w", errorsx.ErrNotFound)
	}
	file := files[0]

	// ACL - check if the user has access to the knowledge base BEFORE checking processing status
	// This ensures unauthorized users get 403, not information about file processing status
	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", file.KBUID, "writer")
	if err != nil {
		logger.Error("failed to check permission in GetSourceFile", zap.Error(err))
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err)
	}
	if !granted {
		return nil, fmt.Errorf("%w: no permission over catalog", errorsx.ErrUnauthorized)
	}

	// After authorization check, validate file processing status
	if file.ProcessStatus != artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED.String() {
		status := artifactpb.FileProcessStatus(artifactpb.FileProcessStatus_value[file.ProcessStatus])
		logger.Info("file processing not complete",
			zap.String("fileUID", fileUID.String()),
			zap.String("currentStatus", status.String()))

		errMsg := fmt.Sprintf("File is still being processed (current status: %s). Please wait for processing to complete.", status.String())
		if status == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_FAILED {
			// Include failure reason if available
			if file.ExtraMetaDataUnmarshal != nil && file.ExtraMetaDataUnmarshal.FailReason != "" {
				errMsg = fmt.Sprintf("File processing failed: %s", file.ExtraMetaDataUnmarshal.FailReason)
			} else {
				errMsg = "File processing failed. Please reprocess the file."
			}
		}
		return nil, fmt.Errorf("%s", errMsg)
	}

	source, err := ph.service.Repository().GetTruthSourceByFileUID(ctx, fileUID)
	if err != nil {
		logger.Error("failed to get truth source by file uid", zap.Error(err))
		return nil, fmt.Errorf("failed to get truth source by file uid. err: %w", err)
	}

	// Decide bucket based on file type: TEXT/MARKDOWN -> blob; others -> artifact
	bucketName := config.Config.Minio.BucketName
	if file.Type == artifactpb.FileType_FILE_TYPE_TEXT.String() || file.Type == artifactpb.FileType_FILE_TYPE_MARKDOWN.String() {
		bucketName = repository.BlobBucketName
	}
	content, err := ph.service.Repository().GetFile(ctx, bucketName, source.Dest)
	if err != nil {
		logger.Error("failed to get file from minio", zap.Error(err))
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

// SearchSourceFiles searches the source files of a file
func (ph *PublicHandler) SearchSourceFiles(ctx context.Context, req *artifactpb.SearchSourceFilesRequest) (*artifactpb.SearchSourceFilesResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Check if user can access the namespace
	_, err := ph.service.GetNamespaceAndCheckPermission(ctx, req.NamespaceId)
	if err != nil {
		logger.Error("failed to get namespace and check permission", zap.Error(err))
		return nil, fmt.Errorf("failed to get namespace and check permission: %w", err)
	}

	fileUIDs := make([]uuid.UUID, 0, len(req.FileUids))
	for _, fileUID := range req.FileUids {
		uid, err := uuid.FromString(fileUID)
		if err != nil {
			logger.Error("failed to parse file uid", zap.Error(err))
			return nil, fmt.Errorf("failed to parse file uid: %v. err: %w", err, errorsx.ErrInvalidArgument)
		}
		fileUIDs = append(fileUIDs, uid)
	}

	sources := make([]*artifactpb.SourceFile, 0, len(fileUIDs))
	for _, fileUID := range fileUIDs {
		source, err := ph.service.Repository().GetTruthSourceByFileUID(ctx, fileUID)
		if err != nil {
			logger.Error("failed to get truth source by file uid", zap.Error(err))
			return nil, fmt.Errorf("failed to get truth source by file uid. err: %w", err)
		}

		// ACL check for each source file
		granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", source.KBUID, "reader")
		if err != nil {
			return nil, fmt.Errorf("checking permission: %w", err)
		}
		if !granted {
			return nil, fmt.Errorf("%w: no permission over catalog", errorsx.ErrUnauthorized)
		}

		// Get file content from MinIO
		bucket := repository.BucketFromDestination(source.Dest)
		content, err := ph.service.Repository().GetFile(ctx, bucket, source.Dest)
		if err != nil {
			logger.Error("failed to get file from minio", zap.Error(err))
			continue
		}

		sources = append(sources, &artifactpb.SourceFile{
			OriginalFileUid:  source.OriginalFileUID.String(),
			OriginalFileName: source.OriginalFileName,
			Content:          string(content),
			CreateTime:       timestamppb.New(source.CreateTime),
			UpdateTime:       timestamppb.New(source.UpdateTime),
		})
	}

	return &artifactpb.SearchSourceFilesResponse{
		SourceFiles: sources,
	}, nil
}
