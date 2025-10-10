package handler

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/pipeline"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactPb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
)

// QuestionAnswering provides the response to the prompted question, returning
// contextual information like the chunks used to build the answer.
func (ph *PublicHandler) QuestionAnswering(
	ctx context.Context,
	req *artifactPb.QuestionAnsweringRequest,
) (*artifactPb.QuestionAnsweringResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	t := time.Now()
	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		logger.Error("failed to get namespace by ns id", zap.Error(err))
		return nil, fmt.Errorf("failed to get namespace by ns id. err: %w", err)
	}
	logger.Info("get namespace by ns id", zap.Duration("duration", time.Since(t)))
	t = time.Now()
	ownerUID := ns.NsUID
	kb, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ownerUID, req.CatalogId)
	if err != nil {
		logger.Error("failed to get catalog by namespace and catalog id", zap.Error(err))
		return nil, fmt.Errorf("failed to get catalog by namespace and catalog id. err: %w", err)
	}
	logger.Info("get catalog by owner and kb id", zap.Duration("duration", time.Since(t)))
	t = time.Now()
	// ACL : check user has access to the catalog
	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", kb.UID, "reader")
	if err != nil {
		logger.Error("failed to check permission", zap.Error(err))
		return nil, fmt.Errorf("failed to check permission. err: %w", err)
	}
	if !granted {
		return nil, fmt.Errorf("SimilarityChunksSearch permission denied. err: %w", errorsx.ErrUnauthenticated)
	}
	logger.Info("check permission", zap.Duration("duration", time.Since(t)))
	t = time.Now()
	// check auth user has access to the requester
	err = ph.service.ACLClient().CheckRequesterPermission(ctx)
	if err != nil {
		logger.Error("failed to check requester permission", zap.Error(err))
		return nil, fmt.Errorf("failed to check requester permission. err: %w", err)
	}

	// Convert file UID strings to UUIDs for cache lookup and status checking
	var fileUIDs []types.FileUIDType
	if len(req.GetFileUids()) > 0 {
		fileUIDs = make([]types.FileUIDType, 0, len(req.GetFileUids()))
		for _, uidStr := range req.GetFileUids() {
			uid, err := uuid.FromString(uidStr)
			if err != nil {
				logger.Warn("Invalid file UID", zap.String("uid", uidStr), zap.Error(err))
				continue
			}
			fileUIDs = append(fileUIDs, types.FileUIDType(uid))
		}
	}

	// ===== NEW: Try chat cache path for files still being processed =====
	if len(fileUIDs) > 0 {
		// Check if files are still processing
		allCompleted, processingCount, err := ph.service.CheckFilesProcessingStatus(ctx, fileUIDs)
		if err != nil {
			logger.Warn("Failed to check file processing status", zap.Error(err))
			// Continue to traditional RAG path
		} else if !allCompleted {
			logger.Info("Some files still processing, checking for chat cache",
				zap.Int("processingCount", processingCount),
				zap.Int("totalFiles", len(fileUIDs)))

			// Try to get chat cache from Redis
			chatCache, err := ph.service.GetChatCacheForFiles(ctx, kb.UID, fileUIDs)
			if err != nil {
				logger.Warn("Failed to retrieve chat cache from Redis", zap.Error(err))
			} else if chatCache != nil {
				// Chat cache or file references found! Use for instant chat
				if chatCache.CachedContextEnabled {
					logger.Info("Using cached context for instant response",
						zap.String("cacheName", chatCache.CacheName),
						zap.Int("fileCount", chatCache.FileCount),
						zap.Time("expireTime", chatCache.ExpireTime))
				} else {
					logger.Info("Using stored file content for instant response (uncached files)",
						zap.Int("fileCount", len(chatCache.FileContents)))
				}

				answer, err := ph.service.ChatWithCache(ctx, chatCache, req.GetQuestion())
				if err != nil {
					logger.Error("Chat response failed, falling back to traditional RAG", zap.Error(err))
					// Fall through to traditional RAG path below
				} else {
					// Success! Return answer from cache or file content
					logger.Info("Successfully generated response using chat metadata",
						zap.Bool("cached", chatCache.CachedContextEnabled),
						zap.String("prompt", req.GetQuestion()),
						zap.Int("answerLength", len(answer)))

					return &artifactPb.QuestionAnsweringResponse{
						Answer:        answer,
						SimilarChunks: []*artifactPb.SimilarityChunk{}, // Empty - no vector search performed
					}, nil
				}
			} else {
				logger.Info("No active chat cache or file references found, using traditional RAG",
					zap.Int("fileCount", len(fileUIDs)))
			}
		}
	}

	// ===== EXISTING: Traditional RAG path (vector search + pipeline) =====
	logger.Info("Using traditional RAG (vector search + LLM pipeline)")

	// retrieve the chunks based on the similarity
	scReq := &artifactPb.SimilarityChunksSearchRequest{
		TextPrompt:  req.GetQuestion(),
		TopK:        uint32(req.GetTopK()),
		CatalogId:   req.GetCatalogId(),
		NamespaceId: req.GetNamespaceId(),
		FileUids:    req.GetFileUids(),
	}

	// Extract authentication metadata from context to pass to worker
	var requestMetadata map[string][]string
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		md, ok = metadata.FromOutgoingContext(ctx)
	}
	if ok {
		requestMetadata = md
	}

	// Embed prompt using service
	textVector, err := ph.service.EmbedTexts(ctx, []string{req.GetQuestion()}, 32, requestMetadata)
	if err != nil {
		logger.Error("failed to vectorize prompt", zap.Error(err))
		return nil, fmt.Errorf("failed to vectorize prompt. err: %w", err)
	}

	simChunksScores, err := ph.service.SimilarityChunksSearch(ctx, ownerUID, scReq, textVector)
	if err != nil {
		logger.Error("failed to get similarity chunks", zap.Error(err))
		return nil, fmt.Errorf("failed to get similarity chunks. err: %w", err)
	}
	var chunkUIDs []types.TextChunkUIDType
	for _, simChunk := range simChunksScores {
		chunkUIDs = append(chunkUIDs, simChunk.ChunkUID)
	}
	logger.Info("get similarity chunks", zap.Duration("duration", time.Since(t)))
	t = time.Now()
	// fetch the chunks metadata
	chunks, err := ph.service.Repository().GetTextChunksByUIDs(ctx, chunkUIDs)
	if err != nil {
		logger.Error("failed to get chunks by uids", zap.Error(err))
		return nil, fmt.Errorf("failed to get chunks by uids. err: %w", err)
	}
	// chunks content
	chunkFilePaths := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		chunkFilePaths = append(chunkFilePaths, chunk.ContentDest)
	}
	logger.Info("get chunks by uids", zap.Duration("duration", time.Since(t)))
	t = time.Now()

	// Get chunk contents using service
	chunkContents, err := ph.service.GetFilesByPaths(ctx, config.Config.Minio.BucketName, chunkFilePaths)
	if err != nil {
		logger.Error("failed to get chunks content", zap.Error(err))
		return nil, fmt.Errorf("failed to get chunks content. err: %w", err)
	}
	logger.Info("get chunks content from minIO", zap.Duration("duration", time.Since(t)))

	// fetch the file names
	fileUIDMapName := make(map[types.FileUIDType]string)
	for _, chunk := range chunks {
		fileUIDMapName[chunk.KBFileUID] = ""
	}
	fileUids := make([]types.FileUIDType, 0, len(fileUIDMapName))
	for fileUID := range fileUIDMapName {
		fileUids = append(fileUids, fileUID)
	}
	t = time.Now()
	files, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(
		ctx, fileUids, repository.KnowledgeBaseFileColumn.UID, repository.KnowledgeBaseFileColumn.Name)
	if err != nil {
		logger.Error("failed to get catalog files by file uids", zap.Error(err))
		return nil, fmt.Errorf("failed to get catalog files by file uids. err: %w", err)
	}
	for _, file := range files {
		fileUIDMapName[file.UID] = file.Name
	}
	logger.Info("get catalog files by file uids", zap.Duration("duration", time.Since(t)))

	// prepare the response
	simChunks := make([]*artifactPb.SimilarityChunk, 0, len(chunks))
	for i, chunk := range chunks {
		if !chunk.Retrievable {
			continue
		}
		simChunks = append(simChunks, &artifactPb.SimilarityChunk{
			ChunkUid:        chunk.UID.String(),
			SimilarityScore: float32(simChunksScores[i].Score),
			TextContent:     string(chunkContents[i].Content),
			SourceFile:      fileUIDMapName[chunk.KBFileUID],
		})
	}
	chunksForQA := make([]string, 0, len(simChunks))
	for _, simChunk := range simChunks {
		chunksForQA = append(chunksForQA, simChunk.TextContent)
	}

	answer, err := pipeline.ChatPipe(ctx, ph.service.PipelinePublicClient(), req.Question, chunksForQA)
	if err != nil {
		logger.Error("failed to get chat response", zap.Error(err))
		return nil, fmt.Errorf("failed to get chat response. err: %w", err)
	}

	return &artifactPb.QuestionAnsweringResponse{SimilarChunks: simChunks, Answer: answer}, nil
}
