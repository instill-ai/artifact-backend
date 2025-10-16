package handler

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"

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
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get namespace by ns id. err: %w", err),
			"Unable to access the specified namespace. Please check the namespace ID and try again.",
		)
	}
	logger.Info("get namespace by ns id", zap.Duration("duration", time.Since(t)))
	t = time.Now()
	ownerUID := ns.NsUID
	kb, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ownerUID, req.CatalogId)
	if err != nil {
		logger.Error("failed to get catalog by namespace and catalog id", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get catalog by namespace and catalog id. err: %w", err),
			"Unable to access the specified catalog. Please check the catalog ID and try again.",
		)
	}
	logger.Info("get catalog by owner and kb id", zap.Duration("duration", time.Since(t)))
	t = time.Now()
	// ACL : check user has access to the catalog
	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", kb.UID, "reader")
	if err != nil {
		logger.Error("failed to check permission", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to check permission. err: %w", err),
			"Unable to verify access permissions. Please try again.",
		)
	}
	if !granted {
		return nil, errorsx.AddMessage(
			errorsx.ErrUnauthenticated,
			"You don't have permission to access this catalog. Please contact the owner for access.",
		)
	}
	logger.Info("check permission", zap.Duration("duration", time.Since(t)))
	t = time.Now()
	// check auth user has access to the requester
	err = ph.service.ACLClient().CheckRequesterPermission(ctx)
	if err != nil {
		logger.Error("failed to check requester permission", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to check requester permission. err: %w", err),
			"Unable to verify your authentication. Please log in again and try again.",
		)
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

	// Embed prompt using appropriate provider based on KB's embedding config
	// Note: For question answering, we use QUESTION_ANSWERING task type which optimizes
	// for finding documents that contain answers to the question
	textVector, err := ph.service.EmbedTexts(ctx, &kb.UID, []string{req.GetQuestion()}, "QUESTION_ANSWERING")
	if err != nil {
		logger.Error("failed to vectorize prompt", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to vectorize prompt. err: %w", err),
			"Unable to process your question. Please try again.",
		)
	}

	simChunksScores, err := ph.service.SimilarityChunksSearch(ctx, ownerUID, scReq, textVector)
	if err != nil {
		logger.Error("failed to get similarity chunks", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get similarity chunks. err: %w", err),
			"Unable to search for relevant content. Please try again.",
		)
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
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get chunks by uids. err: %w", err),
			"Unable to retrieve relevant content. Please try again.",
		)
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
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get chunks content. err: %w", err),
			"Unable to load content for answering your question. Please try again.",
		)
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
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get catalog files by file uids. err: %w", err),
			"Unable to load file information. Please try again.",
		)
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
		// Convert chunk to protobuf format to include page references
		chunkMetadata := convertToProtoChunk(chunk)

		simChunks = append(simChunks, &artifactPb.SimilarityChunk{
			ChunkUid:        chunk.UID.String(),
			SimilarityScore: float32(simChunksScores[i].Score),
			TextContent:     string(chunkContents[i].Content),
			SourceFile:      fileUIDMapName[chunk.KBFileUID],
			ChunkMetadata:   chunkMetadata, // Include full chunk with page references
		})
	}
	chunksForQA := make([]string, 0, len(simChunks))
	for _, simChunk := range simChunks {
		chunksForQA = append(chunksForQA, simChunk.TextContent)
	}

	answer, err := pipeline.ChatPipe(ctx, ph.service.PipelinePublicClient(), req.Question, chunksForQA)
	if err != nil {
		logger.Error("failed to get chat response", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get chat response. err: %w", err),
			"Unable to generate an answer to your question. Please try again.",
		)
	}

	return &artifactPb.QuestionAnsweringResponse{SimilarChunks: simChunks, Answer: answer}, nil
}
