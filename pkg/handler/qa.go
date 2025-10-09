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

	// Embed question using service
	textVector, err := ph.service.EmbedTexts(ctx, []string{req.GetQuestion()}, 32, requestMetadata)
	if err != nil {
		logger.Error("failed to vectorize question", zap.Error(err))
		return nil, fmt.Errorf("failed to vectorize question. err: %w", err)
	}

	simChunksScores, err := ph.service.SimilarityChunksSearch(ctx, ownerUID, scReq, textVector)
	if err != nil {
		logger.Error("failed to get similarity chunks", zap.Error(err))
		return nil, fmt.Errorf("failed to get similarity chunks. err: %w", err)
	}
	var chunkUIDs []uuid.UUID
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
	fileUIDMapName := make(map[uuid.UUID]string)
	for _, chunk := range chunks {
		fileUIDMapName[chunk.KBFileUID] = ""
	}
	fileUids := make([]uuid.UUID, 0, len(fileUIDMapName))
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
	answer, err := pipeline.QuestionAnsweringPipe(ctx, ph.service.PipelinePublicClient(), req.Question, chunksForQA)
	if err != nil {
		logger.Error("failed to get question answering response", zap.Error(err))
		return nil, fmt.Errorf("failed to get question answering response. err: %w", err)
	}
	return &artifactPb.QuestionAnsweringResponse{SimilarChunks: simChunks, Answer: answer}, nil
}
