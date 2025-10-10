package handler

import (
	"context"
	"encoding/base64"
	"fmt"

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

// GetFileCatalog returns a view of the file within the catalog, with the text
// chunks it generated after being processed.
func (ph *PublicHandler) GetFileCatalog(ctx context.Context, req *artifactpb.GetFileCatalogRequest) (*artifactpb.GetFileCatalogResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// ACL - check if the user(uid from context) has access to the knowledge
	// base of source file.
	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		return nil, fmt.Errorf("getting namespace: %w", err)
	}

	kb, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, req.GetCatalogId())
	if err != nil {
		return nil, fmt.Errorf("fetching catalog: %w", err)
	}

	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", kb.UID, "reader")
	switch {
	case err != nil:
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err)
	case !granted:
		return nil, fmt.Errorf("%w: no permission over catalog", errorsx.ErrUnauthorized)
	}

	fileUID := uuid.FromStringOrNil(req.GetFileUid())
	kbfs, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []types.FileUIDType{types.FileUIDType(fileUID)})
	switch {
	case err != nil:
		return nil, fmt.Errorf("fetching file from repository: %w", err)
	case len(kbfs) == 0 || kbfs[0].KBUID != kb.UID:
		return nil, fmt.Errorf("fetching file from repository: %w", errorsx.ErrNotFound)
	}

	kbFile := &(kbfs[0])

	// Get source file.
	source, err := ph.service.Repository().GetTruthSourceByFileUID(ctx, kbFile.UID)
	if err != nil {
		return nil, fmt.Errorf("fetching truth source: %w", err)
	}

	// Get the source file sourceContent from MinIO using destination of source.
	sourceContent, err := getFileWithBucketFallback(ctx, ph.service.Repository(), config.Config.Minio.BucketName, source.Dest)
	if err != nil {
		return nil, fmt.Errorf("getting file from blob storage: %w", err)
	}

	// Get text chunks.
	//
	// NOTE: in the future, we may support other types of segment, e.g. image,
	// audio, etc.
	_, _, textChunks, chunkUIDToContent, _, err := ph.service.GetChunksByFile(ctx, kbFile)
	if err != nil {
		return nil, fmt.Errorf("fetching file chunks: %w", err)
	}

	pbChunks := make([]*artifactpb.GetFileCatalogResponse_Chunk, 0, len(textChunks))

	// Map chunks to embeddings.
	embeddings, err := ph.service.Repository().ListEmbeddingsByKBFileUID(ctx, kbFile.UID)
	if err != nil {
		return nil, fmt.Errorf("getting file embeddings: %w", err)
	}

	// NOTE: in the future if we support embeddings for other types of source,
	// we need to filter here.
	targetSourceTable := repository.TextChunkTableName

	embeddingMap := make(map[types.SourceUIDType]repository.EmbeddingModel)
	for _, embedding := range embeddings {
		if embedding.SourceTable != targetSourceTable {
			continue
		}

		embeddingMap[embedding.SourceUID] = embedding
	}

	for _, chunk := range textChunks {
		logger := logger.With(zap.String("chunkUID", chunk.UID.String()))

		embedding, ok := embeddingMap[chunk.UID]
		if !ok {
			logger.Error("Couldn't find embedding for chunk")
		}

		content, ok := chunkUIDToContent[chunk.UID]
		if !ok {
			logger.Error("Couldn't find content for chunk")
		}

		var createTime *timestamppb.Timestamp
		if chunk.CreateTime != nil {
			createTime = timestamppb.New(*chunk.CreateTime)
		}
		pbChunks = append(pbChunks, &artifactpb.GetFileCatalogResponse_Chunk{
			Uid:           chunk.UID.String(),
			Type:          artifactpb.GetFileCatalogResponse_CHUNK_TYPE_TEXT,
			StartPosition: int32(chunk.StartPos),
			EndPosition:   int32(chunk.EndPos),
			Content:       string(content),
			TokenCount:    int32(chunk.Tokens),
			Embedding:     embedding.Vector,
			CreateTime:    createTime,
			Retrievable:   chunk.Retrievable,
		})
	}

	var fileCreateTime *timestamppb.Timestamp
	if kbFile.CreateTime != nil {
		fileCreateTime = timestamppb.New(*kbFile.CreateTime)
	}

	var totalTokens int32
	for _, chunk := range pbChunks {
		totalTokens += chunk.TokenCount
	}

	pipelines := getPipelines(kbFile)

	// Retrieve the original file content from MinIO.
	minIOPath := kbFile.Destination
	originalContent, err := getFileWithBucketFallback(ctx, ph.service.Repository(), config.Config.Minio.BucketName, minIOPath)
	if err != nil {
		return nil, fmt.Errorf("fetching original file from blob: %w", err)
	}

	originalDataBase64 := base64.StdEncoding.EncodeToString(originalContent)

	return &artifactpb.GetFileCatalogResponse{
		OriginalData: originalDataBase64,
		FileMetadata: &artifactpb.GetFileCatalogResponse_FileMetadata{
			Uid:           kbFile.UID.String(),
			Filename:      kbFile.Name,
			FileType:      artifactpb.FileType(artifactpb.FileType_value[kbFile.Type]),
			Size:          kbFile.Size,
			CreateTime:    fileCreateTime,
			ProcessStatus: artifactpb.FileProcessStatus(artifactpb.FileProcessStatus_value[kbFile.ProcessStatus]),
		},
		Text: &artifactpb.GetFileCatalogResponse_Text{
			Pipelines:  pipelines,
			Content:    string(sourceContent),
			ChunkCount: int32(len(pbChunks)),
			TokenCount: totalTokens,
			UpdateTime: fileCreateTime,
		},
		Chunks: pbChunks,
	}, nil
}

func getPipelines(kbf *repository.KnowledgeBaseFileModel) []string {
	if kbf == nil || kbf.ExtraMetaDataUnmarshal == nil {
		return nil
	}
	pipes := []string{}
	if kbf.ExtraMetaDataUnmarshal.ConvertingPipe != "" {
		pipes = append(pipes, kbf.ExtraMetaDataUnmarshal.ConvertingPipe)
	}
	if kbf.ExtraMetaDataUnmarshal.SummarizingPipe != "" {
		pipes = append(pipes, kbf.ExtraMetaDataUnmarshal.SummarizingPipe)
	}
	if kbf.ExtraMetaDataUnmarshal.ChunkingPipe != "" {
		pipes = append(pipes, kbf.ExtraMetaDataUnmarshal.ChunkingPipe)
	}
	if kbf.ExtraMetaDataUnmarshal.EmbeddingPipe != "" {
		pipes = append(pipes, kbf.ExtraMetaDataUnmarshal.EmbeddingPipe)
	}
	return pipes
}

// getFileWithBucketFallback tries the inferred bucket and, on failure, the alternative bucket.
func getFileWithBucketFallback(ctx context.Context, m repository.ObjectStorage, primaryBucket, path string) ([]byte, error) {
	data, err := m.GetFile(ctx, primaryBucket, path)
	if err == nil {
		return data, nil
	}
	// decide alternative bucket
	alt := config.Config.Minio.BucketName
	if primaryBucket == alt {
		alt = repository.BlobBucketName
	}
	return m.GetFile(ctx, alt, path)
}
