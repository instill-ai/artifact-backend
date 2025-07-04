package handler

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/instill-ai/artifact-backend/pkg/errors"
	"github.com/instill-ai/artifact-backend/pkg/minio"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/x/log"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

func (ph *PublicHandler) GetFileCatalog(ctx context.Context, req *artifactpb.GetFileCatalogRequest) (*artifactpb.GetFileCatalogResponse, error) {
	log, _ := log.GetZapLogger(ctx)

	fileUID := uuid.FromStringOrNil(req.FileUid)

	// get kbFile by kbFile uid or catalog uid and kbFile id(name)
	var kbFile *repository.KnowledgeBaseFile
	if fileUID == uuid.Nil {
		// use catalog id and file id to get kbFile
		fileID := req.FileId
		if fileID == "" {
			return nil, fmt.Errorf("%w: need either file UID or file ID", errors.ErrInvalidArgument)
		}

		ns, err := ph.service.GetNamespaceByNsID(ctx, req.NamespaceId)
		if err != nil {
			return nil, fmt.Errorf("fetching namespace: %w", err)
		}
		kb, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, req.CatalogId)
		if err != nil {
			return nil, fmt.Errorf("fetching catalog: %w", err)
		}

		kbFile, err = ph.service.Repository().GetKnowledgebaseFileByKbUIDAndFileID(ctx, kb.UID, fileID)
		if err != nil {
			return nil, fmt.Errorf("fetching file: %w", err)
		}
	} else {
		// use file uid to get kbFile
		kbfs, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{fileUID})
		if err != nil {
			return nil, fmt.Errorf("fetching file from repository: %w", err)
		} else if len(kbfs) == 0 {
			return nil, fmt.Errorf("fetching file from repository: %w", errors.ErrNotFound)
		}
		kbFile = &(kbfs[0])
	}

	// ACL - check if the user(uid from context) has access to the knowledge base of source file.
	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", kbFile.KnowledgeBaseUID, "reader")
	if err != nil {
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err)
	}
	if !granted {
		return nil, fmt.Errorf("%w: no permission over catalog", errors.ErrUnauthorized)
	}

	// get source file
	source, err := ph.service.Repository().GetTruthSourceByFileUID(ctx, kbFile.UID)
	if err != nil {
		return nil, fmt.Errorf("fetching truth source: %w", err)
	}

	// get the source file sourceContent from minIO using dest of source
	sourceContent, err := ph.service.MinIO().GetFile(ctx, minio.KnowledgeBaseBucketName, source.Dest)
	if err != nil {
		return nil, fmt.Errorf("getting file from blob storage: %w", err)
	}

	// get chunks
	// NOTE: in the future, we may support other types of segment, e.g. image, audio, etc.
	_, _, textChunks, chunkUIDToContent, _, err := ph.service.GetChunksByFile(ctx, kbFile)
	if err != nil {
		return nil, fmt.Errorf("fetching file chunks: %w", err)
	}

	pbChunks := make([]*artifactpb.GetFileCatalogResponse_Chunk, 0, len(textChunks))

	// get embeddings
	embeddings, err := ph.service.Repository().ListEmbeddingsByKbFileUID(ctx, kbFile.UID)
	if err != nil {
		return nil, fmt.Errorf("getting file embeddings: %w", err)
	}
	// map chunks to embeddings
	embeddingMap := make(map[uuid.UUID]repository.Embedding)

	// NOTE: in the future if we support embeddings for other types of source, we need to filter here
	targetSourceTable := ph.service.Repository().TextChunkTableName()
	for _, embedding := range embeddings {
		if embedding.SourceTable != targetSourceTable {
			continue
		}
		// map chunk uid to embedding
		embeddingMap[embedding.SourceUID] = embedding
	}

	for _, chunk := range textChunks {
		log := log.With(zap.String("chunkUID", chunk.UID.String()))
		embedding, ok := embeddingMap[chunk.UID]
		if !ok {
			log.Error("Couldn't find embedding for chunk")
		}
		content, ok := chunkUIDToContent[chunk.UID]
		if !ok {
			log.Error("Couldn't find content for chunk")
		}
		var createTime *timestamppb.Timestamp = nil
		if chunk.CreateTime != nil {
			createTime = timestamppb.New(*chunk.CreateTime)
		}
		pbChunks = append(pbChunks, &artifactpb.GetFileCatalogResponse_Chunk{
			Uid:         chunk.UID.String(),
			Type:        artifactpb.GetFileCatalogResponse_CHUNK_TYPE_TEXT,
			StartPos:    int32(chunk.StartPos),
			EndPos:      int32(chunk.EndPos),
			Content:     content,
			TokensNum:   int32(chunk.Tokens),
			Embedding:   embedding.Vector,
			CreateTime:  createTime,
			Retrievable: chunk.Retrievable,
		})
	}
	var fileCreateTime *timestamppb.Timestamp = nil
	if kbFile.CreateTime != nil {
		fileCreateTime = timestamppb.New(*kbFile.CreateTime)
	}
	pipelineIDs := getPipelineIDs(kbFile)
	var totalTokens int32 = 0
	for _, chunk := range pbChunks {
		totalTokens += chunk.TokensNum
	}

	// Retrieve the original file content from MinIO
	originalContent, err := ph.service.MinIO().GetFile(ctx, minio.KnowledgeBaseBucketName, kbFile.Destination)
	if err != nil {
		return nil, fmt.Errorf("fetching original file from blob: %w", err)
	}

	// Encode the original content to base64
	originalDataBase64 := base64.StdEncoding.EncodeToString(originalContent)

	// Add the originalData field to the response
	return &artifactpb.GetFileCatalogResponse{
		OriginalData: originalDataBase64,
		Metadata: &artifactpb.GetFileCatalogResponse_Metadata{
			FileUid:           kbFile.UID.String(),
			FileId:            kbFile.Name,
			FileType:          artifactpb.FileType(artifactpb.FileType_value[kbFile.Type]),
			FileSize:          kbFile.Size,
			FileUploadTime:    fileCreateTime,
			FileProcessStatus: artifactpb.FileProcessStatus(artifactpb.FileProcessStatus_value[kbFile.ProcessStatus]),
		},
		Text: &artifactpb.GetFileCatalogResponse_Text{
			PipelineIds:                  pipelineIDs,
			TransformedContent:           string(sourceContent),
			TransformedContentChunkNum:   int32(len(pbChunks)),
			TransformedContentTokenNum:   totalTokens,
			TransformedContentUpdateTime: fileCreateTime,
		},
		Chunks: pbChunks,
	}, nil
}

func getPipelineIDs(kbf *repository.KnowledgeBaseFile) []string {
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
