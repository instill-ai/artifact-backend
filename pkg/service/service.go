package service

import (
	"context"

	"github.com/redis/go-redis/v9"

	"github.com/instill-ai/artifact-backend/pkg/acl"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/resource"
	"github.com/instill-ai/artifact-backend/pkg/types"
	"github.com/instill-ai/artifact-backend/pkg/worker"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	mgmtpb "github.com/instill-ai/protogen-go/core/mgmt/v1beta"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
)

// FileContent is an alias to worker.FileContent for backwards compatibility
type FileContent = worker.FileContent

// Service defines the Artifact domain use cases.
type Service interface {
	CheckNamespacePermission(context.Context, *resource.Namespace) error
	SimilarityChunksSearch(context.Context, types.OwnerUIDType, *artifactpb.SimilarityChunksSearchRequest, [][]float32) ([]SimChunk, error)
	GetNamespaceByNsID(context.Context, string) (*resource.Namespace, error)
	GetChunksByFile(context.Context, *repository.KnowledgeBaseFileModel) (types.SourceTableType, types.SourceUIDType, []repository.TextChunkModel, []string, error)
	GetConvertedFilePathsByFileUID(context.Context, types.KBUIDType, types.FileUIDType) ([]string, error)
	GetTextChunkFilePathsByFileUID(context.Context, types.KBUIDType, types.FileUIDType) ([]string, error)
	ListRepositoryTags(context.Context, *artifactpb.ListRepositoryTagsRequest) (*artifactpb.ListRepositoryTagsResponse, error)
	CreateRepositoryTag(context.Context, *artifactpb.CreateRepositoryTagRequest) (*artifactpb.CreateRepositoryTagResponse, error)
	GetRepositoryTag(context.Context, *artifactpb.GetRepositoryTagRequest) (*artifactpb.GetRepositoryTagResponse, error)
	DeleteRepositoryTag(context.Context, *artifactpb.DeleteRepositoryTagRequest) (*artifactpb.DeleteRepositoryTagResponse, error)
	GetUploadURL(context.Context, *artifactpb.GetObjectUploadURLRequest, types.NamespaceUIDType, string, types.CreatorUIDType) (*artifactpb.GetObjectUploadURLResponse, error)
	GetDownloadURL(context.Context, *artifactpb.GetObjectDownloadURLRequest, types.NamespaceUIDType, string) (*artifactpb.GetObjectDownloadURLResponse, error)
	CheckCatalogUserPermission(context.Context, string, string, string) (*resource.Namespace, *repository.KnowledgeBaseModel, error)
	GetNamespaceAndCheckPermission(context.Context, string) (*resource.Namespace, error)

	// Worker orchestration use cases (abstracted from worker package)
	ProcessFile(context.Context, types.KBUIDType, []types.FileUIDType, types.UserUIDType, types.RequesterUIDType) error
	CleanupFile(context.Context, types.FileUIDType, types.UserUIDType, types.RequesterUIDType, string, bool) error
	CleanupKnowledgeBase(context.Context, types.KBUIDType) error
	GetFilesByPaths(context.Context, string, []string) ([]FileContent, error)
	DeleteFiles(context.Context, string, []string) error
	EmbedTexts(context.Context, *types.KBUIDType, []string, string) ([][]float32, error)

	// Chat cache use cases (for instant chat during file processing)
	GetChatCacheForFiles(context.Context, types.KBUIDType, []types.FileUIDType) (*repository.ChatCacheMetadata, error)
	ChatWithCache(context.Context, *repository.ChatCacheMetadata, string) (string, error)
	CheckFilesProcessingStatus(context.Context, []types.FileUIDType) (allCompleted bool, processingCount int, err error)

	// TODO instead of exposing these dependencies, Service should expose use
	// cases. We're drawing a line for now here in the refactor and take this
	// as valid in order to move forward with new features, but we should avoid
	// using these methods in them.
	Repository() repository.Repository
	ACLClient() *acl.ACLClient
	RedisClient() *redis.Client
	PipelinePublicClient() pipelinepb.PipelinePublicServiceClient
}

type service struct {
	repository     repository.Repository
	mgmtPrv        mgmtpb.MgmtPrivateServiceClient
	pipelinePub    pipelinepb.PipelinePublicServiceClient
	registryClient RegistryClient
	redisClient    *redis.Client
	aclClient      *acl.ACLClient
	worker         Worker
}

// NewService initiates a service instance
func NewService(
	r repository.Repository,
	mgmtPrv mgmtpb.MgmtPrivateServiceClient,
	pipelinePub pipelinepb.PipelinePublicServiceClient,
	rgc RegistryClient,
	rc *redis.Client,
	aclClient *acl.ACLClient,
	w Worker,
) Service {
	return &service{
		repository:     r,
		mgmtPrv:        mgmtPrv,
		pipelinePub:    pipelinePub,
		registryClient: rgc,
		redisClient:    rc,
		aclClient:      aclClient,
		worker:         w,
	}
}

func (s *service) Repository() repository.Repository                            { return s.repository }
func (s *service) ACLClient() *acl.ACLClient                                    { return s.aclClient }
func (s *service) PipelinePublicClient() pipelinepb.PipelinePublicServiceClient { return s.pipelinePub }
func (s *service) RedisClient() *redis.Client                                   { return s.redisClient }

// ProcessFile orchestrates the file processing workflow for one or more files
func (s *service) ProcessFile(ctx context.Context, kbUID types.KBUIDType, fileUIDs []types.FileUIDType, userUID, requesterUID types.RequesterUIDType) error {
	return s.worker.ProcessFile(ctx, kbUID, fileUIDs, userUID, requesterUID)
}

// CleanupFile cleans up a file using workflow
func (s *service) CleanupFile(ctx context.Context, fileUID types.FileUIDType, userUID, requesterUID types.RequesterUIDType, workflowID string, includeOriginalFile bool) error {
	return s.worker.CleanupFile(ctx, fileUID, userUID, requesterUID, workflowID, includeOriginalFile)
}

// GetFilesByPaths retrieves files by their paths using workflow
func (s *service) GetFilesByPaths(ctx context.Context, bucket string, filePaths []string) ([]FileContent, error) {
	return s.worker.GetFilesByPaths(ctx, bucket, filePaths)
}

// DeleteFiles deletes files using workflow
func (s *service) DeleteFiles(ctx context.Context, bucket string, filePaths []string) error {
	return s.worker.DeleteFiles(ctx, bucket, filePaths)
}

// CleanupKnowledgeBase cleans up a knowledge base using workflow
func (s *service) CleanupKnowledgeBase(ctx context.Context, kbUID types.KBUIDType) error {
	return s.worker.CleanupKnowledgeBase(ctx, kbUID)
}

// EmbedTexts embeds texts with a specific task type optimization
// kbUID is optional and used to select the appropriate provider based on KB's embedding config
func (s *service) EmbedTexts(ctx context.Context, kbUID *types.KBUIDType, texts []string, taskType string) ([][]float32, error) {
	return s.worker.EmbedTexts(ctx, kbUID, texts, taskType)
}
