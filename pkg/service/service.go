package service

import (
	"context"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/instill-ai/artifact-backend/pkg/acl"
	"github.com/instill-ai/artifact-backend/pkg/ai"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/resource"
	"github.com/instill-ai/artifact-backend/pkg/types"
	"github.com/instill-ai/artifact-backend/pkg/worker"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
	mgmtpb "github.com/instill-ai/protogen-go/mgmt/v1beta"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/v1beta"
)

// FileContent is an alias to worker.FileContent for backwards compatibility
type FileContent = worker.FileContent

// Service defines the Artifact domain use cases.
type Service interface {
	CheckNamespacePermission(context.Context, *resource.Namespace) error
	SearchChunks(context.Context, types.OwnerUIDType, *artifactpb.SearchChunksRequest, [][]float32) ([]SimChunk, error)
	GetNamespaceByNsID(context.Context, string) (*resource.Namespace, error)
	FetchUserByUID(context.Context, string) (*mgmtpb.User, error)
	FetchUserByID(context.Context, string) (*mgmtpb.User, error)
	FetchOwnerByNamespace(context.Context, *resource.Namespace) (*mgmtpb.Owner, error)
	GetChunksByFile(context.Context, *repository.FileModel) (types.SourceTableType, types.SourceUIDType, []repository.ChunkModel, []string, error)
	GetConvertedFilePathsByFileUID(context.Context, types.KBUIDType, types.FileUIDType) ([]string, error)
	GetTextChunkFilePathsByFileUID(context.Context, types.KBUIDType, types.FileUIDType) ([]string, error)
	// Repository tag methods removed - tag.proto was removed from protobuf definitions
	GetUploadURL(context.Context, *artifactpb.GetObjectUploadURLRequest, types.NamespaceUIDType, string, types.CreatorUIDType) (*artifactpb.GetObjectUploadURLResponse, error)
	GetObject(context.Context, types.NamespaceUIDType, string, string) (*artifactpb.Object, error)
	UpdateObject(context.Context, types.NamespaceUIDType, string, string, *artifactpb.Object, *fieldmaskpb.FieldMask) (*artifactpb.Object, error)
	GetDownloadURL(context.Context, string, types.NamespaceUIDType, string, int32, string) (*artifactpb.GetObjectDownloadURLResponse, error)
	GetDownloadURLByObjectUID(context.Context, types.ObjectUIDType, types.NamespaceUIDType, string, int32, string) (*artifactpb.GetObjectDownloadURLResponse, error)
	CheckCatalogUserPermission(context.Context, string, string, string) (*resource.Namespace, *repository.KnowledgeBaseModel, error)
	GetNamespaceAndCheckPermission(context.Context, string) (*resource.Namespace, error)

	// Worker orchestration use cases (abstracted from worker package)
	ProcessFile(context.Context, types.KBUIDType, []types.FileUIDType, types.UserUIDType, types.RequesterUIDType) error
	// ProcessFileDualMode processes a file for both production and staging KBs during an update
	ProcessFileDualMode(ctx context.Context, prodKBUID, stagingKBUID types.KBUIDType, fileUIDs []types.FileUIDType, userUID, requesterUID types.RequesterUIDType) error
	CleanupFile(context.Context, types.FileUIDType, types.UserUIDType, types.RequesterUIDType, string, bool) error
	CleanupKnowledgeBase(context.Context, types.KBUIDType) error
	GetFilesByPaths(context.Context, string, []string) ([]FileContent, error)
	DeleteFiles(context.Context, string, []string) error
	EmbedTexts(context.Context, *types.KBUIDType, []string, string) ([][]float32, error)

	// Cache management use cases
	GetOrCreateFileCache(context.Context, types.KBUIDType, types.FileUIDType, string, string, artifactpb.File_Type, string) (*repository.CacheMetadata, error)
	RenewFileCache(context.Context, types.KBUIDType, types.FileUIDType, string) (*repository.CacheMetadata, error)

	// RAG System Update use cases
	RollbackAdmin(context.Context, types.OwnerUIDType, string, string) (*artifactpb.RollbackAdminResponse, error)
	PurgeRollbackAdmin(context.Context, types.OwnerUIDType, string) (*artifactpb.PurgeRollbackAdminResponse, error)
	SetRollbackRetentionAdmin(context.Context, types.OwnerUIDType, string, int32, artifactpb.SetRollbackRetentionAdminRequest_TimeUnit) (*artifactpb.SetRollbackRetentionAdminResponse, error)
	GetKnowledgeBaseUpdateStatusAdmin(context.Context) (*artifactpb.GetKnowledgeBaseUpdateStatusAdminResponse, error)

	// RAG System Update - manual-triggered update management
	ExecuteKnowledgeBaseUpdateAdmin(context.Context, *artifactpb.ExecuteKnowledgeBaseUpdateAdminRequest) (*artifactpb.ExecuteKnowledgeBaseUpdateAdminResponse, error)
	AbortKnowledgeBaseUpdateAdmin(context.Context, *artifactpb.AbortKnowledgeBaseUpdateAdminRequest) (*artifactpb.AbortKnowledgeBaseUpdateAdminResponse, error)

	// System Management use cases (admin only)
	GetSystemAdmin(context.Context, string) (*artifactpb.GetSystemAdminResponse, error)
	CreateSystemAdmin(context.Context, *artifactpb.CreateSystemAdminRequest) (*artifactpb.CreateSystemAdminResponse, error)
	UpdateSystemAdmin(context.Context, *artifactpb.UpdateSystemAdminRequest) (*artifactpb.UpdateSystemAdminResponse, error)
	ListSystemsAdmin(context.Context) (*artifactpb.ListSystemsAdminResponse, error)
	DeleteSystemAdmin(context.Context, string) (*artifactpb.DeleteSystemAdminResponse, error)
	RenameSystemAdmin(context.Context, *artifactpb.RenameSystemAdminRequest) (*artifactpb.RenameSystemAdminResponse, error)
	SetDefaultSystemAdmin(context.Context, *artifactpb.SetDefaultSystemAdminRequest) (*artifactpb.SetDefaultSystemAdminResponse, error)
	GetDefaultSystemAdmin(context.Context, *artifactpb.GetDefaultSystemAdminRequest) (*artifactpb.GetDefaultSystemAdminResponse, error)

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
	repository  repository.Repository
	mgmtPrv     mgmtpb.MgmtPrivateServiceClient
	pipelinePub pipelinepb.PipelinePublicServiceClient
	redisClient *redis.Client
	aclClient   *acl.ACLClient
	worker      Worker
	aiClient    ai.Client
}

// NewService initiates a service instance
func NewService(
	r repository.Repository,
	mgmtPrv mgmtpb.MgmtPrivateServiceClient,
	pipelinePub pipelinepb.PipelinePublicServiceClient,
	rc *redis.Client,
	aclClient *acl.ACLClient,
	w Worker,
	aiClient ai.Client,
) Service {
	return &service{
		repository:  r,
		mgmtPrv:     mgmtPrv,
		pipelinePub: pipelinePub,
		redisClient: rc,
		aclClient:   aclClient,
		worker:      w,
		aiClient:    aiClient,
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

// ProcessFileDualMode processes a file for both production and staging KBs during an update
// This ensures files uploaded during an update are immediately queryable (in production)
// while also being ready with the new configuration after swap (in staging)
func (s *service) ProcessFileDualMode(ctx context.Context, prodKBUID, stagingKBUID types.KBUIDType, fileUIDs []types.FileUIDType, userUID, requesterUID types.RequesterUIDType) error {
	return s.worker.ProcessFileDualMode(ctx, prodKBUID, stagingKBUID, fileUIDs, userUID, requesterUID)
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
