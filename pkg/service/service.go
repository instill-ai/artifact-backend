package service

import (
	"context"

	"github.com/gofrs/uuid"
	"github.com/redis/go-redis/v9"

	temporalclient "go.temporal.io/sdk/client"

	"github.com/instill-ai/artifact-backend/pkg/acl"
	"github.com/instill-ai/artifact-backend/pkg/minio"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/resource"
	"github.com/instill-ai/artifact-backend/pkg/temporal"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	mgmtpb "github.com/instill-ai/protogen-go/core/mgmt/v1beta"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
)

// Service defines the Artifact domain use cases.
type Service interface {
	CheckNamespacePermission(context.Context, *resource.Namespace) error
	ConvertToMDPipe(context.Context, MDConversionParams) (*MDConversionResult, error)
	GenerateSummary(context.Context, string, string) (string, error)
	ChunkMarkdownPipe(context.Context, string) (*ChunkingResult, error)
	ChunkTextPipe(context.Context, string) (*ChunkingResult, error)
	EmbeddingTextBatch(context.Context, []string) ([][]float32, error)
	EmbeddingTextPipe(context.Context, []string) ([][]float32, error)
	QuestionAnsweringPipe(context.Context, string, []string) (string, error)
	SimilarityChunksSearch(context.Context, uuid.UUID, *artifactpb.SimilarityChunksSearchRequest) ([]SimChunk, error)
	UpdateCatalogFileTags(context.Context, *artifactpb.UpdateCatalogFileTagsRequest) (*artifactpb.UpdateCatalogFileTagsResponse, error)
	GetNamespaceByNsID(context.Context, string) (*resource.Namespace, error)
	GetChunksByFile(context.Context, *repository.KnowledgeBaseFile) (SourceTableType, SourceIDType, []repository.TextChunk, map[ChunkUIDType]ContentType, []string, error)
	GetFilesByPaths(context.Context, string, []string) ([]temporal.FileContent, error)
	DeleteFiles(context.Context, string, []string) error
	DeleteFilesWithPrefix(context.Context, string, string) error
	DeleteKnowledgeBase(context.Context, string) error
	DeleteConvertedFileByFileUID(context.Context, uuid.UUID, uuid.UUID) error
	DeleteTextChunksByFileUID(context.Context, uuid.UUID, uuid.UUID) error
	SaveTextChunks(context.Context, uuid.UUID, uuid.UUID, map[string][]byte) (map[string]string, error)
	TriggerCleanupKnowledgeBaseWorkflow(context.Context, string) error
	ListRepositoryTags(context.Context, *artifactpb.ListRepositoryTagsRequest) (*artifactpb.ListRepositoryTagsResponse, error)
	CreateRepositoryTag(context.Context, *artifactpb.CreateRepositoryTagRequest) (*artifactpb.CreateRepositoryTagResponse, error)
	GetRepositoryTag(context.Context, *artifactpb.GetRepositoryTagRequest) (*artifactpb.GetRepositoryTagResponse, error)
	DeleteRepositoryTag(context.Context, *artifactpb.DeleteRepositoryTagRequest) (*artifactpb.DeleteRepositoryTagResponse, error)
	GetUploadURL(context.Context, *artifactpb.GetObjectUploadURLRequest, uuid.UUID, string, uuid.UUID) (*artifactpb.GetObjectUploadURLResponse, error)
	GetDownloadURL(context.Context, *artifactpb.GetObjectDownloadURLRequest, uuid.UUID, string) (*artifactpb.GetObjectDownloadURLResponse, error)
	CheckCatalogUserPermission(context.Context, string, string, string) (*resource.Namespace, *repository.KnowledgeBase, error)
	GetNamespaceAndCheckPermission(context.Context, string) (*resource.Namespace, error)

	// TODO instead of exposing this dependencies, Service should expose use
	// cases. We're drawing a line for now here in the refactor and take this
	// as valid in order to move forward with new features, but we should avoid
	// using these methods in them.
	Repository() repository.RepositoryI
	MinIO() minio.MinioI
	ACLClient() *acl.ACLClient
	VectorDB() VectorDatabase
	RedisClient() *redis.Client
	TemporalClient() temporalclient.Client
}

type service struct {
	repository     repository.RepositoryI
	minIO          minio.MinioI
	mgmtPrv        mgmtpb.MgmtPrivateServiceClient
	pipelinePub    pipelinepb.PipelinePublicServiceClient
	registryClient RegistryClient
	redisClient    *redis.Client
	vectorDB       VectorDatabase
	aclClient      *acl.ACLClient
	temporalClient temporalclient.Client
}

// NewService initiates a service instance
func NewService(
	r repository.RepositoryI,
	mc minio.MinioI,
	mgmtPrv mgmtpb.MgmtPrivateServiceClient,
	pipelinePub pipelinepb.PipelinePublicServiceClient,
	rgc RegistryClient,
	rc *redis.Client,
	vectorDB VectorDatabase,
	aclClient *acl.ACLClient,
	temporalClient temporalclient.Client,
) Service {
	return &service{
		repository:     r,
		minIO:          mc,
		mgmtPrv:        mgmtPrv,
		pipelinePub:    pipelinePub,
		registryClient: rgc,
		redisClient:    rc,
		vectorDB:       vectorDB,
		aclClient:      aclClient,
		temporalClient: temporalClient,
	}
}

func (s *service) Repository() repository.RepositoryI    { return s.repository }
func (s *service) MinIO() minio.MinioI                   { return s.minIO }
func (s *service) ACLClient() *acl.ACLClient             { return s.aclClient }
func (s *service) RedisClient() *redis.Client            { return s.redisClient }
func (s *service) VectorDB() VectorDatabase              { return s.vectorDB }
func (s *service) TemporalClient() temporalclient.Client { return s.temporalClient }
