package service

import (
	"context"

	"github.com/gofrs/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/instill-ai/artifact-backend/pkg/acl"
	"github.com/instill-ai/artifact-backend/pkg/minio"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/resource"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	mgmtpb "github.com/instill-ai/protogen-go/core/mgmt/v1beta"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
)

// Service defines the Artifact domain use cases.
type Service interface {
	CheckNamespacePermission(context.Context, *resource.Namespace) error
	ConvertToMDPipe(_ context.Context, fileUID uuid.UUID, fileBase64 string, _ artifactpb.FileType, _ []PipelineRelease) (string, error)
	GenerateSummary(_ context.Context, content, fileType string) (string, error)
	ChunkMarkdownPipe(_ context.Context, markdown string) ([]Chunk, error)
	ChunkTextPipe(_ context.Context, text string) ([]Chunk, error)
	EmbeddingTextPipe(_ context.Context, texts []string) ([][]float32, error)
	QuestionAnsweringPipe(_ context.Context, question string, simChunks []string) (string, error)
	SimilarityChunksSearch(_ context.Context, ownerUID uuid.UUID, _ *artifactpb.SimilarityChunksSearchRequest) ([]SimChunk, error)
	GetNamespaceByNsID(_ context.Context, nsID string) (*resource.Namespace, error)
	GetChunksByFile(context.Context, *repository.KnowledgeBaseFile) (SourceTableType, SourceIDType, []repository.TextChunk, map[ChunkUIDType]ContentType, []string, error)
	ListRepositoryTags(context.Context, *artifactpb.ListRepositoryTagsRequest) (*artifactpb.ListRepositoryTagsResponse, error)
	CreateRepositoryTag(context.Context, *artifactpb.CreateRepositoryTagRequest) (*artifactpb.CreateRepositoryTagResponse, error)
	GetRepositoryTag(context.Context, *artifactpb.GetRepositoryTagRequest) (*artifactpb.GetRepositoryTagResponse, error)
	DeleteRepositoryTag(context.Context, *artifactpb.DeleteRepositoryTagRequest) (*artifactpb.DeleteRepositoryTagResponse, error)
	GetUploadURL(_ context.Context, _ *artifactpb.GetObjectUploadURLRequest, namespaceUID uuid.UUID, namespaceID string, creatorUID uuid.UUID) (*artifactpb.GetObjectUploadURLResponse, error)
	GetDownloadURL(_ context.Context, _ *artifactpb.GetObjectDownloadURLRequest, namespaceUID uuid.UUID, namespaceID string) (*artifactpb.GetObjectDownloadURLResponse, error)
	CheckCatalogUserPermission(_ context.Context, nsID, catalogID, authUID string) (*resource.Namespace, *repository.KnowledgeBase, error)
	GetNamespaceAndCheckPermission(_ context.Context, nsID string) (*resource.Namespace, error)

	// TODO instead of exposing this dependencies, Service should expose use
	// cases. We're drawing a line for now here in the refactor and take this
	// as valid in order to move forward with new features, but we should avoid
	// using these methods in them.
	Repository() repository.RepositoryI
	MinIO() minio.MinioI
	ACLClient() *acl.ACLClient
	VectorDB() VectorDatabase
	RedisClient() *redis.Client
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
	}
}

func (s *service) Repository() repository.RepositoryI { return s.repository }
func (s *service) MinIO() minio.MinioI                { return s.minIO }
func (s *service) ACLClient() *acl.ACLClient          { return s.aclClient }
func (s *service) RedisClient() *redis.Client         { return s.redisClient }
func (s *service) VectorDB() VectorDatabase           { return s.vectorDB }
