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

// Workflow parameter types - these match the worker package types structurally

// ProcessFileWorkflowParam defines the parameters for the ProcessFileWorkflow
type ProcessFileWorkflowParam struct {
	FileUID          uuid.UUID
	KnowledgeBaseUID uuid.UUID
	UserUID          uuid.UUID
	RequesterUID     uuid.UUID
}

// CleanupFileWorkflowParam defines the parameters for the CleanupFileWorkflow
type CleanupFileWorkflowParam struct {
	FileUID             uuid.UUID
	IncludeOriginalFile bool
	UserUID             uuid.UUID
	RequesterUID        uuid.UUID
	WorkflowID          string
}

// EmbedTextsWorkflowParam defines the parameters for the EmbedTextsWorkflow
type EmbedTextsWorkflowParam struct {
	Texts           []string
	BatchSize       int
	RequestMetadata map[string][]string
}

// DeleteFilesWorkflowParam defines the parameters for the DeleteFilesWorkflow
type DeleteFilesWorkflowParam struct {
	Bucket    string
	FilePaths []string
}

// GetFilesWorkflowParam defines the parameters for the GetFilesWorkflow
type GetFilesWorkflowParam struct {
	Bucket    string
	FilePaths []string
}

// FileContent defines the content of a file
type FileContent struct {
	Index   int
	Name    string
	Content []byte
}

// CleanupKnowledgeBaseWorkflowParam defines the parameters for the CleanupKnowledgeBaseWorkflow
type CleanupKnowledgeBaseWorkflowParam struct {
	KnowledgeBaseUID uuid.UUID
}

// Workflow interfaces - these match the worker package interfaces structurally

// ProcessFileWorkflow interface
type ProcessFileWorkflow interface {
	Execute(ctx context.Context, param ProcessFileWorkflowParam) error
}

// CleanupFileWorkflow interface
type CleanupFileWorkflow interface {
	Execute(ctx context.Context, param CleanupFileWorkflowParam) error
}

// CleanupKnowledgeBaseWorkflow interface
type CleanupKnowledgeBaseWorkflow interface {
	Execute(ctx context.Context, param CleanupKnowledgeBaseWorkflowParam) error
}

// EmbedTextsWorkflow interface
type EmbedTextsWorkflow interface {
	Execute(ctx context.Context, param EmbedTextsWorkflowParam) ([][]float32, error)
}

// DeleteFilesWorkflow interface
type DeleteFilesWorkflow interface {
	Execute(ctx context.Context, param DeleteFilesWorkflowParam) error
}

// GetFilesWorkflow interface
type GetFilesWorkflow interface {
	Execute(ctx context.Context, param GetFilesWorkflowParam) ([]FileContent, error)
}

// Service defines the Artifact domain use cases.
type Service interface {
	CheckNamespacePermission(context.Context, *resource.Namespace) error
	ConvertToMarkdownPipe(context.Context, MarkdownConversionParams) (*MarkdownConversionResult, error)
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
	GetFilesByPaths(context.Context, string, []string) ([]FileContent, error)
	DeleteFiles(context.Context, string, []string) error
	DeleteFilesWithPrefix(context.Context, string, string) error
	DeleteKnowledgeBase(context.Context, string) error
	DeleteConvertedFileByFileUID(context.Context, uuid.UUID, uuid.UUID) error
	DeleteTextChunksByFileUID(context.Context, uuid.UUID, uuid.UUID) error
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
	PipelinePublicClient() pipelinepb.PipelinePublicServiceClient

	// Workflow interfaces for proper decoupling
	ProcessFileWorkflow() ProcessFileWorkflow
	CleanupFileWorkflow() CleanupFileWorkflow
	CleanupKnowledgeBaseWorkflow() CleanupKnowledgeBaseWorkflow
	EmbedTextsWorkflow() EmbedTextsWorkflow
	DeleteFilesWorkflow() DeleteFilesWorkflow
	GetFilesWorkflow() GetFilesWorkflow
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

	// Workflow implementations
	processFileWorkflow          ProcessFileWorkflow
	cleanupFileWorkflow          CleanupFileWorkflow
	cleanupKnowledgeBaseWorkflow CleanupKnowledgeBaseWorkflow
	embedTextsWorkflow           EmbedTextsWorkflow
	deleteFilesWorkflow          DeleteFilesWorkflow
	getFilesWorkflow             GetFilesWorkflow
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
	processFileWorkflow ProcessFileWorkflow,
	cleanupFileWorkflow CleanupFileWorkflow,
	cleanupKnowledgeBaseWorkflow CleanupKnowledgeBaseWorkflow,
	embedTextsWorkflow EmbedTextsWorkflow,
	deleteFilesWorkflow DeleteFilesWorkflow,
	getFilesWorkflow GetFilesWorkflow,
) Service {
	return &service{
		repository:                   r,
		minIO:                        mc,
		mgmtPrv:                      mgmtPrv,
		pipelinePub:                  pipelinePub,
		registryClient:               rgc,
		redisClient:                  rc,
		vectorDB:                     vectorDB,
		aclClient:                    aclClient,
		processFileWorkflow:          processFileWorkflow,
		cleanupFileWorkflow:          cleanupFileWorkflow,
		cleanupKnowledgeBaseWorkflow: cleanupKnowledgeBaseWorkflow,
		embedTextsWorkflow:           embedTextsWorkflow,
		deleteFilesWorkflow:          deleteFilesWorkflow,
		getFilesWorkflow:             getFilesWorkflow,
	}
}

func (s *service) Repository() repository.RepositoryI                           { return s.repository }
func (s *service) MinIO() minio.MinioI                                          { return s.minIO }
func (s *service) ACLClient() *acl.ACLClient                                    { return s.aclClient }
func (s *service) PipelinePublicClient() pipelinepb.PipelinePublicServiceClient { return s.pipelinePub }
func (s *service) RedisClient() *redis.Client                                   { return s.redisClient }
func (s *service) VectorDB() VectorDatabase                                     { return s.vectorDB }

// Workflow getters
func (s *service) ProcessFileWorkflow() ProcessFileWorkflow {
	return s.processFileWorkflow
}

func (s *service) CleanupFileWorkflow() CleanupFileWorkflow {
	return s.cleanupFileWorkflow
}

func (s *service) CleanupKnowledgeBaseWorkflow() CleanupKnowledgeBaseWorkflow {
	return s.cleanupKnowledgeBaseWorkflow
}

func (s *service) EmbedTextsWorkflow() EmbedTextsWorkflow {
	return s.embedTextsWorkflow
}

func (s *service) DeleteFilesWorkflow() DeleteFilesWorkflow {
	return s.deleteFilesWorkflow
}

func (s *service) GetFilesWorkflow() GetFilesWorkflow {
	return s.getFilesWorkflow
}
