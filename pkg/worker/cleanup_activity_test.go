package worker

import (
	"context"
	"fmt"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/gojuno/minimock/v3"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/acl"
	"github.com/instill-ai/artifact-backend/pkg/minio"
	"github.com/instill-ai/artifact-backend/pkg/mock"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/resource"
	"github.com/instill-ai/artifact-backend/pkg/service"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

func TestCleanupFileWorkflowParam_Validation(t *testing.T) {
	c := qt.New(t)
	fileUID := uuid.Must(uuid.NewV4())
	userUID := uuid.Must(uuid.NewV4())

	param := service.CleanupFileWorkflowParam{
		FileUID:             fileUID,
		IncludeOriginalFile: true,
		UserUID:             userUID,
		WorkflowID:          "test-workflow-id",
	}

	c.Assert(param.FileUID, qt.Not(qt.Equals), uuid.Nil)
	c.Assert(param.UserUID, qt.Not(qt.Equals), uuid.Nil)
	c.Assert(param.WorkflowID, qt.Not(qt.Equals), "")
	c.Assert(param.IncludeOriginalFile, qt.IsTrue)
}

func TestCleanupKnowledgeBaseWorkflowParam_Validation(t *testing.T) {
	c := qt.New(t)
	kbUID := uuid.Must(uuid.NewV4())

	param := service.CleanupKnowledgeBaseWorkflowParam{
		KnowledgeBaseUID: kbUID,
	}

	c.Assert(param.KnowledgeBaseUID, qt.Not(qt.Equals), uuid.Nil)
	c.Assert(param.KnowledgeBaseUID, qt.Equals, kbUID)
}

func TestDeleteOriginalFileActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())
	bucket := "test-bucket"
	destination := "kb/file/test.pdf"

	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.GetKnowledgeBaseFilesByFileUIDsMock.
		When(minimock.AnyContext, []uuid.UUID{fileUID}).
		Then([]repository.KnowledgeBaseFile{
			{UID: fileUID, Destination: destination},
		}, nil)

	mockService := &mockServiceWrapper{
		repo: mockRepo,
		deleteFilesCalled: func(ctx context.Context, b string, paths []string) error {
			c.Check(b, qt.Equals, bucket)
			c.Check(paths, qt.DeepEquals, []string{destination})
			return nil
		},
	}

	w := &Worker{
		service: mockService,
		log:     zap.NewNop(),
	}

	param := &DeleteOriginalFileActivityParam{
		FileUID: fileUID,
		Bucket:  bucket,
	}

	err := w.DeleteOriginalFileActivity(ctx, param)
	c.Assert(err, qt.IsNil)
}

func TestDeleteOriginalFileActivity_FileNotFound(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())
	bucket := "test-bucket"

	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.GetKnowledgeBaseFilesByFileUIDsMock.
		When(minimock.AnyContext, []uuid.UUID{fileUID}).
		Then([]repository.KnowledgeBaseFile{}, nil)

	mockService := &mockServiceWrapper{repo: mockRepo}

	w := &Worker{
		service: mockService,
		log:     zap.NewNop(),
	}

	param := &DeleteOriginalFileActivityParam{
		FileUID: fileUID,
		Bucket:  bucket,
	}

	// Should not error when file not found (already deleted)
	err := w.DeleteOriginalFileActivity(ctx, param)
	c.Assert(err, qt.IsNil)
}

func TestDeleteOriginalFileActivity_NoDestination(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())
	bucket := "test-bucket"

	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.GetKnowledgeBaseFilesByFileUIDsMock.
		When(minimock.AnyContext, []uuid.UUID{fileUID}).
		Then([]repository.KnowledgeBaseFile{
			{UID: fileUID, Destination: ""}, // No destination
		}, nil)

	mockService := &mockServiceWrapper{repo: mockRepo}

	w := &Worker{
		service: mockService,
		log:     zap.NewNop(),
	}

	param := &DeleteOriginalFileActivityParam{
		FileUID: fileUID,
		Bucket:  bucket,
	}

	// Should not error when no destination (nothing to delete)
	err := w.DeleteOriginalFileActivity(ctx, param)
	c.Assert(err, qt.IsNil)
}

func TestDeleteConvertedFileActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())
	convertedFileUID := uuid.Must(uuid.NewV4())

	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.GetConvertedFileByFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then(&repository.ConvertedFile{
			UID:   convertedFileUID,
			KbUID: kbUID,
		}, nil)

	mockRepo.HardDeleteConvertedFileByFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then(nil)

	deleteCalled := false
	mockService := &mockServiceWrapper{
		repo: mockRepo,
		deleteConvertedFileCalled: func(ctx context.Context, kuid, fuid uuid.UUID) error {
			deleteCalled = true
			c.Check(kuid, qt.Equals, kbUID)
			c.Check(fuid, qt.Equals, fileUID)
			return nil
		},
	}

	w := &Worker{
		service: mockService,
		log:     zap.NewNop(),
	}

	param := &DeleteConvertedFileActivityParam{
		FileUID: fileUID,
	}

	err := w.DeleteConvertedFileActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	c.Assert(deleteCalled, qt.IsTrue)
}

func TestDeleteConvertedFileActivity_NotFound(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())

	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.GetConvertedFileByFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then(nil, fmt.Errorf("not found"))

	mockService := &mockServiceWrapper{repo: mockRepo}

	w := &Worker{
		service: mockService,
		log:     zap.NewNop(),
	}

	param := &DeleteConvertedFileActivityParam{
		FileUID: fileUID,
	}

	// Should not error when converted file not found (nothing to delete)
	err := w.DeleteConvertedFileActivity(ctx, param)
	c.Assert(err, qt.IsNil)
}

func TestDeleteChunksFromMinIOActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())
	chunkUID := uuid.Must(uuid.NewV4())

	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.ListChunksByKbFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then([]repository.TextChunk{
			{UID: chunkUID, KbUID: kbUID},
		}, nil)

	mockRepo.HardDeleteChunksByKbFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then(nil)

	deleteCalled := false
	mockService := &mockServiceWrapper{
		repo: mockRepo,
		deleteTextChunksCalled: func(ctx context.Context, kuid, fuid uuid.UUID) error {
			deleteCalled = true
			c.Check(kuid, qt.Equals, kbUID)
			c.Check(fuid, qt.Equals, fileUID)
			return nil
		},
	}

	w := &Worker{
		service: mockService,
		log:     zap.NewNop(),
	}

	param := &DeleteChunksFromMinIOActivityParam{
		FileUID: fileUID,
	}

	err := w.DeleteChunksFromMinIOActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	c.Assert(deleteCalled, qt.IsTrue)
}

func TestDeleteEmbeddingsFromVectorDBActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())
	embeddingUID := uuid.Must(uuid.NewV4())

	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.ListEmbeddingsByKbFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then([]repository.Embedding{
			{UID: embeddingUID, KbUID: kbUID},
		}, nil)

	mockRepo.HardDeleteEmbeddingsByKbFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then(nil)

	vectorDBCalled := false
	mockService := &mockServiceWrapper{
		repo: mockRepo,
		vectorDBDeleteCalled: func(ctx context.Context, collection string, fuid uuid.UUID) error {
			vectorDBCalled = true
			expectedCollection := service.KBCollectionName(kbUID)
			c.Check(collection, qt.Equals, expectedCollection)
			c.Check(fuid, qt.Equals, fileUID)
			return nil
		},
	}

	w := &Worker{
		service: mockService,
		log:     zap.NewNop(),
	}

	param := &DeleteEmbeddingsFromVectorDBActivityParam{
		FileUID: fileUID,
	}

	err := w.DeleteEmbeddingsFromVectorDBActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	c.Assert(vectorDBCalled, qt.IsTrue)
}

func TestDeleteEmbeddingsFromVectorDBActivity_CollectionNotFound(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())
	embeddingUID := uuid.Must(uuid.NewV4())

	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.ListEmbeddingsByKbFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then([]repository.Embedding{
			{UID: embeddingUID, KbUID: kbUID},
		}, nil)

	mockRepo.HardDeleteEmbeddingsByKbFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then(nil)

	mockService := &mockServiceWrapper{
		repo: mockRepo,
		vectorDBDeleteCalled: func(ctx context.Context, collection string, fuid uuid.UUID) error {
			return fmt.Errorf("can't find collection")
		},
	}

	w := &Worker{
		service: mockService,
		log:     zap.NewNop(),
	}

	param := &DeleteEmbeddingsFromVectorDBActivityParam{
		FileUID: fileUID,
	}

	// Should not error when collection not found (already cleaned up)
	err := w.DeleteEmbeddingsFromVectorDBActivity(ctx, param)
	c.Assert(err, qt.IsNil)
}

func TestDropVectorDBCollectionActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	kbUID := uuid.Must(uuid.NewV4())

	dropCalled := false
	mockService := &mockServiceWrapper{
		repo: mock.NewRepositoryIMock(mc),
		vectorDBDropCalled: func(ctx context.Context, collection string) error {
			dropCalled = true
			expectedCollection := service.KBCollectionName(kbUID)
			c.Check(collection, qt.Equals, expectedCollection)
			return nil
		},
	}

	w := &Worker{
		service: mockService,
		log:     zap.NewNop(),
	}

	param := &DropVectorDBCollectionActivityParam{
		KnowledgeBaseUID: kbUID,
	}

	err := w.DropVectorDBCollectionActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	c.Assert(dropCalled, qt.IsTrue)
}

func TestDropVectorDBCollectionActivity_AlreadyDropped(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	kbUID := uuid.Must(uuid.NewV4())

	mockService := &mockServiceWrapper{
		repo: mock.NewRepositoryIMock(mc),
		vectorDBDropCalled: func(ctx context.Context, collection string) error {
			return fmt.Errorf("can't find collection")
		},
	}

	w := &Worker{
		service: mockService,
		log:     zap.NewNop(),
	}

	param := &DropVectorDBCollectionActivityParam{
		KnowledgeBaseUID: kbUID,
	}

	// Should not error when collection doesn't exist (already dropped)
	err := w.DropVectorDBCollectionActivity(ctx, param)
	c.Assert(err, qt.IsNil)
}

// mockServiceWrapper wraps Worker's service interface methods
type mockServiceWrapper struct {
	repo                      repository.RepositoryI
	deleteFilesCalled         func(ctx context.Context, bucket string, paths []string) error
	deleteConvertedFileCalled func(ctx context.Context, kbUID, fileUID uuid.UUID) error
	deleteTextChunksCalled    func(ctx context.Context, kbUID, fileUID uuid.UUID) error
	vectorDBDeleteCalled      func(ctx context.Context, collection string, fileUID uuid.UUID) error
	vectorDBDropCalled        func(ctx context.Context, collection string) error
}

func (m *mockServiceWrapper) Repository() repository.RepositoryI { return m.repo }
func (m *mockServiceWrapper) MinIO() minio.MinioI                { return nil }
func (m *mockServiceWrapper) ACLClient() *acl.ACLClient          { return nil }
func (m *mockServiceWrapper) VectorDB() service.VectorDatabase {
	return &mockVectorDB{deleteCalled: m.vectorDBDeleteCalled, dropCalled: m.vectorDBDropCalled}
}
func (m *mockServiceWrapper) RedisClient() *redis.Client                       { return nil }
func (m *mockServiceWrapper) ProcessFileWorkflow() service.ProcessFileWorkflow { return nil }
func (m *mockServiceWrapper) CleanupFileWorkflow() service.CleanupFileWorkflow { return nil }
func (m *mockServiceWrapper) CleanupKnowledgeBaseWorkflow() service.CleanupKnowledgeBaseWorkflow {
	return nil
}
func (m *mockServiceWrapper) EmbedTextsWorkflow() service.EmbedTextsWorkflow { return nil }
func (m *mockServiceWrapper) DeleteFilesWorkflow() service.DeleteFilesWorkflow {
	return nil
}
func (m *mockServiceWrapper) GetFilesWorkflow() service.GetFilesWorkflow { return nil }

// Implement required service methods
func (m *mockServiceWrapper) CheckNamespacePermission(context.Context, *resource.Namespace) error {
	return nil
}
func (m *mockServiceWrapper) ConvertToMDPipe(context.Context, service.MDConversionParams) (*service.MDConversionResult, error) {
	return nil, nil
}
func (m *mockServiceWrapper) GenerateSummary(context.Context, string, string) (string, error) {
	return "", nil
}
func (m *mockServiceWrapper) ChunkMarkdownPipe(context.Context, string) (*service.ChunkingResult, error) {
	return nil, nil
}
func (m *mockServiceWrapper) ChunkTextPipe(context.Context, string) (*service.ChunkingResult, error) {
	return nil, nil
}
func (m *mockServiceWrapper) EmbeddingTextBatch(context.Context, []string) ([][]float32, error) {
	return nil, nil
}
func (m *mockServiceWrapper) EmbeddingTextPipe(context.Context, []string) ([][]float32, error) {
	return nil, nil
}
func (m *mockServiceWrapper) QuestionAnsweringPipe(context.Context, string, []string) (string, error) {
	return "", nil
}
func (m *mockServiceWrapper) SimilarityChunksSearch(context.Context, uuid.UUID, *artifactpb.SimilarityChunksSearchRequest) ([]service.SimChunk, error) {
	return nil, nil
}
func (m *mockServiceWrapper) GetNamespaceByNsID(context.Context, string) (*resource.Namespace, error) {
	return nil, nil
}
func (m *mockServiceWrapper) GetChunksByFile(context.Context, *repository.KnowledgeBaseFile) (service.SourceTableType, service.SourceIDType, []repository.TextChunk, map[service.ChunkUIDType]service.ContentType, []string, error) {
	return service.SourceTableType(""), uuid.Nil, nil, nil, nil, nil
}
func (m *mockServiceWrapper) GetFilesByPaths(context.Context, string, []string) ([]service.FileContent, error) {
	return nil, nil
}
func (m *mockServiceWrapper) DeleteFiles(ctx context.Context, bucket string, paths []string) error {
	if m.deleteFilesCalled != nil {
		return m.deleteFilesCalled(ctx, bucket, paths)
	}
	return nil
}
func (m *mockServiceWrapper) DeleteFilesWithPrefix(context.Context, string, string) error {
	return nil
}
func (m *mockServiceWrapper) DeleteKnowledgeBase(context.Context, string) error { return nil }
func (m *mockServiceWrapper) DeleteConvertedFileByFileUID(ctx context.Context, kbUID, fileUID uuid.UUID) error {
	if m.deleteConvertedFileCalled != nil {
		return m.deleteConvertedFileCalled(ctx, kbUID, fileUID)
	}
	return nil
}
func (m *mockServiceWrapper) DeleteTextChunksByFileUID(ctx context.Context, kbUID, fileUID uuid.UUID) error {
	if m.deleteTextChunksCalled != nil {
		return m.deleteTextChunksCalled(ctx, kbUID, fileUID)
	}
	return nil
}
func (m *mockServiceWrapper) TriggerCleanupKnowledgeBaseWorkflow(context.Context, string) error {
	return nil
}
func (m *mockServiceWrapper) ListRepositoryTags(context.Context, *artifactpb.ListRepositoryTagsRequest) (*artifactpb.ListRepositoryTagsResponse, error) {
	return nil, nil
}
func (m *mockServiceWrapper) CreateRepositoryTag(context.Context, *artifactpb.CreateRepositoryTagRequest) (*artifactpb.CreateRepositoryTagResponse, error) {
	return nil, nil
}
func (m *mockServiceWrapper) GetRepositoryTag(context.Context, *artifactpb.GetRepositoryTagRequest) (*artifactpb.GetRepositoryTagResponse, error) {
	return nil, nil
}
func (m *mockServiceWrapper) DeleteRepositoryTag(context.Context, *artifactpb.DeleteRepositoryTagRequest) (*artifactpb.DeleteRepositoryTagResponse, error) {
	return nil, nil
}
func (m *mockServiceWrapper) GetUploadURL(context.Context, *artifactpb.GetObjectUploadURLRequest, uuid.UUID, string, uuid.UUID) (*artifactpb.GetObjectUploadURLResponse, error) {
	return nil, nil
}
func (m *mockServiceWrapper) GetDownloadURL(context.Context, *artifactpb.GetObjectDownloadURLRequest, uuid.UUID, string) (*artifactpb.GetObjectDownloadURLResponse, error) {
	return nil, nil
}
func (m *mockServiceWrapper) CheckCatalogUserPermission(context.Context, string, string, string) (*resource.Namespace, *repository.KnowledgeBase, error) {
	return nil, nil, nil
}
func (m *mockServiceWrapper) GetNamespaceAndCheckPermission(context.Context, string) (*resource.Namespace, error) {
	return nil, nil
}

// mockVectorDB is a simple mock for VectorDB
type mockVectorDB struct {
	deleteCalled func(ctx context.Context, collection string, fileUID uuid.UUID) error
	dropCalled   func(ctx context.Context, collection string) error
}

func (m *mockVectorDB) CreateCollection(_ context.Context, id string) error {
	return nil
}

func (m *mockVectorDB) InsertVectorsInCollection(_ context.Context, collID string, embeddings []service.Embedding) error {
	return nil
}

func (m *mockVectorDB) DeleteEmbeddingsWithFileUID(ctx context.Context, collection string, fileUID uuid.UUID) error {
	if m.deleteCalled != nil {
		return m.deleteCalled(ctx, collection, fileUID)
	}
	return nil
}

func (m *mockVectorDB) DropCollection(ctx context.Context, collection string) error {
	if m.dropCalled != nil {
		return m.dropCalled(ctx, collection)
	}
	return nil
}

func (m *mockVectorDB) SimilarVectorsInCollection(context.Context, service.SimilarVectorSearchParam) ([][]service.SimilarEmbedding, error) {
	return nil, nil
}

func (m *mockVectorDB) CheckFileUIDMetadata(_ context.Context, collID string) (bool, error) {
	return false, nil
}
