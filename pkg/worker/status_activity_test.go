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

func TestUpdateFileStatusActivityParam_Validation(t *testing.T) {
	c := qt.New(t)
	fileUID := uuid.Must(uuid.NewV4())

	tests := []struct {
		name  string
		param *UpdateFileStatusActivityParam
	}{
		{
			name: "Valid status update",
			param: &UpdateFileStatusActivityParam{
				FileUID: fileUID,
				Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED,
				Message: "Success",
			},
		},
		{
			name: "Failed status with message",
			param: &UpdateFileStatusActivityParam{
				FileUID: fileUID,
				Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_FAILED,
				Message: "Error occurred",
			},
		},
		{
			name: "Empty message",
			param: &UpdateFileStatusActivityParam{
				FileUID: fileUID,
				Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING,
				Message: "",
			},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(tt.param.FileUID, qt.Not(qt.Equals), uuid.Nil)
			c.Assert(tt.param.Status, qt.Not(qt.Equals), artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED)
		})
	}
}

func TestFileProcessStatus_Values(t *testing.T) {
	c := qt.New(t)

	statuses := []artifactpb.FileProcessStatus{
		artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CONVERTING,
		artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_SUMMARIZING,
		artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING,
		artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING,
		artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED,
		artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_FAILED,
	}

	for _, status := range statuses {
		c.Run(status.String(), func(c *qt.C) {
			c.Assert(status.String(), qt.Not(qt.Equals), "")
			c.Assert(status, qt.Not(qt.Equals), artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED)
		})
	}
}

func TestGetFileStatusActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())

	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.GetKnowledgeBaseFilesByFileUIDsMock.
		When(minimock.AnyContext, []uuid.UUID{fileUID}).
		Then([]repository.KnowledgeBaseFile{
			{
				UID:           fileUID,
				ProcessStatus: artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED.String(),
			},
		}, nil)

	mockService := &mockStatusServiceWrapper{repo: mockRepo}

	w := &Worker{
		service: mockService,
		log:     zap.NewNop(),
	}

	status, err := w.GetFileStatusActivity(ctx, fileUID)
	c.Assert(err, qt.IsNil)
	c.Assert(status, qt.Equals, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED)
}

func TestGetFileStatusActivity_FileNotFound(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())

	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.GetKnowledgeBaseFilesByFileUIDsMock.
		When(minimock.AnyContext, []uuid.UUID{fileUID}).
		Then([]repository.KnowledgeBaseFile{}, nil)

	mockService := &mockStatusServiceWrapper{repo: mockRepo}

	w := &Worker{
		service: mockService,
		log:     zap.NewNop(),
	}

	status, err := w.GetFileStatusActivity(ctx, fileUID)
	c.Assert(err, qt.ErrorMatches, ".*File not found.*")
	c.Assert(status, qt.Equals, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED)
}

func TestGetFileStatusActivity_DatabaseError(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())

	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.GetKnowledgeBaseFilesByFileUIDsMock.
		When(minimock.AnyContext, []uuid.UUID{fileUID}).
		Then(nil, fmt.Errorf("database error"))

	mockService := &mockStatusServiceWrapper{repo: mockRepo}

	w := &Worker{
		service: mockService,
		log:     zap.NewNop(),
	}

	status, err := w.GetFileStatusActivity(ctx, fileUID)
	c.Assert(err, qt.ErrorMatches, ".*Failed to get file.*")
	c.Assert(status, qt.Equals, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED)
}

func TestUpdateFileStatusActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())

	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.GetKnowledgeBaseFilesByFileUIDsMock.
		When(minimock.AnyContext, []uuid.UUID{fileUID}).
		Then([]repository.KnowledgeBaseFile{
			{UID: fileUID, ProcessStatus: "FILE_PROCESS_STATUS_WAITING"},
		}, nil)

	updateCalled := false
	mockRepo.UpdateKnowledgeBaseFileMock.
		Inspect(func(ctx context.Context, uid string, updateMap map[string]any) {
			updateCalled = true
			c.Check(uid, qt.Equals, fileUID.String())
		}).
		Return(&repository.KnowledgeBaseFile{}, nil)

	mockService := &mockStatusServiceWrapper{repo: mockRepo}

	w := &Worker{
		service: mockService,
		log:     zap.NewNop(),
	}

	param := &UpdateFileStatusActivityParam{
		FileUID: fileUID,
		Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED,
		Message: "",
	}

	err := w.UpdateFileStatusActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	c.Assert(updateCalled, qt.IsTrue)
}

func TestUpdateFileStatusActivity_WithMessage(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())
	failMessage := "Conversion failed: invalid file format"

	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.GetKnowledgeBaseFilesByFileUIDsMock.
		When(minimock.AnyContext, []uuid.UUID{fileUID}).
		Then([]repository.KnowledgeBaseFile{
			{UID: fileUID, ProcessStatus: "FILE_PROCESS_STATUS_CONVERTING"},
		}, nil)

	metadataUpdateCalled := false
	mockRepo.UpdateKBFileMetadataMock.
		Inspect(func(ctx context.Context, fuid uuid.UUID, metadata repository.ExtraMetaData) {
			metadataUpdateCalled = true
			c.Check(fuid, qt.Equals, fileUID)
			c.Check(metadata.FailReason, qt.Equals, failMessage)
		}).
		Return(nil)

	statusUpdateCalled := false
	mockRepo.UpdateKnowledgeBaseFileMock.
		Inspect(func(ctx context.Context, uid string, updateMap map[string]any) {
			statusUpdateCalled = true
			c.Check(uid, qt.Equals, fileUID.String())
		}).
		Return(&repository.KnowledgeBaseFile{}, nil)

	mockService := &mockStatusServiceWrapper{repo: mockRepo}

	w := &Worker{
		service: mockService,
		log:     zap.NewNop(),
	}

	param := &UpdateFileStatusActivityParam{
		FileUID: fileUID,
		Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_FAILED,
		Message: failMessage,
	}

	err := w.UpdateFileStatusActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	c.Assert(metadataUpdateCalled, qt.IsTrue)
	c.Assert(statusUpdateCalled, qt.IsTrue)
}

func TestUpdateFileStatusActivity_FileDeleted(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())

	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.GetKnowledgeBaseFilesByFileUIDsMock.
		When(minimock.AnyContext, []uuid.UUID{fileUID}).
		Then([]repository.KnowledgeBaseFile{}, nil) // File was deleted

	mockService := &mockStatusServiceWrapper{repo: mockRepo}

	w := &Worker{
		service: mockService,
		log:     zap.NewNop(),
	}

	param := &UpdateFileStatusActivityParam{
		FileUID: fileUID,
		Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED,
		Message: "",
	}

	// Should not error when file is deleted (graceful handling)
	err := w.UpdateFileStatusActivity(ctx, param)
	c.Assert(err, qt.IsNil)
}

// mockStatusServiceWrapper is a simple mock for service.Service
type mockStatusServiceWrapper struct {
	repo repository.RepositoryI
}

func (m *mockStatusServiceWrapper) Repository() repository.RepositoryI               { return m.repo }
func (m *mockStatusServiceWrapper) MinIO() minio.MinioI                              { return nil }
func (m *mockStatusServiceWrapper) ACLClient() *acl.ACLClient                        { return nil }
func (m *mockStatusServiceWrapper) VectorDB() service.VectorDatabase                 { return nil }
func (m *mockStatusServiceWrapper) RedisClient() *redis.Client                       { return nil }
func (m *mockStatusServiceWrapper) ProcessFileWorkflow() service.ProcessFileWorkflow { return nil }
func (m *mockStatusServiceWrapper) CleanupFileWorkflow() service.CleanupFileWorkflow { return nil }
func (m *mockStatusServiceWrapper) CleanupKnowledgeBaseWorkflow() service.CleanupKnowledgeBaseWorkflow {
	return nil
}
func (m *mockStatusServiceWrapper) EmbedTextsWorkflow() service.EmbedTextsWorkflow   { return nil }
func (m *mockStatusServiceWrapper) DeleteFilesWorkflow() service.DeleteFilesWorkflow { return nil }
func (m *mockStatusServiceWrapper) GetFilesWorkflow() service.GetFilesWorkflow       { return nil }
func (m *mockStatusServiceWrapper) CheckNamespacePermission(context.Context, *resource.Namespace) error {
	return nil
}
func (m *mockStatusServiceWrapper) ConvertToMDPipe(context.Context, service.MDConversionParams) (*service.MDConversionResult, error) {
	return nil, nil
}
func (m *mockStatusServiceWrapper) GenerateSummary(context.Context, string, string) (string, error) {
	return "", nil
}
func (m *mockStatusServiceWrapper) ChunkMarkdownPipe(context.Context, string) (*service.ChunkingResult, error) {
	return nil, nil
}
func (m *mockStatusServiceWrapper) ChunkTextPipe(context.Context, string) (*service.ChunkingResult, error) {
	return nil, nil
}
func (m *mockStatusServiceWrapper) EmbeddingTextBatch(context.Context, []string) ([][]float32, error) {
	return nil, nil
}
func (m *mockStatusServiceWrapper) EmbeddingTextPipe(context.Context, []string) ([][]float32, error) {
	return nil, nil
}
func (m *mockStatusServiceWrapper) QuestionAnsweringPipe(context.Context, string, []string) (string, error) {
	return "", nil
}
func (m *mockStatusServiceWrapper) SimilarityChunksSearch(context.Context, uuid.UUID, *artifactpb.SimilarityChunksSearchRequest) ([]service.SimChunk, error) {
	return nil, nil
}
func (m *mockStatusServiceWrapper) GetNamespaceByNsID(context.Context, string) (*resource.Namespace, error) {
	return nil, nil
}
func (m *mockStatusServiceWrapper) GetChunksByFile(context.Context, *repository.KnowledgeBaseFile) (service.SourceTableType, service.SourceIDType, []repository.TextChunk, map[service.ChunkUIDType]service.ContentType, []string, error) {
	return service.SourceTableType(""), uuid.Nil, nil, nil, nil, nil
}
func (m *mockStatusServiceWrapper) GetFilesByPaths(context.Context, string, []string) ([]service.FileContent, error) {
	return nil, nil
}
func (m *mockStatusServiceWrapper) DeleteFiles(context.Context, string, []string) error { return nil }
func (m *mockStatusServiceWrapper) DeleteFilesWithPrefix(context.Context, string, string) error {
	return nil
}
func (m *mockStatusServiceWrapper) DeleteKnowledgeBase(context.Context, string) error { return nil }
func (m *mockStatusServiceWrapper) DeleteConvertedFileByFileUID(context.Context, uuid.UUID, uuid.UUID) error {
	return nil
}
func (m *mockStatusServiceWrapper) DeleteTextChunksByFileUID(context.Context, uuid.UUID, uuid.UUID) error {
	return nil
}
func (m *mockStatusServiceWrapper) TriggerCleanupKnowledgeBaseWorkflow(context.Context, string) error {
	return nil
}
func (m *mockStatusServiceWrapper) ListRepositoryTags(context.Context, *artifactpb.ListRepositoryTagsRequest) (*artifactpb.ListRepositoryTagsResponse, error) {
	return nil, nil
}
func (m *mockStatusServiceWrapper) CreateRepositoryTag(context.Context, *artifactpb.CreateRepositoryTagRequest) (*artifactpb.CreateRepositoryTagResponse, error) {
	return nil, nil
}
func (m *mockStatusServiceWrapper) GetRepositoryTag(context.Context, *artifactpb.GetRepositoryTagRequest) (*artifactpb.GetRepositoryTagResponse, error) {
	return nil, nil
}
func (m *mockStatusServiceWrapper) DeleteRepositoryTag(context.Context, *artifactpb.DeleteRepositoryTagRequest) (*artifactpb.DeleteRepositoryTagResponse, error) {
	return nil, nil
}
func (m *mockStatusServiceWrapper) GetUploadURL(context.Context, *artifactpb.GetObjectUploadURLRequest, uuid.UUID, string, uuid.UUID) (*artifactpb.GetObjectUploadURLResponse, error) {
	return nil, nil
}
func (m *mockStatusServiceWrapper) GetDownloadURL(context.Context, *artifactpb.GetObjectDownloadURLRequest, uuid.UUID, string) (*artifactpb.GetObjectDownloadURLResponse, error) {
	return nil, nil
}
func (m *mockStatusServiceWrapper) CheckCatalogUserPermission(context.Context, string, string, string) (*resource.Namespace, *repository.KnowledgeBase, error) {
	return nil, nil, nil
}
func (m *mockStatusServiceWrapper) GetNamespaceAndCheckPermission(context.Context, string) (*resource.Namespace, error) {
	return nil, nil
}
