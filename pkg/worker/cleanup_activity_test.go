package worker

import (
	"context"
	"fmt"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/gojuno/minimock/v3"
	"go.uber.org/zap"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/mock"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"
)

func TestCleanupFileWorkflowParam_Validation(t *testing.T) {
	c := qt.New(t)
	fileUID := uuid.Must(uuid.NewV4())
	userUID := uuid.Must(uuid.NewV4())

	param := service.CleanupFileWorkflowParam{
		FileUID:             fileUID,
		IncludeOriginalFile: true,
		UserUID:             userUID,
		RequesterUID:        uuid.Must(uuid.NewV4()),
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

	mockService := mock.NewServiceMock(mc)
	mockService.RepositoryMock.Return(mockRepo)
	mockService.DeleteFilesMock.
		When(minimock.AnyContext, bucket, []string{destination}).
		Then(nil)

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

	mockService := mock.NewServiceMock(mc)
	mockService.RepositoryMock.Return(mockRepo)

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

	mockService := mock.NewServiceMock(mc)
	mockService.RepositoryMock.Return(mockRepo)

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

	mockService := mock.NewServiceMock(mc)
	mockService.RepositoryMock.Return(mockRepo)
	mockService.DeleteConvertedFileByFileUIDMock.
		When(minimock.AnyContext, kbUID, fileUID).
		Then(nil)

	w := &Worker{
		service: mockService,
		log:     zap.NewNop(),
	}

	param := &DeleteConvertedFileActivityParam{
		FileUID: fileUID,
	}

	err := w.DeleteConvertedFileActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	// minimock will automatically verify that DeleteConvertedFileByFileUID was called correctly
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

	mockService := mock.NewServiceMock(mc)
	mockService.RepositoryMock.Return(mockRepo)

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

	mockService := mock.NewServiceMock(mc)
	mockService.RepositoryMock.Return(mockRepo)
	mockService.DeleteTextChunksByFileUIDMock.
		When(minimock.AnyContext, kbUID, fileUID).
		Then(nil)

	w := &Worker{
		service: mockService,
		log:     zap.NewNop(),
	}

	param := &DeleteChunksFromMinIOActivityParam{
		FileUID: fileUID,
	}

	err := w.DeleteChunksFromMinIOActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	// minimock will automatically verify that DeleteTextChunksByFileUID was called correctly
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

	mockVectorDB := mock.NewVectorDatabaseMock(mc)
	expectedCollection := service.KBCollectionName(kbUID)
	mockVectorDB.DeleteEmbeddingsWithFileUIDMock.
		When(minimock.AnyContext, expectedCollection, fileUID).
		Then(nil)

	mockService := mock.NewServiceMock(mc)
	mockService.RepositoryMock.Return(mockRepo)
	mockService.VectorDBMock.Return(mockVectorDB)

	w := &Worker{
		service: mockService,
		log:     zap.NewNop(),
	}

	param := &DeleteEmbeddingsFromVectorDBActivityParam{
		FileUID: fileUID,
	}

	err := w.DeleteEmbeddingsFromVectorDBActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	// minimock will automatically verify that VectorDB.DeleteEmbeddingsWithFileUID was called correctly
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

	mockVectorDB := mock.NewVectorDatabaseMock(mc)
	expectedCollection := service.KBCollectionName(kbUID)
	mockVectorDB.DeleteEmbeddingsWithFileUIDMock.
		When(minimock.AnyContext, expectedCollection, fileUID).
		Then(fmt.Errorf("can't find collection"))

	mockService := mock.NewServiceMock(mc)
	mockService.RepositoryMock.Return(mockRepo)
	mockService.VectorDBMock.Return(mockVectorDB)

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

	mockVectorDB := mock.NewVectorDatabaseMock(mc)
	expectedCollection := service.KBCollectionName(kbUID)
	mockVectorDB.DropCollectionMock.
		When(minimock.AnyContext, expectedCollection).
		Then(nil)

	mockService := mock.NewServiceMock(mc)
	mockService.VectorDBMock.Return(mockVectorDB)

	w := &Worker{
		service: mockService,
		log:     zap.NewNop(),
	}

	param := &DropVectorDBCollectionActivityParam{
		KnowledgeBaseUID: kbUID,
	}

	err := w.DropVectorDBCollectionActivity(ctx, param)
	c.Assert(err, qt.IsNil)
	// minimock will automatically verify that VectorDB.DropCollection was called correctly
}

func TestDropVectorDBCollectionActivity_AlreadyDropped(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	kbUID := uuid.Must(uuid.NewV4())

	mockVectorDB := mock.NewVectorDatabaseMock(mc)
	expectedCollection := service.KBCollectionName(kbUID)
	mockVectorDB.DropCollectionMock.
		When(minimock.AnyContext, expectedCollection).
		Then(fmt.Errorf("can't find collection"))

	mockService := mock.NewServiceMock(mc)
	mockService.VectorDBMock.Return(mockVectorDB)

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
