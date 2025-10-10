package worker

import (
	"context"
	"fmt"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/gojuno/minimock/v3"
	"go.uber.org/zap"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/worker/mock"
)

func TestCleanupFileWorkflowParam_Validation(t *testing.T) {
	c := qt.New(t)
	fileUID := uuid.Must(uuid.NewV4())
	userUID := uuid.Must(uuid.NewV4())

	param := CleanupFileWorkflowParam{
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

	param := CleanupKnowledgeBaseWorkflowParam{
		KBUID: kbUID,
	}

	c.Assert(param.KBUID, qt.Not(qt.Equals), uuid.Nil)
	c.Assert(param.KBUID, qt.Equals, kbUID)
}

func TestDeleteOriginalFileActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())
	bucket := "test-bucket"
	destination := "kb/file/test.pdf"

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetKnowledgeBaseFilesByFileUIDsMock.
		When(minimock.AnyContext, []uuid.UUID{fileUID}).
		Then([]repository.KnowledgeBaseFileModel{
			{UID: fileUID, Destination: destination},
		}, nil)
	mockRepository.DeleteFileMock.Return(nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

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

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetKnowledgeBaseFilesByFileUIDsMock.
		When(minimock.AnyContext, []uuid.UUID{fileUID}).
		Then([]repository.KnowledgeBaseFileModel{}, nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

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

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetKnowledgeBaseFilesByFileUIDsMock.
		When(minimock.AnyContext, []uuid.UUID{fileUID}).
		Then([]repository.KnowledgeBaseFileModel{
			{UID: fileUID, Destination: ""}, // No destination
		}, nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

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

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetConvertedFileByFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then(&repository.ConvertedFileModel{
			UID:   convertedFileUID,
			KBUID: kbUID,
		}, nil)

	mockRepository.HardDeleteConvertedFileByFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then(nil)
	mockRepository.ListConvertedFilesByFileUIDMock.Return([]string{}, nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

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

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetConvertedFileByFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then(nil, fmt.Errorf("not found"))

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	param := &DeleteConvertedFileActivityParam{
		FileUID: fileUID,
	}

	// Should not error when converted file not found (nothing to delete)
	err := w.DeleteConvertedFileActivity(ctx, param)
	c.Assert(err, qt.IsNil)
}

func TestDeleteTextChunksFromMinIOActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())
	textChunkUID := uuid.Must(uuid.NewV4())

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.ListTextChunksByKBFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then([]repository.TextChunkModel{
			{UID: textChunkUID, KBUID: kbUID},
		}, nil)

	mockRepository.ListTextChunksByFileUIDMock.
		When(minimock.AnyContext, kbUID, fileUID).
		Then([]string{"chunk-path-1"}, nil)

	mockRepository.DeleteFileMock.Return(nil)

	mockRepository.HardDeleteTextChunksByKBFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then(nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	param := &DeleteTextChunksFromMinIOActivityParam{
		FileUID: fileUID,
	}

	err := w.DeleteTextChunksFromMinIOActivity(ctx, param)
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

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.ListEmbeddingsByKBFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then([]repository.EmbeddingModel{
			{UID: embeddingUID, KBUID: kbUID},
		}, nil)

	mockRepository.DeleteEmbeddingsWithFileUIDMock.Return(nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

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

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.ListEmbeddingsByKBFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then([]repository.EmbeddingModel{
			{UID: embeddingUID, KBUID: kbUID},
		}, nil)

	mockRepository.DeleteEmbeddingsWithFileUIDMock.Return(fmt.Errorf("can't find collection"))

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

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

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.DropCollectionMock.Return(nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	param := &DropVectorDBCollectionActivityParam{
		KBUID: kbUID,
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

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.DropCollectionMock.Return(fmt.Errorf("can't find collection"))

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	param := &DropVectorDBCollectionActivityParam{
		KBUID: kbUID,
	}

	// Should not error when collection doesn't exist (already dropped)
	err := w.DropVectorDBCollectionActivity(ctx, param)
	c.Assert(err, qt.IsNil)
}
