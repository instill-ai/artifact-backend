package worker

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/gojuno/minimock/v3"
	"go.uber.org/zap"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"
	"github.com/instill-ai/artifact-backend/pkg/worker/mock"

	miniox "github.com/instill-ai/x/minio"
	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
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

	mockStorage := mock.NewStorageMock(mc)
	mockStorage.DeleteFileMock.Return(nil)

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetFilesByFileUIDsMock.
		When(minimock.AnyContext, []uuid.UUID{fileUID}).
		Then([]repository.FileModel{
			{UID: fileUID, StoragePath: destination},
		}, nil)
	mockRepository.GetMinIOStorageMock.Return(mockStorage)
	mockRepository.DeleteObjectByStoragePathMock.Return(nil)

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
	mockRepository.GetFilesByFileUIDsMock.
		When(minimock.AnyContext, []uuid.UUID{fileUID}).
		Then([]repository.FileModel{}, nil)

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
	mockRepository.GetFilesByFileUIDsMock.
		When(minimock.AnyContext, []uuid.UUID{fileUID}).
		Then([]repository.FileModel{
			{UID: fileUID, StoragePath: ""}, // No destination
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

	mockStorage := mock.NewStorageMock(mc)
	mockStorage.ListConvertedFilesByFileUIDMock.Return([]string{}, nil)

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetAllConvertedFilesByFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then([]repository.ConvertedFileModel{
			{
				UID:              convertedFileUID,
				KnowledgeBaseUID: kbUID,
			},
		}, nil)

	mockRepository.HardDeleteConvertedFileByFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then(nil)
	mockRepository.GetMinIOStorageMock.Return(mockStorage)

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
	mockRepository.GetAllConvertedFilesByFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then([]repository.ConvertedFileModel{}, nil)

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

	mockStorage := mock.NewStorageMock(mc)
	mockStorage.ListTextChunksByFileUIDMock.
		When(minimock.AnyContext, kbUID, fileUID).
		Then([]string{"chunk-path-1"}, nil)
	mockStorage.DeleteFileMock.Return(nil)

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.ListTextChunksByKBFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then([]repository.ChunkModel{
			{UID: textChunkUID, KnowledgeBaseUID: kbUID},
		}, nil)

	mockRepository.GetMinIOStorageMock.Return(mockStorage)

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

	activeCollectionUID := types.KBUIDType(uuid.Must(uuid.NewV4()))

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.ListEmbeddingsByKBFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then([]repository.EmbeddingModel{
			{UID: embeddingUID, KnowledgeBaseUID: kbUID},
		}, nil)

	// Mock for getting active_collection_uid
	mockRepository.GetKnowledgeBaseByUIDMock.
		When(minimock.AnyContext, types.KBUIDType(kbUID)).
		Then(&repository.KnowledgeBaseModel{
			UID:                 types.KBUIDType(kbUID),
			ActiveCollectionUID: activeCollectionUID,
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
	activeCollectionUID := types.KBUIDType(uuid.Must(uuid.NewV4()))

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.ListEmbeddingsByKBFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then([]repository.EmbeddingModel{
			{UID: embeddingUID, KnowledgeBaseUID: kbUID},
		}, nil)

	// Mock for getting active_collection_uid
	mockRepository.GetKnowledgeBaseByUIDMock.
		When(minimock.AnyContext, types.KBUIDType(kbUID)).
		Then(&repository.KnowledgeBaseModel{
			UID:                 types.KBUIDType(kbUID),
			ActiveCollectionUID: activeCollectionUID,
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
	activeCollectionUID := uuid.Must(uuid.NewV4())

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetKnowledgeBaseByUIDIncludingDeletedMock.Return(&repository.KnowledgeBaseModel{
		UID:                 kbUID,
		ActiveCollectionUID: activeCollectionUID,
	}, nil)
	mockRepository.IsCollectionInUseMock.Return(false, nil)
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
	activeCollectionUID := uuid.Must(uuid.NewV4())

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetKnowledgeBaseByUIDIncludingDeletedMock.Return(&repository.KnowledgeBaseModel{
		UID:                 kbUID,
		ActiveCollectionUID: activeCollectionUID,
	}, nil)
	mockRepository.IsCollectionInUseMock.Return(false, nil)
	mockRepository.DropCollectionMock.Return(fmt.Errorf("can't find collection"))

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	param := &DropVectorDBCollectionActivityParam{
		KBUID: kbUID,
	}

	// Should not error when collection doesn't exist (already dropped)
	err := w.DropVectorDBCollectionActivity(ctx, param)
	c.Assert(err, qt.IsNil)
}

// ===== DeleteOldConvertedFilesActivity Orphan Cleanup Tests =====

func TestDeleteOldConvertedFilesActivity_NoConvertedFiles(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	config.Config.Minio = miniox.Config{BucketName: "test-bucket"}

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetAllConvertedFilesByFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then([]repository.ConvertedFileModel{}, nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	param := &DeleteOldConvertedFilesActivityParam{
		FileUID: fileUID,
	}

	err := w.DeleteOldConvertedFilesActivity(ctx, param)
	c.Assert(err, qt.IsNil)
}

func TestDeleteOldConvertedFilesActivity_SingleConvertedFileNoChunks(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	config.Config.Minio = miniox.Config{BucketName: "test-bucket"}

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())
	convertedFileUID := uuid.Must(uuid.NewV4())

	mockStorage := mock.NewStorageMock(mc)
	mockStorage.DeleteFileMock.Return(nil)

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetAllConvertedFilesByFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then([]repository.ConvertedFileModel{
			{
				UID:           convertedFileUID,
				ConvertedType: artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_CONTENT.String(),
				StoragePath:   "kb/file/converted-file/content.md",
			},
		}, nil)
	mockRepository.GetTextChunksBySourceMock.Return([]repository.ChunkModel{}, nil)
	mockRepository.GetMinIOStorageMock.Return(mockStorage)
	mockRepository.DeleteConvertedFileMock.Return(nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	param := &DeleteOldConvertedFilesActivityParam{
		FileUID: fileUID,
	}

	err := w.DeleteOldConvertedFilesActivity(ctx, param)
	c.Assert(err, qt.IsNil)

	// 1 converted file blob deleted
	c.Assert(mockStorage.DeleteFileAfterCounter(), qt.Equals, uint64(1))
}

func TestDeleteOldConvertedFilesActivity_ConvertedFileWithChunks(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	config.Config.Minio = miniox.Config{BucketName: "test-bucket"}

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())
	convertedFileUID := uuid.Must(uuid.NewV4())
	chunk1UID := uuid.Must(uuid.NewV4())
	chunk2UID := uuid.Must(uuid.NewV4())
	chunk3UID := uuid.Must(uuid.NewV4())

	dbChunkPaths := []string{
		"kb/file/chunk/" + chunk1UID.String() + ".txt",
		"kb/file/chunk/" + chunk2UID.String() + ".txt",
		"kb/file/chunk/" + chunk3UID.String() + ".txt",
	}

	var deleteCount atomic.Int64
	mockStorage := mock.NewStorageMock(mc)
	mockStorage.DeleteFileMock.Set(func(_ context.Context, _ string, _ string) error {
		deleteCount.Add(1)
		return nil
	})

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetAllConvertedFilesByFileUIDMock.
		When(minimock.AnyContext, fileUID).
		Then([]repository.ConvertedFileModel{
			{
				UID:           convertedFileUID,
				ConvertedType: artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_CONTENT.String(),
				StoragePath:   "kb/file/converted-file/content.md",
			},
		}, nil)
	mockRepository.GetTextChunksBySourceMock.Return([]repository.ChunkModel{
		{UID: chunk1UID, StoragePath: dbChunkPaths[0]},
		{UID: chunk2UID, StoragePath: dbChunkPaths[1]},
		{UID: chunk3UID, StoragePath: dbChunkPaths[2]},
	}, nil)
	mockRepository.GetMinIOStorageMock.Return(mockStorage)
	mockRepository.DeleteConvertedFileMock.Return(nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	param := &DeleteOldConvertedFilesActivityParam{
		FileUID: fileUID,
	}

	err := w.DeleteOldConvertedFilesActivity(ctx, param)
	c.Assert(err, qt.IsNil)

	// 3 DB chunk blobs + 1 converted file blob = 4 total deletes
	c.Assert(deleteCount.Load(), qt.Equals, int64(4))
}
