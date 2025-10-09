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

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetKnowledgeBaseFilesByFileUIDsMock.
		When(minimock.AnyContext, []uuid.UUID{fileUID}).
		Then([]repository.KnowledgeBaseFileModel{
			{UID: fileUID, ProcessStatus: artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED.String()},
		}, nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	status, err := w.GetFileStatusActivity(ctx, &GetFileStatusActivityParam{FileUID: fileUID})
	c.Assert(err, qt.IsNil)
	c.Assert(status, qt.Equals, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED)
}

func TestGetFileStatusActivity_FileNotFound(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetKnowledgeBaseFilesByFileUIDsMock.
		When(minimock.AnyContext, []uuid.UUID{fileUID}).
		Then([]repository.KnowledgeBaseFileModel{}, nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	status, err := w.GetFileStatusActivity(ctx, &GetFileStatusActivityParam{FileUID: fileUID})
	c.Assert(err, qt.ErrorMatches, ".*File not found.*")
	c.Assert(status, qt.Equals, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED)
}

func TestGetFileStatusActivity_DatabaseError(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetKnowledgeBaseFilesByFileUIDsMock.
		When(minimock.AnyContext, []uuid.UUID{fileUID}).
		Then(nil, fmt.Errorf("database error"))

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	status, err := w.GetFileStatusActivity(ctx, &GetFileStatusActivityParam{FileUID: fileUID})
	c.Assert(err, qt.ErrorMatches, ".*Unable to retrieve file status.*")
	c.Assert(status, qt.Equals, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED)
}

func TestUpdateFileStatusActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetKnowledgeBaseFilesByFileUIDsMock.
		When(minimock.AnyContext, []uuid.UUID{fileUID}).
		Then([]repository.KnowledgeBaseFileModel{
			{UID: fileUID, ProcessStatus: artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_PROCESSING.String()},
		}, nil)
	mockRepository.UpdateKnowledgeBaseFileMock.Return(&repository.KnowledgeBaseFileModel{}, nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}
	err := w.UpdateFileStatusActivity(ctx, &UpdateFileStatusActivityParam{
		FileUID: fileUID,
		Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED,
		Message: "",
	})
	c.Assert(err, qt.IsNil)
}

func TestUpdateFileStatusActivity_WithMessage(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := uuid.Must(uuid.NewV4())
	failMessage := "Conversion failed: invalid file format"

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetKnowledgeBaseFilesByFileUIDsMock.
		When(minimock.AnyContext, []uuid.UUID{fileUID}).
		Then([]repository.KnowledgeBaseFileModel{
			{UID: fileUID, ProcessStatus: "FILE_PROCESS_STATUS_CONVERTING"},
		}, nil)

	metadataUpdateCalled := false
	mockRepository.UpdateKnowledgeFileMetadataMock.
		Inspect(func(ctx context.Context, fuid uuid.UUID, metadata repository.ExtraMetaData) {
			metadataUpdateCalled = true
			c.Check(fuid, qt.Equals, fileUID)
			c.Check(metadata.FailReason, qt.Equals, failMessage)
		}).
		Return(nil)

	statusUpdateCalled := false
	mockRepository.UpdateKnowledgeBaseFileMock.
		Inspect(func(ctx context.Context, uid string, updateMap map[string]any) {
			statusUpdateCalled = true
			c.Check(uid, qt.Equals, fileUID.String())
		}).
		Return(&repository.KnowledgeBaseFileModel{}, nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

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

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetKnowledgeBaseFilesByFileUIDsMock.
		When(minimock.AnyContext, []uuid.UUID{fileUID}).
		Then([]repository.KnowledgeBaseFileModel{}, nil) // File was deleted

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	param := &UpdateFileStatusActivityParam{
		FileUID: fileUID,
		Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED,
		Message: "",
	}

	// Should not error when file is deleted (graceful handling)
	err := w.UpdateFileStatusActivity(ctx, param)
	c.Assert(err, qt.IsNil)
}
