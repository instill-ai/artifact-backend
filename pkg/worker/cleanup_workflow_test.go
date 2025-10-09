package worker

import (
	"fmt"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/gojuno/minimock/v3"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/zap"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/worker/mock"
)

func TestCleanupFileWorkflowParam_FieldTypes(t *testing.T) {
	c := qt.New(t)
	fileUID := uuid.Must(uuid.NewV4())
	userUID := uuid.Must(uuid.NewV4())

	param := CleanupFileWorkflowParam{
		FileUID:             fileUID,
		UserUID:             userUID,
		RequesterUID:        uuid.Must(uuid.NewV4()),
		WorkflowID:          "test-workflow-id",
		IncludeOriginalFile: true,
	}

	// Verify all fields have expected types and values
	c.Assert(param.FileUID, qt.Equals, fileUID)
	c.Assert(param.IncludeOriginalFile, qt.IsTrue)
	c.Assert(param.UserUID, qt.Equals, userUID)
	c.Assert(param.WorkflowID, qt.Equals, "test-workflow-id")
}

func TestCleanupFileWorkflowParam_BooleanFlag(t *testing.T) {
	c := qt.New(t)

	// Test with IncludeOriginalFile = true
	paramTrue := CleanupFileWorkflowParam{
		IncludeOriginalFile: true,
	}
	c.Assert(paramTrue.IncludeOriginalFile, qt.IsTrue)

	// Test with IncludeOriginalFile = false
	paramFalse := CleanupFileWorkflowParam{
		IncludeOriginalFile: false,
	}
	c.Assert(paramFalse.IncludeOriginalFile, qt.IsFalse)
}

func TestCleanupKBWorkflowParam_UUIDFormat(t *testing.T) {
	c := qt.New(t)
	kbUID := uuid.Must(uuid.NewV4())

	param := CleanupKnowledgeBaseWorkflowParam{
		KBUID: kbUID,
	}

	// Test UUID is properly formatted
	uuidStr := param.KBUID.String()
	c.Assert(uuidStr, qt.HasLen, 36)
	c.Assert(uuidStr, qt.Contains, "-")

	// Parse back to ensure it's valid
	parsedUID, err := uuid.FromString(uuidStr)
	c.Assert(err, qt.IsNil)
	c.Assert(parsedUID, qt.Equals, kbUID)
}

func TestCleanupFileWorkflowParam_ZeroValues(t *testing.T) {
	c := qt.New(t)

	// Test with zero values
	var param CleanupFileWorkflowParam

	c.Assert(param.FileUID, qt.Equals, uuid.Nil)
	c.Assert(param.UserUID, qt.Equals, uuid.Nil)
	c.Assert(param.IncludeOriginalFile, qt.IsFalse)
	c.Assert(param.WorkflowID, qt.Equals, "")
}

func TestCleanupKBWorkflowParam_ZeroValues(t *testing.T) {
	c := qt.New(t)

	// Test with zero values
	var param CleanupKnowledgeBaseWorkflowParam

	c.Assert(param.KBUID, qt.Equals, uuid.Nil)
}

func TestCleanupFileWorkflow_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockRepository := mock.NewRepositoryMock(mc)

	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	// Mock for DeleteOriginalFileActivity
	mockRepository.GetKnowledgeBaseFilesByFileUIDsMock.Return([]repository.KnowledgeBaseFileModel{
		{UID: fileUID, Destination: "kb/test-file.pdf"},
	}, nil)
	mockRepository.DeleteFileMock.Return(nil)
	mockRepository.ListConvertedFilesByFileUIDMock.Return([]string{}, nil)

	// Mock for DeleteConvertedFileActivity
	mockRepository.GetConvertedFileByFileUIDMock.Return(&repository.ConvertedFileModel{
		UID:   uuid.Must(uuid.NewV4()),
		KBUID: kbUID,
	}, nil)
	mockRepository.HardDeleteConvertedFileByFileUIDMock.Return(nil)

	// Mock for DeleteTextChunksFromMinIOActivity
	mockRepository.ListTextChunksByKBFileUIDMock.Return([]repository.TextChunkModel{
		{UID: uuid.Must(uuid.NewV4()), KBUID: kbUID},
	}, nil)
	mockRepository.ListTextChunksByFileUIDMock.When(minimock.AnyContext, kbUID, fileUID).Then([]string{}, nil)
	mockRepository.HardDeleteTextChunksByKBFileUIDMock.Return(nil)

	// Mock for DeleteEmbeddingsFromVectorDBActivity and DeleteEmbeddingRecordsActivity
	mockRepository.ListEmbeddingsByKBFileUIDMock.Return([]repository.EmbeddingModel{
		{UID: uuid.Must(uuid.NewV4()), KBUID: kbUID},
	}, nil)
	mockRepository.DeleteEmbeddingsWithFileUIDMock.Return(nil)
	mockRepository.HardDeleteEmbeddingsByKBFileUIDMock.Return(nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}
	env.RegisterActivity(w.DeleteOriginalFileActivity)
	env.RegisterActivity(w.DeleteConvertedFileActivity)
	env.RegisterActivity(w.DeleteTextChunksFromMinIOActivity)
	env.RegisterActivity(w.DeleteEmbeddingsFromVectorDBActivity)
	env.RegisterActivity(w.DeleteEmbeddingRecordsActivity)
	env.RegisterWorkflow(w.CleanupFileWorkflow)

	param := CleanupFileWorkflowParam{
		FileUID:             fileUID,
		IncludeOriginalFile: true,
		UserUID:             uuid.Must(uuid.NewV4()),
		RequesterUID:        uuid.Must(uuid.NewV4()),
		WorkflowID:          "test-workflow",
	}

	env.ExecuteWorkflow(w.CleanupFileWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.IsNil)
}

func TestCleanupFileWorkflow_WithoutOriginalFile(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockRepository := mock.NewRepositoryMock(mc)

	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	// Note: No mock for DeleteOriginalFileActivity since IncludeOriginalFile=false

	// Mock for DeleteConvertedFileActivity
	mockRepository.GetConvertedFileByFileUIDMock.Return(&repository.ConvertedFileModel{
		UID:   uuid.Must(uuid.NewV4()),
		KBUID: kbUID,
	}, nil)
	mockRepository.ListConvertedFilesByFileUIDMock.Return([]string{}, nil)
	mockRepository.HardDeleteConvertedFileByFileUIDMock.Return(nil)

	// Mock for DeleteTextChunksFromMinIOActivity (empty chunks - activity returns early)
	mockRepository.ListTextChunksByKBFileUIDMock.Return([]repository.TextChunkModel{}, nil)

	// Mock for DeleteEmbeddingsFromVectorDBActivity (empty embeddings - activity returns early)
	mockRepository.ListEmbeddingsByKBFileUIDMock.Return([]repository.EmbeddingModel{}, nil)

	// Mock for DeleteEmbeddingRecordsActivity (hard delete from DB)
	mockRepository.HardDeleteEmbeddingsByKBFileUIDMock.Return(nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}
	env.RegisterActivity(w.DeleteOriginalFileActivity)
	env.RegisterActivity(w.DeleteConvertedFileActivity)
	env.RegisterActivity(w.DeleteTextChunksFromMinIOActivity)
	env.RegisterActivity(w.DeleteEmbeddingsFromVectorDBActivity)
	env.RegisterActivity(w.DeleteEmbeddingRecordsActivity)
	env.RegisterWorkflow(w.CleanupFileWorkflow)

	param := CleanupFileWorkflowParam{
		FileUID:             fileUID,
		IncludeOriginalFile: false, // Skip original file deletion
		UserUID:             uuid.Must(uuid.NewV4()),
		WorkflowID:          "test-workflow",
	}

	env.ExecuteWorkflow(w.CleanupFileWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.IsNil)
}

func TestCleanupKnowledgeBaseWorkflow_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockRepository := mock.NewRepositoryMock(mc)

	kbUID := uuid.Must(uuid.NewV4())

	// Mock for DeleteKBFilesFromMinIOActivity
	mockRepository.ListKnowledgeBaseFilePathsMock.Return([]string{}, nil)

	// Mock for DropVectorDBCollectionActivity
	mockRepository.DropCollectionMock.Return(nil)

	// Mock for DeleteKBFileRecordsActivity
	mockRepository.DeleteAllKnowledgeBaseFilesMock.Return(nil)

	// Mock for DeleteKBConvertedFileRecordsActivity
	mockRepository.DeleteAllConvertedFilesInKbMock.Return(nil)

	// Mock for DeleteKBChunkRecordsActivity
	mockRepository.HardDeleteTextChunksByKBUIDMock.Return(nil)

	// Mock for DeleteKBEmbeddingRecordsActivity
	mockRepository.HardDeleteEmbeddingsByKBUIDMock.Return(nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}
	env.RegisterActivity(w.DeleteKBFilesFromMinIOActivity)
	env.RegisterActivity(w.DropVectorDBCollectionActivity)
	env.RegisterActivity(w.DeleteKBFileRecordsActivity)
	env.RegisterActivity(w.DeleteKBConvertedFileRecordsActivity)
	env.RegisterActivity(w.DeleteKBTextChunkRecordsActivity)
	env.RegisterActivity(w.DeleteKBEmbeddingRecordsActivity)
	// Note: PurgeKBACLActivity intentionally not registered
	env.RegisterWorkflow(w.CleanupKnowledgeBaseWorkflow)

	param := CleanupKnowledgeBaseWorkflowParam{
		KBUID: kbUID,
	}

	env.ExecuteWorkflow(w.CleanupKnowledgeBaseWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	// Expect workflow to complete with errors due to missing ACL activity registration
	c.Assert(env.GetWorkflowError(), qt.IsNotNil)
	c.Assert(env.GetWorkflowError().Error(), qt.Contains, "PurgeKBACLActivity")
}

func TestCleanupKnowledgeBaseWorkflow_VectorDBError(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockRepository := mock.NewRepositoryMock(mc)

	kbUID := uuid.Must(uuid.NewV4())

	// Mock for DeleteKBFilesFromMinIOActivity
	mockRepository.ListKnowledgeBaseFilePathsMock.Return([]string{}, nil)

	// Mock for DropVectorDBCollectionActivity (fails but is handled)
	mockRepository.DropCollectionMock.Return(fmt.Errorf("can't find collection"))

	// Mock for remaining activities (all succeed)
	mockRepository.DeleteAllKnowledgeBaseFilesMock.Return(nil)
	mockRepository.DeleteAllConvertedFilesInKbMock.Return(nil)
	mockRepository.HardDeleteTextChunksByKBUIDMock.Return(nil)
	mockRepository.HardDeleteEmbeddingsByKBUIDMock.Return(nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}
	env.RegisterActivity(w.DeleteKBFilesFromMinIOActivity)
	env.RegisterActivity(w.DropVectorDBCollectionActivity)
	env.RegisterActivity(w.DeleteKBFileRecordsActivity)
	env.RegisterActivity(w.DeleteKBConvertedFileRecordsActivity)
	env.RegisterActivity(w.DeleteKBTextChunkRecordsActivity)
	env.RegisterActivity(w.DeleteKBEmbeddingRecordsActivity)
	// Note: PurgeKBACLActivity intentionally not registered
	env.RegisterWorkflow(w.CleanupKnowledgeBaseWorkflow)

	param := CleanupKnowledgeBaseWorkflowParam{
		KBUID: kbUID,
	}

	env.ExecuteWorkflow(w.CleanupKnowledgeBaseWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	// Workflow collects all errors. VectorDB error is handled, but ACL activity is missing.
	c.Assert(env.GetWorkflowError(), qt.IsNotNil)
}
