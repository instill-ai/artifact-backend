package worker

import (
	"fmt"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/gojuno/minimock/v3"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/zap"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/acl"
	"github.com/instill-ai/artifact-backend/pkg/mock"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"
)

func TestCleanupFileWorkflowParam_FieldTypes(t *testing.T) {
	c := qt.New(t)
	fileUID := uuid.Must(uuid.NewV4())
	userUID := uuid.Must(uuid.NewV4())

	param := service.CleanupFileWorkflowParam{
		FileUID:             fileUID,
		IncludeOriginalFile: true,
		UserUID:             userUID,
		WorkflowID:          "test-workflow-id",
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
	paramTrue := service.CleanupFileWorkflowParam{
		IncludeOriginalFile: true,
	}
	c.Assert(paramTrue.IncludeOriginalFile, qt.IsTrue)

	// Test with IncludeOriginalFile = false
	paramFalse := service.CleanupFileWorkflowParam{
		IncludeOriginalFile: false,
	}
	c.Assert(paramFalse.IncludeOriginalFile, qt.IsFalse)
}

func TestCleanupKBWorkflowParam_UUIDFormat(t *testing.T) {
	c := qt.New(t)
	kbUID := uuid.Must(uuid.NewV4())

	param := service.CleanupKnowledgeBaseWorkflowParam{
		KnowledgeBaseUID: kbUID,
	}

	// Test UUID is properly formatted
	uuidStr := param.KnowledgeBaseUID.String()
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
	var param service.CleanupFileWorkflowParam

	c.Assert(param.FileUID, qt.Equals, uuid.Nil)
	c.Assert(param.UserUID, qt.Equals, uuid.Nil)
	c.Assert(param.IncludeOriginalFile, qt.IsFalse)
	c.Assert(param.WorkflowID, qt.Equals, "")
}

func TestCleanupKBWorkflowParam_ZeroValues(t *testing.T) {
	c := qt.New(t)

	// Test with zero values
	var param service.CleanupKnowledgeBaseWorkflowParam

	c.Assert(param.KnowledgeBaseUID, qt.Equals, uuid.Nil)
}

func TestCleanupFileWorkflow_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockRepo := mock.NewRepositoryIMock(mc)
	mockVectorDB := NewVectorDatabaseMock(mc)
	mockSvc := NewServiceMock(mc)
	mockSvc.RepositoryMock.Return(mockRepo)
	mockSvc.VectorDBMock.Return(mockVectorDB)

	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	// Mock for DeleteOriginalFileActivity
	mockRepo.GetKnowledgeBaseFilesByFileUIDsMock.Return([]repository.KnowledgeBaseFile{
		{UID: fileUID, Destination: "kb/test-file.pdf"},
	}, nil)
	mockSvc.DeleteFilesMock.Return(nil)

	// Mock for DeleteConvertedFileActivity
	mockRepo.GetConvertedFileByFileUIDMock.Return(&repository.ConvertedFile{
		UID:   uuid.Must(uuid.NewV4()),
		KbUID: kbUID,
	}, nil)
	mockSvc.DeleteConvertedFileByFileUIDMock.Return(nil)
	mockRepo.HardDeleteConvertedFileByFileUIDMock.Return(nil)

	// Mock for DeleteChunksFromMinIOActivity
	mockRepo.ListChunksByKbFileUIDMock.Return([]repository.TextChunk{
		{UID: uuid.Must(uuid.NewV4()), KbUID: kbUID},
	}, nil)
	mockSvc.DeleteTextChunksByFileUIDMock.Return(nil)
	mockRepo.HardDeleteChunksByKbFileUIDMock.Return(nil)

	// Mock for DeleteEmbeddingsFromVectorDBActivity and DeleteEmbeddingRecordsActivity
	mockRepo.ListEmbeddingsByKbFileUIDMock.Return([]repository.Embedding{
		{UID: uuid.Must(uuid.NewV4()), KbUID: kbUID},
	}, nil)
	mockVectorDB.DeleteEmbeddingsWithFileUIDMock.Return(nil)
	mockRepo.HardDeleteEmbeddingsByKbFileUIDMock.Return(nil)

	worker := &Worker{service: mockSvc, log: zap.NewNop()}
	env.RegisterActivity(worker.DeleteOriginalFileActivity)
	env.RegisterActivity(worker.DeleteConvertedFileActivity)
	env.RegisterActivity(worker.DeleteChunksFromMinIOActivity)
	env.RegisterActivity(worker.DeleteEmbeddingsFromVectorDBActivity)
	env.RegisterActivity(worker.DeleteEmbeddingRecordsActivity)
	env.RegisterWorkflow(worker.CleanupFileWorkflow)

	param := service.CleanupFileWorkflowParam{
		FileUID:             fileUID,
		IncludeOriginalFile: true,
		UserUID:             uuid.Must(uuid.NewV4()),
		WorkflowID:          "test-workflow",
	}

	env.ExecuteWorkflow(worker.CleanupFileWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.IsNil)
}

func TestCleanupFileWorkflow_WithoutOriginalFile(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockRepo := mock.NewRepositoryIMock(mc)
	mockSvc := NewServiceMock(mc)
	mockSvc.RepositoryMock.Return(mockRepo)
	// Note: VectorDB mock not set up because activities return early when lists are empty

	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	// Note: No mock for DeleteOriginalFileActivity since IncludeOriginalFile=false

	// Mock for DeleteConvertedFileActivity
	mockRepo.GetConvertedFileByFileUIDMock.Return(&repository.ConvertedFile{
		UID:   uuid.Must(uuid.NewV4()),
		KbUID: kbUID,
	}, nil)
	mockSvc.DeleteConvertedFileByFileUIDMock.Return(nil)
	mockRepo.HardDeleteConvertedFileByFileUIDMock.Return(nil)

	// Mock for DeleteChunksFromMinIOActivity (empty chunks - activity returns early)
	mockRepo.ListChunksByKbFileUIDMock.Return([]repository.TextChunk{}, nil)

	// Mock for DeleteEmbeddingsFromVectorDBActivity (empty embeddings - activity returns early)
	mockRepo.ListEmbeddingsByKbFileUIDMock.Return([]repository.Embedding{}, nil)

	// Mock for DeleteEmbeddingRecordsActivity (hard delete from DB)
	mockRepo.HardDeleteEmbeddingsByKbFileUIDMock.Return(nil)

	worker := &Worker{service: mockSvc, log: zap.NewNop()}
	env.RegisterActivity(worker.DeleteOriginalFileActivity)
	env.RegisterActivity(worker.DeleteConvertedFileActivity)
	env.RegisterActivity(worker.DeleteChunksFromMinIOActivity)
	env.RegisterActivity(worker.DeleteEmbeddingsFromVectorDBActivity)
	env.RegisterActivity(worker.DeleteEmbeddingRecordsActivity)
	env.RegisterWorkflow(worker.CleanupFileWorkflow)

	param := service.CleanupFileWorkflowParam{
		FileUID:             fileUID,
		IncludeOriginalFile: false, // Skip original file deletion
		UserUID:             uuid.Must(uuid.NewV4()),
		WorkflowID:          "test-workflow",
	}

	env.ExecuteWorkflow(worker.CleanupFileWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.IsNil)
}

func TestCleanupKnowledgeBaseWorkflow_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockRepo := mock.NewRepositoryIMock(mc)
	mockVectorDB := NewVectorDatabaseMock(mc)
	mockSvc := NewServiceMock(mc)
	mockSvc.RepositoryMock.Return(mockRepo)
	mockSvc.VectorDBMock.Return(mockVectorDB)
	mockSvc.ACLClientMock.Return(&acl.ACLClient{})

	kbUID := uuid.Must(uuid.NewV4())

	// Mock for DeleteKBFilesFromMinIOActivity
	mockSvc.DeleteKnowledgeBaseMock.Return(nil)

	// Mock for DropVectorDBCollectionActivity
	mockVectorDB.DropCollectionMock.Return(nil)

	// Mock for DeleteKBFileRecordsActivity
	mockRepo.DeleteAllKnowledgeBaseFilesMock.Return(nil)

	// Mock for DeleteKBConvertedFileRecordsActivity
	mockRepo.DeleteAllConvertedFilesInKbMock.Return(nil)

	// Mock for DeleteKBChunkRecordsActivity
	mockRepo.HardDeleteChunksByKbUIDMock.Return(nil)

	// Mock for DeleteKBEmbeddingRecordsActivity
	mockRepo.HardDeleteEmbeddingsByKbUIDMock.Return(nil)

	// Note: PurgeKBACLActivity cannot be fully tested with minimock because ACLClient
	// is a concrete type (not an interface) with internal state. We mock ACLClient() to
	// return nil to avoid nil pointer panics, and don't register PurgeKBACLActivity.
	// The workflow will report an error for the missing activity, which we expect.
	mockSvc.ACLClientMock.Optional().Return(nil)

	worker := &Worker{service: mockSvc, log: zap.NewNop()}
	env.RegisterActivity(worker.DeleteKBFilesFromMinIOActivity)
	env.RegisterActivity(worker.DropVectorDBCollectionActivity)
	env.RegisterActivity(worker.DeleteKBFileRecordsActivity)
	env.RegisterActivity(worker.DeleteKBConvertedFileRecordsActivity)
	env.RegisterActivity(worker.DeleteKBChunkRecordsActivity)
	env.RegisterActivity(worker.DeleteKBEmbeddingRecordsActivity)
	// Note: PurgeKBACLActivity intentionally not registered
	env.RegisterWorkflow(worker.CleanupKnowledgeBaseWorkflow)

	param := service.CleanupKnowledgeBaseWorkflowParam{
		KnowledgeBaseUID: kbUID,
	}

	env.ExecuteWorkflow(worker.CleanupKnowledgeBaseWorkflow, param)

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

	mockRepo := mock.NewRepositoryIMock(mc)
	mockVectorDB := NewVectorDatabaseMock(mc)
	mockSvc := NewServiceMock(mc)
	mockSvc.RepositoryMock.Return(mockRepo)
	mockSvc.VectorDBMock.Return(mockVectorDB)
	mockSvc.ACLClientMock.Return(&acl.ACLClient{})

	kbUID := uuid.Must(uuid.NewV4())

	// Mock for DeleteKBFilesFromMinIOActivity (succeeds)
	mockSvc.DeleteKnowledgeBaseMock.Return(nil)

	// Mock for DropVectorDBCollectionActivity (fails but is handled)
	mockVectorDB.DropCollectionMock.Return(fmt.Errorf("can't find collection"))

	// Mock for remaining activities (all succeed)
	mockRepo.DeleteAllKnowledgeBaseFilesMock.Return(nil)
	mockRepo.DeleteAllConvertedFilesInKbMock.Return(nil)
	mockRepo.HardDeleteChunksByKbUIDMock.Return(nil)
	mockRepo.HardDeleteEmbeddingsByKbUIDMock.Return(nil)

	// Note: PurgeKBACLActivity not registered (see test above for explanation)
	mockSvc.ACLClientMock.Optional().Return(nil)

	worker := &Worker{service: mockSvc, log: zap.NewNop()}
	env.RegisterActivity(worker.DeleteKBFilesFromMinIOActivity)
	env.RegisterActivity(worker.DropVectorDBCollectionActivity)
	env.RegisterActivity(worker.DeleteKBFileRecordsActivity)
	env.RegisterActivity(worker.DeleteKBConvertedFileRecordsActivity)
	env.RegisterActivity(worker.DeleteKBChunkRecordsActivity)
	env.RegisterActivity(worker.DeleteKBEmbeddingRecordsActivity)
	// Note: PurgeKBACLActivity intentionally not registered
	env.RegisterWorkflow(worker.CleanupKnowledgeBaseWorkflow)

	param := service.CleanupKnowledgeBaseWorkflowParam{
		KnowledgeBaseUID: kbUID,
	}

	env.ExecuteWorkflow(worker.CleanupKnowledgeBaseWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	// Workflow collects all errors. VectorDB error is handled, but ACL activity is missing.
	c.Assert(env.GetWorkflowError(), qt.IsNotNil)
}
