package worker

import (
	"testing"

	"github.com/gofrs/uuid"
	"github.com/gojuno/minimock/v3"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/zap"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/worker/mock"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

func TestProcessFileWorkflowParam_Validation(t *testing.T) {
	c := qt.New(t)
	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	tests := []struct {
		name  string
		param ProcessFileWorkflowParam
	}{
		{
			name: "Valid parameters",
			param: ProcessFileWorkflowParam{
				FileUID: fileUID,
				KBUID:   kbUID,
			},
		},
		{
			name: "Different valid parameters",
			param: ProcessFileWorkflowParam{
				FileUID: uuid.Must(uuid.NewV4()),
				KBUID:   uuid.Must(uuid.NewV4()),
			},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(tt.param.FileUID, qt.Not(qt.Equals), uuid.Nil)
			c.Assert(tt.param.KBUID, qt.Not(qt.Equals), uuid.Nil)
		})
	}
}

func TestProcessFileWorkflowParam_FieldTypes(t *testing.T) {
	c := qt.New(t)
	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	param := ProcessFileWorkflowParam{
		FileUID: fileUID,
		KBUID:   kbUID,
	}

	// Verify fields are set correctly
	c.Assert(param.FileUID, qt.Equals, fileUID)
	c.Assert(param.KBUID, qt.Equals, kbUID)
}

func TestProcessFileWorkflowParam_UUIDs(t *testing.T) {
	c := qt.New(t)
	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	param := ProcessFileWorkflowParam{
		FileUID: fileUID,
		KBUID:   kbUID,
	}

	// Verify UUIDs are correctly assigned
	c.Assert(param.FileUID, qt.Equals, fileUID)
	c.Assert(param.KBUID, qt.Equals, kbUID)

	// Verify UUIDs are different
	c.Assert(param.FileUID, qt.Not(qt.Equals), param.KBUID)
}

func TestProcessFileWorkflowParam_UUIDFormat(t *testing.T) {
	c := qt.New(t)
	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	param := ProcessFileWorkflowParam{
		FileUID: fileUID,
		KBUID:   kbUID,
	}

	// Test FileUID is properly formatted
	fileUIDStr := param.FileUID.String()
	c.Assert(fileUIDStr, qt.HasLen, 36)
	c.Assert(fileUIDStr, qt.Contains, "-")

	// Test KBUID is properly formatted
	kbUIDStr := param.KBUID.String()
	c.Assert(kbUIDStr, qt.HasLen, 36)
	c.Assert(kbUIDStr, qt.Contains, "-")

	// Parse back to ensure they're valid
	parsedFileUID, err := uuid.FromString(fileUIDStr)
	c.Assert(err, qt.IsNil)
	c.Assert(parsedFileUID, qt.Equals, fileUID)

	parsedKBUID, err := uuid.FromString(kbUIDStr)
	c.Assert(err, qt.IsNil)
	c.Assert(parsedKBUID, qt.Equals, kbUID)
}

func TestProcessFileWorkflowParam_ZeroValues(t *testing.T) {
	c := qt.New(t)

	// Test with zero values
	var param ProcessFileWorkflowParam

	c.Assert(param.FileUID, qt.Equals, uuid.Nil)
	c.Assert(param.KBUID, qt.Equals, uuid.Nil)
}

func TestProcessFileWorkflowParam_SameFileAndKB(t *testing.T) {
	c := qt.New(t)

	// Edge case: same UUID for file and KB (though unlikely in practice)
	sameUID := uuid.Must(uuid.NewV4())

	param := ProcessFileWorkflowParam{
		FileUID: sameUID,
		KBUID:   sameUID,
	}

	c.Assert(param.FileUID, qt.Equals, param.KBUID)
}

func TestProcessFileWorkflowParam_MultipleInstances(t *testing.T) {
	c := qt.New(t)

	// Test creating multiple param instances with different values
	param1 := ProcessFileWorkflowParam{
		FileUID: uuid.Must(uuid.NewV4()),
		KBUID:   uuid.Must(uuid.NewV4()),
	}

	param2 := ProcessFileWorkflowParam{
		FileUID: uuid.Must(uuid.NewV4()),
		KBUID:   uuid.Must(uuid.NewV4()),
	}

	// Verify each instance has unique values
	c.Assert(param1.FileUID, qt.Not(qt.Equals), param2.FileUID)
	c.Assert(param1.KBUID, qt.Not(qt.Equals), param2.KBUID)
}

func TestProcessFileWorkflowParam_Copy(t *testing.T) {
	c := qt.New(t)
	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	original := ProcessFileWorkflowParam{
		FileUID: fileUID,
		KBUID:   kbUID,
	}

	// Create a copy
	copy := original

	// Verify copy has same values
	c.Assert(copy.FileUID, qt.Equals, original.FileUID)
	c.Assert(copy.KBUID, qt.Equals, original.KBUID)

	// Modify copy
	copy.FileUID = uuid.Must(uuid.NewV4())

	// Verify original is unchanged (value type behavior)
	c.Assert(original.FileUID, qt.Equals, fileUID)
	c.Assert(original.FileUID, qt.Not(qt.Equals), copy.FileUID)
}

// Workflow tests with minimock
// Note: ProcessFileWorkflow is highly complex with 20+ activities, 2+ child workflows,
// and external pipeline calls. Full mocking of this workflow is extremely complex and brittle.
// These tests validate error handling and key failure paths. Full happy-path testing is
// better suited for end-to-end integration tests with real services.

func TestProcessFileWorkflow_GetFileMetadataFailure(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockRepository := mock.NewRepositoryMock(mc)

	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	// Mock GetFileStatusActivity and GetFileMetadataActivity to return empty (file not found)
	mockRepository.GetKnowledgeBaseFilesByFileUIDsMock.Return([]repository.KnowledgeBaseFileModel{}, nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}
	env.RegisterActivity(w.GetFileMetadataActivity)
	env.RegisterActivity(w.GetFileStatusActivity)
	env.RegisterActivity(w.UpdateFileStatusActivity)
	env.RegisterWorkflow(w.ProcessFileWorkflow)

	param := ProcessFileWorkflowParam{
		FileUID: fileUID,
		KBUID:   kbUID,
	}

	env.ExecuteWorkflow(w.ProcessFileWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.ErrorMatches, ".*file status.*")
}

func TestProcessFileWorkflow_GetFileMetadataSuccess(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepository := mock.NewRepositoryMock(mc)

	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	// Mock GetFileMetadataActivity and GetFileStatusActivity to return success
	mockRepository.GetKnowledgeBaseFilesByFileUIDsMock.Return([]repository.KnowledgeBaseFileModel{
		{UID: fileUID, KBUID: kbUID, ProcessStatus: artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_WAITING.String()},
	}, nil)
	// Mock UpdateFileStatusActivity for the defer cleanup function
	mockRepository.UpdateKnowledgeFileMetadataMock.Return(nil)
	mockRepository.UpdateKnowledgeBaseFileMock.Return(&repository.KnowledgeBaseFileModel{}, nil)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	w := &Worker{repository: mockRepository, log: zap.NewNop()}
	env.RegisterActivity(w.GetFileMetadataActivity)
	env.RegisterActivity(w.GetFileStatusActivity)
	env.RegisterActivity(w.UpdateFileStatusActivity)
	env.RegisterWorkflow(w.ProcessFileWorkflow)

	param := ProcessFileWorkflowParam{
		FileUID: fileUID,
		KBUID:   kbUID,
	}

	env.ExecuteWorkflow(w.ProcessFileWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.IsNil)
}
