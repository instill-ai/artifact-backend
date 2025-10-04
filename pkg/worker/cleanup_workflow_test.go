package worker

import (
	"testing"

	"github.com/gofrs/uuid"

	qt "github.com/frankban/quicktest"

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
	fileUID := uuid.Must(uuid.NewV4())
	userUID := uuid.Must(uuid.NewV4())

	// Test with IncludeOriginalFile = true
	paramTrue := service.CleanupFileWorkflowParam{
		FileUID:             fileUID,
		IncludeOriginalFile: true,
		UserUID:             userUID,
		WorkflowID:          "test-workflow-true",
	}
	c.Assert(paramTrue.IncludeOriginalFile, qt.IsTrue)

	// Test with IncludeOriginalFile = false
	paramFalse := service.CleanupFileWorkflowParam{
		FileUID:             fileUID,
		IncludeOriginalFile: false,
		UserUID:             userUID,
		WorkflowID:          "test-workflow-false",
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
