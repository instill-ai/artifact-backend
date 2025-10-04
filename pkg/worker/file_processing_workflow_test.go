package worker

import (
	"testing"

	"github.com/gofrs/uuid"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/service"
)

func TestProcessFileWorkflowParam_Validation(t *testing.T) {
	c := qt.New(t)
	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	tests := []struct {
		name  string
		param service.ProcessFileWorkflowParam
	}{
		{
			name: "Valid parameters",
			param: service.ProcessFileWorkflowParam{
				FileUID:          fileUID,
				KnowledgeBaseUID: kbUID,
			},
		},
		{
			name: "Different valid parameters",
			param: service.ProcessFileWorkflowParam{
				FileUID:          uuid.Must(uuid.NewV4()),
				KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
			},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(tt.param.FileUID, qt.Not(qt.Equals), uuid.Nil)
			c.Assert(tt.param.KnowledgeBaseUID, qt.Not(qt.Equals), uuid.Nil)
		})
	}
}

func TestProcessFileWorkflowParam_FieldTypes(t *testing.T) {
	c := qt.New(t)
	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	param := service.ProcessFileWorkflowParam{
		FileUID:          fileUID,
		KnowledgeBaseUID: kbUID,
	}

	// Verify fields are set correctly
	c.Assert(param.FileUID, qt.Equals, fileUID)
	c.Assert(param.KnowledgeBaseUID, qt.Equals, kbUID)
}

func TestProcessFileWorkflowParam_UUIDs(t *testing.T) {
	c := qt.New(t)
	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	param := service.ProcessFileWorkflowParam{
		FileUID:          fileUID,
		KnowledgeBaseUID: kbUID,
	}

	// Verify UUIDs are correctly assigned
	c.Assert(param.FileUID, qt.Equals, fileUID)
	c.Assert(param.KnowledgeBaseUID, qt.Equals, kbUID)

	// Verify UUIDs are different
	c.Assert(param.FileUID, qt.Not(qt.Equals), param.KnowledgeBaseUID)
}

func TestProcessFileWorkflowParam_UUIDFormat(t *testing.T) {
	c := qt.New(t)
	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	param := service.ProcessFileWorkflowParam{
		FileUID:          fileUID,
		KnowledgeBaseUID: kbUID,
	}

	// Test FileUID is properly formatted
	fileUIDStr := param.FileUID.String()
	c.Assert(fileUIDStr, qt.HasLen, 36)
	c.Assert(fileUIDStr, qt.Contains, "-")

	// Test KnowledgeBaseUID is properly formatted
	kbUIDStr := param.KnowledgeBaseUID.String()
	c.Assert(kbUIDStr, qt.HasLen, 36)
	c.Assert(kbUIDStr, qt.Contains, "-")

	// Parse back to ensure they're valid
	parsedFileUID, err := uuid.FromString(fileUIDStr)
	c.Assert(err, qt.IsNil)
	c.Assert(parsedFileUID, qt.Equals, fileUID)

	parsedKbUID, err := uuid.FromString(kbUIDStr)
	c.Assert(err, qt.IsNil)
	c.Assert(parsedKbUID, qt.Equals, kbUID)
}

func TestProcessFileWorkflowParam_ZeroValues(t *testing.T) {
	c := qt.New(t)

	// Test with zero values
	var param service.ProcessFileWorkflowParam

	c.Assert(param.FileUID, qt.Equals, uuid.Nil)
	c.Assert(param.KnowledgeBaseUID, qt.Equals, uuid.Nil)
}

func TestProcessFileWorkflowParam_SameFileAndKB(t *testing.T) {
	c := qt.New(t)

	// Edge case: same UUID for file and KB (though unlikely in practice)
	sameUID := uuid.Must(uuid.NewV4())

	param := service.ProcessFileWorkflowParam{
		FileUID:          sameUID,
		KnowledgeBaseUID: sameUID,
	}

	c.Assert(param.FileUID, qt.Equals, param.KnowledgeBaseUID)
}

func TestProcessFileWorkflowParam_MultipleInstances(t *testing.T) {
	c := qt.New(t)

	// Test creating multiple param instances with different values
	param1 := service.ProcessFileWorkflowParam{
		FileUID:          uuid.Must(uuid.NewV4()),
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
	}

	param2 := service.ProcessFileWorkflowParam{
		FileUID:          uuid.Must(uuid.NewV4()),
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
	}

	// Verify each instance has unique values
	c.Assert(param1.FileUID, qt.Not(qt.Equals), param2.FileUID)
	c.Assert(param1.KnowledgeBaseUID, qt.Not(qt.Equals), param2.KnowledgeBaseUID)
}

func TestProcessFileWorkflowParam_Copy(t *testing.T) {
	c := qt.New(t)
	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	original := service.ProcessFileWorkflowParam{
		FileUID:          fileUID,
		KnowledgeBaseUID: kbUID,
	}

	// Create a copy
	copy := original

	// Verify copy has same values
	c.Assert(copy.FileUID, qt.Equals, original.FileUID)
	c.Assert(copy.KnowledgeBaseUID, qt.Equals, original.KnowledgeBaseUID)

	// Modify copy
	copy.FileUID = uuid.Must(uuid.NewV4())

	// Verify original is unchanged (value type behavior)
	c.Assert(original.FileUID, qt.Equals, fileUID)
	c.Assert(original.FileUID, qt.Not(qt.Equals), copy.FileUID)
}
