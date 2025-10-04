package worker

import (
	"testing"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/instill-ai/artifact-backend/pkg/service"
)

func TestCleanupFileWorkflowParam_Validation(t *testing.T) {
	fileUID := uuid.Must(uuid.NewV4())
	userUID := uuid.Must(uuid.NewV4())

	param := service.CleanupFileWorkflowParam{
		FileUID:             fileUID,
		IncludeOriginalFile: true,
		UserUID:             userUID,
		WorkflowID:          "test-workflow-id",
	}

	require.NotNil(t, param)
	assert.NotEqual(t, uuid.Nil, param.FileUID)
	assert.NotEqual(t, uuid.Nil, param.UserUID)
	assert.NotEmpty(t, param.WorkflowID)
	assert.True(t, param.IncludeOriginalFile)
}

func TestCleanupKnowledgeBaseWorkflowParam_Validation(t *testing.T) {
	kbUID := uuid.Must(uuid.NewV4())

	param := service.CleanupKnowledgeBaseWorkflowParam{
		KnowledgeBaseUID: kbUID,
	}

	require.NotNil(t, param)
	assert.NotEqual(t, uuid.Nil, param.KnowledgeBaseUID)

	// Validate UUID
	assert.Equal(t, kbUID, param.KnowledgeBaseUID)
}
