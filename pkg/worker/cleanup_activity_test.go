package worker

import (
	"testing"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/instill-ai/artifact-backend/pkg/temporal"
)

func TestCleanupFilesActivityParam_Validation(t *testing.T) {
	fileUID := uuid.Must(uuid.NewV4())

	tests := []struct {
		name  string
		param *CleanupFilesActivityParam
		valid bool
	}{
		{
			name: "Cleanup without original file",
			param: &CleanupFilesActivityParam{
				FileUID:             fileUID,
				FileIDs:             []string{},
				IncludeOriginalFile: false,
			},
			valid: true,
		},
		{
			name: "Cleanup with original file",
			param: &CleanupFilesActivityParam{
				FileUID:             fileUID,
				FileIDs:             []string{},
				IncludeOriginalFile: true,
			},
			valid: true,
		},
		{
			name: "Cleanup with specific temp files",
			param: &CleanupFilesActivityParam{
				FileUID:             fileUID,
				FileIDs:             []string{"temp1", "temp2"},
				IncludeOriginalFile: false,
			},
			valid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NotNil(t, tt.param)
			assert.NotEqual(t, uuid.Nil, tt.param.FileUID)
			assert.NotNil(t, tt.param.FileIDs)
		})
	}
}

func TestCleanupKnowledgeBaseWorkflowParam_Validation(t *testing.T) {
	kbUID := uuid.Must(uuid.NewV4())

	param := temporal.CleanupKnowledgeBaseWorkflowParam{
		KnowledgeBaseUID: kbUID.String(),
	}

	require.NotNil(t, param)
	assert.NotEmpty(t, param.KnowledgeBaseUID)

	// Validate UUID format
	parsedUID, err := uuid.FromString(param.KnowledgeBaseUID)
	require.NoError(t, err)
	assert.Equal(t, kbUID, parsedUID)
}

func TestCleanupFilesActivity_EmptyFiles(t *testing.T) {
	param := &CleanupFilesActivityParam{
		FileUID:             uuid.Must(uuid.NewV4()),
		FileIDs:             []string{},
		IncludeOriginalFile: false,
	}

	assert.Len(t, param.FileIDs, 0)
	assert.False(t, param.IncludeOriginalFile)
}
