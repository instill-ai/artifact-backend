package worker

import (
	"testing"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

func TestUpdateFileStatusActivityParam_Validation(t *testing.T) {
	fileUID := uuid.Must(uuid.NewV4())

	tests := []struct {
		name  string
		param *UpdateFileStatusActivityParam
		valid bool
	}{
		{
			name: "Valid status update",
			param: &UpdateFileStatusActivityParam{
				FileUID: fileUID,
				Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED,
				Message: "Success",
			},
			valid: true,
		},
		{
			name: "Failed status with message",
			param: &UpdateFileStatusActivityParam{
				FileUID: fileUID,
				Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_FAILED,
				Message: "Error occurred",
			},
			valid: true,
		},
		{
			name: "Empty message",
			param: &UpdateFileStatusActivityParam{
				FileUID: fileUID,
				Status:  artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING,
				Message: "",
			},
			valid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NotNil(t, tt.param)
			assert.NotEqual(t, uuid.Nil, tt.param.FileUID)
			assert.NotEqual(t, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, tt.param.Status)
		})
	}
}

func TestFileProcessStatus_Values(t *testing.T) {
	statuses := []artifactpb.FileProcessStatus{
		artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CONVERTING,
		artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_SUMMARIZING,
		artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING,
		artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING,
		artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED,
		artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_FAILED,
	}

	for _, status := range statuses {
		t.Run(status.String(), func(t *testing.T) {
			assert.NotEmpty(t, status.String())
			assert.NotEqual(t, artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, status)
		})
	}
}
