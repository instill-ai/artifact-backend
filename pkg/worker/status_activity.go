package worker

import (
	"context"
	"fmt"

	"github.com/gofrs/uuid"
	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/repository"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

// UpdateFileStatusActivityParam defines the parameters for the UpdateFileStatusActivity
type UpdateFileStatusActivityParam struct {
	FileUID uuid.UUID
	Status  artifactpb.FileProcessStatus
	Message string
}

// GetFileStatusActivity retrieves the current status of a file
func (w *Worker) GetFileStatusActivity(ctx context.Context, fileUID uuid.UUID) (artifactpb.FileProcessStatus, error) {
	w.log.Info("Getting file status", zap.String("fileUID", fileUID.String()))

	files, err := w.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{fileUID})
	if err != nil {
		return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to get file: %s", errorsx.MessageOrErr(err)),
			getFileStatusActivityError,
			err,
		)
	}
	if len(files) == 0 {
		err := fmt.Errorf("file not found: %s", fileUID.String())
		return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, temporal.NewApplicationErrorWithCause(
			"File not found",
			getFileStatusActivityError,
			err,
		)
	}
	file := files[0]

	statusInt, ok := artifactpb.FileProcessStatus_value[file.ProcessStatus]
	if !ok {
		err := fmt.Errorf("invalid process status: %v", file.ProcessStatus)
		return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, temporal.NewApplicationErrorWithCause(
			"Invalid file status",
			getFileStatusActivityError,
			err,
		)
	}

	status := artifactpb.FileProcessStatus(statusInt)
	w.log.Info("File status retrieved", zap.String("fileUID", fileUID.String()), zap.String("status", status.String()))
	return status, nil
}

// UpdateFileStatusActivity updates the file processing status
func (w *Worker) UpdateFileStatusActivity(ctx context.Context, param *UpdateFileStatusActivityParam) error {
	w.log.Info("Updating file status",
		zap.String("fileUID", param.FileUID.String()),
		zap.String("status", param.Status.String()),
		zap.String("message", param.Message))

	files, err := w.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{param.FileUID})
	if err != nil {
		w.log.Error("Failed to get file for status update", zap.Error(err))
		return temporal.NewApplicationErrorWithCause(
			fmt.Sprintf("Failed to get file: %s", errorsx.MessageOrErr(err)),
			updateFileStatusActivityError,
			err,
		)
	}
	if len(files) == 0 {
		return nil
	}

	updateMap := map[string]any{
		repository.KnowledgeBaseFileColumn.ProcessStatus: param.Status.String(),
	}

	if param.Message != "" {
		err := w.service.Repository().UpdateKBFileMetadata(ctx, param.FileUID, repository.ExtraMetaData{FailReason: param.Message})
		if err != nil {
			w.log.Warn("Failed to update file extra metadata with message (file may be deleted)", zap.Error(err))
		}
	}

	_, err = w.service.Repository().UpdateKnowledgeBaseFile(ctx, param.FileUID.String(), updateMap)
	if err != nil {
		w.log.Warn("Failed to update file status (file may be deleted)",
			zap.String("fileUID", param.FileUID.String()),
			zap.Error(err))
		return nil
	}

	w.log.Info("File status updated successfully",
		zap.String("fileUID", param.FileUID.String()),
		zap.String("status", param.Status.String()))
	return nil
}

// Activity error type constants
const (
	getFileStatusActivityError    = "GetFileStatusActivity"
	updateFileStatusActivityError = "UpdateFileStatusActivity"
)
