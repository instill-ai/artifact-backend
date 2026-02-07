package worker

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

// This file contains status tracking activities used by ProcessFileWorkflow:
// - GetFileStatusActivity - Retrieves current file processing status
// - UpdateFileStatusActivity - Updates file processing status and error messages

// UpdateFileStatusActivityParam defines the parameters for the UpdateFileStatusActivity
type UpdateFileStatusActivityParam struct {
	FileUID types.FileUIDType            // File unique identifier
	Status  artifactpb.FileProcessStatus // New processing status
	Message string                       // Optional status message for display
}

// GetFileStatusActivityParam defines the parameters for the GetFileStatusActivity
type GetFileStatusActivityParam struct {
	FileUID types.FileUIDType // File unique identifier
}

// GetFileStatusActivity retrieves the current status of a file
func (w *Worker) GetFileStatusActivity(ctx context.Context, param *GetFileStatusActivityParam) (artifactpb.FileProcessStatus, error) {
	fileUID := param.FileUID
	w.log.Info("Getting file status", zap.String("fileUID", fileUID.String()))

	files, err := w.repository.GetFilesByFileUIDs(ctx, []types.FileUIDType{fileUID})
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to retrieve file status. Please try again.")
		return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, activityError(err, getFileStatusActivityError)
	}
	if len(files) == 0 {
		// File record not found in database - this is non-retryable because:
		// - File was deleted before processing started (common in integration tests)
		// - Invalid file UID was provided
		// Retrying won't help since the file won't reappear in the DB.
		w.log.Info("GetFileStatusActivity: File not found (record missing from database)",
			zap.String("fileUID", fileUID.String()))
		return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED,
			activityErrorNonRetryableFlat(
				"File not found. It may have been deleted.",
				getFileStatusActivityError,
				errorsx.ErrNotFound,
			)
	}
	file := files[0]

	statusInt, ok := artifactpb.FileProcessStatus_value[file.ProcessStatus]
	if !ok {
		err := errorsx.AddMessage(
			fmt.Errorf("invalid process status: %v", file.ProcessStatus),
			"File status is invalid. Please contact support.",
		)
		return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, activityError(err, getFileStatusActivityError)
	}

	status := artifactpb.FileProcessStatus(statusInt)
	return status, nil
}

// UpdateFileStatusActivity updates the file processing status
func (w *Worker) UpdateFileStatusActivity(ctx context.Context, param *UpdateFileStatusActivityParam) error {
	w.log.Info("Updating file status",
		zap.String("fileUID", param.FileUID.String()),
		zap.String("status", param.Status.String()),
		zap.String("message", param.Message))

	files, err := w.repository.GetFilesByFileUIDs(ctx, []types.FileUIDType{param.FileUID})
	if err != nil {
		w.log.Error("Failed to get file for status update", zap.Error(err))
		err = errorsx.AddMessage(err, "Unable to update file status. Please try again.")
		return activityError(err, updateFileStatusActivityError)
	}
	if len(files) == 0 {
		return nil
	}

	updateMap := map[string]any{
		repository.FileColumn.ProcessStatus: param.Status.String(),
	}

	if param.Message != "" {
		err := w.repository.UpdateFileMetadata(ctx, param.FileUID, repository.ExtraMetaData{StatusMessage: param.Message})
		if err != nil {
			w.log.Warn("Failed to update file extra metadata with message (file may be deleted)", zap.Error(err))
		}
	}

	_, err = w.repository.UpdateFile(ctx, param.FileUID.String(), updateMap)
	if err != nil {
		w.log.Warn("Failed to update file status (file may be deleted)",
			zap.String("fileUID", param.FileUID.String()),
			zap.Error(err))
		return nil
	}

	return nil
}

// Activity error type constants
const (
	getFileStatusActivityError    = "GetFileStatusActivity"
	updateFileStatusActivityError = "UpdateFileStatusActivity"
)
