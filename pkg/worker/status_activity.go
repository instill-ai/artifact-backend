package worker

import (
	"context"
	"fmt"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/repository"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// GetFileStatusActivity retrieves the current status of a file
func (w *worker) GetFileStatusActivity(ctx context.Context, fileUID uuid.UUID) (artifactpb.FileProcessStatus, error) {
	w.log.Info("Getting file status", zap.String("fileUID", fileUID.String()))

	files, err := w.repository.GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{fileUID})
	if err != nil {
		return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, fmt.Errorf("failed to get files: %w", err)
	}
	if len(files) == 0 {
		return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, fmt.Errorf("file not found: %s", fileUID.String())
	}
	file := files[0]

	statusInt, ok := artifactpb.FileProcessStatus_value[file.ProcessStatus]
	if !ok {
		return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, fmt.Errorf("invalid process status: %v", file.ProcessStatus)
	}

	status := artifactpb.FileProcessStatus(statusInt)
	w.log.Info("File status retrieved", zap.String("fileUID", fileUID.String()), zap.String("status", status.String()))
	return status, nil
}

// UpdateFileStatusActivity updates the file processing status
func (w *worker) UpdateFileStatusActivity(ctx context.Context, param *UpdateFileStatusActivityParam) error {
	w.log.Info("Updating file status",
		zap.String("fileUID", param.FileUID.String()),
		zap.String("status", param.Status.String()),
		zap.String("message", param.Message))

	files, err := w.repository.GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{param.FileUID})
	if err != nil {
		w.log.Error("Failed to get file for status update", zap.Error(err))
		return fmt.Errorf("failed to get file: %w", err)
	}
	if len(files) == 0 {
		return nil
	}

	updateMap := map[string]any{
		repository.KnowledgeBaseFileColumn.ProcessStatus: param.Status.String(),
	}

	if param.Message != "" {
		err := w.repository.UpdateKBFileMetadata(ctx, param.FileUID, repository.ExtraMetaData{FailReason: param.Message})
		if err != nil {
			w.log.Warn("Failed to update file extra metadata with message (file may be deleted)", zap.Error(err))
		}
	}

	_, err = w.repository.UpdateKnowledgeBaseFile(ctx, param.FileUID.String(), updateMap)
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

// NotifyFileProcessedActivity handles notifications about file processing completion
func (w *worker) NotifyFileProcessedActivity(ctx context.Context, param *NotifyFileProcessedActivityParam) error {
	// TODO: Implement notification functionality
	// Future enhancements could include:
	// - Redis pub/sub notifications for real-time updates
	// - Webhook notifications to external systems
	// - Email notifications for important status changes
	// - Push notifications to mobile apps
	// - Integration with notification preference settings

	w.log.Info("File processing completed",
		zap.String("fileUID", param.FileUID.String()),
		zap.String("status", param.Status.String()))

	return nil
}
