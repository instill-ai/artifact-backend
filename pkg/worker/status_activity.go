package worker

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

// This file contains status tracking activities used by ProcessFileWorkflow:
// - GetFileStatusActivity - Retrieves current file processing status
// - UpdateFileStatusActivity - Updates file processing status and error messages
// - GetKBSystemConfigActivity - Retrieves KB's system configuration

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

	files, err := w.repository.GetKnowledgeBaseFilesByFileUIDs(ctx, []types.FileUIDType{fileUID})
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to retrieve file status. Please try again.")
		return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			getFileStatusActivityError,
			err,
		)
	}
	if len(files) == 0 {
		err := errorsx.AddMessage(
			errorsx.ErrNotFound,
			"File not found. It may have been deleted.",
		)
		return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			getFileStatusActivityError,
			err,
		)
	}
	file := files[0]

	statusInt, ok := artifactpb.FileProcessStatus_value[file.ProcessStatus]
	if !ok {
		err := errorsx.AddMessage(
			fmt.Errorf("invalid process status: %v", file.ProcessStatus),
			"File status is invalid. Please contact support.",
		)
		return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			getFileStatusActivityError,
			err,
		)
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

	files, err := w.repository.GetKnowledgeBaseFilesByFileUIDs(ctx, []types.FileUIDType{param.FileUID})
	if err != nil {
		w.log.Error("Failed to get file for status update", zap.Error(err))
		err = errorsx.AddMessage(err, "Unable to update file status. Please try again.")
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
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
		err := w.repository.UpdateKnowledgeFileMetadata(ctx, param.FileUID, repository.ExtraMetaData{FailReason: param.Message})
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

	return nil
}

// GetKBSystemConfigActivityParam defines the parameters for GetKBSystemConfigActivity
type GetKBSystemConfigActivityParam struct {
	KBUID types.KBUIDType // Knowledge base unique identifier
}

// GetKBSystemConfigActivity retrieves the system configuration for a knowledge base
// This is used to determine routing logic (e.g., OpenAI vs Gemini routes)
func (w *Worker) GetKBSystemConfigActivity(ctx context.Context, param *GetKBSystemConfigActivityParam) (repository.SystemConfigJSON, error) {
	w.log.Info("Getting KB system config", zap.String("kbUID", param.KBUID.String()))

	kb, err := w.repository.GetKnowledgeBaseByUIDWithConfig(ctx, param.KBUID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to retrieve knowledge base configuration. Please try again.")
		return repository.SystemConfigJSON{}, temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			getKBSystemConfigActivityError,
			err,
		)
	}

	return kb.SystemConfig, nil
}

// Activity error type constants
const (
	getFileStatusActivityError     = "GetFileStatusActivity"
	updateFileStatusActivityError  = "UpdateFileStatusActivity"
	getKBSystemConfigActivityError = "GetKBSystemConfigActivity"
)
