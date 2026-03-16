package worker

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
)

const (
	orphanSweeperInterval  = 5 * time.Minute
	orphanStalenessTimeout = 15 * time.Minute
)

// StartOrphanSweeper launches a background goroutine that periodically checks
// for files stuck in non-terminal processing states whose Temporal workflows
// are no longer running. This catches all edge cases: external termination from
// Temporal UI, worker crashes, infrastructure failures, etc.
func (w *Worker) StartOrphanSweeper(ctx context.Context) {
	go func() {
		w.log.Info("Orphan file sweeper started",
			zap.Duration("interval", orphanSweeperInterval),
			zap.Duration("stalenessTimeout", orphanStalenessTimeout))

		ticker := time.NewTicker(orphanSweeperInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				w.log.Info("Orphan file sweeper stopped")
				return
			case <-ticker.C:
				w.sweepOrphanedFiles(ctx)
			}
		}
	}()
}

// sweepOrphanedFiles queries the DB for files in non-terminal processing states
// with stale update_time, checks their Temporal workflow status, and marks them
// as FAILED if the workflow is no longer running.
func (w *Worker) sweepOrphanedFiles(ctx context.Context) {
	nonTerminalStatuses := []string{
		artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_NOTSTARTED.String(),
		artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_PROCESSING.String(),
		artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING.String(),
		artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING.String(),
	}

	staleBefore := time.Now().Add(-orphanStalenessTimeout)

	var staleFiles []repository.FileModel
	err := w.repository.GetDB().WithContext(ctx).
		Where(repository.FileColumn.ProcessStatus+" IN ?", nonTerminalStatuses).
		Where(repository.FileColumn.UpdateTime+" < ?", staleBefore).
		Where(repository.FileColumn.DeleteTime+" IS NULL").
		Find(&staleFiles).Error
	if err != nil {
		w.log.Error("Orphan sweeper: failed to query stale files", zap.Error(err))
		return
	}

	if len(staleFiles) == 0 {
		return
	}

	w.log.Info("Orphan sweeper: found stale files to check",
		zap.Int("count", len(staleFiles)))

	reconciled := 0
	for _, file := range staleFiles {
		workflowID := fmt.Sprintf("process-file-%s", file.UID.String())

		desc, err := w.temporalClient.DescribeWorkflowExecution(ctx, workflowID, "")
		if err != nil {
			// Workflow not found — mark as FAILED
			w.reconcileOrphanedFile(ctx, file.UID, "File processing workflow is no longer running")
			reconciled++
			continue
		}

		status := desc.WorkflowExecutionInfo.Status
		if status != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
			w.reconcileOrphanedFile(ctx, file.UID, fmt.Sprintf("File processing workflow ended with status %s", status.String()))
			reconciled++
		}
	}

	if reconciled > 0 {
		w.log.Info("Orphan sweeper: reconciled stale files",
			zap.Int("reconciled", reconciled),
			zap.Int("total_checked", len(staleFiles)))
	}
}

// reconcileOrphanedFile marks a single orphaned file as FAILED in the database.
func (w *Worker) reconcileOrphanedFile(ctx context.Context, fileUID types.FileUIDType, message string) {
	updateMap := map[string]any{
		repository.FileColumn.ProcessStatus: artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_FAILED.String(),
	}
	if _, err := w.repository.UpdateFile(ctx, fileUID.String(), updateMap); err != nil {
		w.log.Warn("Orphan sweeper: failed to update file status",
			zap.String("fileUID", fileUID.String()),
			zap.Error(err))
		return
	}

	if err := w.repository.UpdateFileMetadata(ctx, fileUID, repository.ExtraMetaData{StatusMessage: message}); err != nil {
		w.log.Warn("Orphan sweeper: failed to update file metadata",
			zap.String("fileUID", fileUID.String()),
			zap.Error(err))
	}

	w.log.Info("Orphan sweeper: marked orphaned file as FAILED",
		zap.String("fileUID", fileUID.String()),
		zap.String("message", message))
}
