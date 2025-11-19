package worker

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	"github.com/redis/go-redis/v9"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/acl"
	"github.com/instill-ai/artifact-backend/pkg/ai"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
	errorsx "github.com/instill-ai/x/errors"
)

// TaskQueue is the Temporal task queue name for all workflows and activities.
const TaskQueue = "artifact-backend"

// TextChunkSize controls tokens per text chunk. Smaller = more embeddings = slower. Larger = faster but less precise.
// TextChunkOverlap controls overlap between text chunks (20% = good context continuity).
const (
	TextChunkSize    = 1000 // Balance: cost vs precision
	TextChunkOverlap = 200  // Balance: context vs redundancy
)

// EmbeddingBatchSize controls embeddings per DB transaction. Smaller = more parallelism + overhead. Larger = less overhead + contention.
// Example: 10 files × 1000 embeddings ÷ 50 = 200 parallel activities. Increase to 100-200 under high load.
const EmbeddingBatchSize = 50

// ActivityTimeoutStandard is timeout for normal activities. ActivityTimeoutLong is for heavy operations.
// Too short = premature failures. Too long = blocked worker slots.
const (
	ActivityTimeoutStandard = 1 * time.Minute // File I/O, DB, MinIO
	ActivityTimeoutLong     = 5 * time.Minute // File conversion, embeddings, synchronization with reconciliation
)

// RetryInitialInterval, RetryBackoffCoefficient, RetryMaximumInterval*, and RetryMaximumAttempts control retry behavior.
// Prevents retry storms under high concurrency.
const (
	RetryInitialInterval         = 1 * time.Second   // Prevents retry storms
	RetryBackoffCoefficient      = 2.0               // Exponential: 1s→2s→4s→8s→16s→32s→64s→100s (capped)
	RetryMaximumIntervalStandard = 30 * time.Second  // Caps exponential backoff for transient failures
	RetryMaximumIntervalLong     = 100 * time.Second // Service recovery (AI client rate limits)
	RetryMaximumAttempts         = 8                 // 8 attempts = up to ~3 minutes with backoff (handles AI rate limiting)
)

// PostFileCompletionFn allows extensions of the worker to provide some logic
// to be executed after a file is successfully processed.
type PostFileCompletionFn func(_ workflow.Context, file *repository.KnowledgeBaseFileModel, effectiveFileType artifactpb.File_Type) error

// Worker implements the Temporal worker with all workflows and activities
type Worker struct {
	// Infrastructure dependencies (primitives)
	repository     repository.Repository
	pipelineClient pipelinepb.PipelinePublicServiceClient
	aclClient      *acl.ACLClient
	redisClient    *redis.Client

	// Temporal client for workflow execution
	temporalClient client.Client

	// Worker-specific dependencies
	aiClient ai.Client // AI client (can be single or composite with routing capabilities)
	log      *zap.Logger

	postFileCompletion PostFileCompletionFn
}

// TemporalClient returns the Temporal client for workflow execution
func (w *Worker) TemporalClient() client.Client {
	return w.temporalClient
}

// GetAIClient returns the AI client for external use (e.g., service layer)
// This client includes routing capabilities via GetClientForModelFamily
// Returns as interface{} (any) to avoid circular imports in the service package
func (w *Worker) GetAIClient() any {
	return w.aiClient
}

// GetRepository returns the repository for external use (e.g., EE worker overrides)
func (w *Worker) GetRepository() repository.Repository {
	return w.repository
}

// GetPipelineClient returns the pipeline client for external use (e.g., EE worker overrides)
func (w *Worker) GetPipelineClient() pipelinepb.PipelinePublicServiceClient {
	return w.pipelineClient
}

// GetLogger returns the logger for external use (e.g., EE worker overrides)
func (w *Worker) GetLogger() *zap.Logger {
	return w.log
}

// SetPostFileCompletionFn allows clients to add logic for files that have been
// successfully processed.
func (w *Worker) SetPostFileCompletionFn(fn PostFileCompletionFn) {
	w.postFileCompletion = fn
}

// New creates a new worker instance with direct dependencies (no circular dependency)
func New(
	temporalClient client.Client,
	repo repository.Repository,
	pipelineClient pipelinepb.PipelinePublicServiceClient,
	aclClient *acl.ACLClient,
	redisClient *redis.Client,
	log *zap.Logger,
	ai ai.Client,
) (*Worker, error) {
	w := &Worker{
		repository:     repo,
		pipelineClient: pipelineClient,
		aclClient:      aclClient,
		redisClient:    redisClient,
		temporalClient: temporalClient,
		log:            log,
		aiClient:       ai,
	}

	return w, nil
}

// Use-case methods for workflow orchestration
// These provide a clean interface for handlers to trigger workflows

// ProcessFile orchestrates the file processing workflow for one or more files
func (w *Worker) ProcessFile(ctx context.Context, kbUID types.KBUIDType, fileUIDs []types.FileUIDType, userUID, requesterUID types.RequesterUIDType) error {
	workflow := NewProcessFileWorkflow(w.temporalClient, w)
	return workflow.Execute(ctx, ProcessFileWorkflowParam{
		KBUID:        kbUID,
		FileUIDs:     fileUIDs,
		UserUID:      userUID,
		RequesterUID: requesterUID,
	})
}

// ProcessFileDualMode processes files for both production and staging KBs in parallel
// Used when files are uploaded during a RAG index update to ensure:
// 1. Files are immediately queryable in production (old config)
// 2. Files are ready in staging with new config (for seamless swap)
func (w *Worker) ProcessFileDualMode(ctx context.Context, prodKBUID, stagingKBUID types.KBUIDType, fileUIDs []types.FileUIDType, userUID, requesterUID types.RequesterUIDType) error {
	// Process for production KB (primary, must succeed)
	errProd := w.ProcessFile(ctx, prodKBUID, fileUIDs, userUID, requesterUID)
	if errProd != nil {
		w.log.Error("Production KB processing failed during dual processing",
			zap.Error(errProd),
			zap.String("prodKBUID", prodKBUID.String()),
			zap.Strings("fileUIDs", func() []string {
				strs := make([]string, len(fileUIDs))
				for i, uid := range fileUIDs {
					strs[i] = uid.String()
				}
				return strs
			}()))
		return errProd
	}

	// Process for staging KB (secondary, failure is non-fatal)
	// If this fails, file will be reprocessed in next update cycle
	if err := w.ProcessFile(ctx, stagingKBUID, fileUIDs, userUID, requesterUID); err != nil {
		w.log.Warn("Staging KB processing failed during dual processing, file will be reprocessed in next update",
			zap.Error(err),
			zap.String("stagingKBUID", stagingKBUID.String()),
			zap.Strings("fileUIDs", func() []string {
				strs := make([]string, len(fileUIDs))
				for i, uid := range fileUIDs {
					strs[i] = uid.String()
				}
				return strs
			}()))
		// Don't return error - production is what matters for user experience
	} else {
		w.log.Info("Dual processing completed successfully",
			zap.String("prodKBUID", prodKBUID.String()),
			zap.String("stagingKBUID", stagingKBUID.String()),
			zap.Int("fileCount", len(fileUIDs)))
	}

	return nil
}

// CleanupFile orchestrates the file cleanup workflow
func (w *Worker) CleanupFile(ctx context.Context, fileUID types.FileUIDType, userUID, requesterUID types.RequesterUIDType, workflowID string, includeOriginalFile bool) error {
	workflow := NewCleanupFileWorkflow(w.temporalClient, w)
	return workflow.Execute(ctx, CleanupFileWorkflowParam{
		FileUID:             fileUID,
		UserUID:             userUID,
		RequesterUID:        requesterUID,
		WorkflowID:          workflowID,
		IncludeOriginalFile: includeOriginalFile,
	})
}

// ===== MinIO Batch Activities =====

// FileContent represents file content with metadata
type FileContent struct {
	Index   int
	Name    string
	Content []byte
}

// DeleteFilesBatchActivityParam defines parameters for deleting multiple files
type DeleteFilesBatchActivityParam struct {
	Bucket    string   // MinIO bucket containing the files
	FilePaths []string // MinIO paths to the files
}

// DeleteFilesBatchActivity deletes multiple files from MinIO in parallel using goroutines
// This is more efficient than using a workflow for simple parallel I/O operations
func (w *Worker) DeleteFilesBatchActivity(ctx context.Context, param *DeleteFilesBatchActivityParam) error {
	w.log.Info("Starting DeleteFilesBatchActivity",
		zap.String("bucket", param.Bucket),
		zap.Int("fileCount", len(param.FilePaths)))

	if len(param.FilePaths) == 0 {
		return nil
	}

	// Use buffered channel to collect errors
	errChan := make(chan error, len(param.FilePaths))

	// Delete files in parallel using goroutines
	for _, filePath := range param.FilePaths {
		filePath := filePath // Capture loop variable
		go func() {
			err := w.repository.GetMinIOStorage().DeleteFile(ctx, param.Bucket, filePath)
			errChan <- err
		}()
	}

	// Collect results and check for errors
	var errors []error
	for i := 0; i < len(param.FilePaths); i++ {
		if err := <-errChan; err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		// Return first error
		err := errorsx.AddMessage(errors[0], fmt.Sprintf("Failed to delete %d/%d files. Please try again.", len(errors), len(param.FilePaths)))
		return temporal.NewApplicationErrorWithCause(
			errorsx.MessageOrErr(err),
			"DeleteFilesBatchActivity",
			err,
		)
	}

	w.log.Info("DeleteFilesBatchActivity completed successfully",
		zap.Int("filesDeleted", len(param.FilePaths)))

	return nil
}

// GetFilesBatchActivityParam defines parameters for getting multiple files
type GetFilesBatchActivityParam struct {
	Bucket    string           // MinIO bucket containing the files
	FilePaths []string         // MinIO paths to the files
	Metadata  *structpb.Struct // Request metadata for authentication context
}

// GetFilesBatchActivityResult contains the batch file retrieval results
type GetFilesBatchActivityResult struct {
	Files []FileContent // Retrieved files in order
}

// GetFilesBatchActivity retrieves multiple files from MinIO in parallel using goroutines
// This is more efficient than using a workflow for simple parallel I/O operations
func (w *Worker) GetFilesBatchActivity(ctx context.Context, param *GetFilesBatchActivityParam) (*GetFilesBatchActivityResult, error) {
	w.log.Info("Starting GetFilesBatchActivity",
		zap.String("bucket", param.Bucket),
		zap.Int("fileCount", len(param.FilePaths)))

	if len(param.FilePaths) == 0 {
		return &GetFilesBatchActivityResult{Files: []FileContent{}}, nil
	}

	// Create authenticated context if metadata provided
	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = CreateAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			err = errorsx.AddMessage(err, "Authentication failed. Please try again.")
			return nil, temporal.NewApplicationErrorWithCause(
				errorsx.MessageOrErr(err),
				"GetFilesBatchActivity",
				err,
			)
		}
	}

	// Use buffered channel to collect results
	type result struct {
		index   int
		content []byte
		err     error
	}
	resultChan := make(chan result, len(param.FilePaths))

	// Get files in parallel using goroutines
	for i, filePath := range param.FilePaths {
		i := i // Capture loop variable
		filePath := filePath
		go func() {
			content, err := w.repository.GetMinIOStorage().GetFile(authCtx, param.Bucket, filePath)
			resultChan <- result{index: i, content: content, err: err}
		}()
	}

	// Collect results and check for errors
	results := make([]FileContent, len(param.FilePaths))
	for i := 0; i < len(param.FilePaths); i++ {
		res := <-resultChan
		if res.err != nil {
			err := errorsx.AddMessage(res.err, fmt.Sprintf("Failed to retrieve file: %s. Please try again.", param.FilePaths[res.index]))
			return nil, temporal.NewApplicationErrorWithCause(
				errorsx.MessageOrErr(err),
				"GetFilesBatchActivity",
				err,
			)
		}
		results[res.index] = FileContent{
			Index:   res.index,
			Name:    filepath.Base(param.FilePaths[res.index]),
			Content: res.content,
		}
	}

	w.log.Info("GetFilesBatchActivity completed successfully",
		zap.Int("filesRetrieved", len(results)))

	return &GetFilesBatchActivityResult{Files: results}, nil
}

// GetFilesByPaths retrieves files by their paths using batch activity
// Batch activities are more efficient than workflows for simple parallel I/O operations
func (w *Worker) GetFilesByPaths(ctx context.Context, bucket string, filePaths []string) ([]FileContent, error) {
	result, err := w.GetFilesBatchActivity(ctx, &GetFilesBatchActivityParam{
		Bucket:    bucket,
		FilePaths: filePaths,
	})
	if err != nil {
		return nil, err
	}
	return result.Files, nil
}

// DeleteFiles deletes files using batch activity
// Batch activities are more efficient than workflows for simple parallel I/O operations
func (w *Worker) DeleteFiles(ctx context.Context, bucket string, filePaths []string) error {
	return w.DeleteFilesBatchActivity(ctx, &DeleteFilesBatchActivityParam{
		Bucket:    bucket,
		FilePaths: filePaths,
	})
}

// CleanupKnowledgeBase cleans up a knowledge base using workflow
func (w *Worker) CleanupKnowledgeBase(ctx context.Context, kbUID types.KBUIDType) error {
	workflow := NewCleanupKnowledgeBaseWorkflow(w.temporalClient, w)
	return workflow.Execute(ctx, CleanupKnowledgeBaseWorkflowParam{
		KBUID: kbUID,
	})
}

// UpdateRAGIndexResult contains the result of knowledge base update execution
type UpdateRAGIndexResult struct {
	Started bool
	Message string
}

// ExecuteKnowledgeBaseUpdate triggers knowledge base updates for specified knowledge bases
// This method is the entry point for the 6-phase UpdateKnowledgeBaseWorkflow
// If knowledgeBaseIDs is empty, updates all eligible KBs. Otherwise, updates only specified KBs.
// If systemID is specified, uses config from that system; otherwise uses KB's current config.
func (w *Worker) ExecuteKnowledgeBaseUpdate(ctx context.Context, knowledgeBaseIDs []string, systemID string) (*UpdateRAGIndexResult, error) {
	// Get list of KBs to update
	var kbs []repository.KnowledgeBaseModel
	var err error

	if len(knowledgeBaseIDs) > 0 {
		// Update specific knowledge bases
		for _, knowledgeBaseID := range knowledgeBaseIDs {
			kb, getErr := w.repository.GetKnowledgeBaseByID(ctx, knowledgeBaseID)
			if getErr != nil {
				w.log.Warn("Unable to get knowledge base for update",
					zap.String("knowledgeBaseID", knowledgeBaseID),
					zap.Error(getErr))
				continue
			}
			// Only include production KBs (not staging) and not already updating
			if !kb.Staging && repository.IsUpdateComplete(kb.UpdateStatus) {
				kbs = append(kbs, *kb)
			}
		}
	} else {
		// Update all eligible KBs
		kbs, err = w.repository.ListKnowledgeBasesForUpdate(ctx, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to list eligible knowledge bases: %w", err)
		}
	}

	if len(kbs) == 0 {
		return &UpdateRAGIndexResult{
			Started: false,
			Message: "No eligible knowledge bases found for update",
		}, nil
	}

	// Start UpdateKnowledgeBaseWorkflow for each KB with concurrency control
	// Use semaphore to limit concurrent KB updates and prevent resource surge
	maxConcurrent := config.Config.RAG.Update.MaxConcurrentKBUpdates
	if maxConcurrent <= 0 {
		maxConcurrent = 5 // Default to 5 concurrent KBs
	}

	semaphore := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup
	var mu sync.Mutex
	successCount := 0

	for _, kb := range kbs {
		wg.Add(1)

		// Acquire semaphore slot
		semaphore <- struct{}{}

		go func(kb repository.KnowledgeBaseModel) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release slot when done

			// Parse owner UID
			ownerUID, err := uuid.FromString(kb.Owner)
			if err != nil {
				w.log.Error("Failed to parse owner UID",
					zap.String("knowledgeBaseID", kb.KBID),
					zap.String("kbUID", kb.UID.String()),
					zap.Error(err))
				return
			}

			// Start child workflow for this KB
			// Use unique workflow ID by adding timestamp to support rollback → re-update scenarios
			workflowOptions := client.StartWorkflowOptions{
				ID:                       fmt.Sprintf("update-kb-%s-%d", kb.UID.String(), time.Now().UnixNano()),
				TaskQueue:                "artifact-backend",
				WorkflowExecutionTimeout: 24 * time.Hour,
			}

			_, err = w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, w.UpdateKnowledgeBaseWorkflow, UpdateKnowledgeBaseWorkflowParam{
				OriginalKBUID:         kb.UID,
				UserUID:               types.UserUIDType(ownerUID),
				RequesterUID:          types.RequesterUIDType(kb.CreatorUID),
				SystemID:              systemID,
				RollbackRetentionDays: config.Config.RAG.Update.RollbackRetentionDays,
				FileBatchSize:         config.Config.RAG.Update.FileBatchSize,
				MinioBucket:           config.Config.Minio.BucketName,
			})

			if err != nil {
				w.log.Error("Failed to start update workflow for KB",
					zap.String("knowledgeBaseID", kb.KBID),
					zap.String("kbUID", kb.UID.String()),
					zap.Error(err))
				return
			}

			mu.Lock()
			successCount++
			mu.Unlock()

			w.log.Info("Update workflow started for KB",
				zap.String("knowledgeBaseID", kb.KBID),
				zap.String("kbUID", kb.UID.String()))
		}(kb)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	if successCount == 0 {
		return &UpdateRAGIndexResult{
			Started: false,
			Message: "Failed to start update workflows for any knowledge bases",
		}, fmt.Errorf("failed to start any update workflows")
	}

	return &UpdateRAGIndexResult{
		Started: true,
		Message: fmt.Sprintf("Knowledge base update initiated for %d/%d knowledge bases (max %d concurrent)", successCount, len(kbs), maxConcurrent),
	}, nil
}

// AbortKBUpdateResult holds results of abort operation
type AbortKBUpdateResult struct {
	Success             bool
	Message             string
	AbortedCount        int
	KnowledgeBaseStatus []KnowledgeBaseAbortStatus
}

// KnowledgeBaseAbortStatus holds status for an individual aborted knowledge base
type KnowledgeBaseAbortStatus struct {
	KnowledgeBaseID  string // Keep field name for backward compatibility with existing code
	KnowledgeBaseUID string // Keep field name for backward compatibility with existing code
	WorkflowID       string
	Status           string
	ErrorMessage     string
}

// AbortKnowledgeBaseUpdate aborts ongoing KB update workflows and cleans up staging resources
func (w *Worker) AbortKnowledgeBaseUpdate(ctx context.Context, knowledgeBaseIDs []string) (*AbortKBUpdateResult, error) {
	// Get list of KBs currently updating
	var kbs []repository.KnowledgeBaseModel

	if len(knowledgeBaseIDs) > 0 {
		// Abort specific knowledge bases
		for _, knowledgeBaseID := range knowledgeBaseIDs {
			kb, getErr := w.repository.GetKnowledgeBaseByID(ctx, knowledgeBaseID)
			if getErr != nil {
				w.log.Warn("Unable to get knowledge base for abort",
					zap.String("knowledgeBaseID", knowledgeBaseID),
					zap.Error(getErr))
				continue
			}
			// Only include KBs currently in an active update state
			if repository.IsUpdateInProgress(kb.UpdateStatus) && kb.UpdateWorkflowID != "" {
				kbs = append(kbs, *kb)
			}
		}
	} else {
		// Abort all currently in-progress KBs (UPDATING, SYNCING, VALIDATING, SWAPPING)
		inProgressStatuses := []string{
			artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_UPDATING.String(),
			artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_SYNCING.String(),
			artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_VALIDATING.String(),
			artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_SWAPPING.String(),
		}
		for _, status := range inProgressStatuses {
			statusKBs, err := w.repository.ListKnowledgeBasesByUpdateStatus(ctx, status)
			if err != nil {
				w.log.Warn("Failed to list KBs by status",
					zap.String("status", status),
					zap.Error(err))
				continue
			}
			for _, kb := range statusKBs {
				if kb.UpdateWorkflowID != "" {
					kbs = append(kbs, kb)
				}
			}
		}
	}

	if len(kbs) == 0 {
		return &AbortKBUpdateResult{
			Success: true,
			Message: "No knowledge bases currently updating",
		}, nil
	}

	// Abort each KB update
	abortedCount := 0
	var knowledgeBaseStatuses []KnowledgeBaseAbortStatus

	for _, kb := range kbs {
		status := KnowledgeBaseAbortStatus{
			KnowledgeBaseID:  kb.KBID,
			KnowledgeBaseUID: kb.UID.String(),
			WorkflowID:       kb.UpdateWorkflowID,
			Status:           artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_ABORTED.String(),
		}

		// Cancel the workflow
		if kb.UpdateWorkflowID != "" {
			err := w.temporalClient.CancelWorkflow(ctx, kb.UpdateWorkflowID, "")
			if err != nil {
				w.log.Warn("Failed to cancel workflow (may have already completed)",
					zap.String("knowledgeBaseID", kb.KBID),
					zap.String("workflowID", kb.UpdateWorkflowID),
					zap.Error(err))
				// Continue with cleanup even if cancel fails
			}
		}

		// Find and cleanup staging KB
		stagingKBID := fmt.Sprintf("%s-staging", kb.KBID)
		ownerUID, err := uuid.FromString(kb.Owner)
		if err != nil {
			w.log.Warn("Failed to parse owner UID",
				zap.String("knowledgeBaseID", kb.KBID),
				zap.String("owner", kb.Owner),
				zap.Error(err))
		} else {
			stagingKB, err := w.repository.GetKnowledgeBaseByOwnerAndKbID(ctx, types.OwnerUIDType(ownerUID), stagingKBID)
			if err == nil && stagingKB != nil {
				// Cleanup staging KB resources
				err = w.CleanupOldKnowledgeBaseActivity(ctx, &CleanupOldKnowledgeBaseActivityParam{
					KBUID: stagingKB.UID,
				})
				if err != nil {
					w.log.Warn("Failed to cleanup staging KB",
						zap.String("stagingKBUID", stagingKB.UID.String()),
						zap.Error(err))
					status.ErrorMessage = fmt.Sprintf("failed to cleanup staging KB: %v", err)
				}
			}
		}

		// Update original KB status to "aborted" and clear workflow ID
		// CRITICAL: We need to explicitly set update_workflow_id to NULL (not empty string)
		// so that the DeleteCatalog safeguards don't block deletion
		// Using "" with Updates() doesn't set it to NULL in the DB
		err = w.repository.UpdateKnowledgeBaseAborted(ctx, kb.UID)
		if err != nil {
			w.log.Error("Failed to update KB status to aborted",
				zap.String("knowledgeBaseID", kb.KBID),
				zap.String("kbUID", kb.UID.String()),
				zap.Error(err))
			status.ErrorMessage = fmt.Sprintf("failed to update status: %v", err)
			status.Status = artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_FAILED.String()
		} else {
			abortedCount++
			w.log.Info("Successfully aborted KB update",
				zap.String("knowledgeBaseID", kb.KBID),
				zap.String("kbUID", kb.UID.String()),
				zap.String("workflowID", kb.UpdateWorkflowID))
		}

		knowledgeBaseStatuses = append(knowledgeBaseStatuses, status)
	}

	if abortedCount == 0 {
		return &AbortKBUpdateResult{
			Success:             false,
			Message:             "Failed to abort any knowledge base updates",
			AbortedCount:        0,
			KnowledgeBaseStatus: knowledgeBaseStatuses,
		}, fmt.Errorf("failed to abort any knowledge base updates")
	}

	return &AbortKBUpdateResult{
		Success:             true,
		Message:             fmt.Sprintf("Successfully aborted %d/%d knowledge base updates", abortedCount, len(kbs)),
		AbortedCount:        abortedCount,
		KnowledgeBaseStatus: knowledgeBaseStatuses,
	}, nil
}

// RescheduleCleanupWorkflow terminates the existing cleanup workflow for a rollback KB
// and starts a new one with updated retention period.
// This is used by SetRollbackRetention to update the cleanup schedule.
func (w *Worker) RescheduleCleanupWorkflow(ctx context.Context, kbUID types.KBUIDType, cleanupAfterSeconds int64) error {
	workflowID := fmt.Sprintf("cleanup-rollback-kb-%s", kbUID.String())

	// CRITICAL: Terminate (not cancel) the old cleanup workflow
	// Terminate is immediate and synchronous, while Cancel is async and the workflow keeps running
	// until it checks for cancellation. We need immediate termination so we can reuse the workflow ID.
	err := w.temporalClient.TerminateWorkflow(ctx, workflowID, "", "Retention period updated, rescheduling cleanup")
	if err != nil {
		// Log but don't fail - workflow might not exist or already completed
		w.log.Info("Could not terminate old cleanup workflow (may not exist or already completed)",
			zap.String("workflowID", workflowID),
			zap.Error(err))
	}

	// Start new cleanup workflow with updated retention
	// Use the same workflow ID - now safe because old workflow was terminated
	workflowOptions := client.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                "artifact-backend",
		WorkflowExecutionTimeout: 7 * 24 * time.Hour, // Max 7 days
	}

	_, err = w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, w.CleanupKnowledgeBaseWorkflow, CleanupKnowledgeBaseWorkflowParam{
		KBUID:               kbUID,
		CleanupAfterSeconds: cleanupAfterSeconds,
		MinioBucket:         config.Config.Minio.BucketName,
	})
	if err != nil {
		w.log.Warn("Could not reschedule cleanup workflow",
			zap.String("kbUID", kbUID.String()),
			zap.Error(err))
		return fmt.Errorf("failed to reschedule cleanup workflow: %w", err)
	}

	w.log.Info("Cleanup workflow rescheduled successfully",
		zap.String("workflowID", workflowID),
		zap.String("kbUID", kbUID.String()),
		zap.Int64("cleanupAfterSeconds", cleanupAfterSeconds))

	return nil
}

// Helper methods (high-level operations used by activities)

// deleteFilesSync deletes multiple files from MinIO (synchronous implementation without workflow)
func (w *Worker) deleteFilesSync(ctx context.Context, bucket string, filePaths []string) error {
	if len(filePaths) == 0 {
		return nil
	}

	// Synchronous deletion (no workflow orchestration)
	// For parallel/reliable deletion, use worker.DeleteFiles workflow instead
	for _, filePath := range filePaths {
		if err := w.repository.GetMinIOStorage().DeleteFile(ctx, bucket, filePath); err != nil {
			return errorsx.AddMessage(
				fmt.Errorf("failed to delete file %s: %w", filePath, err),
				"Unable to delete file. Please try again.",
			)
		}
	}

	return nil
}

// deleteKnowledgeBaseSync deletes all files in a knowledge base
func (w *Worker) deleteKnowledgeBaseSync(ctx context.Context, kbUID string) error {
	kbUUID, err := uuid.FromString(kbUID)
	if err != nil {
		return errorsx.AddMessage(
			fmt.Errorf("invalid knowledge base UID: %w", err),
			"Invalid knowledge base identifier. Please check the knowledge base ID and try again.",
		)
	}

	filePaths, err := w.repository.GetMinIOStorage().ListKnowledgeBaseFilePaths(ctx, kbUUID)
	if err != nil {
		return errorsx.AddMessage(
			fmt.Errorf("failed to list knowledge base files: %w", err),
			"Unable to list knowledge base files. Please try again.",
		)
	}
	return w.deleteFilesSync(ctx, config.Config.Minio.BucketName, filePaths)
}

// deleteConvertedFileByFileUIDSync deletes converted files for a specific file UID
func (w *Worker) deleteConvertedFileByFileUIDSync(ctx context.Context, kbUID, fileUID types.FileUIDType) error {
	filePaths, err := w.repository.GetMinIOStorage().ListConvertedFilesByFileUID(ctx, kbUID, fileUID)
	if err != nil {
		return errorsx.AddMessage(
			fmt.Errorf("failed to list converted files: %w", err),
			"Unable to list converted files. Please try again.",
		)
	}
	return w.deleteFilesSync(ctx, config.Config.Minio.BucketName, filePaths)
}

// deleteTextChunksByFileUIDSync deletes text chunks for a specific file UID
func (w *Worker) deleteTextChunksByFileUIDSync(ctx context.Context, kbUID, fileUID types.FileUIDType) error {
	filePaths, err := w.repository.GetMinIOStorage().ListTextChunksByFileUID(ctx, kbUID, fileUID)
	if err != nil {
		return errorsx.AddMessage(
			fmt.Errorf("failed to list text chunk files: %w", err),
			"Unable to list text chunks. Please try again.",
		)
	}
	return w.deleteFilesSync(ctx, config.Config.Minio.BucketName, filePaths)
}

// getTextChunksByFile returns the text chunks of a file
// Fetches text chunk content directly from MinIO using parallel goroutines for better performance
// IMPORTANT: Returns ALL chunks for the file (content chunks + summary chunks)
func (w *Worker) getTextChunksByFile(ctx context.Context, file *repository.KnowledgeBaseFileModel) (
	types.SourceTableType,
	types.SourceUIDType,
	[]repository.TextChunkModel,
	map[types.TextChunkUIDType]string,
	[]string,
	error,
) {
	var sourceTable string
	var sourceUID types.SourceUIDType

	// Get ALL converted files for this file (there may be multiple: content + summary)
	convertedFiles, err := w.repository.GetAllConvertedFilesByFileUID(ctx, file.UID)
	if err != nil || len(convertedFiles) == 0 {
		// CRITICAL FIX: Create proper error if err is nil (when no converted files exist)
		if err == nil {
			err = errorsx.AddMessage(errorsx.ErrNotFound, fmt.Sprintf("No converted files found for file UID %s", file.UID.String()))
		}
		w.log.Error("Failed to get converted files metadata",
			zap.String("fileUID", file.UID.String()),
			zap.Error(err))
		return sourceTable, sourceUID, nil, nil, nil, errorsx.AddMessage(
			err,
			"Unable to retrieve file information. Please try again.",
		)
	}
	// Use the first converted file for source metadata (legacy compatibility)
	sourceTable = repository.ConvertedFileTableName
	sourceUID = convertedFiles[0].UID

	// Get ALL text chunks for this file (including both content and summary chunks)
	// This is critical for embedding generation - we need to embed ALL chunks, not just content chunks
	chunks, err := w.repository.ListTextChunksByKBFileUID(ctx, file.UID)
	if err != nil {
		w.log.Error("Failed to get text chunks from database", zap.String("fileUID", file.UID.String()), zap.Error(err))
		return sourceTable, sourceUID, nil, nil, nil, err
	}

	w.log.Info("getTextChunksByFile: Retrieved all chunks for file",
		zap.String("fileUID", file.UID.String()),
		zap.Int("totalChunks", len(chunks)))

	// CRITICAL FIX: Handle case where chunking hasn't completed yet (race condition)
	// If no chunks exist, the file is likely still being chunked - fail early
	if len(chunks) == 0 {
		w.log.Error("No text chunks found for file - chunking may not have completed",
			zap.String("fileUID", file.UID.String()))
		return sourceTable, sourceUID, nil, nil, nil, errorsx.AddMessage(
			fmt.Errorf("no text chunks found for file %s", file.UID.String()),
			"File has no text chunks. The chunking process may still be in progress. Please try again.",
		)
	}

	// Fetch text chunks from MinIO in parallel using goroutines with retry
	texts := make([]string, len(chunks))
	var wg sync.WaitGroup
	var mu sync.Mutex
	var fetchErr error

	bucket := config.Config.Minio.BucketName

	for i, chunk := range chunks {
		wg.Add(1)
		go func(idx int, path string) {
			defer wg.Done()

			// CRITICAL FIX: Check if context is already cancelled before starting
			if ctx.Err() != nil {
				mu.Lock()
				if fetchErr == nil {
					fetchErr = ctx.Err()
				}
				mu.Unlock()
				return
			}

			// Retry up to 3 times with exponential backoff for transient failures
			var content []byte
			var err error
			maxAttempts := 3

			for attempt := range maxAttempts {
				// Create a timeout context for this MinIO fetch (max 10s per attempt)
				fetchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				content, err = w.repository.GetMinIOStorage().GetFile(fetchCtx, bucket, path)
				cancel() // Always cancel to free resources

				if err == nil {
					break // Success!
				}

				// Check if parent context was cancelled
				if ctx.Err() != nil {
					err = ctx.Err()
					break
				}

				// Don't sleep after last attempt
				if attempt < maxAttempts-1 {
					// Exponential backoff: 1s, 2s (context-aware)
					backoff := time.Duration(1<<uint(attempt)) * time.Second
					select {
					case <-time.After(backoff):
						// Continue to next attempt
					case <-ctx.Done():
						err = ctx.Err()
						break
					}
				}
			}

			if err != nil {
				mu.Lock()
				if fetchErr == nil {
					fetchErr = errorsx.AddMessage(
						fmt.Errorf("failed to fetch text chunk %s after %d attempts: %w", path, maxAttempts, err),
						"Unable to retrieve text chunks. Please try again.",
					)
				}
				mu.Unlock()
				return
			}

			mu.Lock()
			texts[idx] = string(content)
			mu.Unlock()
		}(i, chunk.ContentDest)
	}

	wg.Wait()

	if fetchErr != nil {
		w.log.Error("Failed to get text chunks from MinIO.",
			zap.String("SourceTable", sourceTable),
			zap.String("SourceUID", sourceUID.String()),
			zap.Error(fetchErr))
		return sourceTable, sourceUID, nil, nil, nil, fetchErr
	}

	// Build text chunk UID to content text map
	chunkUIDToContents := make(map[types.TextChunkUIDType]string, len(chunks))
	for i, c := range chunks {
		chunkUIDToContents[c.UID] = texts[i]
	}

	return sourceTable, sourceUID, chunks, chunkUIDToContents, texts, nil
}
