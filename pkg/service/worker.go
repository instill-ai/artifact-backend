package service

import (
	"context"

	"github.com/instill-ai/artifact-backend/pkg/types"
	"github.com/instill-ai/artifact-backend/pkg/worker"
)

// Worker defines the interface for workflow orchestration.
// This interface allows the service to delegate to the worker without creating
// a circular dependency (service → worker → service).
type Worker interface {
	// ProcessFile orchestrates the file processing workflow for one or more files
	ProcessFile(ctx context.Context, kbUID types.KBUIDType, fileUIDs []types.FileUIDType, userUID, requesterUID types.RequesterUIDType) error

	// ProcessFileDualMode processes files for both production and staging KBs during an update
	// This ensures files uploaded during an update are immediately queryable (in production)
	// while also being ready with the new configuration after swap (in staging)
	ProcessFileDualMode(ctx context.Context, prodKBUID, stagingKBUID types.KBUIDType, fileUIDs []types.FileUIDType, userUID, requesterUID types.RequesterUIDType) error

	// CleanupFile orchestrates the file cleanup workflow
	CleanupFile(ctx context.Context, fileUID types.FileUIDType, userUID, requesterUID types.RequesterUIDType, workflowID string, includeOriginalFile bool) error

	// GetFilesByPaths retrieves files by their paths using workflow
	GetFilesByPaths(ctx context.Context, bucket string, filePaths []string) ([]worker.FileContent, error)

	// DeleteFiles deletes files using workflow
	DeleteFiles(ctx context.Context, bucket string, filePaths []string) error

	// CleanupKnowledgeBase cleans up a knowledge base using workflow
	CleanupKnowledgeBase(ctx context.Context, kbUID types.KBUIDType) error

	// ExecuteKnowledgeBaseUpdate triggers knowledge base updates for specified catalogs
	// This is the entry point for the 6-phase UpdateKnowledgeBaseWorkflow
	// If systemProfile is specified, uses config from that profile; otherwise uses KB's current config
	ExecuteKnowledgeBaseUpdate(ctx context.Context, catalogIDs []string, systemProfile string) (*worker.UpdateRAGIndexResult, error)

	// RescheduleCleanupWorkflow cancels the existing cleanup workflow for a rollback KB
	// and starts a new one with updated retention period
	RescheduleCleanupWorkflow(ctx context.Context, kbUID types.KBUIDType, cleanupAfterSeconds int64) error
}
