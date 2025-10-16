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

	// CleanupFile orchestrates the file cleanup workflow
	CleanupFile(ctx context.Context, fileUID types.FileUIDType, userUID, requesterUID types.RequesterUIDType, workflowID string, includeOriginalFile bool) error

	// GetFilesByPaths retrieves files by their paths using workflow
	GetFilesByPaths(ctx context.Context, bucket string, filePaths []string) ([]worker.FileContent, error)

	// DeleteFiles deletes files using workflow
	DeleteFiles(ctx context.Context, bucket string, filePaths []string) error

	// CleanupKnowledgeBase cleans up a knowledge base using workflow
	CleanupKnowledgeBase(ctx context.Context, kbUID types.KBUIDType) error

	// EmbedTexts embeds texts with a specific task type optimization
	// kbUID is optional and used to select the appropriate provider based on KB's embedding config
	// taskType specifies the optimization (e.g., gemini.TaskTypeRetrievalDocument, gemini.TaskTypeRetrievalQuery, gemini.TaskTypeQuestionAnswering)
	EmbedTexts(ctx context.Context, kbUID *types.KBUIDType, texts []string, taskType string) ([][]float32, error)
}
