package worker

import (
	"context"

	"github.com/gofrs/uuid"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"
	"github.com/instill-ai/artifact-backend/pkg/temporal"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// Worker interface defines the methods for the worker
type Worker interface {
	// File Processing - Workflows
	ProcessFileWorkflow(workflow.Context, temporal.ProcessFileWorkflowParam) error

	// File Processing - Activities
	ConvertFileActivity(context.Context, *ConvertFileActivityParam) error
	ChunkFileActivity(context.Context, *ChunkFileActivityParam) error
	EmbedFileActivity(context.Context, *EmbedFileActivityParam) error
	GenerateSummaryActivity(context.Context, *GenerateSummaryActivityParam) error

	// Embedding - Workflows
	EmbedTextsWorkflow(workflow.Context, temporal.EmbedTextsWorkflowParam) ([][]float32, error)

	// Embedding - Activities
	EmbedTextsActivity(context.Context, *EmbedTextsActivityParam) ([][]float32, error)

	// MinIO Operations - Workflows
	SaveChunksWorkflow(workflow.Context, temporal.SaveChunksWorkflowParam) (map[string]string, error)
	DeleteFilesWorkflow(workflow.Context, temporal.DeleteFilesWorkflowParam) error
	GetFilesWorkflow(workflow.Context, temporal.GetFilesWorkflowParam) ([]temporal.FileContent, error)

	// MinIO Operations - Activities
	SaveChunkActivity(context.Context, *SaveChunkActivityParam) (*SaveChunkActivityResult, error)
	DeleteFileActivity(context.Context, *DeleteFileActivityParam) error
	GetFileActivity(context.Context, *GetFileActivityParam) (*GetFileActivityResult, error)

	// Cleanup - Workflows
	CleanupFileWorkflow(workflow.Context, temporal.CleanupFileWorkflowParam) error
	CleanupKnowledgeBaseWorkflow(workflow.Context, temporal.CleanupKnowledgeBaseWorkflowParam) error

	// Cleanup - Activities
	CleanupFilesActivity(context.Context, *CleanupFilesActivityParam) error
	CleanupKnowledgeBaseActivity(context.Context, temporal.CleanupKnowledgeBaseWorkflowParam) error

	// Status - Activities
	GetFileStatusActivity(context.Context, uuid.UUID) (artifactpb.FileProcessStatus, error)
	ProcessWaitingFileActivity(context.Context, *ProcessWaitingFileActivityParam) (artifactpb.FileProcessStatus, error)
	UpdateFileStatusActivity(context.Context, *UpdateFileStatusActivityParam) error
	NotifyFileProcessedActivity(context.Context, *NotifyFileProcessedActivityParam) error
}

// Config defines the configuration for the worker
type Config struct {
	Repository repository.RepositoryI
	Service    service.Service
}

// worker implements the Worker interface
type worker struct {
	repository repository.RepositoryI
	service    service.Service
	log        *zap.Logger
}

// New creates a new worker instance
func New(config Config, log *zap.Logger) (Worker, error) {
	w := &worker{
		repository: config.Repository,
		service:    config.Service,
		log:        log,
	}
	return w, nil
}

// Activity Parameter Types

// ConvertFileActivityParam defines the parameters for the ConvertFileActivity
type ConvertFileActivityParam struct {
	FileUID          uuid.UUID
	KnowledgeBaseUID uuid.UUID
	UserUID          uuid.UUID
	ConversionType   string
}

// ChunkFileActivityParam defines the parameters for the ChunkFileActivity
type ChunkFileActivityParam struct {
	FileUID          uuid.UUID
	KnowledgeBaseUID uuid.UUID
	UserUID          uuid.UUID
	ChunkSize        int
	ChunkOverlap     int
}

// EmbedFileActivityParam defines the parameters for the EmbedFileActivity
type EmbedFileActivityParam struct {
	FileUID          uuid.UUID
	KnowledgeBaseUID uuid.UUID
	UserUID          uuid.UUID
	EmbeddingModel   string
}

// EmbedTextsActivityParam defines the parameters for the EmbedTextsActivity
type EmbedTextsActivityParam struct {
	Texts           []string
	BatchIndex      int
	RequestMetadata map[string][]string // gRPC metadata for authentication
}

// GenerateSummaryActivityParam defines the parameters for the GenerateSummaryActivity
type GenerateSummaryActivityParam struct {
	FileUID          uuid.UUID
	KnowledgeBaseUID uuid.UUID
	UserUID          uuid.UUID
	RequesterUID     uuid.UUID
}

// ProcessWaitingFileActivityParam defines the parameters for the ProcessWaitingFileActivity
type ProcessWaitingFileActivityParam struct {
	FileUID          uuid.UUID
	KnowledgeBaseUID uuid.UUID
	UserUID          uuid.UUID
	RequesterUID     uuid.UUID
}

// UpdateFileStatusActivityParam defines the parameters for the UpdateFileStatusActivity
type UpdateFileStatusActivityParam struct {
	FileUID uuid.UUID
	Status  artifactpb.FileProcessStatus
	Message string
}

// NotifyFileProcessedActivityParam defines the parameters for the NotifyFileProcessedActivity
type NotifyFileProcessedActivityParam struct {
	FileUID          uuid.UUID
	KnowledgeBaseUID uuid.UUID
	UserUID          uuid.UUID
	Status           artifactpb.FileProcessStatus
}

// CleanupFilesActivityParam defines the parameters for the CleanupFilesActivity
type CleanupFilesActivityParam struct {
	FileUID             uuid.UUID
	FileIDs             []string
	IncludeOriginalFile bool // If true, also delete the original uploaded file (for explicit file deletion)
}

// SaveChunkActivityParam defines parameters for saving a single chunk
type SaveChunkActivityParam struct {
	KnowledgeBaseUID uuid.UUID
	FileUID          uuid.UUID
	ChunkUID         string
	ChunkContent     []byte
}

// SaveChunkActivityResult contains the result of saving a chunk
type SaveChunkActivityResult struct {
	ChunkUID    string
	Destination string
}

// DeleteFileActivityParam defines parameters for deleting a single file
type DeleteFileActivityParam struct {
	Bucket string
	Path   string
}

// GetFileActivityParam defines parameters for getting a single file
type GetFileActivityParam struct {
	Bucket string
	Path   string
	Index  int // For maintaining order
}

// GetFileActivityResult contains the file content
type GetFileActivityResult struct {
	Index   int
	Name    string
	Content []byte
}
