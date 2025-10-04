package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// TaskQueue is the name of the Temporal task queue to use for all workflows and activities
const TaskQueue = "artifact-backend"

// Workflow implementations that wrap Temporal ExecuteWorkflow calls
// Interfaces and parameter types are defined in pkg/service/service.go

type processFileWorkflow struct {
	temporalClient client.Client
}

// NewProcessFileWorkflow creates a new ProcessFileWorkflow instance
func NewProcessFileWorkflow(temporalClient client.Client) service.ProcessFileWorkflow {
	return &processFileWorkflow{temporalClient: temporalClient}
}

func (w *processFileWorkflow) Execute(ctx context.Context, param service.ProcessFileWorkflowParam) error {
	workflowID := fmt.Sprintf("process-file-%s", param.FileUID.String())
	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	_, err := w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, new(Worker).ProcessFileWorkflow, param)
	return err
}

type cleanupFileWorkflow struct {
	temporalClient client.Client
}

// NewCleanupFileWorkflow creates a new CleanupFileWorkflow instance
func NewCleanupFileWorkflow(temporalClient client.Client) service.CleanupFileWorkflow {
	return &cleanupFileWorkflow{temporalClient: temporalClient}
}

func (w *cleanupFileWorkflow) Execute(ctx context.Context, param service.CleanupFileWorkflowParam) error {
	workflowID := fmt.Sprintf("cleanup-file-%s", param.FileUID.String())
	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	_, err := w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, new(Worker).CleanupFileWorkflow, param)
	return err
}

type cleanupKnowledgeBaseWorkflow struct {
	temporalClient client.Client
}

// NewCleanupKnowledgeBaseWorkflow creates a new CleanupKnowledgeBaseWorkflow instance
func NewCleanupKnowledgeBaseWorkflow(temporalClient client.Client) service.CleanupKnowledgeBaseWorkflow {
	return &cleanupKnowledgeBaseWorkflow{temporalClient: temporalClient}
}

func (w *cleanupKnowledgeBaseWorkflow) Execute(ctx context.Context, param service.CleanupKnowledgeBaseWorkflowParam) error {
	workflowID := fmt.Sprintf("cleanup-kb-%s", param.KnowledgeBaseUID.String())
	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	_, err := w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, new(Worker).CleanupKnowledgeBaseWorkflow, param)
	return err
}

type embedTextsWorkflow struct {
	temporalClient client.Client
}

// NewEmbedTextsWorkflow creates a new EmbedTextsWorkflow instance
func NewEmbedTextsWorkflow(temporalClient client.Client) service.EmbedTextsWorkflow {
	return &embedTextsWorkflow{temporalClient: temporalClient}
}

func (w *embedTextsWorkflow) Execute(ctx context.Context, param service.EmbedTextsWorkflowParam) ([][]float32, error) {
	workflowID := fmt.Sprintf("embed-texts-%d-%d", time.Now().UnixNano(), len(param.Texts))
	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	workflowRun, err := w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, new(Worker).EmbedTextsWorkflow, param)
	if err != nil {
		return nil, err
	}

	var vectors [][]float32
	if err = workflowRun.Get(ctx, &vectors); err != nil {
		return nil, err
	}

	return vectors, nil
}

type deleteFilesWorkflow struct {
	temporalClient client.Client
}

// NewDeleteFilesWorkflow creates a new DeleteFilesWorkflow instance
func NewDeleteFilesWorkflow(temporalClient client.Client) service.DeleteFilesWorkflow {
	return &deleteFilesWorkflow{temporalClient: temporalClient}
}

func (w *deleteFilesWorkflow) Execute(ctx context.Context, param service.DeleteFilesWorkflowParam) error {
	workflowID := fmt.Sprintf("delete-files-%d-%d", time.Now().UnixNano(), len(param.FilePaths))
	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	workflowRun, err := w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, new(Worker).DeleteFilesWorkflow, param)
	if err != nil {
		return fmt.Errorf("failed to start delete files workflow: %w", err)
	}

	if err = workflowRun.Get(ctx, nil); err != nil {
		return fmt.Errorf("delete files workflow failed: %w", err)
	}

	return nil
}

type getFilesWorkflow struct {
	temporalClient client.Client
}

// NewGetFilesWorkflow creates a new GetFilesWorkflow instance
func NewGetFilesWorkflow(temporalClient client.Client) service.GetFilesWorkflow {
	return &getFilesWorkflow{temporalClient: temporalClient}
}

func (w *getFilesWorkflow) Execute(ctx context.Context, param service.GetFilesWorkflowParam) ([]service.FileContent, error) {
	workflowID := fmt.Sprintf("get-files-%d-%d", time.Now().UnixNano(), len(param.FilePaths))
	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	workflowRun, err := w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, new(Worker).GetFilesWorkflow, param)
	if err != nil {
		return nil, fmt.Errorf("failed to start get files workflow: %w", err)
	}

	var results []service.FileContent
	if err = workflowRun.Get(ctx, &results); err != nil {
		return nil, fmt.Errorf("get files workflow failed: %w", err)
	}

	return results, nil
}

// Config defines the configuration for the worker
type Config struct {
	Repository repository.RepositoryI
	Service    service.Service
}

// Worker implements the Temporal worker with all workflows and activities
type Worker struct {
	repository repository.RepositoryI
	service    service.Service
	log        *zap.Logger
}

// New creates a new worker instance
func New(config Config, log *zap.Logger) (*Worker, error) {
	w := &Worker{
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
}

// ChunkFileActivityParam defines the parameters for the ChunkFileActivity
type ChunkFileActivityParam struct {
	FileUID          uuid.UUID
	KnowledgeBaseUID uuid.UUID
	UserUID          uuid.UUID
	ChunkSize        int
	ChunkOverlap     int
}

// ChunkFileActivityResult contains the chunks that need to be saved to MinIO
type ChunkFileActivityResult struct {
	// Map of chunkUID -> chunk text content
	ChunksToSave map[string][]byte
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

// UpdateChunkDestinationsActivityParam defines parameters for updating chunk destinations
type UpdateChunkDestinationsActivityParam struct {
	Destinations map[string]string // chunkUID -> destination
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
