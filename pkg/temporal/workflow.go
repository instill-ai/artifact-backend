package temporal

import (
	"github.com/gofrs/uuid"
)

const (
	// TaskQueue is the Temporal task queue name for artifact processing
	TaskQueue = "artifact-backend"
)

// ProcessFileWorkflowParam contains parameters for the file processing workflow
type ProcessFileWorkflowParam struct {
	FileUID          string
	KnowledgeBaseUID string
	UserUID          string
	RequesterUID     string
}

// CleanupFileWorkflowParam contains parameters for the file cleanup workflow
type CleanupFileWorkflowParam struct {
	FileUID             string
	IncludeOriginalFile bool
	UserUID             string
	WorkflowID          string
}

// EmbedTextsWorkflowParam contains parameters for the text embedding workflow
type EmbedTextsWorkflowParam struct {
	Texts           []string
	BatchSize       int
	RequestMetadata map[string][]string // gRPC metadata for authentication
}

// SaveChunksWorkflowParam contains parameters for the save chunks workflow
type SaveChunksWorkflowParam struct {
	KnowledgeBaseUID uuid.UUID
	FileUID          uuid.UUID
	Chunks           map[string][]byte // chunkUID -> content
}

// DeleteFilesWorkflowParam contains parameters for the delete files workflow
type DeleteFilesWorkflowParam struct {
	Bucket    string
	FilePaths []string
}

// GetFilesWorkflowParam contains parameters for the get files workflow
type GetFilesWorkflowParam struct {
	Bucket    string
	FilePaths []string
}

// FileContent represents the content of a retrieved file
type FileContent struct {
	Index   int
	Name    string
	Content []byte
}

// CleanupKnowledgeBaseWorkflowParam contains parameters for the knowledge base cleanup workflow
type CleanupKnowledgeBaseWorkflowParam struct {
	KnowledgeBaseUID string
}
