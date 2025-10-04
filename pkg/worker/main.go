package worker

import (
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"
)

// TaskQueue is the name of the Temporal task queue to use for all workflows and activities
const TaskQueue = "artifact-backend"

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
