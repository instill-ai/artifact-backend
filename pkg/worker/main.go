package worker

import (
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/service"
)

// TaskQueue is the name of the Temporal task queue to use for all workflows and activities
const TaskQueue = "artifact-backend"

// Config defines the configuration for the worker
type Config struct {
	Service service.Service
}

// Worker implements the Temporal worker with all workflows and activities
type Worker struct {
	service service.Service
	log     *zap.Logger
}

// New creates a new worker instance
func New(config Config, log *zap.Logger) (*Worker, error) {
	w := &Worker{
		service: config.Service,
		log:     log,
	}
	return w, nil
}

// SetService updates the worker's service instance.
// This is used during initialization to resolve the circular dependency
// between Worker, workflow wrappers, and Service.
func (w *Worker) SetService(svc service.Service) {
	w.service = svc
}
