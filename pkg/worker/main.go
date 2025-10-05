package worker

import (
	"time"

	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/service"
)

// TaskQueue is the Temporal task queue name for all workflows and activities.
const TaskQueue = "artifact-backend"

// TextChunkSize controls tokens per chunk. Smaller = more embeddings = slower. Larger = faster but less precise.
// TextChunkOverlap controls overlap between chunks (20% = good context continuity).
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
	ActivityTimeoutStandard = 5 * time.Minute  // File I/O, DB, MinIO
	ActivityTimeoutLong     = 10 * time.Minute // File conversion, embeddings
)

// RetryInitialInterval, RetryBackoffCoefficient, RetryMaximumInterval*, and RetryMaximumAttempts control retry behavior.
// Prevents retry storms under high concurrency.
const (
	RetryInitialInterval         = 1 * time.Second   // Prevents retry storms
	RetryBackoffCoefficient      = 2.0               // Exponential: 1s→2s→4s
	RetryMaximumIntervalStandard = 30 * time.Second  // Transient failures
	RetryMaximumIntervalLong     = 100 * time.Second // Service recovery
	RetryMaximumAttempts         = 3                 // 3 attempts = ~7s max
)

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
