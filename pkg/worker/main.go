package worker

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/internal/ai"
	"github.com/instill-ai/artifact-backend/internal/ai/gemini"
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

// Worker implements the Temporal worker with all workflows and activities
type Worker struct {
	service    service.Service
	aiProvider ai.Provider
	log        *zap.Logger
}

// New creates a new worker instance
func New(svc service.Service, log *zap.Logger) (*Worker, error) {
	w := &Worker{
		service: svc,
		log:     log,
	}

	// Initialize AI provider for unstructured data content understanding (Gemini, OpenAI, etc.)
	// If not configured or initialization fails, worker will fall back to pipeline conversion
	var provider ai.Provider
	cfg := config.Config // Access global config
	if cfg.AI.Gemini.APIKey != "" {
		geminiProvider, err := gemini.NewProvider(context.Background(), cfg.AI.Gemini.APIKey)
		if err != nil {
			log.Warn("Failed to initialize AI provider for content understanding, will use pipeline fallback",
				zap.Error(err))
		} else {
			provider = geminiProvider
			log.Info("AI provider for content understanding initialized successfully",
				zap.String("provider", provider.Name()))
		}
	}

	// Future: Try OpenAI if Gemini not configured
	// if provider == nil && cfg.AI.OpenAI.APIKey != "" {
	//     openaiProvider, err := openai.NewProvider(context.Background(), cfg.AI.OpenAI.APIKey)
	//     if err == nil {
	//         provider = openaiProvider
	//     }
	// }

	w.aiProvider = provider

	return w, nil
}

// SetService updates the worker's service instance.
// This is used during initialization to resolve the circular dependency
// between Worker, workflow wrappers, and Service.
func (w *Worker) SetService(svc service.Service) {
	w.service = svc
}
