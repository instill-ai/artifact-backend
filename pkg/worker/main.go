package worker

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	"github.com/redis/go-redis/v9"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/acl"
	"github.com/instill-ai/artifact-backend/pkg/ai"
	"github.com/instill-ai/artifact-backend/pkg/ai/gemini"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
	pipelinepb "github.com/instill-ai/protogen-go/pipeline/v1beta"
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
// ActivityTimeoutEmbedding is extra long for embedding large files (100+ chunks × 10 concurrent API calls).
// Too short = premature failures. Too long = blocked worker slots.
const (
	ActivityTimeoutStandard  = 1 * time.Minute  // File I/O, DB, MinIO
	ActivityTimeoutLong      = 15 * time.Minute // File conversion, caching (incl. GCS upload of large chunks), reconciliation
	ActivityTimeoutEmbedding = 10 * time.Minute // Embeddings: 100+ chunks @ ~3s/chunk = 5-10 min for large files
)

// Standardization timeout constants for dynamic timeout calculation.
// Used by StandardizeFileTypeActivity which calls the file-type-conversion
// pipeline via gRPC. Large video/audio files may trigger a two-pass ffmpeg
// conversion (fast stream-copy + slow audio re-encode fallback).
const (
	StdMinTimeout     = 5 * time.Minute  // Enough for small files and fast stream-copy
	StdMaxTimeout     = 30 * time.Minute // Cap for very large files (2 GB)
	StdBaseTimeout    = 2 * time.Minute  // Fixed overhead: gRPC round-trip, MinIO I/O, DB writes
	StdBytesPerSecond = 512 * 1024       // ~512 KB/s: worst-case two-pass ffmpeg (audio re-encode) + gRPC round-trip
)

// AI Processing timeout constants for dynamic timeout calculation.
// These are used by ProcessContentActivity (single-shot only) and ProcessSummaryActivity.
// The per-batch chunked conversion is handled by separate Temporal activities with their own timeouts.
const (
	AIProcessingMinTimeout     = 15 * time.Minute  // Enough for single-shot + without-cache fallback
	AIProcessingMaxTimeout     = 2 * time.Hour     // For very large single-shot attempts; chunked path uses profile.ActivityTimeout
	AIProcessingBaseTimeout    = 5 * time.Minute   // Fixed overhead: cache creation, GCS upload, retries
	AIProcessingBytesPerSecond = 20 * 1024         // ~20KB/sec: dense PDFs (financial, legal) process much slower than simple text
)

// Step-level timeout constants for the fallback chain inside ProcessContentActivity.
// Each step gets a bounded time budget to prevent any single step from starving
// subsequent fallbacks. Without these, a Gemini 504 on single-shot conversion
// consumes the entire activity timeout, leaving nothing for chunked conversion.
const (
	SingleShotConversionTimeout = 4 * time.Minute // Max wait for a single-shot Gemini call (Gemini's own server-side limit is ~5 min)
	PageCountQueryTimeout       = 3 * time.Minute // Max wait for page-count query (large video caches need more warm-up time)
	ChunkConversionTimeout      = 3 * time.Minute // Max wait per chunk in chunked conversion
)

// Chunked conversion constants for large document fallback.
// When single-shot conversion hits DEADLINE_EXCEEDED, the document is split into
// page-range chunks and each chunk is converted independently using the existing cache.
const (
	ChunkedConversionMinChunkPages   = 5  // Minimum chunk size for adaptive retry
	ChunkedConversionPageCountPrompt = "How many pages does this document have? Respond with ONLY the number, nothing else."
	RateLimitCooldown                = 60 * time.Second // Sleep before batch start and between retry rounds to let API quota recover
)

// Long media chunk processing constants.
// Gemini has hard limits on how much media it can accept in a single cache/request.
// Videos exceeding MaxVideoChunkDuration are physically split into chunks using
// ffmpeg, each processed independently through the existing cache+batch pipeline.
const (
	MaxVideoChunkDuration = 30 * time.Minute // Gemini limit ~45 min for video with audio; 30 min gives safety margin
	MaxAudioChunkDuration = 8 * time.Hour    // Gemini limit ~9.5 hours for audio-only
	ChunkOverlap          = 60 * time.Second // Overlap between adjacent chunks to avoid boundary content loss
)

// BatchProfile holds per-file-type tuning parameters for the concurrent batch
// conversion pipeline. The struct supports two modes:
//
//   - Page-based (documents): batches are page ranges (e.g., pages 1-10).
//     Requires a GetPageCountActivity call. Used when SegmentDuration == 0.
//   - Time-range (video/audio): batches are fixed-duration time segments
//     (e.g., 00:05:00-00:10:00). Deterministic from known media duration,
//     no page count query needed. Used when SegmentDuration > 0.
//
// Callers invoke batchProfile(fileType) once to get the right profile.
type BatchProfile struct {
	// Document-mode parameters (used when SegmentDuration == 0)
	PagesPerBatch  int // Pages per batch activity in page-based mode
	PagesPerChunk  int // Pages per sub-chunk within a batch activity
	DirectMaxPages int // Page count threshold: below this, use single-shot conversion

	// Media-mode parameters (used when SegmentDuration > 0)
	SegmentDuration   time.Duration // Fixed duration per segment (e.g., 5 min). 0 = page-based.
	DirectMaxDuration time.Duration // Duration threshold: below this, use single-shot conversion.

	// Shared parameters
	ChunkTimeout         time.Duration // Per-chunk/segment AI call timeout
	ActivityTimeout      time.Duration // Temporal activity-level timeout
	MaxConcurrentBatches int           // Max parallel batch activities dispatched via Selector
}

// isMediaFileType returns true for video and audio file types that benefit
// from time-range based segmentation instead of page-based batching.
func isMediaFileType(ft artifactpb.File_Type) bool {
	switch ft {
	case artifactpb.File_TYPE_MP4,
		artifactpb.File_TYPE_AVI,
		artifactpb.File_TYPE_MOV,
		artifactpb.File_TYPE_MKV,
		artifactpb.File_TYPE_FLV,
		artifactpb.File_TYPE_WMV,
		artifactpb.File_TYPE_MPEG,
		artifactpb.File_TYPE_WEBM_VIDEO,
		artifactpb.File_TYPE_MP3,
		artifactpb.File_TYPE_WAV,
		artifactpb.File_TYPE_AAC,
		artifactpb.File_TYPE_OGG,
		artifactpb.File_TYPE_FLAC,
		artifactpb.File_TYPE_M4A,
		artifactpb.File_TYPE_WMA,
		artifactpb.File_TYPE_AIFF,
		artifactpb.File_TYPE_WEBM_AUDIO:
		return true
	default:
		return false
	}
}

// isAudioFileType returns true for audio-only file types (no video track).
func isAudioFileType(ft artifactpb.File_Type) bool {
	switch ft {
	case artifactpb.File_TYPE_MP3,
		artifactpb.File_TYPE_WAV,
		artifactpb.File_TYPE_AAC,
		artifactpb.File_TYPE_OGG,
		artifactpb.File_TYPE_FLAC,
		artifactpb.File_TYPE_M4A,
		artifactpb.File_TYPE_WMA,
		artifactpb.File_TYPE_AIFF,
		artifactpb.File_TYPE_WEBM_AUDIO:
		return true
	default:
		return false
	}
}

// audioMIMETypeForFileType returns the MIME type for an audio file type.
func audioMIMETypeForFileType(ft artifactpb.File_Type) string {
	switch ft {
	case artifactpb.File_TYPE_MP3:
		return "audio/mpeg"
	case artifactpb.File_TYPE_WAV:
		return "audio/wav"
	case artifactpb.File_TYPE_AAC:
		return "audio/aac"
	case artifactpb.File_TYPE_OGG:
		return "audio/ogg"
	case artifactpb.File_TYPE_FLAC:
		return "audio/flac"
	case artifactpb.File_TYPE_M4A:
		return "audio/mp4"
	case artifactpb.File_TYPE_WMA:
		return "audio/x-ms-wma"
	case artifactpb.File_TYPE_AIFF:
		return "audio/aiff"
	case artifactpb.File_TYPE_WEBM_AUDIO:
		return "audio/webm"
	default:
		return "audio/mpeg"
	}
}

// fileTypeToModality maps a file type to a modality key for prompt lookup.
func fileTypeToModality(ft artifactpb.File_Type) string {
	if isAudioFileType(ft) {
		return ModalityAudio
	}
	if isVideoFileType(ft) {
		return ModalityVideo
	}
	if isImageFileType(ft) {
		return ModalityImage
	}
	return ModalityDocument
}

// isVideoFileType returns true for video file types.
func isVideoFileType(ft artifactpb.File_Type) bool {
	switch ft {
	case artifactpb.File_TYPE_MP4,
		artifactpb.File_TYPE_AVI,
		artifactpb.File_TYPE_MOV,
		artifactpb.File_TYPE_MKV,
		artifactpb.File_TYPE_FLV,
		artifactpb.File_TYPE_WMV,
		artifactpb.File_TYPE_MPEG,
		artifactpb.File_TYPE_WEBM_VIDEO:
		return true
	default:
		return false
	}
}

// isImageFileType returns true for standalone image file types.
func isImageFileType(ft artifactpb.File_Type) bool {
	switch ft {
	case artifactpb.File_TYPE_PNG,
		artifactpb.File_TYPE_JPEG,
		artifactpb.File_TYPE_GIF,
		artifactpb.File_TYPE_WEBP,
		artifactpb.File_TYPE_TIFF,
		artifactpb.File_TYPE_BMP,
		artifactpb.File_TYPE_HEIC,
		artifactpb.File_TYPE_HEIF,
		artifactpb.File_TYPE_AVIF,
		artifactpb.File_TYPE_SVG:
		return true
	default:
		return false
	}
}

// getContentPromptForFileType returns the modality-appropriate content prompt.
// Resolution order: per-modality override → default override → CE fallback.
func (w *Worker) getContentPromptForFileType(ft artifactpb.File_Type) string {
	modality := fileTypeToModality(ft)

	if w.generateContentPrompts != nil {
		if p, ok := w.generateContentPrompts[modality]; ok {
			return p
		}
	}

	if w.generateContentPrompt != "" {
		return w.generateContentPrompt
	}

	return gemini.GetGenerateContentPrompt()
}

// getVisualOnlyPromptForFileType returns the visual-only prompt variant.
// Resolution: per-modality override for "video_visual" → CE fallback.
func (w *Worker) getVisualOnlyPromptForFileType(ft artifactpb.File_Type) string {
	if w.generateContentPrompts != nil {
		if p, ok := w.generateContentPrompts["video_visual"]; ok {
			return p
		}
	}
	return gemini.GetGenerateContentVideoVisualPrompt()
}

// formatTimestamp formats a time.Duration as "HH:MM:SS" for use in
// time-range prompts sent to the AI model.
func formatTimestamp(d time.Duration) string {
	total := int(d.Seconds())
	h := total / 3600
	m := (total % 3600) / 60
	s := total % 60
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

// batchProfile returns tuning parameters appropriate for the given file type.
//
// Media files (video/audio) use time-range segmentation: fixed 5-minute
// segments with 32 concurrent batches, eliminating the need for a page count
// query. Page-based fields are retained as fallback if duration is unavailable.
//
// Document files use page-based batching with 16 concurrent batches.
func batchProfile(ft artifactpb.File_Type) BatchProfile {
	if isMediaFileType(ft) {
		return BatchProfile{
			// Time-range mode (primary for media)
			SegmentDuration:   5 * time.Minute,
			DirectMaxDuration: 10 * time.Minute,

			// Page-based fallback (used when duration is unavailable)
			PagesPerBatch:  3,
			PagesPerChunk:  3,
			DirectMaxPages: 5,

			// Shared
			ChunkTimeout:         5 * time.Minute,
			ActivityTimeout:      20 * time.Minute,
			MaxConcurrentBatches: 32,
		}
	}
	return BatchProfile{
		PagesPerBatch:        10,
		PagesPerChunk:        10,
		DirectMaxPages:       10,
		ChunkTimeout:         ChunkConversionTimeout,
		ActivityTimeout:      15 * time.Minute,
		MaxConcurrentBatches: 16,
	}
}

// CalculateAIProcessingTimeout returns a dynamic timeout based on file size.
// The timeout must be large enough for the full fallback chain (single-shot →
// chunked → without-cache → fresh-cache+chunked) since each step may consume
// significant time before failing.
//
// Formula: base_timeout + (file_size_bytes / bytes_per_second) * safety_factor
// Safety factor of 3x accounts for API latency, rate limiting, processing variance,
// and the overhead of multiple API calls in chunked conversion mode.
func CalculateAIProcessingTimeout(fileSizeBytes int) time.Duration {
	if fileSizeBytes <= 0 {
		return AIProcessingMinTimeout
	}

	// Calculate estimated processing time with 3x safety factor
	estimatedSeconds := float64(fileSizeBytes) / float64(AIProcessingBytesPerSecond) * 3.0
	calculatedTimeout := AIProcessingBaseTimeout + time.Duration(estimatedSeconds)*time.Second

	// Apply bounds
	if calculatedTimeout < AIProcessingMinTimeout {
		return AIProcessingMinTimeout
	}
	if calculatedTimeout > AIProcessingMaxTimeout {
		return AIProcessingMaxTimeout
	}

	return calculatedTimeout
}

// CalculateStandardizationTimeout returns a file-size-aware timeout for
// StandardizeFileTypeActivity. Large media files need extra time for the
// two-pass ffmpeg conversion plus gRPC transfer of the raw bytes.
func CalculateStandardizationTimeout(fileSizeBytes int) time.Duration {
	if fileSizeBytes <= 0 {
		return StdMinTimeout
	}

	estimatedSeconds := float64(fileSizeBytes) / float64(StdBytesPerSecond)
	calculatedTimeout := StdBaseTimeout + time.Duration(estimatedSeconds)*time.Second

	if calculatedTimeout < StdMinTimeout {
		return StdMinTimeout
	}
	if calculatedTimeout > StdMaxTimeout {
		return StdMaxTimeout
	}

	return calculatedTimeout
}

// RetryInitialInterval, RetryBackoffCoefficient, RetryMaximumInterval*, and RetryMaximumAttempts control retry behavior.
// Prevents retry storms under high concurrency.
const (
	RetryInitialInterval         = 1 * time.Second   // Prevents retry storms
	RetryBackoffCoefficient      = 2.0               // Exponential: 1s→2s→4s→8s→16s→32s→64s→100s (capped)
	RetryMaximumIntervalStandard = 30 * time.Second  // Caps exponential backoff for transient failures
	RetryMaximumIntervalLong     = 100 * time.Second // Service recovery (AI client rate limits)
	RetryMaximumAttempts         = 8                 // 8 attempts = up to ~3 minutes with backoff (handles AI rate limiting)
)

// PostStandardizationFn allows extensions of the worker to provide logic
// to be executed after a file is standardized (e.g., DOCX→PDF, GIF→PNG).
// This is called early in the processing pipeline, before content conversion.
//
// Parameters:
//   - ctx: Temporal workflow context
//   - fileUID: Unique identifier for the file
//   - namespaceUID: Namespace that owns the file
//   - kbUID: Knowledge base that contains the file
//   - standardizedBucket: MinIO bucket containing the standardized file
//   - standardizedPath: Path to the standardized file in MinIO
//   - effectiveFileType: The file type after standardization (e.g., PDF, PNG)
//   - filename: Display name for the file
type PostStandardizationFn func(
	ctx workflow.Context,
	fileUID, namespaceUID, kbUID uuid.UUID,
	standardizedBucket, standardizedPath string,
	effectiveFileType artifactpb.File_Type,
	filename string,
) error

// PostContentConversionFn allows extensions of the worker to provide logic
// to be executed after a file's content is converted to markdown.
// This is called earlier than PostFileCompletionFn - as soon as the markdown
// content is ready, before chunking and embedding are complete.
//
// Parameters:
//   - ctx: Temporal workflow context
//   - fileUID: Unique identifier for the file
//   - namespaceUID: Namespace that owns the file
//   - kbUID: Knowledge base that contains the file
//   - contentBucket: MinIO bucket containing the markdown content
//   - contentPath: Path to the markdown content file in MinIO
//   - filename: Display name for the file
type PostContentConversionFn func(
	ctx workflow.Context,
	fileUID, namespaceUID, kbUID uuid.UUID,
	contentBucket, contentPath string,
	filename string,
) error

// PostSummaryConversionFn allows extensions of the worker to provide logic
// to be executed after a file's summary is generated.
// This is called after summary generation, in parallel with content conversion.
//
// Parameters:
//   - ctx: Temporal workflow context
//   - fileUID: Unique identifier for the file
//   - namespaceUID: Namespace that owns the file
//   - kbUID: Knowledge base that contains the file
//   - summaryBucket: MinIO bucket containing the summary
//   - summaryPath: Path to the summary file in MinIO
//   - filename: Display name for the file
type PostSummaryConversionFn func(
	ctx workflow.Context,
	fileUID, namespaceUID, kbUID uuid.UUID,
	summaryBucket, summaryPath string,
	filename string,
) error

// PostFileCompletionFn allows extensions of the worker to provide some logic
// to be executed after a file is successfully processed.
// The usageData parameter contains token/character usage from all AI activities.
type PostFileCompletionFn func(_ workflow.Context, file *repository.FileModel, effectiveFileType artifactpb.File_Type, usageData *FileProcessingUsageData) error

// FileProcessingUsageData contains token usage metadata collected from file
// processing activities (content, summary, embedding). Each field may be nil
// if the corresponding activity didn't complete or didn't produce usage data
// (e.g., OpenAI pipeline route, or the activity failed before LLM call).
type FileProcessingUsageData struct {
	ContentUsageMetadata   any    // From ProcessContentActivity (nil if content activity didn't complete)
	ContentModel           string // Model used for content generation (e.g., "gemini-2.0-flash-001")
	SummaryUsageMetadata   any    // From ProcessSummaryActivity (nil if summary activity didn't complete)
	SummaryModel           string // Model used for summary generation
	EmbeddingUsageMetadata any    // From EmbedAndSaveChunksActivity (nil if embed activity didn't complete)
	EmbeddingModel         string // Model used for embedding generation (e.g., "text-embedding-004")
}

// PostFileFailureFn allows extensions of the worker to provide some logic
// to be executed when a file processing operation fails.
// This is called in the workflow ONLY when file metadata is available
// (i.e., after the metadata fetch phase). Early failures (e.g., file not found)
// do not trigger this callback since no LLM processing occurred.
//
// Parameters:
//   - ctx: Temporal workflow context
//   - file: The file model
//   - effectiveFileType: The resolved file type after standardization
//   - stage: The processing stage where the failure occurred
//   - err: The error that caused the failure
//   - usageData: Token usage metadata from completed/partially-completed activities.
//     May contain data from activities that called LLM before failing in post-processing.
type PostFileFailureFn func(_ workflow.Context, file *repository.FileModel, effectiveFileType artifactpb.File_Type, stage string, err error, usageData *FileProcessingUsageData) error

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

	postFileCompletion    PostFileCompletionFn
	postFileFailure       PostFileFailureFn
	postContentConversion PostContentConversionFn
	postStandardization   PostStandardizationFn
	postSummaryConversion PostSummaryConversionFn
	generateContentPrompt  string            // Default prompt for AI content generation (backward compat)
	generateContentPrompts map[string]string // Per-modality prompts: "document", "image", "video", "audio"
	generateSummaryPrompt  string            // Prompt for AI summary generation
	aiClientOverrides      map[string]ai.Client // Per-modality AI client overrides
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

// GetRedisClient returns the Redis client for external use (e.g., EE worker event publishing)
func (w *Worker) GetRedisClient() *redis.Client {
	return w.redisClient
}

// SetPostFileCompletionFn allows clients to add logic for files that have been
// successfully processed.
func (w *Worker) SetPostFileCompletionFn(fn PostFileCompletionFn) {
	w.postFileCompletion = fn
}

// SetPostFileFailureFn allows clients to add logic for files that failed
// during processing. Called with available file context when a processing
// stage fails after the metadata fetch phase.
func (w *Worker) SetPostFileFailureFn(fn PostFileFailureFn) {
	w.postFileFailure = fn
}

// SetPostContentConversionFn allows clients to add logic to be executed
// when a file's content is converted to markdown (before chunking/embedding).
func (w *Worker) SetPostContentConversionFn(fn PostContentConversionFn) {
	w.postContentConversion = fn
}

// SetPostStandardizationFn allows clients to add logic to be executed
// when a file is standardized (e.g., DOCX→PDF, GIF→PNG).
// This is called early in the pipeline, before content conversion.
func (w *Worker) SetPostStandardizationFn(fn PostStandardizationFn) {
	w.postStandardization = fn
}

// SetPostSummaryConversionFn allows clients to add logic to be executed
// when a file's summary is generated.
func (w *Worker) SetPostSummaryConversionFn(fn PostSummaryConversionFn) {
	w.postSummaryConversion = fn
}

// SetGenerateContentPrompt allows clients to override the default content generation prompt.
// If not set, defaults to CE's embedded prompt.
func (w *Worker) SetGenerateContentPrompt(prompt string) {
	w.generateContentPrompt = prompt
}

// Modality keys for per-modality prompt overrides.
const (
	ModalityDocument = "document"
	ModalityImage    = "image"
	ModalityVideo    = "video"
	ModalityAudio    = "audio"
)

// SetGenerateContentPromptForModality sets a content generation prompt for a
// specific modality (document, image, video, audio). When set, this prompt is
// used instead of the default for files of the corresponding type.
func (w *Worker) SetGenerateContentPromptForModality(modality, prompt string) {
	if w.generateContentPrompts == nil {
		w.generateContentPrompts = make(map[string]string, 4)
	}
	w.generateContentPrompts[modality] = prompt
}

// SetGenerateSummaryPrompt allows clients to override the summary generation prompt.
// If not set, defaults to CE's embedded prompt.
func (w *Worker) SetGenerateSummaryPrompt(prompt string) {
	w.generateSummaryPrompt = prompt
}

// getGenerateContentPrompt returns the prompt to use, falling back to CE default if not set.
func (w *Worker) getGenerateContentPrompt() string { //nolint:unused
	if w.generateContentPrompt != "" {
		return w.generateContentPrompt
	}
	return gemini.GetGenerateContentPrompt()
}

// getGenerateSummaryPrompt returns the prompt to use, falling back to CE default if not set.
func (w *Worker) getGenerateSummaryPrompt() string {
	if w.generateSummaryPrompt != "" {
		return w.generateSummaryPrompt
	}
	return gemini.GetGenerateSummaryPrompt()
}

// SetAIClientForModality registers an AI client override for a specific
// modality. When set, this client is used instead of the default for files
// of the corresponding type (e.g., a different model for audio processing).
func (w *Worker) SetAIClientForModality(modality string, client ai.Client) {
	if w.aiClientOverrides == nil {
		w.aiClientOverrides = make(map[string]ai.Client, 4)
	}
	w.aiClientOverrides[modality] = client
}

// getAIClientForFileType returns the modality-appropriate AI client.
// Resolution: per-modality override → default aiClient.
func (w *Worker) getAIClientForFileType(ft artifactpb.File_Type) ai.Client {
	if w.aiClientOverrides != nil {
		modality := fileTypeToModality(ft)
		if c, ok := w.aiClientOverrides[modality]; ok {
			return c
		}
	}
	return w.aiClient
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
		return activityError(err, "DeleteFilesBatchActivity")
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
			return nil, activityError(err, "GetFilesBatchActivity")
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
			return nil, activityError(err, "GetFilesBatchActivity")
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
			ownerUID, err := uuid.FromString(kb.NamespaceUID)
			if err != nil {
				w.log.Error("Failed to parse owner UID",
					zap.String("knowledgeBaseID", kb.ID),
					zap.String("kbUID", kb.UID.String()),
					zap.Error(err))
				return
			}

			// Start child workflow for this KB
			// CRITICAL: Allow duplicate workflow IDs to support retrying FAILED updates
			// Without this policy, Temporal rejects re-running workflows with the same ID
			workflowOptions := client.StartWorkflowOptions{
				ID:                       fmt.Sprintf("update-kb-%s", kb.UID.String()),
				TaskQueue:                "artifact-backend",
				WorkflowExecutionTimeout: 24 * time.Hour,
				WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			}

			// Use creator UID as requester, or owner UID if no creator (system KB)
			requesterUID := types.RequesterUIDType(ownerUID)
			if kb.CreatorUID != nil {
				requesterUID = types.RequesterUIDType(*kb.CreatorUID)
			}

			_, err = w.temporalClient.ExecuteWorkflow(ctx, workflowOptions, w.UpdateKnowledgeBaseWorkflow, UpdateKnowledgeBaseWorkflowParam{
				OriginalKBUID:         kb.UID,
				UserUID:               types.UserUIDType(ownerUID),
				RequesterUID:          requesterUID,
				SystemID:              systemID,
				RollbackRetentionDays: config.Config.RAG.Update.RollbackRetentionDays,
				FileBatchSize:         config.Config.RAG.Update.FileBatchSize,
				MinioBucket:           config.Config.Minio.BucketName,
			})

			if err != nil {
				w.log.Error("Failed to start update workflow for KB",
					zap.String("knowledgeBaseID", kb.ID),
					zap.String("kbUID", kb.UID.String()),
					zap.Error(err))
				return
			}

			mu.Lock()
			successCount++
			mu.Unlock()

			w.log.Info("Update workflow started for KB",
				zap.String("knowledgeBaseID", kb.ID),
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
			KnowledgeBaseID:  kb.ID,
			KnowledgeBaseUID: kb.UID.String(),
			WorkflowID:       kb.UpdateWorkflowID,
			Status:           artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_ABORTED.String(),
		}

		// Terminate the workflow (TerminateWorkflow immediately stops, unlike CancelWorkflow which sends a signal)
		if kb.UpdateWorkflowID != "" {
			err := w.temporalClient.TerminateWorkflow(ctx, kb.UpdateWorkflowID, "", "Aborted via AbortKnowledgeBaseUpdate API")
			if err != nil {
				w.log.Warn("Failed to terminate workflow (may have already completed)",
					zap.String("knowledgeBaseID", kb.ID),
					zap.String("workflowID", kb.UpdateWorkflowID),
					zap.Error(err))
				// Continue with cleanup even if terminate fails
			}
		}

		// Find and cleanup staging KB
		stagingID := fmt.Sprintf("%s-staging", kb.ID)
		ownerUID, err := uuid.FromString(kb.NamespaceUID)
		if err != nil {
			w.log.Warn("Failed to parse owner UID",
				zap.String("knowledgeBaseID", kb.ID),
				zap.String("owner", kb.NamespaceUID),
				zap.Error(err))
		} else {
			stagingKB, err := w.repository.GetKnowledgeBaseByOwnerAndKbID(ctx, types.OwnerUIDType(ownerUID), stagingID)
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
				zap.String("knowledgeBaseID", kb.ID),
				zap.String("kbUID", kb.UID.String()),
				zap.Error(err))
			status.ErrorMessage = fmt.Sprintf("failed to update status: %v", err)
			status.Status = artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_FAILED.String()
		} else {
			abortedCount++
			w.log.Info("Successfully aborted KB update",
				zap.String("knowledgeBaseID", kb.ID),
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
