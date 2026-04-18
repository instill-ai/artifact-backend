// Package worker — BackfillThumbnailsWorkflow.
//
// This workflow retrofits thumbnails for files uploaded before the
// `GenerateThumbnailActivity` rollout (Phase 3b of the
// `files-card-preview-optimization` plan). It is a one-shot admin
// operation invoked through a short-lived gRPC handler / Temporal client
// call — there is no schedule attached, because once an installation is
// caught up the worker has nothing more to do.
//
// Contract (mirrors the in-pipeline thumbnail invariant
// `ARTIFACT-INV-THUMBNAIL-NON-BLOCKING`):
//
//   - Never fails the calling surface: any per-file error is logged and
//     skipped. The frontend's fallback chain
//     (`thumbnail_uri` → `derived_resource_uri` → mime icon) keeps the
//     tile usable.
//   - Runs on the original uploaded file (the `file.storage_path` column)
//     rather than the standardized copy: ffmpeg handles every
//     image/video codec natively, and many pre-rollout files never went
//     through `StandardizeFileTypeActivity` at all.
//   - Is resumable. After MaxPollIterationsBeforeContinueAsNew batches we
//     `ContinueAsNew` with the last processed file UID as the new
//     cursor, so workflow history stays bounded even when backfilling
//     millions of rows.
//   - Activity lookups are keyset-paginated on `file.uid`, so repeated
//     restarts cannot revisit files already processed in an earlier
//     iteration.
//
// See `artifact-backend/AGENTS.md` →
// `ARTIFACT-INV-THUMBNAIL-BACKFILL` and the Phase 3e section of the plan.
package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

// BackfillThumbnailsWorkflowParam controls a single execution (or a
// `ContinueAsNew` continuation) of the backfill.
type BackfillThumbnailsWorkflowParam struct {
	// BatchSize is the number of files scanned per activity call. 100 is
	// a good default: big enough to amortize the activity scheduling
	// overhead, small enough that any single batch comfortably fits the
	// `ActivityTimeoutStandard` budget.
	BatchSize int
	// MaxFiles caps the total number of files the workflow will attempt.
	// Zero means "process everything we find". Operators typically leave
	// it zero; the cap exists for rehearsal runs and integration tests.
	MaxFiles int
	// AfterFileUID is the keyset cursor. Callers pass the zero UUID on
	// the first invocation; each `ContinueAsNew` advances it to the
	// highest UID from the previous batch so we never revisit files.
	AfterFileUID types.FileUIDType
	// ProcessedCount is carried across `ContinueAsNew` iterations so the
	// final log line reflects the full run, not just the last slice.
	ProcessedCount int
	// IterationCount tracks how many batches the workflow has executed
	// in the current run (reset on `ContinueAsNew`). See
	// `MaxPollIterationsBeforeContinueAsNew`.
	IterationCount int
}

// BackfillThumbnailsWorkflowName is the registered Temporal workflow
// name. Operators pass this to the `commander backfill-thumbnails`
// helper (see `artifact-backend-ee/docs/runbook-backfill-thumbnails.md`
// — the runbook lives in the EE repo because the backfill is an
// operational task on EE deployments; the workflow code itself is CE).
const BackfillThumbnailsWorkflowName = "BackfillThumbnailsWorkflow"

// backfillThumbnailsWorkflowStarter is the adapter used by callers
// (handlers, tests, commander scripts) to enqueue the workflow through
// the Temporal client without knowing the TaskQueue layout.
type backfillThumbnailsWorkflowStarter struct {
	temporalClient client.Client
	worker         *Worker
}

// NewBackfillThumbnailsWorkflow wires the workflow starter.
func NewBackfillThumbnailsWorkflow(temporalClient client.Client, worker *Worker) *backfillThumbnailsWorkflowStarter {
	return &backfillThumbnailsWorkflowStarter{
		temporalClient: temporalClient,
		worker:         worker,
	}
}

// Execute kicks off a single backfill run. The workflow is idempotent —
// re-running it when nothing is missing completes in a handful of ms.
func (s *backfillThumbnailsWorkflowStarter) Execute(ctx context.Context, param BackfillThumbnailsWorkflowParam) (string, error) {
	workflowID := fmt.Sprintf("backfill-thumbnails-%d", time.Now().UnixNano())
	options := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}
	run, err := s.temporalClient.ExecuteWorkflow(ctx, options, s.worker.BackfillThumbnailsWorkflow, param)
	if err != nil {
		return "", errorsx.AddMessage(err, "Unable to start thumbnail backfill workflow. Please try again.")
	}
	return run.GetID(), nil
}

// BackfillThumbnailsWorkflow walks the file table in file-UID order,
// skipping rows that already have a thumbnail, and invokes
// `GenerateThumbnailActivity` for the rest. Each per-file activity
// failure is logged but does not fail the workflow — Temporal's own
// retry policy (MaximumAttempts = 3, same as the live pipeline) handles
// transient errors inside the activity boundary.
func (w *Worker) BackfillThumbnailsWorkflow(ctx workflow.Context, param BackfillThumbnailsWorkflowParam) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("BackfillThumbnailsWorkflow: starting",
		"batchSize", param.BatchSize,
		"maxFiles", param.MaxFiles,
		"afterFileUID", param.AfterFileUID.String(),
		"processedCount", param.ProcessedCount,
		"iterationCount", param.IterationCount)

	if param.BatchSize <= 0 {
		param.BatchSize = 100
	}

	listCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    3,
		},
	})

	// Thumbnail activity options mirror the forward-path
	// (see process_file_workflow.go). 5-minute wall-clock ceiling is
	// plenty for ffmpeg to render one frame from even a 2 GB video.
	thumbCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    RetryInitialInterval,
			BackoffCoefficient: RetryBackoffCoefficient,
			MaximumInterval:    RetryMaximumIntervalStandard,
			MaximumAttempts:    3,
		},
	})

	cursor := param.AfterFileUID
	processed := param.ProcessedCount
	iteration := param.IterationCount

	for {
		if param.MaxFiles > 0 && processed >= param.MaxFiles {
			logger.Info("BackfillThumbnailsWorkflow: MaxFiles reached, stopping",
				"processedCount", processed)
			return nil
		}

		var listResult ListFilesMissingThumbnailsActivityResult
		listErr := workflow.ExecuteActivity(listCtx, w.ListFilesMissingThumbnailsActivity, &ListFilesMissingThumbnailsActivityParam{
			AfterFileUID: cursor,
			BatchSize:    param.BatchSize,
		}).Get(listCtx, &listResult)
		if listErr != nil {
			return errorsx.AddMessage(listErr, "Failed to list files missing thumbnails during backfill")
		}

		if len(listResult.Files) == 0 {
			logger.Info("BackfillThumbnailsWorkflow: no more files to backfill",
				"processedCount", processed,
				"iterationCount", iteration)
			return nil
		}

		// Dispatch the thumbnail activity for each file in the batch.
		// We fan out (no `.Get()` inside the loop) and then wait on all
		// futures, mirroring the "fire, collect, tolerate" pattern the
		// cleanup workflow uses. Any single failure is logged and
		// skipped — preserves the non-fatal contract.
		type pending struct {
			fileUID types.FileUIDType
			future  workflow.Future
		}
		futures := make([]pending, 0, len(listResult.Files))
		for _, f := range listResult.Files {
			if param.MaxFiles > 0 && processed+len(futures) >= param.MaxFiles {
				break
			}
			futures = append(futures, pending{
				fileUID: f.FileUID,
				future: workflow.ExecuteActivity(thumbCtx, w.GenerateThumbnailActivity, &GenerateThumbnailActivityParam{
					FileUID:         f.FileUID,
					KBUID:           f.KBUID,
					Bucket:          config.Config.Minio.BucketName,
					Destination:     f.StoragePath,
					FileType:        f.FileType,
					FileDisplayName: f.DisplayName,
					Metadata:        f.Metadata,
				}),
			})
		}

		succeeded, skipped, failed := 0, 0, 0
		for _, p := range futures {
			var res GenerateThumbnailActivityResult
			if err := p.future.Get(thumbCtx, &res); err != nil {
				failed++
				logger.Warn("BackfillThumbnailsWorkflow: activity failed for file (skipping)",
					"fileUID", p.fileUID.String(),
					"error", err.Error())
				continue
			}
			if res.Skipped {
				skipped++
				continue
			}
			succeeded++
		}

		processed += len(futures)
		iteration++
		cursor = listResult.Files[len(listResult.Files)-1].FileUID

		logger.Info("BackfillThumbnailsWorkflow: batch complete",
			"batchSize", len(futures),
			"succeeded", succeeded,
			"skipped", skipped,
			"failed", failed,
			"processedCount", processed,
			"nextCursor", cursor.String(),
			"iterationCount", iteration)

		// When the last batch came up short of `BatchSize`, the table
		// has been drained — exit cleanly.
		if len(listResult.Files) < param.BatchSize {
			logger.Info("BackfillThumbnailsWorkflow: drained file table",
				"processedCount", processed)
			return nil
		}

		// Bound workflow history length: same threshold as the cleanup
		// polling loops (see `MaxPollIterationsBeforeContinueAsNew`).
		if iteration >= MaxPollIterationsBeforeContinueAsNew {
			logger.Info("BackfillThumbnailsWorkflow: ContinueAsNew to cap history",
				"processedCount", processed,
				"iterationCount", iteration,
				"nextCursor", cursor.String())
			return workflow.NewContinueAsNewError(ctx, w.BackfillThumbnailsWorkflow, BackfillThumbnailsWorkflowParam{
				BatchSize:      param.BatchSize,
				MaxFiles:       param.MaxFiles,
				AfterFileUID:   cursor,
				ProcessedCount: processed,
				IterationCount: 0,
			})
		}
	}
}

// ListFilesMissingThumbnailsActivityParam is the request side of the
// backfill's scan activity.
type ListFilesMissingThumbnailsActivityParam struct {
	AfterFileUID types.FileUIDType
	BatchSize    int
}

// ListFilesMissingThumbnailsActivityResult carries a single keyset
// page's worth of work items.
type ListFilesMissingThumbnailsActivityResult struct {
	Files []BackfillFileDescriptor
}

// BackfillFileDescriptor is the serialization-friendly mirror of
// `repository.FileMissingThumbnail`. It stays inside the worker package
// so that activities can round-trip through Temporal's JSON dataconverter
// without leaking gorm tags or internal fields. The file type is
// materialised as the proto enum so the downstream activity does not
// have to re-parse it.
type BackfillFileDescriptor struct {
	FileUID     types.FileUIDType
	KBUID       types.KBUIDType
	FileType    artifactpb.File_Type
	StoragePath string
	DisplayName string
	Metadata    *structpb.Struct
}

// ListFilesMissingThumbnailsActivity returns a keyset-paginated batch of
// files that still need thumbnails. It is intentionally thin — all the
// heavy lifting (the NOT EXISTS scan, the file-type gate) lives in the
// repository so unit tests can hit the SQL layer directly and so
// EE can add custom filters by wrapping the method.
func (w *Worker) ListFilesMissingThumbnailsActivity(ctx context.Context, param *ListFilesMissingThumbnailsActivityParam) (*ListFilesMissingThumbnailsActivityResult, error) {
	logger := w.log.With(
		zap.String("afterFileUID", param.AfterFileUID.String()),
		zap.Int("batchSize", param.BatchSize))

	rows, err := w.repository.ListFilesMissingThumbnails(ctx, param.AfterFileUID, param.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("listing files missing thumbnails: %w", err)
	}

	// Deduplicate on FileUID: a file joined to multiple KBs shows up
	// once per `file_knowledge_base` row. We keep the first association
	// — `GenerateThumbnailActivity` only needs *a* KB to derive the
	// converted-file path, and the resulting blob is shared across all
	// associations (same `file_uid` / same `CONVERTED_FILE_TYPE_THUMBNAIL`
	// row).
	seen := make(map[types.FileUIDType]struct{}, len(rows))
	descs := make([]BackfillFileDescriptor, 0, len(rows))
	for _, r := range rows {
		if _, ok := seen[r.FileUID]; ok {
			continue
		}
		seen[r.FileUID] = struct{}{}

		ft, ok := artifactpb.File_Type_value[r.FileType]
		if !ok {
			logger.Warn("backfill: skipping file with unknown FileType string",
				zap.String("fileUID", r.FileUID.String()),
				zap.String("fileType", r.FileType))
			continue
		}

		var meta *structpb.Struct
		if r.ExternalMetadata != "" {
			// ExternalMetadata is optional and free-form on pre-rollout
			// files; unmarshal best-effort and fall back to nil so the
			// activity can still run. We do not fail the whole batch
			// for a malformed record — that would block every later
			// file from being processed.
			s := &structpb.Struct{}
			if err := protojson.Unmarshal([]byte(r.ExternalMetadata), s); err == nil {
				meta = s
			}
		}

		descs = append(descs, BackfillFileDescriptor{
			FileUID:     r.FileUID,
			KBUID:       r.KBUID,
			FileType:    artifactpb.File_Type(ft),
			StoragePath: r.StoragePath,
			DisplayName: r.DisplayName,
			Metadata:    meta,
		})
	}

	// Guard rail: guarantee that the cursor always advances. If every
	// row in the batch was filtered out (should be rare — only happens
	// when the file_type column drifts), still bump the cursor to the
	// last raw UID so the caller does not spin on the same page.
	if len(descs) == 0 && len(rows) > 0 {
		descs = append(descs, BackfillFileDescriptor{
			FileUID:  rows[len(rows)-1].FileUID,
			KBUID:    types.KBUIDType(uuid.Nil),
			FileType: artifactpb.File_TYPE_UNSPECIFIED,
		})
	}

	return &ListFilesMissingThumbnailsActivityResult{Files: descs}, nil
}

// Ensure the repository field is interpreted correctly in tests. The
// generated mock satisfies `repository.Repository`, and its
// `ListFilesMissingThumbnails` shim is auto-created by mockery on the
// next `go generate`.
var _ = (*repository.FileMissingThumbnail)(nil)
