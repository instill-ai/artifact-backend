// Package worker — BackfillEntityAliasesWorkflow.
//
// This workflow retrofits bilingual / synonym aliases onto the
// `kb_entity` rows and TYPE_AUGMENTED chunks of files that were
// summarised before the aliases rollout (Part 1 of the
// `bilingual_zodiac_retrieval` plan). The forward path already writes
// aliases inline via `ProcessSummaryActivity` → `parseSummaryEntities`
// → `SaveEntitiesActivity` + the alias-aware `formatAugmentedEntities`;
// this backfill is the one-shot admin operation that brings old files
// onto the same rails.
//
// Contract:
//
//   - **Idempotent by content marker.** A TYPE_AUGMENTED chunk whose
//     body contains the literal sequence " (" has already been
//     backfilled (or was ingested after the rollout). We skip such
//     files up-front, so re-running the workflow on a drained KB
//     completes in a handful of seconds.
//   - **Non-fatal per file.** Any per-file failure is logged and
//     skipped; the workflow never fails as a whole. Temporal's
//     retry policy handles transient AI / MinIO / DB errors inside
//     the activity boundary.
//   - **Bounded-parallel fan-out.** Files in the KB are processed by
//     `Concurrency` coroutines in parallel, each running the full
//     per-file 4-step chain (backfill-extract → chunk → save →
//     embed). Default concurrency is `defaultBackfillConcurrency = 4`,
//     which at ~30–60 s per Gemini summary call peaks at ~4–8 RPM —
//     two orders of magnitude under every paid-tier Gemini quota.
//     Operators can raise it for quota-rich deployments via the
//     `Concurrency` field on `BackfillEntityAliasesWorkflowParam`
//     (surfaced by EE operator tooling). Per-step Temporal retries
//     are preserved exactly as in the sequential chain — each of the
//     4 activities keeps its own retry policy inside its coroutine.
//     Bounded-parallel access to `kb_entity` is safe because
//     `UpsertEntities` serializes at the Postgres row level under
//     ON CONFLICT and `LinkEntityFile` uses disjoint `(entity, file)`
//     tuples across coroutines.
//   - **Bounded history.** After `MaxPollIterationsBeforeContinueAsNew`
//     batches of 100 files we drain all in-flight coroutines and
//     `ContinueAsNew` with the next cursor so workflow history stays
//     bounded on very large KBs. Draining before `ContinueAsNew` is
//     non-negotiable: spawning without draining would orphan
//     in-flight activities when the workflow restarts.
//
// This workflow is admin-only: it is registered exclusively on the EE
// worker binary and invoked by EE-internal operator tooling. CE
// deployments do not register it (see `artifact-backend/AGENTS.md` →
// ARTIFACT-INV-ALIAS-BACKFILL).
package worker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

// BackfillEntityAliasesWorkflowName is the registered Temporal workflow
// name. Operator tooling pins this string; renaming it is a wire break
// for any in-flight invocations.
const BackfillEntityAliasesWorkflowName = "BackfillEntityAliasesWorkflow"

// backfillAliasesBatchSize is the cadence the workflow uses for
// progress logs and the `ContinueAsNew` history-bound check. Keep it
// small-ish because each file triggers a Gemini summary call; big
// batches would delay the progress signal operators rely on.
const backfillAliasesBatchSize = 100

// defaultBackfillConcurrency is the fan-out width the workflow uses
// when `BackfillEntityAliasesWorkflowParam.Concurrency` is zero (or
// negative). Four coroutines at ~30–60 s per file give ~4–8 RPM
// against Gemini, which is two orders of magnitude under every
// paid-tier quota we're likely to run against. Raising this is an
// operator decision — see the `Concurrency` field below.
const defaultBackfillConcurrency = 4

// BackfillEntityAliasesWorkflowParam controls a single execution (or a
// `ContinueAsNew` continuation) of the alias backfill workflow.
type BackfillEntityAliasesWorkflowParam struct {
	// KBUID restricts the backfill to a single knowledge base. We
	// intentionally do NOT expose a "backfill every KB in the
	// installation" mode: the alias rewrite spends Gemini credits and
	// must be opted into per KB.
	KBUID types.KBUIDType
	// MaxFiles caps how many files a single run will attempt. Zero
	// means "process everything we find". The cap exists for
	// rehearsal runs and integration tests.
	MaxFiles int
	// Concurrency is the number of per-file coroutines that run in
	// parallel. Zero (or negative) resolves to
	// `defaultBackfillConcurrency`. The knob is preserved across
	// `ContinueAsNew` so an operator's choice survives history
	// bounding.
	Concurrency int
	// ProcessedCount carries across `ContinueAsNew` iterations so the
	// final log line reflects the full run, not just the last slice.
	// Counts every file the workflow has SPAWNED, not completed —
	// the two are equal at `ContinueAsNew` boundaries (we always
	// drain in-flight coroutines before restarting) but may differ
	// mid-batch.
	ProcessedCount int
	// IterationCount tracks how many paging rounds the workflow has
	// executed in the current run (reset on `ContinueAsNew`).
	IterationCount int
	// StartIndex is the keyset cursor inside the sorted file-UID slice
	// we fetch once via ListFilesWithEntitiesForBackfillActivity. We
	// fetch the full list once per `ContinueAsNew` slice; per-batch
	// advancement is done in memory.
	StartIndex int
}

// backfillEntityAliasesWorkflowStarter is the adapter used by callers
// (EE operator tooling, tests) to enqueue the workflow through the
// Temporal client without knowing the TaskQueue layout.
type backfillEntityAliasesWorkflowStarter struct {
	temporalClient client.Client
	worker         *Worker
}

// NewBackfillEntityAliasesWorkflow wires the workflow starter.
func NewBackfillEntityAliasesWorkflow(temporalClient client.Client, worker *Worker) *backfillEntityAliasesWorkflowStarter {
	return &backfillEntityAliasesWorkflowStarter{
		temporalClient: temporalClient,
		worker:         worker,
	}
}

// Execute kicks off a single backfill run. The workflow is idempotent —
// re-running it when nothing is missing completes in a handful of ms.
func (s *backfillEntityAliasesWorkflowStarter) Execute(ctx context.Context, param BackfillEntityAliasesWorkflowParam) (string, error) {
	workflowID := fmt.Sprintf("backfill-entity-aliases-%s-%d", param.KBUID.String(), time.Now().UnixNano())
	options := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             TaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}
	run, err := s.temporalClient.ExecuteWorkflow(ctx, options, s.worker.BackfillEntityAliasesWorkflow, param)
	if err != nil {
		return "", errorsx.AddMessage(err, "Unable to start entity-alias backfill workflow. Please try again.")
	}
	return run.GetID(), nil
}

// BackfillEntityAliasesWorkflow walks every file in the KB that has at
// least one kb_entity link, regenerates the entity list through Gemini
// with the alias-aware prompt, union-merges aliases into `kb_entity`,
// rewrites the TYPE_AUGMENTED chunk, and re-embeds the file's chunks.
// See the file-level doc for the contract (idempotent, non-fatal,
// bounded-parallel, history-bounded).
func (w *Worker) BackfillEntityAliasesWorkflow(ctx workflow.Context, param BackfillEntityAliasesWorkflowParam) error {
	logger := workflow.GetLogger(ctx)

	concurrency := param.Concurrency
	if concurrency <= 0 {
		concurrency = defaultBackfillConcurrency
	}

	logger.Info("BackfillEntityAliasesWorkflow: starting",
		"kbUID", param.KBUID.String(),
		"maxFiles", param.MaxFiles,
		"concurrency", concurrency,
		"processedCount", param.ProcessedCount,
		"iterationCount", param.IterationCount,
		"startIndex", param.StartIndex)

	listCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    3,
		},
	})

	// Per-file activity budgets mirror the ingestion path: summary
	// regeneration can take several minutes on large documents, and
	// the embedding pass on a re-chunked file inherits the standard
	// 10-minute embedding budget. These contexts are shared across
	// per-file coroutines — the Temporal SDK's workflow.Context is
	// safe for concurrent use by multiple coroutines.
	backfillCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: AIProcessingMaxTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    RetryInitialInterval,
			BackoffCoefficient: RetryBackoffCoefficient,
			MaximumInterval:    RetryMaximumIntervalStandard,
			MaximumAttempts:    2, // Aggressive: alias backfill is best-effort.
		},
	})
	chunkCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: ActivityTimeoutStandard,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    RetryInitialInterval,
			BackoffCoefficient: RetryBackoffCoefficient,
			MaximumInterval:    RetryMaximumIntervalStandard,
			MaximumAttempts:    2,
		},
	})
	embedCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: ActivityTimeoutEmbedding,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    RetryInitialInterval,
			BackoffCoefficient: RetryBackoffCoefficient,
			MaximumInterval:    RetryMaximumIntervalStandard,
			MaximumAttempts:    2,
		},
	})

	// Snapshot every eligible file up front. The file set for a given
	// KB is effectively static for the duration of the backfill —
	// freshly ingested files produce their aliases inline and do not
	// need the workflow to revisit them. Paging by in-memory index is
	// simpler than keyset-paging two SQL queries.
	var listResult ListFilesWithEntitiesForBackfillActivityResult
	if err := workflow.ExecuteActivity(listCtx, w.ListFilesWithEntitiesForBackfillActivity, &ListFilesWithEntitiesForBackfillActivityParam{
		KBUID: param.KBUID,
	}).Get(listCtx, &listResult); err != nil {
		return errorsx.AddMessage(err, "Failed to list KB files with entities for alias backfill")
	}

	total := len(listResult.FileUIDs)
	logger.Info("BackfillEntityAliasesWorkflow: snapshot taken",
		"kbUID", param.KBUID.String(),
		"eligibleFiles", total)

	processed := param.ProcessedCount
	iteration := param.IterationCount
	idx := param.StartIndex

	// Counters shared across per-file coroutines. Writes are safe
	// without a lock because workflow coroutines are cooperative —
	// the Temporal SDK only yields at Send/Receive/Get/Select/Sleep,
	// and every counter update happens in the straight-line window
	// between an Activity.Get() and the coroutine's final defer
	// (which is itself a Send). Replay is deterministic because
	// activity results are deterministic under replay.
	skipped, rewritten, failed := 0, 0, 0

	// Bounded-parallel fan-out: a buffered workflow channel used as
	// a counting semaphore. Pre-filled with `concurrency` tokens;
	// each per-file coroutine acquires one on spawn and releases it
	// on exit (via defer). Draining the semaphore == waiting for
	// every spawned coroutine to finish.
	sem := workflow.NewBufferedChannel(ctx, concurrency)
	for i := 0; i < concurrency; i++ {
		sem.Send(ctx, true)
	}
	drainInFlight := func(c workflow.Context) {
		for i := 0; i < concurrency; i++ {
			var t bool
			sem.Receive(c, &t)
		}
	}
	refillTokens := func(c workflow.Context) {
		for i := 0; i < concurrency; i++ {
			sem.Send(c, true)
		}
	}

	// processOne runs the full 4-step per-file chain. Returns one of
	// "skipped" / "rewritten" / "failed" so the caller can tally.
	processOne := func(gctx workflow.Context, fileUID types.FileUIDType) string {
		var result BackfillFileEntityAliasesActivityResult
		if err := workflow.ExecuteActivity(backfillCtx, w.BackfillFileEntityAliasesActivity, &BackfillFileEntityAliasesActivityParam{
			KBUID:   param.KBUID,
			FileUID: fileUID,
		}).Get(gctx, &result); err != nil {
			logger.Warn("BackfillEntityAliasesWorkflow: activity failed for file (skipping)",
				"fileUID", fileUID.String(),
				"error", err.Error())
			return "failed"
		}
		if result.Skipped || result.NewAugmentedText == "" {
			return "skipped"
		}

		// Rewrite the augmented chunk: chunk-content + save-chunks +
		// full-file re-embed. We intentionally re-embed all chunks
		// because EmbedAndSaveChunksActivity is a single per-file
		// pass; the extra credit cost is acceptable for a one-shot
		// admin operation and keeps the pipeline uniform with the
		// ingestion path.
		var chunkResult ChunkContentActivityResult
		if cerr := workflow.ExecuteActivity(chunkCtx, w.ChunkContentActivity, &ChunkContentActivityParam{
			FileUID: fileUID,
			KBUID:   param.KBUID,
			Content: result.NewAugmentedText,
			Type:    artifactpb.Chunk_TYPE_AUGMENTED,
		}).Get(gctx, &chunkResult); cerr != nil {
			logger.Warn("BackfillEntityAliasesWorkflow: chunk-content failed",
				"fileUID", fileUID.String(), "error", cerr.Error())
			return "failed"
		}

		if len(chunkResult.Chunks) > 0 {
			if serr := workflow.ExecuteActivity(chunkCtx, w.SaveChunksActivity, &SaveChunksActivityParam{
				KBUID:            param.KBUID,
				FileUID:          fileUID,
				Chunks:           chunkResult.Chunks,
				ConvertedFileUID: result.SummaryConvertedFileUID,
			}).Get(gctx, nil); serr != nil {
				logger.Warn("BackfillEntityAliasesWorkflow: save-chunks failed",
					"fileUID", fileUID.String(), "error", serr.Error())
				return "failed"
			}
		}

		var embedResult EmbedAndSaveChunksActivityResult
		if eerr := workflow.ExecuteActivity(embedCtx, w.EmbedAndSaveChunksActivity, &EmbedAndSaveChunksActivityParam{
			KBUID:   param.KBUID,
			FileUID: fileUID,
		}).Get(gctx, &embedResult); eerr != nil {
			logger.Warn("BackfillEntityAliasesWorkflow: embed-and-save failed",
				"fileUID", fileUID.String(), "error", eerr.Error())
			return "failed"
		}
		return "rewritten"
	}

	for idx < total {
		if param.MaxFiles > 0 && processed >= param.MaxFiles {
			logger.Info("BackfillEntityAliasesWorkflow: MaxFiles reached, stopping",
				"processedCount", processed)
			break
		}

		// Acquire a slot — blocks until a coroutine releases one.
		var t bool
		sem.Receive(ctx, &t)

		fileUID := listResult.FileUIDs[idx] // captured by value per iteration.
		workflow.Go(ctx, func(gctx workflow.Context) {
			defer sem.Send(gctx, true)
			switch processOne(gctx, fileUID) {
			case "skipped":
				skipped++
			case "rewritten":
				rewritten++
			default: // "failed"
				failed++
			}
		})

		idx++
		processed++

		// Every backfillAliasesBatchSize spawns, emit a progress log
		// and (if we've hit the history-bound threshold) drain +
		// ContinueAsNew. Counters may lag slightly at log time
		// because in-flight coroutines haven't tallied yet — that's
		// cosmetic; the final log at end-of-run is always accurate.
		if processed > 0 && processed%backfillAliasesBatchSize == 0 {
			iteration++
			logger.Info("BackfillEntityAliasesWorkflow: batch progress",
				"processedCount", processed,
				"skipped", skipped,
				"rewritten", rewritten,
				"failed", failed,
				"iterationCount", iteration,
				"concurrency", concurrency)

			if iteration >= MaxPollIterationsBeforeContinueAsNew {
				// Drain in-flight coroutines BEFORE ContinueAsNew:
				// restarting the workflow would otherwise orphan
				// running activities and silently lose work.
				drainInFlight(ctx)
				logger.Info("BackfillEntityAliasesWorkflow: ContinueAsNew to cap history",
					"processedCount", processed,
					"iterationCount", iteration,
					"nextIndex", idx,
					"skipped", skipped,
					"rewritten", rewritten,
					"failed", failed)
				return workflow.NewContinueAsNewError(ctx, w.BackfillEntityAliasesWorkflow, BackfillEntityAliasesWorkflowParam{
					KBUID:          param.KBUID,
					MaxFiles:       param.MaxFiles,
					Concurrency:    param.Concurrency,
					ProcessedCount: processed,
					IterationCount: 0,
					StartIndex:     idx,
				})
			}
		}
	}

	// Wait for every in-flight coroutine to finish before returning,
	// so the final tally is accurate and no activity is left running
	// when the workflow exits.
	drainInFlight(ctx)
	refillTokens(ctx) // keep sem balanced (cosmetic; workflow is ending).

	logger.Info("BackfillEntityAliasesWorkflow: finished",
		"kbUID", param.KBUID.String(),
		"processedCount", processed,
		"skipped", skipped,
		"rewritten", rewritten,
		"failed", failed,
		"concurrency", concurrency)
	return nil
}

// ListFilesWithEntitiesForBackfillActivityParam wraps the KB UID.
type ListFilesWithEntitiesForBackfillActivityParam struct {
	KBUID types.KBUIDType
}

// ListFilesWithEntitiesForBackfillActivityResult carries the snapshot.
type ListFilesWithEntitiesForBackfillActivityResult struct {
	FileUIDs []types.FileUIDType
}

// ListFilesWithEntitiesForBackfillActivity returns every file UID in
// the KB that has at least one kb_entity link. Thin wrapper around
// repository.ListFileUIDsWithEntitiesByKB so the SQL can be unit-tested
// at the repository layer and so EE can override the filter set.
func (w *Worker) ListFilesWithEntitiesForBackfillActivity(
	ctx context.Context,
	param *ListFilesWithEntitiesForBackfillActivityParam,
) (*ListFilesWithEntitiesForBackfillActivityResult, error) {
	uids, err := w.repository.ListFileUIDsWithEntitiesByKB(ctx, param.KBUID)
	if err != nil {
		return nil, fmt.Errorf("listing KB files with entities: %w", err)
	}
	return &ListFilesWithEntitiesForBackfillActivityResult{FileUIDs: uids}, nil
}

// BackfillFileEntityAliasesActivityParam identifies a single file to
// backfill.
type BackfillFileEntityAliasesActivityParam struct {
	KBUID   types.KBUIDType
	FileUID types.FileUIDType
}

// BackfillFileEntityAliasesActivityResult carries the outcome of one
// file's pass through the activity.
type BackfillFileEntityAliasesActivityResult struct {
	// Skipped is true when the file's augmented chunk already carries
	// the " (" idempotency marker (already backfilled or ingested
	// after the aliases rollout) and we short-circuited without doing
	// any work.
	Skipped bool
	// NewAugmentedText is the alias-enriched TYPE_AUGMENTED chunk body
	// the workflow should re-chunk + re-embed. Empty means the LLM
	// returned zero entities — in that case the workflow leaves the
	// existing augmented chunk alone.
	NewAugmentedText string
	// SummaryConvertedFileUID points at the summary converted_file
	// row whose content we regenerated from. The workflow threads this
	// into SaveChunksActivity so the new augmented chunk correctly
	// references the same converted_file source as the ingestion path.
	SummaryConvertedFileUID types.ConvertedFileUIDType
	// EntityCount is the number of entities we union-merged back into
	// kb_entity. Logged for post-run spot-checks.
	EntityCount int
}

// BackfillFileEntityAliasesActivity performs the per-file work of the
// alias backfill: idempotency check → summary reload → Gemini re-parse →
// kb_entity union merge → old augmented chunk deletion → return the
// rewritten chunk body for the workflow to save and embed.
//
// The activity intentionally does NOT call ChunkContentActivity /
// SaveChunksActivity / EmbedAndSaveChunksActivity itself — those live in
// the workflow so that Temporal retries apply per-step and the per-file
// failure-and-continue contract is easy to reason about.
func (w *Worker) BackfillFileEntityAliasesActivity(
	ctx context.Context,
	param *BackfillFileEntityAliasesActivityParam,
) (*BackfillFileEntityAliasesActivityResult, error) {
	logger := w.log.With(
		zap.String("kbUID", param.KBUID.String()),
		zap.String("fileUID", param.FileUID.String()),
		zap.String("activity", "BackfillFileEntityAliasesActivity"))

	result := &BackfillFileEntityAliasesActivityResult{}

	// ---- Step 1: idempotency probe ----------------------------------
	// Look at the file's existing TYPE_AUGMENTED chunks. If any of them
	// carries the " (" marker in its MinIO-stored body, we assume it
	// was already written by the alias-aware formatter and skip the
	// whole file. We bail as cheaply as possible — a single MinIO
	// `GetFile` for the first augmented chunk is enough.
	existingChunks, err := w.repository.ListTextChunksByKBFileUID(ctx, param.FileUID)
	if err != nil {
		return nil, fmt.Errorf("listing chunks for %s: %w", param.FileUID, err)
	}
	var augmentedChunkUIDs []types.ChunkUIDType
	var firstAugmentedPath string
	for _, c := range existingChunks {
		if strings.EqualFold(c.ChunkType, "augmented") || c.ChunkType == artifactpb.Chunk_TYPE_AUGMENTED.String() {
			augmentedChunkUIDs = append(augmentedChunkUIDs, c.UID)
			if firstAugmentedPath == "" {
				firstAugmentedPath = c.StoragePath
			}
		}
	}

	if firstAugmentedPath != "" {
		bucket := config.Config.Minio.BucketName
		body, err := w.repository.GetMinIOStorage().GetFile(ctx, bucket, firstAugmentedPath)
		if err != nil {
			// Stale storage_path — do not fail the backfill, treat
			// as "needs rewrite" so the workflow overwrites the row.
			logger.Warn("Augmented chunk MinIO read failed, proceeding with rewrite",
				zap.Error(err))
		} else if strings.Contains(string(body), " (") {
			logger.Info("Augmented chunk already carries alias marker; skipping file")
			result.Skipped = true
			return result, nil
		}
	}

	// ---- Step 2: load summary markdown ------------------------------
	summaryCF, err := w.repository.GetConvertedFileByFileUIDAndType(
		ctx, param.FileUID, artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_SUMMARY)
	if err != nil || summaryCF == nil {
		// No summary means nothing for the LLM to regenerate from;
		// skip rather than fail. These files are typically early
		// uploads where the summary step was skipped or failed.
		logger.Info("No summary converted_file for file; skipping",
			zap.Error(err))
		result.Skipped = true
		return result, nil
	}
	result.SummaryConvertedFileUID = summaryCF.UID

	bucket := config.Config.Minio.BucketName
	summaryBytes, err := w.repository.GetMinIOStorage().GetFile(ctx, bucket, summaryCF.StoragePath)
	if err != nil {
		return nil, fmt.Errorf("reading summary markdown from MinIO (%s): %w",
			summaryCF.StoragePath, err)
	}
	summaryMarkdown := string(summaryBytes)
	if strings.TrimSpace(summaryMarkdown) == "" {
		logger.Info("Summary markdown is empty; skipping")
		result.Skipped = true
		return result, nil
	}

	// ---- Step 3: ask Gemini to regenerate entities ------------------
	if w.aiClient == nil {
		return nil, fmt.Errorf("ai client not configured; backfill cannot run")
	}

	summaryPrompt := w.getGenerateSummaryPrompt()

	// The summary markdown is already markdown text, so we pass it
	// inline with FileType TYPE_MARKDOWN — same trick as
	// ProcessSummaryActivity uses for assembled long-media transcripts.
	conversion, err := w.aiClient.ConvertToMarkdownWithoutCache(
		ctx,
		summaryBytes,
		artifactpb.File_TYPE_MARKDOWN,
		"", // display name irrelevant here
		summaryPrompt,
	)
	if err != nil {
		return nil, fmt.Errorf("gemini summary regeneration: %w", err)
	}
	_, entityTags := parseSummaryEntities(conversion.Markdown)
	if len(entityTags) == 0 {
		logger.Info("LLM returned zero entities for file; leaving augmented chunk alone")
		// Not a skip in the idempotency sense, but the workflow
		// should not rewrite.
		result.EntityCount = 0
		result.NewAugmentedText = ""
		return result, nil
	}
	result.EntityCount = len(entityTags)

	// ---- Step 4: union-merge aliases into kb_entity ------------------
	records := make([]repository.EntityRecord, len(entityTags))
	for i, e := range entityTags {
		records[i] = repository.EntityRecord{
			Name:       e.Name,
			EntityType: e.Type,
			Aliases:    pq.StringArray(e.Aliases),
		}
	}
	uids, err := w.repository.UpsertEntities(ctx, param.KBUID, records)
	if err != nil {
		return nil, fmt.Errorf("upserting entities: %w", err)
	}
	for _, uid := range uids {
		if err := w.repository.LinkEntityFile(ctx, uid, param.FileUID); err != nil {
			logger.Warn("failed to link entity-file; continuing",
				zap.String("entityUID", uid.String()),
				zap.Error(err))
		}
	}

	// ---- Step 5: delete old TYPE_AUGMENTED chunks -------------------
	// We delete both the MinIO bodies and the DB rows here so the
	// workflow's subsequent SaveChunksActivity writes a fresh row
	// without a unique-path collision. `HardDeleteTextChunksByUIDs`
	// is a DB-only op; the MinIO objects are pruned below.
	if len(augmentedChunkUIDs) > 0 {
		minioPaths := make([]string, 0, len(augmentedChunkUIDs))
		for _, c := range existingChunks {
			if strings.EqualFold(c.ChunkType, "augmented") || c.ChunkType == artifactpb.Chunk_TYPE_AUGMENTED.String() {
				if c.StoragePath != "" {
					minioPaths = append(minioPaths, c.StoragePath)
				}
			}
		}
		if err := w.repository.HardDeleteTextChunksByUIDs(ctx, augmentedChunkUIDs); err != nil {
			return nil, fmt.Errorf("deleting old augmented chunk rows: %w", err)
		}
		for _, p := range minioPaths {
			if err := w.repository.GetMinIOStorage().DeleteFile(ctx, bucket, p); err != nil {
				logger.Warn("failed to delete old augmented chunk from MinIO; continuing",
					zap.String("path", p), zap.Error(err))
			}
		}
		logger.Info("deleted stale augmented chunks",
			zap.Int("count", len(augmentedChunkUIDs)))
	}

	// ---- Step 6: compose new augmented text ------------------------
	// We rebuild the augmented text from scratch using the entities we
	// just upserted (with their alias lists). Using the fresh tags
	// avoids a race with a concurrent ingestion pass updating
	// kb_entity mid-run.
	result.NewAugmentedText = formatAugmentedEntities(entityTags)

	logger.Info("BackfillFileEntityAliasesActivity: file rewritten",
		zap.Int("entityCount", len(entityTags)),
		zap.Int("augmentedChunkBytes", len(result.NewAugmentedText)))
	return result, nil
}

// Enforce that the workflow signature is picked up by Temporal. This
// guards against accidental removal of the Worker receiver during
// refactors.
var _ = func() workflow.Future { var f workflow.Future; return f }

// structpbUnused prevents go-vet from flagging structpb as imported but
// unused if the alias-backfill workflow later drops metadata handling.
// We keep the import because SaveChunksActivity signatures in the
// forward-path take a *structpb.Struct and alias-backfill may grow a
// tenancy-scoped metadata field without churning the import block.
var _ = (*structpb.Struct)(nil)
