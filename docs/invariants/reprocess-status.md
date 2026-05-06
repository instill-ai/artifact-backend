# Reprocess status invariants

## Reprocess-status flicker elimination (`ARTIFACT-INV-reprocess-status-flicker-elimination`)

`file.process_status` is the single observable handle that downstream consumers (the console, downstream services that derive cell/preview status, anything paginating `ListFiles` with status filters) use to render whether a file is currently being processed. When `ProcessFileWorkflow` is restarted on a file whose persisted status is `FILE_PROCESS_STATUS_FAILED` or `FILE_PROCESS_STATUS_COMPLETED` — the auto-reprocess branch — the workflow MUST persist `FILE_PROCESS_STATUS_PROCESSING` to the database **before any stage-level activity runs**. Otherwise the gap between "the workflow has decided to reprocess" and "the next stage activity writes its own status" is an observable window where the UI re-reads the stale `FAILED` (or `COMPLETED`) row and surfaces a transient "Processing failed" badge / stale-completed thumbnail before the new pipeline lands.

**Non-negotiable rules** (`pkg/worker/process_file_workflow.go`, `pkg/worker/status_activity.go`):

1. **The auto-reprocess detection block persists immediately.** When the metadata-evaluation loop observes `startStatus ∈ {COMPLETED, FAILED}` and locally promotes it to `PROCESSING`, the workflow MUST execute `UpdateFileStatusActivity{Status: PROCESSING, Message: ""}` before returning to the loop or proceeding to Step 2. The local field assignment alone (`startStatus = PROCESSING`) is invisible to consumers — only the activity write is.
2. **The persistence is fire-and-forget on failure.** The workflow MUST log a `Warn` and continue if the persistence call errors. A missed write degrades gracefully back to the pre-fix flicker (Step 2a's later persistence still fires); blocking the reprocess on this write would replace a UX flicker with an outage.
3. **Step 2a's `UpdateFileStatusActivity(PROCESSING)` stays.** The new persistence is additive: Step 2a's per-file write at the start of full-processing remains, both because it covers the originally-`NOTSTARTED` / `PROCESSING` cases (which never enter the auto-reprocess detection block) and because it is the structural marker that "Step 2 is starting." Removing it would re-introduce the flicker for non-auto-reprocess paths.
4. **Activity ordering is enforced by test, not by comment.** The deterministic Temporal `WorkflowEnvironment` test `TestProcessFileWorkflow_AutoReprocessFromFailed_PersistsProcessingBeforeAnyOtherActivity` counts `UpdateFileStatusActivity` calls before the first `DeleteOldConvertedFilesActivity` (the first stage activity in Step 2b). Pre-fix the count is 1 (Step 2a); post-fix the count is 2 (auto-reprocess persistence + Step 2a). The test asserts `>= 2` and freezes the contract — removing the new persistence drops the count back to 1 and turns the test red.

**Why this fix is elegant, robust, and future-proof:**

- **Elegant** — single insertion point inside the existing auto-reprocess detection block. No new activities, no new RPC, no new state machine. Reuses the activity that every other status transition in this file already calls.
- **Robust** — handles all auto-reprocess paths uniformly because the detection block is the single gate (`startStatus ∈ {COMPLETED, FAILED}`). The fire-and-forget envelope means a transient DB error degrades to the pre-fix flicker rather than to a stuck file.
- **Future-proof** — every future starting status that enters the auto-reprocess branch (e.g. a hypothetical `PARTIAL_FAILURE` added to `FileProcessStatus`) inherits the persistence behaviour for free; the rule is "if the local mutation runs, the activity runs."

**Regression case:** auto-reprocess of an image cell whose initial run had failed produced a transient "Processing failed" badge in the cell card before the regenerated image rendered. Pre-fix: the workflow's metadata loop ran `GetFileStatus` → `GetFileMetadata` → set `startStatus = PROCESSING` (locally only) → exited the loop → entered Step 2 → Step 2a's `UpdateFileStatusActivity(PROCESSING)` was the first DB write of `PROCESSING`. The console's polling read of `file.process_status` between the metadata loop ending and Step 2a starting saw `FAILED`, rendered the "Processing failed" cell, and only later (after Step 2a's write reached the DB) re-rendered with the regenerated image.

**Tests pinning this invariant:**

- `pkg/worker/process_file_workflow_test.go::TestProcessFileWorkflow_AutoReprocessFromFailed_PersistsProcessingBeforeAnyOtherActivity` — Temporal `testsuite.WorkflowEnvironment` + `OnActivity` recorder. Mocks a file with `ProcessStatus = FILE_PROCESS_STATUS_FAILED`; asserts `>= 2` `UpdateFileStatusActivity` calls before the first `DeleteOldConvertedFilesActivity`, and that every status persisted in that window is `PROCESSING`.

## Reprocess-no-terminate race (`ARTIFACT-INV-REPROCESS-NO-TERMINATE-RACE`)

When two or more callers fire `ReprocessFileAdmin` (or the public
`ReprocessFile`) for the same `fileUID` within the lifetime of a single
`ProcessFileWorkflow`, exactly **one** workflow MUST run to completion;
every concurrent caller MUST observe `codes.AlreadyExists` (HTTP 409 via
grpc-gateway), and the file's `process_status` MUST never be
side-effected by a duplicate-start path. The pre-fix
terminate-and-restart implementation violated this: the terminator
wrote `process_status = FAILED` to the database between the previous
workflow's `WORKFLOW_EXECUTION_STATUS_TERMINATED` transition and the
freshly-started workflow's first `UpdateFileStatusActivity` write, so
every fan-in burst (autofill drift gate, repeated UI clicks, scripted
recoveries) corrupted the file's status to `FAILED` even though a fresh
workflow was legitimately running.

**Non-negotiable rules** (`pkg/worker/process_file_workflow.go`,
`pkg/handler/private.go`, `pkg/handler/file.go`):

1. **`processFileWorkflow.Execute` is the single idempotency gate.**
   Before calling `temporalClient.ExecuteWorkflow`, `Execute` MUST call
   `temporalClient.DescribeWorkflowExecution(ctx, workflowID, "")`. If
   `descErr == nil` AND
   `desc.WorkflowExecutionInfo.Status == WORKFLOW_EXECUTION_STATUS_RUNNING`,
   `Execute` MUST return `ErrReprocessAlreadyRunning` (a sentinel that
   wraps `errorsx.ErrAlreadyExists`). The pre-fix
   `terminateExistingWorkflow(...)` call MUST NOT be reintroduced —
   terminating-then-restarting is the bug.
2. **Every other `Describe` outcome falls through to the start path.**
   `descErr != nil` means either the workflow does not exist (the
   normal first-call case) or a transient Temporal RPC error (rare).
   Statuses other than `RUNNING` (`COMPLETED`, `FAILED`, `CANCELED`,
   `TERMINATED`, `TIMED_OUT`) all permit a fresh start because the
   prior run is finished. `WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE` is
   the second line of defence against rare double-starts that race the
   describe gate.
3. **Handlers call `service.ProcessFile` BEFORE the DB status stamp.**
   `ReprocessFileAdmin` (`pkg/handler/private.go`) and `ReprocessFile`
   (`pkg/handler/file.go`) MUST invoke `h.service.ProcessFile(...)`
   first. Only when that call returns `nil` may the handler call
   `Repository.ProcessFiles(...)` (which stamps `process_status =
   PROCESSING`). Reversing the order regresses to the pre-fix bug
   class: a duplicate caller would otherwise overwrite a
   `CHUNKING`/`EMBEDDING` status with `PROCESSING` even though
   `Execute` was about to refuse the start.
4. **Errors wrapping `errorsx.ErrAlreadyExists` map to
   `codes.AlreadyExists`.** Both handlers detect the wrapped sentinel
   via `errors.Is(err, errorsx.ErrAlreadyExists)` and return
   `status.Error(codes.AlreadyExists, ...)`. Any other path that
   produces this sentinel (future fan-in coalescing in callers) gets
   the same surfacing for free.
5. **Cell-worker pairing (`AUTOFILL-EE-INV-DRIFT-FANIN-COALESCE`).**
   The autofill cell-worker's drift gate MUST coalesce per-`fileUID`
   reprocess kicks via a Redis singleton before they reach this gate.
   Defence-in-depth: even if upstream coalescing is bypassed, this
   invariant ensures the file status never thrashes.

**Why this fix is elegant, robust, and future-proof:**

- **Elegant** — single check at the top of `Execute`, no new
  state machine, no new RPC. The existing
  `WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE` already guarantees that
  Temporal will accept a fresh start if the prior run is finished, so
  the gate only needs to refuse `RUNNING` — every other status falls
  through naturally.
- **Robust** — covers every caller (autofill, manual UI reprocess,
  admin RPC, future scripted callers) uniformly because they all
  funnel through `service.ProcessFile` → `Execute`. A transient
  Temporal RPC failure in `DescribeWorkflowExecution` is treated as
  "not running" and recovers via `ALLOW_DUPLICATE`; it can never wedge
  a file in `FAILED` again.
- **Future-proof** — any new caller (e.g. a queue-based reprocess
  pipeline) inherits the idempotent contract by construction; callers
  that want a "force restart" semantics MUST first cancel the running
  workflow explicitly through Temporal (out of band), which is the
  correct behaviour because it makes the destructive intent visible to
  the operator instead of hiding it inside a handler.

**Regression case:** during the 2026-05-10 `col-X0WlOK5wa9` audio-files
incident, the autofill drift gate fan-in plus the user's reprocess
clicks fired 4–10 concurrent `ReprocessFileAdmin` calls per file. The
pre-fix terminate-and-restart loop ran at sub-second intervals; every
file ended `FAILED` even though `ProcessFileWorkflow` was alive. Live
reproduction during the fix verified the same race (status flips to
`FAILED` mid-restart), and the post-fix path returns `AlreadyExists`
for the duplicates while the original workflow runs to `COMPLETED`.

**Tests pinning this invariant:**

- `integration-test/standalone-reprocess-idempotent.js` — k6 stack
  test that fires `CONCURRENT_REPROCESS_CALLS` (=8)
  `ReprocessFileAdmin` calls in a single `http.batch()` against a
  freshly-uploaded file. Asserts (a) exactly one HTTP 200 and `N-1`
  HTTP 409 responses, (b) every 409 carries `codes.AlreadyExists`, (c)
  the file eventually settles in `FILE_PROCESS_STATUS_COMPLETED` (never
  `FAILED`). Pre-fix every call returns 200 and the file ends up
  `FAILED`; post-fix the contract above holds.
- Cross-link: `agent-backend-ee/docs/invariants/autofill.md`
  (`AUTOFILL-EE-INV-DRIFT-FANIN-COALESCE`) — the upstream defence on
  the autofill side that coalesces drift-triggered reprocess calls per
  `fileUID`.

## Reprocess embed-only fast path (`ARTIFACT-INV-REPROCESS-EMBED-ONLY-FAST-PATH`)

When `ProcessFileWorkflow` is restarted on a file whose persisted
status is `FILE_PROCESS_STATUS_COMPLETED` and the cross-datastore
integrity probe classifies the drift as `INTEGRITY_STATE_MISSING_MILVUS`
**with `ChunkRowsInPg > 0` AND `ConvertedFilePresent == true`**, the
workflow MUST skip the full Convert + Cache + ProcessContent +
ProcessSummary + Chunk pipeline and route the file directly through
`EmbedAndSaveChunksActivity` followed by `UpdateEmbeddingMetadataActivity`
and a final `UpdateFileStatusActivity(COMPLETED)`. The pre-fix path
re-ran the entire pipeline (PDF re-extraction, LLM summary, re-chunk)
to recover what was structurally a re-embed-only failure mode; in the
2026-05-10 `col-LG9QdYz4NJ` recovery window 26 files paid the full
pipeline (~1 hour), where the fast path would have taken ~5 minutes.

**Non-negotiable rules** (`pkg/worker/process_file_workflow.go`,
`pkg/worker/probe_integrity_activity.go`):

1. **The probe only runs on the COMPLETED branch of the auto-reprocess
   detection block.** Files in `FAILED` may have partial / inconsistent
   `chunk` and `converted_file` rows from the failed run; embedding
   them silently would produce a "complete-looking" but corrupt index.
   The fast path is COMPLETED-only; FAILED stays on the full reprocess
   path even when the probe would otherwise classify it as
   `MISSING_MILVUS`.
2. **The probe MUST require all three preconditions.** `ChunkRowsInPg
   == 0` (EMPTY_PG) and `ConvertedFilePresent == false`
   (MISSING_CONVERTED_FILE) are deliberately not eligible for the fast
   path because there is no source material to embed. Any future
   `IntegrityState` value MUST be evaluated against the same three
   preconditions before being added to the fast-path branch.
3. **`startStatus` is promoted to `FILE_PROCESS_STATUS_EMBEDDING`.**
   The metadata-evaluation block sets `fm.shouldProcessEmbed = true`
   when `startStatus == EMBEDDING`. Promoting `startStatus` (rather
   than introducing a new `shouldProcessEmbedOnly` flag) reuses the
   existing flag plumbing — fewer code paths, fewer drift surfaces.
   The persistence write that already exists (rule 1 of
   `ARTIFACT-INV-reprocess-status-flicker-elimination`) writes
   `EMBEDDING` to the database immediately, so consumers polling
   `process_status` see the correct phase from the first observable
   moment.
4. **Probe failure falls through to the full reprocess path.** A
   transient `DescribeWorkflowExecution`-class Temporal error or a
   PostgreSQL hiccup MUST log `Warn` and proceed as if the probe
   returned `HEALTHY` (i.e. take the full reprocess path). The fast
   path is strictly a cost reduction, not a correctness contract; any
   classification ambiguity defaults to the safer-and-slower path.
5. **Embed-only stage failures terminate the workflow with FAILED.**
   The fast path MUST NOT silently re-route to the full reprocess if
   `EmbedAndSaveChunksActivity` fails — the file is flipped to
   `FAILED` via the existing `handleFileError` path. The cell-worker
   drift gate or the user's next reprocess click is the recovery; we
   do NOT double the recovery cost by running the full pipeline as a
   silent retry.
6. **`ProbeFileChunkIntegrityActivity` and
   `CheckFileChunkIntegrityAdmin` (handler) share classification
   semantics by construction.** The classification logic is duplicated
   intentionally — both must be kept byte-equivalent because the
   cell-worker's `RECOMMENDED_ACTION_REPROCESS_FILE` decision (driven
   by the handler) and the workflow's "skip Convert+Chunk" decision
   (driven by the activity) cannot diverge. Future schema changes to
   `IntegrityState` MUST update both call sites in the same commit.

**Why this fix is elegant, robust, and future-proof:**

- **Elegant** — a single conditional in the metadata-evaluation block
  decides between the fast path and the full path; the workflow flow
  is otherwise unchanged. No new RPC, no new schema, no caller-side
  changes (the cell-worker and handler still call the unchanged
  `ReprocessFileAdmin`). The activity-side classification is shared
  with the existing handler so the contract is enforced by code, not
  by docs.
- **Robust** — the three-precondition gate (`State == MISSING_MILVUS`
  AND `ChunkRowsInPg > 0` AND `ConvertedFilePresent == true`) covers
  the exact failure mode observed in production; partial failures
  (EMPTY_PG, MISSING_CONVERTED_FILE) keep the full reprocess; FAILED
  files keep the full reprocess; probe errors keep the full reprocess.
  The fast path can only fire when it is provably safe.
- **Future-proof** — adding a new `IntegrityState` (e.g. a future
  `INTEGRITY_STATE_MISSING_SUMMARY`) requires zero changes to the
  workflow's branching code; the new state would simply not match the
  three-precondition gate and would fall through to the full reprocess
  by default. The fast path is opt-in for each future state via a
  one-line addition to the gate, never opt-out.

**Regression case:** during the 2026-05-10 `col-LG9QdYz4NJ` recovery,
the cell-worker's drift gate (post-`AUTOFILL-EE-INV-DRIFT-FANIN-COALESCE`)
correctly fanned-in into a single `ReprocessFileByUID` per upstream
file, and the artifact-backend's idempotency gate
(`ARTIFACT-INV-REPROCESS-NO-TERMINATE-RACE`) correctly admitted
exactly one workflow per file. The remaining cost was the workflow
itself: 26 files × full pipeline ≈ 1 hour of recovery per drift event.
The fast path collapses that to a parallel embed-only pass that
completes in ~5 minutes for the same 26 files.

**Tests pinning this invariant:**

- `pkg/worker/probe_integrity_activity_test.go::TestProbeFileChunkIntegrityActivity_ClassifiesMissingMilvusForReembedFastPath`
  — asserts that the probe returns `MISSING_MILVUS` with
  `ChunkRowsInPg > 0` and `ConvertedFilePresent == true` when those
  three preconditions hold. Pre-fix the activity does not exist;
  post-fix the test pins the classification contract that the
  workflow's gate depends on.
- `pkg/worker/probe_integrity_activity_test.go::TestProbeFileChunkIntegrityActivity_ClassifiesEmptyPgAsNonFastPathSafe`
  — asserts that EMPTY_PG (chunks = 0) returns `EMPTY_PG`, not
  `MISSING_MILVUS`. This is the fast-path-disabling guard: the
  three-precondition gate will not match.
- `pkg/worker/probe_integrity_activity_test.go::TestProbeFileChunkIntegrityActivity_ClassifiesMissingConvertedFileSeparately`
  — asserts that a missing `converted_file` row classifies as
  `MISSING_CONVERTED_FILE` (also fast-path-disabled), not
  `MISSING_MILVUS`. Distinguishing these two is what keeps the fast
  path from silently embedding into a corrupt index.
- `pkg/worker/probe_integrity_activity_test.go::TestProbeFileChunkIntegrityActivity_ClassifiesHealthy`
  — asserts that a fully-intact file returns `HEALTHY`, so the
  workflow takes neither the fast path nor the full reprocess.
- `pkg/worker/probe_integrity_activity_test.go::TestProbeFileChunkIntegrityActivity_NotCompleted`
  — asserts the activity short-circuits for non-`COMPLETED` files.
  This guards against future callers wiring the activity into a
  branch other than the COMPLETED auto-reprocess block.
