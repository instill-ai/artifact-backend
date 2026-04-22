# CLAUDE.md — artifact-backend

Guidance for Claude Code when working in this repository.

## Purpose

`artifact-backend` is the Instill Artifact service: it orchestrates unstructured
data (documents, images, audio, video) into the **Instill Catalog** — an AI-ready
format for RAG and knowledge-base workflows. It owns file ingestion, conversion,
chunking, embedding, vector indexing, and catalog/knowledge-base CRUD.

- Docs: https://www.instill-ai.dev/docs/artifact/introduction
- API Reference: https://openapi.instill-ai.dev/reference/artifact

## Stack

- **Go** (`go 1.25.6`, see `go.mod`) — gRPC service with grpc-gateway REST shim.
- **gRPC / protobuf** via `github.com/instill-ai/protogen-go` (local replace
  → `../protogen-go`) and `google.golang.org/grpc`.
- **Persistence**: PostgreSQL via GORM (`gorm.io/driver/postgres`), SQL
  migrations with `golang-migrate/v4`.
- **Vector DB**: Milvus (`milvus-io/milvus/client/v2`).
- **Object storage**: MinIO (`minio-go/v7`) and GCS (`cloud.google.com/go/storage`).
- **Cache / queue**: Redis (`redis/go-redis/v9`).
- **Workflow orchestration**: Temporal (`go.temporal.io/sdk`) — the `worker`
  binary runs async file-processing workflows.
- **AI**: OpenAI (`openai-go/v3`), Google GenAI (`google.golang.org/genai`),
  tiktoken, plus the sibling `pipeline-backend` for pipeline-based processing.
- **Auth/ACL**: OpenFGA (`openfga/api/proto`).
- Config via `koanf` from `config/config.yaml` + env.

The sibling repos `../protogen-go` and `../x` are consumed via `replace`
directives in `go.mod` — keep them checked out and in sync.

## Binaries (from `cmd/`)

Four `main` packages, all built by the Dockerfile:

- `cmd/main` → `artifact-backend` — gRPC + REST API server.
- `cmd/migration` → `artifact-backend-migrate` — runs SQL migrations.
- `cmd/init` → `artifact-backend-init` — seeds initial data / ACL.
- `cmd/worker` → `artifact-backend-worker` — Temporal worker for file pipelines.

Production startup sequence (see `Makefile` `latest` target): `migrate` → `init`
→ `artifact-backend` (plus `worker` as a separate container).

## Architecture (`pkg/`)

- `handler/` — gRPC handlers implementing the protogen service interfaces.
- `service/` — business logic (knowledge base, file processing, chat/RAG).
- `repository/` — GORM data access: `file.go`, `chunk.go`, `embedding.go`,
  `knowledge_base.go`, `converted_file.go`, `tag.go`, `object.go`, `vector.go`,
  `system.go`, etc.
- `db/` — DB connection plus `db/migration/` SQL migration files.
- `worker/` — Temporal workflows/activities for async ingestion.
- `ai/` — AI client abstractions (OpenAI, Gemini, embedding).
- `pipeline/` — integration with `pipeline-backend` for conversion/chunking.
- `acl/` — OpenFGA permission checks.
- `types/`, `resource/`, `constant/`, `utils/` — shared helpers.
- `mock/` — minimock-generated mocks (`make go-gen`).

## Verified commands (from `Makefile`)

- `make help` — list targets.
- `make go-gen` — regenerate mocks via `go generate ./...`.
- `make unit-test` — `go test -v -race -coverpkg=./... -coverprofile=...`.
- `make integration-test` — k6-based tests against a running stack
  (parallel by default; `CI=true` runs sequentially). Requires `instill-core`
  running locally. Override `API_GATEWAY_URL` / `DB_HOST` when running inside
  the compose network (e.g. `API_GATEWAY_URL=api-gateway:8080 DB_HOST=pg_sql`).
- `make build-dev` / `make build-latest` — build Docker images.
- `make dev` / `make latest` — run container against the `instill-network`
  Docker network. `make latest` also starts the `-worker` container and runs
  migrate + init before the main binary.
- `make logs` / `make stop` / `make rm` / `make top` — container lifecycle.

Standard Go commands work too: `go build ./cmd/main`, `go test ./pkg/...`.

## Migration & proto workflow

- **SQL migrations** live in `pkg/db/migration/` and are applied by
  `cmd/migration`. Add a new numbered pair (`NNNNNN_name.up.sql` /
  `.down.sql`); `make latest` runs them automatically, or invoke
  `./artifact-backend-migrate` directly.
- **Protos** are not generated here. They live in `instill-ai/protobufs` and
  are published through `instill-ai/protogen-go`, which this repo consumes via
  the local `replace` → `../protogen-go`. To pick up new RPCs: update protos
  upstream, regenerate in `protogen-go`, then update handlers/services here.
- After changing repository or service interfaces, run `make go-gen` to
  refresh minimock mocks under `pkg/mock/`.

## Integration-test pairing rule

Go unit tests here cover handlers, repositories, activities, and worker
utilities in isolation. They do **not** exercise the full stack
(API → Temporal worker → Postgres + MinIO + Milvus). That end-to-end
surface lives in the sibling EE repo, `../artifact-backend-ee/integration-test/`,
as k6 suites (`rest-*.js` / `standalone-*.js`).

**Rule:** when a change here is observable at the API or data-store layer
— new/changed endpoint behavior, Temporal workflow ordering, activity
contracts (e.g. `FileContent.Error` in `GetFilesBatchActivity`), chunk
or embedding lifecycle, MinIO layout, data-integrity invariants
(`ARTIFACT-INV-*`), etc. — the CE PR MUST also add or update a matching
test in `../artifact-backend-ee/integration-test/`.

Do not merge a CE PR that fixes a production bug without an integration
test that would have caught it; the k6 suite is our only regression net
above the Go unit layer. If the CE behavior requires an out-of-band
perturbation that the public API cannot produce (e.g. dropping a MinIO
blob to fabricate an orphan chunk row), add a helper script under
`../artifact-backend-ee/integration-test/scripts/` and wrap it from
`helper.js`.

See `../artifact-backend-ee/AGENTS.md` → "Integration test contract" for
the EE-side authoring conventions.

## Invariants

- **ARTIFACT-INV-LIST-VIEW-FANOUT.** `PublicHandler.ListFiles` fans out
  per-row presigns through the shared `resolveDerivedResourceURI` helper
  (same code path as `GetFile`) via an `errgroup` with **bounded
  concurrency (16 workers)** whenever `ListFilesRequest.view` is set.
  `File.derived_resource_uri` is populated only for view-opted calls;
  unset `view` keeps the legacy metadata-only behaviour so cheap listings
  stay cheap. Per-row errors log at `warn` via `logx.GetZapLogger` and
  surface as an empty string rather than failing the whole call.
  `ListFilesAdminRequest.view` mirrors the public field verbatim so
  `agent-backend-ee` (and future internal callers) can opt into the same
  fan-out path. See `docs/blob-storage-architecture.md` "`ListFiles`
  fan-out presign" and the regression k6 suite
  `artifact-backend-ee/integration-test/standalone-list-files-view.js`.

- **ARTIFACT-INV-THUMBNAIL-NON-BLOCKING.** `GenerateThumbnailActivity`
  runs exactly once per file immediately after
  `StandardizeFileTypeActivity`, dispatched as a fire-and-forget
  `workflow.ExecuteActivity` future — the parent `ProcessFileWorkflow`
  MUST NOT `.Get()` it, because a preview failure is non-fatal. The
  activity is registered with `MaximumAttempts = 3`, produces a 1024 px
  WebP (quality 80) via the already-shipped `ffmpeg` binary, and persists
  through `SaveConvertedFile(..., "webp")` + a `converted_file` row of
  type `CONVERTED_FILE_TYPE_THUMBNAIL`. Idempotency is enforced at the
  activity boundary by `GetConvertedFileByFileUIDAndType`, so Temporal
  retries never create duplicate MinIO objects. Only image and video
  types render; PDF/document/audio/text short-circuit as
  `Skipped=true` so the frontend's mime-icon fallback stays the
  authoritative path (see console-ee `C-INV-21`). Callers populating
  `File.thumbnail_uri` MUST resolve the converted row through the same
  `GetConvertedFileByFileUIDAndType` lookup and render the stable
  `/v1alpha/blob-urls/{object_uid}` URL — never a per-row presign — so
  the resulting asset is Cloudflare-cacheable.

- **ARTIFACT-INV-THUMBNAIL-URI-RESOLUTION.** `File.thumbnail_uri` is
  populated by the `resolveThumbnailURI(ctx, kbFile)` helper in
  `pkg/handler/file.go`, which is called from both `GetFile` and the
  `ListFiles` view fan-out. The helper looks up
  `GetConvertedFileByFileUIDAndType(fileUID, CONVERTED_FILE_TYPE_THUMBNAIL)`,
  generates a 15-minute MinIO presign for the row's storage path, and
  encodes it through `service.EncodeBlobURL` so the final URL routes
  through `/v1alpha/blob-urls/...` and is Cloudflare-cacheable. All
  failure modes (missing row, presign error, encoding error) are
  non-fatal and return `nil`, because the frontend Files card preview
  owns a graceful fallback chain: `thumbnail_uri` →
  `derived_resource_uri` → mime icon (see console-ee `C-INV-21`).
  Resolution is independent of the requested `view`, so callers MUST
  attempt it on every `GetFile` response — not just when a content-
  bearing view was asked for. The 15-minute TTL matches the
  derived-resource presign and is well below the 6-month MinIO retention
  (see `ARTIFACT-INV-MINIO-RETENTION`), so a stale URL can always be
  refreshed by a single `GetFile` round-trip.

- **ARTIFACT-INV-THUMBNAIL-BACKFILL.** `BackfillThumbnailsWorkflow` in
  `pkg/worker/backfill_thumbnails_workflow.go` is the admin-only
  counterpart to the in-pipeline thumbnail activity: it retroactively
  produces thumbnails for files uploaded before `GenerateThumbnailActivity`
  shipped (or for any file that is missing its thumbnail for any reason).
  Contract:
  - Keyset-paginated on `file.uid` ascending, via
    `repository.ListFilesMissingThumbnails(afterFileUID, batchSize)`.
    The SQL uses a `NOT EXISTS` against `converted_file.converted_type =
    'CONVERTED_FILE_TYPE_THUMBNAIL'` and filters `file_type` on the
    shared `thumbnailableFileTypes` allow-list — this list MUST stay in
    sync with `classifyThumbnailSource` in
    `pkg/worker/thumbnail_activity.go`.
  - Runs on the ORIGINAL uploaded file at `file.storage_path`. The
    target bucket MUST be resolved via
    `object.BucketFromDestination(storagePath)` — never hardcoded to
    `config.Config.Minio.BucketName`. The `file` table serves two
    bucket layouts (`core-artifact` for `kb-<uid>/...` and legacy
    `uploaded-file/...` paths, `core-blob` for `ns-<ns>/obj-<obj>`
    object-API paths); hardcoding the default skipped every
    blob-bucket row with "key does not exist" in a prior regression.
    The standardized copy is unavailable for pre-rollout rows, and
    ffmpeg decodes every supported image/video codec natively so the
    output is identical. Regression test:
    `TestBackfillThumbnailsWorkflow_ResolvesBucketFromStoragePath` in
    `pkg/worker/backfill_thumbnails_workflow_test.go`.
  - Per-file failures are logged and skipped — the workflow is
    non-fatal, matching `ARTIFACT-INV-THUMBNAIL-NON-BLOCKING`.
    `GenerateThumbnailActivity`'s own MaximumAttempts = 3 absorbs
    transient errors inside the activity boundary.
  - Uses `ContinueAsNew` after `MaxPollIterationsBeforeContinueAsNew`
    batches so workflow history stays bounded on multi-million-row
    backfills. The cursor carries the last processed `FileUID` across
    continuations.
  - Invoked through `commander thumbnails backfill` (see
    `artifact-backend-ee/docs/runbook-backfill-thumbnails.md`, which
    lives in the EE repo because the backfill is an EE-operational
    task); never scheduled. Nothing in
    the hot path depends on it — if the workflow is never run, the
    frontend just falls back to `derived_resource_uri` for legacy
    rows.

- **ARTIFACT-INV-ALIAS-BACKFILL.** `BackfillEntityAliasesWorkflow` in
  `pkg/worker/backfill_alias_activity.go` is an EE-admin-only
  counterpart to the in-pipeline alias rewrite. It walks every file in
  a single KB that already has kb_entity links, regenerates the entity
  list through Gemini with the alias-aware summary prompt, union-merges
  aliases into `kb_entity.aliases`, and rewrites the file's
  TYPE_AUGMENTED chunk so BM25 can bridge bilingual / synonym surface
  forms (e.g. `chicken` → `雞`).

  **Registration surface.** The workflow and its activities live in
  this CE package for reuse but are registered **only** on the EE
  worker binary (`artifact-backend-ee/cmd/worker/main.go`). The CE
  `cmd/worker/main.go` MUST NOT register them: CE ships an OSS stack
  with no operator path to trigger an admin backfill, and CE prose /
  code MUST NOT reference EE-internal operator tooling (see
  `instill-core-ee/AGENTS.md` → "Commander is EE-private"). This is
  the common-code-in-CE / EE-only-registration split also used by the
  thumbnail backfill.

  Contract:
  - Scoped per `KBUID` — there is no "backfill every KB" mode.
    Each invocation opts in per KB because the rewrite spends Gemini
    credits.
  - Idempotent via the content marker: a TYPE_AUGMENTED chunk whose
    MinIO body contains the literal `" ("` sequence has already been
    written by the alias-aware `formatAugmentedEntities`, so the file
    is skipped. Re-running the workflow on a drained KB completes in
    seconds.
  - Snapshot-paged: the workflow pulls the full eligible file-UID
    slice once via `repository.ListFileUIDsWithEntitiesByKB` and then
    fans it out to a pool of per-file coroutines. Default concurrency
    is `defaultBackfillConcurrency = 4`; the knob is exposed via
    `BackfillEntityAliasesWorkflowParam.Concurrency` and preserved
    across `ContinueAsNew`. At ~30–60 s per Gemini summary call this
    peaks at ~4–8 RPM, two orders of magnitude under every paid-tier
    Gemini quota, so operators can raise it (to 8 or 16) in
    quota-rich deployments. Bounded-parallel writes to `kb_entity`
    are safe because `UpsertEntities` serializes at the Postgres row
    level under ON CONFLICT and `LinkEntityFile` uses disjoint
    `(entity, file)` tuples across coroutines. The workflow MUST
    drain every in-flight coroutine before `ContinueAsNew` (and
    before returning): restarting history without draining would
    orphan running activities and silently lose work.
  - `kb_entity.aliases` is `TEXT[] NOT NULL DEFAULT '{}'` with a GIN
    index (migration 000069). The ON CONFLICT branch of
    `repository.UpsertEntities` union-merges the existing array with
    the new aliases via
    `SELECT ARRAY(SELECT DISTINCT UNNEST(kb_entity.aliases || EXCLUDED.aliases))`
    so re-running the workflow never shrinks the alias set.
  - Per-file failures are logged and skipped; the workflow is
    non-fatal. Temporal's retry policy (MaximumAttempts = 2, trimmed
    from the ingestion path's 3 because backfill is best-effort)
    absorbs transient errors inside the activity boundary.
  - Uses `ContinueAsNew` after `MaxPollIterationsBeforeContinueAsNew`
    paging rounds (at `backfillAliasesBatchSize = 100` files per
    progress-log cadence) so workflow history stays bounded on very
    large KBs. The `Concurrency` param survives the restart.
  - Never scheduled. Nothing in the hot path depends on it — freshly
    ingested files produce aliases inline via `ProcessSummaryActivity`
    → `parseSummaryEntities`, and the surface fix for bilingual
    retrieval degrades gracefully when the backfill has not yet been
    run.

- **ARTIFACT-INV-MEDIA-DURATION-PROBE-FALLBACK.**
  `probeMediaDuration` in
  `pkg/worker/process_file_activity.go` MUST walk an ffprobe
  fallback chain before giving up on a media file's duration:
  `stream=duration` on `v:0` → `stream=duration` on `a:0` →
  `format=duration`. The first positive, finite float wins; `N/A`,
  empty output, and non-positive/NaN/Inf values are filtered by
  `parseDurationLines` and fall through to the next query. A
  single-shot `format=duration` probe is a regression — it returns
  `"N/A"` for fragmented MP4, streaming MP4, certain browser-
  recorded videos, and some remuxer outputs, even when per-stream
  duration boxes are well-formed. This was the root cause of the
  kb-2gNsSWakjs production incident, where 17 files were stuck
  `FILE_PROCESS_STATUS_PROCESSING` (or `FAILED` with the generic
  "interrupted" status_message) because `GetMediaDurationActivity`
  blew up with `strconv.ParseFloat: parsing "N/A"` three retries in
  a row.

  **Error-typing contract.** `probeMediaDuration` returns the sentinel
  `errMediaDurationUnprobable` in either of two content-intrinsic
  shapes, both of which are non-retryable:

  1. Every ffprobe fallback ran cleanly (exit 0) but produced `N/A`
     or empty output (the original production bug — fragmented MP4,
     MediaRecorder output, remuxed streams).
  2. Every fallback failed with an `*exec.ExitError` (ffprobe parsed
     the file but rejected each query with e.g. `Invalid data found…`
     or `stream not found`). Retrying is pointless — the bytes are
     fixed — so surfacing this as retryable would land the file back
     in the original "swallow-and-route-to-single-shot" wedge. The
     Go-level filter is `errors.As(err, &*exec.ExitError)` on every
     failed query; the probe also records the first ~200 bytes of
     stderr per query in the sentinel's wrapped error message so
     operators can tell which diagnostic killed the probe without
     re-running ffprobe by hand.

  Transient failures (ffprobe binary missing from `$PATH`,
  `context.Canceled` / deadline, I/O errors creating the temp file,
  auth failures) MUST propagate as plain wrapped errors so Temporal's
  default retry policy still absorbs them. Context errors surface
  with the prefix `ffprobe canceled:`; other non-exec errors surface
  with `ffprobe invocation failed on every query:`. The activity
  wrapper (`GetMediaDurationActivity`) MUST detect the sentinel via
  `errors.Is(err, errMediaDurationUnprobable)` and convert it to a
  **non-retryable** Temporal `ApplicationError` of type
  `mediaDurationUnprobableErrorType`
  (string: `GetMediaDurationActivity:MEDIA_DURATION_UNPROBABLE`).
  The type string is stable by contract because `ProcessFileWorkflow`
  matches on it via
  `errors.As(&applicationErr) && applicationErr.Type() == …`.

  **Workflow fail-fast contract.** `ProcessFileWorkflow` in
  `pkg/worker/process_file_workflow.go` MUST, on receiving
  `MEDIA_DURATION_UNPROBABLE`, call `handleFileError(fileUID,
  "probe media duration", err)` so the file transitions to
  `FILE_PROCESS_STATUS_FAILED` with a deterministic
  `status_message`, and mark the file in `filesCompleted` so the
  cache-creation, content-, and summary-activity loops append-
  filter it out. Silently `continue`-ing leaves
  `fileDurationSec` / `longMediaFiles` unset for the file, which
  routes a ~500 MB+ video through the non-long-media single-shot
  path — that path wedges `ProcessContentActivity` /
  `ProcessSummaryActivity` on the worker and the only recovery is
  a worker restart.

  **Test contract.** Unit tests in
  `pkg/worker/process_file_activity_test.go` (`TestProbeMediaDuration_*`
  and `TestParseDurationLines`) pin: each of the three fallback paths
  that hit; the all-N/A sentinel path; the all-exit-err sentinel
  path; the mixed exit-err + N/A sentinel path; the
  ffprobe-missing / transient path that stays retryable; the
  `context.Canceled` path that stays retryable; and the non-positive
  / NaN / Inf filtering rules. Tests swap `ffprobeRun` via
  `withFakeFFprobe(t, …)` so the fake doesn't depend on a real
  ffprobe binary being present; that knob is a test-only var and
  production callers MUST NOT reassign it.

  The EE integration gate is
  `artifact-backend-ee/integration-test/standalone-media-duration-probe.js`
  (happy-path MP4 → `FILE_PROCESS_STATUS_COMPLETED` with non-zero
  `File.length.coordinates[0]`; unprobable fixture →
  `FILE_PROCESS_STATUS_FAILED` within 90 s with `status_message`
  prefix `probe media duration:`). See
  `artifact-backend-ee/AGENTS.md` →
  `ARTIFACT-EE-INV-MEDIA-DURATION-REGRESSION-GATE` for the fixture
  regeneration recipe and run instructions.

  If you extend the fallback chain (e.g. adding
  `-select_streams a:0?` or a subtitle-stream fallback), extend
  `fakeFFprobeByQuery` accordingly, add a corresponding
  `TestProbeMediaDuration_*` case, and re-run the EE k6 scenario
  against a live stack to confirm the fixture still fails fast.

- **ARTIFACT-INV-VIDEO-REMUX-FAIL-FAST.**
  `remuxVideoToConvertedFolder` in
  `pkg/worker/process_file_activity.go` MUST walk an ffmpeg fallback
  chain — `fast remux` (`-c copy`) → `audio re-encode`
  (`-c:v copy -c:a aac`) → `full re-encode` (`-c:v libx264 -c:a aac`)
  — and only return `errVideoRemuxUnconvertable` when EVERY attempt
  fails with an `*exec.ExitError`. If ANY attempt fails with a
  non-exit error (ffmpeg missing from `$PATH`, MinIO I/O, context
  cancellation, temp-file errors), the helper MUST return a plain
  wrapped error so Temporal's default retry policy still absorbs it.
  A bare first-attempt failure that short-circuits the chain is a
  regression — it was the second retry-storm shape uncovered while
  validating `ARTIFACT-INV-MEDIA-DURATION-PROBE-FALLBACK`: an
  "unprobable" MP4 can actually survive the ffprobe duration check,
  then wedge `StandardizeFileTypeActivity` on `invalid STSD entries
  0` across three retries (~45 s each with the 5 s / 1.5× / 60 s
  policy), producing a generic "File processing was interrupted or
  terminated before completion" status_message on the file and an
  opaque process_outcome.

  **Error-typing contract.** `StandardizeFileTypeActivity` MUST detect
  `errVideoRemuxUnconvertable` via `errors.Is(err, …)` and convert
  it to a **non-retryable** Temporal `ApplicationError` of type
  `videoRemuxUnconvertableErrorType`
  (string: `StandardizeFileTypeActivity:VIDEO_REMUX_UNCONVERTABLE`).
  The type string is stable by contract because operators and
  alerting pipelines match on it to distinguish intrinsic file
  malformation from transient ffmpeg/O&M failures.

  **Workflow fail-fast contract.** `ProcessFileWorkflow` in
  `pkg/worker/process_file_workflow.go` MUST, on receiving the
  activity error, call `handleFileError(fm.fileUID, "file type
  standardization", err)` AND mark the file in both `filesFailed`
  AND `filesCompleted` BEFORE returning. The `filesCompleted[…] =
  true` marker is non-negotiable: the workflow's deferred cleanup
  (at the top of `ProcessFileWorkflow`) runs on every non-successful
  exit and rewrites any file where `filesCompleted[fileUID]` is
  still `false` with the generic "File processing was interrupted
  or terminated before completion" status_message. That overwrite
  silently clobbered the `VIDEO_REMUX_UNCONVERTABLE` fail-fast
  signal — this was the third regression surface discovered during
  the kb-2gNsSWakjs post-mortem. The same contract applies to the
  `"get file status"` and `"get file metadata"` early-return paths
  and to every `return handleFileError(…)` call that bypasses the
  per-file `filesCompleted[…] = true` pattern used by the
  post-standardization loops.

  **Test contract.** The EE integration gate is
  `artifact-backend-ee/integration-test/standalone-media-duration-probe.js`
  — its fail-fast scenario now accepts EITHER `probe media duration:`
  OR `file type standardization:` as the status_message prefix,
  because `video-sample-unprobable.mp4` actually trips the remux
  branch (ffprobe is tolerant enough to return a positive duration
  from its `tkhd` / `mdhd` fallback before ffmpeg's stream-copy
  rejects the zero-length `stsd`). Both prefixes are valid
  non-retryable fail-fast signals; the invariant the test pins is
  that structurally broken media reaches `FILE_PROCESS_STATUS_FAILED`
  inside the 90 s budget, not that any one component is the first to
  reject it.

- **ARTIFACT-INV-CE-DOCS-SELF-CONTAINED.** Files under `docs/` in this
  repo are CE engineering documentation and MUST NOT reference
  EE-only repos (`artifact-backend-ee`, `agent-backend-ee`,
  `console-ee`, etc.), EE-only surfaces (Cloudflare, the EE API
  gateway, the EE console's global search modal, billing / credit
  flows), or EE-only operational artifacts (runbooks, commander EE
  flags, EE-only integration tests). CE docs should describe CE
  behaviour and link only to CE-internal paths; downstream
  forwarding, FGA filtering, Cloudflare cache rules, and other
  EE-specific concerns belong in `artifact-backend-ee/docs/`. This
  file (`AGENTS.md`) is exempt because it is the cross-repo contract
  layer — CE↔EE worker registration parity, the k6 integration-test
  contract, and the "which tests live where" mapping inherently
  mention EE paths — but prose docs under `docs/` must stay
  self-contained. Regression check:
  `rg -n '(-ee|/ee|enterprise| EE |\bEE\b)' -g '!AGENTS.md' docs/`
  in this repo MUST return no matches (case-insensitive matches on
  common English words like "sheet", "See", "speed", "feedback" are
  fine; only real EE references count). Acceptance artifacts,
  operational follow-ups, and EE-only integration-test naming all
  live in `artifact-backend-ee/docs/` — e.g. the Phase 4 follow-up
  notes moved to
  `artifact-backend-ee/docs/files-card-preview-optimization-followups.md`,
  and the backfill runbook lives at
  `artifact-backend-ee/docs/runbook-backfill-thumbnails.md`.

## Working here

- Always `cd` into this repo before running `go`, `make`, or `git`.
- Do not create files at `/Users/Pinglin/Workspace` — all changes live inside
  this repo.
- Keep `../protogen-go` and `../x` checked out for builds to resolve.
