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
