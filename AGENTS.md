# artifact-backend

Community artifact backend for file ingestion, KB/catalog CRUD, conversion, chunking, embeddings, search, thumbnails, and Temporal workers.

## Stack

Go 1.25.6, gRPC/grpc-gateway, PostgreSQL/GORM, Milvus, MinIO/GCS, Redis, Temporal, OpenFGA.

## Common Commands

- `make unit-test or targeted go test ./pkg/...`
- `Regenerate mocks with make go-gen after interface changes`

## Working Rules

- CE is self-contained OSS. Do not reference specific downstream / operator-overlay repos, tooling, or file paths from source, code comments, docs, invariants, or this `AGENTS.md`. When operator-only concerns must be named, use edition-neutral language ("operator tooling", "admin CLI", "downstream consumers").
- Shared workflow / backfill code may live in CE, but operator-admin-only Temporal registrations (`w.RegisterWorkflow` / `w.RegisterActivity`) MUST NOT appear in the CE worker binary. CE ships an OSS stack with no admin path to trigger such workflows; leaving a CE registration advertises workflow types the OSS surface cannot invoke.
- Invariants are split per-domain under `docs/invariants/`; read the relevant section before touching that subsystem.

## Invariants Index

`docs/invariants/README.md` indexes the per-domain `ARTIFACT-INV-*` invariants. Read only what you need:

- `list-files.md` — `ListFiles` permission filter pushdown and view fan-out.
- `thumbnails.md` — non-blocking generation, URI resolution, backfill workflow.
- `entity-aliases.md` — `BackfillEntityAliasesWorkflow` (CE code, operator-only registration).
- `media-processing.md` — ffprobe duration fallback chain and ffmpeg remux fail-fast.
- `storage-streaming.md` — GCS resume loop, MinIO range offset-zero footgun, MinIO keep-alive TODO.
