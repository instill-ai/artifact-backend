# artifact-backend invariants

On-demand reference for `ARTIFACT-INV-*` invariants. Read only the section relevant to the subsystem you are touching — these files are intentionally not loaded as always-applied Cursor rules.

## Domain files

| File | Invariants |
| --- | --- |
| [`list-files.md`](./list-files.md) | `ARTIFACT-INV-LIST-FILES-PERMISSION-FILTER`, `ARTIFACT-INV-LIST-VIEW-FANOUT` |
| [`thumbnails.md`](./thumbnails.md) | `ARTIFACT-INV-THUMBNAIL-NON-BLOCKING`, `ARTIFACT-INV-THUMBNAIL-URI-RESOLUTION`, `ARTIFACT-INV-THUMBNAIL-BACKFILL` |
| [`entity-aliases.md`](./entity-aliases.md) | `ARTIFACT-INV-ALIAS-BACKFILL` |
| [`media-processing.md`](./media-processing.md) | `ARTIFACT-INV-MEDIA-DURATION-PROBE-FALLBACK`, `ARTIFACT-INV-VIDEO-REMUX-FAIL-FAST` |
| [`storage-streaming.md`](./storage-streaming.md) | `ARTIFACT-INV-GCS-STREAM-RESUME`, `ARTIFACT-INV-MINIO-RANGE-OFFSET-ZERO`, `ARTIFACT-INV-MINIO-CLIENT-KEEPALIVE(TODO)` |
| [`reprocess-status.md`](./reprocess-status.md) | `ARTIFACT-INV-reprocess-status-flicker-elimination` |

## Cross-cutting rules

- CE is self-contained OSS. Source, docs, invariants, and rule files MUST NOT name specific downstream / operator-overlay repos, tooling, or file paths. Use edition-neutral language ("operator tooling", "downstream consumers", "the operator overlay's worker binary") when operator-only concerns must be referenced.
- Shared workflow / backfill code may live in CE, but operator-admin-only Temporal registrations MUST NOT appear in the CE worker binary — CE has no OSS path to invoke them.
