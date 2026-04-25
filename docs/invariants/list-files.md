# `ListFiles` permission filter & view fan-out

## Permission-filter pushdown

**ARTIFACT-INV-LIST-FILES-PERMISSION-FILTER.** `PublicHandler.ListFiles` delegates to `PublicHandler.ListFilesWithPermissionFilter(ctx, req, nil)` — the latter is the actual implementation and the supported extension point for downstream consumers that need per-row authorization in addition to namespace / KB ACL. The supplied `*repository.FilePermissionFilter` is compiled into the same SQL `WHERE` as namespace / KB / AIP-160 filters, which keeps `total_size` / `next_page_token` accurate under filtering — a post-pagination filter cannot achieve this and must not be reintroduced. The filter API is intentionally generic (`TagsOverlap` / `UIDsIn` / `TagsLikeNone` / `VisibilityIn` over opaque strings and UUIDs); the repository / handler have zero semantic understanding of any tag value, which keeps higher-level authorization concepts (collections, FGA, etc.) out of the OSS surface.

## View fan-out

**ARTIFACT-INV-LIST-VIEW-FANOUT.** `PublicHandler.ListFiles` fans out per-row presigns through the shared `resolveDerivedResourceURI` helper (same code path as `GetFile`) via an `errgroup` with **bounded concurrency (16 workers)** whenever `ListFilesRequest.view` is set. `File.derived_resource_uri` is populated only for view-opted calls; unset `view` keeps the legacy metadata-only behaviour so cheap listings stay cheap. Per-row errors log at `warn` via `logx.GetZapLogger` and surface as an empty string rather than failing the whole call. `ListFilesAdminRequest.view` mirrors the public field verbatim so internal callers can opt into the same fan-out path. See `docs/blob-storage-architecture.md` "`ListFiles` fan-out presign".
