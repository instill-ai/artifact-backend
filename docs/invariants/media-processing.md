# Media processing — duration probe & video remux

## ffprobe duration fallback chain

**ARTIFACT-INV-MEDIA-DURATION-PROBE-FALLBACK.** `probeMediaDuration` in `pkg/worker/process_file_activity.go` MUST walk an ffprobe fallback chain before giving up on a media file's duration: `stream=duration` on `v:0` → `stream=duration` on `a:0` → `format=duration`. The first positive, finite float wins; `N/A`, empty output, and non-positive/NaN/Inf values are filtered by `parseDurationLines` and fall through to the next query. A single-shot `format=duration` probe is a regression — it returns `"N/A"` for fragmented MP4, streaming MP4, certain browser-recorded videos, and some remuxer outputs, even when per-stream duration boxes are well-formed. This was the root cause of the kb-2gNsSWakjs production incident, where 17 files were stuck `FILE_PROCESS_STATUS_PROCESSING` (or `FAILED` with the generic "interrupted" status_message) because `GetMediaDurationActivity` blew up with `strconv.ParseFloat: parsing "N/A"` three retries in a row.

### Error-typing contract

`probeMediaDuration` returns the sentinel `errMediaDurationUnprobable` in either of two content-intrinsic shapes, both of which are non-retryable:

1. Every ffprobe fallback ran cleanly (exit 0) but produced `N/A` or empty output (the original production bug — fragmented MP4, MediaRecorder output, remuxed streams).
2. Every fallback failed with an `*exec.ExitError` (ffprobe parsed the file but rejected each query with e.g. `Invalid data found…` or `stream not found`). Retrying is pointless — the bytes are fixed — so surfacing this as retryable would land the file back in the original "swallow-and-route-to-single-shot" wedge. The Go-level filter is `errors.As(err, &*exec.ExitError)` on every failed query; the probe also records the first ~200 bytes of stderr per query in the sentinel's wrapped error message so operators can tell which diagnostic killed the probe without re-running ffprobe by hand.

Transient failures (ffprobe binary missing from `$PATH`, `context.Canceled` / deadline, I/O errors creating the temp file, auth failures) MUST propagate as plain wrapped errors so Temporal's default retry policy still absorbs them. Context errors surface with the prefix `ffprobe canceled:`; other non-exec errors surface with `ffprobe invocation failed on every query:`. The activity wrapper (`GetMediaDurationActivity`) MUST detect the sentinel via `errors.Is(err, errMediaDurationUnprobable)` and convert it to a **non-retryable** Temporal `ApplicationError` of type `mediaDurationUnprobableErrorType` (string: `GetMediaDurationActivity:MEDIA_DURATION_UNPROBABLE`). The type string is stable by contract because `ProcessFileWorkflow` matches on it via `errors.As(&applicationErr) && applicationErr.Type() == …`.

### Workflow fail-fast contract

`ProcessFileWorkflow` in `pkg/worker/process_file_workflow.go` MUST, on receiving `MEDIA_DURATION_UNPROBABLE`, call `handleFileError(fileUID, "probe media duration", err)` so the file transitions to `FILE_PROCESS_STATUS_FAILED` with a deterministic `status_message`, and mark the file in `filesCompleted` so the cache-creation, content-, and summary-activity loops append-filter it out. Silently `continue`-ing leaves `fileDurationSec` / `longMediaFiles` unset for the file, which routes a ~500 MB+ video through the non-long-media single-shot path — that path wedges `ProcessContentActivity` / `ProcessSummaryActivity` on the worker and the only recovery is a worker restart.

### Test contract

Unit tests in `pkg/worker/process_file_activity_test.go` (`TestProbeMediaDuration_*` and `TestParseDurationLines`) pin: each of the three fallback paths that hit; the all-N/A sentinel path; the all-exit-err sentinel path; the mixed exit-err + N/A sentinel path; the ffprobe-missing / transient path that stays retryable; the `context.Canceled` path that stays retryable; and the non-positive / NaN / Inf filtering rules. Tests swap `ffprobeRun` via `withFakeFFprobe(t, …)` so the fake doesn't depend on a real ffprobe binary being present; that knob is a test-only var and production callers MUST NOT reassign it.

If you extend the fallback chain (e.g. adding `-select_streams a:0?` or a subtitle-stream fallback), extend `fakeFFprobeByQuery` accordingly and add a corresponding `TestProbeMediaDuration_*` case.

## Video remux fail-fast

**ARTIFACT-INV-VIDEO-REMUX-FAIL-FAST.** `remuxVideoToConvertedFolder` in `pkg/worker/process_file_activity.go` MUST walk an ffmpeg fallback chain — `fast remux` (`-c copy`) → `audio re-encode` (`-c:v copy -c:a aac`) → `full re-encode` (`-c:v libx264 -c:a aac`) — and only return `errVideoRemuxUnconvertable` when EVERY attempt fails with an `*exec.ExitError`. If ANY attempt fails with a non-exit error (ffmpeg missing from `$PATH`, MinIO I/O, context cancellation, temp-file errors), the helper MUST return a plain wrapped error so Temporal's default retry policy still absorbs it. A bare first-attempt failure that short-circuits the chain is a regression — it was the second retry-storm shape uncovered while validating `ARTIFACT-INV-MEDIA-DURATION-PROBE-FALLBACK`: an "unprobable" MP4 can actually survive the ffprobe duration check, then wedge `StandardizeFileTypeActivity` on `invalid STSD entries 0` across three retries (~45 s each with the 5 s / 1.5× / 60 s policy), producing a generic "File processing was interrupted or terminated before completion" status_message on the file and an opaque process_outcome.

### Error-typing contract

`StandardizeFileTypeActivity` MUST detect `errVideoRemuxUnconvertable` via `errors.Is(err, …)` and convert it to a **non-retryable** Temporal `ApplicationError` of type `videoRemuxUnconvertableErrorType` (string: `StandardizeFileTypeActivity:VIDEO_REMUX_UNCONVERTABLE`). The type string is stable by contract because operators and alerting pipelines match on it to distinguish intrinsic file malformation from transient ffmpeg/O&M failures.

### Workflow fail-fast contract

`ProcessFileWorkflow` in `pkg/worker/process_file_workflow.go` MUST, on receiving the activity error, call `handleFileError(fm.fileUID, "file type standardization", err)` AND mark the file in both `filesFailed` AND `filesCompleted` BEFORE returning. The `filesCompleted[…] = true` marker is non-negotiable: the workflow's deferred cleanup (at the top of `ProcessFileWorkflow`) runs on every non-successful exit and rewrites any file where `filesCompleted[fileUID]` is still `false` with the generic "File processing was interrupted or terminated before completion" status_message. That overwrite silently clobbered the `VIDEO_REMUX_UNCONVERTABLE` fail-fast signal — this was the third regression surface discovered during the kb-2gNsSWakjs post-mortem. The same contract applies to the `"get file status"` and `"get file metadata"` early-return paths and to every `return handleFileError(…)` call that bypasses the per-file `filesCompleted[…] = true` pattern used by the post-standardization loops.

### Test contract

Both `MEDIA_DURATION_UNPROBABLE` and `VIDEO_REMUX_UNCONVERTABLE` are valid non-retryable fail-fast signals; the invariant pinned at the file-process layer is that structurally broken media reaches `FILE_PROCESS_STATUS_FAILED` inside a bounded budget (currently 90 s), not that any one component is the first to reject it. ffprobe is tolerant enough to return a positive duration from its `tkhd` / `mdhd` fallback even on bytes that ffmpeg later rejects with a zero-length `stsd`, so the same broken fixture can legitimately surface as either prefix (`probe media duration:` or `file type standardization:`).
