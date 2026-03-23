# RAG Indexing Pipeline

This document describes the end-to-end RAG (Retrieval-Augmented Generation) indexing pipeline implemented by `ProcessFileWorkflow` in artifact-backend. The pipeline transforms uploaded files into searchable, embeddable chunks stored in Milvus.

## Overview

```mermaid
flowchart TD
    Upload["File Upload (MinIO)"] --> Meta["Metadata Fetch"]
    Meta --> Std["Standardize File Type"]
    Std --> Probe{"Media file?"}
    Probe -->|Yes| Duration["Probe Duration (ffprobe)"]
    Probe -->|No| Cache["Cache File Context"]
    Duration --> AudioOnly{"Audio-only file?"}
    AudioOnly -->|Yes| GCSAudio["Upload to GCS → ConvertAudioDirect"]
    AudioOnly -->|No| HybridSplit["Extract Audio + Split Video"]
    HybridSplit --> AudioBranch["Audio: GCS → ConvertAudioDirect"]
    HybridSplit --> VisualBranch["Visual: Cache Chunks → Visual-Only Batches"]
    AudioBranch --> Merge["Merge Audio + Visual by Timestamp"]
    VisualBranch --> Merge
    Cache --> Convert["AI Content Conversion"]
    GCSAudio --> Assemble["Assemble Content"]
    Convert --> Assemble
    Merge --> Assemble
    Assemble --> Summary["Generate Summary"]
    Summary --> Chunk["Text Chunking"]
    Chunk --> Embed["Embed & Save to Milvus"]
```

## Components

| Component | Role |
|-----------|------|
| **Temporal** | Orchestrates the workflow as activities with retries and timeouts |
| **MinIO** | Stores original files, standardized files, converted content, and temp chunks |
| **PostgreSQL** | Tracks file metadata, processing status, converted files, and chunk records |
| **Gemini** | AI model (`gemini-3.1-pro-preview`) for multimodal content extraction, speaker identification, and summary generation |
| **Vertex AI Cache** | Caches uploaded files in Gemini context for efficient multi-batch access |
| **Milvus** | Vector database storing embeddings for semantic search |
| **ffmpeg / ffprobe** | Media duration probing, physical splitting, and lossless audio extraction |
| **GCS** | Temporary storage for audio files passed to Gemini via `FileData` (gs:// URI) for direct audio transcription |

## Pipeline Phases

### Phase 1: Metadata & Status Check

```mermaid
flowchart LR
    A["GetFileStatusActivity"] --> B["GetFileMetadataActivity"]
    B --> C{"Status?"}
    C -->|NOTSTARTED / PROCESSING| D["Full processing"]
    C -->|CHUNKING| E["Resume from chunking"]
    C -->|EMBEDDING| F["Resume from embedding"]
    C -->|COMPLETED / FAILED| D
```

The workflow fetches each file's current status and metadata (file type, storage path, KB model family). Files that previously completed or failed are reprocessed from scratch, unless the `x-instill-patch` flag is set on a completed file — in that case, the patch-only fast path is used (see [Patch-Only Reprocessing Optimization](#patch-only-reprocessing-optimization)). Files interrupted at chunking or embedding resume from their last checkpoint.

### Phase 2: File Type Standardization

**Activity:** `StandardizeFileTypeActivity`

Converts files to a canonical format via the `indexing-convert-file-type` pipeline:

| Input Type | Output Format |
|---|---|
| Documents (DOCX, PPTX, HTML, CSV, etc.) | PDF |
| Images (JPG, TIFF, BMP, etc.) | PNG |
| Audio (MP3, WAV, AAC, M4A, etc.) | OGG |
| Video (AVI, MOV, MKV, FLV, etc.) | MP4 |

The standardized file is always re-generated to ensure it reflects the latest conversion logic.

#### PDF Text-Based Classification

**Activity:** `ClassifyPDFTextActivity` (runs after standardization for document types)

After a document is standardized to PDF, the pipeline classifies it as **text-based** or **image-based**. This classification drives downstream visual grounding behavior:

- **Text-based PDFs** have a selectable text layer (e.g., natively authored PDFs, DOCX/PPTX conversions). Citations for these files carry a `highlight_text` field and the frontend uses PDF.js text-layer highlighting for precise text selection.
- **Image-based PDFs** are scanned documents without a text layer (e.g., scanned receipts, photographed pages). Citations for these files use bounding box polygon overlays (`UNIT_PIXEL` coordinates).

**How it works:**

1. The standardized PDF is read using `github.com/ledongthuc/pdf`.
2. Text is extracted from the first few pages using `pdf.GetPlainText()`.
3. If extracted text exceeds a character threshold, the file is classified as text-based; otherwise image-based.
4. The result is stored in the `is_text_based` boolean column on the `file` table (DB migration `000064`).
5. The `is_text_based` field is exposed via the `File` protobuf message and returned in API responses.

**Key files:**

| File | Purpose |
|---|---|
| `pkg/worker/pdf_text_detect.go` | `IsTextBasedPDF()` — text extraction and classification logic |
| `pkg/worker/process_file_activity.go` | Calls classification after standardization |
| `pkg/db/migration/000064_add_is_text_based_to_file.up.sql` | Adds `is_text_based` column |
| `pkg/repository/file.go` | Reads/writes `is_text_based` from DB |
| `pkg/handler/file.go` | Exposes `is_text_based` in API responses |

### Phase 3: Duration Probing & Routing (Media Only)

**Activity:** `GetMediaDurationActivity`

For media files, `ffprobe` extracts the exact duration before any AI processing. All media files are routed to the dedicated media processing path for maximum transcript accuracy:

| Media Type | Routing |
|---|---|
| **Audio-only** (MP3, WAV, AAC, etc.) | Routed to `processAudioOnlyLongMedia` — uploads to GCS and uses `ConvertAudioDirect` with `AudioTimestamp: true` for maximum accuracy |
| **Video** (any duration) | Routed to hybrid two-pass processing — audio is extracted and transcribed separately, video is processed visual-only, results merged by timestamp |

### Phase 4: AI Content Conversion

All file types (documents, images, audio, video) use `gemini-3.1-pro-preview` for content extraction and summary generation.

#### Short Path (Documents & Images)

```mermaid
flowchart TD
    CF["CacheFileContextActivity"] --> Single{"Small enough?"}
    Single -->|Yes| Direct["ProcessContentActivity (single-shot)"]
    Single -->|No| Batch["ConvertBatchActivity (parallel batches)"]
    Batch --> Save["SaveAssembledContentActivity"]
    Direct --> Done["Content ready"]
    Save --> Done
```

1. **Cache creation** — Uploads the standardized file to Gemini's context cache.
2. **Single-shot attempt** — `ProcessContentActivity` tries to convert the entire file in one call.
3. **Batch fallback** — If the file is too large (many pages), it falls back to `ConvertBatchActivity` which processes page ranges in parallel, controlled by `BatchProfile`:

| File Type | Segment Size | Max Concurrent Batches |
|---|---|---|
| Documents | 10 pages | 16 |

1. **Assembly** — `SaveAssembledContentActivity` concatenates batch results, merges cross-boundary HTML tables, and uploads the final markdown.

#### Audio-Only Path (Direct GCS Transcription)

**Function:** `processAudioOnlyLongMedia`

All audio-only files (MP3, WAV, AAC, OGG, FLAC, M4A, WMA, AIFF, WEBM_AUDIO) are routed through this path regardless of duration. It leverages Gemini's native `AudioTimestamp: true` capability for high-accuracy timestamped transcription in a single API call.

```mermaid
flowchart TD
    Upload["1. UploadToGCSActivity<br/>(MinIO → GCS)"]
    Upload --> Transcribe["2. TranscribeAudioActivity<br/>(ConvertAudioDirect with speech-only prompt)"]
    Transcribe --> Save["3. SaveAssembledContentActivity"]
    Save --> Summary["4. ProcessSummaryActivity"]
    Summary --> Cleanup["5. Cleanup GCS + MinIO temp files"]
```

The speech-only prompt instructs the model to produce only `[Audio: HH:MM:SS]` and `[Sound: HH:MM:SS]` tags with speaker labeling. No visual content is generated.

#### Video Path (Hybrid Two-Pass)

All video files use a **hybrid two-pass architecture** that separates audio and visual processing for maximum transcript accuracy. Audio is extracted and transcribed independently via Gemini's `AudioTimestamp: true`, while video is processed visual-only. This avoids the timing drift and content corruption that occurs when audio and video are transcribed together in a multimodal call.

```mermaid
flowchart TD
    Split["1. SplitMediaChunksActivity<br/>(ffmpeg -c copy, 30s overlap)"]
    Extract["1b. ExtractAudioActivity<br/>(ffmpeg lossless audio extraction)"]
    Split --> CacheAll["2a. CacheFileContextActivity × N chunks<br/>(all concurrent)"]
    Extract --> UploadGCS["2b. UploadToGCSActivity<br/>(audio → GCS)"]
    CacheAll --> Speakers["2c. IdentifySpeakersActivity<br/>(from chunk 0 cache)"]
    UploadGCS --> AudioTranscribe["2d. TranscribeAudioActivity<br/>(ConvertAudioDirect, concurrent with visual)"]
    Speakers --> VisualBatches["2e. ConvertBatchActivity × all segments<br/>(visual-only mode, concurrent)"]
    AudioTranscribe --> Merge["3. mergeAudioAndVisual<br/>(interleave by timestamp)"]
    VisualBatches --> Assemble["3a. SaveAssembledContentActivity<br/>(offset timestamps + trim overlap)"]
    Assemble --> Merge
    Merge --> Summary["4. ProcessSummaryActivity<br/>(from merged transcript text)"]
    Summary --> Cleanup["5. Cleanup GCS + MinIO temp files"]
```

**Audio branch (concurrent):**

1. `ExtractAudioActivity` uses FFmpeg to losslessly extract the audio track. If no audio stream exists, the workflow falls back to single-pass video processing with the original prompt.
2. `UploadToGCSActivity` moves the audio from MinIO to GCS.
3. `TranscribeAudioActivity` calls `ConvertAudioDirect` with a speech-only prompt, producing `[Audio:]` and `[Sound:]` entries with `HH:MM:SS` timestamps.

**Visual branch (concurrent):**

1. Physical chunks are cached and batched as before, but with `VisualOnly: true` on each `ConvertBatchActivity`. This uses a visual-only prompt that produces only `[Video:]` entries with rich visual descriptions (`[Location:]`, `[Image:]`, `[Chart:]`, `[Diagram:]`, `[Icon:]`, `[Logo:]` sub-tags). No `[Audio:]` or `[Sound:]` tags are generated.
2. `SaveAssembledContentActivity` handles timestamp offsetting and overlap trimming as before.

**Merge:**
`mergeAudioAndVisual` interleaves the disjoint audio and visual entries by `HH:MM:SS` timestamp. A stable sort preserves ordering for entries at the same timestamp. As defense-in-depth, any accidental `[Audio:]` or `[Sound:]` tags in the visual output are stripped before merging.

**Speaker identification:** `IdentifySpeakersActivity` queries chunk 0's cache with a dedicated prompt to extract speaker names (e.g., "Alice Smith, Bob Jones"). The result is formatted as a `[SPEAKER CONTEXT]` string injected into both the audio transcription prompt and the visual batch prompts.

**Assembly with timestamp offsetting and overlap trimming:**

`SaveAssembledContentActivity` receives a flat list of temp MinIO paths along with a `ChunkOffsets` array describing each chunk's batch count, absolute start offset, and overlap duration. During assembly:

1. **Timestamp offsetting** — `offsetTimestamps` shifts all inline timestamp tags (`[Video: HH:MM:SS]`, `[Video: HH:MM:SS - HH:MM:SS]`) from chunk-relative to absolute time using each chunk's `Offset`.
2. **Overlap trimming** — For non-first chunks, `clipToTimeRange` removes transcript lines whose absolute timestamps fall within the overlap region already covered by the previous chunk.
3. **Concatenation** — The trimmed, offset-adjusted batch results are concatenated into the final visual transcript.

**Summary from transcript text:** The summary is generated from the merged markdown text via `ContentMarkdown`, bypassing the original media file entirely. The `fileType` is overridden to `TYPE_MARKDOWN` so the AI model treats it as text input.

### Phase 5: Summary Generation

**Activity:** `ProcessSummaryActivity`

Generates a concise summary of the content. For short files with cached context, the summary reads from the Gemini cache. For long media files, the summary is generated from the assembled markdown transcript (passed as `ContentMarkdown`), with `fileType` overridden to `TYPE_MARKDOWN`.

#### Phase 4b / 5b: Patch Merge (Optional)

After `ProcessContentActivity` produces the initial converted content, and after `ProcessSummaryActivity` produces the initial summary, a **patch merge** step is applied if a patch file exists for the file.

**How patches are stored:**

When a user submits patches via the `update-file-content` MCP tool (or the `UpdateFile` gRPC API with `update_mask=content`), the patch instructions are:

1. Written to `kb-{kbUID}/file-{fileUID}/converted-file/patch.md` in MinIO.
2. Flagged in the file's `ExternalMetadata` as `x-instill-patch: "true"`.

**How patches are applied during processing:**

```mermaid
flowchart TD
    ContentGen["ProcessContentActivity generates contentInMarkdown"] --> WindowMerge["patchWindowMerge"]
    WindowMerge --> ParseSegs["parseContentSegments: split by [Page: N] markers"]
    ParseSegs --> SmallDoc{"≤ 15 segments?"}
    SmallDoc -->|yes| FullMerge["mergeWithLLM: send full content"]
    SmallDoc -->|no| LocateTarget["locateTargetSegments"]
    LocateTarget --> ExplicitPage{"explicit 'page N'\nin patch?"}
    ExplicitPage -->|yes| PageWindow["build window: target pages ± 3 buffer"]
    ExplicitPage -->|no| KeywordScore["keyword-score all segments → top match"]
    KeywordScore --> NoMatch{"no match?"}
    NoMatch -->|yes| FullMerge
    NoMatch -->|no| PageWindow
    PageWindow --> WindowLLM["mergeWithLLM: send window only (~5–7 pages)"]
    FullMerge --> Splice["splice merged window back into full document"]
    WindowLLM --> Splice
    Splice --> SaveContent["Save patched content to MinIO"]

    SummaryGen["ProcessSummaryActivity generates summary"] --> CheckPatch2["readPatchIfPresent: read patch.md from MinIO"]
    CheckPatch2 -->|patch.md exists| MergeSummary["mergeWithLLM: LLM applies patch to summary\n(summaries are short — no windowing needed)"]
    CheckPatch2 -->|not found| SkipSummary["Use raw summary as-is"]
    MergeSummary --> SaveSummary["Proceed with patched summary"]
    SkipSummary --> SaveSummary
```

**Windowed merge — token efficiency:**

For documents with more than 5 `[Page: N]` segments the merge does not send the entire document to the LLM. Instead, `patchWindowMerge` (`pkg/worker/content_window.go`) uses a two-stage strategy to narrow the edit scope:

| Strategy | Trigger | How it works |
|---|---|---|
| **Explicit page reference** | Patch text contains "page N" / "pages N-M" | Extract the referenced page indices; build a window of those pages ± 2 buffer pages |
| **Keyword scoring** | No explicit page reference | Tokenize significant words from the patch; score every segment by keyword hit count; use the highest-scoring segment ± 2 buffer pages |
| **Full-document fallback** | No target located (keyword score = 0) | Log a warning and send the full content (same as the original behaviour) |

For a 160-page document (≈180,000 tokens), a patch such as "In page 79, 'Jonhn' should be 'John'" sends only pages 77–81 (≈3,000 tokens) to the LLM — a **~60× reduction** in input and output tokens.

Summary patch merge always uses full-document merge because summaries are already short.

**Merge prompt templates:**

The merge prompts are embedded at compile time from:

- `pkg/ai/gemini/prompt/patch_merge_content.md` — content merge instructions
- `pkg/ai/gemini/prompt/patch_merge_summary.md` — summary merge instructions

Both are loaded once at startup via the same `//go:embed prompt/*.md` pattern used by all other AI prompts. They can be updated without changing Go code by editing the `.md` files and rebuilding.

If the LLM returns an empty response, the original AI-generated content is used as a fallback with no data loss.

**Key lifecycle properties:**

- `patch.md` is **not** tracked in the `converted_file` DB table; `DeleteOldConvertedFilesActivity` only deletes DB-tracked records, so patch files are **naturally preserved** across every reprocess.
- The patch is applied on **every reprocess** — it survives model upgrades, system prompt changes, and manual retriggers.
- To remove a patch permanently, call `UpdateFile` with `update_mask=content` and `content=""`. This deletes `patch.md` from MinIO and removes the `x-instill-patch` flag.

**MinIO path for patches:**

```
{bucket}/kb-{kbUID}/file-{fileUID}/converted-file/patch.md
```

### Phase 6: Text Chunking

**Activity:** `ChunkContentActivity`

Splits the converted markdown into overlapping text chunks for embedding. Chunk boundaries respect document structure (headings, paragraphs) where possible.

### Phase 7: Embedding & Vector Storage

**Activity:** `EmbedAndSaveChunksActivity`

Each text chunk is embedded using the KB's configured embedding model (`gemini-embedding-001` by default, 3072 dimensions), and the resulting vectors are upserted into the Milvus collection for semantic search.

## Error Handling & Resilience

- **Per-file isolation** — Each file in a batch workflow is processed independently. A single file's failure doesn't block others.
- **Retry policies** — All activities have configurable retry policies with exponential backoff. Transient errors (rate limits, deadlines) are retried automatically.
- **Cache expiration recovery** — If a Gemini cache expires mid-processing, the workflow detects `isCacheExpired` and re-creates the cache before retrying.
- **Disconnected cleanup** — Cache deletion and temp file cleanup use `workflow.NewDisconnectedContext` to ensure cleanup runs even if the parent context is cancelled.
- **Adaptive chunking** — Document batch conversion uses adaptive splitting: if page tags are missing from the AI response, it recursively bisects the range and retries.
- **Graceful speaker identification** — If `IdentifySpeakersActivity` fails or returns "UNKNOWN", the workflow proceeds without speaker context. This is non-fatal; the model will use its own labeling.
- **Long media summary isolation** — Long media files handle summary generation inside `processLongMedia`, skipping the standard summary future path in the main workflow to avoid nil-future panics.
- **Hybrid fallback** — If audio extraction finds no audio stream, or if GCS upload fails, the workflow falls back to single-pass video processing with the original (non-visual-only) prompt.
- **Audio transcript failure tolerance** — If the audio transcription branch fails but visual batches succeed, the workflow uses the visual-only content without merging. This ensures partial results are still usable.
- **GCS cleanup** — Extracted audio files uploaded to GCS are cleaned up via `DeleteFromGCSActivity` in a disconnected context, ensuring cleanup runs even on workflow cancellation.

## Patch-Only Reprocessing Optimization

When a file is reprocessed solely because a user submitted a patch (via `update-file-content`), the workflow detects this and takes a **fast path** that skips all content generation activities. This avoids re-running expensive operations like audio transcription or file type standardization when only a small textual correction needs to be merged.

### Detection

A file qualifies for patch-only mode when all of these are true:

1. The file's `ExternalMetadata` contains `x-instill-patch: "true"` (set by `UpdateFile` when a patch is stored)
2. The file's previous `ProcessStatus` was `COMPLETED` (it was fully processed before)

If either condition fails, the workflow falls back to the full pipeline.

### Fast-Path Sequence

```
ReadExistingContentActivity → ApplyPatchToContentActivity → DeleteOldConvertedFiles
→ SavePatchedContent (Create/Upload/UpdateDest) → ProcessSummaryActivity
→ ChunkContent → EmbedAndSaveChunks → COMPLETED
```

**Skipped activities:** `StandardizeFileTypeActivity`, media probing, Gemini cache creation, `ProcessContentActivity` (transcription/OCR).

### Graceful Fallback

If `ReadExistingContentActivity` fails (e.g., the content record was deleted or MinIO is unavailable), the file is automatically reclassified as needing full processing and joins the standard pipeline.

## Key Constants

| Constant | Value | Purpose |
|---|---|---|
| `DefaultModel` | `gemini-3.1-pro-preview` | AI model for all content extraction and summary generation |
| `DefaultEmbeddingModel` | `gemini-embedding-001` | Embedding model (3072 dimensions) |
| `MaxVideoChunkDuration` | 30 min | Physical split duration for video chunks (all video files use hybrid two-pass) |
| `ChunkOverlap` | 30 sec | Overlap between adjacent physical chunks for boundary continuity |
| `DefaultCacheTTL` | 2 hours | Gemini context cache time-to-live |
| `RateLimitCooldown` | 60 sec | Pause before batch rounds to let API quota recover |
