# File Type Conversion Pipeline

## Overview

This pipeline converts various file types into standardized formats for consistent processing and storage. It accepts documents, images, audio, and video files and converts them to industry-standard formats.

## Key Features

- **Document Conversion**: Converts various document formats to PDF
- **Image Conversion**: Converts various image formats to PNG
- **Audio Conversion**: Converts various audio formats to OGG
- **Video Conversion**: Converts various video formats to MP4 with Gemini API compatibility
- **Conditional Processing**: Only processes the file types that are provided as input

## Video Conversion: Two-Pass Gemini Compliance

MP4-to-MP4 conversions use a two-pass strategy to guarantee the output file is
accepted by the Gemini API, which rejects files where audio data appears before
video data.

1. **Fast path** — `ffmpeg -c copy -avoid_negative_ts make_zero -movflags +faststart`.
   Stream-copies all tracks (no re-encoding), shifts timestamps non-negative,
   and moves the moov atom to the front. Completes in ~1 min for 500 MB files.
2. **Compliance check** — `ffprobe` inspects the first 20 packets for negative
   video DTS or audio byte offsets preceding video byte offsets.
3. **Slow path (fallback)** — If non-compliant, re-runs with `-c:v copy -c:a aac -b:a 192k`
   to re-encode audio and guarantee correct interleaving.

For non-MP4-to-MP4 conversions (e.g., MKV → MP4), the pipeline uses explicit
stream mapping (`-map 0:v:0 -map 0:a?`) with `+faststart`.

## Long Media Chunking (Upstream Workflow)

After standardization, the `ProcessFileWorkflow` in `artifact-backend` detects media
files that exceed Gemini's processing limits:

| Media type | Limit | Chunk size |
|---|---|---|
| Video (with audio) | ~45 min | 30 min |
| Audio-only | ~9.5 h | 8 h |

When a file exceeds the limit, the workflow:

1. **Probes duration** with `ffprobe` (`GetMediaDurationActivity`).
2. **Splits** the file into overlapping physical chunks via `ffmpeg -c copy`
   (`SplitMediaChunksActivity`). Adjacent chunks share a 30-second overlap to
   avoid boundary content loss.
3. **Processes each chunk** independently through the existing cache → batch
   pipeline. Timestamps in each chunk are relative to chunk start.
4. **Offsets timestamps** in each chunk's transcript back to absolute time
   using `offsetTimestamps`.
5. **Assembles** all chunk transcripts into a single document
   (`SaveAssembledContentActivity`).
6. **Generates a summary** from the assembled transcript text (not from the
   raw media file), avoiding the Gemini length limit entirely.

Short media files (within the limit) continue through the existing single-shot
or batch path with no changes.

## How to Use

Upload one or more files of different types (document, image, audio, or video). The pipeline will automatically detect the file type and convert it to the appropriate standardized format.

## Input Variables

- `document`: Document file to convert (optional)
- `image`: Image file to convert (optional)
- `audio`: Audio file to convert (optional)
- `video`: Video file to convert (optional)

## Output Fields

- `document`: Document file converted to PDF format
- `image`: Image file converted to PNG format
- `audio`: Audio file converted to OGG format
- `video`: Video file converted to MP4 format

## Use Cases

This pipeline is ideal for:

- Normalizing file formats across a system
- Ensuring consistent file types for downstream processing
- Preparing files for storage or archival
- Converting files before indexing in a knowledge base
