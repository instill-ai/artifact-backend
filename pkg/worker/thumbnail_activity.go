// Package worker — GenerateThumbnailActivity.
//
// This activity is the non-blocking sibling of StandardizeFileTypeActivity.
// It produces a tiny, Cloudflare-cacheable preview asset that the Files-page
// card grid can load through `File.thumbnail_uri` without any per-row presign
// fan-out on the backend.
//
// Contract (see `artifact-backend/AGENTS.md` →
// `ARTIFACT-INV-THUMBNAIL-NON-BLOCKING` and the
// `files-card-preview-optimization` plan, Phase 3):
//
//   - Runs AFTER StandardizeFileTypeActivity so it always operates on the
//     AI-native (PNG/MP4/OGG/PDF) copy that the rest of the pipeline writes
//     to the converted-file folder. This guarantees exactly one thumbnail per
//     original file regardless of the input format.
//   - Produces a 1024 px-wide WebP (quality 80) with aspect ratio preserved.
//   - Persists the bytes through `SaveConvertedFile(..., "webp")` and creates
//     a matching `converted_file` row with
//     `ConvertedFileType_CONVERTED_FILE_TYPE_THUMBNAIL`.
//   - Is idempotent at the activity boundary — if a row with the same
//     (file_uid, CONVERTED_FILE_TYPE_THUMBNAIL) already exists (eg. a retry
//     after the MinIO write succeeded), the activity returns success without
//     re-uploading.
//   - Must never fail the parent workflow: callers invoke it with
//     `MaximumAttempts = 3` and treat any final error as non-fatal.
package worker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

// Thumbnail output parameters. Kept in one place so integration tests,
// backfill workflows, and the frontend renderer stay in sync.
const (
	// ThumbnailMaxDimensionPx is the longest edge of the generated thumbnail.
	// 1024 px matches the largest card size used in `FilesListView` on
	// 4K monitors while keeping the WebP payload well under 200 KB for
	// typical photographic content.
	ThumbnailMaxDimensionPx = 1024

	// ThumbnailWebPQuality is the libwebp quality factor. 80 is the widely
	// used sweet spot between size and perceptual quality.
	ThumbnailWebPQuality = 80

	// ThumbnailFileExt is the file extension used when persisting to MinIO.
	// The value must stay in sync with the case arm in
	// `minioStorage.SaveConvertedFile`.
	ThumbnailFileExt = "webp"

	generateThumbnailActivityError = "GenerateThumbnailActivity"
)

// GenerateThumbnailActivityParam captures everything the activity needs to
// resolve the source bytes in MinIO and to persist the thumbnail. We take
// the *effective* (post-standardization) bucket and destination so that the
// input is always an AI-native format — never a DOCX or GIF.
type GenerateThumbnailActivityParam struct {
	FileUID         types.FileUIDType
	KBUID           types.KBUIDType
	Bucket          string               // MinIO bucket containing the standardized file.
	Destination     string               // MinIO path of the standardized file.
	FileType        artifactpb.File_Type // Effective (post-standardization) file type.
	FileDisplayName string               // Used only for logging.
	Metadata        *structpb.Struct     // Passed through for AuthN when needed.
}

// GenerateThumbnailActivityResult is intentionally slim — callers only need
// the object UID to populate `File.thumbnail_uri`. The destination is kept
// for observability and for the backfill workflow integration test.
type GenerateThumbnailActivityResult struct {
	ConvertedFileUID     types.ConvertedFileUIDType
	ThumbnailDestination string
	Skipped              bool   // True when no thumbnail is produced for this file type.
	SkippedReason        string // Populated when Skipped=true.
}

// GenerateThumbnailActivity creates a 1024 px WebP preview for the supplied
// file. See the package-level comment for the full contract.
//
// The activity is designed to be safe under Temporal retries:
//
//  1. Before any work, we check whether a `converted_file` row of type
//     `CONVERTED_FILE_TYPE_THUMBNAIL` already exists for (file_uid) and
//     short-circuit if so.
//  2. Only after a successful MinIO write do we create the DB row, mirroring
//     the ordering used by the content/summary pipeline.
//
// Errors are wrapped with `activityErrorWithCauseFlat` so that Temporal
// surfaces them under the `GenerateThumbnailActivity` marker.
func (w *Worker) GenerateThumbnailActivity(ctx context.Context, param *GenerateThumbnailActivityParam) (*GenerateThumbnailActivityResult, error) {
	logger := w.log.With(
		zap.String("fileUID", param.FileUID.String()),
		zap.String("kbUID", param.KBUID.String()),
		zap.String("fileType", param.FileType.String()),
		zap.String("fileDisplayName", param.FileDisplayName))
	logger.Info("GenerateThumbnailActivity: starting")

	// Decide up front whether this file type produces a thumbnail. Keeping
	// the list narrow (image + video) avoids dragging in poppler/LibreOffice
	// just for a preview and lets the frontend fall back to the mime icon.
	kind := classifyThumbnailSource(param.FileType)
	if kind == thumbnailSourceUnsupported {
		logger.Info("GenerateThumbnailActivity: skipping — file type not supported")
		return &GenerateThumbnailActivityResult{
			Skipped:       true,
			SkippedReason: fmt.Sprintf("file type %s does not produce a thumbnail", param.FileType.String()),
		}, nil
	}

	// Idempotency guard. If a previous attempt already created the row, reuse
	// it — the MinIO blob is addressed by UID so it is guaranteed to be the
	// same physical object.
	if existing, ok := w.findExistingThumbnail(ctx, param.FileUID); ok {
		logger.Info("GenerateThumbnailActivity: reusing existing thumbnail",
			zap.String("convertedFileUID", existing.UID.String()))
		return &GenerateThumbnailActivityResult{
			ConvertedFileUID:     existing.UID,
			ThumbnailDestination: existing.StoragePath,
		}, nil
	}

	sourceBytes, err := w.repository.GetMinIOStorage().GetFile(ctx, param.Bucket, param.Destination)
	if err != nil {
		return nil, activityErrorWithCauseFlat(
			fmt.Sprintf("failed to download source file: %s", errorsx.MessageOrErr(err)),
			generateThumbnailActivityError,
			err,
		)
	}

	thumbBytes, err := renderThumbnailWebP(ctx, kind, sourceBytes)
	if err != nil {
		return nil, activityErrorWithCauseFlat(
			fmt.Sprintf("failed to render thumbnail: %s", errorsx.MessageOrErr(err)),
			generateThumbnailActivityError,
			err,
		)
	}

	convertedFileUID := types.ConvertedFileUIDType(uuid.Must(uuid.NewV4()))

	destination, err := w.repository.GetMinIOStorage().SaveConvertedFile(
		ctx,
		param.KBUID,
		param.FileUID,
		convertedFileUID,
		ThumbnailFileExt,
		thumbBytes,
	)
	if err != nil {
		return nil, activityErrorWithCauseFlat(
			fmt.Sprintf("failed to upload thumbnail to MinIO: %s", errorsx.MessageOrErr(err)),
			generateThumbnailActivityError,
			err,
		)
	}

	row := repository.ConvertedFileModel{
		UID:              convertedFileUID,
		KnowledgeBaseUID: param.KBUID,
		FileUID:          param.FileUID,
		ContentType:      "image/webp",
		ConvertedType:    artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_THUMBNAIL.String(),
		StoragePath:      destination,
	}
	if _, err := w.repository.CreateConvertedFileWithDestination(ctx, row); err != nil {
		// Best-effort compensate the MinIO write so we do not leak orphan
		// objects. The deletion itself is best-effort too — if it fails we
		// still return the DB error because that is the authoritative
		// failure the caller must see.
		if delErr := w.repository.GetMinIOStorage().DeleteFile(ctx, param.Bucket, destination); delErr != nil {
			logger.Warn("GenerateThumbnailActivity: failed to compensate MinIO write after DB failure",
				zap.String("destination", destination),
				zap.Error(delErr))
		}
		return nil, activityErrorWithCauseFlat(
			fmt.Sprintf("failed to create thumbnail DB record: %s", errorsx.MessageOrErr(err)),
			generateThumbnailActivityError,
			err,
		)
	}

	logger.Info("GenerateThumbnailActivity: thumbnail persisted",
		zap.String("convertedFileUID", convertedFileUID.String()),
		zap.String("destination", destination),
		zap.Int("bytes", len(thumbBytes)))

	return &GenerateThumbnailActivityResult{
		ConvertedFileUID:     convertedFileUID,
		ThumbnailDestination: destination,
	}, nil
}

// findExistingThumbnail returns the existing
// CONVERTED_FILE_TYPE_THUMBNAIL row for `fileUID`, if any. Using the typed
// lookup (rather than scanning all converted rows) keeps the idempotency
// guard O(1) at the DB layer and mirrors how the handler already resolves
// content/summary rows.
func (w *Worker) findExistingThumbnail(ctx context.Context, fileUID types.FileUIDType) (*repository.ConvertedFileModel, bool) {
	row, err := w.repository.GetConvertedFileByFileUIDAndType(
		ctx,
		fileUID,
		artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_THUMBNAIL,
	)
	if err != nil {
		// A miss is the happy path (no existing row) and shows up as a gorm
		// "record not found" error — we do not need to surface it.
		return nil, false
	}
	if row == nil || row.StoragePath == "" {
		return nil, false
	}
	return row, true
}

// thumbnailKind enumerates the render strategies implemented below.
// Unsupported types short-circuit the activity rather than producing a
// placeholder, which keeps the frontend fallback chain
// (thumbnail_uri → derived_resource_uri → mime icon) meaningful.
type thumbnailKind int

const (
	thumbnailSourceUnsupported thumbnailKind = iota
	thumbnailSourceImage
	thumbnailSourceVideo
)

// classifyThumbnailSource maps a post-standardization File_Type to the render
// strategy we should use. Callers treat `thumbnailSourceUnsupported` as a
// hard skip.
func classifyThumbnailSource(ft artifactpb.File_Type) thumbnailKind {
	switch ft {
	case artifactpb.File_TYPE_PNG,
		artifactpb.File_TYPE_JPEG,
		artifactpb.File_TYPE_GIF,
		artifactpb.File_TYPE_BMP,
		artifactpb.File_TYPE_TIFF,
		artifactpb.File_TYPE_WEBP:
		return thumbnailSourceImage
	case artifactpb.File_TYPE_MP4,
		artifactpb.File_TYPE_AVI,
		artifactpb.File_TYPE_MOV,
		artifactpb.File_TYPE_MKV,
		artifactpb.File_TYPE_FLV,
		artifactpb.File_TYPE_WMV,
		artifactpb.File_TYPE_MPEG,
		artifactpb.File_TYPE_WEBM_VIDEO:
		return thumbnailSourceVideo
	default:
		return thumbnailSourceUnsupported
	}
}

// renderThumbnailWebP shells out to ffmpeg (already available on the worker
// image — see `ExtractAudioActivity`) to produce a 1024 px-wide WebP.
//
// We prefer ffmpeg over a pure-Go decoder/resizer for two reasons:
//
//  1. It covers every image and video codec the ingestion pipeline accepts,
//     including animated GIFs (first frame) and HEIC/TIFF variants.
//  2. It runs the same binary we already ship and monitor, so we avoid
//     importing another CGO dependency just for previews.
func renderThumbnailWebP(ctx context.Context, kind thumbnailKind, source []byte) ([]byte, error) {
	inExt := "bin"
	switch kind {
	case thumbnailSourceImage:
		inExt = "img"
	case thumbnailSourceVideo:
		inExt = "mp4"
	}

	inFile, err := os.CreateTemp("", "thumb-in-*."+inExt)
	if err != nil {
		return nil, fmt.Errorf("create input temp file: %w", err)
	}
	inPath := inFile.Name()
	defer os.Remove(inPath)
	if _, err := inFile.Write(source); err != nil {
		inFile.Close()
		return nil, fmt.Errorf("write source temp file: %w", err)
	}
	if err := inFile.Close(); err != nil {
		return nil, fmt.Errorf("close source temp file: %w", err)
	}

	outFile, err := os.CreateTemp("", "thumb-out-*."+ThumbnailFileExt)
	if err != nil {
		return nil, fmt.Errorf("create output temp file: %w", err)
	}
	outPath := outFile.Name()
	outFile.Close()
	defer os.Remove(outPath)

	// `-vf` scale keeps aspect ratio: width=min(src,1024), height=auto.
	// `-frames:v 1` makes this work for both stills and videos — for video
	// inputs we also seek to 1 s to skip blank intro frames, falling back to
	// frame 0 for very short clips via `-ss 00:00:01 -noaccurate_seek`.
	args := []string{
		"-y",
		"-hide_banner",
		"-loglevel", "error",
	}
	if kind == thumbnailSourceVideo {
		args = append(args, "-ss", "00:00:01")
	}
	args = append(args,
		"-i", inPath,
		"-frames:v", "1",
		"-vf", fmt.Sprintf("scale=w='min(%d,iw)':h=-2", ThumbnailMaxDimensionPx),
		"-c:v", "libwebp",
		"-quality", fmt.Sprintf("%d", ThumbnailWebPQuality),
		"-preset", "photo",
		outPath,
	)

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("ffmpeg failed: %w: %s", err, string(out))
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		return nil, fmt.Errorf("read thumbnail output: %w", err)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("ffmpeg produced empty thumbnail (input ext=%s)", filepath.Ext(inPath))
	}
	return data, nil
}
