package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/gojuno/minimock/v3"
	"go.uber.org/zap"
	"gorm.io/gorm"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"
	"github.com/instill-ai/artifact-backend/pkg/worker/mock"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
)

// TestClassifyThumbnailSource pins the activity's file-type routing. Keeping
// this table-driven guards the contract in the plan
// (`CONVERTED_FILE_TYPE_THUMBNAIL`): image/video produce a thumbnail, every
// other standardized type must fall through so the frontend's mime-icon
// fallback stays the authoritative path.
func TestClassifyThumbnailSource(t *testing.T) {
	c := qt.New(t)

	cases := []struct {
		name string
		ft   artifactpb.File_Type
		want thumbnailKind
	}{
		{"png_is_image", artifactpb.File_TYPE_PNG, thumbnailSourceImage},
		{"jpeg_is_image", artifactpb.File_TYPE_JPEG, thumbnailSourceImage},
		{"gif_is_image", artifactpb.File_TYPE_GIF, thumbnailSourceImage},
		{"webp_is_image", artifactpb.File_TYPE_WEBP, thumbnailSourceImage},
		{"tiff_is_image", artifactpb.File_TYPE_TIFF, thumbnailSourceImage},
		{"bmp_is_image", artifactpb.File_TYPE_BMP, thumbnailSourceImage},

		{"mp4_is_video", artifactpb.File_TYPE_MP4, thumbnailSourceVideo},
		{"mov_is_video", artifactpb.File_TYPE_MOV, thumbnailSourceVideo},
		{"webm_is_video", artifactpb.File_TYPE_WEBM_VIDEO, thumbnailSourceVideo},

		{"pdf_unsupported", artifactpb.File_TYPE_PDF, thumbnailSourceUnsupported},
		{"doc_unsupported", artifactpb.File_TYPE_DOC, thumbnailSourceUnsupported},
		{"mp3_unsupported", artifactpb.File_TYPE_MP3, thumbnailSourceUnsupported},
		{"text_unsupported", artifactpb.File_TYPE_TEXT, thumbnailSourceUnsupported},
		{"unspecified_unsupported", artifactpb.File_TYPE_UNSPECIFIED, thumbnailSourceUnsupported},
	}

	for _, tc := range cases {
		tc := tc
		c.Run(tc.name, func(c *qt.C) {
			c.Assert(classifyThumbnailSource(tc.ft), qt.Equals, tc.want)
		})
	}
}

// TestGenerateThumbnailActivity_SkipsUnsupportedType verifies that we do not
// touch MinIO or the DB for file types the activity cannot preview. This is
// the primary safety net for the "never fail the parent workflow" contract —
// an unsupported type must short-circuit cleanly.
func TestGenerateThumbnailActivity_SkipsUnsupportedType(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := types.FileUIDType(uuid.Must(uuid.NewV4()))
	kbUID := types.KBUIDType(uuid.Must(uuid.NewV4()))

	mockRepository := mock.NewRepositoryMock(mc)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	res, err := w.GenerateThumbnailActivity(ctx, &GenerateThumbnailActivityParam{
		FileUID:         fileUID,
		KBUID:           kbUID,
		Bucket:          "test-bucket",
		Destination:     "kb/file/doc.pdf",
		FileType:        artifactpb.File_TYPE_PDF,
		FileDisplayName: "doc.pdf",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(res, qt.Not(qt.IsNil))
	c.Assert(res.Skipped, qt.IsTrue)
	c.Assert(res.SkippedReason, qt.Not(qt.Equals), "")
	c.Assert(res.ThumbnailDestination, qt.Equals, "")
	c.Assert(res.ConvertedFileUID, qt.Equals, types.ConvertedFileUIDType(uuid.Nil))
}

// TestGenerateThumbnailActivity_ReusesExistingRow verifies the idempotency
// guard: a second activity attempt (eg. a Temporal retry after a
// transient failure) must NOT re-run ffmpeg or re-upload. This guarantee is
// what lets us register the activity with MaximumAttempts=3 without piling
// up duplicate blobs.
func TestGenerateThumbnailActivity_ReusesExistingRow(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := types.FileUIDType(uuid.Must(uuid.NewV4()))
	kbUID := types.KBUIDType(uuid.Must(uuid.NewV4()))
	existingUID := types.ConvertedFileUIDType(uuid.Must(uuid.NewV4()))
	existingPath := "kb-uid/file-uid/existing.webp"

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetConvertedFileByFileUIDAndTypeMock.
		Expect(minimock.AnyContext, fileUID, artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_THUMBNAIL).
		Return(&repository.ConvertedFileModel{
			UID:           existingUID,
			StoragePath:   existingPath,
			ConvertedType: artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_THUMBNAIL.String(),
		}, nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	res, err := w.GenerateThumbnailActivity(ctx, &GenerateThumbnailActivityParam{
		FileUID:         fileUID,
		KBUID:           kbUID,
		Bucket:          "test-bucket",
		Destination:     "kb/file/photo.png",
		FileType:        artifactpb.File_TYPE_PNG,
		FileDisplayName: "photo.png",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(res.Skipped, qt.IsFalse)
	c.Assert(res.ConvertedFileUID, qt.Equals, existingUID)
	c.Assert(res.ThumbnailDestination, qt.Equals, existingPath)
}

// TestFindExistingThumbnail_NotFoundIsHappyPath ensures a gorm "record not
// found" error from the typed lookup returns `(nil, false)` rather than
// surfacing the error — this is the hot path on every first-time run.
func TestFindExistingThumbnail_NotFoundIsHappyPath(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := types.FileUIDType(uuid.Must(uuid.NewV4()))

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetConvertedFileByFileUIDAndTypeMock.
		Expect(minimock.AnyContext, fileUID, artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_THUMBNAIL).
		Return(nil, gorm.ErrRecordNotFound)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	row, ok := w.findExistingThumbnail(ctx, fileUID)
	c.Assert(ok, qt.IsFalse)
	c.Assert(row, qt.IsNil)
}

// TestFindExistingThumbnail_ErrorDoesNotPanic is a defensive check: even a
// non-"not found" DB error must not panic; the caller treats it as a miss
// and proceeds to generation.
func TestFindExistingThumbnail_ErrorDoesNotPanic(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	ctx := context.Background()
	fileUID := types.FileUIDType(uuid.Must(uuid.NewV4()))

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.GetConvertedFileByFileUIDAndTypeMock.
		Expect(minimock.AnyContext, fileUID, artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_THUMBNAIL).
		Return(nil, errors.New("db down"))

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	row, ok := w.findExistingThumbnail(ctx, fileUID)
	c.Assert(ok, qt.IsFalse)
	c.Assert(row, qt.IsNil)
}
