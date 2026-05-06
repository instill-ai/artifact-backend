package worker

import (
	"context"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/gojuno/minimock/v3"
	"go.uber.org/zap"
	"gorm.io/gorm"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/worker/mock"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
)

// TestProbeFileChunkIntegrityActivity_ClassifiesMissingMilvusForReembedFastPath
// pins ARTIFACT-INV-REPROCESS-EMBED-ONLY-FAST-PATH
// (docs/invariants/reprocess-status.md).
//
// When a file has all the prerequisites for an embed-only recovery
// (ProcessStatus = COMPLETED, chunk rows in PG > 0,
//  vectors in Milvus = 0, converted_file present),
// the probe MUST classify the file as MISSING_MILVUS with
// ConvertedFilePresent = true. This is the precise classification
// the workflow keys on to skip the full Convert + Chunk pipeline and
// jump straight to EmbedAndSaveChunksActivity. Any drift in this
// classification (e.g. always returning MISSING_CONVERTED_FILE first)
// silently disables the fast path — the workflow falls back to the
// expensive full reprocess and the optimisation evaporates.
func TestProbeFileChunkIntegrityActivity_ClassifiesMissingMilvusForReembedFastPath(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepository := mock.NewRepositoryMock(mc)
	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	mockRepository.GetFilesByFileUIDsMock.Return([]repository.FileModel{{
		UID:           fileUID,
		ProcessStatus: artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED.String(),
	}}, nil)
	mockRepository.GetChunkCountByFileUIDMock.Return(int64(42), nil)
	mockRepository.GetKnowledgeBaseUIDsForFileMock.Return([]uuid.UUID{kbUID}, nil)
	mockRepository.GetKnowledgeBaseByUIDMock.Return(&repository.KnowledgeBaseModel{UID: kbUID}, nil)
	mockRepository.CountVectorsForFileUIDMock.Return(int64(0), nil)
	mockRepository.GetConvertedFileByFileUIDMock.Return(&repository.ConvertedFileModel{}, nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}
	res, err := w.ProbeFileChunkIntegrityActivity(context.Background(), &ProbeFileChunkIntegrityActivityParam{FileUID: fileUID})

	c.Assert(err, qt.IsNil)
	c.Assert(res.State, qt.Equals, artifactpb.IntegrityState_INTEGRITY_STATE_MISSING_MILVUS)
	c.Assert(res.ChunkRowsInPg, qt.Equals, int64(42))
	c.Assert(res.VectorsInMilvus, qt.Equals, int64(0))
	c.Assert(res.ConvertedFilePresent, qt.IsTrue)
}

// TestProbeFileChunkIntegrityActivity_ClassifiesEmptyPgAsNonFastPathSafe
// pins the conservative side of ARTIFACT-INV-REPROCESS-EMBED-ONLY-FAST-PATH.
//
// EMPTY_PG (chunk rows in PG = 0) MUST NOT be classified as
// MISSING_MILVUS — the embed-only fast path requires PG chunks to
// embed. Misclassifying EMPTY_PG would route the workflow through
// EmbedAndSaveChunksActivity with zero rows to embed and silently
// "complete" without restoring any vectors.
func TestProbeFileChunkIntegrityActivity_ClassifiesEmptyPgAsNonFastPathSafe(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepository := mock.NewRepositoryMock(mc)
	fileUID := uuid.Must(uuid.NewV4())

	mockRepository.GetFilesByFileUIDsMock.Return([]repository.FileModel{{
		UID:           fileUID,
		ProcessStatus: artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED.String(),
	}}, nil)
	mockRepository.GetChunkCountByFileUIDMock.Return(int64(0), nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}
	res, err := w.ProbeFileChunkIntegrityActivity(context.Background(), &ProbeFileChunkIntegrityActivityParam{FileUID: fileUID})

	c.Assert(err, qt.IsNil)
	c.Assert(res.State, qt.Equals, artifactpb.IntegrityState_INTEGRITY_STATE_EMPTY_PG)
	c.Assert(res.ChunkRowsInPg, qt.Equals, int64(0))
}

// TestProbeFileChunkIntegrityActivity_ClassifiesMissingConvertedFileSeparately
// pins the conservative side of ARTIFACT-INV-REPROCESS-EMBED-ONLY-FAST-PATH.
//
// When PG chunks are intact and Milvus matches but the converted_file
// row is gone, this is NOT a re-embed-only recovery — the workflow
// would still need to re-create the converted_file from the original
// upload. The probe MUST distinguish this from MISSING_MILVUS so the
// workflow does not skip ConvertFile.
func TestProbeFileChunkIntegrityActivity_ClassifiesMissingConvertedFileSeparately(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepository := mock.NewRepositoryMock(mc)
	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	mockRepository.GetFilesByFileUIDsMock.Return([]repository.FileModel{{
		UID:           fileUID,
		ProcessStatus: artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED.String(),
	}}, nil)
	mockRepository.GetChunkCountByFileUIDMock.Return(int64(10), nil)
	mockRepository.GetKnowledgeBaseUIDsForFileMock.Return([]uuid.UUID{kbUID}, nil)
	mockRepository.GetKnowledgeBaseByUIDMock.Return(&repository.KnowledgeBaseModel{UID: kbUID}, nil)
	mockRepository.CountVectorsForFileUIDMock.Return(int64(10), nil)
	mockRepository.GetConvertedFileByFileUIDMock.Return(nil, gorm.ErrRecordNotFound)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}
	res, err := w.ProbeFileChunkIntegrityActivity(context.Background(), &ProbeFileChunkIntegrityActivityParam{FileUID: fileUID})

	c.Assert(err, qt.IsNil)
	c.Assert(res.State, qt.Equals, artifactpb.IntegrityState_INTEGRITY_STATE_MISSING_CONVERTED_FILE)
	c.Assert(res.ConvertedFilePresent, qt.IsFalse)
}

// TestProbeFileChunkIntegrityActivity_ClassifiesHealthy validates that
// when every cross-datastore artefact is intact, the probe returns
// HEALTHY. The workflow then takes neither the embed-only fast path
// nor the full reprocess — the file is already in the correct state
// and the caller (cell-worker drift gate) should never have asked.
func TestProbeFileChunkIntegrityActivity_ClassifiesHealthy(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepository := mock.NewRepositoryMock(mc)
	fileUID := uuid.Must(uuid.NewV4())
	kbUID := uuid.Must(uuid.NewV4())

	mockRepository.GetFilesByFileUIDsMock.Return([]repository.FileModel{{
		UID:           fileUID,
		ProcessStatus: artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED.String(),
	}}, nil)
	mockRepository.GetChunkCountByFileUIDMock.Return(int64(7), nil)
	mockRepository.GetKnowledgeBaseUIDsForFileMock.Return([]uuid.UUID{kbUID}, nil)
	mockRepository.GetKnowledgeBaseByUIDMock.Return(&repository.KnowledgeBaseModel{UID: kbUID}, nil)
	mockRepository.CountVectorsForFileUIDMock.Return(int64(7), nil)
	mockRepository.GetConvertedFileByFileUIDMock.Return(&repository.ConvertedFileModel{}, nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}
	res, err := w.ProbeFileChunkIntegrityActivity(context.Background(), &ProbeFileChunkIntegrityActivityParam{FileUID: fileUID})

	c.Assert(err, qt.IsNil)
	c.Assert(res.State, qt.Equals, artifactpb.IntegrityState_INTEGRITY_STATE_HEALTHY)
	c.Assert(res.ConvertedFilePresent, qt.IsTrue)
}

// TestProbeFileChunkIntegrityActivity_NotCompleted validates the probe
// short-circuits for non-COMPLETED files. The workflow only takes the
// fast path when startStatus is COMPLETED, but the probe guards
// against future callers reusing the activity from a different
// branch.
func TestProbeFileChunkIntegrityActivity_NotCompleted(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepository := mock.NewRepositoryMock(mc)
	fileUID := uuid.Must(uuid.NewV4())

	mockRepository.GetFilesByFileUIDsMock.Return([]repository.FileModel{{
		UID:           fileUID,
		ProcessStatus: artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_PROCESSING.String(),
	}}, nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}
	res, err := w.ProbeFileChunkIntegrityActivity(context.Background(), &ProbeFileChunkIntegrityActivityParam{FileUID: fileUID})

	c.Assert(err, qt.IsNil)
	c.Assert(res.State, qt.Equals, artifactpb.IntegrityState_INTEGRITY_STATE_NOT_COMPLETED)
}

// Compile-time guard that the activity result struct exposes the
// fields the workflow's MISSING_MILVUS branch reads. If anyone
// renames a field the build breaks here, not at runtime in
// production.
var _ = func() *ProbeFileChunkIntegrityActivityResult {
	var r ProbeFileChunkIntegrityActivityResult
	_ = r.State
	_ = r.ChunkRowsInPg
	_ = r.VectorsInMilvus
	_ = r.ConvertedFilePresent
	return &r
}
