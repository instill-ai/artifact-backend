package worker

import (
	"context"
	"errors"
	"fmt"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

// ProbeFileChunkIntegrityActivityParam carries the inputs for
// ProbeFileChunkIntegrityActivity.
type ProbeFileChunkIntegrityActivityParam struct {
	FileUID types.FileUIDType
}

// ProbeFileChunkIntegrityActivityResult mirrors the load-bearing
// fields of artifactpb.CheckFileChunkIntegrityAdminResponse that the
// workflow uses to decide between "full reprocess" and "embed-only
// fast path". We do not return the full proto envelope because
// Temporal payload encoding is keyed on Go types and the response
// proto evolves independently — keeping a small typed struct keeps
// the activity contract narrow.
type ProbeFileChunkIntegrityActivityResult struct {
	State                artifactpb.IntegrityState
	ChunkRowsInPg        int64
	VectorsInMilvus      int64
	ConvertedFilePresent bool
}

const probeFileChunkIntegrityActivityError = "ProbeFileChunkIntegrityActivity"

// ProbeFileChunkIntegrityActivity is the workflow-side equivalent of
// the CheckFileChunkIntegrityAdmin handler in pkg/handler/private.go.
// It exists so ProcessFileWorkflow can detect the MISSING_MILVUS
// drift class (PG chunks present + converted_file present + zero
// Milvus vectors) and route the file into the embed-only fast path
// instead of paying the full Convert + Chunk + Embed pipeline cost
// for what is structurally a re-embed-only recovery.
//
// The classification logic MUST stay byte-equivalent to the handler
// so the cell-worker's `RECOMMENDED_ACTION_REPROCESS_FILE` decision
// (driven by the handler) and the workflow's "skip Convert+Chunk"
// decision (driven by this activity) cannot diverge. See:
//
//   - ARTIFACT-INV-FILE-CHUNK-INTEGRITY-PROBE in
//     docs/invariants/vector-search.md
//   - ARTIFACT-INV-REPROCESS-EMBED-ONLY-FAST-PATH in
//     docs/invariants/reprocess-status.md
func (w *Worker) ProbeFileChunkIntegrityActivity(ctx context.Context, param *ProbeFileChunkIntegrityActivityParam) (*ProbeFileChunkIntegrityActivityResult, error) {
	logger := w.log.With(zap.String("fileUID", param.FileUID.String()))

	files, err := w.repository.GetFilesByFileUIDs(ctx, []types.FileUIDType{param.FileUID})
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to look up file for integrity probe.")
		return nil, activityError(err, probeFileChunkIntegrityActivityError)
	}
	if len(files) == 0 {
		return &ProbeFileChunkIntegrityActivityResult{
			State: artifactpb.IntegrityState_INTEGRITY_STATE_FILE_NOT_FOUND,
		}, nil
	}
	file := files[0]

	processStatus := artifactpb.FileProcessStatus(artifactpb.FileProcessStatus_value[file.ProcessStatus])
	if processStatus != artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED {
		return &ProbeFileChunkIntegrityActivityResult{
			State: artifactpb.IntegrityState_INTEGRITY_STATE_NOT_COMPLETED,
		}, nil
	}

	chunkCount, err := w.repository.GetChunkCountByFileUID(ctx, file.UID)
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to count file chunks for integrity probe.")
		return nil, activityError(err, probeFileChunkIntegrityActivityError)
	}
	if chunkCount == 0 {
		return &ProbeFileChunkIntegrityActivityResult{
			State:         artifactpb.IntegrityState_INTEGRITY_STATE_EMPTY_PG,
			ChunkRowsInPg: 0,
		}, nil
	}

	kbUIDs, err := w.repository.GetKnowledgeBaseUIDsForFile(ctx, file.UID)
	if err != nil || len(kbUIDs) == 0 {
		logger.Warn("ProbeFileChunkIntegrityActivity: KB association lookup failed; treat as tombstone",
			zap.Error(err))
		return &ProbeFileChunkIntegrityActivityResult{
			State:         artifactpb.IntegrityState_INTEGRITY_STATE_FILE_NOT_FOUND,
			ChunkRowsInPg: chunkCount,
		}, nil
	}

	kb, err := w.repository.GetKnowledgeBaseByUID(ctx, kbUIDs[0])
	if err != nil {
		err = errorsx.AddMessage(err, "Unable to look up knowledge base for integrity probe.")
		return nil, activityError(err, probeFileChunkIntegrityActivityError)
	}

	collectionUID := kb.ActiveCollectionUID
	if collectionUID == uuid.Nil {
		collectionUID = kb.UID
	}
	collectionName := constant.KBCollectionName(collectionUID)
	vectorCount, err := w.repository.CountVectorsForFileUID(ctx, collectionName, file.UID)
	if err != nil {
		err = errorsx.AddMessage(
			fmt.Errorf("counting Milvus vectors: %w", err),
			"Unable to count vectors for integrity probe.",
		)
		return nil, activityError(err, probeFileChunkIntegrityActivityError)
	}

	if vectorCount < 0 || vectorCount != chunkCount {
		convertedPresent := true
		if _, cErr := w.repository.GetConvertedFileByFileUID(ctx, file.UID); cErr != nil {
			if errors.Is(cErr, gorm.ErrRecordNotFound) {
				convertedPresent = false
			} else {
				cErr = errorsx.AddMessage(cErr, "Unable to look up converted file for integrity probe.")
				return nil, activityError(cErr, probeFileChunkIntegrityActivityError)
			}
		}
		return &ProbeFileChunkIntegrityActivityResult{
			State:                artifactpb.IntegrityState_INTEGRITY_STATE_MISSING_MILVUS,
			ChunkRowsInPg:        chunkCount,
			VectorsInMilvus:      vectorCount,
			ConvertedFilePresent: convertedPresent,
		}, nil
	}

	convertedPresent := true
	if _, err := w.repository.GetConvertedFileByFileUID(ctx, file.UID); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			convertedPresent = false
		} else {
			err = errorsx.AddMessage(err, "Unable to look up converted file for integrity probe.")
			return nil, activityError(err, probeFileChunkIntegrityActivityError)
		}
	}

	if !convertedPresent {
		return &ProbeFileChunkIntegrityActivityResult{
			State:                artifactpb.IntegrityState_INTEGRITY_STATE_MISSING_CONVERTED_FILE,
			ChunkRowsInPg:        chunkCount,
			VectorsInMilvus:      vectorCount,
			ConvertedFilePresent: false,
		}, nil
	}

	return &ProbeFileChunkIntegrityActivityResult{
		State:                artifactpb.IntegrityState_INTEGRITY_STATE_HEALTHY,
		ChunkRowsInPg:        chunkCount,
		VectorsInMilvus:      vectorCount,
		ConvertedFilePresent: true,
	}, nil
}
