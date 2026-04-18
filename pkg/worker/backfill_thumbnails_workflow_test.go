package worker

import (
	"context"
	"sync"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/zap"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/repository/object"
	"github.com/instill-ai/artifact-backend/pkg/types"
	workermock "github.com/instill-ai/artifact-backend/pkg/worker/mock"
)

// TestListFilesMissingThumbnailsActivity_Dedup verifies that duplicate
// (file_uid, kb_uid) pairs — which occur when a file is associated with
// multiple KBs — collapse to a single BackfillFileDescriptor. This keeps
// `GenerateThumbnailActivity` from being scheduled twice for the same
// file and protects `ARTIFACT-INV-THUMBNAIL-NON-BLOCKING`'s idempotency
// guard from racing with itself.
func TestListFilesMissingThumbnailsActivity_Dedup(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	fileA := types.FileUIDType(uuid.Must(uuid.NewV4()))
	fileB := types.FileUIDType(uuid.Must(uuid.NewV4()))
	kb1 := types.KBUIDType(uuid.Must(uuid.NewV4()))
	kb2 := types.KBUIDType(uuid.Must(uuid.NewV4()))

	mockRepo := workermock.NewRepositoryMock(mc)
	mockRepo.ListFilesMissingThumbnailsMock.Return([]repository.FileMissingThumbnail{
		{FileUID: fileA, KBUID: kb1, FileType: "TYPE_PNG", StoragePath: "kb1/a.png", DisplayName: "a.png"},
		{FileUID: fileA, KBUID: kb2, FileType: "TYPE_PNG", StoragePath: "kb1/a.png", DisplayName: "a.png"},
		{FileUID: fileB, KBUID: kb2, FileType: "TYPE_MP4", StoragePath: "kb2/b.mp4", DisplayName: "b.mp4"},
	}, nil)

	w := &Worker{repository: mockRepo, log: zap.NewNop()}
	res, err := w.ListFilesMissingThumbnailsActivity(c.Context(), &ListFilesMissingThumbnailsActivityParam{
		BatchSize: 10,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(res.Files, qt.HasLen, 2)
	c.Assert(res.Files[0].FileUID, qt.Equals, fileA)
	c.Assert(res.Files[0].KBUID, qt.Equals, kb1, qt.Commentf("first-association wins"))
	c.Assert(res.Files[1].FileUID, qt.Equals, fileB)
}

// TestListFilesMissingThumbnailsActivity_UnknownFileTypeIsSkipped guards
// the workflow from crashing when a `file.file_type` string in the DB
// drifts from the proto enum (eg. a legacy row). The activity must skip
// the row but still return a cursor-advancing descriptor so the outer
// loop cannot spin.
func TestListFilesMissingThumbnailsActivity_UnknownFileTypeIsSkipped(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	fileA := types.FileUIDType(uuid.Must(uuid.NewV4()))
	kb1 := types.KBUIDType(uuid.Must(uuid.NewV4()))

	mockRepo := workermock.NewRepositoryMock(mc)
	mockRepo.ListFilesMissingThumbnailsMock.Return([]repository.FileMissingThumbnail{
		{FileUID: fileA, KBUID: kb1, FileType: "TYPE_MARS_ROCKS", StoragePath: "kb1/a.png", DisplayName: "a.png"},
	}, nil)

	w := &Worker{repository: mockRepo, log: zap.NewNop()}
	res, err := w.ListFilesMissingThumbnailsActivity(c.Context(), &ListFilesMissingThumbnailsActivityParam{
		BatchSize: 10,
	})
	c.Assert(err, qt.IsNil)
	// Guard-rail row advances the cursor even though the file type was
	// rejected — see the "Guard rail" block in
	// ListFilesMissingThumbnailsActivity.
	c.Assert(res.Files, qt.HasLen, 1)
	c.Assert(res.Files[0].FileUID, qt.Equals, fileA)
}

// TestBackfillThumbnailsWorkflow_Drains verifies that the workflow exits
// cleanly when the repository returns an empty batch — the "nothing to
// backfill" steady state that production should hit after the first
// successful run.
func TestBackfillThumbnailsWorkflow_Drains(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepo := workermock.NewRepositoryMock(mc)
	mockRepo.ListFilesMissingThumbnailsMock.Return(nil, nil)

	w := &Worker{repository: mockRepo, log: zap.NewNop()}

	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(w.ListFilesMissingThumbnailsActivity)
	env.RegisterActivity(w.GenerateThumbnailActivity)
	env.RegisterWorkflow(w.BackfillThumbnailsWorkflow)

	env.ExecuteWorkflow(w.BackfillThumbnailsWorkflow, BackfillThumbnailsWorkflowParam{
		BatchSize: 50,
	})
	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.IsNil)
}

// TestBackfillThumbnailsWorkflowParam_ZeroValues protects against
// regressions in the zero-value defaults that the workflow relies on
// when operators invoke it without specifying a batch size.
func TestBackfillThumbnailsWorkflowParam_ZeroValues(t *testing.T) {
	c := qt.New(t)

	var p BackfillThumbnailsWorkflowParam
	c.Assert(p.BatchSize, qt.Equals, 0)
	c.Assert(p.MaxFiles, qt.Equals, 0)
	c.Assert(p.AfterFileUID, qt.Equals, types.FileUIDType(uuid.Nil))
	c.Assert(p.ProcessedCount, qt.Equals, 0)
	c.Assert(p.IterationCount, qt.Equals, 0)
}

// TestBackfillThumbnailsWorkflow_ResolvesBucketFromStoragePath guards
// against hardcoding `config.Config.Minio.BucketName` when dispatching
// `GenerateThumbnailActivity` during the backfill. The `file.storage_path`
// column can hold paths for two distinct buckets:
//
//   - `core-artifact` — legacy / KB-direct uploads (`kb-<uid>/...`,
//     `.../uploaded-file/...`).
//   - `core-blob` — object-API uploads via presigned URLs
//     (`ns-<namespace_uid>/obj-<object_uid>`).
//
// A prior regression hardcoded `core-artifact` for every row, which
// silently failed every `ns-` file with "The specified key does not exist"
// because the blob actually lives in `core-blob`. The workflow must
// delegate to `object.BucketFromDestination` — the single source of
// truth for this convention — for every dispatched activity.
func TestBackfillThumbnailsWorkflow_ResolvesBucketFromStoragePath(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	artifactFileUID := types.FileUIDType(uuid.Must(uuid.NewV4()))
	blobFileUID := types.FileUIDType(uuid.Must(uuid.NewV4()))
	kbUID := types.KBUIDType(uuid.Must(uuid.NewV4()))

	const artifactPath = "kb-11111111-1111-1111-1111-111111111111/file-22222222-2222-2222-2222-222222222222/source.png"
	const blobPath = "ns-33333333-3333-3333-3333-333333333333/obj-44444444-4444-4444-4444-444444444444"

	mockRepo := workermock.NewRepositoryMock(mc)
	// First call yields both files; second call signals drain.
	mockRepo.ListFilesMissingThumbnailsMock.Set(func(
		_ context.Context,
		after types.FileUIDType,
		_ int,
	) ([]repository.FileMissingThumbnail, error) {
		if after == types.FileUIDType(uuid.Nil) {
			return []repository.FileMissingThumbnail{
				{FileUID: artifactFileUID, KBUID: kbUID, FileType: "TYPE_PNG", StoragePath: artifactPath, DisplayName: "source.png"},
				{FileUID: blobFileUID, KBUID: kbUID, FileType: "TYPE_MP4", StoragePath: blobPath, DisplayName: "video.mp4"},
			}, nil
		}
		return nil, nil
	})

	w := &Worker{repository: mockRepo, log: zap.NewNop()}

	// Capture the bucket / destination tuples that the workflow passes
	// to `GenerateThumbnailActivity` — these are the fields the
	// regression hid.
	type dispatched struct{ Bucket, Destination string }
	var (
		mu  sync.Mutex
		got []dispatched
	)

	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(w.ListFilesMissingThumbnailsActivity)
	env.RegisterActivity(w.GenerateThumbnailActivity)
	env.OnActivity(w.GenerateThumbnailActivity, mock.Anything, mock.Anything).
		Return(func(_ context.Context, p *GenerateThumbnailActivityParam) (*GenerateThumbnailActivityResult, error) {
			mu.Lock()
			got = append(got, dispatched{Bucket: p.Bucket, Destination: p.Destination})
			mu.Unlock()
			return &GenerateThumbnailActivityResult{ConvertedFileUID: types.ConvertedFileUIDType(uuid.Must(uuid.NewV4()))}, nil
		})
	env.RegisterWorkflow(w.BackfillThumbnailsWorkflow)

	// BatchSize must be > len(files) so the workflow recognises the
	// first batch as the drain batch and exits cleanly without a
	// second iteration.
	env.ExecuteWorkflow(w.BackfillThumbnailsWorkflow, BackfillThumbnailsWorkflowParam{BatchSize: 10})

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.IsNil)
	c.Assert(got, qt.HasLen, 2)

	index := make(map[string]dispatched, len(got))
	for _, d := range got {
		index[d.Destination] = d
	}

	// Both dispatches must resolve bucket via BucketFromDestination —
	// never via a hardcoded default.
	c.Assert(index[artifactPath].Bucket, qt.Equals, object.BucketFromDestination(artifactPath),
		qt.Commentf("artifact-bucket path must resolve via BucketFromDestination"))
	c.Assert(index[blobPath].Bucket, qt.Equals, object.BucketFromDestination(blobPath),
		qt.Commentf("blob-bucket path must resolve via BucketFromDestination"))

	// Additionally assert the two buckets diverge — the whole point of
	// the helper. If they collapse to the same string we are back in
	// the regression.
	c.Assert(index[artifactPath].Bucket, qt.Not(qt.Equals), index[blobPath].Bucket,
		qt.Commentf("artifact-bucket and blob-bucket paths must route to different buckets"))
	c.Assert(index[blobPath].Bucket, qt.Equals, object.BlobBucketName,
		qt.Commentf("ns-*/obj-* paths must route to core-blob"))
}
