package worker

import (
	"testing"

	"github.com/gofrs/uuid"
	"github.com/gojuno/minimock/v3"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/zap"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"
	"github.com/instill-ai/artifact-backend/pkg/worker/mock"
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

	mockRepo := mock.NewRepositoryMock(mc)
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

	mockRepo := mock.NewRepositoryMock(mc)
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

	mockRepo := mock.NewRepositoryMock(mc)
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
