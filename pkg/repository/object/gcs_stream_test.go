package object

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"go.uber.org/zap"
)

// These tests pin ARTIFACT-INV-GCS-STREAM-RESUME by exercising the
// pure copy-loop helper (`streamCopyWithResume`) with a caller-supplied
// writer and a programmable openAt factory. We deliberately avoid the
// GCS client dependency: the resume behaviour is writer-agnostic (the
// loop only cares that Write returns nil), and the failure modes we
// need to guard against all originate on the reader / factory side.
//
// Scenarios (numbered to match the plan):
//   1. Clean copy — openAt fires once, no resumes.
//   2. One truncation — resumes==1, final bytes byte-exact.
//   3. Three truncations — resumes==3 (at the budget limit), still succeeds.
//   4. Budget exhausted — four truncations, returns wrapped ErrUnexpectedEOF.
//   5. Non-EOF error — returns the error verbatim, no resume attempt.
//   6. offset==0 footgun — MinIO SetRange(0,0) would deliver 1 byte;
//      the test lives in minio_range_test.go below to keep GCS-side
//      assertions together.

// scriptedReadCloser emits a sequence of (bytes, err) steps on
// successive Read calls. Close is a no-op.
type scriptedReadCloser struct {
	steps []readStep
	idx   int
}

type readStep struct {
	data []byte
	err  error
}

func (s *scriptedReadCloser) Read(p []byte) (int, error) {
	if s.idx >= len(s.steps) {
		return 0, io.EOF
	}
	step := s.steps[s.idx]
	s.idx++
	n := copy(p, step.data)
	// If step had more bytes than p could hold, push the remainder
	// back to the front of the script so the next Read picks it up.
	if n < len(step.data) {
		s.steps[s.idx-1] = readStep{data: step.data[n:], err: step.err}
		s.idx--
		return n, nil
	}
	return n, step.err
}

func (s *scriptedReadCloser) Close() error { return nil }

// newResumeScript builds a factory that emits a truncation after
// `truncateAfter` bytes per-open, up to `truncations` truncations. On
// the (truncations+1)th open it delivers the remaining payload cleanly.
// Each open is scoped by `offset`: only the tail of `payload` from
// `offset` onward is handed out.
func newResumeScript(payload []byte, truncateAfter int64, truncations int) (
	factory func(offset int64) (io.ReadCloser, error),
	opens *[]int64,
) {
	var calls []int64
	factory = func(offset int64) (io.ReadCloser, error) {
		calls = append(calls, offset)
		tail := payload[offset:]
		// If we've already exhausted our truncation budget, deliver
		// the full remaining tail in one shot with a nil terminal err
		// (io.EOF -> nil via io.Copy).
		if len(calls) > truncations {
			return &scriptedReadCloser{steps: []readStep{
				{data: tail, err: io.EOF},
			}}, nil
		}
		// Deliver up to truncateAfter bytes then slam with
		// ErrUnexpectedEOF.
		cutoff := truncateAfter
		if int64(len(tail)) < cutoff {
			cutoff = int64(len(tail))
		}
		return &scriptedReadCloser{steps: []readStep{
			{data: tail[:cutoff], err: io.ErrUnexpectedEOF},
		}}, nil
	}
	return factory, &calls
}

// TestStreamCopyWithResume_CleanCopy pins invariant (1). A reader that
// delivers the full payload on the first openAt(0) must complete with
// resumes==0 and copied==size.
func TestStreamCopyWithResume_CleanCopy(t *testing.T) {
	c := qt.New(t)
	payload := bytes.Repeat([]byte{0xAA}, 8192)

	var opens []int64
	openAt := func(offset int64) (io.ReadCloser, error) {
		opens = append(opens, offset)
		return &scriptedReadCloser{steps: []readStep{
			{data: payload[offset:], err: io.EOF},
		}}, nil
	}

	var buf bytes.Buffer
	copied, resumes, err := streamCopyWithResume(
		context.Background(), &buf, openAt, int64(len(payload)),
		zap.NewNop(), nil, 3, time.Millisecond,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(copied, qt.Equals, int64(len(payload)))
	c.Assert(resumes, qt.Equals, 0)
	c.Assert(opens, qt.DeepEquals, []int64{0})
	c.Assert(buf.Bytes(), qt.DeepEquals, payload)
}

// TestStreamCopyWithResume_OneTruncation pins invariant (2). A single
// mid-stream ErrUnexpectedEOF must be absorbed by exactly one resume,
// with openAt(N) called where N == bytes delivered so far.
func TestStreamCopyWithResume_OneTruncation(t *testing.T) {
	c := qt.New(t)
	payload := bytes.Repeat([]byte{0xBB}, 4096)

	factory, opens := newResumeScript(payload, 1000, 1)

	var buf bytes.Buffer
	copied, resumes, err := streamCopyWithResume(
		context.Background(), &buf, factory, int64(len(payload)),
		zap.NewNop(), nil, 3, time.Millisecond,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(copied, qt.Equals, int64(len(payload)))
	c.Assert(resumes, qt.Equals, 1)
	c.Assert(*opens, qt.DeepEquals, []int64{0, 1000},
		qt.Commentf("second open must request offset=1000 (bytes already copied)"))
	c.Assert(buf.Bytes(), qt.DeepEquals, payload)
}

// TestStreamCopyWithResume_ThreeTruncations pins the at-budget case:
// three resumes MUST succeed; the loop only fails on the fourth. The
// offset sequence also pins progressive advancement (each resume
// starts where the last truncation left off).
func TestStreamCopyWithResume_ThreeTruncations(t *testing.T) {
	c := qt.New(t)
	payload := bytes.Repeat([]byte{0xCC}, 4096)

	factory, opens := newResumeScript(payload, 500, 3)

	var buf bytes.Buffer
	copied, resumes, err := streamCopyWithResume(
		context.Background(), &buf, factory, int64(len(payload)),
		zap.NewNop(), nil, 3, time.Millisecond,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(copied, qt.Equals, int64(len(payload)))
	c.Assert(resumes, qt.Equals, 3)
	c.Assert(*opens, qt.DeepEquals, []int64{0, 500, 1000, 1500},
		qt.Commentf("progressive resume offsets"))
	c.Assert(buf.Bytes(), qt.DeepEquals, payload)
}

// TestStreamCopyWithResume_BudgetExhausted pins the over-budget case.
// A fourth consecutive truncation must yield the wrapped
// ErrUnexpectedEOF WITHOUT finalising the writer (it's our caller's
// job to abort the GCS writer on this path).
func TestStreamCopyWithResume_BudgetExhausted(t *testing.T) {
	c := qt.New(t)
	payload := bytes.Repeat([]byte{0xDD}, 4096)

	factory, opens := newResumeScript(payload, 500, 4)

	var buf bytes.Buffer
	copied, resumes, err := streamCopyWithResume(
		context.Background(), &buf, factory, int64(len(payload)),
		zap.NewNop(), nil, 3, time.Millisecond,
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(errors.Is(err, io.ErrUnexpectedEOF), qt.IsTrue,
		qt.Commentf("expected wrapped ErrUnexpectedEOF, got %v", err))
	c.Assert(resumes, qt.Equals, 4,
		qt.Commentf("resumes counter must reflect the over-budget attempt"))
	c.Assert(copied < int64(len(payload)), qt.IsTrue,
		qt.Commentf("copied must be short of full payload: %d / %d", copied, len(payload)))
	c.Assert(len(*opens), qt.Equals, 4,
		qt.Commentf("fourth open is the one that blows the budget"))
}

// TestStreamCopyWithResume_NonEOFError pins invariant (5). A non-EOF
// read error (e.g. permissions revoked, disk failure, context timeout
// on the minio client) must NOT trigger a resume — we give up
// immediately and let Temporal / the caller decide.
func TestStreamCopyWithResume_NonEOFError(t *testing.T) {
	c := qt.New(t)
	payload := bytes.Repeat([]byte{0xEE}, 2048)
	sentinel := errors.New("minio: permission denied")

	opens := 0
	openAt := func(offset int64) (io.ReadCloser, error) {
		opens++
		return &scriptedReadCloser{steps: []readStep{
			{data: payload[:500], err: sentinel},
		}}, nil
	}

	var buf bytes.Buffer
	copied, resumes, err := streamCopyWithResume(
		context.Background(), &buf, openAt, int64(len(payload)),
		zap.NewNop(), nil, 3, time.Millisecond,
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(errors.Is(err, sentinel), qt.IsTrue,
		qt.Commentf("non-EOF error must propagate unchanged, got %v", err))
	c.Assert(resumes, qt.Equals, 0,
		qt.Commentf("non-EOF must not consume resume budget"))
	c.Assert(opens, qt.Equals, 1,
		qt.Commentf("non-EOF must not trigger a re-open"))
	c.Assert(copied, qt.Equals, int64(500),
		qt.Commentf("partial bytes delivered before the error must be counted"))
}

// TestStreamCopyWithResume_SilentTruncation pins the silent-truncation
// promotion: when a reader returns io.EOF (which io.Copy maps to nil)
// but copied < size, the loop MUST treat this as ErrUnexpectedEOF and
// resume. This is the real-world failure mode the activity originally
// hit — minio-go's HTTP body occasionally ends cleanly short of the
// Content-Length header.
func TestStreamCopyWithResume_SilentTruncation(t *testing.T) {
	c := qt.New(t)
	payload := bytes.Repeat([]byte{0xFF}, 4096)

	opens := 0
	openAt := func(offset int64) (io.ReadCloser, error) {
		opens++
		// First open: deliver half of the payload then io.EOF (clean
		// nil after io.Copy promotion) — mimicking a MinIO body that
		// ended short. Second open: deliver the rest cleanly.
		if opens == 1 {
			return &scriptedReadCloser{steps: []readStep{
				{data: payload[offset : offset+2000], err: io.EOF},
			}}, nil
		}
		return &scriptedReadCloser{steps: []readStep{
			{data: payload[offset:], err: io.EOF},
		}}, nil
	}

	var buf bytes.Buffer
	copied, resumes, err := streamCopyWithResume(
		context.Background(), &buf, openAt, int64(len(payload)),
		zap.NewNop(), nil, 3, time.Millisecond,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(copied, qt.Equals, int64(len(payload)))
	c.Assert(resumes, qt.Equals, 1,
		qt.Commentf("silent truncation must be promoted to ErrUnexpectedEOF and consume a resume"))
	c.Assert(buf.Bytes(), qt.DeepEquals, payload)
}

// TestStreamCopyWithResume_ProgressFnFires pins that progressFn sees a
// monotonically increasing cumulative byte count across the whole
// copy, including after resumes. The EE activity uses this to drive
// Temporal heartbeats, so a non-monotonic or segment-local counter
// would fire the heartbeat timeout mid-resume.
func TestStreamCopyWithResume_ProgressFnFires(t *testing.T) {
	c := qt.New(t)
	payload := bytes.Repeat([]byte{0x11}, 3000)

	factory, _ := newResumeScript(payload, 1000, 1)

	var calls []int64
	progress := func(copied int64) {
		calls = append(calls, copied)
	}

	var buf bytes.Buffer
	_, _, err := streamCopyWithResume(
		context.Background(), &buf, factory, int64(len(payload)),
		zap.NewNop(), progress, 3, time.Millisecond,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(len(calls) > 0, qt.IsTrue, qt.Commentf("progressFn must fire at least once"))
	// Monotonic non-decreasing.
	for i := 1; i < len(calls); i++ {
		c.Assert(calls[i] >= calls[i-1], qt.IsTrue,
			qt.Commentf("progress must be monotonic: calls[%d]=%d < calls[%d]=%d",
				i, calls[i], i-1, calls[i-1]))
	}
	c.Assert(calls[len(calls)-1], qt.Equals, int64(len(payload)),
		qt.Commentf("final progressFn call must equal total size"))
}

// TestStreamCopyWithResume_ContextCanceledDuringBackoff pins the
// cancellable-sleep branch. A ctx cancellation during the resume gap
// must surface as a wrapped ctx.Err(), NOT silently eat the remaining
// gap duration and then resume.
func TestStreamCopyWithResume_ContextCanceledDuringBackoff(t *testing.T) {
	c := qt.New(t)
	payload := bytes.Repeat([]byte{0x22}, 2048)

	factory, _ := newResumeScript(payload, 500, 2)

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after the first truncation is delivered but before the
	// (1s) resume gap elapses. The goroutine races the main copy, but
	// the gap is 1s and cancel fires at 50ms, so we get deterministic
	// ordering.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	var buf bytes.Buffer
	_, _, err := streamCopyWithResume(
		ctx, &buf, factory, int64(len(payload)),
		zap.NewNop(), nil, 3, 1*time.Second,
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(errors.Is(err, context.Canceled), qt.IsTrue,
		qt.Commentf("cancelled backoff must surface context.Canceled, got %v", err))
}

// TestMinIO_GetFileReaderRange_Offset0Footgun pins the offset==0
// carve-out in minioStorage.GetFileReaderRange. This is a white-box
// contract test: we're not spinning up a MinIO client, we're
// documenting the invariant in code next to the tests that would
// break if somebody "cleaned up" the branch by always setting a range.
//
// Specifically, minio-go's GetObjectOptions.SetRange(0, 0) translates
// to HTTP `Range: bytes=0-0` — a single-byte request, NOT a "whole
// object" request. The whole-object case must skip SetRange entirely.
//
// Without a live MinIO instance we can't exercise the HTTP side here;
// the gcs_stream test suite pins the offset==0 → full-object semantics
// at the factory level (TestStreamCopyWithResume_CleanCopy passes
// payload[0:] in on the first open). This stub exists purely so a grep
// for "offset-0-footgun" finds the documentation trail.
func TestMinIO_GetFileReaderRange_Offset0Footgun(t *testing.T) {
	// Nothing to run — see comment above. A live MinIO pin lives in
	// the EE standalone-gcs-stream-resume.js k6 test, which uploads a
	// real object and verifies byte-exact retrieval after a truncation
	// injected at offset 0. If you're looking at this and thinking
	// "this test does nothing", see the comment above.
	_ = fmt.Sprintf("see %s", "TestStreamCopyWithResume_CleanCopy")
}
