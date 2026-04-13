package worker

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gojuno/minimock/v3"
	"go.uber.org/zap"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/ai"
	"github.com/instill-ai/artifact-backend/pkg/worker/mock"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
)

func TestIsRateLimited(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"RESOURCE_EXHAUSTED", fmt.Errorf("Error 429, Status: RESOURCE_EXHAUSTED"), true},
		{"429 status code", fmt.Errorf("Error 429, Message: Rate limit exceeded"), true},
		{"DEADLINE_EXCEEDED", fmt.Errorf("Error 504, Status: DEADLINE_EXCEEDED"), false},
		{"generic error", fmt.Errorf("some other error"), false},
		{"RESOURCE_EXHAUSTED in wrapped", fmt.Errorf("failed: %w", fmt.Errorf("RESOURCE_EXHAUSTED")), true},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(isRateLimited(tt.err), qt.Equals, tt.expected)
		})
	}
}

func TestIsCacheExpired(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"cache expired message", fmt.Errorf("Cache content 12345 is expired."), true},
		{"not expired", fmt.Errorf("Error 504, DEADLINE_EXCEEDED"), false},
		{"wrapped expired", fmt.Errorf("failed: %w", fmt.Errorf("content is expired")), true},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(isCacheExpired(tt.err), qt.Equals, tt.expected)
		})
	}
}

func TestIsDeadlineExceeded(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"DEADLINE_EXCEEDED", fmt.Errorf("Status: DEADLINE_EXCEEDED"), true},
		{"504 code", fmt.Errorf("Error 504"), true},
		{"context deadline exceeded", fmt.Errorf("context deadline exceeded"), true},
		{"429 rate limit", fmt.Errorf("Error 429"), false},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(isDeadlineExceeded(tt.err), qt.Equals, tt.expected)
		})
	}
}

func TestIsTransientError(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"DEADLINE_EXCEEDED", fmt.Errorf("DEADLINE_EXCEEDED"), true},
		{"RESOURCE_EXHAUSTED", fmt.Errorf("RESOURCE_EXHAUSTED"), true},
		{"cache expired", fmt.Errorf("is expired"), true},
		{"CANCELLED", fmt.Errorf("CANCELLED"), true},
		{"UNAVAILABLE", fmt.Errorf("UNAVAILABLE"), true},
		{"503", fmt.Errorf("Error 503"), true},
		{"429", fmt.Errorf("Error 429"), true},
		{"499", fmt.Errorf("Error 499"), true},
		{"permanent error", fmt.Errorf("file not found"), false},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(isTransientError(tt.err), qt.Equals, tt.expected)
		})
	}
}

func TestMergeHTMLTables_EmptyInput(t *testing.T) {
	c := qt.New(t)
	c.Assert(mergeHTMLTables(nil), qt.Equals, "")
	c.Assert(mergeHTMLTables([]string{}), qt.Equals, "")
}

func TestMergeHTMLTables_SingleBatch(t *testing.T) {
	c := qt.New(t)
	input := []string{"<p>Hello world</p>"}
	c.Assert(mergeHTMLTables(input), qt.Equals, "<p>Hello world</p>")
}

func TestMergeHTMLTables_NoCrossBoundaryTable(t *testing.T) {
	c := qt.New(t)

	batches := []string{
		"<table><tr><td>A</td></tr></table>\n\nSome text",
		"More text\n\n<table><tr><td>B</td></tr></table>",
	}
	result := mergeHTMLTables(batches)

	c.Assert(result, qt.Contains, "</table>")
	c.Assert(result, qt.Contains, "Some text")
	c.Assert(result, qt.Contains, "More text")
}

func TestMergeHTMLTables_CrossBoundaryTableMerge(t *testing.T) {
	c := qt.New(t)

	batches := []string{
		"<p>Intro</p>\n<table><tr><td>Row1</td></tr>",
		"<table><tr><td>Row2</td></tr></table>\n<p>End</p>",
	}
	result := mergeHTMLTables(batches)

	// The unclosed <table> in batch 0 and the duplicate <table> open in batch 1
	// should be merged: batch 1's <table> tag should be stripped.
	c.Assert(result, qt.Not(qt.Contains), "</table>\n\n<table>")
	c.Assert(result, qt.Contains, "Row1")
	c.Assert(result, qt.Contains, "Row2")
	c.Assert(result, qt.Contains, "</table>")
}

func TestMergeHTMLTables_MultipleBatches(t *testing.T) {
	c := qt.New(t)

	batches := []string{
		"<p>Page 1</p>",
		"<p>Page 2</p>",
		"<p>Page 3</p>",
	}
	result := mergeHTMLTables(batches)

	c.Assert(result, qt.Contains, "Page 1")
	c.Assert(result, qt.Contains, "Page 2")
	c.Assert(result, qt.Contains, "Page 3")
}

func TestMergeHTMLTables_ContinuationWithTR(t *testing.T) {
	c := qt.New(t)

	batches := []string{
		"<table><thead><tr><th>Header</th></tr></thead><tbody><tr><td>Row1</td></tr>",
		"<tr><td>Row2</td></tr></tbody></table>",
	}
	result := mergeHTMLTables(batches)

	// Batch 0 has unclosed table, batch 1 starts with <tr> (not <table>),
	// so they should be joined with a single newline (not double)
	c.Assert(result, qt.Contains, "Row1")
	c.Assert(result, qt.Contains, "Row2")
	c.Assert(result, qt.Contains, "</table>")
}

// ===== Page Tag Validation Tests =====

func TestCountPresentPageTags(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		markdown string
		start    int
		end      int
		expected map[int]bool
	}{
		{
			name:     "all tags present",
			markdown: "[Page: 1]\nContent 1\n\n[Page: 2]\nContent 2\n\n[Page: 3]\nContent 3",
			start:    1,
			end:      3,
			expected: map[int]bool{1: true, 2: true, 3: true},
		},
		{
			name:     "some tags missing",
			markdown: "[Page: 1]\nContent 1\n\nContent 2 without tag\n\n[Page: 3]\nContent 3",
			start:    1,
			end:      3,
			expected: map[int]bool{1: true, 3: true},
		},
		{
			name:     "no tags present",
			markdown: "Just plain content\nwith no page tags",
			start:    1,
			end:      3,
			expected: map[int]bool{},
		},
		{
			name:     "tags outside requested range are ignored",
			markdown: "[Page: 1]\nContent 1\n\n[Page: 5]\nContent 5\n\n[Page: 10]\nContent 10",
			start:    3,
			end:      7,
			expected: map[int]bool{5: true},
		},
		{
			name:     "empty markdown",
			markdown: "",
			start:    1,
			end:      5,
			expected: map[int]bool{},
		},
		{
			name:     "single page",
			markdown: "[Page: 42]\nSome content on page 42",
			start:    42,
			end:      42,
			expected: map[int]bool{42: true},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			found := countPresentPageTags(tt.markdown, tt.start, tt.end)
			c.Assert(found, qt.DeepEquals, tt.expected)
		})
	}
}

func TestConvertChunkAdaptive_AllTagsPresent(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	allPagesMarkdown := "[Page: 1]\nContent 1\n\n[Page: 2]\nContent 2\n\n[Page: 3]\nContent 3"

	mockAI := mock.NewClientMock(mc)
	mockAI.ConvertToMarkdownWithCacheMock.Return(&ai.ConversionResult{
		Markdown: allPagesMarkdown,
	}, nil)

	w := &Worker{aiClient: mockAI, log: zap.NewNop()}

	acc := &usageAccumulator{}
	result, err := w.convertChunkAdaptive(context.Background(), "cache-123", 1, 3, 10, "prompt", acc, ChunkConversionTimeout, artifactpb.File_TYPE_PDF)
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.Equals, allPagesMarkdown)

	c.Assert(mockAI.ConvertToMarkdownWithCacheAfterCounter(), qt.Equals, uint64(1))
}

func TestConvertChunkAdaptive_MissingPageTags_SplitsAndRetries(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	var callCount atomic.Int64
	mockAI := mock.NewClientMock(mc)
	mockAI.ConvertToMarkdownWithCacheMock.Set(func(_ context.Context, _ string, prompt string) (*ai.ConversionResult, error) {
		n := callCount.Add(1)
		switch n {
		case 1:
			// First call: pages 1-4, return with page 2 missing
			return &ai.ConversionResult{
				Markdown: "[Page: 1]\nContent 1\n\n[Page: 3]\nContent 3\n\n[Page: 4]\nContent 4",
			}, nil
		case 2:
			// Split left half: pages 1-2
			return &ai.ConversionResult{
				Markdown: "[Page: 1]\nContent 1\n\n[Page: 2]\nContent 2",
			}, nil
		case 3:
			// Split right half: pages 3-4
			return &ai.ConversionResult{
				Markdown: "[Page: 3]\nContent 3\n\n[Page: 4]\nContent 4",
			}, nil
		default:
			return &ai.ConversionResult{Markdown: ""}, nil
		}
	})

	w := &Worker{aiClient: mockAI, log: zap.NewNop()}

	acc := &usageAccumulator{}
	result, err := w.convertChunkAdaptive(context.Background(), "cache-123", 1, 4, 10, "prompt", acc, ChunkConversionTimeout, artifactpb.File_TYPE_PDF)
	c.Assert(err, qt.IsNil)

	// All 4 page tags should now be present in the final output
	found := countPresentPageTags(result, 1, 4)
	c.Assert(len(found), qt.Equals, 4)
	for p := 1; p <= 4; p++ {
		c.Assert(found[p], qt.IsTrue, qt.Commentf("missing [Page: %d]", p))
	}

	// Should have been called 3 times: initial + 2 halves
	c.Assert(callCount.Load(), qt.Equals, int64(3))
}

func TestConvertChunkAdaptive_SinglePageTagInjection(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	mockAI := mock.NewClientMock(mc)
	mockAI.ConvertToMarkdownWithCacheMock.Return(&ai.ConversionResult{
		Markdown: "Content without any page tag",
	}, nil)

	w := &Worker{aiClient: mockAI, log: zap.NewNop()}

	result, err := w.convertChunkAdaptive(context.Background(), "cache-123", 7, 7, 10, "prompt", nil, ChunkConversionTimeout, artifactpb.File_TYPE_PDF)
	c.Assert(err, qt.IsNil)

	c.Assert(strings.HasPrefix(result, "[Page: 7]\n"), qt.IsTrue,
		qt.Commentf("Expected [Page: 7] prefix, got: %s", result[:min(50, len(result))]))
	c.Assert(strings.Contains(result, "Content without any page tag"), qt.IsTrue)
}

func TestConvertChunkAdaptive_SinglePageTagAlreadyPresent(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	mockAI := mock.NewClientMock(mc)
	mockAI.ConvertToMarkdownWithCacheMock.Return(&ai.ConversionResult{
		Markdown: "[Page: 7]\nContent with existing tag",
	}, nil)

	w := &Worker{aiClient: mockAI, log: zap.NewNop()}

	result, err := w.convertChunkAdaptive(context.Background(), "cache-123", 7, 7, 10, "prompt", nil, ChunkConversionTimeout, artifactpb.File_TYPE_PDF)
	c.Assert(err, qt.IsNil)

	c.Assert(result, qt.Equals, "[Page: 7]\nContent with existing tag")
	// No double injection
	c.Assert(strings.Count(result, "[Page: 7]"), qt.Equals, 1)
}

// ===== Usage Accumulator Tests =====

func TestUsageAccumulator_Add(t *testing.T) {
	c := qt.New(t)

	acc := &usageAccumulator{}

	// Simulate Gemini UsageMetadata (matches genai.GenerateContentResponseUsageMetadata JSON fields)
	usage1 := map[string]interface{}{
		"promptTokenCount":        int32(100),
		"candidatesTokenCount":    int32(200),
		"totalTokenCount":         int32(300),
		"cachedContentTokenCount": int32(50),
	}
	acc.Add(usage1, "gemini-2.0-flash-001")
	c.Assert(acc.PromptTokenCount, qt.Equals, int64(100))
	c.Assert(acc.CandidatesTokenCount, qt.Equals, int64(200))
	c.Assert(acc.TotalTokenCount, qt.Equals, int64(300))
	c.Assert(acc.CachedContentTokenCount, qt.Equals, int64(50))
	c.Assert(acc.Model, qt.Equals, "gemini-2.0-flash-001")
	c.Assert(acc.CallCount, qt.Equals, 1)

	// Second call accumulates
	usage2 := map[string]interface{}{
		"promptTokenCount":        int32(150),
		"candidatesTokenCount":    int32(250),
		"totalTokenCount":         int32(400),
		"cachedContentTokenCount": int32(75),
	}
	acc.Add(usage2, "gemini-2.0-flash-001")
	c.Assert(acc.PromptTokenCount, qt.Equals, int64(250))
	c.Assert(acc.CandidatesTokenCount, qt.Equals, int64(450))
	c.Assert(acc.TotalTokenCount, qt.Equals, int64(700))
	c.Assert(acc.CachedContentTokenCount, qt.Equals, int64(125))
	c.Assert(acc.CallCount, qt.Equals, 2)
}

func TestUsageAccumulator_AddNil(t *testing.T) {
	c := qt.New(t)

	acc := &usageAccumulator{}
	acc.Add(nil, "model")
	c.Assert(acc.CallCount, qt.Equals, 0)

	var nilAcc *usageAccumulator
	nilAcc.Add(map[string]interface{}{"totalTokenCount": 100}, "model")
	// Should not panic
}

func TestUsageAccumulator_ToMap(t *testing.T) {
	c := qt.New(t)

	acc := &usageAccumulator{
		PromptTokenCount:        500,
		CandidatesTokenCount:    1000,
		TotalTokenCount:         1500,
		CachedContentTokenCount: 200,
		CallCount:               3,
	}
	m := acc.ToMap()
	c.Assert(m["promptTokenCount"], qt.Equals, int64(500))
	c.Assert(m["candidatesTokenCount"], qt.Equals, int64(1000))
	c.Assert(m["totalTokenCount"], qt.Equals, int64(1500))
	c.Assert(m["cachedContentTokenCount"], qt.Equals, int64(200))
	c.Assert(m["callCount"], qt.Equals, 3)
}

func TestUsageAccumulator_ToMapNil(t *testing.T) {
	c := qt.New(t)

	var nilAcc *usageAccumulator
	c.Assert(nilAcc.ToMap(), qt.IsNil)
}

func TestUsageAccumulator_WiredThroughConvertChunk(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	mockAI := mock.NewClientMock(mc)
	var callCount atomic.Int64
	mockAI.ConvertToMarkdownWithCacheMock.Set(func(_ context.Context, _ string, _ string) (*ai.ConversionResult, error) {
		n := callCount.Add(1)
		return &ai.ConversionResult{
			Markdown: fmt.Sprintf("[Page: %d]\nContent %d", n, n),
			Model:    "gemini-2.0-flash-001",
			UsageMetadata: map[string]interface{}{
				"promptTokenCount":     int32(100),
				"candidatesTokenCount": int32(200),
				"totalTokenCount":      int32(300),
			},
		}, nil
	})

	w := &Worker{aiClient: mockAI, log: zap.NewNop()}
	acc := &usageAccumulator{}

	// Convert 2 pages individually
	_, err := w.convertChunk(context.Background(), "cache", 1, 1, 2, "prompt", acc, ChunkConversionTimeout, artifactpb.File_TYPE_PDF)
	c.Assert(err, qt.IsNil)
	_, err = w.convertChunk(context.Background(), "cache", 2, 2, 2, "prompt", acc, ChunkConversionTimeout, artifactpb.File_TYPE_PDF)
	c.Assert(err, qt.IsNil)

	c.Assert(acc.CallCount, qt.Equals, 2)
	c.Assert(acc.PromptTokenCount, qt.Equals, int64(200))
	c.Assert(acc.CandidatesTokenCount, qt.Equals, int64(400))
	c.Assert(acc.TotalTokenCount, qt.Equals, int64(600))
	c.Assert(acc.Model, qt.Equals, "gemini-2.0-flash-001")
}

func TestAggregateBatchUsage(t *testing.T) {
	c := qt.New(t)

	batches := map[int]map[string]interface{}{
		0: {
			"promptTokenCount":        int64(100),
			"candidatesTokenCount":    int64(200),
			"totalTokenCount":         int64(300),
			"cachedContentTokenCount": int64(50),
			"callCount":               2,
		},
		1: {
			"promptTokenCount":        int64(150),
			"candidatesTokenCount":    int64(250),
			"totalTokenCount":         int64(400),
			"cachedContentTokenCount": int64(75),
			"callCount":               3,
		},
	}

	result := aggregateBatchUsage(batches)
	c.Assert(result["promptTokenCount"], qt.Equals, int64(250))
	c.Assert(result["candidatesTokenCount"], qt.Equals, int64(450))
	c.Assert(result["totalTokenCount"], qt.Equals, int64(700))
	c.Assert(result["cachedContentTokenCount"], qt.Equals, int64(125))
	c.Assert(result["callCount"], qt.Equals, int64(5))
}

func TestAggregateBatchUsage_Empty(t *testing.T) {
	c := qt.New(t)
	c.Assert(aggregateBatchUsage(nil), qt.IsNil)
	c.Assert(aggregateBatchUsage(map[int]map[string]interface{}{}), qt.IsNil)
}

// ===== BatchProfile Tests =====

func TestBatchProfile_DocumentDefaults(t *testing.T) {
	c := qt.New(t)

	docTypes := []artifactpb.File_Type{
		artifactpb.File_TYPE_PDF,
		artifactpb.File_TYPE_DOCX,
		artifactpb.File_TYPE_PPTX,
		artifactpb.File_TYPE_HTML,
		artifactpb.File_TYPE_CSV,
		artifactpb.File_TYPE_PNG,
		artifactpb.File_TYPE_UNSPECIFIED,
	}

	for _, ft := range docTypes {
		p := batchProfile(ft)
		c.Assert(p.PagesPerBatch, qt.Equals, 3, qt.Commentf("type=%s", ft))
		c.Assert(p.PagesPerChunk, qt.Equals, 3, qt.Commentf("type=%s", ft))
		c.Assert(p.ChunkTimeout, qt.Equals, ChunkConversionTimeout, qt.Commentf("type=%s", ft))
		c.Assert(p.ActivityTimeout, qt.Equals, 15*time.Minute, qt.Commentf("type=%s", ft))
		c.Assert(p.DirectMaxPages, qt.Equals, 10, qt.Commentf("type=%s", ft))
		c.Assert(p.MaxConcurrentBatches, qt.Equals, 16, qt.Commentf("type=%s", ft))
		c.Assert(p.SegmentDuration, qt.Equals, time.Duration(0), qt.Commentf("type=%s", ft))
		c.Assert(p.DirectMaxDuration, qt.Equals, time.Duration(0), qt.Commentf("type=%s", ft))
		c.Assert(isMediaFileType(ft), qt.IsFalse, qt.Commentf("type=%s", ft))
	}
}

func TestBatchProfile_VideoTypes(t *testing.T) {
	c := qt.New(t)

	videoTypes := []artifactpb.File_Type{
		artifactpb.File_TYPE_MP4,
		artifactpb.File_TYPE_AVI,
		artifactpb.File_TYPE_MOV,
		artifactpb.File_TYPE_MKV,
		artifactpb.File_TYPE_FLV,
		artifactpb.File_TYPE_WMV,
		artifactpb.File_TYPE_MPEG,
		artifactpb.File_TYPE_WEBM_VIDEO,
	}

	for _, ft := range videoTypes {
		p := batchProfile(ft)
		c.Assert(p.PagesPerBatch, qt.Equals, 3, qt.Commentf("type=%s", ft))
		c.Assert(p.PagesPerChunk, qt.Equals, 3, qt.Commentf("type=%s", ft))
		c.Assert(p.ChunkTimeout, qt.Equals, 5*time.Minute, qt.Commentf("type=%s", ft))
		c.Assert(p.ActivityTimeout, qt.Equals, 20*time.Minute, qt.Commentf("type=%s", ft))
		c.Assert(p.DirectMaxPages, qt.Equals, 5, qt.Commentf("type=%s", ft))
		c.Assert(p.MaxConcurrentBatches, qt.Equals, 32, qt.Commentf("type=%s", ft))
		c.Assert(p.SegmentDuration, qt.Equals, 2*time.Minute, qt.Commentf("type=%s", ft))
		c.Assert(p.DirectMaxDuration, qt.Equals, 10*time.Minute, qt.Commentf("type=%s", ft))
		c.Assert(isMediaFileType(ft), qt.IsTrue, qt.Commentf("type=%s", ft))
	}
}

func TestBatchProfile_AudioTypes(t *testing.T) {
	c := qt.New(t)

	audioTypes := []artifactpb.File_Type{
		artifactpb.File_TYPE_MP3,
		artifactpb.File_TYPE_WAV,
		artifactpb.File_TYPE_AAC,
		artifactpb.File_TYPE_OGG,
		artifactpb.File_TYPE_FLAC,
		artifactpb.File_TYPE_M4A,
		artifactpb.File_TYPE_WMA,
		artifactpb.File_TYPE_AIFF,
		artifactpb.File_TYPE_WEBM_AUDIO,
	}

	for _, ft := range audioTypes {
		p := batchProfile(ft)
		c.Assert(p.PagesPerBatch, qt.Equals, 3, qt.Commentf("type=%s", ft))
		c.Assert(p.PagesPerChunk, qt.Equals, 3, qt.Commentf("type=%s", ft))
		c.Assert(p.ChunkTimeout, qt.Equals, 5*time.Minute, qt.Commentf("type=%s", ft))
		c.Assert(p.ActivityTimeout, qt.Equals, 20*time.Minute, qt.Commentf("type=%s", ft))
		c.Assert(p.DirectMaxPages, qt.Equals, 5, qt.Commentf("type=%s", ft))
		c.Assert(p.MaxConcurrentBatches, qt.Equals, 32, qt.Commentf("type=%s", ft))
		c.Assert(p.SegmentDuration, qt.Equals, 2*time.Minute, qt.Commentf("type=%s", ft))
		c.Assert(p.DirectMaxDuration, qt.Equals, 10*time.Minute, qt.Commentf("type=%s", ft))
		c.Assert(isMediaFileType(ft), qt.IsTrue, qt.Commentf("type=%s", ft))
	}
}

func TestIsMediaFileType(t *testing.T) {
	c := qt.New(t)

	c.Assert(isMediaFileType(artifactpb.File_TYPE_MP4), qt.IsTrue)
	c.Assert(isMediaFileType(artifactpb.File_TYPE_WEBM_VIDEO), qt.IsTrue)
	c.Assert(isMediaFileType(artifactpb.File_TYPE_MP3), qt.IsTrue)
	c.Assert(isMediaFileType(artifactpb.File_TYPE_WEBM_AUDIO), qt.IsTrue)
	c.Assert(isMediaFileType(artifactpb.File_TYPE_PDF), qt.IsFalse)
	c.Assert(isMediaFileType(artifactpb.File_TYPE_DOCX), qt.IsFalse)
	c.Assert(isMediaFileType(artifactpb.File_TYPE_UNSPECIFIED), qt.IsFalse)
}

func TestFormatTimestamp(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		d        time.Duration
		expected string
	}{
		{"zero", 0, "00:00:00"},
		{"30 seconds", 30 * time.Second, "00:00:30"},
		{"5 minutes", 5 * time.Minute, "00:05:00"},
		{"1 hour 30 min 45 sec", time.Hour + 30*time.Minute + 45*time.Second, "01:30:45"},
		{"2 hours 28 min 55 sec", 2*time.Hour + 28*time.Minute + 55*time.Second, "02:28:55"},
		{"10 hours", 10 * time.Hour, "10:00:00"},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(formatTimestamp(tt.d), qt.Equals, tt.expected)
		})
	}
}

func TestConvertTimeRange(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	mockAI := mock.NewClientMock(mc)
	mockAI.ConvertToMarkdownWithCacheMock.Set(func(_ context.Context, _ string, prompt string) (*ai.ConversionResult, error) {
		c.Assert(strings.Contains(prompt, "00:05:00"), qt.IsTrue, qt.Commentf("prompt should contain start timestamp"))
		c.Assert(strings.Contains(prompt, "00:10:00"), qt.IsTrue, qt.Commentf("prompt should contain end timestamp"))
		// Media prompts should NOT ask for [Page: N] tags
		c.Assert(strings.Contains(prompt, "[Page:"), qt.IsFalse, qt.Commentf("prompt must not contain [Page:] instruction"))
		return &ai.ConversionResult{
			Markdown: "[Audio: 00:05:03] Speaker: Hello\n\n[Audio: 00:09:55] Speaker: Goodbye",
			Model:    "gemini-2.0-flash-001",
			UsageMetadata: map[string]interface{}{
				"totalTokenCount": int32(500),
			},
		}, nil
	})

	w := &Worker{aiClient: mockAI, log: zap.NewNop()}
	acc := &usageAccumulator{}

	result, err := w.convertTimeRange(context.Background(), "cache-123",
		5*time.Minute, 10*time.Minute, 5*time.Minute, 2, "base prompt", acc, 5*time.Minute, artifactpb.File_TYPE_MP4, false, "")
	c.Assert(err, qt.IsNil)
	// No [Page: N] tags in output
	c.Assert(strings.Contains(result, "[Page:"), qt.IsFalse)
	c.Assert(strings.Contains(result, "[Audio: 00:05:03]"), qt.IsTrue)
	c.Assert(acc.CallCount, qt.Equals, 1)
}

func TestConvertTimeRange_StripsPageTagsFromModel(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	mockAI := mock.NewClientMock(mc)
	mockAI.ConvertToMarkdownWithCacheMock.Return(&ai.ConversionResult{
		Markdown: "[Page: 1]\n[Audio: 00:00:05] Content with model-emitted page tag",
	}, nil)

	w := &Worker{aiClient: mockAI, log: zap.NewNop()}
	acc := &usageAccumulator{}

	result, err := w.convertTimeRange(context.Background(), "cache-123",
		0, 5*time.Minute, 0, 1, "prompt", acc, 5*time.Minute, artifactpb.File_TYPE_MP4, false, "")
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Contains(result, "[Page:"), qt.IsFalse,
		qt.Commentf("Page tags should be stripped, got: %s", result))
	c.Assert(strings.Contains(result, "[Audio: 00:00:05]"), qt.IsTrue)
}

func TestConvertTimeRange_TruncatesContentBeyondEndTime(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	// Model ignores time range and returns full transcript
	mockAI := mock.NewClientMock(mc)
	mockAI.ConvertToMarkdownWithCacheMock.Return(&ai.ConversionResult{
		Markdown: "[Audio: 00:00:05] First line\n[Audio: 00:03:00] Middle line\n[Audio: 00:04:59] Near boundary\n[Audio: 00:07:00] Beyond end time\n[Audio: 00:15:00] Way beyond",
	}, nil)

	w := &Worker{aiClient: mockAI, log: zap.NewNop()}
	acc := &usageAccumulator{}

	result, err := w.convertTimeRange(context.Background(), "cache-123",
		0, 5*time.Minute, 0, 1, "prompt", acc, 5*time.Minute, artifactpb.File_TYPE_MP4, false, "")
	c.Assert(err, qt.IsNil)
	// Content within range should be kept
	c.Assert(strings.Contains(result, "First line"), qt.IsTrue)
	c.Assert(strings.Contains(result, "Near boundary"), qt.IsTrue)
	// Content well beyond 5m + 30s tolerance should be truncated
	c.Assert(strings.Contains(result, "Way beyond"), qt.IsFalse)
}

func TestConvertTimeRangeRobust_RetriesTransient(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	var callCount atomic.Int64
	mockAI := mock.NewClientMock(mc)
	mockAI.ConvertToMarkdownWithCacheMock.Set(func(_ context.Context, _ string, _ string) (*ai.ConversionResult, error) {
		n := callCount.Add(1)
		if n <= 2 {
			return nil, fmt.Errorf("Error 429, RESOURCE_EXHAUSTED")
		}
		return &ai.ConversionResult{
			Markdown: "[Audio: 00:00:05] Success after retries",
		}, nil
	})

	w := &Worker{aiClient: mockAI, log: zap.NewNop()}
	acc := &usageAccumulator{}

	result, err := w.convertTimeRangeRobust(context.Background(), "cache-123",
		0, 5*time.Minute, 0, 1, "prompt", acc, 5*time.Minute, artifactpb.File_TYPE_MP4, false, "")
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Contains(result, "Success after retries"), qt.IsTrue)
	c.Assert(callCount.Load(), qt.Equals, int64(3))
}

func TestConvertTimeRangeRobust_PropagatesCacheExpired(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(t)

	mockAI := mock.NewClientMock(mc)
	mockAI.ConvertToMarkdownWithCacheMock.Return(nil, fmt.Errorf("Cache content 12345 is expired."))

	w := &Worker{aiClient: mockAI, log: zap.NewNop()}
	acc := &usageAccumulator{}

	_, err := w.convertTimeRangeRobust(context.Background(), "cache-123",
		0, 5*time.Minute, 0, 1, "prompt", acc, 5*time.Minute, artifactpb.File_TYPE_MP4, false, "")
	c.Assert(err, qt.IsNotNil)
	c.Assert(isCacheExpired(err), qt.IsTrue)
	c.Assert(mockAI.ConvertToMarkdownWithCacheAfterCounter(), qt.Equals, uint64(1))
}

func TestTimeRangeSlotGeneration(t *testing.T) {
	c := qt.New(t)

	segDuration := 5 * time.Minute

	tests := []struct {
		name          string
		totalDuration time.Duration
		expectedSlots int
	}{
		{"exact 5 min", 5 * time.Minute, 1},
		{"exact 10 min", 10 * time.Minute, 2},
		{"2h28m55s", 2*time.Hour + 28*time.Minute + 55*time.Second, 30},
		{"1 second", 1 * time.Second, 1},
		{"4m59s", 4*time.Minute + 59*time.Second, 1},
		{"5m01s", 5*time.Minute + 1*time.Second, 2},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			segCount := 0
			for start := time.Duration(0); start < tt.totalDuration; start += segDuration {
				segCount++
			}
			c.Assert(segCount, qt.Equals, tt.expectedSlots, qt.Commentf("duration=%s", tt.totalDuration))

			// Verify last segment doesn't exceed total duration
			lastStart := time.Duration(segCount-1) * segDuration
			lastEnd := lastStart + segDuration
			if lastEnd > tt.totalDuration {
				lastEnd = tt.totalDuration
			}
			c.Assert(lastEnd <= tt.totalDuration, qt.IsTrue)
		})
	}
}

// ===== Time-range truncation and timestamp parsing tests =====

func TestParseTimestampDuration(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		input    string
		expected time.Duration
		wantErr  bool
	}{
		{"zero", "00:00:00", 0, false},
		{"30 seconds", "00:00:30", 30 * time.Second, false},
		{"5 minutes", "00:05:00", 5 * time.Minute, false},
		{"1h30m45s", "01:30:45", time.Hour + 30*time.Minute + 45*time.Second, false},
		{"MM:SS zero", "00:00", 0, false},
		{"MM:SS 5m30s", "05:30", 5*time.Minute + 30*time.Second, false},
		{"MM:SS 15m41s", "15:41", 15*time.Minute + 41*time.Second, false},
		{"invalid single part", "42", 0, true},
		{"non-numeric", "aa:bb:cc", 0, true},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			d, err := parseTimestampDuration(tt.input)
			if tt.wantErr {
				c.Assert(err, qt.IsNotNil)
			} else {
				c.Assert(err, qt.IsNil)
				c.Assert(d, qt.Equals, tt.expected)
			}
		})
	}
}

func TestClipToTimeRange_NoClippingNeeded(t *testing.T) {
	c := qt.New(t)

	content := "[Audio: 00:00:05] Hello\n[Audio: 00:03:00] Middle\n[Audio: 00:04:50] Near end"
	result, removed := clipToTimeRange(content, 0, 5*time.Minute, defaultEndTolerance)
	c.Assert(removed, qt.Equals, 0)
	c.Assert(result, qt.Equals, content)
}

func TestClipToTimeRange_ClipsEndBoundary(t *testing.T) {
	c := qt.New(t)

	content := "[Audio: 00:00:05] First\n[Audio: 00:03:00] Middle\n[Audio: 00:04:50] Near end\n[Audio: 00:07:00] Beyond end\n[Audio: 00:15:00] Way beyond"
	result, removed := clipToTimeRange(content, 0, 5*time.Minute, defaultEndTolerance)
	c.Assert(removed > 0, qt.IsTrue)
	c.Assert(strings.Contains(result, "First"), qt.IsTrue)
	c.Assert(strings.Contains(result, "Near end"), qt.IsTrue)
	c.Assert(strings.Contains(result, "Way beyond"), qt.IsFalse)
}

func TestClipToTimeRange_ClipsStartBoundary(t *testing.T) {
	c := qt.New(t)

	// Segment 2: 05:00-10:00. Model returned content starting from 00:00.
	content := "[Audio: 00:00:05] Before start\n[Audio: 00:02:00] Still before\n[Audio: 00:04:00] Just before\n[Audio: 00:05:10] In range\n[Audio: 00:08:00] Middle\n[Audio: 00:09:50] Near end"
	result, removed := clipToTimeRange(content, 5*time.Minute, 10*time.Minute, defaultEndTolerance)
	c.Assert(removed > 0, qt.IsTrue)
	c.Assert(strings.Contains(result, "Before start"), qt.IsFalse)
	c.Assert(strings.Contains(result, "Still before"), qt.IsFalse)
	c.Assert(strings.Contains(result, "Just before"), qt.IsFalse)
	c.Assert(strings.Contains(result, "In range"), qt.IsTrue)
	c.Assert(strings.Contains(result, "Near end"), qt.IsTrue)
}

func TestClipToTimeRange_ClipsBothBoundaries(t *testing.T) {
	c := qt.New(t)

	// Model returned the entire file for a middle segment.
	content := "[Audio: 00:00:05] Before\n[Audio: 00:05:10] Start of range\n[Audio: 00:08:00] Middle\n[Audio: 00:12:00] After end\n[Audio: 00:20:00] Way after"
	result, removed := clipToTimeRange(content, 5*time.Minute, 10*time.Minute, defaultEndTolerance)
	c.Assert(removed > 0, qt.IsTrue)
	c.Assert(strings.Contains(result, "Before"), qt.IsFalse)
	c.Assert(strings.Contains(result, "Start of range"), qt.IsTrue)
	c.Assert(strings.Contains(result, "Middle"), qt.IsTrue)
	c.Assert(strings.Contains(result, "After end"), qt.IsFalse)
	c.Assert(strings.Contains(result, "Way after"), qt.IsFalse)
}

func TestClipToTimeRange_RespectsToleranceWindow(t *testing.T) {
	c := qt.New(t)

	// 5:15 is within the 30s tolerance of 5:00 end time — should be kept
	content := "[Audio: 00:04:50] Near end\n[Audio: 00:05:15] Just over boundary"
	result, removed := clipToTimeRange(content, 0, 5*time.Minute, defaultEndTolerance)
	c.Assert(removed, qt.Equals, 0)
	c.Assert(result, qt.Equals, content)
}

func TestClipToTimeRange_StartToleranceKeepsBorderline(t *testing.T) {
	c := qt.New(t)

	// 04:50 is within 15s tolerance of 05:00 start — should be kept
	content := "[Audio: 00:04:50] Borderline\n[Audio: 00:05:10] In range"
	result, removed := clipToTimeRange(content, 5*time.Minute, 10*time.Minute, defaultEndTolerance)
	c.Assert(removed, qt.Equals, 0)
	c.Assert(strings.Contains(result, "Borderline"), qt.IsTrue)
}

func TestClipToTimeRange_NoTimestamps(t *testing.T) {
	c := qt.New(t)

	content := "Plain text without timestamps\nAnother line"
	result, removed := clipToTimeRange(content, 0, 5*time.Minute, defaultEndTolerance)
	c.Assert(removed, qt.Equals, 0)
	c.Assert(result, qt.Equals, content)
}

func TestClipToTimeRange_EmptyInput(t *testing.T) {
	c := qt.New(t)

	result, removed := clipToTimeRange("", 0, 5*time.Minute, defaultEndTolerance)
	c.Assert(removed, qt.Equals, 0)
	c.Assert(result, qt.Equals, "")
}

func TestClipToTimeRange_AllBeyondEnd(t *testing.T) {
	c := qt.New(t)

	content := "[Audio: 00:10:00] First line beyond\n[Audio: 00:15:00] Second line beyond"
	result, removed := clipToTimeRange(content, 0, 5*time.Minute, defaultEndTolerance)
	c.Assert(removed, qt.Equals, 2)
	c.Assert(result, qt.Equals, "")
}

func TestClipToTimeRange_AllBeforeStart(t *testing.T) {
	c := qt.New(t)

	content := "[Audio: 00:00:05] Early\n[Audio: 00:02:00] Still early"
	result, removed := clipToTimeRange(content, 5*time.Minute, 10*time.Minute, defaultEndTolerance)
	c.Assert(removed, qt.Equals, 2)
	c.Assert(result, qt.Equals, "")
}

func TestClipToTimeRange_ZeroStartTolerance(t *testing.T) {
	c := qt.New(t)

	// With zero tolerance, 04:50 (10s before 05:00) must be clipped.
	content := "[Audio: 00:04:50] Lookback context\n[Audio: 00:05:10] Real content"
	result, removed := clipToTimeRange(content, 5*time.Minute, 10*time.Minute, 0)
	c.Assert(removed, qt.Equals, 1)
	c.Assert(strings.Contains(result, "Lookback context"), qt.IsFalse)
	c.Assert(strings.Contains(result, "Real content"), qt.IsTrue)
}

func TestClipToTimeRange_StrictStartWithLookahead(t *testing.T) {
	c := qt.New(t)

	// Simulates the overlap scenario: API window was [1:45, 4:00] but logical
	// ownership starts at 2:00. The model produced accurate timestamps for the
	// lookback region (01:45-01:59) which must be clipped at the boundary.
	content := "[Video: 00:01:45 - 00:02:00] Scene description\n" +
		"[Audio: 00:01:45] Pre-boundary speech A\n" +
		"[Audio: 00:01:50] Pre-boundary speech B\n" +
		"[Audio: 00:01:55] Pre-boundary speech C\n" +
		"[Audio: 00:01:59] Pre-boundary speech D\n" +
		"\n" +
		"[Video: 00:02:00 - 00:02:59] New scene\n" +
		"[Audio: 00:02:00] Post-boundary speech E\n" +
		"[Audio: 00:02:10] Post-boundary speech F\n" +
		"[Audio: 00:02:30] Post-boundary speech G"
	result, removed := clipToTimeRange(content, 2*time.Minute, 4*time.Minute, 0)
	c.Assert(removed > 0, qt.IsTrue)
	c.Assert(strings.Contains(result, "Pre-boundary speech A"), qt.IsFalse)
	c.Assert(strings.Contains(result, "Pre-boundary speech D"), qt.IsFalse)
	c.Assert(strings.Contains(result, "Post-boundary speech E"), qt.IsTrue)
	c.Assert(strings.Contains(result, "Post-boundary speech G"), qt.IsTrue)
}

func TestClipToTimeRange_ZeroToleranceExactBoundary(t *testing.T) {
	c := qt.New(t)

	// Lines at exactly the start time should be kept, even with zero tolerance.
	content := "[Audio: 00:02:00] At boundary\n[Audio: 00:02:15] After boundary"
	result, removed := clipToTimeRange(content, 2*time.Minute, 4*time.Minute, 0)
	c.Assert(removed, qt.Equals, 0)
	c.Assert(strings.Contains(result, "At boundary"), qt.IsTrue)
	c.Assert(strings.Contains(result, "After boundary"), qt.IsTrue)
}

func TestEnforceTimestampMonotonicity_NoRegression(t *testing.T) {
	c := qt.New(t)

	content := "[Audio: 00:00:05] First\n[Audio: 00:03:00] Second\n[Audio: 00:05:00] Third"
	result, removed := enforceTimestampMonotonicity(content)
	c.Assert(removed, qt.Equals, 0)
	c.Assert(result, qt.Equals, content)
}

func TestEnforceTimestampMonotonicity_RemovesDuplicates(t *testing.T) {
	c := qt.New(t)

	// Simulates two overlapping segments assembled together:
	// Segment 1 goes to 05:00, then segment 2 starts at 03:00 (overlap).
	content := "[Audio: 00:00:05] A\n[Audio: 00:03:00] B\n[Audio: 00:05:00] C\n[Audio: 00:03:00] B duplicate\n[Audio: 00:04:00] C duplicate\n[Audio: 00:07:00] D"
	result, removed := enforceTimestampMonotonicity(content)
	c.Assert(removed, qt.Equals, 2)
	c.Assert(strings.Contains(result, "B duplicate"), qt.IsFalse)
	c.Assert(strings.Contains(result, "C duplicate"), qt.IsFalse)
	c.Assert(strings.Contains(result, "[Audio: 00:07:00] D"), qt.IsTrue)
}

func TestEnforceTimestampMonotonicity_PreservesNonTimestampedLines(t *testing.T) {
	c := qt.New(t)

	content := "# Title\n[Audio: 00:05:00] First\nSome description\n[Audio: 00:03:00] Regressed\n[Audio: 00:07:00] Continuing"
	result, removed := enforceTimestampMonotonicity(content)
	c.Assert(removed, qt.Equals, 1)
	c.Assert(strings.Contains(result, "# Title"), qt.IsTrue)
	c.Assert(strings.Contains(result, "Some description"), qt.IsTrue)
	c.Assert(strings.Contains(result, "Regressed"), qt.IsFalse)
	c.Assert(strings.Contains(result, "Continuing"), qt.IsTrue)
}

func TestEnforceTimestampMonotonicity_EmptyInput(t *testing.T) {
	c := qt.New(t)

	result, removed := enforceTimestampMonotonicity("")
	c.Assert(removed, qt.Equals, 0)
	c.Assert(result, qt.Equals, "")
}

// ===== offsetTimestamps Tests =====

func TestOffsetTimestamps_ZeroOffset(t *testing.T) {
	c := qt.New(t)

	input := "[Audio: 00:05:00] Hello\n[Video: 00:10:00] World"
	result := offsetTimestamps(input, 0)
	c.Assert(result, qt.Equals, input)
}

func TestOffsetTimestamps_EmptyInput(t *testing.T) {
	c := qt.New(t)

	c.Assert(offsetTimestamps("", 30*time.Minute), qt.Equals, "")
}

func TestOffsetTimestamps_30MinOffset(t *testing.T) {
	c := qt.New(t)

	input := "[Audio: 00:00:05] First\n[Audio: 00:03:00] Second\n[Audio: 00:04:50] Third"
	result := offsetTimestamps(input, 30*time.Minute)
	c.Assert(strings.Contains(result, "[Audio: 00:30:05]"), qt.IsTrue, qt.Commentf("got: %s", result))
	c.Assert(strings.Contains(result, "[Audio: 00:33:00]"), qt.IsTrue, qt.Commentf("got: %s", result))
	c.Assert(strings.Contains(result, "[Audio: 00:34:50]"), qt.IsTrue, qt.Commentf("got: %s", result))
}

func TestOffsetTimestamps_LargeOffset(t *testing.T) {
	c := qt.New(t)

	input := "[Audio: 00:05:00] Content at 5 min"
	result := offsetTimestamps(input, 2*time.Hour+30*time.Minute)
	c.Assert(strings.Contains(result, "[Audio: 02:35:00]"), qt.IsTrue, qt.Commentf("got: %s", result))
}

func TestOffsetTimestamps_MultipleTagTypes(t *testing.T) {
	c := qt.New(t)

	input := "[Audio: 00:01:00] Audio\n[Video: 00:02:00] Video\n[Sound: 00:03:00] Sound"
	result := offsetTimestamps(input, time.Hour)
	c.Assert(strings.Contains(result, "[Audio: 01:01:00]"), qt.IsTrue, qt.Commentf("got: %s", result))
	c.Assert(strings.Contains(result, "[Video: 01:02:00]"), qt.IsTrue, qt.Commentf("got: %s", result))
	c.Assert(strings.Contains(result, "[Sound: 01:03:00]"), qt.IsTrue, qt.Commentf("got: %s", result))
}

func TestOffsetTimestamps_MixedContent(t *testing.T) {
	c := qt.New(t)

	input := "# Chapter 1\n[Audio: 00:00:10] Introduction\nSome plain text\n[Audio: 00:05:30] Second segment\n\n## Notes\nNo timestamps here"
	result := offsetTimestamps(input, 30*time.Minute)
	c.Assert(strings.Contains(result, "# Chapter 1"), qt.IsTrue)
	c.Assert(strings.Contains(result, "[Audio: 00:30:10]"), qt.IsTrue, qt.Commentf("got: %s", result))
	c.Assert(strings.Contains(result, "Some plain text"), qt.IsTrue)
	c.Assert(strings.Contains(result, "[Audio: 00:35:30]"), qt.IsTrue, qt.Commentf("got: %s", result))
	c.Assert(strings.Contains(result, "No timestamps here"), qt.IsTrue)
}

func TestOffsetTimestamps_PreservesNonTimestampContent(t *testing.T) {
	c := qt.New(t)

	input := "Plain text without any timestamps\nAnother line"
	result := offsetTimestamps(input, 30*time.Minute)
	c.Assert(result, qt.Equals, input)
}

// ===== parseHHMMSS Tests =====

func TestParseHHMMSS_Valid(t *testing.T) {
	c := qt.New(t)

	c.Assert(parseHHMMSS("00:00:00"), qt.Equals, 0)
	c.Assert(parseHHMMSS("00:00:30"), qt.Equals, 30)
	c.Assert(parseHHMMSS("00:05:00"), qt.Equals, 300)
	c.Assert(parseHHMMSS("01:00:00"), qt.Equals, 3600)
	c.Assert(parseHHMMSS("02:30:45"), qt.Equals, 2*3600+30*60+45)
}

func TestParseHHMMSS_InvalidFormats(t *testing.T) {
	c := qt.New(t)

	c.Assert(parseHHMMSS(""), qt.Equals, 0)
	c.Assert(parseHHMMSS("05:00"), qt.Equals, 0)
	c.Assert(parseHHMMSS("not-a-time"), qt.Equals, 0)
}

// ===== splitIntoTimestampedEntries Tests =====

func TestSplitIntoTimestampedEntries_AudioOnly(t *testing.T) {
	c := qt.New(t)

	input := "[Audio: 00:00:05] Hello world\n[Audio: 00:01:00] Second entry"
	entries := splitIntoTimestampedEntries(input)
	c.Assert(len(entries), qt.Equals, 2)
	c.Assert(entries[0].seconds, qt.Equals, 5)
	c.Assert(strings.Contains(entries[0].text, "Hello world"), qt.IsTrue)
	c.Assert(entries[1].seconds, qt.Equals, 60)
	c.Assert(strings.Contains(entries[1].text, "Second entry"), qt.IsTrue)
}

func TestSplitIntoTimestampedEntries_VideoOnly(t *testing.T) {
	c := qt.New(t)

	input := "[Video: 00:00:10] Scene one\n[Video: 00:02:00] Scene two"
	entries := splitIntoTimestampedEntries(input)
	c.Assert(len(entries), qt.Equals, 2)
	c.Assert(entries[0].seconds, qt.Equals, 10)
	c.Assert(entries[1].seconds, qt.Equals, 120)
}

func TestSplitIntoTimestampedEntries_MultilineBlocks(t *testing.T) {
	c := qt.New(t)

	input := "[Audio: 00:00:05] Speaker A says something\nContinuation of speech\n[Audio: 00:01:00] Speaker B replies"
	entries := splitIntoTimestampedEntries(input)
	c.Assert(len(entries), qt.Equals, 2)
	c.Assert(strings.Contains(entries[0].text, "Continuation of speech"), qt.IsTrue)
}

func TestSplitIntoTimestampedEntries_Empty(t *testing.T) {
	c := qt.New(t)

	entries := splitIntoTimestampedEntries("")
	c.Assert(len(entries), qt.Equals, 0)
}

func TestSplitIntoTimestampedEntries_NoTimestamps(t *testing.T) {
	c := qt.New(t)

	entries := splitIntoTimestampedEntries("Just some plain text\nNo timestamps here")
	c.Assert(len(entries), qt.Equals, 0)
}

// ===== mergeAudioAndVisual Tests =====

func TestMergeAudioAndVisual_InterleavedByTimestamp(t *testing.T) {
	c := qt.New(t)

	audio := "[Audio: 00:00:05] Hello\n[Audio: 00:00:30] World"
	visual := "[Video: 00:00:10] A person speaking\n[Video: 00:00:25] Close-up of a whiteboard"

	merged := mergeAudioAndVisual(audio, visual)

	lines := strings.Split(merged, "\n")
	c.Assert(len(lines) >= 4, qt.IsTrue, qt.Commentf("expected at least 4 lines, got %d: %q", len(lines), merged))

	c.Assert(strings.Contains(lines[0], "[Audio: 00:00:05]"), qt.IsTrue,
		qt.Commentf("first entry should be earliest timestamp (00:00:05), got: %s", lines[0]))

	audioIdx := strings.Index(merged, "[Audio: 00:00:30]")
	videoIdx := strings.Index(merged, "[Video: 00:00:25]")
	c.Assert(videoIdx < audioIdx, qt.IsTrue,
		qt.Commentf("[Video: 00:00:25] should come before [Audio: 00:00:30]"))
}

func TestMergeAudioAndVisual_AudioOnlyInput(t *testing.T) {
	c := qt.New(t)

	audio := "[Audio: 00:00:05] Only audio\n[Audio: 00:01:00] More audio"
	merged := mergeAudioAndVisual(audio, "")

	c.Assert(strings.Contains(merged, "[Audio: 00:00:05]"), qt.IsTrue)
	c.Assert(strings.Contains(merged, "[Audio: 00:01:00]"), qt.IsTrue)
}

func TestMergeAudioAndVisual_VisualOnlyInput(t *testing.T) {
	c := qt.New(t)

	visual := "[Video: 00:00:10] Scene\n[Video: 00:02:00] Another scene"
	merged := mergeAudioAndVisual("", visual)

	c.Assert(strings.Contains(merged, "[Video: 00:00:10]"), qt.IsTrue)
	c.Assert(strings.Contains(merged, "[Video: 00:02:00]"), qt.IsTrue)
}

func TestMergeAudioAndVisual_BothEmpty(t *testing.T) {
	c := qt.New(t)

	merged := mergeAudioAndVisual("", "")
	c.Assert(merged, qt.Equals, "")
}

func TestMergeAudioAndVisual_StripsAccidentalAudioFromVisual(t *testing.T) {
	c := qt.New(t)

	audio := "[Audio: 00:00:05] Real audio"
	visual := "[Audio: 00:00:10] Accidental audio\n[Video: 00:00:15] Real visual"

	merged := mergeAudioAndVisual(audio, visual)

	audioCount := strings.Count(merged, "[Audio:")
	c.Assert(audioCount, qt.Equals, 1,
		qt.Commentf("accidental audio should be stripped; got %d Audio tags in: %q", audioCount, merged))
	c.Assert(strings.Contains(merged, "[Video: 00:00:15]"), qt.IsTrue)
}

func TestMergeAudioAndVisual_StripsAccidentalSoundFromVisual(t *testing.T) {
	c := qt.New(t)

	audio := "[Sound: 00:00:05] Real sound"
	visual := "[Sound: 00:00:10] Accidental sound\n[Video: 00:00:15] Real visual"

	merged := mergeAudioAndVisual(audio, visual)

	soundCount := strings.Count(merged, "[Sound:")
	c.Assert(soundCount, qt.Equals, 1,
		qt.Commentf("accidental sound should be stripped; got %d Sound tags in: %q", soundCount, merged))
}

func TestMergeAudioAndVisual_EqualTimestampsPreserveOrder(t *testing.T) {
	c := qt.New(t)

	audio := "[Audio: 00:01:00] Audio at 1 min"
	visual := "[Video: 00:01:00] Video at 1 min"

	merged := mergeAudioAndVisual(audio, visual)

	audioIdx := strings.Index(merged, "[Audio: 00:01:00]")
	videoIdx := strings.Index(merged, "[Video: 00:01:00]")
	c.Assert(audioIdx >= 0, qt.IsTrue)
	c.Assert(videoIdx >= 0, qt.IsTrue)
	c.Assert(audioIdx < videoIdx, qt.IsTrue,
		qt.Commentf("stable sort: audio entries (appended first) should come before visual at equal timestamps"))
}

func TestMergeAudioAndVisual_LargeTimestamps(t *testing.T) {
	c := qt.New(t)

	audio := "[Audio: 02:30:00] Late in the video"
	visual := "[Video: 01:15:00] Midway visual"

	merged := mergeAudioAndVisual(audio, visual)

	visualIdx := strings.Index(merged, "[Video: 01:15:00]")
	audioIdx := strings.Index(merged, "[Audio: 02:30:00]")
	c.Assert(visualIdx < audioIdx, qt.IsTrue,
		qt.Commentf("01:15:00 should come before 02:30:00"))
}
