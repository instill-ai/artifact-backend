package worker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/ai"
	"github.com/instill-ai/artifact-backend/pkg/ai/gemini"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
)

// inlineTimestampPattern matches timestamp tag formats used in media transcripts.
// Supported formats:
//   - Single:  [Audio: HH:MM:SS], [Sound: MM:SS], [Video: 01:23:45]
//   - Range:   [Video: HH:MM:SS - HH:MM:SS]
//
// Capture groups:
//
//	1 = tag type (Audio|Sound|Video)
//	2 = first (or only) timestamp — may be HH:MM:SS or MM:SS
//	3 = optional second timestamp in a range (same formats)
var inlineTimestampPattern = regexp.MustCompile(
	`\[(Audio|Sound|Video):\s*` +
		`(\d{1,2}:\d{2}(?::\d{2})?)` +
		`(?:\s*-\s*(\d{1,2}:\d{2}(?::\d{2})?))?` +
		`\]`,
)

// clipToTimeRange enforces both start and end boundaries on a media transcript.
// Lines whose inline timestamps fall before startTime (minus tolerance) or
// after endTime (plus tolerance) are removed. Non-timestamped preamble lines
// before the first in-range timestamp are also stripped when startTime > 0.
// Returns the clipped content and the total number of lines removed.
func clipToTimeRange(markdown string, startTime, endTime time.Duration) (string, int) {
	const tolerance = 30 * time.Second
	lines := strings.Split(markdown, "\n")

	startIdx := 0
	endIdx := len(lines)

	// --- Clip the start boundary (for segments after the first) ---
	if startTime > 0 {
		lastBeforeStart := -1
		for i, line := range lines {
			m := inlineTimestampPattern.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			ts, err := parseTimestampDuration(m[2])
			if err != nil {
				continue
			}
			if ts < startTime-tolerance {
				lastBeforeStart = i
			} else {
				break
			}
		}
		if lastBeforeStart >= 0 {
			startIdx = lastBeforeStart + 1
		}
	}

	// --- Clip the end boundary ---
	for i := startIdx; i < len(lines); i++ {
		m := inlineTimestampPattern.FindStringSubmatch(lines[i])
		if m == nil {
			continue
		}
		ts, err := parseTimestampDuration(m[2])
		if err != nil {
			continue
		}
		if ts > endTime+tolerance {
			endIdx = i
			break
		}
	}

	removed := startIdx + (len(lines) - endIdx)
	if removed == 0 {
		return markdown, 0
	}

	kept := strings.Join(lines[startIdx:endIdx], "\n")
	return strings.TrimSpace(kept), removed
}

// parseTimestampDuration parses "HH:MM:SS" or "MM:SS" into a time.Duration.
func parseTimestampDuration(s string) (time.Duration, error) {
	parts := strings.Split(s, ":")
	switch len(parts) {
	case 3:
		h, err := strconv.Atoi(parts[0])
		if err != nil {
			return 0, err
		}
		m, err := strconv.Atoi(parts[1])
		if err != nil {
			return 0, err
		}
		sec, err := strconv.Atoi(parts[2])
		if err != nil {
			return 0, err
		}
		return time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(sec)*time.Second, nil
	case 2:
		m, err := strconv.Atoi(parts[0])
		if err != nil {
			return 0, err
		}
		sec, err := strconv.Atoi(parts[1])
		if err != nil {
			return 0, err
		}
		return time.Duration(m)*time.Minute + time.Duration(sec)*time.Second, nil
	default:
		return 0, fmt.Errorf("invalid timestamp format: %s", s)
	}
}

// enforceTimestampMonotonicity is a defense-in-depth pass for assembled media
// transcripts. It walks the content and drops any line whose inline timestamp
// is strictly less than the highest timestamp seen so far (i.e., a duplicate
// that slipped through per-segment clipping). Non-timestamped lines are always
// kept. Returns the cleaned content and the number of lines removed.
func enforceTimestampMonotonicity(markdown string) (string, int) {
	lines := strings.Split(markdown, "\n")
	kept := make([]string, 0, len(lines))
	var highWater time.Duration
	removed := 0

	for _, line := range lines {
		m := inlineTimestampPattern.FindStringSubmatch(line)
		if m == nil {
			kept = append(kept, line)
			continue
		}
		ts, err := parseTimestampDuration(m[2])
		if err != nil {
			kept = append(kept, line)
			continue
		}
		if ts < highWater {
			removed++
			continue
		}
		highWater = ts
		kept = append(kept, line)
	}

	if removed == 0 {
		return markdown, 0
	}
	return strings.TrimSpace(strings.Join(kept, "\n")), removed
}

// modalityTokenCount mirrors the Gemini API ModalityTokenCount message.
// See: https://cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/ModalityTokenCount
type modalityTokenCount struct {
	Modality   string `json:"modality"`
	TokenCount int64  `json:"tokenCount"`
}

// usageAccumulator collects AI token usage across multiple API calls within
// a single Temporal activity. Passed by pointer through the conversion call
// chain so that recursive splits, retries, and sub-chunks all contribute to
// a single aggregate total.
//
// Fields match the official Gemini API UsageMetadata schema:
// https://cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/GenerateContentResponse#UsageMetadata
type usageAccumulator struct {
	PromptTokenCount          int64
	CandidatesTokenCount      int64
	TotalTokenCount           int64
	CachedContentTokenCount   int64
	ThoughtsTokenCount        int64
	ToolUsePromptTokenCount   int64
	PromptTokensDetails       map[string]int64
	CacheTokensDetails        map[string]int64
	CandidatesTokensDetails   map[string]int64
	ToolUsePromptTokensDetails map[string]int64
	TrafficType               string
	Model                     string
	CallCount                 int
}

// geminiUsageMetadata is the JSON wire format for Gemini's UsageMetadata.
type geminiUsageMetadata struct {
	PromptTokenCount           int64                `json:"promptTokenCount"`
	CandidatesTokenCount       int64                `json:"candidatesTokenCount"`
	TotalTokenCount            int64                `json:"totalTokenCount"`
	CachedContentTokenCount    int64                `json:"cachedContentTokenCount"`
	ThoughtsTokenCount         int64                `json:"thoughtsTokenCount"`
	ToolUsePromptTokenCount    int64                `json:"toolUsePromptTokenCount"`
	PromptTokensDetails        []modalityTokenCount `json:"promptTokensDetails"`
	CacheTokensDetails         []modalityTokenCount `json:"cacheTokensDetails"`
	CandidatesTokensDetails    []modalityTokenCount `json:"candidatesTokensDetails"`
	ToolUsePromptTokensDetails []modalityTokenCount `json:"toolUsePromptTokensDetails"`
	TrafficType                string               `json:"trafficType"`
}

// mergeModalityDetails sums per-modality token counts into a running map.
func mergeModalityDetails(dst map[string]int64, src []modalityTokenCount) map[string]int64 {
	if len(src) == 0 {
		return dst
	}
	if dst == nil {
		dst = make(map[string]int64, len(src))
	}
	for _, m := range src {
		if m.Modality != "" {
			dst[m.Modality] += m.TokenCount
		}
	}
	return dst
}

// Add extracts token counts from an ai.ConversionResult.UsageMetadata (typed
// as any) and sums them into the accumulator. The underlying type is
// *genai.GenerateContentResponseUsageMetadata; we decode via JSON to avoid a
// direct package dependency.
func (a *usageAccumulator) Add(usage any, model string) {
	if a == nil || usage == nil {
		return
	}
	a.CallCount++
	if a.Model == "" && model != "" {
		a.Model = model
	}

	b, err := json.Marshal(usage)
	if err != nil {
		return
	}
	var m geminiUsageMetadata
	if err := json.Unmarshal(b, &m); err != nil {
		return
	}

	a.PromptTokenCount += m.PromptTokenCount
	a.CandidatesTokenCount += m.CandidatesTokenCount
	a.TotalTokenCount += m.TotalTokenCount
	a.CachedContentTokenCount += m.CachedContentTokenCount
	a.ThoughtsTokenCount += m.ThoughtsTokenCount
	a.ToolUsePromptTokenCount += m.ToolUsePromptTokenCount

	a.PromptTokensDetails = mergeModalityDetails(a.PromptTokensDetails, m.PromptTokensDetails)
	a.CacheTokensDetails = mergeModalityDetails(a.CacheTokensDetails, m.CacheTokensDetails)
	a.CandidatesTokensDetails = mergeModalityDetails(a.CandidatesTokensDetails, m.CandidatesTokensDetails)
	a.ToolUsePromptTokensDetails = mergeModalityDetails(a.ToolUsePromptTokensDetails, m.ToolUsePromptTokensDetails)

	if a.TrafficType == "" && m.TrafficType != "" {
		a.TrafficType = m.TrafficType
	}
}

// modalityMapToSlice converts the internal per-modality map back to the
// Gemini API array format for serialisation.
func modalityMapToSlice(m map[string]int64) []map[string]interface{} {
	if len(m) == 0 {
		return nil
	}
	out := make([]map[string]interface{}, 0, len(m))
	for modality, count := range m {
		out = append(out, map[string]interface{}{
			"modality":   modality,
			"tokenCount": count,
		})
	}
	return out
}

// ToMap returns the accumulated totals as a serialisable map suitable for
// Temporal payloads and the UpdateUsageMetadataActivity pipeline. The output
// uses the same JSON field names as the Gemini API UsageMetadata response.
func (a *usageAccumulator) ToMap() map[string]interface{} {
	if a == nil {
		return nil
	}
	out := map[string]interface{}{
		"promptTokenCount":        a.PromptTokenCount,
		"candidatesTokenCount":    a.CandidatesTokenCount,
		"totalTokenCount":         a.TotalTokenCount,
		"cachedContentTokenCount": a.CachedContentTokenCount,
		"callCount":               a.CallCount,
	}

	if a.ThoughtsTokenCount > 0 {
		out["thoughtsTokenCount"] = a.ThoughtsTokenCount
	}
	if a.ToolUsePromptTokenCount > 0 {
		out["toolUsePromptTokenCount"] = a.ToolUsePromptTokenCount
	}
	if s := modalityMapToSlice(a.PromptTokensDetails); s != nil {
		out["promptTokensDetails"] = s
	}
	if s := modalityMapToSlice(a.CacheTokensDetails); s != nil {
		out["cacheTokensDetails"] = s
	}
	if s := modalityMapToSlice(a.CandidatesTokensDetails); s != nil {
		out["candidatesTokensDetails"] = s
	}
	if s := modalityMapToSlice(a.ToolUsePromptTokensDetails); s != nil {
		out["toolUsePromptTokensDetails"] = s
	}
	if a.TrafficType != "" {
		out["trafficType"] = a.TrafficType
	}

	return out
}

// isDeadlineExceeded checks whether an error indicates a server-side or
// client-side processing timeout. This includes:
//   - Gemini API: DEADLINE_EXCEEDED / HTTP 504
//   - Go context: "context deadline exceeded" from per-step time budgets
//
// Both cases indicate the conversion was too slow and a chunked fallback
// should be attempted.
func isDeadlineExceeded(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "DEADLINE_EXCEEDED") ||
		strings.Contains(msg, "504") ||
		strings.Contains(msg, "context deadline exceeded")
}

// isTransientError checks whether an error is a transient server/API failure
// that is likely to succeed on retry. This covers rate limiting, timeouts,
// cancellations, temporary unavailability, cache expiry (which resolves
// on retry because the caller recreates the cache), and Gemini API glitches
// such as "The document has no pages" which occur under heavy load.
func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return isDeadlineExceeded(err) ||
		isCacheExpired(err) ||
		strings.Contains(msg, "RESOURCE_EXHAUSTED") ||
		strings.Contains(msg, "429") ||
		strings.Contains(msg, "CANCELLED") ||
		strings.Contains(msg, "499") ||
		strings.Contains(msg, "UNAVAILABLE") ||
		strings.Contains(msg, "503") ||
		strings.Contains(msg, "document has no pages")
}

// isCacheExpired checks whether an error indicates that the Gemini content
// cache has expired. This is returned as INVALID_ARGUMENT (400) with a message
// like "Cache content <id> is expired."
// Cache expiry is recoverable: the caller can create a fresh cache and retry.
func isCacheExpired(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "is expired")
}

// isRateLimited checks whether an error indicates a rate-limiting response
// from the AI API (HTTP 429 / RESOURCE_EXHAUSTED).
func isRateLimited(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "RESOURCE_EXHAUSTED") || strings.Contains(msg, "429")
}

const (
	chunkRetryMaxAttempts  = 5
	chunkRetryInitialDelay = 5 * time.Second
	chunkRetryMaxDelay     = 120 * time.Second
)

// convertChunkRobust wraps convertChunkAdaptive (which handles DEADLINE_EXCEEDED
// via recursive split) with in-activity exponential backoff for transient errors
// (rate-limit, API glitches like "document has no pages", etc.).
// Cache expired errors propagate immediately (require workflow-level cache recreation).
// Non-transient errors also propagate immediately.
func (w *Worker) convertChunkRobust(ctx context.Context, cacheName string, start, end, totalPages int, prompt string, acc *usageAccumulator, chunkTimeout time.Duration, fileType artifactpb.File_Type) (string, error) {
	var lastErr error
	delay := chunkRetryInitialDelay

	for attempt := 1; attempt <= chunkRetryMaxAttempts; attempt++ {
		markdown, err := w.convertChunkAdaptive(ctx, cacheName, start, end, totalPages, prompt, acc, chunkTimeout, fileType)
		if err == nil {
			return markdown, nil
		}

		if isCacheExpired(err) || !isTransientError(err) {
			return "", err
		}

		lastErr = err
		if attempt < chunkRetryMaxAttempts {
			w.log.Warn("Transient error, retrying after backoff",
				zap.Int("attempt", attempt),
				zap.Duration("delay", delay),
				zap.Int("start", start),
				zap.Int("end", end),
				zap.Error(err))

			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(delay):
			}

			delay = time.Duration(math.Min(float64(delay*2), float64(chunkRetryMaxDelay)))
		}
	}

	return "", fmt.Errorf("transient error retries exhausted for pages %d-%d: %w", start, end, lastErr)
}

// convertTimeRange converts a single time-range segment of a media file and
// returns the markdown output. The prompt instructs the model to process only
// the content between startTime and endTime.
//
// Media segments do NOT use [Page: N] tags — the inline timestamps
// ([Audio: HH:MM:SS], [Sound: HH:MM:SS], etc.) are the ground truth for
// position mapping. After the AI call, output is truncated to the requested
// time range to guard against models that ignore the boundary instruction.
func (w *Worker) convertTimeRange(ctx context.Context, cacheName string, startTime, endTime time.Duration, segmentIndex int, prompt string, acc *usageAccumulator, chunkTimeout time.Duration, fileType artifactpb.File_Type) (string, error) {
	timeRangePrompt := fmt.Sprintf(
		"[CRITICAL INSTRUCTION] Convert the video/audio content between timestamps %s and %s.\n"+
			"- Transcribe ALL speech, describe visual content, and note on-screen text.\n"+
			"- Use absolute timestamps from the original media (do NOT reset to 00:00:00).\n"+
			"- Do NOT include content from outside this time range.\n\n%s",
		formatTimestamp(startTime), formatTimestamp(endTime), prompt,
	)

	w.log.Info("Converting time range",
		zap.String("start", formatTimestamp(startTime)),
		zap.String("end", formatTimestamp(endTime)),
		zap.Int("segmentIndex", segmentIndex))

	if chunkTimeout <= 0 {
		chunkTimeout = ChunkConversionTimeout
	}
	chunkCtx, chunkCancel := context.WithTimeout(ctx, chunkTimeout)
	result, err := w.getAIClientForFileType(fileType).ConvertToMarkdownWithCache(chunkCtx, cacheName, timeRangePrompt)
	chunkCancel()
	if err != nil {
		return "", err
	}

	acc.Add(result.UsageMetadata, result.Model)

	markdown := result.Markdown

	// Strip any [Page: N] tags the model may have emitted on its own.
	markdown = pageTagPattern.ReplaceAllString(markdown, "")
	markdown = strings.TrimSpace(markdown)

	// Clip content outside the requested [startTime, endTime] window.
	// This guards against models that ignore the time-range instruction and
	// transcribe the entire file.
	clipped, n := clipToTimeRange(markdown, startTime, endTime)
	if n > 0 {
		w.log.Warn("Model returned content outside requested time range, clipped",
			zap.Int("segmentIndex", segmentIndex),
			zap.String("requestedStart", formatTimestamp(startTime)),
			zap.String("requestedEnd", formatTimestamp(endTime)),
			zap.Int("linesRemoved", n))
		markdown = clipped
	}

	return markdown, nil
}

// convertTimeRangeRobust wraps convertTimeRange with in-activity exponential
// backoff for transient errors (rate-limit, API glitches, etc.).
// Cache-expired errors propagate immediately (require workflow-level cache recreation).
// Non-transient errors also propagate immediately.
func (w *Worker) convertTimeRangeRobust(ctx context.Context, cacheName string, startTime, endTime time.Duration, segmentIndex int, prompt string, acc *usageAccumulator, chunkTimeout time.Duration, fileType artifactpb.File_Type) (string, error) {
	var lastErr error
	delay := chunkRetryInitialDelay

	for attempt := 1; attempt <= chunkRetryMaxAttempts; attempt++ {
		markdown, err := w.convertTimeRange(ctx, cacheName, startTime, endTime, segmentIndex, prompt, acc, chunkTimeout, fileType)
		if err == nil {
			return markdown, nil
		}

		if isCacheExpired(err) || !isTransientError(err) {
			return "", err
		}

		lastErr = err
		if attempt < chunkRetryMaxAttempts {
			w.log.Warn("Transient error in time-range conversion, retrying after backoff",
				zap.Int("attempt", attempt),
				zap.Duration("delay", delay),
				zap.Int("segmentIndex", segmentIndex),
				zap.String("start", formatTimestamp(startTime)),
				zap.String("end", formatTimestamp(endTime)),
				zap.Error(err))

			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(delay):
			}

			delay = time.Duration(math.Min(float64(delay*2), float64(chunkRetryMaxDelay)))
		}
	}

	return "", fmt.Errorf("transient error retries exhausted for segment %d (%s-%s): %w",
		segmentIndex, formatTimestamp(startTime), formatTimestamp(endTime), lastErr)
}

// getPageCount asks the AI model (via the existing cache) how many pages the
// document has. The call is lightweight because the file content is already
// cached — only the small prompt travels over the wire.
// Uses PageCountQueryTimeout to prevent hanging on unresponsive APIs.
func (w *Worker) getPageCount(ctx context.Context, cacheName string, fileType artifactpb.File_Type) (int, error) {
	queryCtx, queryCancel := context.WithTimeout(ctx, PageCountQueryTimeout)
	defer queryCancel()

	result, err := w.getAIClientForFileType(fileType).ConvertToMarkdownWithCache(queryCtx, cacheName, ChunkedConversionPageCountPrompt)
	if err != nil {
		return 0, fmt.Errorf("failed to get page count: %w", err)
	}

	text := strings.TrimSpace(result.Markdown)
	pageCount, err := strconv.Atoi(text)
	if err != nil {
		for _, word := range strings.Fields(text) {
			if n, parseErr := strconv.Atoi(word); parseErr == nil {
				return n, nil
			}
		}
		return 0, fmt.Errorf("could not parse page count from response %q: %w", text, err)
	}

	if pageCount <= 0 {
		return 0, fmt.Errorf("invalid page count: %d", pageCount)
	}

	return pageCount, nil
}

// SpeakerIdentificationPrompt asks the model to list speaker names.
const SpeakerIdentificationPrompt = "List every distinct speaker you can identify by name in this recording. " +
	"Output ONLY a comma-separated list of full names (e.g., \"Alice Smith, Bob Jones\"). " +
	"If no speakers can be identified by name, output \"UNKNOWN\"."

// IdentifySpeakersActivityParam defines input for IdentifySpeakersActivity.
type IdentifySpeakersActivityParam struct {
	CacheName string
	FileType  artifactpb.File_Type
}

// IdentifySpeakersActivityResult contains the identified speaker context string.
type IdentifySpeakersActivityResult struct {
	SpeakerContext string
}

// IdentifySpeakersActivity queries a cached media file to identify speaker names.
// Returns a prompt-ready context string, or empty if identification fails.
func (w *Worker) IdentifySpeakersActivity(ctx context.Context, param *IdentifySpeakersActivityParam) (*IdentifySpeakersActivityResult, error) {
	queryCtx, queryCancel := context.WithTimeout(ctx, PageCountQueryTimeout)
	defer queryCancel()

	result, err := w.getAIClientForFileType(param.FileType).ConvertToMarkdownWithCache(queryCtx, param.CacheName, SpeakerIdentificationPrompt)
	if err != nil {
		w.log.Warn("Speaker identification failed, proceeding without speaker context", zap.Error(err))
		return &IdentifySpeakersActivityResult{}, nil
	}

	names := strings.TrimSpace(result.Markdown)
	if names == "" || strings.EqualFold(names, "UNKNOWN") {
		return &IdentifySpeakersActivityResult{}, nil
	}

	speakerCtx := "[SPEAKER CONTEXT] The speakers in this recording are: " + names + ". " +
		"Always use these exact names when attributing speech — never use generic labels like \"Speaker 1\"."
	return &IdentifySpeakersActivityResult{SpeakerContext: speakerCtx}, nil
}

// convertChunk converts a single page range and returns the markdown output.
// If acc is non-nil, token usage from the API call is added to the accumulator.
func (w *Worker) convertChunk(ctx context.Context, cacheName string, start, end, totalPages int, prompt string, acc *usageAccumulator, chunkTimeout time.Duration, fileType artifactpb.File_Type) (string, error) {
	pageRangePrompt := fmt.Sprintf(
		"[CRITICAL INSTRUCTION] Convert pages %d through %d of this document.\n"+
			"- You MUST include a [Page: X] tag on its own line for EVERY page from %d to %d.\n"+
			"- Do NOT include [Page: X] tags for any page outside %d-%d.\n"+
			"- If a table or structured element continues beyond page %d, include the remaining rows WITHOUT adding page tags for those overflow pages.\n"+
			"- If a table started before page %d, SKIP the rows already captured by a prior batch.\n\n%s",
		start, end, start, end, start, end, end, start, prompt,
	)

	w.log.Info("Converting page range",
		zap.Int("start", start),
		zap.Int("end", end),
		zap.Int("totalPages", totalPages))

	if chunkTimeout <= 0 {
		chunkTimeout = ChunkConversionTimeout
	}
	chunkCtx, chunkCancel := context.WithTimeout(ctx, chunkTimeout)
	result, err := w.getAIClientForFileType(fileType).ConvertToMarkdownWithCache(chunkCtx, cacheName, pageRangePrompt)
	chunkCancel()
	if err != nil {
		return "", err
	}

	acc.Add(result.UsageMetadata, result.Model)
	return result.Markdown, nil
}

// countPresentPageTags extracts which page numbers have [Page: X] tags in the
// markdown and returns the set of found page numbers.
func countPresentPageTags(markdown string, start, end int) map[int]bool {
	found := make(map[int]bool)
	matches := pageTagPattern.FindAllStringSubmatch(markdown, -1)
	for _, m := range matches {
		if len(m) >= 2 {
			if num, err := strconv.Atoi(m[1]); err == nil && num >= start && num <= end {
				found[num] = true
			}
		}
	}
	return found
}

// convertChunkAdaptive converts a page range, recursively splitting on
// DEADLINE_EXCEEDED or missing page tags until the chunk reaches single-page
// granularity (or ChunkedConversionMinChunkPages for timeouts).
//
// After a successful API call, it validates that every expected [Page: X] tag
// is present. If the AI omitted tags (common under heavy load or adaptive
// splitting), the range is split and each half retried independently so that
// smaller ranges produce correct tags.
func (w *Worker) convertChunkAdaptive(ctx context.Context, cacheName string, start, end, totalPages int, prompt string, acc *usageAccumulator, chunkTimeout time.Duration, fileType artifactpb.File_Type) (string, error) {
	markdown, err := w.convertChunk(ctx, cacheName, start, end, totalPages, prompt, acc, chunkTimeout, fileType)
	if err == nil {
		chunkSize := end - start + 1
		if chunkSize > 1 {
			found := countPresentPageTags(markdown, start, end)
			if len(found) < chunkSize {
				var missing []int
				for p := start; p <= end; p++ {
					if !found[p] {
						missing = append(missing, p)
					}
				}
				w.log.Warn("AI output missing page tags, splitting and retrying",
					zap.Int("start", start),
					zap.Int("end", end),
					zap.Int("expected", chunkSize),
					zap.Int("found", len(found)),
					zap.Ints("missingPages", missing))

				return w.splitAndRetry(ctx, cacheName, start, end, totalPages, prompt, acc, chunkTimeout, fileType)
			}
		} else {
			found := countPresentPageTags(markdown, start, end)
			if !found[start] {
				w.log.Info("Single-page output missing [Page: X] tag, injecting deterministically",
					zap.Int("page", start))
				markdown = fmt.Sprintf("[Page: %d]\n%s", start, markdown)
			}
		}
		return markdown, nil
	}

	chunkSize := end - start + 1
	if isDeadlineExceeded(err) && chunkSize > ChunkedConversionMinChunkPages {
		w.log.Warn("Chunk timed out, splitting in half and retrying",
			zap.Int("start", start),
			zap.Int("end", end),
			zap.Int("chunkSize", chunkSize),
			zap.Error(err))

		return w.splitAndRetry(ctx, cacheName, start, end, totalPages, prompt, acc, chunkTimeout, fileType)
	}

	return "", fmt.Errorf("chunked conversion failed for pages %d-%d: %w", start, end, err)
}

// splitAndRetry splits a page range in half and converts each half recursively.
func (w *Worker) splitAndRetry(ctx context.Context, cacheName string, start, end, totalPages int, prompt string, acc *usageAccumulator, chunkTimeout time.Duration, fileType artifactpb.File_Type) (string, error) {
	mid := (start + end) / 2
	firstHalf, err1 := w.convertChunkAdaptive(ctx, cacheName, start, mid, totalPages, prompt, acc, chunkTimeout, fileType)
	if err1 != nil {
		return "", fmt.Errorf("chunked conversion failed for pages %d-%d: %w", start, mid, err1)
	}
	secondHalf, err2 := w.convertChunkAdaptive(ctx, cacheName, mid+1, end, totalPages, prompt, acc, chunkTimeout, fileType)
	if err2 != nil {
		return "", fmt.Errorf("chunked conversion failed for pages %d-%d: %w", mid+1, end, err2)
	}
	return firstHalf + "\n\n" + secondHalf, nil
}

// convertInPageRanges converts a large document in fixed-size page-range chunks
// using the existing cache. Each chunk produces markdown for a subset of pages,
// and the outputs are concatenated to form the complete document.
//
// Cache auto-refresh: if fileContent is non-nil and the cache expires mid-
// conversion, a fresh cache is created transparently and the failed chunk is
// retried. This preserves work already done on earlier chunks instead of
// discarding the entire conversion.
//
// Adaptive retry: if a chunk fails with DEADLINE_EXCEEDED, it is recursively
// split in half until the chunk is at or below ChunkedConversionMinChunkPages.
func (w *Worker) convertInPageRanges(ctx context.Context, cacheName string, pageCount int, prompt string, fileContent *ai.FileContent, fileType artifactpb.File_Type) (string, error) { //nolint:unused
	pagesPerChunk := 10
	var chunks []string
	var cacheCleanups []string
	defer func() {
		for _, name := range cacheCleanups {
			if delErr := w.aiClient.DeleteCache(context.Background(), name); delErr != nil {
				w.log.Warn("Failed to clean up refreshed cache", zap.String("cacheName", name), zap.Error(delErr))
			}
		}
	}()

	for start := 1; start <= pageCount; start += pagesPerChunk {
		end := start + pagesPerChunk - 1
		if end > pageCount {
			end = pageCount
		}

		markdown, err := w.convertChunkAdaptive(ctx, cacheName, start, end, pageCount, prompt, nil, ChunkConversionTimeout, fileType)
		if err != nil {
			if isCacheExpired(err) {
				if fileContent == nil {
					return "", fmt.Errorf("chunked conversion failed for pages %d-%d (cache expired, no file content for refresh): %w", start, end, err)
				}

				w.log.Warn("Cache expired mid-conversion, creating fresh cache and continuing",
					zap.Int("completedChunks", len(chunks)),
					zap.Int("failedStart", start))

				freshCache, cacheErr := w.aiClient.CreateCache(ctx, []ai.FileContent{*fileContent}, gemini.GetCacheTTL())
				if cacheErr != nil {
					return "", fmt.Errorf("failed to recreate expired cache at pages %d-%d: %w", start, end, cacheErr)
				}
				cacheCleanups = append(cacheCleanups, freshCache.CacheName)
				cacheName = freshCache.CacheName

				markdown, err = w.convertChunkAdaptive(ctx, cacheName, start, end, pageCount, prompt, nil, ChunkConversionTimeout, fileType)
				if err != nil {
					return "", fmt.Errorf("chunked conversion failed for pages %d-%d after cache refresh: %w", start, end, err)
				}
				chunks = append(chunks, markdown)
				continue
			}

			return "", err
		}
		chunks = append(chunks, markdown)
	}

	return strings.Join(chunks, "\n\n"), nil
}

// ===== Per-Batch Temporal Activities =====

// GetPageCountActivityParam defines input for GetPageCountActivity.
type GetPageCountActivityParam struct {
	CacheName string
	FileType  artifactpb.File_Type
}

// GetPageCountActivityResult defines output from GetPageCountActivity.
type GetPageCountActivityResult struct {
	PageCount int
}

// GetPageCountActivity wraps the getPageCount helper as a proper Temporal activity.
func (w *Worker) GetPageCountActivity(ctx context.Context, param *GetPageCountActivityParam) (*GetPageCountActivityResult, error) {
	pageCount, err := w.getPageCount(ctx, param.CacheName, param.FileType)
	if err != nil {
		return nil, fmt.Errorf("GetPageCountActivity: %w", err)
	}
	return &GetPageCountActivityResult{PageCount: pageCount}, nil
}

// ConvertBatchActivityParam defines input for ConvertBatchActivity.
// Supports two mutually exclusive modes:
//   - Page-range mode (documents): converts pages StartPage..EndPage.
//   - Time-range mode (audio/video): when UseTimeRange is true, converts the
//     media segment between StartTimestamp and EndTimestamp.
type ConvertBatchActivityParam struct {
	CacheName     string
	StartPage     int
	EndPage       int
	TotalPages    int
	KBUID         types.KBUIDType
	FileUID       types.FileUIDType
	WorkflowRunID string
	BatchIndex    int
	ChunkTimeout  time.Duration // Per-chunk AI call timeout (0 = use default ChunkConversionTimeout)
	PagesPerChunk int           // Sub-chunk size within a batch (0 = use default 10)

	// Time-range mode fields (mutually exclusive with page range).
	// UseTimeRange must be set explicitly because StartTimestamp=0 is valid
	// for the first segment (00:00:00).
	UseTimeRange   bool
	StartTimestamp time.Duration // e.g., 0 for the first segment
	EndTimestamp   time.Duration // e.g., 5*time.Minute
	SegmentIndex   int           // Sequential segment number (1-based)

	FileType artifactpb.File_Type

	// SpeakerContext is an optional hint listing identified speaker names.
	// When non-empty, it is prepended to the prompt so the model can
	// attribute speech to the correct person across independent chunks.
	SpeakerContext string
}

// ConvertBatchActivityResult defines output from ConvertBatchActivity.
type ConvertBatchActivityResult struct {
	TempMinIOPath string
	UsageMetadata map[string]interface{}
	Model         string
}

// ConvertBatchActivity converts a batch segment using the cached content.
// In page-range mode (documents), it splits into sub-chunks and uses convertChunkRobust.
// In time-range mode (audio/video), it calls convertTimeRangeRobust once for the segment.
// Writes the result to a temporary MinIO path (not through Temporal payload)
// to stay well under the ~2MB payload limit for large documents.
// Returns a non-retryable "CacheExpired" error if the cache expires, so the
// workflow can refresh and retry just this batch.
func (w *Worker) ConvertBatchActivity(ctx context.Context, param *ConvertBatchActivityParam) (*ConvertBatchActivityResult, error) {
	logger := w.log.With(
		zap.String("fileUID", param.FileUID.String()),
		zap.Int("batchIndex", param.BatchIndex))

	chunkTimeout := param.ChunkTimeout
	if chunkTimeout <= 0 {
		chunkTimeout = ChunkConversionTimeout
	}

	prompt := w.getContentPromptForFileType(param.FileType)
	if param.SpeakerContext != "" {
		prompt = param.SpeakerContext + "\n\n" + prompt
	}
	acc := &usageAccumulator{}
	var batchMarkdown string

	if param.UseTimeRange {
		// Time-range mode: single AI call for this segment
		logger = logger.With(
			zap.String("startTimestamp", formatTimestamp(param.StartTimestamp)),
			zap.String("endTimestamp", formatTimestamp(param.EndTimestamp)),
			zap.Int("segmentIndex", param.SegmentIndex))
		logger.Info("ConvertBatchActivity started (time-range mode)")

		markdown, err := w.convertTimeRangeRobust(ctx, param.CacheName,
			param.StartTimestamp, param.EndTimestamp, param.SegmentIndex,
			prompt, acc, chunkTimeout, param.FileType)
		if err != nil {
			if isCacheExpired(err) {
				return nil, temporal.NewNonRetryableApplicationError(
					fmt.Sprintf("cache expired at segment %d (%s-%s)",
						param.SegmentIndex, formatTimestamp(param.StartTimestamp), formatTimestamp(param.EndTimestamp)),
					"CacheExpired", err)
			}
			return nil, fmt.Errorf("batch %d (segment %d, %s-%s) failed: %w",
				param.BatchIndex, param.SegmentIndex,
				formatTimestamp(param.StartTimestamp), formatTimestamp(param.EndTimestamp), err)
		}
		batchMarkdown = markdown
	} else {
		// Page-range mode: split into sub-chunks
		logger = logger.With(
			zap.Int("startPage", param.StartPage),
			zap.Int("endPage", param.EndPage))
		logger.Info("ConvertBatchActivity started (page-range mode)")

		pagesPerChunk := param.PagesPerChunk
		if pagesPerChunk <= 0 {
			pagesPerChunk = 10
		}

		var chunks []string
		for start := param.StartPage; start <= param.EndPage; start += pagesPerChunk {
			end := start + pagesPerChunk - 1
			if end > param.EndPage {
				end = param.EndPage
			}

			markdown, err := w.convertChunkRobust(ctx, param.CacheName, start, end, param.TotalPages, prompt, acc, chunkTimeout, param.FileType)
			if err != nil {
				if isCacheExpired(err) {
					return nil, temporal.NewNonRetryableApplicationError(
						fmt.Sprintf("cache expired at pages %d-%d", start, end),
						"CacheExpired", err)
				}
				return nil, fmt.Errorf("batch %d failed at pages %d-%d: %w", param.BatchIndex, start, end, err)
			}
			chunks = append(chunks, markdown)
		}
		batchMarkdown = strings.Join(chunks, "\n\n")
	}

	tempPath := fmt.Sprintf("temp/batches/%s/%s/batch-%03d.md",
		param.WorkflowRunID, param.FileUID.String(), param.BatchIndex)

	b64 := base64.StdEncoding.EncodeToString([]byte(batchMarkdown))
	bucket := config.Config.Minio.BucketName
	if err := w.repository.GetMinIOStorage().UploadBase64File(ctx, bucket, tempPath, b64, "text/markdown"); err != nil {
		return nil, fmt.Errorf("failed to write batch %d to MinIO: %w", param.BatchIndex, err)
	}

	logger.Info("ConvertBatchActivity completed",
		zap.Int("markdownLen", len(batchMarkdown)),
		zap.String("tempPath", tempPath),
		zap.Int("apiCalls", acc.CallCount),
		zap.Int64("totalTokens", acc.TotalTokenCount))

	return &ConvertBatchActivityResult{
		TempMinIOPath: tempPath,
		UsageMetadata: acc.ToMap(),
		Model:         acc.Model,
	}, nil
}

// ChunkOffsetInfo describes the time offset and batch count for one physical
// chunk of a long media file. Used by SaveAssembledContentActivity to shift
// chunk-relative timestamps to absolute positions in the original media and
// to trim the overlap region from non-first chunks.
type ChunkOffsetInfo struct {
	PathCount       int           // number of batch paths belonging to this chunk
	Offset          time.Duration // absolute start time of this chunk in the original media
	OverlapDuration time.Duration // leading overlap to trim from non-first chunks (0 for chunk 0)
}

// SaveAssembledContentActivityParam defines input for SaveAssembledContentActivity.
type SaveAssembledContentActivityParam struct {
	FileUID        types.FileUIDType
	KBUID          types.KBUIDType
	FileType       artifactpb.File_Type
	TempMinIOPaths []string
	// IsMedia indicates that the batches came from time-range media segmentation.
	// When true, assembly skips page-tag processing since media content uses
	// inline timestamps instead of [Page: N] delimiters.
	IsMedia bool
	// ChunkOffsets describes per-chunk timestamp offsets for long media files.
	// Paths in TempMinIOPaths are laid out consecutively by chunk: the first
	// ChunkOffsets[0].PathCount paths belong to chunk 0, the next
	// ChunkOffsets[1].PathCount paths to chunk 1, etc. Each batch's content is
	// shifted by its chunk's Offset before concatenation. Nil for non-long-media.
	ChunkOffsets []ChunkOffsetInfo
}

// SaveAssembledContentActivityResult defines output from SaveAssembledContentActivity.
type SaveAssembledContentActivityResult struct {
	Content          string
	ConvertedFileUID types.ConvertedFileUIDType
	ContentBucket    string
	ContentPath      string
	Length           []uint32
	PageCount        int32
	PositionData     *types.PositionData
}

// SaveAssembledContentActivity reads all batch markdown from MinIO temp paths,
// assembles them (merging cross-boundary HTML tables), runs ExtractPageDelimiters,
// saves the final content to DB/MinIO, and cleans up temp files.
func (w *Worker) SaveAssembledContentActivity(ctx context.Context, param *SaveAssembledContentActivityParam) (*SaveAssembledContentActivityResult, error) {
	logger := w.log.With(
		zap.String("fileUID", param.FileUID.String()),
		zap.Int("batchCount", len(param.TempMinIOPaths)))

	logger.Info("SaveAssembledContentActivity started")

	bucket := config.Config.Minio.BucketName

	batches := make([]string, 0, len(param.TempMinIOPaths))
	for i, path := range param.TempMinIOPaths {
		data, err := w.repository.GetMinIOStorage().GetFile(ctx, bucket, path)
		if err != nil {
			return nil, fmt.Errorf("failed to read batch %d from MinIO (%s): %w", i, path, err)
		}
		batches = append(batches, string(data))
	}

	// Apply per-chunk timestamp offsets and trim overlap for long media files.
	if len(param.ChunkOffsets) > 0 {
		idx := 0
		for _, ci := range param.ChunkOffsets {
			for j := 0; j < ci.PathCount && idx < len(batches); j++ {
				batches[idx] = offsetTimestamps(batches[idx], ci.Offset)
				idx++
			}
			// Trim leading overlap: for non-first chunks, remove lines whose
			// absolute timestamps fall within the overlap window that was
			// already covered by the previous chunk.
			if ci.OverlapDuration > 0 {
				overlapEnd := ci.Offset + ci.OverlapDuration
				start := idx - ci.PathCount
				for k := start; k < idx && k < len(batches); k++ {
					clipped, n := clipToTimeRange(batches[k], overlapEnd, 1<<62)
					if n > 0 {
						batches[k] = clipped
					}
				}
			}
		}
	}

	var contentInMarkdown string
	var positionData *types.PositionData
	var length []uint32
	var pageCount int32

	if param.IsMedia {
		// Media files: segments are time-range based. No [Page: N] tags exist.
		// Concatenate with double newlines and treat as a single logical page.
		joined := strings.Join(batches, "\n\n")
		joined = pageTagPattern.ReplaceAllString(joined, "")
		joined = strings.TrimSpace(joined)

		// Defense-in-depth: remove any lines whose timestamps regress, which
		// indicates duplicate content that slipped through per-segment clipping.
		cleaned, dupLines := enforceTimestampMonotonicity(joined)
		if dupLines > 0 {
			logger.Warn("Removed duplicate timestamp lines during media assembly",
				zap.Int("linesRemoved", dupLines))
		}
		contentInMarkdown = cleaned

		contentLength := uint32(len([]rune(contentInMarkdown)))
		length = []uint32{contentLength}
		pageCount = 1
		positionData = &types.PositionData{
			PageDelimiters: []uint32{contentLength},
		}
		logger.Info("Media assembly: concatenated time-range segments as single page",
			zap.Int("contentLen", len(contentInMarkdown)),
			zap.Int("segmentCount", len(batches)))
	} else {
		assembled := mergeHTMLTables(batches)
		assembled = deduplicatePageTags(assembled)
		contentInMarkdown, positionData, length, pageCount = ExtractPageDelimiters(
			assembled, w.log, map[string]any{"batched": true, "batchCount": len(batches)})
	}

	convertedFileUID, _ := uuid.NewV4()

	if _, err := w.CreateConvertedFileRecordActivity(ctx, &CreateConvertedFileRecordActivityParam{
		KBUID:            param.KBUID,
		FileUID:          param.FileUID,
		ConvertedFileUID: convertedFileUID,
		ConvertedType:    artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_CONTENT,
		Destination:      fmt.Sprintf("placeholder-pending-upload-%s", convertedFileUID.String()),
		PositionData:     positionData,
	}); err != nil {
		return nil, fmt.Errorf("failed to create converted file record: %w", err)
	}

	uploadResult, err := w.UploadConvertedFileToMinIOActivity(ctx, &UploadConvertedFileToMinIOActivityParam{
		KBUID:            param.KBUID,
		FileUID:          param.FileUID,
		ConvertedFileUID: convertedFileUID,
		Content:          contentInMarkdown,
	})
	if err != nil {
		_ = w.DeleteConvertedFileRecordActivity(ctx, &DeleteConvertedFileRecordActivityParam{
			ConvertedFileUID: convertedFileUID,
		})
		return nil, fmt.Errorf("failed to upload assembled content to MinIO: %w", err)
	}

	if err := w.UpdateConvertedFileDestinationActivity(ctx, &UpdateConvertedFileDestinationActivityParam{
		ConvertedFileUID: convertedFileUID,
		Destination:      uploadResult.Destination,
	}); err != nil {
		_ = w.DeleteConvertedFileFromMinIOActivity(ctx, &DeleteConvertedFileFromMinIOActivityParam{
			Bucket:      bucket,
			Destination: uploadResult.Destination,
		})
		_ = w.DeleteConvertedFileRecordActivity(ctx, &DeleteConvertedFileRecordActivityParam{
			ConvertedFileUID: convertedFileUID,
		})
		return nil, fmt.Errorf("failed to update converted file destination: %w", err)
	}

	for _, path := range param.TempMinIOPaths {
		if delErr := w.repository.GetMinIOStorage().DeleteFile(ctx, bucket, path); delErr != nil {
			logger.Warn("Failed to clean up temp batch file", zap.String("path", path), zap.Error(delErr))
		}
	}

	logger.Info("SaveAssembledContentActivity completed",
		zap.Int("contentLen", len(contentInMarkdown)),
		zap.Int32("pageCount", pageCount),
		zap.String("convertedFileUID", convertedFileUID.String()))

	return &SaveAssembledContentActivityResult{
		Content:          contentInMarkdown,
		ConvertedFileUID: convertedFileUID,
		ContentBucket:    bucket,
		ContentPath:      uploadResult.Destination,
		Length:           length,
		PageCount:        pageCount,
		PositionData:     positionData,
	}, nil
}

// ===== LONG MEDIA CHUNK PROCESSING =====

// offsetTimestamps shifts every inline timestamp tag by the given offset.
// Handles both single timestamps ([Audio: HH:MM:SS]) and range timestamps
// ([Video: HH:MM:SS - HH:MM:SS]). Used to convert chunk-relative timestamps
// into absolute timestamps after physical splitting of long media files.
func offsetTimestamps(markdown string, offset time.Duration) string {
	if offset == 0 || markdown == "" {
		return markdown
	}
	return inlineTimestampPattern.ReplaceAllStringFunc(markdown, func(match string) string {
		m := inlineTimestampPattern.FindStringSubmatch(match)
		if len(m) < 3 {
			return match
		}
		tag := m[1]

		ts1, err := parseTimestampDuration(m[2])
		if err != nil {
			return match
		}
		shifted1 := ts1 + offset
		if shifted1 < 0 {
			shifted1 = 0
		}

		if m[3] != "" {
			ts2, err2 := parseTimestampDuration(m[3])
			if err2 != nil {
				return match
			}
			shifted2 := ts2 + offset
			if shifted2 < 0 {
				shifted2 = 0
			}
			return fmt.Sprintf("[%s: %s - %s]", tag, formatTimestamp(shifted1), formatTimestamp(shifted2))
		}
		return fmt.Sprintf("[%s: %s]", tag, formatTimestamp(shifted1))
	})
}

// ChunkInfo describes one physical chunk produced by SplitMediaChunksActivity.
type ChunkInfo struct {
	Index       int           // 0-based chunk index
	MinIOPath   string        // MinIO object path for this chunk
	StartOffset time.Duration // Absolute start time in the original media
	EndOffset   time.Duration // Absolute end time in the original media
}

// SplitMediaChunksActivityParam defines parameters for SplitMediaChunksActivity.
type SplitMediaChunksActivityParam struct {
	Bucket          string           // MinIO bucket of the standardized file
	Destination     string           // MinIO path of the standardized file
	FileUID         string           // File UID (for temp path namespacing)
	WorkflowRunID   string           // Workflow run ID (for temp path namespacing)
	DurationSeconds float64          // Total media duration in seconds (from GetMediaDurationActivity)
	ChunkDuration   time.Duration    // Target duration per chunk (e.g., MaxVideoChunkDuration)
	FileType        artifactpb.File_Type
	Metadata        *structpb.Struct // Request metadata for authentication
}

// SplitMediaChunksActivityResult contains the produced chunks.
type SplitMediaChunksActivityResult struct {
	Chunks []ChunkInfo
}

// SplitMediaChunksActivity downloads a standardized media file from MinIO,
// physically splits it into overlapping chunks with ffmpeg -c copy, uploads
// each chunk to a temp MinIO path, and returns chunk metadata.
func (w *Worker) SplitMediaChunksActivity(ctx context.Context, param *SplitMediaChunksActivityParam) (*SplitMediaChunksActivityResult, error) {
	logger := w.log
	logger.Info("SplitMediaChunksActivity: Starting media split",
		zap.String("fileUID", param.FileUID),
		zap.Float64("durationSeconds", param.DurationSeconds),
		zap.Duration("chunkDuration", param.ChunkDuration))

	authCtx := ctx
	if param.Metadata != nil {
		var err error
		authCtx, err = CreateAuthenticatedContext(ctx, param.Metadata)
		if err != nil {
			logger.Warn("SplitMediaChunksActivity: auth context creation failed, proceeding without auth", zap.Error(err))
		}
	}

	content, err := w.repository.GetMinIOStorage().GetFile(authCtx, param.Bucket, param.Destination)
	if err != nil {
		return nil, fmt.Errorf("download media for splitting: %w", err)
	}

	tmpInput, err := os.CreateTemp("", "media-split-input-*.mp4")
	if err != nil {
		return nil, fmt.Errorf("create temp input file: %w", err)
	}
	defer os.Remove(tmpInput.Name())

	if _, err := tmpInput.Write(content); err != nil {
		tmpInput.Close()
		return nil, fmt.Errorf("write temp input file: %w", err)
	}
	tmpInput.Close()

	totalDuration := time.Duration(param.DurationSeconds * float64(time.Second))
	chunkDur := param.ChunkDuration
	overlap := ChunkOverlap

	var chunks []ChunkInfo
	for i := 0; ; i++ {
		start := time.Duration(i) * chunkDur
		if start >= totalDuration {
			break
		}

		isLast := start+chunkDur >= totalDuration
		var segLen time.Duration
		if isLast {
			segLen = totalDuration - start
		} else {
			segLen = chunkDur + overlap
		}

		tmpChunk, err := os.CreateTemp("", fmt.Sprintf("media-chunk-%03d-*.mp4", i))
		if err != nil {
			return nil, fmt.Errorf("create temp chunk file %d: %w", i, err)
		}
		tmpChunkPath := tmpChunk.Name()
		tmpChunk.Close()
		defer os.Remove(tmpChunkPath)

		args := []string{
			"-ss", fmt.Sprintf("%.3f", start.Seconds()),
			"-t", fmt.Sprintf("%.3f", segLen.Seconds()),
			"-i", tmpInput.Name(),
			"-c", "copy",
			"-movflags", "+faststart",
			"-y", tmpChunkPath,
		}

		if out, err := exec.Command("ffmpeg", args...).CombinedOutput(); err != nil {
			return nil, fmt.Errorf("ffmpeg split chunk %d failed: %w\noutput: %s", i, err, string(out))
		}

		chunkData, err := os.ReadFile(tmpChunkPath)
		if err != nil {
			return nil, fmt.Errorf("read chunk %d: %w", i, err)
		}

		minioPath := fmt.Sprintf("temp/chunks/%s/%s/chunk-%03d.mp4", param.WorkflowRunID, param.FileUID, i)
		base64Content := base64.StdEncoding.EncodeToString(chunkData)
		if err := w.repository.GetMinIOStorage().UploadBase64File(authCtx, param.Bucket, minioPath, base64Content, "video/mp4"); err != nil {
			return nil, fmt.Errorf("upload chunk %d to MinIO: %w", i, err)
		}

		endOffset := start + segLen
		if endOffset > totalDuration {
			endOffset = totalDuration
		}

		chunks = append(chunks, ChunkInfo{
			Index:       i,
			MinIOPath:   minioPath,
			StartOffset: start,
			EndOffset:   endOffset,
		})

		logger.Info("SplitMediaChunksActivity: Chunk created",
			zap.Int("index", i),
			zap.String("minioPath", minioPath),
			zap.Duration("start", start),
			zap.Duration("end", endOffset))
	}

	logger.Info("SplitMediaChunksActivity: Split complete",
		zap.Int("totalChunks", len(chunks)))

	return &SplitMediaChunksActivityResult{Chunks: chunks}, nil
}

// mergeHTMLTables joins batch outputs and merges HTML tables that were split
// at batch boundaries. The system instruction specifies HTML for tables, so we
// detect unclosed <table> tags at the end of one batch and table continuation
// at the start of the next.
func mergeHTMLTables(batches []string) string {
	if len(batches) == 0 {
		return ""
	}
	if len(batches) == 1 {
		return batches[0]
	}

	var b strings.Builder
	b.WriteString(batches[0])

	for i := 1; i < len(batches); i++ {
		prev := b.String()
		next := batches[i]

		openCount := strings.Count(strings.ToLower(prev), "<table")
		closeCount := strings.Count(strings.ToLower(prev), "</table")

		if openCount > closeCount {
			trimmedNext := strings.TrimLeft(next, " \t\r\n")
			lowerNext := strings.ToLower(trimmedNext)
			if strings.HasPrefix(lowerNext, "<table") {
				// Next batch starts with a new <table> for the continued table.
				// Strip the duplicate <table...> open tag and merge rows directly.
				tagEnd := strings.Index(trimmedNext, ">")
				if tagEnd >= 0 {
					next = trimmedNext[tagEnd+1:]
				}
			}
			b.WriteString("\n")
			b.WriteString(next)
		} else {
			b.WriteString("\n\n")
			b.WriteString(next)
		}
	}

	return b.String()
}
