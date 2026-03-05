package worker

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"go.uber.org/zap"
)

// isDeadlineExceeded checks whether an error indicates a Gemini server-side
// processing timeout (DEADLINE_EXCEEDED / HTTP 504).
func isDeadlineExceeded(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "DEADLINE_EXCEEDED") || strings.Contains(msg, "504")
}

// getPageCount asks the AI model (via the existing cache) how many pages the
// document has. The call is lightweight because the file content is already
// cached — only the small prompt travels over the wire.
func (w *Worker) getPageCount(ctx context.Context, cacheName string) (int, error) {
	result, err := w.aiClient.ConvertToMarkdownWithCache(ctx, cacheName, ChunkedConversionPageCountPrompt)
	if err != nil {
		return 0, fmt.Errorf("failed to get page count: %w", err)
	}

	text := strings.TrimSpace(result.Markdown)
	// The model might include non-numeric characters; extract the first integer.
	pageCount, err := strconv.Atoi(text)
	if err != nil {
		// Try to extract a number from the response in case of extra text
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

// convertInPageRanges converts a large document in fixed-size page-range chunks
// using the existing cache. Each chunk produces markdown for a subset of pages,
// and the outputs are concatenated to form the complete document.
func (w *Worker) convertInPageRanges(ctx context.Context, cacheName string, pageCount int, prompt string) (string, error) {
	pagesPerChunk := ChunkedConversionPagesPerChunk
	var chunks []string

	for start := 1; start <= pageCount; start += pagesPerChunk {
		end := start + pagesPerChunk - 1
		if end > pageCount {
			end = pageCount
		}

		pageRangePrompt := fmt.Sprintf(
			"[CRITICAL INSTRUCTION] Convert ONLY pages %d through %d of this document. "+
				"Do NOT process any pages outside this range. Begin output with [Page: %d].\n\n%s",
			start, end, start, prompt,
		)

		w.log.Info("Converting page range",
			zap.Int("start", start),
			zap.Int("end", end),
			zap.Int("totalPages", pageCount))

		result, err := w.aiClient.ConvertToMarkdownWithCache(ctx, cacheName, pageRangePrompt)
		if err != nil {
			return "", fmt.Errorf("chunked conversion failed for pages %d-%d: %w", start, end, err)
		}

		chunks = append(chunks, result.Markdown)
	}

	return strings.Join(chunks, "\n\n"), nil
}
