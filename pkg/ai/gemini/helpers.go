package gemini

import "slices"

// isVideoType checks if the MIME type is a video
func isVideoType(mimeType string) bool {
	return len(mimeType) >= 5 && mimeType[:5] == "video"
}

// isTextBasedContent checks if the MIME type is text-based content that should be sent as text, not as a blob
// Gemini API expects text content (plain text, HTML, Markdown, CSV) to be in Text parts for proper understanding
func isTextBasedContent(mimeType string) bool {
	textBasedTypes := []string{
		"text/plain",
		"text/html",
		"text/markdown",
		"text/csv",
	}
	return slices.Contains(textBasedTypes, mimeType)
}
