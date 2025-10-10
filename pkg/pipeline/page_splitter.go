package pipeline

import (
	"fmt"

	"github.com/pkoukk/tiktoken-go"

	"github.com/instill-ai/artifact-backend/pkg/repository"
)

// PageSplitter can split a string into chunks according to a set of delimiters
// (i.e., byte positions).
type PageSplitter struct {
	PageDelimiters []uint32
}

// Split breaks a string into chunks according to the splitter's delimiters.
// Chunks with empty text are skipped.
func (ps *PageSplitter) Split(content string) ([]TextChunk, error) {
	chunks := make([]TextChunk, 0, len(ps.PageDelimiters))
	runes := []rune(content)

	var start uint32
	for i, delim := range ps.PageDelimiters {
		if int(delim) > len(runes) {
			return nil, fmt.Errorf("page delimiter exceeds content size")
		}

		page := uint32(i + 1) // pages are 1-indexed
		text := string(runes[start:delim])
		if len(text) == 0 {
			continue
		}

		tkm, err := tiktoken.EncodingForModel("gpt-4") // same model as in the chunking recipes
		if err != nil {
			return nil, fmt.Errorf("unsupported encoding model: %w", err)
		}
		tokenCount := len(tkm.Encode(text, nil, nil))

		chunk := TextChunk{
			Start:  int(start),
			End:    int(delim),
			Text:   text,
			Tokens: tokenCount,
			Reference: &repository.TextChunkReference{
				PageRange: [2]uint32{page, page},
			},
		}
		chunks = append(chunks, chunk)

		start = delim
	}

	return chunks, nil
}
