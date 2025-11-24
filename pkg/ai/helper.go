package ai

import (
	"github.com/pkoukk/tiktoken-go"
)

// EstimateTokenCount estimates the token count for a single text string
// Returns the estimated token count (0 if estimation fails)
// Note: This is an approximation using GPT-4 tokenizer (actual AI client may count differently)
func EstimateTokenCount(text string) int {
	tkm, err := tiktoken.EncodingForModel("gpt-4")
	if err != nil {
		// If we can't get the tokenizer, use a rough estimate: ~4 chars per token
		return len(text) / 4
	}

	return len(tkm.Encode(text, nil, nil))
}
