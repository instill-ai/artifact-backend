package repository

import (
	"fmt"
	"math"
	"regexp"
	"strings"

	"github.com/milvus-io/milvus/client/v2/entity"
)

// BM25 constants
const (
	// BM25 parameters (standard values)
	k1 = 1.5  // Term frequency saturation parameter
	b  = 0.75 // Length normalization parameter
)

// BM25Encoder handles sparse vector generation using BM25 algorithm
type BM25Encoder struct {
	// Vocabulary maps tokens to indices
	vocabulary map[string]int32
	// Document frequencies for each token
	docFreqs map[string]int
	// Total number of documents
	numDocs int
	// Average document length
	avgDocLen float64
	// Compiled regex for tokenization
	tokenizer *regexp.Regexp
}

// NewBM25Encoder creates a new BM25 encoder
func NewBM25Encoder() *BM25Encoder {
	return &BM25Encoder{
		vocabulary: make(map[string]int32),
		docFreqs:   make(map[string]int),
		numDocs:    0,
		avgDocLen:  0,
		// Simple tokenizer: splits on non-alphanumeric, lowercases
		tokenizer: regexp.MustCompile(`[^\w]+`),
	}
}

// Tokenize splits text into tokens
func (e *BM25Encoder) Tokenize(text string) []string {
	// Lowercase and split on non-word characters
	text = strings.ToLower(text)
	tokens := e.tokenizer.Split(text, -1)

	// Filter empty tokens
	result := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if len(token) > 0 {
			result = append(result, token)
		}
	}

	return result
}

// Fit builds the vocabulary and document frequencies from a corpus
// This should be called once with all chunk texts before encoding
func (e *BM25Encoder) Fit(corpus []string) {
	// Reset state
	e.vocabulary = make(map[string]int32)
	e.docFreqs = make(map[string]int)
	e.numDocs = len(corpus)

	totalLen := 0
	vocabIndex := int32(0)

	// Build vocabulary and document frequencies
	for _, doc := range corpus {
		tokens := e.Tokenize(doc)
		totalLen += len(tokens)

		// Track unique tokens in this document
		seen := make(map[string]bool)
		for _, token := range tokens {
			if !seen[token] {
				e.docFreqs[token]++
				seen[token] = true
			}

			// Add to vocabulary if new
			if _, exists := e.vocabulary[token]; !exists {
				e.vocabulary[token] = vocabIndex
				vocabIndex++
			}
		}
	}

	// Calculate average document length
	if e.numDocs > 0 {
		e.avgDocLen = float64(totalLen) / float64(e.numDocs)
	}
}

// calculateIDF computes the Inverse Document Frequency for a token
func (e *BM25Encoder) calculateIDF(token string) float64 {
	docFreq := e.docFreqs[token]
	if docFreq == 0 {
		return 0
	}

	// IDF formula: log((N - df + 0.5) / (df + 0.5) + 1)
	// where N = total docs, df = document frequency
	numerator := float64(e.numDocs-docFreq) + 0.5
	denominator := float64(docFreq) + 0.5

	return math.Log(numerator/denominator + 1)
}

// Encode converts text to a BM25 sparse vector
func (e *BM25Encoder) Encode(text string) (entity.SparseEmbedding, error) {
	tokens := e.Tokenize(text)
	docLen := len(tokens)

	// Count term frequencies
	termFreqs := make(map[string]int)
	for _, token := range tokens {
		termFreqs[token]++
	}

	// Calculate BM25 scores
	// BM25 formula: IDF(token) * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * docLen / avgDocLen))
	scores := make(map[int32]float32)

	for token, tf := range termFreqs {
		// Skip tokens not in vocabulary
		idx, exists := e.vocabulary[token]
		if !exists {
			continue
		}

		idf := e.calculateIDF(token)

		// BM25 score calculation
		numerator := float64(tf) * (k1 + 1)
		denominator := float64(tf) + k1*(1-b+b*float64(docLen)/e.avgDocLen)

		score := idf * (numerator / denominator)
		scores[idx] = float32(score)
	}

	// Convert to sparse embedding format
	positions := make([]uint32, 0, len(scores))
	values := make([]float32, 0, len(scores))

	for idx, score := range scores {
		if score > 0 { // Only include non-zero scores
			positions = append(positions, uint32(idx))
			values = append(values, score)
		}
	}

	// Use Milvus SDK v2.4.2 API to create sparse embedding
	return entity.NewSliceSparseEmbedding(positions, values)
}

// EncodeDocuments is a convenience method to encode multiple documents
func (e *BM25Encoder) EncodeDocuments(documents []string) ([]entity.SparseEmbedding, error) {
	result := make([]entity.SparseEmbedding, len(documents))
	for i, doc := range documents {
		sparse, err := e.Encode(doc)
		if err != nil {
			return nil, fmt.Errorf("encoding document %d: %w", i, err)
		}
		result[i] = sparse
	}
	return result, nil
}

// EncodeQueries encodes query texts into sparse vectors for hybrid search
// In BM25, query encoding is the same as document encoding
func (e *BM25Encoder) EncodeQueries(queries []string) ([]entity.SparseEmbedding, error) {
	return e.EncodeDocuments(queries)
}
