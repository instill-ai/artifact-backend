package service

import (
	"context"
	"strings"

	"github.com/gofrs/uuid"
)

// Embedding is a vector representation of an object (extracted from a file as
// part or the whole of its contents) in a collection.
type Embedding struct {
	SourceTable  string
	SourceUID    string
	EmbeddingUID string
	Vector       []float32
	FileUID      string
	FileName     string
	FileType     string
	ContentType  string
}

// SimilarEmbedding extends Embedding to add a similarity search score.
type SimilarEmbedding struct {
	Embedding
	Score float32
}

// SimilarVectorSearchParam contains the parameters for a similarity vector
// search.
type SimilarVectorSearchParam struct {
	CollectionID string
	Vectors      [][]float32
	TopK         uint32
	FileName     string
	FileType     string
	ContentType  string
}

// VectorDatabase implements the use necesasry cases to interact with a vector
// database.
type VectorDatabase interface {
	CreateCollection(_ context.Context, id string) error
	InsertVectorsInCollection(_ context.Context, collID string, embeddings []Embedding) error
	DropCollection(_ context.Context, id string) error
	SimilarVectorsInCollection(context.Context, SimilarVectorSearchParam) ([][]SimilarEmbedding, error)
	DeleteEmbeddingsInCollection(_ context.Context, collID string, embeddingUID []string) error
}

const kbCollectionPrefix = "kb_"

// KBCollectionName returns the collection name for a given knowledge base,
// identified by its uuid-formatted UID.
// For historical reasons, collection names can only contain numbers, letters
// and underscores, so UUID is here converted to a valid collection name.
func KBCollectionName(uid uuid.UUID) string {
	return kbCollectionPrefix + strings.ReplaceAll(uid.String(), "-", "_")
}
