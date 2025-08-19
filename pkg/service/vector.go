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
	FileUID      uuid.UUID
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
	FileUIDs     []uuid.UUID
	FileType     string
	ContentType  string

	// The filename filter was implemented back when the filename in a catalog was
	// unique, which isn't the case anymore. Using this filter might yield
	// unexpected results if there are several files with the same name in the
	// collection.
	// We need this field, however, as a fallback for collections that don't
	// have a file UID in the schema. Some collections have rigid schemas
	// without dynamic fields, so the original schema (with filename) couldn't
	// be extended and backfilled to have a file UID.
	FileNames []string
}

// VectorDatabase implements the use necesasry cases to interact with a vector
// database.
type VectorDatabase interface {
	CreateCollection(_ context.Context, id string) error
	InsertVectorsInCollection(_ context.Context, collID string, embeddings []Embedding) error
	DropCollection(_ context.Context, id string) error
	SimilarVectorsInCollection(context.Context, SimilarVectorSearchParam) ([][]SimilarEmbedding, error)
	DeleteEmbeddingsInCollection(_ context.Context, collID string, embeddingUID []string) error
	// CheckFileUIDMetadata checks if the collection has the file UID metadata
	// field, which wasn't introduced since the beginning and is not present in
	// legacy collections.
	CheckFileUIDMetadata(_ context.Context, collectionID string) (bool, error)
}

const kbCollectionPrefix = "kb_"

// KBCollectionName returns the collection name for a given knowledge base,
// identified by its uuid-formatted UID.
// For historical reasons, collection names can only contain numbers, letters
// and underscores, so UUID is here converted to a valid collection name.
func KBCollectionName(uid uuid.UUID) string {
	return kbCollectionPrefix + strings.ReplaceAll(uid.String(), "-", "_")
}
