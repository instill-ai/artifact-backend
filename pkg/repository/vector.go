package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gofrs/uuid"

	"github.com/instill-ai/artifact-backend/pkg/types"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"go.uber.org/zap"

	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
)

// VectorEmbedding is a vector representation of an object (extracted from a file as
// part or the whole of its contents) in a collection.
type VectorEmbedding struct {
	SourceTable  string
	SourceUID    string
	EmbeddingUID string
	Vector       []float32
	FileUID      types.FileUIDType
	Filename     string
	ContentType  string // MIME type (e.g., "text/markdown", "application/pdf")
	ChunkType    string // Chunk classification ("content", "summary", "augmented")
	Tags         []string
}

// SimilarVectorEmbedding extends VectorEmbedding to add a similarity search score.
type SimilarVectorEmbedding struct {
	VectorEmbedding
	Score float32
}

// SearchVectorParam contains the parameters for a similarity vector
// search.
type SearchVectorParam struct {
	CollectionID string
	Vectors      [][]float32
	TopK         uint32
	FileUIDs     []types.FileUIDType
	ContentType  string // MIME type filter (e.g., "text/markdown", "application/pdf")
	ChunkType    string // Chunk classification filter ("content", "summary", "augmented")

	// The filename filter was implemented back when the filename in a catalog was
	// unique, which isn't the case anymore. Using this filter might yield
	// unexpected results if there are several files with the same name in the
	// collection.
	// We need this field, however, as a fallback for collections that don't
	// have a file UID in the schema. Some collections have rigid schemas
	// without dynamic fields, so the original schema (with filename) couldn't
	// be extended and backfilled to have a file UID.
	Filenames []string
}

// VectorDatabase implements the necessary use cases to interact with a vector
// database (e.g., Milvus).
type VectorDatabase interface {
	CreateCollection(_ context.Context, id string, dimensionality uint32) error
	InsertVectorsInCollection(_ context.Context, collID string, embeddings []VectorEmbedding) error
	DropCollection(_ context.Context, id string) error
	SearchVectorsInCollection(context.Context, SearchVectorParam) ([][]SimilarVectorEmbedding, error)
	DeleteEmbeddingsWithFileUID(_ context.Context, collID string, fileUID types.FileUIDType) error
	// CheckFileUIDMetadata checks if the collection has the file UID metadata
	// field, which wasn't introduced since the beginning and is not present in
	// legacy collections.
	CheckFileUIDMetadata(_ context.Context, collectionID string) (bool, error)
	// FlushCollection flushes a collection to persist data immediately
	FlushCollection(_ context.Context, collectionID string) error
	// CollectionExists checks if a collection exists in the vector database
	CollectionExists(_ context.Context, collectionID string) (bool, error)
}

// Milvus implementation constants
const (
	scanNList  = 1024
	metricType = entity.COSINE
	withRaw    = true

	nProbe   = 250
	reorderK = 250

	kbCollectionFieldSourceTable  = "source_table"
	kbCollectionFieldSourceUID    = "source_uid"
	kbCollectionFieldEmbeddingUID = "embedding_uid"
	kbCollectionFieldEmbedding    = "embedding"
	kbCollectionFieldFileUID      = "file_uid"
	kbCollectionFieldFileName     = "file_name"
	kbCollectionFieldFileType     = "file_type"
	kbCollectionFieldContentType  = "content_type"
	kbCollectionFieldTags         = "tags"
)

type milvusClient struct {
	c client.Client
}

// NewVectorDatabase returns a VectorDatabase implementation (milvus).
func NewVectorDatabase(ctx context.Context, host, port string) (db VectorDatabase, closeFn func() error, _ error) {
	c, err := client.NewGrpcClient(ctx, host+":"+port)
	if err != nil {
		return nil, nil, err
	}

	return &milvusClient{
		c: c,
	}, c.Close, nil
}

func (m *milvusClient) CreateCollection(ctx context.Context, collectionName string, dimensionality uint32) error {
	logger, _ := logx.GetZapLogger(ctx)
	logger = logger.With(zap.String("collection_name", collectionName), zap.Uint32("dimensionality", dimensionality))

	// 1. Check if the collection already exists
	has, err := m.c.HasCollection(ctx, collectionName)
	if err != nil {
		return fmt.Errorf("checking collection existence: %w", err)
	}
	if has {
		logger.Info("Skipping collection creation: already exists.")
		return nil
	}

	// 2. Create the collection with the specified schema
	// Use the provided dimensionality from KB's system config
	vectorDimStr := fmt.Sprintf("%d", dimensionality)
	schema := &entity.Schema{
		CollectionName: collectionName,
		Description:    "",
		Fields: []*entity.Field{
			{Name: kbCollectionFieldSourceTable, DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "255"}},
			{Name: kbCollectionFieldSourceUID, DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "255"}},
			{Name: kbCollectionFieldEmbeddingUID, DataType: entity.FieldTypeVarChar, PrimaryKey: true, TypeParams: map[string]string{"max_length": "255"}},
			{Name: kbCollectionFieldEmbedding, DataType: entity.FieldTypeFloatVector, TypeParams: map[string]string{"dim": vectorDimStr}},
			{Name: kbCollectionFieldFileUID, DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "255"}},
			{Name: kbCollectionFieldFileName, DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "255"}},
			{Name: kbCollectionFieldFileType, DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "255"}},
			{Name: kbCollectionFieldContentType, DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "255"}},
			{Name: kbCollectionFieldTags, DataType: entity.FieldTypeArray, ElementType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_capacity": "100", "max_length": "255"}},
		},
	}

	err = m.c.CreateCollection(ctx, schema, 1)
	if err != nil {
		return fmt.Errorf("creating collection: %w", err)
	}

	// 3. Create indexes
	vectorIdx, err := entity.NewIndexSCANN(metricType, scanNList, withRaw)
	if err != nil {
		return fmt.Errorf("building index: %w", err)
	}

	for field, idx := range map[string]entity.Index{
		kbCollectionFieldEmbedding: vectorIdx,
		kbCollectionFieldFileUID:   entity.NewScalarIndexWithType(entity.Inverted),
	} {
		err = m.c.CreateIndex(ctx, collectionName, field, idx, false)
		if err != nil {
			return fmt.Errorf("creating index for field %s: %w", field, err)
		}
	}

	logger.Info("Collection created successfully.")
	return nil
}

func (m *milvusClient) InsertVectorsInCollection(ctx context.Context, collectionName string, embeddings []VectorEmbedding) error {
	logger, _ := logx.GetZapLogger(ctx)
	logger = logger.With(zap.String("collection_name", collectionName))

	// Check if the collection exists
	has, err := m.c.HasCollection(ctx, collectionName)
	if err != nil {
		return fmt.Errorf("checking collection existence: %w", err)
	}
	if !has {
		return fmt.Errorf("collection does not exist: %w", errorsx.ErrNotFound)
	}

	// Get collection schema to determine vector dimension
	collection, err := m.c.DescribeCollection(ctx, collectionName)
	if err != nil {
		return fmt.Errorf("describing collection: %w", err)
	}

	// Find the embedding field to get its dimension
	var vectorDim int
	for _, field := range collection.Schema.Fields {
		if field.Name == kbCollectionFieldEmbedding {
			if dimStr, ok := field.TypeParams["dim"]; ok {
				if _, err := fmt.Sscanf(dimStr, "%d", &vectorDim); err != nil {
					return fmt.Errorf("failed to parse vector dimension: %w", err)
				}
			}
			break
		}
	}
	if vectorDim == 0 {
		return fmt.Errorf("could not determine vector dimension from collection schema")
	}

	// Prepare the data for insertion
	vectorCount := len(embeddings)
	sourceTables := make([]string, vectorCount)
	sourceUIDs := make([]string, vectorCount)
	embeddingUIDs := make([]string, vectorCount) // Use the provided embeddingUID instead of generating a new one
	vectors := make([][]float32, vectorCount)
	fileUIDs := make([]string, vectorCount)
	fileNames := make([]string, vectorCount)
	fileTypes := make([]string, vectorCount)
	contentTypes := make([]string, vectorCount)
	tags := make([][]string, vectorCount)

	for i, embedding := range embeddings {
		sourceTables[i] = embedding.SourceTable
		sourceUIDs[i] = embedding.SourceUID
		embeddingUIDs[i] = embedding.EmbeddingUID // Use the embeddingUID from the input struct
		fileUIDs[i] = embedding.FileUID.String()
		fileNames[i] = embedding.Filename
		fileTypes[i] = embedding.ContentType  // MIME type
		contentTypes[i] = embedding.ChunkType // chunk type
		tags[i] = embedding.Tags
		vectors[i] = make([]float32, len(embedding.Vector))
		for j, val := range embedding.Vector {
			vectors[i][j] = float32(val)
		}
	}

	// Create the columns for insertion
	columns := []entity.Column{
		entity.NewColumnVarChar(kbCollectionFieldSourceTable, sourceTables),
		entity.NewColumnVarChar(kbCollectionFieldSourceUID, sourceUIDs),
		entity.NewColumnVarChar(kbCollectionFieldEmbeddingUID, embeddingUIDs),
		entity.NewColumnFloatVector(kbCollectionFieldEmbedding, vectorDim, vectors),
	}

	hasMetadata, hasFileUID, hasTags, err := m.checkMetadataFields(ctx, collectionName)
	if err != nil {
		return fmt.Errorf("checking metadata fields: %w", err)
	}

	if hasMetadata {
		columns = append(columns,
			entity.NewColumnVarChar(kbCollectionFieldFileName, fileNames),
			entity.NewColumnVarChar(kbCollectionFieldFileType, fileTypes),
			entity.NewColumnVarChar(kbCollectionFieldContentType, contentTypes),
		)
	}

	if hasFileUID {
		columns = append(columns, entity.NewColumnVarChar(kbCollectionFieldFileUID, fileUIDs))
	}

	if hasTags {
		// Convert [][]string to [][][]byte for Milvus array column
		tagsBytes := make([][][]byte, len(tags))
		for i, tagList := range tags {
			tagsBytes[i] = make([][]byte, len(tagList))
			for j, tag := range tagList {
				tagsBytes[i][j] = []byte(tag)
			}
		}
		columns = append(columns, entity.NewColumnVarCharArray(kbCollectionFieldTags, tagsBytes))
	}

	// Insert the data with retry
	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		_, err = m.c.Upsert(ctx, collectionName, "", columns...)
		if err == nil {
			break
		}
		logger.Warn("Failed to insert vectors, retrying", zap.Int("attempt", attempt), zap.Error(err))
		time.Sleep(time.Second * time.Duration(attempt))
	}
	if err != nil {
		return fmt.Errorf("inserting vectors: %w", err)
	}

	// Note: Flush removed for performance. Call FlushCollection separately after all batches complete.
	logger.Info("Successfully inserted vectors", zap.Int("count", vectorCount))
	return nil
}

// FlushCollection flushes a collection to persist all data immediately
func (m *milvusClient) FlushCollection(ctx context.Context, collectionName string) error {
	logger, _ := logx.GetZapLogger(ctx)
	logger = logger.With(zap.String("collection_name", collectionName))

	// Check if the collection exists
	has, err := m.c.HasCollection(ctx, collectionName)
	if err != nil {
		return fmt.Errorf("checking collection existence: %w", err)
	}
	if !has {
		return fmt.Errorf("collection does not exist: %w", errorsx.ErrNotFound)
	}

	// Flush the collection with retry
	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err = m.c.Flush(ctx, collectionName, false)
		if err == nil {
			break
		}
		logger.Warn("Failed to flush collection, retrying", zap.Int("attempt", attempt), zap.Error(err))
		time.Sleep(time.Second * time.Duration(attempt))
	}
	if err != nil {
		return fmt.Errorf("flushing collection: %w", err)
	}

	logger.Info("Successfully flushed collection")
	return nil
}

func (m *milvusClient) DeleteEmbeddingsWithFileUID(ctx context.Context, collectionName string, fileUID types.FileUIDType) error {
	logger, _ := logx.GetZapLogger(ctx)
	logger = logger.With(zap.String("collection_name", collectionName), zap.String("file_uid", fileUID.String()))

	// Check if collection exists first
	has, err := m.c.HasCollection(ctx, collectionName)
	if err != nil {
		return fmt.Errorf("checking collection existence: %w", err)
	}

	// If collection doesn't exist, there's nothing to delete - return success
	if !has {
		logger.Info("Collection does not exist, skipping delete")
		return nil
	}

	_, hasFileUID, _, err := m.checkMetadataFields(ctx, collectionName)
	if err != nil {
		return fmt.Errorf("checking metadata fields: %w", err)
	}

	if !hasFileUID {
		logger.Info("Collection does not have file_uid field, skipping delete")
		return nil
	}

	// Load collection if needed - check load state first to avoid redundant load
	loadState, err := m.c.GetLoadState(ctx, collectionName, []string{})
	if err != nil {
		return fmt.Errorf("checking load state: %w", err)
	}

	// Only load if not already loaded
	if loadState != entity.LoadStateLoaded {
		if err = m.c.LoadCollection(ctx, collectionName, false); err != nil {
			return fmt.Errorf("loading collection for delete: %w", err)
		}
		logger.Info("Collection loaded for delete operation")
	} else {
		logger.Info("Collection already loaded, skipping load")
	}

	expr := fmt.Sprintf("%s == '%s'", kbCollectionFieldFileUID, fileUID.String())
	if err := m.c.Delete(ctx, collectionName, "", expr); err != nil {
		return fmt.Errorf("deleting embeddings: %w", err)
	}

	logger.Info("Successfully deleted embeddings")
	return nil
}

func (m *milvusClient) fileUIDFilter(fileUIDs []types.FileUIDType) string {
	validUIDs := make([]string, 0, len(fileUIDs))
	for _, uid := range fileUIDs {
		if uid.IsNil() {
			continue
		}
		validUIDs = append(validUIDs, `"`+uid.String()+`"`)
	}

	if len(validUIDs) == 0 {
		return ""
	}

	return fmt.Sprintf("%s in [%s]", kbCollectionFieldFileUID, strings.Join(validUIDs, ","))
}

func (m *milvusClient) SearchVectorsInCollection(ctx context.Context, p SearchVectorParam) ([][]SimilarVectorEmbedding, error) {
	logger, _ := logx.GetZapLogger(ctx)

	collectionName := p.CollectionID
	topK := int(p.TopK)

	logger = logger.With(zap.String("collection_name", collectionName))

	// Check if the collection exists
	t := time.Now()
	has, err := m.c.HasCollection(ctx, collectionName)
	if err != nil {
		return nil, fmt.Errorf("checking collection existence: %w", err)
	}
	if !has {
		return nil, fmt.Errorf("checking collection existence: %w", errorsx.ErrNotFound)
	}

	logger.Info("Existence check.", zap.Duration("duration", time.Since(t)))
	t = time.Now()

	// Load the collection if it's not already loaded
	err = m.c.LoadCollection(ctx, collectionName, false)
	if err != nil {
		return nil, fmt.Errorf("loading collection: %w", err)
	}

	logger.Info("Collection load.", zap.Duration("duration", time.Since(t)))

	hasMetadata, hasFileUID, _, err := m.checkMetadataFields(ctx, collectionName)
	if err != nil {
		return nil, fmt.Errorf("checking metadata fields: %w", err)
	}

	outputFields := []string{
		kbCollectionFieldSourceTable,
		kbCollectionFieldSourceUID,
		kbCollectionFieldEmbeddingUID,
		kbCollectionFieldEmbedding,
	}
	var filterStrs []string
	if hasMetadata {
		outputFields = append(
			outputFields,
			kbCollectionFieldFileName,
			kbCollectionFieldFileType,
			kbCollectionFieldContentType,
		)

		if hasFileUID {
			outputFields = append(outputFields, kbCollectionFieldFileUID)

			filter := m.fileUIDFilter(p.FileUIDs)
			if filter != "" {
				filterStrs = append(filterStrs, filter)
			}
		} else if len(p.Filenames) > 0 {
			// Filename filter is only used for backwards compatibility in
			// collections that lack the file UID metadata.
			filter := fmt.Sprintf(`%s in ["%s"]`, kbCollectionFieldFileName, strings.Join(p.Filenames, `","`))
			filterStrs = append(filterStrs, filter)
		}

		if p.ContentType != "" {
			filterStrs = append(filterStrs, fmt.Sprintf("%s == '%s'", kbCollectionFieldFileType, p.ContentType))
		}

		if p.ChunkType != "" {
			filterStrs = append(filterStrs, fmt.Sprintf("%s == '%s'", kbCollectionFieldContentType, p.ChunkType))
		}
	}

	t = time.Now()
	// Convert the input vector to float32
	milvusVectors := make([]entity.Vector, len(p.Vectors))
	// milvus search vector support batch search, but we just need one vector
	for i, v := range p.Vectors {
		milvusVectors[i] = entity.FloatVector(v)
	}
	// Perform the search
	sp, err := entity.NewIndexSCANNSearchParam(nProbe, reorderK)
	if err != nil {
		return nil, fmt.Errorf("creating search param: %w", err)
	}

	filterExpr := strings.Join(filterStrs, " and ")

	results, err := m.c.Search(
		ctx,
		collectionName,
		nil,
		filterExpr,
		outputFields,
		milvusVectors,
		kbCollectionFieldEmbedding,
		metricType,
		topK,
		sp,
	)
	if err != nil {
		return nil, fmt.Errorf("searching embeddings: %w", err)
	}
	logger.Info("Embeddings search.", zap.Duration("duration", time.Since(t)))

	// Extract the embeddings from the search results
	var embeddings [][]SimilarVectorEmbedding
	for _, result := range results {
		if result.ResultCount == 0 {
			continue
		}
		sourceTables, err := getStringData(result.Fields.GetColumn(kbCollectionFieldSourceTable))
		if err != nil {
			return nil, fmt.Errorf("getting source table column value: %w", err)
		}

		sourceUIDs, err := getStringData(result.Fields.GetColumn(kbCollectionFieldSourceUID))
		if err != nil {
			return nil, fmt.Errorf("getting source UID column value: %w", err)
		}
		embeddingUIDs, err := getStringData(result.Fields.GetColumn(kbCollectionFieldEmbeddingUID))
		if err != nil {
			return nil, fmt.Errorf("getting embedding UID column value: %w", err)
		}
		vectors := result.Fields.GetColumn(kbCollectionFieldEmbedding).(*entity.ColumnFloatVector)
		scores := result.Scores

		// Extract metadata fields if available
		var fileUIDs, fileNames, fileTypes, contentTypes []string
		if hasMetadata {
			fileNames, err = getStringData(result.Fields.GetColumn(kbCollectionFieldFileName))
			if err != nil {
				return nil, fmt.Errorf("getting file name column value: %w", err)
			}
			fileTypes, err = getStringData(result.Fields.GetColumn(kbCollectionFieldFileType))
			if err != nil {
				return nil, fmt.Errorf("getting file type column value: %w", err)
			}
			contentTypes, err = getStringData(result.Fields.GetColumn(kbCollectionFieldContentType))
			if err != nil {
				return nil, fmt.Errorf("getting content type column value: %w", err)
			}
			if hasFileUID {
				fileUIDs, err = getStringData(result.Fields.GetColumn(kbCollectionFieldFileUID))
				if err != nil {
					return nil, fmt.Errorf("getting file UID column value: %w", err)
				}
			}
		}

		tempVectors := []SimilarVectorEmbedding{}
		for i := range sourceTables {
			emb := VectorEmbedding{
				SourceTable:  sourceTables[i],
				SourceUID:    sourceUIDs[i],
				EmbeddingUID: embeddingUIDs[i],
				Vector:       vectors.Data()[i],
			}
			if hasMetadata {
				emb.Filename = fileNames[i]
				emb.ContentType = fileTypes[i]  // MIME type from file_type field
				emb.ChunkType = contentTypes[i] // chunk type from content_type field
				if hasFileUID {
					emb.FileUID = uuid.FromStringOrNil(fileUIDs[i])
				}
			}
			tempVectors = append(tempVectors, SimilarVectorEmbedding{
				VectorEmbedding: emb,
				Score:           scores[i],
			})
		}
		embeddings = append(embeddings, tempVectors)
	}

	return embeddings, nil
}

func (m *milvusClient) DropCollection(ctx context.Context, collectionName string) error {
	return m.c.DropCollection(ctx, collectionName)
}

func (m *milvusClient) CheckFileUIDMetadata(ctx context.Context, collectionName string) (bool, error) {
	_, hasFileUID, _, err := m.checkMetadataFields(ctx, collectionName)
	return hasFileUID, err
}

// checkMetadataFields returns whether the collection schema has metadata
// fields. Additionally, it checks the file UID and tags metadata fields separately as they
// were introduced later and certain legacy collections don't have them.
func (m *milvusClient) checkMetadataFields(ctx context.Context, collectionName string) (hasMetadata, hasFileUID, hasTags bool, _ error) {
	collDesc, err := m.c.DescribeCollection(ctx, collectionName)
	if err != nil {
		return false, false, false, fmt.Errorf("describing collection: %w", err)
	}

	var existingFields = map[string]bool{}
	for _, field := range collDesc.Schema.Fields {
		existingFields[field.Name] = true
	}

	hasMetadata = existingFields[kbCollectionFieldFileName] &&
		existingFields[kbCollectionFieldFileType] &&
		existingFields[kbCollectionFieldContentType]

	hasFileUID = existingFields[kbCollectionFieldFileUID]
	hasTags = existingFields[kbCollectionFieldTags]

	return hasMetadata, hasFileUID, hasTags, nil
}

func getStringData(col entity.Column) ([]string, error) {
	switch v := col.(type) {
	case *entity.ColumnVarChar:
		return v.Data(), nil
	case *entity.ColumnString:
		return v.Data(), nil
	default:
		return nil, fmt.Errorf("unexpected column type for string data: %T", col)
	}
}

// CollectionExists checks if a collection exists in Milvus
func (m *milvusClient) CollectionExists(ctx context.Context, collectionID string) (bool, error) {
	logger, _ := logx.GetZapLogger(ctx)
	logger = logger.With(zap.String("collection_id", collectionID))

	has, err := m.c.HasCollection(ctx, collectionID)
	if err != nil {
		logger.Error("Failed to check collection existence",
			zap.String("collection", collectionID),
			zap.Error(err))
		return false, fmt.Errorf("checking collection existence: %w", err)
	}

	logger.Debug("Collection existence check completed",
		zap.Bool("exists", has))

	return has, nil
}
