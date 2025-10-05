package milvus

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/service"

	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
)

const kbCollectionPrefix = "kb_"

// collectionName returns the collection name for a given knowledge base,
// identified by its uuid-formatted UID.
// For historical reasons, collection names can only contain numbers, letters
// and underscores, so UUID is here converted to a valid collection name.
func collectionName(uid uuid.UUID) string {
	return kbCollectionPrefix + strings.ReplaceAll(uid.String(), "-", "_")
}

const (
	vectorDim  = 1536
	scaNNList  = 1024
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

// NewVectorDatabase returns a VectorDatabase implementation for Milvus.
func NewVectorDatabase(ctx context.Context, host, port string) (db service.VectorDatabase, closeFn func() error, _ error) {
	c, err := client.NewGrpcClient(ctx, host+":"+port)
	if err != nil {
		return nil, nil, err
	}

	return &milvusClient{c: c}, c.Close, nil
}

func (m *milvusClient) CreateCollection(ctx context.Context, kbUID uuid.UUID) error {
	collectionName := collectionName(kbUID)
	logger, _ := logx.GetZapLogger(ctx)
	logger = logger.With(zap.String("collection_name", collectionName))

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
	schema := entity.NewSchema().
		WithName(collectionName).
		WithField(
			entity.NewField().
				WithName(kbCollectionFieldSourceTable).
				WithDataType(entity.FieldTypeVarChar).
				WithMaxLength(255),
		).
		WithField(
			entity.NewField().
				WithName(kbCollectionFieldSourceUID).
				WithDataType(entity.FieldTypeVarChar).
				WithMaxLength(255),
		).
		WithField(
			entity.NewField().
				WithName(kbCollectionFieldEmbeddingUID).
				WithDataType(entity.FieldTypeVarChar).
				WithIsPrimaryKey(true).
				WithMaxLength(255),
		).
		WithField(
			entity.NewField().
				WithName(kbCollectionFieldEmbedding).
				WithDataType(entity.FieldTypeFloatVector).
				WithDim(vectorDim),
		).
		WithField(
			entity.NewField().
				WithName(kbCollectionFieldFileUID).
				WithDataType(entity.FieldTypeVarChar).
				WithMaxLength(255),
		).
		WithField(
			entity.NewField().
				WithName(kbCollectionFieldFileName).
				WithDataType(entity.FieldTypeVarChar).
				WithMaxLength(255),
		).
		WithField(
			entity.NewField().
				WithName(kbCollectionFieldFileType).
				WithDataType(entity.FieldTypeVarChar).
				WithMaxLength(255),
		).
		WithField(
			entity.NewField().
				WithName(kbCollectionFieldContentType).
				WithDataType(entity.FieldTypeVarChar).
				WithMaxLength(255),
		).
		WithField(
			entity.NewField().
				WithName(kbCollectionFieldTags).
				WithDataType(entity.FieldTypeArray).
				WithMaxCapacity(1024). // TODO import from service
				WithElementType(entity.FieldTypeVarChar).
				WithMaxLength(255),
		)

	err = m.c.CreateCollection(ctx, schema, 1)
	if err != nil {
		return fmt.Errorf("creating collection: %w", err)
	}

	// 3. Create indexes
	vectorIdx, err := entity.NewIndexSCANN(metricType, scaNNList, withRaw)
	if err != nil {
		return fmt.Errorf("building index: %w", err)
	}

	for field, idx := range map[string]entity.Index{
		kbCollectionFieldEmbedding: vectorIdx,
		kbCollectionFieldFileUID:   entity.NewScalarIndexWithType(entity.Inverted),
		kbCollectionFieldTags:      entity.NewScalarIndexWithType(entity.Inverted),
	} {
		err = m.c.CreateIndex(ctx, collectionName, field, idx, false)
		if err != nil {
			return fmt.Errorf("creating index for field %s: %w", field, err)
		}
	}

	logger.Info("Collection created successfully.")
	return nil
}

func (m *milvusClient) UpsertVectorsInCollection(ctx context.Context, kbUID uuid.UUID, embeddings []service.Embedding) error {
	collectionName := collectionName(kbUID)
	logger, _ := logx.GetZapLogger(ctx)
	logger = logger.With(zap.String("collection_name", collectionName))

	// Check if the collection exists
	has, err := m.c.HasCollection(ctx, collectionName)
	if err != nil {
		return fmt.Errorf("checking collection existence: %w", err)
	}
	if !has {
		return fmt.Errorf("checking collection existence: %w", errorsx.ErrNotFound)
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
	tags := make([][][]byte, vectorCount)

	for i, embedding := range embeddings {
		sourceTables[i] = embedding.SourceTable
		sourceUIDs[i] = embedding.SourceUID
		embeddingUIDs[i] = embedding.EmbeddingUID // Use the embeddingUID from the input struct
		fileUIDs[i] = embedding.FileUID.String()
		fileNames[i] = embedding.FileName
		fileTypes[i] = embedding.FileType
		contentTypes[i] = embedding.ContentType

		vectors[i] = make([]float32, len(embedding.Vector))
		for j, val := range embedding.Vector {
			vectors[i][j] = float32(val)
		}

		tags[i] = make([][]byte, len(embedding.Tags))
		for j, tag := range embedding.Tags {
			tags[i][j] = []byte(tag)
		}
	}

	// Create the columns for insertion
	columns := []entity.Column{
		entity.NewColumnVarChar(kbCollectionFieldSourceTable, sourceTables),
		entity.NewColumnVarChar(kbCollectionFieldSourceUID, sourceUIDs),
		entity.NewColumnVarChar(kbCollectionFieldEmbeddingUID, embeddingUIDs),
		entity.NewColumnFloatVector(kbCollectionFieldEmbedding, vectorDim, vectors),
	}

	fields, err := m.extractCollectionFields(ctx, collectionName)
	if err != nil {
		return fmt.Errorf("checking metadata fields: %w", err)
	}

	if fields.hasLegacyMetadata() {
		columns = append(columns,
			entity.NewColumnVarChar(kbCollectionFieldFileName, fileNames),
			entity.NewColumnVarChar(kbCollectionFieldFileType, fileTypes),
			entity.NewColumnVarChar(kbCollectionFieldContentType, contentTypes),
		)
	}

	if fields.hasFileUID() {
		columns = append(columns, entity.NewColumnVarChar(kbCollectionFieldFileUID, fileUIDs))
	}

	if fields.hasTags() {
		columns = append(columns, entity.NewColumnVarCharArray(kbCollectionFieldTags, tags))
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

func (m *milvusClient) DeleteEmbeddingsWithFileUID(ctx context.Context, kbUID uuid.UUID, fileUID uuid.UUID) error {
	collectionName := collectionName(kbUID)
	
	// Check if collection exists first
	has, err := m.c.HasCollection(ctx, collectionName)
	if err != nil {
		return fmt.Errorf("checking collection existence: %w", err)
	}
	
	// If collection does not exist, there is nothing to delete - return success
	if !has {
		return nil
	}
	
	_, hasFileUID, err := m.checkMetadataFields(ctx, collectionName)
	if err != nil {
		return fmt.Errorf("checking metadata fields: %w", err)
	}
	if !hasFileUID {
		return fmt.Errorf("collection %s does not have file_uid field", collectionName)
	}
	
	if err = m.c.LoadCollection(ctx, collectionName, false); err != nil {
		return fmt.Errorf("loading collection for delete: %w", err)
	}
	
	expr := fmt.Sprintf("%s == '%s'", kbCollectionFieldFileUID, fileUID.String())
	if err := m.c.Delete(ctx, collectionName, "", expr); err != nil {
		return fmt.Errorf("deleting embeddings: %w", err)
	}
	
	return nil
	expr := fmt.Sprintf("%s == '%s'", kbCollectionFieldFileUID, fileUID.String())
	if err := m.c.Delete(ctx, collectionName, "", expr); err != nil {
		return fmt.Errorf("deleting embeddings: %w", err)
	}

	return nil
}

func (m *milvusClient) fileUIDFilter(fileUIDs []uuid.UUID) string {
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

func (m *milvusClient) SimilarVectorsInCollection(ctx context.Context, p service.SimilarVectorSearchParam) ([][]service.SimilarEmbedding, error) {
	logger, _ := logx.GetZapLogger(ctx)

	collectionName := collectionName(p.KnowledgeBaseUID)
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

	fields, err := m.extractCollectionFields(ctx, collectionName)
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
	fileUIDFilter := m.fileUIDFilter(p.FileUIDs)

	if fields.hasLegacyMetadata() {
		outputFields = append(
			outputFields,
			kbCollectionFieldFileName,
			kbCollectionFieldFileType,
			kbCollectionFieldContentType,
		)

		if fields.hasFileUID() {
			outputFields = append(outputFields, kbCollectionFieldFileUID)
			if fileUIDFilter != "" {
				filterStrs = append(filterStrs, fileUIDFilter)
			}
		} else if len(p.FileNames) > 0 {
			// Filename filter is only used for backwards compatibility in
			// collections that lack the file UID metadata.
			filter := fmt.Sprintf(`%s in ["%s"]`, kbCollectionFieldFileName, strings.Join(p.FileNames, `","`))
			filterStrs = append(filterStrs, filter)
		}

		if p.FileType != "" {
			filterStrs = append(filterStrs, fmt.Sprintf("%s == '%s'", kbCollectionFieldFileType, p.FileType))
		}

		if p.ContentType != "" {
			filterStrs = append(filterStrs, fmt.Sprintf("%s == '%s'", kbCollectionFieldContentType, p.ContentType))
		}
	}

	// Several files can share a tag, but all the embeddings extracted from a
	// file share the same tags. Therefore, a file UID filter takes precedence
	// over a tag filter.
	if len(p.Tags) > 0 && fileUIDFilter == "" {
		// If tags are provided but collection doesn't support them, ignore tag filter
		if !fields.hasTags() {
			logger.Info("Collection does not support tags, ignoring tag filter")
		} else {
			// Use ARRAY_CONTAINS_ANY for OR logic with multiple tags
			// Format: ARRAY_CONTAINS_ANY(tags, ["tag1", "tag2"])
			tagList := make([]string, len(p.Tags))
			for i, tag := range p.Tags {
				tagList[i] = fmt.Sprintf("\"%s\"", tag)
			}
			filter := fmt.Sprintf("ARRAY_CONTAINS_ANY(%s, [%s])", kbCollectionFieldTags, strings.Join(tagList, ","))
			filterStrs = append(filterStrs, filter)
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
	results, err := m.c.Search(
		ctx,
		collectionName,
		nil,
		strings.Join(filterStrs, " and "),
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
	var embeddings [][]service.SimilarEmbedding
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
		tempVectors := []service.SimilarEmbedding{}
		for i := 0; i < len(sourceTables); i++ {
			tempVectors = append(tempVectors, service.SimilarEmbedding{
				Embedding: service.Embedding{
					SourceTable:  sourceTables[i],
					SourceUID:    sourceUIDs[i],
					EmbeddingUID: embeddingUIDs[i],
					Vector:       vectors.Data()[i]},
				Score: scores[i],
			})
		}
		embeddings = append(embeddings, tempVectors)
	}

	return embeddings, nil
}

// DropCollection removes a collection from the vector database.
func (m *milvusClient) DropCollection(ctx context.Context, kbUID uuid.UUID) error {
	collectionName := collectionName(kbUID)
	return m.c.DropCollection(ctx, collectionName)
}

// CheckFileUIDMetadata returns whether the collection's schema allows for file
// UID metadata.
func (m *milvusClient) CheckFileUIDMetadata(ctx context.Context, kbUID uuid.UUID) (bool, error) {
	fields, err := m.extractCollectionFields(ctx, collectionName(kbUID))
	if err != nil {
		return false, err
	}

	return fields.hasFileUID(), nil
}

// CheckTagsMetadata returns whether the collection's schema allows for tag
// metadata.
func (m *milvusClient) CheckTagsMetadata(ctx context.Context, kbUID uuid.UUID) (bool, error) {
	fields, err := m.extractCollectionFields(ctx, collectionName(kbUID))
	if err != nil {
		return false, err
	}

	return fields.hasTags(), nil
}

// collectionFields is used to check the metadata present in a collection.
// Collections schemas have gone through modifications over time and this
// allows us to set the expectations when inserting and fetching data from
// them.
//
// TODO: There's an upcoming feature changing the embedding method (with a new
// vector size), which will require a collection migration. This is an
// opportunity to unify the collection schemas.
type collectionFields []string

func (cf collectionFields) hasLegacyMetadata() bool {
	return slices.Contains(cf, kbCollectionFieldFileName) &&
		slices.Contains(cf, kbCollectionFieldFileType) &&
		slices.Contains(cf, kbCollectionFieldContentType)
}

func (cf collectionFields) hasFileUID() bool {
	return slices.Contains(cf, kbCollectionFieldFileUID)
}

func (cf collectionFields) hasTags() bool {
	return slices.Contains(cf, kbCollectionFieldTags)
}

func (m *milvusClient) extractCollectionFields(ctx context.Context, collectionName string) (collectionFields, error) {
	collDesc, err := m.c.DescribeCollection(ctx, collectionName)
	if err != nil {
		return nil, fmt.Errorf("describing collection: %w", err)
	}

	fields := collectionFields(make([]string, len(collDesc.Schema.Fields)))
	for i, field := range collDesc.Schema.Fields {
		fields[i] = field.Name
	}

	return fields, nil
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
