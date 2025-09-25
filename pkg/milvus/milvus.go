package milvus

import (
	"context"
	"fmt"
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

func (m *milvusClient) CreateCollection(ctx context.Context, collectionName string) error {
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
	vectorDim := fmt.Sprintf("%d", vectorDim)
	schema := &entity.Schema{
		CollectionName: collectionName,
		Description:    "",
		Fields: []*entity.Field{
			{Name: kbCollectionFieldSourceTable, DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "255"}},
			{Name: kbCollectionFieldSourceUID, DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "255"}},
			{Name: kbCollectionFieldEmbeddingUID, DataType: entity.FieldTypeVarChar, PrimaryKey: true, TypeParams: map[string]string{"max_length": "255"}},
			{Name: kbCollectionFieldEmbedding, DataType: entity.FieldTypeFloatVector, TypeParams: map[string]string{"dim": vectorDim}},
			{Name: kbCollectionFieldFileUID, DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "255"}},
			{Name: kbCollectionFieldFileName, DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "255"}},
			{Name: kbCollectionFieldFileType, DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "255"}},
			{Name: kbCollectionFieldContentType, DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "255"}},
		},
	}

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
	} {
		err = m.c.CreateIndex(ctx, collectionName, field, idx, false)
		if err != nil {
			return fmt.Errorf("creating index for field %s: %w", field, err)
		}
	}

	logger.Info("Collection created successfully.")
	return nil
}

func (m *milvusClient) InsertVectorsInCollection(ctx context.Context, collectionName string, embeddings []service.Embedding) error {
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
	}

	// Create the columns for insertion
	columns := []entity.Column{
		entity.NewColumnVarChar(kbCollectionFieldSourceTable, sourceTables),
		entity.NewColumnVarChar(kbCollectionFieldSourceUID, sourceUIDs),
		entity.NewColumnVarChar(kbCollectionFieldEmbeddingUID, embeddingUIDs),
		entity.NewColumnFloatVector(kbCollectionFieldEmbedding, vectorDim, vectors),
	}

	hasMetadata, hasFileUID, err := m.checkMetadataFields(ctx, collectionName)
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

	// Flush the collection with retry
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err = m.c.Flush(ctx, collectionName, false)
		if err == nil {
			break
		}
		logger.Warn("Failed to flush collection, retrying", zap.Int("attempt", attempt), zap.Error(err))
		time.Sleep(time.Second * time.Duration(attempt))
	}
	if err != nil {
		return fmt.Errorf("flushing collection after insertion: %w", err)
	}

	logger.Info("Successfully inserted and flushed vectors")
	return nil
}

func (m *milvusClient) DeleteEmbeddingsWithFileUID(ctx context.Context, collectionName string, fileUID uuid.UUID) error {
	_, hasFileUID, err := m.checkMetadataFields(ctx, collectionName)
	if err != nil {
		return fmt.Errorf("checking metadata fields: %w", err)
	}

	if !hasFileUID {
		return nil
	}

	if err = m.c.LoadCollection(ctx, collectionName, false); err != nil {
		return fmt.Errorf("loading collection: %w", err)
	}

	expr := fmt.Sprintf("%s == '%s'", kbCollectionFieldFileUID, fileUID.String())
	if err := m.c.Delete(ctx, collectionName, "", expr); err != nil {
		return fmt.Errorf("deleting embeddings: %w", err)
	}

	return nil
}

func (m *milvusClient) DeleteEmbeddingsInCollection(ctx context.Context, collectionName string, embeddingUID []string) error {
	// Construct the delete expression
	// The expression should be in the format: "embedding_uid in ['pk1', 'pk2', ...]"
	expr := fmt.Sprintf("embedding_uid in ['%s']", strings.Join(embeddingUID, "','"))

	err := m.c.Delete(ctx, collectionName, "", expr)
	if err != nil {
		return fmt.Errorf("failed to delete embeddings: %w", err)
	}
	err = m.c.Flush(ctx, collectionName, false)
	if err != nil {
		return fmt.Errorf("failed to flush collection after deletion: %w", err)
	}
	return err
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

	hasMetadata, hasFileUID, err := m.checkMetadataFields(ctx, collectionName)
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

func (m *milvusClient) DropCollection(ctx context.Context, collectionName string) error {
	return m.c.DropCollection(ctx, collectionName)
}

func (m *milvusClient) CheckFileUIDMetadata(ctx context.Context, collectionName string) (bool, error) {
	_, hasFileUID, err := m.checkMetadataFields(ctx, collectionName)
	return hasFileUID, err
}

// checkMetadataFields returns whether the collection schema has metadata
// fields. Additionally, it checks the file UID metadata field separately as it
// was introduced later and certain legacy collections don't have it.
func (m *milvusClient) checkMetadataFields(ctx context.Context, collectionName string) (hasMetadata, hasFileUID bool, _ error) {
	collDesc, err := m.c.DescribeCollection(ctx, collectionName)
	if err != nil {
		return false, false, fmt.Errorf("describing collection: %w", err)
	}

	var existingFields = map[string]bool{}
	for _, field := range collDesc.Schema.Fields {
		existingFields[field.Name] = true
	}

	hasMetadata = existingFields[kbCollectionFieldFileName] &&
		existingFields[kbCollectionFieldFileType] &&
		existingFields[kbCollectionFieldContentType]

	hasFileUID = existingFields[kbCollectionFieldFileUID]

	return hasMetadata, hasFileUID, nil
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
