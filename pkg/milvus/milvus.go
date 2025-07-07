package milvus

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/service"
	"github.com/instill-ai/x/log"
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
	logger, _ := log.GetZapLogger(ctx)

	// 1. Check if the collection already exists
	has, err := m.c.HasCollection(ctx, collectionName)
	if err != nil {
		return fmt.Errorf("failed to check collection existence: %w", err)
	}
	if has {
		logger.Info("Collection already exists", zap.String("collection_name", collectionName))
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
			{Name: kbCollectionFieldFileName, DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "255"}},
			{Name: kbCollectionFieldFileType, DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "255"}},
			{Name: kbCollectionFieldContentType, DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "255"}},
		},
	}

	err = m.c.CreateCollection(ctx, schema, 1)
	if err != nil {
		return fmt.Errorf("failed to create collection: %w", err)
	}

	// 3. Create index
	index, err := entity.NewIndexSCANN(metricType, scaNNList, withRaw)
	if err != nil {
		logger.Error("Failed to create index", zap.Error(err))
		return fmt.Errorf("failed to create index: %w", err)
	}

	err = m.c.CreateIndex(ctx, collectionName, kbCollectionFieldEmbedding, index, false)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}

	logger.Info("Collection created successfully", zap.String("collection_name", collectionName))
	return nil
}

func (m *milvusClient) InsertVectorsInCollection(ctx context.Context, collectionName string, embeddings []service.Embedding) error {
	logger, _ := log.GetZapLogger(ctx)

	// Check if the collection exists
	has, err := m.c.HasCollection(ctx, collectionName)
	if err != nil {
		logger.Error("Failed to check collection existence", zap.Error(err))
		return fmt.Errorf("failed to check collection existence: %w", err)
	}
	if !has {
		logger.Error("Collection does not exist", zap.String("collection", collectionName))
		return fmt.Errorf("collection %s does not exist", collectionName)
	}

	// Prepare the data for insertion
	vectorCount := len(embeddings)
	sourceTables := make([]string, vectorCount)
	sourceUIDs := make([]string, vectorCount)
	embeddingUIDs := make([]string, vectorCount) // Use the provided embeddingUID instead of generating a new one
	vectors := make([][]float32, vectorCount)
	fileNames := make([]string, vectorCount)
	fileTypes := make([]string, vectorCount)
	contentTypes := make([]string, vectorCount)

	for i, embedding := range embeddings {
		sourceTables[i] = embedding.SourceTable
		sourceUIDs[i] = embedding.SourceUID
		embeddingUIDs[i] = embedding.EmbeddingUID // Use the embeddingUID from the input struct
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

	hasMetadata, err := m.checkMetadataField(ctx, collectionName)
	if err != nil {
		logger.Error("Failed to check metadata existence", zap.Error(err))
		return fmt.Errorf("failed to check metadata existence: %w", err)
	}

	if hasMetadata {
		columns = append(columns,
			entity.NewColumnVarChar(kbCollectionFieldFileName, fileNames),
			entity.NewColumnVarChar(kbCollectionFieldFileType, fileTypes),
			entity.NewColumnVarChar(kbCollectionFieldContentType, contentTypes))
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
		logger.Error("Failed to insert vectors after retries", zap.Error(err))
		return fmt.Errorf("failed to insert vectors: %w", err)
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
		logger.Error("Failed to flush collection after retries", zap.Error(err))
		return fmt.Errorf("failed to flush collection after insertion: %w", err)
	}

	logger.Info("Successfully inserted and flushed vectors", zap.String("collection", collectionName))
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

func (m *milvusClient) SimilarVectorsInCollection(ctx context.Context, p service.SimilarVectorSearchParam) ([][]service.SimilarEmbedding, error) {
	logger, _ := log.GetZapLogger(ctx)

	collectionName := p.CollectionID
	vectors := p.Vectors
	topK := int(p.TopK)
	fileName := p.FileName
	fileType := p.FileType
	contentType := p.ContentType

	// Check if the collection exists
	t := time.Now()
	has, err := m.c.HasCollection(ctx, collectionName)
	if err != nil {
		logger.Error("failed to check collection existence", zap.Error(err))
		return nil, fmt.Errorf("failed to check collection existence: %w", err)
	}
	if !has {
		logger.Error("collection does not exist", zap.String("collection_name", collectionName))
		return nil, fmt.Errorf("collection %s does not exist", collectionName)
	}
	logger.Info("check collection existence", zap.Duration("duration", time.Since(t)))
	t = time.Now()

	// Load the collection if it's not already loaded
	err = m.c.LoadCollection(ctx, collectionName, false)
	if err != nil {
		logger.Error("failed to load collection", zap.Error(err))
		return nil, fmt.Errorf("failed to load collection: %w", err)
	}
	logger.Info("load collection", zap.Duration("duration", time.Since(t)))

	hasMetadata, err := m.checkMetadataField(ctx, collectionName)
	if err != nil {
		logger.Error("failed to describe collection", zap.Error(err))
		return nil, fmt.Errorf("failed to describe collection: %w", err)
	}

	outputFields := []string{
		kbCollectionFieldSourceTable,
		kbCollectionFieldSourceUID,
		kbCollectionFieldEmbeddingUID,
		kbCollectionFieldEmbedding,
	}
	var filterStrs []string
	if hasMetadata {
		// TODO filter by UID && add to hasmetadata
		if fileName != "" {
			filterStrs = append(filterStrs, fmt.Sprintf("file_name == '%s'", fileName))
		}
		if fileType != "" {
			filterStrs = append(filterStrs, fmt.Sprintf("file_type == '%s'", fileType))
		}
		if contentType != "" {
			filterStrs = append(filterStrs, fmt.Sprintf("content_type == '%s'", contentType))
		}
		outputFields = append(outputFields,
			kbCollectionFieldFileName,
			kbCollectionFieldFileType,
			kbCollectionFieldContentType)
	}

	t = time.Now()
	// Convert the input vector to float32
	milvusVectors := make([]entity.Vector, len(vectors))
	// milvus search vector support batch search, but we just need one vector
	for i, v := range vectors {
		milvusVectors[i] = entity.FloatVector(v)
	}
	// Perform the search
	sp, err := entity.NewIndexSCANNSearchParam(nProbe, reorderK)
	if err != nil {
		logger.Error("failed to create search param", zap.Error(err))
		return nil, fmt.Errorf("failed to create search param: %w", err)
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
		logger.Error("failed to search embeddings", zap.Error(err))
		return nil, fmt.Errorf("failed to search embeddings: %w", err)
	}
	logger.Info("search embeddings", zap.Duration("duration", time.Since(t)))
	// Extract the embeddings from the search results
	var embeddings [][]service.SimilarEmbedding
	for _, result := range results {
		if result.ResultCount == 0 {
			continue
		}
		sourceTables, err := getStringData(result.Fields.GetColumn(kbCollectionFieldSourceTable))
		if err != nil {
			logger.Error("error with source_table column", zap.Error(err))
			return nil, fmt.Errorf("error with source_table column: %w", err)
		}

		sourceUIDs, err := getStringData(result.Fields.GetColumn(kbCollectionFieldSourceUID))
		if err != nil {
			logger.Error("error with source_uid column", zap.Error(err))
			return nil, fmt.Errorf("error with source_uid column: %w", err)
		}
		embeddingUIDs, err := getStringData(result.Fields.GetColumn(kbCollectionFieldEmbeddingUID))
		if err != nil {
			logger.Error("error with embedding_uid column", zap.Error(err))
			return nil, fmt.Errorf("error with embedding_uid column: %w", err)
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

func (m *milvusClient) checkMetadataField(ctx context.Context, collectionName string) (bool, error) {
	collDesc, err := m.c.DescribeCollection(ctx, collectionName)
	if err != nil {
		return false, fmt.Errorf("failed to describe collection: %w", err)
	}

	var existingFields = map[string]bool{}
	for _, field := range collDesc.Schema.Fields {
		existingFields[field.Name] = true
	}
	return existingFields[kbCollectionFieldFileName] &&
		existingFields[kbCollectionFieldFileType] &&
		existingFields[kbCollectionFieldContentType], nil
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
