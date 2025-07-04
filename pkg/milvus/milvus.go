package milvus

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"go.uber.org/zap"

	"github.com/instill-ai/x/log"
)

type MilvusClientI interface {
	GetVersion(ctx context.Context) (string, error)
	GetHealth(ctx context.Context) (bool, error)
	CreateKnowledgeBaseCollection(ctx context.Context, kbUID string) error
	InsertVectorsToKnowledgeBaseCollection(ctx context.Context, kbUID string, embeddings []Embedding) error
	GetAllCollectionNames(ctx context.Context) ([]*entity.Collection, error)
	DeleteCollection(ctx context.Context, collectionName string) error
	// drop knowledge base collection
	DropKnowledgeBaseCollection(ctx context.Context, kbUID string) error
	ListEmbeddings(ctx context.Context, collectionName string) ([]Embedding, error)
	SearchSimilarEmbeddings(ctx context.Context, collectionName string, vectors [][]float32, topK int, fileName, fileType, contentType string) ([][]SimilarEmbedding, error)
	// SearchSimilarEmbeddingsInKB search similar embeddings in knowledge base.
	// The topK has default value 5
	SearchSimilarEmbeddingsInKB(ctx context.Context, kbUID string, vectors [][]float32, topK int, fileName, fileType, contentType string) ([][]SimilarEmbedding, error)
	DeleteEmbedding(ctx context.Context, collectionName string, embeddingUID []string) error
	DeleteEmbeddingsInKb(ctx context.Context, kbUID string, embeddingUID []string) error
	// GetKnowledgeBaseCollectionName returns the collection name for a knowledge base
	GetKnowledgeBaseCollectionName(kbUID string) string
	Close()
}

type MilvusClient struct {
	c client.Client
}

const (
	VectorDim  = 1536
	VectorType = entity.FieldTypeFloatVector
	ScannNlist = 1024
	MetricType = entity.COSINE
	WitRaw     = true
)

// Search parameter
const (
	Nprobe   = 250
	ReorderK = 250
)

type Embedding struct {
	SourceTable  string
	SourceUID    string
	EmbeddingUID string
	Vector       []float32
	FileName     string
	FileType     string
	ContentType  string
}

const (
	KbCollectionFieldSourceTable  = "source_table"
	KbCollectionFieldSourceUID    = "source_uid"
	KbCollectionFieldEmbeddingUID = "embedding_uid"
	KbCollectionFieldEmbedding    = "embedding"
	KbCollectionFieldFileName     = "file_name"
	KbCollectionFieldFileType     = "file_type"
	KbCollectionFieldContentType  = "content_type"
)

func NewMilvusClient(ctx context.Context, host, port string) (MilvusClientI, error) {
	c, err := client.NewGrpcClient(ctx, host+":"+port)
	// c2,err := client.NewClient(ctx, client.Config{
	// 	Address: host+":" + port,
	// })
	if err != nil {
		return nil, err
	}
	return &MilvusClient{c: c}, nil
}

func (m *MilvusClient) GetVersion(ctx context.Context) (string, error) {
	v, err := m.c.GetVersion(ctx)
	return v, err
}

// GetHealth
func (m *MilvusClient) GetHealth(ctx context.Context) (bool, error) {
	h, err := m.c.CheckHealth(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to check health: %w", err)
	}
	if h == nil {
		return false, fmt.Errorf("health check returned nil")
	}
	return h.IsHealthy, err
}

// CreateKnowledgeBaseCollection
func (m *MilvusClient) CreateKnowledgeBaseCollection(ctx context.Context, kbUID string) error {
	logger, _ := log.GetZapLogger(ctx)
	collectionName := m.GetKnowledgeBaseCollectionName(kbUID)

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
	vectorDim := fmt.Sprintf("%d", VectorDim)
	schema := &entity.Schema{
		CollectionName: collectionName,
		Description:    "",
		Fields: []*entity.Field{
			{Name: KbCollectionFieldSourceTable, DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "255"}},
			{Name: KbCollectionFieldSourceUID, DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "255"}},
			{Name: KbCollectionFieldEmbeddingUID, DataType: entity.FieldTypeVarChar, PrimaryKey: true, TypeParams: map[string]string{"max_length": "255"}},
			{Name: KbCollectionFieldEmbedding, DataType: entity.FieldTypeFloatVector, TypeParams: map[string]string{"dim": vectorDim}},
			{Name: KbCollectionFieldFileName, DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "255"}},
			{Name: KbCollectionFieldFileType, DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "255"}},
			{Name: KbCollectionFieldContentType, DataType: entity.FieldTypeVarChar, TypeParams: map[string]string{"max_length": "255"}},
		},
	}

	err = m.c.CreateCollection(ctx, schema, 1)
	if err != nil {
		return fmt.Errorf("failed to create collection: %w", err)
	}

	// 3. Create index
	index, err := entity.NewIndexSCANN(MetricType, ScannNlist, WitRaw)
	if err != nil {
		logger.Error("Failed to create index", zap.Error(err))
		return fmt.Errorf("failed to create index: %w", err)
	}

	err = m.c.CreateIndex(ctx, collectionName, KbCollectionFieldEmbedding, index, false)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}

	logger.Info("Collection created successfully", zap.String("collection_name", collectionName))
	return nil
}

// InsertVectorsToKnowledgeBaseCollection
func (m *MilvusClient) InsertVectorsToKnowledgeBaseCollection(ctx context.Context, kbUID string, embeddings []Embedding) error {
	logger, _ := log.GetZapLogger(ctx)
	collectionName := m.GetKnowledgeBaseCollectionName(kbUID)

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
		entity.NewColumnVarChar(KbCollectionFieldSourceTable, sourceTables),
		entity.NewColumnVarChar(KbCollectionFieldSourceUID, sourceUIDs),
		entity.NewColumnVarChar(KbCollectionFieldEmbeddingUID, embeddingUIDs),
		entity.NewColumnFloatVector(KbCollectionFieldEmbedding, VectorDim, vectors),
	}

	hasMetadata, err := m.checkMetadataField(ctx, collectionName)
	if err != nil {
		logger.Error("Failed to check metadata existence", zap.Error(err))
		return fmt.Errorf("failed to check metadata existence: %w", err)
	}

	if hasMetadata {
		columns = append(columns,
			entity.NewColumnVarChar(KbCollectionFieldFileName, fileNames),
			entity.NewColumnVarChar(KbCollectionFieldFileType, fileTypes),
			entity.NewColumnVarChar(KbCollectionFieldContentType, contentTypes))
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

// GetAllCollectionNames returns all collection names
func (m *MilvusClient) GetAllCollectionNames(ctx context.Context) ([]*entity.Collection, error) {
	collections, err := m.c.ListCollections(ctx)
	return collections, err
}

// DeleteCollection deletes a collection
func (m *MilvusClient) DeleteCollection(ctx context.Context, collectionName string) error {
	err := m.c.DropCollection(ctx, collectionName)
	return err
}

// Helper function to safely get string data from a column
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

// ListEmbeddings returns all embeddings
func (m *MilvusClient) ListEmbeddings(ctx context.Context, collectionName string) ([]Embedding, error) {
	// Check if the collection exists
	has, err := m.c.HasCollection(ctx, collectionName)
	if err != nil {
		return nil, fmt.Errorf("failed to check collection existence: %w", err)
	}
	if !has {
		return nil, fmt.Errorf("collection %s does not exist", collectionName)
	}

	// Load the collection if it's not already loaded
	err = m.c.LoadCollection(ctx, collectionName, false)
	if err != nil {
		return nil, fmt.Errorf("failed to load collection: %w", err)
	}

	var allEmbeddings []Embedding
	offset := int64(0)
	limit := int64(1000) // Adjust this based on your needs and memory constraints

	for {

		fields := []string{
			KbCollectionFieldSourceTable,
			KbCollectionFieldSourceUID,
			KbCollectionFieldEmbeddingUID,
			KbCollectionFieldEmbedding,
		}

		hasMetadata, err := m.checkMetadataField(ctx, collectionName)
		if err != nil {
			return nil, fmt.Errorf("failed to check metadata: %w", err)
		}

		if hasMetadata {
			fields = append(fields,
				KbCollectionFieldFileName,
				KbCollectionFieldFileType,
				KbCollectionFieldContentType)
		}

		// Perform a query to get a batch of embeddings
		queryResult, err := m.c.Query(ctx, collectionName, nil, "", fields, client.WithOffset(offset), client.WithLimit(limit))
		if err != nil {
			return nil, fmt.Errorf("failed to query embeddings: %w", err)
		}

		if len(queryResult) == 0 {
			break // No more results
		}

		// Extract embeddings from the query result
		embeddingUIDs, err := getStringData(queryResult.GetColumn(KbCollectionFieldEmbeddingUID))
		if err != nil {
			return nil, fmt.Errorf("error with embedding_uid column: %w", err)
		}

		sourceTables, err := getStringData(queryResult.GetColumn(KbCollectionFieldSourceTable))
		if err != nil {
			return nil, fmt.Errorf("error with source_table column: %w", err)
		}

		sourceUIDs, err := getStringData(queryResult.GetColumn(KbCollectionFieldSourceUID))
		if err != nil {
			return nil, fmt.Errorf("error with source_uid column: %w", err)
		}

		vectors, ok := queryResult.GetColumn(KbCollectionFieldEmbedding).(*entity.ColumnFloatVector)
		if !ok {
			return nil, fmt.Errorf("unexpected type for embedding column: %T", queryResult[3])
		}

		for i := 0; i < len(embeddingUIDs); i++ {

			allEmbeddings = append(allEmbeddings, Embedding{
				SourceTable:  sourceTables[i],
				SourceUID:    sourceUIDs[i],
				EmbeddingUID: embeddingUIDs[i],
				Vector:       vectors.Data()[i],
			})
		}

		if int64(len(embeddingUIDs)) < limit {
			break // Last batch
		}

		offset += limit
	}

	return allEmbeddings, nil
}

// DeleteEmbedding delete an embedding by embeddingUID
func (m *MilvusClient) DeleteEmbedding(ctx context.Context, collectionName string, embeddingUID []string) error {
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

// DeleteEmbeddingsInKb
func (m *MilvusClient) DeleteEmbeddingsInKb(ctx context.Context, kbUID string, embeddingUID []string) error {
	collectionName := m.GetKnowledgeBaseCollectionName(kbUID)
	return m.DeleteEmbedding(ctx, collectionName, embeddingUID)
}

type SimilarEmbedding struct {
	Embedding
	Score float32
}

// SearchSimilarEmbeddings searches for embeddings similar to the input vector
// topk has default value 5, when topk <= 0, it will be set to 5.
func (m *MilvusClient) SearchSimilarEmbeddings(ctx context.Context, collectionName string, vectors [][]float32, topK int, fileName, fileType, contentType string) ([][]SimilarEmbedding, error) {
	logger, err := log.GetZapLogger(ctx)
	if err != nil {
		logger.Error("failed to get logger", zap.Error(err))
		return nil, fmt.Errorf("failed to get logger: %w", err)
	}

	// set default topK
	if topK <= 0 {
		topK = 5
	}

	t := time.Now()
	// Check if the collection exists
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
		KbCollectionFieldSourceTable,
		KbCollectionFieldSourceUID,
		KbCollectionFieldEmbeddingUID,
		KbCollectionFieldEmbedding,
	}
	var filterStrs []string
	if hasMetadata {
		// set filter string
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
			KbCollectionFieldFileName,
			KbCollectionFieldFileType,
			KbCollectionFieldContentType)
	}

	t = time.Now()
	// Convert the input vector to float32
	milvusVectors := make([]entity.Vector, len(vectors))
	// milvus search vector support batch search, but we just need one vector
	for i, v := range vectors {
		milvusVectors[i] = entity.FloatVector(v)
	}
	// Perform the search
	sp, err := entity.NewIndexSCANNSearchParam(Nprobe, ReorderK)
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
		KbCollectionFieldEmbedding,
		MetricType,
		topK,
		sp,
	)
	if err != nil {
		logger.Error("failed to search embeddings", zap.Error(err))
		return nil, fmt.Errorf("failed to search embeddings: %w", err)
	}
	logger.Info("search embeddings", zap.Duration("duration", time.Since(t)))
	// Extract the embeddings from the search results
	var embeddings [][]SimilarEmbedding
	for _, result := range results {
		if result.ResultCount == 0 {
			continue
		}
		sourceTables, err := getStringData(result.Fields.GetColumn(KbCollectionFieldSourceTable))
		if err != nil {
			logger.Error("error with source_table column", zap.Error(err))
			return nil, fmt.Errorf("error with source_table column: %w", err)
		}

		sourceUIDs, err := getStringData(result.Fields.GetColumn(KbCollectionFieldSourceUID))
		if err != nil {
			logger.Error("error with source_uid column", zap.Error(err))
			return nil, fmt.Errorf("error with source_uid column: %w", err)
		}
		embeddingUIDs, err := getStringData(result.Fields.GetColumn(KbCollectionFieldEmbeddingUID))
		if err != nil {
			logger.Error("error with embedding_uid column", zap.Error(err))
			return nil, fmt.Errorf("error with embedding_uid column: %w", err)
		}
		vectors := result.Fields.GetColumn(KbCollectionFieldEmbedding).(*entity.ColumnFloatVector)
		scores := result.Scores
		tempVectors := []SimilarEmbedding{}
		for i := 0; i < len(sourceTables); i++ {
			tempVectors = append(tempVectors, SimilarEmbedding{
				Embedding: Embedding{
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

// Close
func (m *MilvusClient) Close() {
	m.c.Close()
}

const kbCollectionPrefix = "kb_"

// GetKnowledgeBaseCollectionName returns the collection name for a knowledge base
func (m *MilvusClient) GetKnowledgeBaseCollectionName(kbUID string) string {
	// collection name can only contain numbers, letters and underscores: invalid parameter
	// turn kbUID(uuid) into a valid collection name
	kbUID = strings.ReplaceAll(kbUID, "-", "_")
	return kbCollectionPrefix + kbUID
}

// GetSimilarEmbeddingsInKB
func (m *MilvusClient) SearchSimilarEmbeddingsInKB(ctx context.Context, kbUID string, vectors [][]float32, topK int, fileName, fileType, contentType string) ([][]SimilarEmbedding, error) {
	collectionName := m.GetKnowledgeBaseCollectionName(kbUID)
	return m.SearchSimilarEmbeddings(ctx, collectionName, vectors, topK, fileName, fileType, contentType)
}

// Drop KnowledgeBaseCollection
func (m *MilvusClient) DropKnowledgeBaseCollection(ctx context.Context, kbUID string) error {
	collectionName := m.GetKnowledgeBaseCollectionName(kbUID)
	return m.DeleteCollection(ctx, collectionName)
}

func (m *MilvusClient) checkMetadataField(ctx context.Context, collectionName string) (bool, error) {
	collDesc, err := m.c.DescribeCollection(ctx, collectionName)
	if err != nil {
		return false, fmt.Errorf("failed to describe collection: %w", err)
	}

	var existingFields = map[string]bool{}
	for _, field := range collDesc.Schema.Fields {
		existingFields[field.Name] = true
	}
	return existingFields[KbCollectionFieldFileName] &&
		existingFields[KbCollectionFieldFileType] &&
		existingFields[KbCollectionFieldContentType], nil
}
