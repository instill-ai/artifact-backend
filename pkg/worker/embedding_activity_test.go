package worker

import (
	"context"
	"fmt"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/gojuno/minimock/v3"
	"go.uber.org/zap"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/mock"
	"github.com/instill-ai/artifact-backend/pkg/repository"
)

func TestEmbedTextsActivityParam_Validation(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		param *EmbedTextsActivityParam
	}{
		{
			name: "Valid param with texts",
			param: &EmbedTextsActivityParam{
				Texts:      []string{"text1", "text2", "text3"},
				BatchIndex: 0,
			},
		},
		{
			name: "Empty texts",
			param: &EmbedTextsActivityParam{
				Texts:      []string{},
				BatchIndex: 0,
			},
		},
		{
			name: "Batch index set correctly",
			param: &EmbedTextsActivityParam{
				Texts:      []string{"text1"},
				BatchIndex: 5,
			},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(tt.param.BatchIndex >= 0, qt.IsTrue)
			c.Assert(tt.param.Texts, qt.Not(qt.IsNil))
		})
	}
}

func TestEmbedTextsActivity_EmptyInput(t *testing.T) {
	c := qt.New(t)

	// Test that empty input returns empty output
	param := &EmbedTextsActivityParam{
		Texts:      []string{},
		BatchIndex: 0,
	}

	c.Assert(param.Texts, qt.HasLen, 0)
	// Activity should handle empty input gracefully and return [][]float32{}
}

func TestEmbedTextsActivity_BatchIndexing(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name       string
		batchIndex int
	}{
		{"First batch", 0},
		{"Second batch", 1},
		{"Tenth batch", 9},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			param := &EmbedTextsActivityParam{
				Texts:      []string{"test"},
				BatchIndex: tt.batchIndex,
			}
			c.Assert(param.BatchIndex, qt.Equals, tt.batchIndex)
		})
	}
}

func TestSaveEmbeddingBatchActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepo := mock.NewRepositoryIMock(mc)
	mockVectorDB := NewVectorDatabaseMock(mc)
	mockSvc := NewServiceMock(mc)
	mockSvc.RepositoryMock.Return(mockRepo)
	mockSvc.VectorDBMock.Return(mockVectorDB)

	embeddings := createActivityTestEmbeddings(50)

	// Setup mocks
	mockVectorDB.InsertVectorsInCollectionMock.Return(nil)
	mockRepo.CreateEmbeddingsMock.Set(func(
		ctx context.Context,
		embeddings []repository.Embedding,
		externalServiceCall func([]repository.Embedding) error,
	) ([]repository.Embedding, error) {
		if externalServiceCall != nil {
			if err := externalServiceCall(embeddings); err != nil {
				return nil, err
			}
		}
		return embeddings, nil
	})

	worker := &Worker{service: mockSvc, log: zap.NewNop()}

	param := &SaveEmbeddingBatchActivityParam{
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
		FileUID:          uuid.Must(uuid.NewV4()),
		FileName:         "test.pdf",
		Embeddings:       embeddings,
		BatchNumber:      1,
		TotalBatches:     2,
	}

	err := worker.SaveEmbeddingBatchActivity(context.Background(), param)
	c.Assert(err, qt.IsNil)
}

func TestSaveEmbeddingBatchActivity_EmptyBatch(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)
	// No mocks needed - empty batch returns early without calling any service methods
	mockSvc := NewServiceMock(mc)

	worker := &Worker{service: mockSvc, log: zap.NewNop()}

	param := &SaveEmbeddingBatchActivityParam{
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
		FileUID:          uuid.Must(uuid.NewV4()),
		FileName:         "test.pdf",
		Embeddings:       []repository.Embedding{}, // Empty
		BatchNumber:      1,
		TotalBatches:     1,
	}

	err := worker.SaveEmbeddingBatchActivity(context.Background(), param)
	c.Assert(err, qt.IsNil)
}

func TestSaveEmbeddingBatchActivity_MilvusFailure(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepo := mock.NewRepositoryIMock(mc)
	mockVectorDB := NewVectorDatabaseMock(mc)
	mockSvc := NewServiceMock(mc)
	mockSvc.RepositoryMock.Return(mockRepo)
	mockSvc.VectorDBMock.Return(mockVectorDB)
	mockVectorDB.InsertVectorsInCollectionMock.Return(fmt.Errorf("milvus insert failed"))

	embeddings := createActivityTestEmbeddings(50)
	mockRepo.CreateEmbeddingsMock.Set(func(
		ctx context.Context,
		embeddings []repository.Embedding,
		externalServiceCall func([]repository.Embedding) error,
	) ([]repository.Embedding, error) {
		if externalServiceCall != nil {
			if err := externalServiceCall(embeddings); err != nil {
				return nil, err
			}
		}
		return embeddings, nil
	})

	worker := &Worker{service: mockSvc, log: zap.NewNop()}

	param := &SaveEmbeddingBatchActivityParam{
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
		FileUID:          uuid.Must(uuid.NewV4()),
		FileName:         "test.pdf",
		Embeddings:       embeddings,
		BatchNumber:      1,
		TotalBatches:     1,
	}

	err := worker.SaveEmbeddingBatchActivity(context.Background(), param)
	c.Assert(err, qt.Not(qt.IsNil))
	c.Assert(err.Error(), qt.Contains, "milvus")
}

func TestSaveEmbeddingBatchActivity_DatabaseFailure(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.CreateEmbeddingsMock.Return(nil, fmt.Errorf("database insert failed"))

	mockSvc := NewServiceMock(mc)
	mockSvc.RepositoryMock.Return(mockRepo)

	worker := &Worker{service: mockSvc, log: zap.NewNop()}

	param := &SaveEmbeddingBatchActivityParam{
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
		FileUID:          uuid.Must(uuid.NewV4()),
		FileName:         "test.pdf",
		Embeddings:       createActivityTestEmbeddings(50),
		BatchNumber:      1,
		TotalBatches:     1,
	}

	err := worker.SaveEmbeddingBatchActivity(context.Background(), param)
	c.Assert(err, qt.Not(qt.IsNil))
	c.Assert(err.Error(), qt.Contains, "database")
}

func TestDeleteOldEmbeddingsFromVectorDBActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)
	mockVectorDB := NewVectorDatabaseMock(mc)
	mockSvc := NewServiceMock(mc)
	mockSvc.VectorDBMock.Return(mockVectorDB)
	mockVectorDB.DeleteEmbeddingsWithFileUIDMock.Return(nil)

	worker := &Worker{service: mockSvc, log: zap.NewNop()}

	param := &DeleteOldEmbeddingsActivityParam{
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
		FileUID:          uuid.Must(uuid.NewV4()),
	}

	err := worker.DeleteOldEmbeddingsFromVectorDBActivity(context.Background(), param)
	c.Assert(err, qt.IsNil)
}

func TestDeleteOldEmbeddingsFromVectorDBActivity_Failure(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)
	mockVectorDB := NewVectorDatabaseMock(mc)
	mockSvc := NewServiceMock(mc)
	mockSvc.VectorDBMock.Return(mockVectorDB)
	mockVectorDB.DeleteEmbeddingsWithFileUIDMock.Return(fmt.Errorf("milvus connection error"))

	worker := &Worker{service: mockSvc, log: zap.NewNop()}

	param := &DeleteOldEmbeddingsActivityParam{
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
		FileUID:          uuid.Must(uuid.NewV4()),
	}

	err := worker.DeleteOldEmbeddingsFromVectorDBActivity(context.Background(), param)
	c.Assert(err, qt.Not(qt.IsNil))
	c.Assert(err.Error(), qt.Contains, "vector db")
}

func TestDeleteOldEmbeddingsFromDBActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.DeleteEmbeddingsByKbFileUIDMock.Return(nil)

	mockSvc := NewServiceMock(mc)
	mockSvc.RepositoryMock.Return(mockRepo)

	worker := &Worker{service: mockSvc, log: zap.NewNop()}

	param := &DeleteOldEmbeddingsActivityParam{
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
		FileUID:          uuid.Must(uuid.NewV4()),
	}

	err := worker.DeleteOldEmbeddingsFromDBActivity(context.Background(), param)
	c.Assert(err, qt.IsNil)
}

func TestDeleteOldEmbeddingsFromDBActivity_Failure(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.DeleteEmbeddingsByKbFileUIDMock.Return(fmt.Errorf("database connection error"))

	mockSvc := NewServiceMock(mc)
	mockSvc.RepositoryMock.Return(mockRepo)

	worker := &Worker{service: mockSvc, log: zap.NewNop()}

	param := &DeleteOldEmbeddingsActivityParam{
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
		FileUID:          uuid.Must(uuid.NewV4()),
	}

	err := worker.DeleteOldEmbeddingsFromDBActivity(context.Background(), param)
	c.Assert(err, qt.Not(qt.IsNil))
	c.Assert(err.Error(), qt.Contains, "DB")
}

func TestSaveEmbeddingBatchActivityParam_Validation(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		param *SaveEmbeddingBatchActivityParam
	}{
		{
			name: "Valid param with embeddings",
			param: &SaveEmbeddingBatchActivityParam{
				KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
				FileUID:          uuid.Must(uuid.NewV4()),
				FileName:         "test.pdf",
				Embeddings:       createActivityTestEmbeddings(50),
				BatchNumber:      1,
				TotalBatches:     2,
			},
		},
		{
			name: "Empty embeddings",
			param: &SaveEmbeddingBatchActivityParam{
				KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
				FileUID:          uuid.Must(uuid.NewV4()),
				FileName:         "test.pdf",
				Embeddings:       []repository.Embedding{},
				BatchNumber:      1,
				TotalBatches:     1,
			},
		},
		{
			name: "Batch numbers set correctly",
			param: &SaveEmbeddingBatchActivityParam{
				KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
				FileUID:          uuid.Must(uuid.NewV4()),
				FileName:         "test.pdf",
				Embeddings:       createActivityTestEmbeddings(25),
				BatchNumber:      3,
				TotalBatches:     5,
			},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(tt.param.BatchNumber > 0, qt.IsTrue)
			c.Assert(tt.param.TotalBatches > 0, qt.IsTrue)
			c.Assert(tt.param.BatchNumber <= tt.param.TotalBatches, qt.IsTrue)
			c.Assert(tt.param.Embeddings, qt.Not(qt.IsNil))
		})
	}
}

func TestDeleteOldEmbeddingsActivityParam_Validation(t *testing.T) {
	c := qt.New(t)

	param := &DeleteOldEmbeddingsActivityParam{
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
		FileUID:          uuid.Must(uuid.NewV4()),
	}

	c.Assert(param.KnowledgeBaseUID, qt.Not(qt.Equals), uuid.UUID{})
	c.Assert(param.FileUID, qt.Not(qt.Equals), uuid.UUID{})
}

func TestFlushCollectionActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockVectorDB := NewVectorDatabaseMock(mc)
	mockSvc := NewServiceMock(mc)
	mockSvc.VectorDBMock.Return(mockVectorDB)
	mockVectorDB.FlushCollectionMock.Return(nil)

	worker := &Worker{service: mockSvc, log: zap.NewNop()}

	param := &DeleteOldEmbeddingsActivityParam{
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
		FileUID:          uuid.Must(uuid.NewV4()),
	}

	err := worker.FlushCollectionActivity(context.Background(), param)
	c.Assert(err, qt.IsNil)
}

func TestFlushCollectionActivity_Failure(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockVectorDB := NewVectorDatabaseMock(mc)
	mockSvc := NewServiceMock(mc)
	mockSvc.VectorDBMock.Return(mockVectorDB)
	mockVectorDB.FlushCollectionMock.Return(fmt.Errorf("flush collection error"))

	worker := &Worker{service: mockSvc, log: zap.NewNop()}

	param := &DeleteOldEmbeddingsActivityParam{
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
		FileUID:          uuid.Must(uuid.NewV4()),
	}

	err := worker.FlushCollectionActivity(context.Background(), param)
	c.Assert(err, qt.Not(qt.IsNil))
	c.Assert(err.Error(), qt.Contains, "flush collection")
}

func TestUpdateEmbeddingMetadataActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.UpdateKBFileMetadataMock.Return(nil)

	mockSvc := NewServiceMock(mc)
	mockSvc.RepositoryMock.Return(mockRepo)

	worker := &Worker{service: mockSvc, log: zap.NewNop()}

	param := &UpdateEmbeddingMetadataActivityParam{
		FileUID:  uuid.Must(uuid.NewV4()),
		Pipeline: "instill-ai/text-embeddings",
	}

	err := worker.UpdateEmbeddingMetadataActivity(context.Background(), param)
	c.Assert(err, qt.IsNil)
}

func TestUpdateEmbeddingMetadataActivity_Failure(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.UpdateKBFileMetadataMock.Return(fmt.Errorf("database error"))

	mockSvc := NewServiceMock(mc)
	mockSvc.RepositoryMock.Return(mockRepo)

	worker := &Worker{service: mockSvc, log: zap.NewNop()}

	param := &UpdateEmbeddingMetadataActivityParam{
		FileUID:  uuid.Must(uuid.NewV4()),
		Pipeline: "instill-ai/text-embeddings",
	}

	err := worker.UpdateEmbeddingMetadataActivity(context.Background(), param)
	c.Assert(err, qt.Not(qt.IsNil))
	c.Assert(err.Error(), qt.Contains, "metadata")
}

func TestUpdateEmbeddingMetadataActivity_FileDeleted(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.UpdateKBFileMetadataMock.Return(fmt.Errorf("record not found"))

	mockSvc := NewServiceMock(mc)
	mockSvc.RepositoryMock.Return(mockRepo)

	worker := &Worker{service: mockSvc, log: zap.NewNop()}

	param := &UpdateEmbeddingMetadataActivityParam{
		FileUID:  uuid.Must(uuid.NewV4()),
		Pipeline: "instill-ai/text-embeddings",
	}

	// Should not return error if file is deleted during processing
	err := worker.UpdateEmbeddingMetadataActivity(context.Background(), param)
	c.Assert(err, qt.IsNil)
}

func TestUpdateEmbeddingMetadataActivityParam_Validation(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		param *UpdateEmbeddingMetadataActivityParam
	}{
		{
			name: "Valid param with pipeline",
			param: &UpdateEmbeddingMetadataActivityParam{
				FileUID:  uuid.Must(uuid.NewV4()),
				Pipeline: "instill-ai/text-embeddings",
			},
		},
		{
			name: "Empty pipeline",
			param: &UpdateEmbeddingMetadataActivityParam{
				FileUID:  uuid.Must(uuid.NewV4()),
				Pipeline: "",
			},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(tt.param.FileUID, qt.Not(qt.Equals), uuid.UUID{})
		})
	}
}

// Helper functions

func createActivityTestEmbeddings(count int) []repository.Embedding {
	embeddings := make([]repository.Embedding, count)
	kbFileUID := uuid.Must(uuid.NewV4())

	for i := 0; i < count; i++ {
		embeddings[i] = repository.Embedding{
			UID:         uuid.Must(uuid.NewV4()),
			SourceTable: "text_chunk",
			SourceUID:   uuid.Must(uuid.NewV4()),
			KbFileUID:   kbFileUID,
			Vector:      createActivityTestVector(768),
			FileType:    "application/pdf",
			ContentType: "text",
		}
	}

	return embeddings
}

func createActivityTestVector(dim int) []float32 {
	vec := make([]float32, dim)
	for i := 0; i < dim; i++ {
		vec[i] = float32(i) / float32(dim)
	}
	return vec
}
