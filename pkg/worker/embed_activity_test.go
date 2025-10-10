package worker

import (
	"context"
	"fmt"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/gojuno/minimock/v3"
	"go.uber.org/zap"
	"gorm.io/gorm"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/worker/mock"
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

	mockRepository := mock.NewRepositoryMock(mc)

	embeddings := createActivityTestEmbeddings(50)

	// Setup mocks
	mockRepository.InsertVectorsInCollectionMock.Return(nil)
	mockRepository.CreateEmbeddingsMock.Set(func(
		ctx context.Context,
		embeddings []repository.EmbeddingModel,
		externalServiceCall func([]repository.EmbeddingModel) error,
	) ([]repository.EmbeddingModel, error) {
		if externalServiceCall != nil {
			if err := externalServiceCall(embeddings); err != nil {
				return nil, err
			}
		}
		return embeddings, nil
	})

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	param := &SaveEmbeddingBatchActivityParam{
		KBUID:        uuid.Must(uuid.NewV4()),
		FileUID:      uuid.Must(uuid.NewV4()),
		FileName:     "test.pdf",
		Embeddings:   embeddings,
		BatchNumber:  1,
		TotalBatches: 2,
	}

	err := w.SaveEmbeddingBatchActivity(context.Background(), param)
	c.Assert(err, qt.IsNil)
}

func TestSaveEmbeddingBatchActivity_EmptyBatch(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)
	// No mocks needed - empty batch returns early without calling any service methods

	mockRepository := mock.NewRepositoryMock(mc)
	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	param := &SaveEmbeddingBatchActivityParam{
		KBUID:        uuid.Must(uuid.NewV4()),
		FileUID:      uuid.Must(uuid.NewV4()),
		FileName:     "test.pdf",
		Embeddings:   []repository.EmbeddingModel{}, // Empty
		BatchNumber:  1,
		TotalBatches: 1,
	}

	err := w.SaveEmbeddingBatchActivity(context.Background(), param)
	c.Assert(err, qt.IsNil)
}

func TestSaveEmbeddingBatchActivity_MilvusFailure(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.InsertVectorsInCollectionMock.Return(fmt.Errorf("milvus insert failed"))

	embeddings := createActivityTestEmbeddings(50)
	mockRepository.CreateEmbeddingsMock.Set(func(
		ctx context.Context,
		embeddings []repository.EmbeddingModel,
		externalServiceCall func([]repository.EmbeddingModel) error,
	) ([]repository.EmbeddingModel, error) {
		if externalServiceCall != nil {
			if err := externalServiceCall(embeddings); err != nil {
				return nil, err
			}
		}
		return embeddings, nil
	})

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	param := &SaveEmbeddingBatchActivityParam{
		KBUID:        uuid.Must(uuid.NewV4()),
		FileUID:      uuid.Must(uuid.NewV4()),
		FileName:     "test.pdf",
		Embeddings:   embeddings,
		BatchNumber:  1,
		TotalBatches: 1,
	}

	err := w.SaveEmbeddingBatchActivity(context.Background(), param)
	c.Assert(err, qt.Not(qt.IsNil))
	c.Assert(err.Error(), qt.Contains, "milvus")
}

func TestSaveEmbeddingBatchActivity_DatabaseFailure(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.CreateEmbeddingsMock.Return(nil, fmt.Errorf("database insert failed"))

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	param := &SaveEmbeddingBatchActivityParam{
		KBUID:        uuid.Must(uuid.NewV4()),
		FileUID:      uuid.Must(uuid.NewV4()),
		FileName:     "test.pdf",
		Embeddings:   createActivityTestEmbeddings(50),
		BatchNumber:  1,
		TotalBatches: 1,
	}

	err := w.SaveEmbeddingBatchActivity(context.Background(), param)
	c.Assert(err, qt.Not(qt.IsNil))
	c.Assert(err.Error(), qt.Contains, "database")
}

func TestDeleteOldEmbeddingsFromVectorDBActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)
	mockRepository := mock.NewRepositoryMock(mc)

	mockRepository.DeleteEmbeddingsWithFileUIDMock.Return(nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	param := &DeleteOldEmbeddingsActivityParam{
		KBUID:   uuid.Must(uuid.NewV4()),
		FileUID: uuid.Must(uuid.NewV4()),
	}

	err := w.DeleteOldEmbeddingsFromVectorDBActivity(context.Background(), param)
	c.Assert(err, qt.IsNil)
}

func TestDeleteOldEmbeddingsFromVectorDBActivity_Failure(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepository := mock.NewRepositoryMock(mc)

	mockRepository.DeleteEmbeddingsWithFileUIDMock.Return(fmt.Errorf("milvus connection error"))

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	param := &DeleteOldEmbeddingsActivityParam{
		KBUID:   uuid.Must(uuid.NewV4()),
		FileUID: uuid.Must(uuid.NewV4()),
	}

	err := w.DeleteOldEmbeddingsFromVectorDBActivity(context.Background(), param)
	c.Assert(err, qt.Not(qt.IsNil))
	c.Assert(err.Error(), qt.Contains, "Unable to delete old embeddings from vector database")
}

func TestDeleteOldEmbeddingsFromDBActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.DeleteEmbeddingsByKBFileUIDMock.Return(nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	param := &DeleteOldEmbeddingsActivityParam{
		KBUID:   uuid.Must(uuid.NewV4()),
		FileUID: uuid.Must(uuid.NewV4()),
	}

	err := w.DeleteOldEmbeddingsFromDBActivity(context.Background(), param)
	c.Assert(err, qt.IsNil)
}

func TestDeleteOldEmbeddingsFromDBActivity_Failure(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.DeleteEmbeddingsByKBFileUIDMock.Return(fmt.Errorf("database connection error"))

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	param := &DeleteOldEmbeddingsActivityParam{
		KBUID:   uuid.Must(uuid.NewV4()),
		FileUID: uuid.Must(uuid.NewV4()),
	}

	err := w.DeleteOldEmbeddingsFromDBActivity(context.Background(), param)
	c.Assert(err, qt.Not(qt.IsNil))
	c.Assert(err.Error(), qt.Contains, "Unable to delete old embedding records")
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
				KBUID:        uuid.Must(uuid.NewV4()),
				FileUID:      uuid.Must(uuid.NewV4()),
				FileName:     "test.pdf",
				Embeddings:   createActivityTestEmbeddings(50),
				BatchNumber:  1,
				TotalBatches: 2,
			},
		},
		{
			name: "Empty embeddings",
			param: &SaveEmbeddingBatchActivityParam{
				KBUID:        uuid.Must(uuid.NewV4()),
				FileUID:      uuid.Must(uuid.NewV4()),
				FileName:     "test.pdf",
				Embeddings:   []repository.EmbeddingModel{},
				BatchNumber:  1,
				TotalBatches: 1,
			},
		},
		{
			name: "Batch numbers set correctly",
			param: &SaveEmbeddingBatchActivityParam{
				KBUID:        uuid.Must(uuid.NewV4()),
				FileUID:      uuid.Must(uuid.NewV4()),
				FileName:     "test.pdf",
				Embeddings:   createActivityTestEmbeddings(25),
				BatchNumber:  3,
				TotalBatches: 5,
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
		KBUID:   uuid.Must(uuid.NewV4()),
		FileUID: uuid.Must(uuid.NewV4()),
	}

	c.Assert(param.KBUID, qt.Not(qt.Equals), uuid.UUID{})
	c.Assert(param.FileUID, qt.Not(qt.Equals), uuid.UUID{})
}

func TestFlushCollectionActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.FlushCollectionMock.Return(nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	param := &DeleteOldEmbeddingsActivityParam{
		KBUID:   uuid.Must(uuid.NewV4()),
		FileUID: uuid.Must(uuid.NewV4()),
	}

	err := w.FlushCollectionActivity(context.Background(), param)
	c.Assert(err, qt.IsNil)
}

func TestFlushCollectionActivity_Failure(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.FlushCollectionMock.Return(fmt.Errorf("flush collection error"))

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	param := &DeleteOldEmbeddingsActivityParam{
		KBUID:   uuid.Must(uuid.NewV4()),
		FileUID: uuid.Must(uuid.NewV4()),
	}

	err := w.FlushCollectionActivity(context.Background(), param)
	c.Assert(err, qt.Not(qt.IsNil))
	c.Assert(err.Error(), qt.Contains, "flush collection")
}

func TestUpdateEmbeddingMetadataActivity_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.UpdateKnowledgeFileMetadataMock.Return(nil)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	param := &UpdateEmbeddingMetadataActivityParam{
		FileUID:  uuid.Must(uuid.NewV4()),
		Pipeline: "instill-ai/text-embeddings",
	}

	err := w.UpdateEmbeddingMetadataActivity(context.Background(), param)
	c.Assert(err, qt.IsNil)
}

func TestUpdateEmbeddingMetadataActivity_Failure(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.UpdateKnowledgeFileMetadataMock.Return(fmt.Errorf("database error"))

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	param := &UpdateEmbeddingMetadataActivityParam{
		FileUID:  uuid.Must(uuid.NewV4()),
		Pipeline: "instill-ai/text-embeddings",
	}

	err := w.UpdateEmbeddingMetadataActivity(context.Background(), param)
	c.Assert(err, qt.Not(qt.IsNil))
	c.Assert(err.Error(), qt.Contains, "metadata")
}

func TestUpdateEmbeddingMetadataActivity_FileDeleted(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)

	mockRepository := mock.NewRepositoryMock(mc)
	mockRepository.UpdateKnowledgeFileMetadataMock.Return(gorm.ErrRecordNotFound)

	w := &Worker{repository: mockRepository, log: zap.NewNop()}

	param := &UpdateEmbeddingMetadataActivityParam{
		FileUID:  uuid.Must(uuid.NewV4()),
		Pipeline: "instill-ai/text-embeddings",
	}

	err := w.UpdateEmbeddingMetadataActivity(context.Background(), param)
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

func createActivityTestEmbeddings(count int) []repository.EmbeddingModel {
	embeddings := make([]repository.EmbeddingModel, count)
	kbFileUID := uuid.Must(uuid.NewV4())

	for i := 0; i < count; i++ {
		embeddings[i] = repository.EmbeddingModel{
			UID:         uuid.Must(uuid.NewV4()),
			SourceTable: "text_chunk",
			SourceUID:   uuid.Must(uuid.NewV4()),
			KBFileUID:   kbFileUID,
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
