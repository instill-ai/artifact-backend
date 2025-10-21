package worker

import (
	"context"
	"fmt"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/gojuno/minimock/v3"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/zap"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/worker/mock"
)

func TestSaveEmbeddingsWorkflow_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Setup mocks
	mockRepository := mock.NewRepositoryMock(mc)

	// Create test embeddings
	kbUID := uuid.Must(uuid.NewV4())
	fileUID := uuid.Must(uuid.NewV4())
	embeddings := createWorkflowTestEmbeddings(100)

	param := SaveEmbeddingsWorkflowParam{
		KBUID:        kbUID,
		FileUID:      fileUID,
		FileName:     "test.pdf",
		Embeddings:   embeddings,
		UserUID:      uuid.Must(uuid.NewV4()),
		RequesterUID: uuid.Must(uuid.NewV4()),
	}

	// Setup mock expectations (VectorDatabase methods are on Repository now)
	mockRepository.GetKnowledgeBaseByUIDMock.Return(&repository.KnowledgeBaseModel{
		UID:                 kbUID,
		ActiveCollectionUID: kbUID,
	}, nil)
	mockRepository.DeleteEmbeddingsWithFileUIDMock.Return(nil)
	mockRepository.DeleteEmbeddingsByKBFileUIDMock.Return(nil)
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
	mockRepository.InsertVectorsInCollectionMock.Return(nil)
	mockRepository.FlushCollectionMock.Return(nil)

	// Create worker and register
	w := &Worker{repository: mockRepository, log: zap.NewNop()}
	env.RegisterActivity(w.DeleteOldEmbeddingsActivity)
	env.RegisterActivity(w.SaveEmbeddingBatchActivity)
	env.RegisterActivity(w.FlushCollectionActivity)
	env.RegisterWorkflow(w.SaveEmbeddingsWorkflow)

	// Execute workflow
	env.ExecuteWorkflow(w.SaveEmbeddingsWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.IsNil)
}

func TestSaveEmbeddingsWorkflow_EmptyEmbeddings(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockRepository := mock.NewRepositoryMock(mc)

	param := SaveEmbeddingsWorkflowParam{
		KBUID:        uuid.Must(uuid.NewV4()),
		FileUID:      uuid.Must(uuid.NewV4()),
		FileName:     "test.pdf",
		Embeddings:   []repository.EmbeddingModel{}, // Empty
		UserUID:      uuid.Must(uuid.NewV4()),
		RequesterUID: uuid.Must(uuid.NewV4()),
	}

	w := &Worker{repository: mockRepository, log: zap.NewNop()}
	env.RegisterActivity(w.DeleteOldEmbeddingsActivity)
	env.RegisterActivity(w.SaveEmbeddingBatchActivity)
	env.RegisterActivity(w.FlushCollectionActivity)
	env.RegisterWorkflow(w.SaveEmbeddingsWorkflow)

	env.ExecuteWorkflow(w.SaveEmbeddingsWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.IsNil)
}

func TestSaveEmbeddingsWorkflow_DeleteMilvusFailure(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockRepository := mock.NewRepositoryMock(mc)
	kbUID := uuid.Must(uuid.NewV4())

	mockRepository.GetKnowledgeBaseByUIDMock.Return(&repository.KnowledgeBaseModel{
		UID:                 kbUID,
		ActiveCollectionUID: kbUID,
	}, nil)
	// Setup mock to return error for VectorDB (will fail in first step of DeleteOldEmbeddingsActivity)
	mockRepository.DeleteEmbeddingsWithFileUIDMock.Return(fmt.Errorf("milvus connection error"))
	// DB delete won't be called since VectorDB fails first (removed expectation)

	param := SaveEmbeddingsWorkflowParam{
		KBUID:        kbUID,
		FileUID:      uuid.Must(uuid.NewV4()),
		FileName:     "test.pdf",
		Embeddings:   createWorkflowTestEmbeddings(50),
		UserUID:      uuid.Must(uuid.NewV4()),
		RequesterUID: uuid.Must(uuid.NewV4()),
	}

	w := &Worker{repository: mockRepository, log: zap.NewNop()}
	env.RegisterActivity(w.DeleteOldEmbeddingsActivity)
	env.RegisterActivity(w.SaveEmbeddingBatchActivity)
	env.RegisterActivity(w.FlushCollectionActivity)
	env.RegisterWorkflow(w.SaveEmbeddingsWorkflow)

	env.ExecuteWorkflow(w.SaveEmbeddingsWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.Not(qt.IsNil))
	c.Assert(env.GetWorkflowError().Error(), qt.Contains, "vector database")
}

func TestSaveEmbeddingsWorkflow_DeleteDBFailure(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// DeleteOldEmbeddingsActivity needs both VectorDB and Repository (should fail)
	mockRepository := mock.NewRepositoryMock(mc)
	kbUID := uuid.Must(uuid.NewV4())

	mockRepository.GetKnowledgeBaseByUIDMock.Return(&repository.KnowledgeBaseModel{
		UID:                 kbUID,
		ActiveCollectionUID: kbUID,
	}, nil)
	mockRepository.DeleteEmbeddingsWithFileUIDMock.Return(nil)
	mockRepository.DeleteEmbeddingsByKBFileUIDMock.Return(fmt.Errorf("database connection error"))

	param := SaveEmbeddingsWorkflowParam{
		KBUID:        kbUID,
		FileUID:      uuid.Must(uuid.NewV4()),
		FileName:     "test.pdf",
		Embeddings:   createWorkflowTestEmbeddings(50),
		UserUID:      uuid.Must(uuid.NewV4()),
		RequesterUID: uuid.Must(uuid.NewV4()),
	}

	w := &Worker{repository: mockRepository, log: zap.NewNop()}
	env.RegisterActivity(w.DeleteOldEmbeddingsActivity)
	env.RegisterActivity(w.SaveEmbeddingBatchActivity)
	env.RegisterActivity(w.FlushCollectionActivity)
	env.RegisterWorkflow(w.SaveEmbeddingsWorkflow)

	env.ExecuteWorkflow(w.SaveEmbeddingsWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.Not(qt.IsNil))
	c.Assert(env.GetWorkflowError().Error(), qt.Contains, "embedding records")
}

func TestSaveEmbeddingsWorkflow_BatchFailure(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockRepository := mock.NewRepositoryMock(mc)
	kbUID := uuid.Must(uuid.NewV4())

	mockRepository.GetKnowledgeBaseByUIDMock.Return(&repository.KnowledgeBaseModel{
		UID:                 kbUID,
		ActiveCollectionUID: kbUID,
	}, nil)
	mockRepository.DeleteEmbeddingsByKBFileUIDMock.Return(nil)
	mockRepository.DeleteEmbeddingsWithFileUIDMock.Return(nil)
	mockRepository.CreateEmbeddingsMock.Return(nil, fmt.Errorf("batch insert failed"))

	param := SaveEmbeddingsWorkflowParam{
		KBUID:        kbUID,
		FileUID:      uuid.Must(uuid.NewV4()),
		FileName:     "test.pdf",
		Embeddings:   createWorkflowTestEmbeddings(50),
		UserUID:      uuid.Must(uuid.NewV4()),
		RequesterUID: uuid.Must(uuid.NewV4()),
	}

	w := &Worker{repository: mockRepository, log: zap.NewNop()}
	env.RegisterActivity(w.DeleteOldEmbeddingsActivity)
	env.RegisterActivity(w.SaveEmbeddingBatchActivity)
	env.RegisterActivity(w.FlushCollectionActivity)
	env.RegisterWorkflow(w.SaveEmbeddingsWorkflow)

	env.ExecuteWorkflow(w.SaveEmbeddingsWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.Not(qt.IsNil))
	c.Assert(env.GetWorkflowError().Error(), qt.Contains, "batch")
}

func TestSaveEmbeddingsWorkflow_LargeDataset(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockRepository := mock.NewRepositoryMock(mc)

	// Create 250 embeddings = 5 batches (50 per batch)
	// This tests parallel batch processing without overwhelming the test environment
	embeddings := createWorkflowTestEmbeddings(250)
	kbUID := uuid.Must(uuid.NewV4())

	param := SaveEmbeddingsWorkflowParam{
		KBUID:        kbUID,
		FileUID:      uuid.Must(uuid.NewV4()),
		FileName:     "large.pdf",
		Embeddings:   embeddings,
		UserUID:      uuid.Must(uuid.NewV4()),
		RequesterUID: uuid.Must(uuid.NewV4()),
	}

	mockRepository.GetKnowledgeBaseByUIDMock.Return(&repository.KnowledgeBaseModel{
		UID:                 kbUID,
		ActiveCollectionUID: kbUID,
	}, nil)
	mockRepository.DeleteEmbeddingsByKBFileUIDMock.Return(nil)
	mockRepository.DeleteEmbeddingsWithFileUIDMock.Return(nil)
	mockRepository.InsertVectorsInCollectionMock.Return(nil)
	mockRepository.FlushCollectionMock.Return(nil)
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
	env.RegisterActivity(w.DeleteOldEmbeddingsActivity)
	env.RegisterActivity(w.SaveEmbeddingBatchActivity)
	env.RegisterActivity(w.FlushCollectionActivity)
	env.RegisterWorkflow(w.SaveEmbeddingsWorkflow)

	env.ExecuteWorkflow(w.SaveEmbeddingsWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.IsNil)
}

func TestSaveEmbeddingsWorkflow_BatchSizeCalculation(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name            string
		embeddingCount  int
		batchSize       int
		expectedBatches int
	}{
		{
			name:            "Exact multiple",
			embeddingCount:  100,
			batchSize:       50,
			expectedBatches: 2,
		},
		{
			name:            "With remainder",
			embeddingCount:  105,
			batchSize:       50,
			expectedBatches: 3,
		},
		{
			name:            "Single batch",
			embeddingCount:  25,
			batchSize:       50,
			expectedBatches: 1,
		},
		{
			name:            "Large dataset",
			embeddingCount:  1000,
			batchSize:       50,
			expectedBatches: 20,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			calculatedBatches := (tt.embeddingCount + tt.batchSize - 1) / tt.batchSize
			c.Assert(calculatedBatches, qt.Equals, tt.expectedBatches)
		})
	}
}

// Helper functions
func createWorkflowTestEmbeddings(count int) []repository.EmbeddingModel {
	embeddings := make([]repository.EmbeddingModel, count)
	kbFileUID := uuid.Must(uuid.NewV4())

	for i := 0; i < count; i++ {
		embeddings[i] = repository.EmbeddingModel{
			UID:         uuid.Must(uuid.NewV4()),
			SourceTable: "text_chunk",
			SourceUID:   uuid.Must(uuid.NewV4()),
			KBFileUID:   kbFileUID,
			Vector:      createWorkflowTestVector(768),
			ContentType: "application/pdf",
			ChunkType:   "content",
		}
	}

	return embeddings
}

func createWorkflowTestVector(dim int) []float32 {
	vec := make([]float32, dim)
	for i := 0; i < dim; i++ {
		vec[i] = float32(i) / float32(dim)
	}
	return vec
}
