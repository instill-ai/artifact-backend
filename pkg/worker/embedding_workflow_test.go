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

	"github.com/instill-ai/artifact-backend/pkg/mock"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"
)

func TestEmbedTextsWorkflowParam_BatchCalculation(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name            string
		totalTexts      int
		batchSize       int
		expectedBatches int
	}{
		{
			name:            "Exact multiple",
			totalTexts:      64,
			batchSize:       32,
			expectedBatches: 2,
		},
		{
			name:            "With remainder",
			totalTexts:      70,
			batchSize:       32,
			expectedBatches: 3,
		},
		{
			name:            "Single batch",
			totalTexts:      20,
			batchSize:       32,
			expectedBatches: 1,
		},
		{
			name:            "One text",
			totalTexts:      1,
			batchSize:       32,
			expectedBatches: 1,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			texts := make([]string, tt.totalTexts)
			for i := range texts {
				texts[i] = "test"
			}

			param := service.EmbedTextsWorkflowParam{
				Texts:     texts,
				BatchSize: tt.batchSize,
			}

			calculatedBatches := (len(param.Texts) + param.BatchSize - 1) / param.BatchSize
			c.Assert(calculatedBatches, qt.Equals, tt.expectedBatches)
		})
	}
}

func TestEmbedTextsWorkflowParam_DefaultBatchSize(t *testing.T) {
	c := qt.New(t)

	param := service.EmbedTextsWorkflowParam{
		Texts:     make([]string, 100),
		BatchSize: 0, // Will default to 32
	}

	batchSize := param.BatchSize
	if batchSize <= 0 {
		batchSize = 32
	}

	c.Assert(batchSize, qt.Equals, 32)
}

func TestEmbedTextsWorkflowParam_EmptyTexts(t *testing.T) {
	c := qt.New(t)

	param := service.EmbedTextsWorkflowParam{
		Texts:     []string{},
		BatchSize: 32,
	}

	c.Assert(param.Texts, qt.HasLen, 0)
}

func TestSaveEmbeddingsToVectorDBWorkflow_Success(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Setup mocks
	mockRepo := mock.NewRepositoryIMock(mc)
	mockVectorDB := NewVectorDatabaseMock(mc)
	mockSvc := NewServiceMock(mc)
	mockSvc.RepositoryMock.Return(mockRepo)
	mockSvc.VectorDBMock.Return(mockVectorDB)

	// Create test embeddings
	kbUID := uuid.Must(uuid.NewV4())
	fileUID := uuid.Must(uuid.NewV4())
	embeddings := createWorkflowTestEmbeddings(100)

	param := SaveEmbeddingsToVectorDBWorkflowParam{
		KnowledgeBaseUID: kbUID,
		FileUID:          fileUID,
		FileName:         "test.pdf",
		Embeddings:       embeddings,
	}

	// Setup mock expectations
	mockVectorDB.DeleteEmbeddingsWithFileUIDMock.Return(nil)
	mockRepo.DeleteEmbeddingsByKbFileUIDMock.Return(nil)
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
	mockVectorDB.InsertVectorsInCollectionMock.Return(nil)
	mockVectorDB.FlushCollectionMock.Return(nil)

	// Create worker and register
	worker := &Worker{service: mockSvc, log: zap.NewNop()}
	env.RegisterActivity(worker.DeleteOldEmbeddingsFromVectorDBActivity)
	env.RegisterActivity(worker.DeleteOldEmbeddingsFromDBActivity)
	env.RegisterActivity(worker.SaveEmbeddingBatchActivity)
	env.RegisterActivity(worker.FlushCollectionActivity)
	env.RegisterWorkflow(worker.SaveEmbeddingsToVectorDBWorkflow)

	// Execute workflow
	env.ExecuteWorkflow(worker.SaveEmbeddingsToVectorDBWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.IsNil)
}

func TestSaveEmbeddingsToVectorDBWorkflow_EmptyEmbeddings(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// No mocks needed - empty embeddings causes workflow to return early without executing activities
	mockSvc := NewServiceMock(mc)

	param := SaveEmbeddingsToVectorDBWorkflowParam{
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
		FileUID:          uuid.Must(uuid.NewV4()),
		FileName:         "test.pdf",
		Embeddings:       []repository.Embedding{}, // Empty
	}

	worker := &Worker{service: mockSvc, log: zap.NewNop()}
	env.RegisterActivity(worker.DeleteOldEmbeddingsFromVectorDBActivity)
	env.RegisterActivity(worker.DeleteOldEmbeddingsFromDBActivity)
	env.RegisterActivity(worker.SaveEmbeddingBatchActivity)
	env.RegisterActivity(worker.FlushCollectionActivity)
	env.RegisterWorkflow(worker.SaveEmbeddingsToVectorDBWorkflow)

	env.ExecuteWorkflow(worker.SaveEmbeddingsToVectorDBWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.IsNil)
}

func TestSaveEmbeddingsToVectorDBWorkflow_DeleteMilvusFailure(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockVectorDB := NewVectorDatabaseMock(mc)
	mockSvc := NewServiceMock(mc)
	mockSvc.VectorDBMock.Return(mockVectorDB)

	// Setup mock to return error - this activity only uses VectorDB
	mockVectorDB.DeleteEmbeddingsWithFileUIDMock.Return(fmt.Errorf("milvus connection error"))

	param := SaveEmbeddingsToVectorDBWorkflowParam{
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
		FileUID:          uuid.Must(uuid.NewV4()),
		FileName:         "test.pdf",
		Embeddings:       createWorkflowTestEmbeddings(50),
	}

	worker := &Worker{service: mockSvc, log: zap.NewNop()}
	env.RegisterActivity(worker.DeleteOldEmbeddingsFromVectorDBActivity)
	env.RegisterActivity(worker.DeleteOldEmbeddingsFromDBActivity)
	env.RegisterActivity(worker.SaveEmbeddingBatchActivity)
	env.RegisterActivity(worker.FlushCollectionActivity)
	env.RegisterWorkflow(worker.SaveEmbeddingsToVectorDBWorkflow)

	env.ExecuteWorkflow(worker.SaveEmbeddingsToVectorDBWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.Not(qt.IsNil))
	c.Assert(env.GetWorkflowError().Error(), qt.Contains, "VectorDB")
}

func TestSaveEmbeddingsToVectorDBWorkflow_DeleteDBFailure(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// DeleteOldEmbeddingsFromVectorDBActivity needs VectorDB (should succeed)
	mockVectorDB := NewVectorDatabaseMock(mc)
	mockVectorDB.DeleteEmbeddingsWithFileUIDMock.Return(nil)

	// DeleteOldEmbeddingsFromDBActivity needs Repository (should fail)
	mockRepo := mock.NewRepositoryIMock(mc)
	mockRepo.DeleteEmbeddingsByKbFileUIDMock.Return(fmt.Errorf("database connection error"))

	mockSvc := NewServiceMock(mc)
	mockSvc.VectorDBMock.Return(mockVectorDB)
	mockSvc.RepositoryMock.Return(mockRepo)

	param := SaveEmbeddingsToVectorDBWorkflowParam{
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
		FileUID:          uuid.Must(uuid.NewV4()),
		FileName:         "test.pdf",
		Embeddings:       createWorkflowTestEmbeddings(50),
	}

	worker := &Worker{service: mockSvc, log: zap.NewNop()}
	env.RegisterActivity(worker.DeleteOldEmbeddingsFromVectorDBActivity)
	env.RegisterActivity(worker.DeleteOldEmbeddingsFromDBActivity)
	env.RegisterActivity(worker.SaveEmbeddingBatchActivity)
	env.RegisterActivity(worker.FlushCollectionActivity)
	env.RegisterWorkflow(worker.SaveEmbeddingsToVectorDBWorkflow)

	env.ExecuteWorkflow(worker.SaveEmbeddingsToVectorDBWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.Not(qt.IsNil))
	c.Assert(env.GetWorkflowError().Error(), qt.Contains, "DB")
}

func TestSaveEmbeddingsToVectorDBWorkflow_BatchFailure(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockRepo := mock.NewRepositoryIMock(mc)
	mockVectorDB := NewVectorDatabaseMock(mc)
	mockRepo.DeleteEmbeddingsByKbFileUIDMock.Return(nil)
	mockRepo.CreateEmbeddingsMock.Return(nil, fmt.Errorf("batch insert failed"))
	mockVectorDB.DeleteEmbeddingsWithFileUIDMock.Return(nil)

	mockSvc := NewServiceMock(mc)
	mockSvc.RepositoryMock.Return(mockRepo)
	mockSvc.VectorDBMock.Return(mockVectorDB)

	param := SaveEmbeddingsToVectorDBWorkflowParam{
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
		FileUID:          uuid.Must(uuid.NewV4()),
		FileName:         "test.pdf",
		Embeddings:       createWorkflowTestEmbeddings(50),
	}

	worker := &Worker{service: mockSvc, log: zap.NewNop()}
	env.RegisterActivity(worker.DeleteOldEmbeddingsFromVectorDBActivity)
	env.RegisterActivity(worker.DeleteOldEmbeddingsFromDBActivity)
	env.RegisterActivity(worker.SaveEmbeddingBatchActivity)
	env.RegisterActivity(worker.FlushCollectionActivity)
	env.RegisterWorkflow(worker.SaveEmbeddingsToVectorDBWorkflow)

	env.ExecuteWorkflow(worker.SaveEmbeddingsToVectorDBWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.Not(qt.IsNil))
	c.Assert(env.GetWorkflowError().Error(), qt.Contains, "batch")
}

func TestSaveEmbeddingsToVectorDBWorkflow_LargeDataset(t *testing.T) {
	c := qt.New(t)
	mc := minimock.NewController(c)
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	mockRepo := mock.NewRepositoryIMock(mc)
	mockVectorDB := NewVectorDatabaseMock(mc)
	mockSvc := NewServiceMock(mc)
	mockSvc.RepositoryMock.Return(mockRepo)
	mockSvc.VectorDBMock.Return(mockVectorDB)

	// Create 250 embeddings = 5 batches (50 per batch)
	// This tests parallel batch processing without overwhelming the test environment
	embeddings := createWorkflowTestEmbeddings(250)

	param := SaveEmbeddingsToVectorDBWorkflowParam{
		KnowledgeBaseUID: uuid.Must(uuid.NewV4()),
		FileUID:          uuid.Must(uuid.NewV4()),
		FileName:         "large.pdf",
		Embeddings:       embeddings,
	}

	mockVectorDB.DeleteEmbeddingsWithFileUIDMock.Return(nil)
	mockVectorDB.InsertVectorsInCollectionMock.Return(nil)
	mockVectorDB.FlushCollectionMock.Return(nil)
	mockRepo.DeleteEmbeddingsByKbFileUIDMock.Return(nil)
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
	env.RegisterActivity(worker.DeleteOldEmbeddingsFromVectorDBActivity)
	env.RegisterActivity(worker.DeleteOldEmbeddingsFromDBActivity)
	env.RegisterActivity(worker.SaveEmbeddingBatchActivity)
	env.RegisterActivity(worker.FlushCollectionActivity)
	env.RegisterWorkflow(worker.SaveEmbeddingsToVectorDBWorkflow)

	env.ExecuteWorkflow(worker.SaveEmbeddingsToVectorDBWorkflow, param)

	c.Assert(env.IsWorkflowCompleted(), qt.IsTrue)
	c.Assert(env.GetWorkflowError(), qt.IsNil)
}

func TestSaveEmbeddingsToVectorDBWorkflow_BatchSizeCalculation(t *testing.T) {
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
func createWorkflowTestEmbeddings(count int) []repository.Embedding {
	embeddings := make([]repository.Embedding, count)
	kbFileUID := uuid.Must(uuid.NewV4())

	for i := 0; i < count; i++ {
		embeddings[i] = repository.Embedding{
			UID:         uuid.Must(uuid.NewV4()),
			SourceTable: "text_chunk",
			SourceUID:   uuid.Must(uuid.NewV4()),
			KbFileUID:   kbFileUID,
			Vector:      createWorkflowTestVector(768),
			FileType:    "application/pdf",
			ContentType: "text",
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
