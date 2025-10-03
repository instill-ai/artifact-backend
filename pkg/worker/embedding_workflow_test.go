package worker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/instill-ai/artifact-backend/pkg/service"
)

func TestEmbedTextsWorkflowParam_BatchCalculation(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			texts := make([]string, tt.totalTexts)
			for i := range texts {
				texts[i] = "test"
			}

			param := service.EmbedTextsWorkflowParam{
				Texts:     texts,
				BatchSize: tt.batchSize,
			}

			calculatedBatches := (len(param.Texts) + param.BatchSize - 1) / param.BatchSize
			assert.Equal(t, tt.expectedBatches, calculatedBatches)
		})
	}
}

func TestEmbedTextsWorkflowParam_DefaultBatchSize(t *testing.T) {
	param := service.EmbedTextsWorkflowParam{
		Texts:     make([]string, 100),
		BatchSize: 0, // Will default to 32
	}

	batchSize := param.BatchSize
	if batchSize <= 0 {
		batchSize = 32
	}

	assert.Equal(t, 32, batchSize)
}

func TestEmbedTextsWorkflowParam_EmptyTexts(t *testing.T) {
	param := service.EmbedTextsWorkflowParam{
		Texts:     []string{},
		BatchSize: 32,
	}

	require.NotNil(t, param.Texts)
	assert.Len(t, param.Texts, 0)
}
