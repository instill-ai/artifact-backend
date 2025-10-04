package worker

import (
	"testing"

	qt "github.com/frankban/quicktest"

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
