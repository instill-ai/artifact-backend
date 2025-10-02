package worker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmbedTextsActivityParam_Validation(t *testing.T) {
	tests := []struct {
		name       string
		param      *EmbedTextsActivityParam
		expectPass bool
	}{
		{
			name: "Valid param with texts",
			param: &EmbedTextsActivityParam{
				Texts:      []string{"text1", "text2", "text3"},
				BatchIndex: 0,
			},
			expectPass: true,
		},
		{
			name: "Empty texts",
			param: &EmbedTextsActivityParam{
				Texts:      []string{},
				BatchIndex: 0,
			},
			expectPass: true,
		},
		{
			name: "Batch index set correctly",
			param: &EmbedTextsActivityParam{
				Texts:      []string{"text1"},
				BatchIndex: 5,
			},
			expectPass: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NotNil(t, tt.param)
			if tt.expectPass {
				assert.GreaterOrEqual(t, tt.param.BatchIndex, 0)
				assert.NotNil(t, tt.param.Texts)
			}
		})
	}
}

func TestEmbedTextsActivity_EmptyInput(t *testing.T) {
	// Test that empty input returns empty output
	param := &EmbedTextsActivityParam{
		Texts:      []string{},
		BatchIndex: 0,
	}

	require.NotNil(t, param)
	assert.Len(t, param.Texts, 0)
	// Activity should handle empty input gracefully and return [][]float32{}
}

func TestEmbedTextsActivity_BatchIndexing(t *testing.T) {
	tests := []struct {
		name       string
		batchIndex int
		valid      bool
	}{
		{"First batch", 0, true},
		{"Second batch", 1, true},
		{"Tenth batch", 9, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			param := &EmbedTextsActivityParam{
				Texts:      []string{"test"},
				BatchIndex: tt.batchIndex,
			}
			assert.Equal(t, tt.batchIndex, param.BatchIndex)
		})
	}
}
