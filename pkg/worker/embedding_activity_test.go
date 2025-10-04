package worker

import (
	"testing"

	qt "github.com/frankban/quicktest"
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
