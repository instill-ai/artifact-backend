package service

import (
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/repository"

	pipelinepb "github.com/instill-ai/protogen-go/pipeline/pipeline/v1beta"
)

func TestConvertResultParser(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		response *structpb.Struct
		expected *MDConversionResult
		wantErr  string
	}{
		{
			name: "single string result",
			response: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"convert_result": structpb.NewStringValue("Hello world"),
				},
			},
			expected: &MDConversionResult{
				Markdown: "Hello world",
			},
		},
		{
			name: "array of strings result",
			response: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"convert_result": structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewStringValue("Page 1"),
							structpb.NewStringValue("Page 2"),
						},
					}),
				},
			},
			expected: &MDConversionResult{
				Markdown: "Page 1\nPage 2",
				Length:   []uint32{2},
				PositionData: &repository.PositionData{
					PageDelimiters: []uint32{7, 13}, // "Page 1\n" = 7 chars, "Page 2" = 6 chars
				},
			},
		},
		{
			name: "fallback to convert_result2",
			response: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"convert_result": structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewStringValue("Page 1\n"),
							structpb.NewStringValue("Page 2"),
						},
					}),
				},
			},
			expected: &MDConversionResult{
				Markdown: "Page 1\nPage 2",
				Length:   []uint32{2},
				PositionData: &repository.PositionData{
					PageDelimiters: []uint32{7, 13}, // "Page 1\n" = 7 chars, "Page 2" = 6 chars
				},
			},
		},
		{
			name: "empty response",
			response: &structpb.Struct{
				Fields: map[string]*structpb.Value{},
			},
			expected: nil,
			wantErr:  "no conversion result fields found in response",
		},
		{
			name: "both fields empty",
			response: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"convert_result":  structpb.NewStringValue(""),
					"convert_result2": structpb.NewStringValue(""),
				},
			},
			expected: nil,
			wantErr:  "empty markdown string in conversion result",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			// Create a mock response
			resp := &pipelinepb.TriggerNamespacePipelineReleaseResponse{
				Outputs: []*structpb.Struct{tt.response},
			}

			result, err := convertResultParser(resp)
			if tt.wantErr != "" {
				c.Assert(err, qt.IsNotNil)
				c.Assert(err, qt.ErrorMatches, tt.wantErr)
				return
			}

			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, tt.expected)
		})
	}
}
