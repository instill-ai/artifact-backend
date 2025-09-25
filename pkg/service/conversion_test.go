package service

import (
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/pkg/repository"

	qt "github.com/frankban/quicktest"
)

func TestProtoListToStrings(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		list     *structpb.ListValue
		suffix   string
		expected []string
	}{
		{
			name:     "empty list",
			list:     &structpb.ListValue{Values: []*structpb.Value{}},
			suffix:   "",
			expected: []string{},
		},
		{
			name: "single string",
			list: &structpb.ListValue{
				Values: []*structpb.Value{
					structpb.NewStringValue("hello"),
				},
			},
			suffix:   "",
			expected: []string{"hello"},
		},
		{
			name: "multiple strings without suffix",
			list: &structpb.ListValue{
				Values: []*structpb.Value{
					structpb.NewStringValue("hello"),
					structpb.NewStringValue("world"),
					structpb.NewStringValue("test"),
				},
			},
			suffix:   "",
			expected: []string{"hello", "world", "test"},
		},
		{
			name: "multiple strings with suffix",
			list: &structpb.ListValue{
				Values: []*structpb.Value{
					structpb.NewStringValue("hello"),
					structpb.NewStringValue("world"),
					structpb.NewStringValue("test"),
				},
			},
			suffix:   "\n",
			expected: []string{"hello\n", "world\n", "test"},
		},
		{
			name: "strings with existing suffix",
			list: &structpb.ListValue{
				Values: []*structpb.Value{
					structpb.NewStringValue("hello\n"),
					structpb.NewStringValue("world\n"),
					structpb.NewStringValue("test"),
				},
			},
			suffix:   "\n",
			expected: []string{"hello\n", "world\n", "test"},
		},
		{
			name: "empty strings filtered out",
			list: &structpb.ListValue{
				Values: []*structpb.Value{
					structpb.NewStringValue("hello"),
					structpb.NewStringValue(""),
					structpb.NewStringValue("world"),
					structpb.NewStringValue(""),
					structpb.NewStringValue("test"),
				},
			},
			suffix:   "",
			expected: []string{"hello", "world", "test"},
		},
		{
			name:     "nil list",
			list:     nil,
			suffix:   "",
			expected: []string{},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			result := ProtoListToStrings(tt.list, tt.suffix)
			c.Assert(result, qt.DeepEquals, tt.expected)
		})
	}
}

func TestPositionDataFromPages(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		pages    []string
		expected *repository.PositionData
	}{
		{
			name:     "empty pages",
			pages:    []string{},
			expected: nil,
		},
		{
			name:     "nil pages",
			pages:    nil,
			expected: nil,
		},
		{
			name:  "single page",
			pages: []string{"hello world"},
			expected: &repository.PositionData{
				PageDelimiters: []uint32{11}, // "hello world" has 11 characters
			},
		},
		{
			name:  "multiple pages",
			pages: []string{"hello", "world", "test"},
			expected: &repository.PositionData{
				PageDelimiters: []uint32{5, 10, 14}, // cumulative: 5, 5+5, 5+5+4
			},
		},
		{
			name:  "pages with unicode characters",
			pages: []string{"héllo", "wörld", "tëst"},
			expected: &repository.PositionData{
				PageDelimiters: []uint32{5, 10, 14}, // unicode characters count as 1 rune each
			},
		},
		{
			name:  "empty pages filtered",
			pages: []string{"hello", "", "world", "", "test"},
			expected: &repository.PositionData{
				PageDelimiters: []uint32{5, 5, 10, 10, 14}, // empty strings still count as pages
			},
		},
		{
			name:  "pages with newlines",
			pages: []string{"hello\nworld", "test\ncase"},
			expected: &repository.PositionData{
				PageDelimiters: []uint32{11, 20}, // "hello\nworld" = 11 chars, "test\ncase" = 9 chars
			},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			result := PositionDataFromPages(tt.pages)
			if tt.expected == nil {
				c.Assert(result, qt.IsNil)
			} else {
				c.Assert(result, qt.DeepEquals, tt.expected)
			}
		})
	}
}
