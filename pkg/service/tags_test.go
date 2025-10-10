package service

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestAreTagsEqual(t *testing.T) {
	c := qt.New(t)

	testcases := []struct {
		name     string
		tags1    []string
		tags2    []string
		expected bool
	}{
		{
			name:     "both empty",
			tags1:    []string{},
			tags2:    []string{},
			expected: true,
		},
		{
			name:     "one empty, one nil",
			tags1:    []string{},
			tags2:    nil,
			expected: true,
		},
		{
			name:     "both nil",
			tags1:    nil,
			tags2:    nil,
			expected: true,
		},
		{
			name:     "different lengths",
			tags1:    []string{"tag1"},
			tags2:    []string{"tag1", "tag2"},
			expected: false,
		},
		{
			name:     "same tags in same order",
			tags1:    []string{"tag1", "tag2"},
			tags2:    []string{"tag1", "tag2"},
			expected: true,
		},
		{
			name:     "same tags in different order",
			tags1:    []string{"tag1", "tag2"},
			tags2:    []string{"tag2", "tag1"},
			expected: true,
		},
		{
			name:     "different tags",
			tags1:    []string{"tag1", "tag2"},
			tags2:    []string{"tag1", "tag3"},
			expected: false,
		},
		{
			name:     "single tag same",
			tags1:    []string{"tag1"},
			tags2:    []string{"tag1"},
			expected: true,
		},
		{
			name:     "single tag different",
			tags1:    []string{"tag1"},
			tags2:    []string{"tag2"},
			expected: false,
		},
		{
			name:     "multiple tags with duplicates in different order",
			tags1:    []string{"tag1", "tag2", "tag1"},
			tags2:    []string{"tag1", "tag1", "tag2"},
			expected: true,
		},
		{
			name:     "multiple tags with different duplicates",
			tags1:    []string{"tag1", "tag2", "tag1"},
			tags2:    []string{"tag1", "tag2", "tag2"},
			expected: false,
		},
		{
			name:     "case sensitive comparison",
			tags1:    []string{"Tag1", "tag2"},
			tags2:    []string{"tag1", "Tag2"},
			expected: false,
		},
		{
			name:     "empty strings",
			tags1:    []string{"", "tag1"},
			tags2:    []string{"tag1", ""},
			expected: true,
		},
		{
			name:     "all empty strings",
			tags1:    []string{"", ""},
			tags2:    []string{"", ""},
			expected: true,
		},
	}

	for _, tc := range testcases {
		c.Run(tc.name, func(c *qt.C) {
			result := areTagsEqual(tc.tags1, tc.tags2)
			c.Check(result, qt.Equals, tc.expected)
		})
	}
}
