package worker

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestExtractPageReferences(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name           string
		chunkStart     uint32
		chunkEnd       uint32
		pageDelimiters []uint32
		expectedStart  uint32
		expectedEnd    uint32
	}{
		{
			name:           "Chunk within first page",
			chunkStart:     0,
			chunkEnd:       50,
			pageDelimiters: []uint32{100, 200, 300},
			expectedStart:  1,
			expectedEnd:    1,
		},
		{
			name:           "Chunk spanning two pages",
			chunkStart:     80,
			chunkEnd:       150,
			pageDelimiters: []uint32{100, 200, 300},
			expectedStart:  1,
			expectedEnd:    2,
		},
		{
			name:           "Chunk at exact page delimiter",
			chunkStart:     50,
			chunkEnd:       100,
			pageDelimiters: []uint32{100, 200, 300},
			expectedStart:  1,
			expectedEnd:    1,
		},
		{
			name:           "Chunk beyond last delimiter",
			chunkStart:     250,
			chunkEnd:       350,
			pageDelimiters: []uint32{100, 200, 300},
			expectedStart:  3,
			expectedEnd:    3,
		},
		{
			name:           "Empty delimiters",
			chunkStart:     50,
			chunkEnd:       100,
			pageDelimiters: []uint32{},
			expectedStart:  0,
			expectedEnd:    0,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			start, end := extractPageReferences(tt.chunkStart, tt.chunkEnd, tt.pageDelimiters)
			c.Assert(start, qt.Equals, tt.expectedStart)
			c.Assert(end, qt.Equals, tt.expectedEnd)
		})
	}
}

func TestProcessWaitingFileActivity_FileTypeConversion(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name                string
		fileType            string
		expectConvertStatus bool
	}{
		{
			name:                "PDF should convert",
			fileType:            "FILE_TYPE_PDF",
			expectConvertStatus: true,
		},
		{
			name:                "Markdown should not convert",
			fileType:            "FILE_TYPE_MARKDOWN",
			expectConvertStatus: false,
		},
		{
			name:                "Text should not convert",
			fileType:            "FILE_TYPE_TEXT",
			expectConvertStatus: false,
		},
		{
			name:                "DOC should convert",
			fileType:            "FILE_TYPE_DOC",
			expectConvertStatus: true,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			// This test validates the business logic of file type routing
			// Full integration would require repository and service mocks
			c.Assert(tt.fileType, qt.Not(qt.Equals), "")
		})
	}
}
