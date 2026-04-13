package service

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
	filetype "github.com/instill-ai/x/file"
)

func TestGetDownloadURL_UnsupportedFormat(t *testing.T) {
	c := qt.New(t)
	s := &service{}

	_, err := s.GetDownloadURL(t.Context(), "obj-test", types.NamespaceUIDType{}, "ns", 1, "", "png", "")
	c.Assert(err, qt.IsNotNil)
	c.Check(err.Error(), qt.Contains, "unsupported format")
}

func TestConvertedFileCacheSuffix(t *testing.T) {
	c := qt.New(t)
	c.Check(convertedFileCacheSuffix, qt.Equals, "-converted.pdf")
}

func TestNeedFileTypeConversion_ConvertsOfficeFormats(t *testing.T) {
	c := qt.New(t)

	convertToPDF := []artifactpb.File_Type{
		artifactpb.File_TYPE_DOC,
		artifactpb.File_TYPE_DOCX,
		artifactpb.File_TYPE_PPT,
		artifactpb.File_TYPE_PPTX,
	}

	for _, ft := range convertToPDF {
		needs, ext, _ := filetype.NeedFileTypeConversion(ft)
		c.Check(needs, qt.IsTrue, qt.Commentf("expected %s to need conversion", ft))
		c.Check(ext, qt.Equals, "pdf")
	}

	// XLS converts to XLSX (for structured parsing with excelize), not PDF
	needs, ext, target := filetype.NeedFileTypeConversion(artifactpb.File_TYPE_XLS)
	c.Check(needs, qt.IsTrue, qt.Commentf("expected XLS to need conversion"))
	c.Check(ext, qt.Equals, "xlsx")
	c.Check(target, qt.Equals, artifactpb.File_TYPE_XLSX)

	nonConvertible := []artifactpb.File_Type{
		artifactpb.File_TYPE_PDF,
		artifactpb.File_TYPE_PNG,
		artifactpb.File_TYPE_JPEG,
		artifactpb.File_TYPE_TEXT,
		artifactpb.File_TYPE_XLSX, // parsed directly by excelize
	}

	for _, ft := range nonConvertible {
		needs, _, _ := filetype.NeedFileTypeConversion(ft)
		c.Check(needs, qt.IsFalse, qt.Commentf("expected %s to NOT need conversion", ft))
	}
}

func TestDetermineFileType_OfficeFormats(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		contentType string
		filename    string
		expected    artifactpb.File_Type
	}{
		{"application/vnd.openxmlformats-officedocument.wordprocessingml.document", "report.docx", artifactpb.File_TYPE_DOCX},
		{"application/msword", "report.doc", artifactpb.File_TYPE_DOC},
		{"application/vnd.openxmlformats-officedocument.presentationml.presentation", "slides.pptx", artifactpb.File_TYPE_PPTX},
		{"application/vnd.ms-powerpoint", "slides.ppt", artifactpb.File_TYPE_PPT},
		{"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "data.xlsx", artifactpb.File_TYPE_XLSX},
		{"application/vnd.ms-excel", "data.xls", artifactpb.File_TYPE_XLS},
		{"application/pdf", "doc.pdf", artifactpb.File_TYPE_PDF},
		{"image/png", "image.png", artifactpb.File_TYPE_PNG},
	}

	for _, tt := range tests {
		c.Run(tt.filename, func(c *qt.C) {
			got := filetype.DetermineFileType(tt.contentType, tt.filename)
			c.Check(got, qt.Equals, tt.expected)
		})
	}
}
