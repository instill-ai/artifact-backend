package worker

import (
	"bytes"
	"strings"
	"unicode"

	"github.com/ledongthuc/pdf"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
)

const minTextCharsForTextBased = 50

// ClassifyDocumentTextBased determines whether a document file contains a
// native text layer. For non-PDF office formats (DOCX, PPTX, etc.) the
// answer is always true because they are converted from structured data.
// For PDFs, the function attempts to extract text from the first page; if
// at least minTextCharsForTextBased printable characters are found it is
// classified as text-based.
func ClassifyDocumentTextBased(fileType artifactpb.File_Type, pdfContent []byte) bool {
	switch fileType {
	case artifactpb.File_TYPE_DOCX, artifactpb.File_TYPE_DOC,
		artifactpb.File_TYPE_PPTX, artifactpb.File_TYPE_PPT,
		artifactpb.File_TYPE_XLSX, artifactpb.File_TYPE_XLS,
		artifactpb.File_TYPE_TEXT, artifactpb.File_TYPE_MARKDOWN,
		artifactpb.File_TYPE_HTML, artifactpb.File_TYPE_CSV,
		artifactpb.File_TYPE_JSON:
		return true
	case artifactpb.File_TYPE_PDF:
		return pdfHasTextLayer(pdfContent)
	default:
		return false
	}
}

// pdfHasTextLayer returns true when the PDF contains a real text layer by
// extracting text from the first page and checking whether there are enough
// printable characters.
func pdfHasTextLayer(data []byte) bool {
	if len(data) == 0 {
		return false
	}

	reader := bytes.NewReader(data)
	r, err := pdf.NewReader(reader, int64(len(data)))
	if err != nil {
		return false
	}

	pagesToCheck := r.NumPage()
	if pagesToCheck > 3 {
		pagesToCheck = 3
	}

	var total int
	for i := 1; i <= pagesToCheck; i++ {
		page := r.Page(i)
		if page.V.IsNull() {
			continue
		}
		text, err := page.GetPlainText(nil)
		if err != nil {
			continue
		}
		total += countPrintable(text)
		if total >= minTextCharsForTextBased {
			return true
		}
	}
	return false
}

func countPrintable(s string) int {
	s = strings.TrimSpace(s)
	n := 0
	for _, r := range s {
		if unicode.IsPrint(r) && !unicode.IsSpace(r) {
			n++
		}
	}
	return n
}
