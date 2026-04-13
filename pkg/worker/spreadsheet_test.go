package worker

import (
	"strings"
	"testing"

	"github.com/xuri/excelize/v2"
)

func createTestXLSX(t *testing.T, sheets map[string][][]string) []byte {
	t.Helper()
	f := excelize.NewFile()
	defer f.Close()

	first := true
	for name, rows := range sheets {
		if first {
			if err := f.SetSheetName("Sheet1", name); err != nil {
				t.Fatal(err)
			}
			first = false
		} else {
			if _, err := f.NewSheet(name); err != nil {
				t.Fatal(err)
			}
		}
		for r, row := range rows {
			for c, val := range row {
				cell, _ := excelize.CoordinatesToCellName(c+1, r+1)
				if err := f.SetCellValue(name, cell, val); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	buf, err := f.WriteToBuffer()
	if err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestConvertSpreadsheetToMarkdown_SingleSheet(t *testing.T) {
	data := createTestXLSX(t, map[string][][]string{
		"Revenue": {
			{"Quarter", "Amount"},
			{"Q1", "$1M"},
			{"Q2", "$1.5M"},
		},
	})

	md, posData, length, pageCount, err := convertSpreadsheetToMarkdown(data)
	if err != nil {
		t.Fatal(err)
	}

	if pageCount != 1 {
		t.Errorf("pageCount = %d, want 1", pageCount)
	}

	if !strings.Contains(md, "[Sheet: Revenue]") {
		t.Error("markdown should contain [Sheet: Revenue] marker")
	}

	if !strings.Contains(md, "| Quarter | Amount |") {
		t.Error("markdown should contain header row")
	}

	if !strings.Contains(md, "| Q1 | $1M |") {
		t.Error("markdown should contain data rows")
	}

	if posData == nil {
		t.Fatal("positionData should not be nil")
	}

	if len(length) != 1 || length[0] == 0 {
		t.Errorf("length should have 1 non-zero element, got %v", length)
	}
}

func TestConvertSpreadsheetToMarkdown_MultiSheet(t *testing.T) {
	data := createTestXLSX(t, map[string][][]string{
		"Revenue": {
			{"Quarter", "Amount"},
			{"Q1", "$1M"},
		},
		"Expenses": {
			{"Category", "Cost"},
			{"Payroll", "$800K"},
		},
	})

	md, posData, _, pageCount, err := convertSpreadsheetToMarkdown(data)
	if err != nil {
		t.Fatal(err)
	}

	if pageCount != 2 {
		t.Errorf("pageCount = %d, want 2", pageCount)
	}

	if !strings.Contains(md, "[Sheet: Revenue]") {
		t.Error("markdown should contain [Sheet: Revenue]")
	}
	if !strings.Contains(md, "[Sheet: Expenses]") {
		t.Error("markdown should contain [Sheet: Expenses]")
	}

	if posData == nil || len(posData.PageDelimiters) != 2 {
		t.Errorf("should have 2 page delimiters, got %v", posData)
	}
}

func TestConvertSpreadsheetToMarkdown_EmptySheet(t *testing.T) {
	data := createTestXLSX(t, map[string][][]string{
		"Empty": {},
	})

	md, _, _, pageCount, err := convertSpreadsheetToMarkdown(data)
	if err != nil {
		t.Fatal(err)
	}

	if pageCount != 1 {
		t.Errorf("pageCount = %d, want 1", pageCount)
	}

	if !strings.Contains(md, "[Sheet: Empty]") {
		t.Error("markdown should contain [Sheet: Empty] marker even for empty sheet")
	}
}

func TestConvertSpreadsheetToMarkdown_RowCap(t *testing.T) {
	rows := make([][]string, maxSpreadsheetRows+100)
	rows[0] = []string{"Header"}
	for i := 1; i < len(rows); i++ {
		rows[i] = []string{"data"}
	}

	data := createTestXLSX(t, map[string][][]string{
		"Big": rows,
	})

	md, _, _, _, err := convertSpreadsheetToMarkdown(data)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(md, "more rows") {
		t.Error("markdown should contain truncation note")
	}
}

func TestConvertSpreadsheetToMarkdown_ColumnCap(t *testing.T) {
	header := make([]string, maxSpreadsheetCols+10)
	for i := range header {
		header[i] = "Col"
	}
	data := createTestXLSX(t, map[string][][]string{
		"Wide": {header},
	})

	md, _, _, _, err := convertSpreadsheetToMarkdown(data)
	if err != nil {
		t.Fatal(err)
	}

	// Count pipe characters in the header line to verify column cap
	lines := strings.Split(md, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "|") && strings.Contains(line, "Col") {
			pipes := strings.Count(line, "|")
			// pipes = colCount + 1 (leading pipe)
			if pipes > maxSpreadsheetCols+1 {
				t.Errorf("header has %d pipes, expected at most %d (cap=%d)",
					pipes, maxSpreadsheetCols+1, maxSpreadsheetCols)
			}
			break
		}
	}
}

func TestEscapeMarkdownTableCell(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"simple", "simple"},
		{"has|pipe", "has\\|pipe"},
		{"has\nnewline", "has newline"},
		{"has\r\nnewline", "has newline"},
	}
	for _, tt := range tests {
		got := escapeMarkdownTableCell(tt.input)
		if got != tt.want {
			t.Errorf("escapeMarkdownTableCell(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
