package handler

import (
	"fmt"
	"testing"
	"unicode/utf8"
)

func TestUploadCatalogFile(t *testing.T) {
	// Check rune count
	input := "-"

	actual := utf8.RuneCountInString(input)
	// print actual
	fmt.Println(actual)

	// check string length
	expected := len(input)
	fmt.Println(expected)
}
