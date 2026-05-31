package handler

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"go.einride.tech/aip/filtering"
)

func TestNewListFilesFilterDeclarations_TimeFields(t *testing.T) {
	c := qt.New(t)

	declarations, err := newListFilesFilterDeclarations()
	c.Assert(err, qt.IsNil)

	filter := `update_time >= timestamp("2026-01-01T00:00:00.000Z") AND create_time < timestamp("2027-01-01T00:00:00.000Z") AND NOT in_collection`
	parsed, err := filtering.ParseFilter(filterRequestWrapper{filter: filter}, declarations)
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.CheckedExpr, qt.IsNotNil)
}
