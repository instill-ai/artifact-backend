package types

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestTextChunkReference_JSONMarshalPascalCase verifies that TextChunkReference
// marshals to JSON with PascalCase field names (PageRange, not page_range)
func TestTextChunkReference_JSONMarshalPascalCase(t *testing.T) {
	c := qt.New(t)

	ref := TextChunkReference{
		PageRange: [2]uint32{1, 5},
	}

	jsonBytes, err := json.Marshal(ref)
	c.Assert(err, qt.IsNil)

	jsonStr := string(jsonBytes)
	c.Check(jsonStr, qt.Contains, "PageRange", qt.Commentf("Should use PascalCase"))
	c.Check(jsonStr, qt.Not(qt.Contains), "page_range", qt.Commentf("Should NOT use snake_case"))

	// Verify exact format
	c.Check(jsonStr, qt.Equals, `{"PageRange":[1,5]}`)
}

// TestTextChunkReference_JSONUnmarshalPascalCase verifies that TextChunkReference
// can unmarshal from JSON with PascalCase field names
func TestTextChunkReference_JSONUnmarshalPascalCase(t *testing.T) {
	c := qt.New(t)

	jsonStr := `{"PageRange":[2,10]}`
	var ref TextChunkReference

	err := json.Unmarshal([]byte(jsonStr), &ref)
	c.Assert(err, qt.IsNil)
	c.Check(ref.PageRange, qt.DeepEquals, [2]uint32{2, 10})
}

// TestTextChunkReference_JSONUnmarshalSnakeCase verifies backward compatibility
// with legacy snake_case format (should NOT work since we enforce PascalCase)
func TestTextChunkReference_JSONUnmarshalSnakeCase_ShouldFail(t *testing.T) {
	c := qt.New(t)

	// Old format with snake_case should not unmarshal into PageRange field
	jsonStr := `{"page_range":[2,10]}`
	var ref TextChunkReference

	err := json.Unmarshal([]byte(jsonStr), &ref)
	c.Assert(err, qt.IsNil) // Unmarshal succeeds but field is not populated

	// The PageRange field should be empty (zero value) since the JSON uses wrong case
	c.Check(ref.PageRange, qt.DeepEquals, [2]uint32{0, 0},
		qt.Commentf("snake_case JSON should not populate PascalCase field"))
}

// TestPositionData_JSONMarshalPascalCase verifies that PositionData
// marshals to JSON with PascalCase field names (PageDelimiters, not page_delimiters)
func TestPositionData_JSONMarshalPascalCase(t *testing.T) {
	c := qt.New(t)

	pos := PositionData{
		PageDelimiters: []uint32{0, 100, 200, 300},
	}

	jsonBytes, err := json.Marshal(pos)
	c.Assert(err, qt.IsNil)

	jsonStr := string(jsonBytes)
	c.Check(jsonStr, qt.Contains, "PageDelimiters", qt.Commentf("Should use PascalCase"))
	c.Check(jsonStr, qt.Not(qt.Contains), "page_delimiters", qt.Commentf("Should NOT use snake_case"))

	// Verify exact format
	c.Check(jsonStr, qt.Equals, `{"PageDelimiters":[0,100,200,300]}`)
}

// TestPositionData_JSONUnmarshalPascalCase verifies that PositionData
// can unmarshal from JSON with PascalCase field names
func TestPositionData_JSONUnmarshalPascalCase(t *testing.T) {
	c := qt.New(t)

	jsonStr := `{"PageDelimiters":[0,50,100]}`
	var pos PositionData

	err := json.Unmarshal([]byte(jsonStr), &pos)
	c.Assert(err, qt.IsNil)
	c.Check(pos.PageDelimiters, qt.DeepEquals, []uint32{0, 50, 100})
}

// TestPositionData_JSONUnmarshalSnakeCase_ShouldFail verifies that
// snake_case JSON does not populate PascalCase fields (enforcing standard)
func TestPositionData_JSONUnmarshalSnakeCase_ShouldFail(t *testing.T) {
	c := qt.New(t)

	// Old format with snake_case should not unmarshal into PageDelimiters field
	jsonStr := `{"page_delimiters":[0,50,100]}`
	var pos PositionData

	err := json.Unmarshal([]byte(jsonStr), &pos)
	c.Assert(err, qt.IsNil) // Unmarshal succeeds but field is not populated

	// The PageDelimiters field should be empty since the JSON uses wrong case
	c.Check(pos.PageDelimiters, qt.IsNil,
		qt.Commentf("snake_case JSON should not populate PascalCase field"))
}

// TestTextChunkReference_EmptyValue tests marshaling of zero value
func TestTextChunkReference_EmptyValue(t *testing.T) {
	c := qt.New(t)

	ref := TextChunkReference{}

	jsonBytes, err := json.Marshal(ref)
	c.Assert(err, qt.IsNil)

	// PageRange is an array [2]uint32, so zero value is [0,0] which gets marshaled
	// The omitempty tag doesn't work with arrays (only slices and pointers)
	jsonStr := string(jsonBytes)
	c.Check(jsonStr, qt.Equals, `{"PageRange":[0,0]}`)
}

// TestTextChunkReference_RoundTrip tests full marshal/unmarshal cycle
func TestTextChunkReference_RoundTrip(t *testing.T) {
	c := qt.New(t)

	original := TextChunkReference{
		PageRange: [2]uint32{3, 7},
	}

	// Marshal to JSON
	jsonBytes, err := json.Marshal(original)
	c.Assert(err, qt.IsNil)

	// Unmarshal back
	var decoded TextChunkReference
	err = json.Unmarshal(jsonBytes, &decoded)
	c.Assert(err, qt.IsNil)

	// Should be identical
	c.Check(decoded, qt.DeepEquals, original)
}

// TestPositionData_RoundTrip tests full marshal/unmarshal cycle
func TestPositionData_RoundTrip(t *testing.T) {
	c := qt.New(t)

	original := PositionData{
		PageDelimiters: []uint32{0, 123, 456, 789},
	}

	// Marshal to JSON
	jsonBytes, err := json.Marshal(original)
	c.Assert(err, qt.IsNil)

	// Unmarshal back
	var decoded PositionData
	err = json.Unmarshal(jsonBytes, &decoded)
	c.Assert(err, qt.IsNil)

	// Should be identical
	c.Check(decoded, qt.DeepEquals, original)
}
