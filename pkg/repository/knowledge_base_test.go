package repository

import (
	"testing"

	qt "github.com/frankban/quicktest"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// TestCatalogTypeEnumValues verifies the enum values match expected protobuf definitions
// This ensures that the protobuf enum is correctly imported and usable
func TestCatalogTypeEnumValues(t *testing.T) {
	c := qt.New(t)

	// Verify enum integer values (these are defined in the protobuf)
	c.Check(int32(artifactpb.CatalogType_CATALOG_TYPE_UNSPECIFIED), qt.Equals, int32(0))
	c.Check(int32(artifactpb.CatalogType_CATALOG_TYPE_PERSISTENT), qt.Equals, int32(1))
	c.Check(int32(artifactpb.CatalogType_CATALOG_TYPE_EPHEMERAL), qt.Equals, int32(2))

	// Verify string representation for database storage
	c.Check(artifactpb.CatalogType_CATALOG_TYPE_PERSISTENT.String(), qt.Equals, "CATALOG_TYPE_PERSISTENT")
	c.Check(artifactpb.CatalogType_CATALOG_TYPE_EPHEMERAL.String(), qt.Equals, "CATALOG_TYPE_EPHEMERAL")
	c.Check(artifactpb.CatalogType_CATALOG_TYPE_UNSPECIFIED.String(), qt.Equals, "CATALOG_TYPE_UNSPECIFIED")
}

// TestCatalogTypeStringFormat verifies that catalog type enums convert to the correct
// string format that should be stored in the database
func TestCatalogTypeStringFormat(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		enum     artifactpb.CatalogType
		expected string
	}{
		{
			name:     "PERSISTENT enum to string",
			enum:     artifactpb.CatalogType_CATALOG_TYPE_PERSISTENT,
			expected: "CATALOG_TYPE_PERSISTENT",
		},
		{
			name:     "EPHEMERAL enum to string",
			enum:     artifactpb.CatalogType_CATALOG_TYPE_EPHEMERAL,
			expected: "CATALOG_TYPE_EPHEMERAL",
		},
		{
			name:     "UNSPECIFIED enum to string",
			enum:     artifactpb.CatalogType_CATALOG_TYPE_UNSPECIFIED,
			expected: "CATALOG_TYPE_UNSPECIFIED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.enum.String()
			c.Check(result, qt.Equals, tt.expected,
				qt.Commentf("Enum %d should convert to string %q", tt.enum, tt.expected))
		})
	}
}

// TestKnowledgeBaseModel_CatalogTypeField verifies that the CatalogType field
// is correctly defined as a string in the model for database storage
func TestKnowledgeBaseModel_CatalogTypeField(t *testing.T) {
	c := qt.New(t)

	// Create a model with catalog type
	model := KnowledgeBaseModel{
		CatalogType: artifactpb.CatalogType_CATALOG_TYPE_PERSISTENT.String(),
	}

	// Verify the field is stored as the full enum name
	c.Check(model.CatalogType, qt.Equals, "CATALOG_TYPE_PERSISTENT")
	c.Check(model.CatalogType, qt.Not(qt.Equals), "persistent",
		qt.Commentf("Should use full enum name, not legacy format"))
}
