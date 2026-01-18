package repository

import (
	"testing"

	qt "github.com/frankban/quicktest"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
)

// TestCatalogTypeEnumValues verifies the enum values match expected protobuf definitions
// This ensures that the protobuf enum is correctly imported and usable
func TestCatalogTypeEnumValues(t *testing.T) {
	c := qt.New(t)

	// Verify enum integer values (these are defined in the protobuf)
	c.Check(int32(artifactpb.KnowledgeBaseType_KNOWLEDGE_BASE_TYPE_UNSPECIFIED), qt.Equals, int32(0))
	c.Check(int32(artifactpb.KnowledgeBaseType_KNOWLEDGE_BASE_TYPE_PERSISTENT), qt.Equals, int32(1))
	c.Check(int32(artifactpb.KnowledgeBaseType_KNOWLEDGE_BASE_TYPE_EPHEMERAL), qt.Equals, int32(2))

	// Verify string representation for database storage
	c.Check(artifactpb.KnowledgeBaseType_KNOWLEDGE_BASE_TYPE_PERSISTENT.String(), qt.Equals, "KNOWLEDGE_BASE_TYPE_PERSISTENT")
	c.Check(artifactpb.KnowledgeBaseType_KNOWLEDGE_BASE_TYPE_EPHEMERAL.String(), qt.Equals, "KNOWLEDGE_BASE_TYPE_EPHEMERAL")
	c.Check(artifactpb.KnowledgeBaseType_KNOWLEDGE_BASE_TYPE_UNSPECIFIED.String(), qt.Equals, "KNOWLEDGE_BASE_TYPE_UNSPECIFIED")
}

// TestKnowledgeBaseTypeStringFormat verifies that knowledge base type enums convert to the correct
// string format that should be stored in the database
func TestCatalogTypeStringFormat(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		enum     artifactpb.KnowledgeBaseType
		expected string
	}{
		{
			name:     "PERSISTENT enum to string",
			enum:     artifactpb.KnowledgeBaseType_KNOWLEDGE_BASE_TYPE_PERSISTENT,
			expected: "KNOWLEDGE_BASE_TYPE_PERSISTENT",
		},
		{
			name:     "EPHEMERAL enum to string",
			enum:     artifactpb.KnowledgeBaseType_KNOWLEDGE_BASE_TYPE_EPHEMERAL,
			expected: "KNOWLEDGE_BASE_TYPE_EPHEMERAL",
		},
		{
			name:     "UNSPECIFIED enum to string",
			enum:     artifactpb.KnowledgeBaseType_KNOWLEDGE_BASE_TYPE_UNSPECIFIED,
			expected: "KNOWLEDGE_BASE_TYPE_UNSPECIFIED",
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

	// Create a model with knowledge base type
	model := KnowledgeBaseModel{
		KnowledgeBaseType: artifactpb.KnowledgeBaseType_KNOWLEDGE_BASE_TYPE_PERSISTENT.String(),
	}

	// Verify the field is stored as the full enum name
	c.Check(model.KnowledgeBaseType, qt.Equals, "KNOWLEDGE_BASE_TYPE_PERSISTENT")
	c.Check(model.KnowledgeBaseType, qt.Not(qt.Equals), "persistent",
		qt.Commentf("Should use full enum name, not legacy format"))
}
