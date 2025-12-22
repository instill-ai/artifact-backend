package repository

import (
	"testing"

	qt "github.com/frankban/quicktest"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

func TestIsUpdateInProgress(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		status   string
		expected bool
	}{
		{
			name:     "UPDATING status",
			status:   artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_UPDATING.String(),
			expected: true,
		},
		{
			name:     "SWAPPING status",
			status:   artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_SWAPPING.String(),
			expected: true,
		},
		{
			name:     "VALIDATING status",
			status:   artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_VALIDATING.String(),
			expected: true,
		},
		{
			name:     "SYNCING status",
			status:   artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_SYNCING.String(),
			expected: true,
		},
		{
			name:     "COMPLETED status",
			status:   artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED.String(),
			expected: false,
		},
		{
			name:     "FAILED status",
			status:   artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_FAILED.String(),
			expected: false,
		},
		{
			name:     "Empty status",
			status:   "",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsUpdateInProgress(tt.status)
			c.Check(result, qt.Equals, tt.expected)
		})
	}
}

func TestIsUpdateComplete(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		status   string
		expected bool
	}{
		{
			name:     "Empty status",
			status:   "",
			expected: true,
		},
		{
			name:     "COMPLETED status",
			status:   artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED.String(),
			expected: true,
		},
		{
			name:     "FAILED status",
			status:   artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_FAILED.String(),
			expected: true,
		},
		{
			name:     "ROLLED_BACK status",
			status:   artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_ROLLED_BACK.String(),
			expected: true,
		},
		{
			name:     "ABORTED status",
			status:   artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_ABORTED.String(),
			expected: true,
		},
		{
			name:     "UPDATING status",
			status:   artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_UPDATING.String(),
			expected: false,
		},
		{
			name:     "SWAPPING status",
			status:   artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_SWAPPING.String(),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsUpdateComplete(tt.status)
			c.Check(result, qt.Equals, tt.expected)
		})
	}
}

func TestIsDualProcessingNeeded(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		status   string
		expected bool
	}{
		{
			name:     "UPDATING status - needs dual processing",
			status:   artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_UPDATING.String(),
			expected: true,
		},
		{
			name:     "SWAPPING status - needs dual processing",
			status:   artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_SWAPPING.String(),
			expected: true,
		},
		{
			name:     "COMPLETED status - needs dual processing (retention period)",
			status:   artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED.String(),
			expected: true,
		},
		{
			name:     "ROLLED_BACK status - needs dual processing (retention period)",
			status:   artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_ROLLED_BACK.String(),
			expected: true,
		},
		{
			name:     "FAILED status - no dual processing",
			status:   artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_FAILED.String(),
			expected: false,
		},
		{
			name:     "Empty status - no dual processing",
			status:   "",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsDualProcessingNeeded(tt.status)
			c.Check(result, qt.Equals, tt.expected)
		})
	}
}

func TestEmbeddingConfigJSON_Value(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		config   SystemConfigJSON
		expected string
	}{
		{
			name: "Valid config",
			config: SystemConfigJSON{
				RAG: RAGConfig{
					Embedding: EmbeddingConfig{
						ModelFamily:    "gemini",
						Dimensionality: 3072,
					},
				},
			},
			expected: `{"rag":{"embedding":{"model_family":"gemini","dimensionality":3072}}}`,
		},
		{
			name:     "Empty config",
			config:   SystemConfigJSON{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err := tt.config.Value()
			if tt.expected == "" {
				c.Check(value, qt.IsNil)
				c.Check(err, qt.IsNil)
			} else {
				c.Check(err, qt.IsNil)
				c.Check(value, qt.Equals, tt.expected)
			}
		})
	}
}

func TestEmbeddingConfigJSON_Scan(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		input    interface{}
		expected SystemConfigJSON
		hasError bool
	}{
		{
			name:  "Valid JSON",
			input: []byte(`{"rag":{"embedding":{"model_family":"gemini","dimensionality":3072}}}`),
			expected: SystemConfigJSON{
				RAG: RAGConfig{
					Embedding: EmbeddingConfig{
						ModelFamily:    "gemini",
						Dimensionality: 3072,
					},
				},
			},
			hasError: false,
		},
		{
			name:     "Nil input",
			input:    nil,
			expected: SystemConfigJSON{},
			hasError: false,
		},
		{
			name:     "Invalid type",
			input:    123,
			expected: SystemConfigJSON{},
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var config SystemConfigJSON
			err := config.Scan(tt.input)
			if tt.hasError {
				c.Check(err, qt.Not(qt.IsNil))
			} else {
				c.Check(err, qt.IsNil)
				c.Check(config.RAG.Embedding.ModelFamily, qt.Equals, tt.expected.RAG.Embedding.ModelFamily)
				c.Check(config.RAG.Embedding.Dimensionality, qt.Equals, tt.expected.RAG.Embedding.Dimensionality)
			}
		})
	}
}

func TestTagsArray_Value(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		tags     TagsArray
		expected string
	}{
		{
			name:     "Multiple tags",
			tags:     TagsArray{"tag1", "tag2", "tag3"},
			expected: "{tag1,tag2,tag3}",
		},
		{
			name:     "Single tag",
			tags:     TagsArray{"tag1"},
			expected: "{tag1}",
		},
		{
			name:     "Empty tags",
			tags:     TagsArray{},
			expected: "{}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err := tt.tags.Value()
			c.Check(err, qt.IsNil)
			c.Check(value, qt.Equals, tt.expected)
		})
	}
}

func TestTagsArray_Scan(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		input    interface{}
		expected TagsArray
	}{
		{
			name:     "Multiple tags",
			input:    "{tag1,tag2,tag3}",
			expected: TagsArray{"tag1", "tag2", "tag3"},
		},
		{
			name:     "Single tag",
			input:    "{tag1}",
			expected: TagsArray{"tag1"},
		},
		{
			name:     "Empty tags",
			input:    "{}",
			expected: TagsArray{},
		},
		{
			name:     "Nil input",
			input:    nil,
			expected: TagsArray{},
		},
		{
			name:     "Tags with quotes",
			input:    `{"tag1","tag2"}`,
			expected: TagsArray{"tag1", "tag2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var tags TagsArray
			err := tags.Scan(tt.input)
			c.Check(err, qt.IsNil)
			c.Check(len(tags), qt.Equals, len(tt.expected))
			for i, tag := range tags {
				c.Check(tag, qt.Equals, tt.expected[i])
			}
		})
	}
}

func TestKnowledgeBaseModel_TableName(t *testing.T) {
	c := qt.New(t)

	model := KnowledgeBaseModel{}
	tableName := model.TableName()

	c.Check(tableName, qt.Equals, "knowledge_base")
}

func TestDualProcessingTarget(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name        string
		target      *DualProcessingTarget
		checkFields bool
	}{
		{
			name: "Dual processing needed",
			target: &DualProcessingTarget{
				IsNeeded:    true,
				TargetKB:    &KnowledgeBaseModel{},
				Phase:       "updating",
				Description: "Test description",
			},
			checkFields: true,
		},
		{
			name: "Dual processing not needed",
			target: &DualProcessingTarget{
				IsNeeded:    false,
				TargetKB:    nil,
				Phase:       "",
				Description: "",
			},
			checkFields: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.checkFields {
				c.Check(tt.target.IsNeeded, qt.Equals, tt.target.IsNeeded)
				c.Check(tt.target.Phase, qt.Equals, tt.target.Phase)
				c.Check(tt.target.Description, qt.Equals, tt.target.Description)
			}
		})
	}
}

func TestKnowledgeBaseColumns(t *testing.T) {
	c := qt.New(t)

	// Test that column constants are properly defined
	c.Check(KnowledgeBaseColumn.UID, qt.Equals, "uid")
	c.Check(KnowledgeBaseColumn.KBID, qt.Equals, "id")
	c.Check(KnowledgeBaseColumn.Description, qt.Equals, "description")
	c.Check(KnowledgeBaseColumn.Tags, qt.Equals, "tags")
	c.Check(KnowledgeBaseColumn.NamespaceUID, qt.Equals, "namespace_uid")
	c.Check(KnowledgeBaseColumn.CreateTime, qt.Equals, "create_time")
	c.Check(KnowledgeBaseColumn.UpdateTime, qt.Equals, "update_time")
	c.Check(KnowledgeBaseColumn.DeleteTime, qt.Equals, "delete_time")
	c.Check(KnowledgeBaseColumn.Usage, qt.Equals, "usage")
	c.Check(KnowledgeBaseColumn.KnowledgeBaseType, qt.Equals, "knowledge_base_type")
	c.Check(KnowledgeBaseColumn.ActiveCollectionUID, qt.Equals, "active_collection_uid")
	c.Check(KnowledgeBaseColumn.Staging, qt.Equals, "staging")
	c.Check(KnowledgeBaseColumn.UpdateStatus, qt.Equals, "update_status")
	c.Check(KnowledgeBaseColumn.UpdateWorkflowID, qt.Equals, "update_workflow_id")
	c.Check(KnowledgeBaseColumn.UpdateStartedAt, qt.Equals, "update_started_at")
	c.Check(KnowledgeBaseColumn.UpdateCompletedAt, qt.Equals, "update_completed_at")
	c.Check(KnowledgeBaseColumn.RollbackRetentionUntil, qt.Equals, "rollback_retention_until")
}

func TestParsePostgresArray(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "Simple array",
			input:    "{a,b,c}",
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "Array with quotes",
			input:    `{"a","b","c"}`,
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "Empty array",
			input:    "{}",
			expected: []string{},
		},
		{
			name:     "Single element",
			input:    "{a}",
			expected: []string{"a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parsePostgresArray(tt.input)
			c.Check(len(result), qt.Equals, len(tt.expected))
			for i, val := range result {
				c.Check(val, qt.Equals, tt.expected[i])
			}
		})
	}
}

func TestFormatPostgresArray(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		input    []string
		expected string
	}{
		{
			name:     "Multiple elements",
			input:    []string{"a", "b", "c"},
			expected: "{a,b,c}",
		},
		{
			name:     "Single element",
			input:    []string{"a"},
			expected: "{a}",
		},
		{
			name:     "Empty array",
			input:    []string{},
			expected: "{}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatPostgresArray(tt.input)
			c.Check(result, qt.Equals, tt.expected)
		})
	}
}
