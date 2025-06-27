package repository

import (
	"testing"
)

// test ExtraMetaDataUnmarshal, when extra metadata is empty
func TestKnowledgeBaseFile_ExtraMetaDataUnmarshal_Empty(t *testing.T) {
	kf := KnowledgeBaseFile{ExtraMetaData: `{"fail_reason":"some reason"}`}
	err := kf.ExtraMetaDataUnmarshalFunc()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if kf.ExtraMetaDataUnmarshal.FailReason != "some reason" {
		t.Errorf("Expected FaileReason to be %q, but got %q", "some reason", kf.ExtraMetaDataUnmarshal.FailReason)
	}
}

// test ExtraMetaDataUnmarshal, when extra metadata is ""
func TestKnowledgeBaseFile_ExtraMetaDataUnmarshal_EmptyString(t *testing.T) {
	kf := KnowledgeBaseFile{ExtraMetaData: ""}
	err := kf.ExtraMetaDataUnmarshalFunc()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if kf.ExtraMetaDataUnmarshal != nil {
		t.Errorf("Expected ExtraMetaDataUnmarshal to be nil, but got %v", kf.ExtraMetaDataUnmarshal)
	}
}

// test ExtraMetaDataMarshal when ExtraMetaDataUnmarshal is nil
func TestKnowledgeBaseFile_ExtraMetaDataMarshal_Nil(t *testing.T) {
	kf := KnowledgeBaseFile{}
	err := kf.ExtraMetaDataMarshal()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if kf.ExtraMetaData != "{}" {
		t.Errorf("Expected ExtraMetaData to be empty, but got %q", kf.ExtraMetaData)
	}
}
