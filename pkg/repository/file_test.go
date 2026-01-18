package repository

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// test ExtraMetaDataUnmarshal, when extra metadata is empty
func TestKnowledgeBaseFile_ExtraMetaDataUnmarshal_Empty(t *testing.T) {
	c := qt.New(t)

	kf := FileModel{ExtraMetaData: `{"status_message":"processing complete"}`}
	err := kf.ExtraMetaDataUnmarshalFunc()
	c.Check(err, qt.IsNil)
	c.Check(kf.ExtraMetaDataUnmarshal.StatusMessage, qt.Equals, "processing complete")
}

// test ExtraMetaDataUnmarshal, when extra metadata is ""
func TestKnowledgeBaseFile_ExtraMetaDataUnmarshal_EmptyString(t *testing.T) {
	c := qt.New(t)

	kf := FileModel{ExtraMetaData: ""}
	err := kf.ExtraMetaDataUnmarshalFunc()
	c.Check(err, qt.IsNil)
	c.Check(kf.ExtraMetaDataUnmarshal, qt.IsNil)
}

// test ExtraMetaDataMarshal when ExtraMetaDataUnmarshal is nil
func TestKnowledgeBaseFile_ExtraMetaDataMarshal_Nil(t *testing.T) {
	c := qt.New(t)

	kf := FileModel{}
	err := kf.ExtraMetaDataMarshal()
	c.Check(err, qt.IsNil)
	c.Check(kf.ExtraMetaData, qt.Equals, "{}")
}
