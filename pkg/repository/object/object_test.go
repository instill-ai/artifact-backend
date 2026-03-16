package object

import (
	"testing"

	"github.com/gofrs/uuid"
	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/types"
)

func TestGetBlobObjectPath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name      string
		nsUID     types.NamespaceUIDType
		objUID    types.ObjectUIDType
		wantPath  string
	}{
		{
			name:     "standard UUIDs",
			nsUID:    uuid.Must(uuid.FromString("11111111-1111-1111-1111-111111111111")),
			objUID:   uuid.Must(uuid.FromString("22222222-2222-2222-2222-222222222222")),
			wantPath: "ns-11111111-1111-1111-1111-111111111111/obj-22222222-2222-2222-2222-222222222222",
		},
		{
			name:     "nil UUIDs",
			nsUID:    uuid.Nil,
			objUID:   uuid.Nil,
			wantPath: "ns-00000000-0000-0000-0000-000000000000/obj-00000000-0000-0000-0000-000000000000",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			got := GetBlobObjectPath(tt.nsUID, tt.objUID)
			c.Check(got, qt.Equals, tt.wantPath)
		})
	}
}

func TestConstants(t *testing.T) {
	c := qt.New(t)

	c.Check(BlobBucketName, qt.Equals, "core-blob")
	c.Check(ConvertedFileDir, qt.Equals, "converted-file")
	c.Check(ChunkDir, qt.Equals, "chunk")
}
