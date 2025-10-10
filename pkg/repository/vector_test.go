package repository

import (
	"testing"

	"github.com/gofrs/uuid"

	qt "github.com/frankban/quicktest"
	"github.com/instill-ai/artifact-backend/pkg/types"
)

func TestFileUIDFilter(t *testing.T) {
	c := qt.New(t)

	// Create some test UUIDs
	uid1 := uuid.FromStringOrNil("6e362976-bfc1-4677-b761-dcc12495b5bd")
	uid2 := uuid.FromStringOrNil("f6f9f6ed-ab85-4753-9dbc-6dbd9a3818f3")
	badUID := uuid.FromStringOrNil("nope")

	testcases := []struct {
		name     string
		fileUIDs []types.FileUIDType
		want     string
	}{
		{
			name:     "ok - single valid UUID",
			fileUIDs: []types.FileUIDType{uid1},
			want:     `file_uid in ["6e362976-bfc1-4677-b761-dcc12495b5bd"]`,
		},
		{
			name:     "ok - multiple valid UUIDs",
			fileUIDs: []types.FileUIDType{uid1, uid2},
			want:     `file_uid in ["6e362976-bfc1-4677-b761-dcc12495b5bd","f6f9f6ed-ab85-4753-9dbc-6dbd9a3818f3"]`,
		},
		{
			name:     "ok - empty slice",
			fileUIDs: []types.FileUIDType{},
			want:     "",
		},
		{
			name:     "ok - nil slice",
			fileUIDs: nil,
			want:     "",
		},
		{
			name:     "ok - single nil UUID",
			fileUIDs: []types.FileUIDType{badUID},
			want:     "",
		},
		{
			name:     "ok - mixed valid and nil UUIDs",
			fileUIDs: []types.FileUIDType{uid1, badUID, uid2},
			want:     `file_uid in ["6e362976-bfc1-4677-b761-dcc12495b5bd","f6f9f6ed-ab85-4753-9dbc-6dbd9a3818f3"]`,
		},
	}

	for _, tc := range testcases {
		c.Run(tc.name, func(c *qt.C) {
			client := &milvusClient{}
			got := client.fileUIDFilter(tc.fileUIDs)
			c.Check(got, qt.Equals, tc.want)
		})
	}
}
