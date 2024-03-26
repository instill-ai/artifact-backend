package service

import (
	"context"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/gojuno/minimock/v3"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/instill-ai/artifact-backend/pkg/mock"
	artifactPB "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	timestampPB "google.golang.org/protobuf/types/known/timestamppb"
)

const repo = "krule-wombat/llava-34b"

var cmpPB = qt.CmpEquals(
	cmpopts.IgnoreUnexported(
		artifactPB.RepositoryTag{},
		timestampPB.Timestamp{},
	),
)

func TestService_ListRepositoryTags(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	var pageSize int32 = 2
	newReq := func(reqMod func(*artifactPB.ListRepositoryTagsRequest)) *artifactPB.ListRepositoryTagsRequest {
		req := &artifactPB.ListRepositoryTagsRequest{
			Parent:   "repositories/" + repo,
			PageSize: &pageSize,
		}
		if reqMod != nil {
			reqMod(req)
		}
		return req
	}

	tagIDs := []string{"1.0.0", "1.0.1", "1.1.0-beta", "1.1.0", "latest"}
	want := make([]*artifactPB.RepositoryTag, len(tagIDs))
	for i, tagID := range tagIDs {
		want[i] = &artifactPB.RepositoryTag{
			Name: "repositories/krule-wombat/llava-34b/tags/" + tagID,
			Id:   tagID,
		}
	}

	testcases := []struct {
		name string
		in   func(*artifactPB.ListRepositoryTagsRequest)

		registryTags []string
		registryErr  error

		wantErr string
		want    []*artifactPB.RepositoryTag
	}{
		{
			name:    "nok - namespace error",
			in:      func(req *artifactPB.ListRepositoryTagsRequest) { req.Parent = "repository/" + repo },
			wantErr: "namespace error",
		},
		{
			name:        "nok - client error",
			registryErr: fmt.Errorf("foo"),
			wantErr:     "foo",
		},
		{
			name:         "ok - no pagination",
			in:           func(req *artifactPB.ListRepositoryTagsRequest) { req.PageSize = nil },
			registryTags: tagIDs,
			want:         want,
		},
		{
			name: "ok - page -1",
			in: func(req *artifactPB.ListRepositoryTagsRequest) {
				var page int32 = -1
				req.Page = &page
			},

			registryTags: tagIDs,
			want:         want[:2],
		},
		{
			name:         "ok - page 0",
			registryTags: tagIDs,
			want:         want[:2],
		},
		{
			name: "ok - page 1",
			in: func(req *artifactPB.ListRepositoryTagsRequest) {
				var page int32 = 1
				req.Page = &page
			},

			registryTags: tagIDs,
			want:         want[2:4],
		},
		{
			name: "ok - page 2",
			in: func(req *artifactPB.ListRepositoryTagsRequest) {
				var page int32 = 2
				req.Page = &page
			},

			registryTags: tagIDs,
			want:         want[4:],
		},
		{
			name: "ok - page 3",
			in: func(req *artifactPB.ListRepositoryTagsRequest) {
				var page int32 = 3
				req.Page = &page
			},

			registryTags: tagIDs,
			want:         want[0:0],
		},
	}

	for _, tc := range testcases {
		c.Run(tc.name, func(c *qt.C) {
			registry := mock.NewRegistryClientMock(c)
			if tc.registryTags != nil || tc.registryErr != nil {
				registry.ListTagsMock.When(minimock.AnyContext, repo).
					Then(tc.registryTags, tc.registryErr)
			}

			s := NewService(registry)
			got, err := s.ListRepositoryTags(ctx, newReq(tc.in))
			if tc.wantErr != "" {
				c.Check(err, qt.ErrorMatches, tc.wantErr)
				return
			}

			c.Check(got.GetTags(), cmpPB, tc.want)
		})
	}
}
