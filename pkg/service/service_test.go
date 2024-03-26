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

	newReq := func(reqMod func(*artifactPB.ListRepositoryTagsRequest)) *artifactPB.ListRepositoryTagsRequest {
		req := &artifactPB.ListRepositoryTagsRequest{
			Parent: "repositories/" + repo,
		}
		if reqMod != nil {
			reqMod(req)
		}
		return req
	}

	tagIDs := []string{"v1", "v2"}
	want := []*artifactPB.RepositoryTag{
		{
			Name: "repositories/krule-wombat/llava-34b/tags/v1",
			Id:   "v1",
		},
		{
			Name: "repositories/krule-wombat/llava-34b/tags/v2",
			Id:   "v2",
		},
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
			name:         "ok",
			registryTags: tagIDs,
			want:         want,
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
