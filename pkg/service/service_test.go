package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/gojuno/minimock/v3"
	"github.com/google/go-cmp/cmp/cmpopts"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/instill-ai/artifact-backend/pkg/mock"
)

const repo = "krule-wombat/llava-34b"

var cmpPB = qt.CmpEquals(
	cmpopts.IgnoreUnexported(
		artifactpb.RepositoryTag{},
		timestamppb.Timestamp{},
	),
)

func TestService_ListRepositoryTags(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	var pageSize int32 = 2
	newReq := func(reqMod func(*artifactpb.ListRepositoryTagsRequest)) *artifactpb.ListRepositoryTagsRequest {
		req := &artifactpb.ListRepositoryTagsRequest{
			Parent:   "repositories/" + repo,
			PageSize: &pageSize,
		}
		if reqMod != nil {
			reqMod(req)
		}
		return req
	}

	want := []*artifactpb.RepositoryTag{
		{
			Id:     "1.0.0",
			Digest: "sha256:fffdcab19393a354155d33f2eec1fca1e35c70989f6a804ecc9fa66e4919cfe6",
		},
		{
			Id:     "1.0.1",
			Digest: "sha256:3f6974ba91e0ed662c1230d064f2540c6f47e124165a02bde1aee1d8151240ec",
		},
		{
			Id:     "1.1.0-beta",
			Digest: "sha256:6511193f8114a2f011790619698efe12a8119ed9a17e2e36f4c1c759ccf173ab",
		},
		{
			Id:     "1.1.0",
			Digest: "sha256:6beea2e5531a0606613594fd3ed92d71bbdcef99dd3237522049a0b32cad736c",
		},
		{
			Id:     "latest",
			Digest: "sha256:48a2ee4befe9662f1b5056c5c03464b2eec6dd6855e299ef7dd72c8daafa0a02",
		},
	}

	// Build test data.
	tagIDs := make([]string, len(want))
	t0 := time.Now().UTC()
	for i, tag := range want {
		tagIDs[i] = tag.Id
		want[i].Name = "repositories/krule-wombat/llava-34b/tags/" + tag.Id

		t := t0.Add(-1 * time.Second * time.Duration(i))
		want[i].UpdateTime = timestamppb.New(t)
	}

	testcases := []struct {
		name string
		in   func(*artifactpb.ListRepositoryTagsRequest)

		registryTags []string
		registryErr  error

		repoErr error

		wantErr string
		want    []*artifactpb.RepositoryTag
	}{
		{
			name:    "nok - namespace error",
			in:      func(req *artifactpb.ListRepositoryTagsRequest) { req.Parent = "repository/" + repo },
			wantErr: "namespace error",
		},
		{
			name:        "nok - client error",
			registryErr: fmt.Errorf("foo"),
			wantErr:     "foo",
		},
		{
			name:         "nok - repo error",
			registryTags: tagIDs,
			repoErr:      fmt.Errorf("foo"),
			wantErr:      "failed to fetch tag .*: foo",
		},
		{
			name:         "ok - no pagination",
			in:           func(req *artifactpb.ListRepositoryTagsRequest) { req.PageSize = nil },
			registryTags: tagIDs,
			want:         want,
		},
		{
			name: "ok - page -1",
			in: func(req *artifactpb.ListRepositoryTagsRequest) {
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
			in: func(req *artifactpb.ListRepositoryTagsRequest) {
				var page int32 = 1
				req.Page = &page
			},

			registryTags: tagIDs,
			want:         want[2:4],
		},
		{
			name: "ok - page 2",
			in: func(req *artifactpb.ListRepositoryTagsRequest) {
				var page int32 = 2
				req.Page = &page
			},

			registryTags: tagIDs,
			want:         want[4:],
		},
		{
			name: "ok - page 3",
			in: func(req *artifactpb.ListRepositoryTagsRequest) {
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

			repository := mock.NewRepositoryMock(c)
			for _, wantedTag := range tc.want {
				repository.GetRepositoryTagMock.When(minimock.AnyContext, wantedTag.Name).
					Then(wantedTag, nil)
			}

			if tc.repoErr != nil {
				repository.GetRepositoryTagMock.Return(nil, tc.repoErr)
			}

			s := NewService(repository, registry)
			got, err := s.ListRepositoryTags(ctx, newReq(tc.in))
			if tc.wantErr != "" {
				c.Check(err, qt.ErrorMatches, tc.wantErr)
				return
			}

			c.Check(got.GetTags(), cmpPB, tc.want)
		})
	}
}
