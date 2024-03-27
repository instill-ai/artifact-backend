package service_test

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
	artifact "github.com/instill-ai/artifact-backend/pkg/service"
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

	// Build test data.
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

	wantWithEmptyOptional := make([]*artifactpb.RepositoryTag, len(want))
	tagIDs := make([]string, len(want))
	t0 := time.Now().UTC()
	for i, tag := range want {
		id := tag.Id
		name := "repositories/krule-wombat/llava-34b/tags/" + tag.Id

		tagIDs[i] = id
		wantWithEmptyOptional[i] = &artifactpb.RepositoryTag{Id: id, Name: name}

		want[i].Name = name
		t := t0.Add(-1 * time.Second * time.Duration(i))
		want[i].UpdateTime = timestamppb.New(t)
	}

	testcases := []struct {
		name string
		in   func(*artifactpb.ListRepositoryTagsRequest)

		registryTags []string
		registryErr  error

		repoTags []*artifactpb.RepositoryTag
		repoErr  error

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
			repoTags:     want[0:1],
			repoErr:      fmt.Errorf("foo"),
			wantErr:      "failed to fetch tag .*: foo",
		},
		{
			name:         "ok - not found in repo",
			registryTags: tagIDs,
			repoTags:     want[:2],
			repoErr:      fmt.Errorf("repo error: %w", artifact.ErrNotFound),
			want:         wantWithEmptyOptional,
		},
		{
			name:         "ok - no pagination",
			in:           func(req *artifactpb.ListRepositoryTagsRequest) { req.PageSize = nil },
			registryTags: tagIDs,
			repoTags:     want,
			want:         want,
		},
		{
			name: "ok - page -1",
			in: func(req *artifactpb.ListRepositoryTagsRequest) {
				var page int32 = -1
				req.Page = &page
			},

			registryTags: tagIDs,
			repoTags:     want[:2],
			want:         want[:2],
		},
		{
			name:         "ok - page 0",
			registryTags: tagIDs,
			repoTags:     want[:2],
			want:         want[:2],
		},
		{
			name: "ok - page 1",
			in: func(req *artifactpb.ListRepositoryTagsRequest) {
				var page int32 = 1
				req.Page = &page
			},

			registryTags: tagIDs,
			repoTags:     want[2:4],
			want:         want[2:4],
		},
		{
			name: "ok - page 2",
			in: func(req *artifactpb.ListRepositoryTagsRequest) {
				var page int32 = 2
				req.Page = &page
			},

			registryTags: tagIDs,
			repoTags:     want[4:],
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
			for _, repoTag := range tc.repoTags {
				name := artifact.RepositoryTagName(repoTag.Name)
				repository.GetRepositoryTagMock.When(minimock.AnyContext, name).
					Then(repoTag, tc.repoErr)
			}

			s := artifact.NewService(repository, registry)
			resp, err := s.ListRepositoryTags(ctx, newReq(tc.in))
			if tc.wantErr != "" {
				c.Check(err, qt.ErrorMatches, tc.wantErr)
				return
			}

			c.Check(err, qt.IsNil)
			for i, got := range resp.GetTags() {
				c.Check(got, cmpPB, tc.want[i], qt.Commentf(tc.want[i].Id))
			}
		})
	}
}

func TestService_CreateRepositoryTag(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	newTag := func() *artifactpb.RepositoryTag {
		return &artifactpb.RepositoryTag{
			Name:       "repositories/shake/home/tags/1.3.0",
			Id:         "1.3.0",
			Digest:     "sha256:ab9ed2553e5a1c7f717436ffa070a06da8f2fe15caab4b71e2f02ce4efcae423",
			UpdateTime: timestamppb.Now(),
		}
	}

	c.Run("nok - invalid name", func(c *qt.C) {
		t := newTag()
		t.Name = "shake/home:1.3.0"
		req := &artifactpb.CreateRepositoryTagRequest{Tag: t}

		s := artifact.NewService(nil, nil)
		_, err := s.CreateRepositoryTag(ctx, req)
		c.Check(err, qt.ErrorMatches, "invalid tag name")
	})

	req := &artifactpb.CreateRepositoryTagRequest{Tag: newTag()}
	clearedTag, want := newTag(), newTag()
	clearedTag.UpdateTime = nil

	c.Run("nok - repo error", func(c *qt.C) {
		repository := mock.NewRepositoryMock(c)
		repository.UpsertRepositoryTagMock.When(minimock.AnyContext, clearedTag).Then(nil, fmt.Errorf("foo"))

		s := artifact.NewService(repository, nil)
		_, err := s.CreateRepositoryTag(ctx, req)
		c.Check(err, qt.ErrorMatches, "failed to upsert tag .*: foo")
	})

	c.Run("ok", func(c *qt.C) {
		repository := mock.NewRepositoryMock(c)
		repository.UpsertRepositoryTagMock.When(minimock.AnyContext, clearedTag).Then(want, nil)

		s := artifact.NewService(repository, nil)
		resp, err := s.CreateRepositoryTag(ctx, req)
		c.Check(err, qt.IsNil)
		c.Check(resp.GetTag(), cmpPB, want)
	})
}
