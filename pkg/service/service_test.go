package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gojuno/minimock/v3"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/types/known/timestamppb"

	qt "github.com/frankban/quicktest"

	"github.com/instill-ai/artifact-backend/pkg/mock"
	"github.com/instill-ai/artifact-backend/pkg/utils"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
)

const repo = "krule-wombat/llava-34b"

var cmpPB = qt.CmpEquals(
	cmpopts.IgnoreUnexported(
		artifactpb.RepositoryTag{},
		artifactpb.ListRepositoryTagsResponse{},
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

		registryTags      []string
		registryErr       error
		registryDigestErr error

		repoTags []*artifactpb.RepositoryTag
		repoErr  error

		wantErr  string
		wantTags []*artifactpb.RepositoryTag
		respMod  func(*artifactpb.ListRepositoryTagsResponse)
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
			name:              "ok - not found in repo",
			registryTags:      tagIDs,
			repoTags:          want[:2],
			registryDigestErr: fmt.Errorf("foo"),
			repoErr:           fmt.Errorf("repo error: %w", errorsx.ErrNotFound),
			wantTags:          wantWithEmptyOptional[:2],
		},
		{
			name:         "ok - no pagination",
			in:           func(req *artifactpb.ListRepositoryTagsRequest) { req.PageSize = nil },
			registryTags: tagIDs,
			repoTags:     want,
			wantTags:     want,
			respMod: func(resp *artifactpb.ListRepositoryTagsResponse) {
				resp.PageSize = 10
			},
		},
		{
			name: "ok - page -1",
			in: func(req *artifactpb.ListRepositoryTagsRequest) {
				var page int32 = -1
				req.Page = &page
			},

			registryTags: tagIDs,
			repoTags:     want[:2],
			wantTags:     want[:2],
			respMod: func(resp *artifactpb.ListRepositoryTagsResponse) {
				resp.Page = 0
			},
		},
		{
			name:         "ok - page 0",
			registryTags: tagIDs,
			repoTags:     want[:2],
			wantTags:     want[:2],
		},
		{
			name: "ok - page 1",
			in: func(req *artifactpb.ListRepositoryTagsRequest) {
				var page int32 = 1
				req.Page = &page
			},

			registryTags: tagIDs,
			repoTags:     want[2:4],
			wantTags:     want[2:4],
		},
		{
			name: "ok - page 2",
			in: func(req *artifactpb.ListRepositoryTagsRequest) {
				var page int32 = 2
				req.Page = &page
			},

			registryTags: tagIDs,
			repoTags:     want[4:],
			wantTags:     want[4:],
		},
		{
			name: "ok - page 3",
			in: func(req *artifactpb.ListRepositoryTagsRequest) {
				var page int32 = 3
				req.Page = &page
			},

			registryTags: tagIDs,
			wantTags:     want[0:0],
		},
	}

	for _, tc := range testcases {
		c.Run(tc.name, func(c *qt.C) {
			registry := mock.NewRegistryClientMock(c)
			if tc.registryTags != nil || tc.registryErr != nil {
				registry.ListTagsMock.When(minimock.AnyContext, repo).
					Then(tc.registryTags, tc.registryErr)
			}

			repository := mock.NewRepositoryIMock(c)
			for _, repoTag := range tc.repoTags {
				name := utils.RepositoryTagName(repoTag.Name)
				_, id, _ := name.ExtractRepositoryAndID()
				repository.GetRepositoryTagMock.When(minimock.AnyContext, name).
					Then(repoTag, tc.repoErr)
				if tc.registryDigestErr != nil {
					registry.GetTagDigestMock.When(minimock.AnyContext, repo, id).
						Then("", tc.registryDigestErr)
				}
			}

			s := NewService(repository, nil, nil, nil, registry, nil, nil, nil, nil)
			req := newReq(tc.in)
			resp, err := s.ListRepositoryTags(ctx, req)
			if tc.wantErr != "" {
				c.Check(err, qt.ErrorMatches, tc.wantErr)
				return
			}

			c.Check(err, qt.IsNil)

			wantResp := &artifactpb.ListRepositoryTagsResponse{
				Tags:      tc.wantTags,
				TotalSize: int32(len(tc.registryTags)),
				PageSize:  req.GetPageSize(),
				Page:      req.GetPage(),
			}
			if tc.respMod != nil {
				tc.respMod(wantResp)
			}
			c.Check(resp, cmpPB, wantResp)
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

		s := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil)
		_, err := s.CreateRepositoryTag(ctx, req)
		c.Check(err, qt.ErrorMatches, "invalid tag name")
	})

	c.Run("nok - invalid ID", func(c *qt.C) {
		t := newTag()
		t.Id = "latest"
		req := &artifactpb.CreateRepositoryTagRequest{Tag: t}

		s := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil)
		_, err := s.CreateRepositoryTag(ctx, req)
		c.Check(err, qt.ErrorMatches, "invalid tag name")
	})

	req := &artifactpb.CreateRepositoryTagRequest{Tag: newTag()}
	clearedTag, want := newTag(), newTag()
	clearedTag.UpdateTime = nil

	c.Run("nok - repo error", func(c *qt.C) {
		repository := mock.NewRepositoryIMock(c)
		repository.UpsertRepositoryTagMock.When(minimock.AnyContext, clearedTag).Then(nil, fmt.Errorf("foo"))

		s := NewService(repository, nil, nil, nil, nil, nil, nil, nil, nil)
		_, err := s.CreateRepositoryTag(ctx, req)
		c.Check(err, qt.ErrorMatches, "failed to upsert tag .*: foo")
	})

	c.Run("ok", func(c *qt.C) {
		repository := mock.NewRepositoryIMock(c)
		repository.UpsertRepositoryTagMock.When(minimock.AnyContext, clearedTag).Then(want, nil)

		s := NewService(repository, nil, nil, nil, nil, nil, nil, nil, nil)
		resp, err := s.CreateRepositoryTag(ctx, req)
		c.Check(err, qt.IsNil)
		c.Check(resp.GetTag(), cmpPB, want)
	})
}
