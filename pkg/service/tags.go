package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/instill-ai/artifact-backend/pkg/utils"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
)

func (s *service) DeleteRepositoryTag(ctx context.Context, req *artifactpb.DeleteRepositoryTagRequest) (*artifactpb.DeleteRepositoryTagResponse, error) {
	name := utils.RepositoryTagName(req.GetName())
	repo, id, err := name.ExtractRepositoryAndID()
	if err != nil {
		return nil, fmt.Errorf("invalid tag name")
	}

	rt, err := s.repository.GetRepositoryTag(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("failed to find existing tag %s: %w", id, err)
	}

	if err := s.registryClient.DeleteTag(ctx, repo, rt.Digest); err != nil {
		return nil, err
	}

	if err := s.repository.DeleteRepositoryTag(ctx, rt.Digest); err != nil {
		return nil, err
	}

	return &artifactpb.DeleteRepositoryTagResponse{}, nil
}

// CreateRepositoryTag stores the tag information of a pushed repository
// content.
func (s *service) CreateRepositoryTag(ctx context.Context, req *artifactpb.CreateRepositoryTagRequest) (*artifactpb.CreateRepositoryTagResponse, error) {
	name := utils.RepositoryTagName(req.GetTag().GetName())
	_, id, err := name.ExtractRepositoryAndID()
	if err != nil || id != req.GetTag().GetId() {
		return nil, fmt.Errorf("invalid tag name")
	}

	// Clear output-only values.
	tag := req.GetTag()
	tag.UpdateTime = nil

	storedTag, err := s.repository.UpsertRepositoryTag(ctx, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to upsert tag %s: %w", tag.GetId(), err)
	}

	return &artifactpb.CreateRepositoryTagResponse{Tag: storedTag}, nil
}

// GetRepositoryTag retrieve the information of a repository tag.
func (s *service) GetRepositoryTag(ctx context.Context, req *artifactpb.GetRepositoryTagRequest) (*artifactpb.GetRepositoryTagResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	name := utils.RepositoryTagName(req.GetName())
	repo, id, err := name.ExtractRepositoryAndID()
	if err != nil {
		return nil, fmt.Errorf("invalid tag name")
	}

	rt, err := s.repository.GetRepositoryTag(ctx, name)
	if err != nil {
		if !errors.Is(err, errorsx.ErrNotFound) {
			return nil, err
		}
		rt, err = s.populateMissingRepositoryTags(ctx, name, repo, id)
		if err != nil {
			logger.Warn(fmt.Sprintf("Create missing tag record error: %v", err))
			return nil, err
		}
	}

	return &artifactpb.GetRepositoryTagResponse{Tag: rt}, nil
}

// ListRepositoryTags fetches and paginates the tags of a repository in a
// remote distribution registry.
func (s *service) ListRepositoryTags(ctx context.Context, req *artifactpb.ListRepositoryTagsRequest) (*artifactpb.ListRepositoryTagsResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	pageSize := pageSizeInRange(req.GetPageSize())
	page := pageInRange(req.GetPage())
	idx0, idx1 := page*pageSize, (page+1)*pageSize

	// Content registry repository, not to be mixed with s.repository (artifact
	// storage implementation).
	_, repo, ok := strings.Cut(req.GetParent(), "repositories/")
	if !ok {
		return nil, fmt.Errorf("namespace error")
	}

	tagIDs, err := s.registryClient.ListTags(ctx, repo)
	if err != nil {
		return nil, err
	}

	totalSize := len(tagIDs)
	var paginatedIDs []string
	switch {
	case idx0 >= totalSize:
	case idx1 > totalSize:
		paginatedIDs = tagIDs[idx0:]
	default:
		paginatedIDs = tagIDs[idx0:idx1]
	}

	tags := make([]*artifactpb.RepositoryTag, 0, len(paginatedIDs))
	for _, id := range paginatedIDs {
		name := utils.NewRepositoryTagName(repo, id)
		rt, err := s.repository.GetRepositoryTag(ctx, name)
		if err != nil {
			if !errors.Is(err, errorsx.ErrNotFound) {
				return nil, fmt.Errorf("failed to fetch tag %s: %w", id, err)
			}

			// The source of truth for tags is the registry. The local
			// repository only holds extra information we'll aggregate to the
			// tag ID list. If no record is found locally, we create the missing
			// record.
			rt, err = s.populateMissingRepositoryTags(ctx, name, repo, id)
			if err != nil {
				logger.Warn(fmt.Sprintf("Create missing tag record error: %v", err))
				rt = &artifactpb.RepositoryTag{Name: string(name), Id: id}
			}
		}

		tags = append(tags, rt)
	}

	return &artifactpb.ListRepositoryTagsResponse{
		PageSize:  int32(pageSize),
		Page:      int32(page),
		TotalSize: int32(totalSize),
		Tags:      tags,
	}, nil
}

func (s *service) populateMissingRepositoryTags(ctx context.Context, name utils.RepositoryTagName, repo string, id string) (*artifactpb.RepositoryTag, error) {
	digest, err := s.registryClient.GetTagDigest(ctx, repo, id)
	if err != nil {
		return nil, err
	}
	rt := &artifactpb.RepositoryTag{Name: string(name), Id: id, Digest: digest}
	if _, err := s.CreateRepositoryTag(ctx, &artifactpb.CreateRepositoryTagRequest{
		Tag: &artifactpb.RepositoryTag{
			Name:   string(name),
			Id:     id,
			Digest: digest,
		},
	}); err != nil {
		return nil, err
	}

	return rt, nil
}
