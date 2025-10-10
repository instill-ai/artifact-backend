package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/instill-ai/artifact-backend/pkg/types"
	"github.com/instill-ai/artifact-backend/pkg/utils"
	"google.golang.org/protobuf/types/known/timestamppb"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
)

// tagToProto converts a domain Tag to protobuf RepositoryTag
func tagToProto(tag *types.Tag) *artifactpb.RepositoryTag {
	if tag == nil {
		return nil
	}
	return &artifactpb.RepositoryTag{
		Name:       tag.Name,
		Id:         tag.ID,
		Digest:     tag.Digest,
		UpdateTime: timestamppb.New(tag.UpdateTime),
	}
}

// tagFromProto converts a protobuf RepositoryTag to domain Tag
func tagFromProto(pb *artifactpb.RepositoryTag) *types.Tag {
	if pb == nil {
		return nil
	}
	updateTime := pb.GetUpdateTime().AsTime()
	return &types.Tag{
		Name:       pb.GetName(),
		ID:         pb.GetId(),
		Digest:     pb.GetDigest(),
		UpdateTime: updateTime,
	}
}

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

	// Clear output-only values and convert to domain type
	tag := tagFromProto(req.GetTag())

	storedTag, err := s.repository.UpsertRepositoryTag(ctx, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to upsert tag %s: %w", tag.ID, err)
	}

	return &artifactpb.CreateRepositoryTagResponse{Tag: tagToProto(storedTag)}, nil
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
		rtProto, err := s.populateMissingRepositoryTags(ctx, name, repo, id)
		if err != nil {
			logger.Warn(fmt.Sprintf("Create missing tag record error: %v", err))
			return nil, err
		}
		return &artifactpb.GetRepositoryTagResponse{Tag: rtProto}, nil
	}

	return &artifactpb.GetRepositoryTagResponse{Tag: tagToProto(rt)}, nil
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
			rtProto, err := s.populateMissingRepositoryTags(ctx, name, repo, id)
			if err != nil {
				logger.Warn(fmt.Sprintf("Create missing tag record error: %v", err))
				rtProto = &artifactpb.RepositoryTag{Name: string(name), Id: id}
			}
			tags = append(tags, rtProto)
		} else {
			tags = append(tags, tagToProto(rt))
		}
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

// areTagsEqual compares two tag slices for equality, ignoring order.
// Returns true if both slices contain the same tags with the same counts,
// treating nil and empty slices as equal.
func areTagsEqual(tags1, tags2 []string) bool {
	// Quick length check for performance optimization
	if len(tags1) != len(tags2) {
		return false
	}

	// If both are empty, they're equal
	if len(tags1) == 0 {
		return true
	}

	// Create copies to avoid modifying the original slices
	sorted1 := make([]string, len(tags1))
	sorted2 := make([]string, len(tags2))
	copy(sorted1, tags1)
	copy(sorted2, tags2)

	// Sort both arrays alphabetically
	sort.Strings(sorted1)
	sort.Strings(sorted2)

	// Compare element by element
	for i := range sorted1 {
		if sorted1[i] != sorted2[i] {
			return false
		}
	}

	return true
}
