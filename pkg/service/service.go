package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	pb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

const (
	defaultPageSize = 10
	maxPageSize     = 100
)

// Service implements the Artifact domain use cases.
type Service struct {
	repository     Repository
	registryClient RegistryClient
}

// NewService initiates a service instance
func NewService(
	r Repository,
	rc RegistryClient,
) *Service {

	return &Service{
		repository:     r,
		registryClient: rc,
	}
}

// ListRepositoryTags fetches and paginates the tags of a repository in a
// remote distribution registry.
func (s *Service) ListRepositoryTags(ctx context.Context, req *pb.ListRepositoryTagsRequest) (*pb.ListRepositoryTagsResponse, error) {
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

	var paginatedIDs []string
	switch {
	case idx0 >= len(tagIDs):
	case idx1 > len(tagIDs):
		paginatedIDs = tagIDs[idx0:]
	default:
		paginatedIDs = tagIDs[idx0:idx1]
	}

	tags := make([]*pb.RepositoryTag, 0, len(paginatedIDs))
	for _, id := range paginatedIDs {
		name := NewRepositoryTagName(repo, id)
		rt, err := s.repository.GetRepositoryTag(ctx, name)
		if err != nil {
			if !errors.Is(err, ErrNotFound) {
				return nil, fmt.Errorf("failed to fetch tag %s: %w", id, err)
			}

			// The source of truth for tags is the registry. The local
			// repository only holds extra information we'll aggregate to the
			// tag ID list. If no record is found locally, the tag object will
			// be returned with empty optional fields.
			rt = &pb.RepositoryTag{Name: string(name), Id: id}
		}

		tags = append(tags, rt)
	}

	return &pb.ListRepositoryTagsResponse{Tags: tags}, nil
}

func pageSizeInRange(pageSize int32) int {
	if pageSize <= 0 {
		return defaultPageSize
	}

	if pageSize > maxPageSize {
		return maxPageSize
	}

	return int(pageSize)
}

func pageInRange(page int32) int {
	if page <= 0 {
		return 0
	}

	return int(page)
}

// CreateRepositoryTag stores the tag information of a pushed repository
// content.
func (s *Service) CreateRepositoryTag(ctx context.Context, req *pb.CreateRepositoryTagRequest) (*pb.CreateRepositoryTagResponse, error) {
	name := RepositoryTagName(req.GetTag().GetName())
	if _, _, err := name.ExtractRepositoryAndID(); err != nil {
		return nil, fmt.Errorf("invalid tag name")
	}

	// Clear output-only values.
	tag := req.GetTag()
	tag.UpdateTime = nil

	storedTag, err := s.repository.UpsertRepositoryTag(ctx, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to upsert tag %s: %w", tag.GetId(), err)
	}

	return &pb.CreateRepositoryTagResponse{Tag: storedTag}, nil
}
