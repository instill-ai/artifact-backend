package service

import (
	"context"
	"fmt"
	"strings"

	pb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

const (
	defaultPageSize = 10
	maxPageSize     = 100
)

// Service implements the Artifact domain use cases.
type Service interface {
	ListRepositoryTags(context.Context, *pb.ListRepositoryTagsRequest) (*pb.ListRepositoryTagsResponse, error)
}

type service struct {
	repository     Repository
	registryClient RegistryClient
}

// NewService initiates a service instance
func NewService(
	r Repository,
	rc RegistryClient,
) Service {

	return &service{
		repository:     r,
		registryClient: rc,
	}
}

// ListRepositoryTags fetches and paginates the tags of a repository in a
// remote distribution registry.
func (s *service) ListRepositoryTags(ctx context.Context, req *pb.ListRepositoryTagsRequest) (*pb.ListRepositoryTagsResponse, error) {
	pageSize := s.pageSizeInRange(req.GetPageSize())
	page := s.pageInRange(req.GetPage())
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
	for _, tagID := range paginatedIDs {
		rt, err := s.repository.GetRepositoryTag(ctx, tagName(repo, tagID))
		if err != nil {
			return nil, fmt.Errorf("failed to fetch tag %s: %w", tagID, err)
		}

		tags = append(tags, rt)
	}

	return &pb.ListRepositoryTagsResponse{Tags: tags}, nil
}

func tagName(repo, id string) string {
	return fmt.Sprintf("repositories/%s/tags/%s", repo, id)
}

func (s *service) pageSizeInRange(pageSize int32) int {
	if pageSize <= 0 {
		return defaultPageSize
	}

	if pageSize > maxPageSize {
		return maxPageSize
	}

	return int(pageSize)
}

func (s *service) pageInRange(page int32) int {
	if page <= 0 {
		return 0
	}

	return int(page)
}
