package service

import (
	"context"
	"fmt"
	"strings"

	pb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// Service implements the Artifact domain use cases.
type Service interface {
	ListRepositoryTags(context.Context, *pb.ListRepositoryTagsRequest) (*pb.ListRepositoryTagsResponse, error)
}

// RegistryClient interacts with a distribution registry to manage
// repositories.
type RegistryClient interface {
	ListTags(_ context.Context, repository string) (tags []string, _ error)
}

type service struct {
	registryClient RegistryClient
}

// NewService initiates a service instance
func NewService(registryClient RegistryClient) Service {
	return &service{
		registryClient: registryClient,
	}
}

// ListRepositoryTags fetches and paginates the tags of a repository in a
// remote distribution registry.
func (s *service) ListRepositoryTags(ctx context.Context, req *pb.ListRepositoryTagsRequest) (*pb.ListRepositoryTagsResponse, error) {
	_, repository, ok := strings.Cut(req.GetParent(), "repositories/")
	if !ok {
		return nil, fmt.Errorf("namespace error")
	}

	tagIDs, err := s.registryClient.ListTags(ctx, repository)
	if err != nil {
		return nil, err
	}

	tags := make([]*pb.RepositoryTag, 0, len(tagIDs))
	for _, tagID := range tagIDs {
		tags = append(tags, &pb.RepositoryTag{
			Id:   tagID,
			Name: tagName(repository, tagID),
		})
	}

	return &pb.ListRepositoryTagsResponse{Tags: tags}, nil
}

func tagName(repo, id string) string {
	return fmt.Sprintf("repositories/%s/tags/%s", repo, id)
}
