package service

import (
	"context"

	pb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// Service implements the Artifact domain use cases.
type Service interface {
	ListRepositoryTags(context.Context, *pb.ListRepositoryTagsRequest) (*pb.ListRepositoryTagsResponse, error)
}

type service struct {
}

// NewService initiates a service instance
func NewService() Service {
	return &service{}
}

// ListRepositoryTags fetches and paginates the tags of a repository in a
// remote distribution registry.
func (s *service) ListRepositoryTags(_ context.Context, _ *pb.ListRepositoryTagsRequest) (*pb.ListRepositoryTagsResponse, error) {
	return &pb.ListRepositoryTagsResponse{}, nil
}
