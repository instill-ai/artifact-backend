package service

import (
	"context"

	pb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// Repository implements the storage functions for Artifact.
type Repository interface {
	GetRepositoryTag(context.Context, RepositoryTagName) (*pb.RepositoryTag, error)
	UpsertRepositoryTag(context.Context, *pb.RepositoryTag) (*pb.RepositoryTag, error)
}
