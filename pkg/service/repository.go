package service

import (
	"context"

	pb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// Repository implements the storage functions for Artifact.
type Repository interface {
	GetRepositoryTag(_ context.Context, name string) (*pb.RepositoryTag, error)
}
