package repository

import (
	"context"
	"fmt"
	"regexp"

	"gorm.io/gorm"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Repository implements Artifact storage functions in PostgreSQL.
type Repository struct {
	db *gorm.DB
}

// NewRepository returns an initialized repository.
func NewRepository(db *gorm.DB) *Repository {
	return &Repository{
		db: db,
	}
}

// tagNameRegexp captures the repository name (owner and ID) and tag ID from a
// tag resource name.
var tagNameRegexp = regexp.MustCompile(`repositories/(([^/]+)/([^/]+))/tags/([^/]+)`)

// GetRepositoryTag fetches the tag information from the repository_tag table.
// The name param is the resource name of the tag, e.g.
// `repositories/admin/hello-world/tags/0.1.1-beta`.
func (r *Repository) GetRepositoryTag(_ context.Context, name string) (*artifactpb.RepositoryTag, error) {
	matches := tagNameRegexp.FindStringSubmatch(name)
	if len(matches) == 0 {
		return nil, fmt.Errorf("invalid tag name")
	}

	repo := matches[1]
	tagID := matches[4]
	dbName := fmt.Sprintf("%s:%s", repo, tagID)

	// In the database, the tag name is the primary key. It is compacted to
	// <repository>:tag to improve the efficiency of the queries.
	return &artifactpb.RepositoryTag{
		Name:       name,
		Id:         tagID,
		Digest:     dbName,
		UpdateTime: timestamppb.Now(),
	}, nil
}
