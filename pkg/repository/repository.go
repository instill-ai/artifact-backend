package repository

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	"google.golang.org/protobuf/types/known/timestamppb"

	artifact "github.com/instill-ai/artifact-backend/pkg/service"
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

// GetRepositoryTag fetches the tag information from the repository_tag table.
// The name param is the resource name of the tag, e.g.
// `repositories/admin/hello-world/tags/0.1.1-beta`.
func (r *Repository) GetRepositoryTag(_ context.Context, name artifact.RepositoryTagName) (*artifactpb.RepositoryTag, error) {
	repo, tagID, err := name.ExtractRepositoryAndID()
	if err != nil {
		return nil, err
	}

	// In the database, the tag name is the primary key. It is compacted to
	// <repository>:tag to improve the efficiency of the queries.
	dbName := fmt.Sprintf("%s:%s", repo, tagID)

	record := new(repositoryTag)
	if result := r.db.Model(record).Where("name = ?", dbName).First(record); result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			return nil, artifact.ErrNotFound
		}

		return nil, result.Error
	}

	return &artifactpb.RepositoryTag{
		Name:       string(name),
		Id:         tagID,
		Digest:     record.Digest,
		UpdateTime: timestamppb.New(record.UpdateTime),
	}, nil
}

// UpsertRepositoryTag stores the provided tag information in the database. The
// update timestamp will be generated on insertion.
func (r *Repository) UpsertRepositoryTag(_ context.Context, tag *artifactpb.RepositoryTag) (*artifactpb.RepositoryTag, error) {
	return &artifactpb.RepositoryTag{
		Name:       tag.GetName(),
		Id:         tag.GetId(),
		Digest:     tag.GetDigest(),
		UpdateTime: timestamppb.Now(),
	}, nil
}
