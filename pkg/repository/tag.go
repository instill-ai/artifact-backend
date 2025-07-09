package repository

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/instill-ai/artifact-backend/pkg/errors"
	"github.com/instill-ai/artifact-backend/pkg/utils"

	pb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// TagI
//
//	GetRepositoryTag(context.Context, utils.RepositoryTagName) (*artifactpb.RepositoryTag, error)
//
// UpsertRepositoryTag(context.Context, *artifactpb.RepositoryTag) (*artifactpb.RepositoryTag, error)
type TagI interface {
	GetRepositoryTag(context.Context, utils.RepositoryTagName) (*pb.RepositoryTag, error)
	UpsertRepositoryTag(context.Context, *pb.RepositoryTag) (*pb.RepositoryTag, error)
	DeleteRepositoryTag(context.Context, string) error
}

// GetRepositoryTag fetches the tag information from the repository_tag table.
// The name param is the resource name of the tag, e.g.
// `repositories/admin/hello-world/tags/0.1.1-beta`.
func (r *Repository) GetRepositoryTag(_ context.Context, name utils.RepositoryTagName) (*pb.RepositoryTag, error) {
	repo, tagID, err := name.ExtractRepositoryAndID()
	if err != nil {
		return nil, err
	}

	record := new(repositoryTag)
	if result := r.db.Model(record).
		Where("name = ?", repositoryTagName(repo, tagID)).
		First(record); result.Error != nil {

		if result.Error == gorm.ErrRecordNotFound {
			return nil, errors.ErrNotFound
		}

		return nil, result.Error
	}

	return &pb.RepositoryTag{
		Name:       string(name),
		Id:         tagID,
		Digest:     record.Digest,
		UpdateTime: timestamppb.New(record.UpdateTime),
	}, nil
}

// UpsertRepositoryTag stores the provided tag information in the database. The
// update timestamp will be generated on insertion.
func (r *Repository) UpsertRepositoryTag(_ context.Context, tag *pb.RepositoryTag) (*pb.RepositoryTag, error) {
	repo, tagID, err := utils.RepositoryTagName(tag.GetName()).ExtractRepositoryAndID()
	if err != nil {
		return nil, err
	}

	record := &repositoryTag{
		Name:   repositoryTagName(repo, tagID),
		Digest: tag.GetDigest(),
	}

	updateOnConflict := clause.OnConflict{
		Columns:   []clause.Column{{Name: "name"}},
		DoUpdates: clause.AssignmentColumns([]string{"digest"}),
	}
	if result := r.db.Clauses(updateOnConflict).Create(record); result.Error != nil {
		return nil, result.Error
	}

	return &pb.RepositoryTag{
		Name:       tag.GetName(),
		Id:         tag.GetId(),
		Digest:     record.Digest,
		UpdateTime: timestamppb.New(record.UpdateTime),
	}, nil
}

// DeleteRepositoryTag delete the tag information from the repository_tag table.
// The name param is the resource name of the tag, e.g.
// `repositories/admin/hello-world/tags/0.1.1-beta`.
func (r *Repository) DeleteRepositoryTag(_ context.Context, digest string) error {
	record := new(repositoryTag)
	if result := r.db.Model(record).
		Where("digest = ?", digest).
		Delete(record); result.Error != nil {

		if result.Error == gorm.ErrRecordNotFound {
			return errors.ErrNotFound
		}

		return result.Error
	}

	return nil
}
