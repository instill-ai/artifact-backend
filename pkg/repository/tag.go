package repository

import (
	"context"

	customerror "github.com/instill-ai/artifact-backend/pkg/customerror"
	"github.com/instill-ai/artifact-backend/pkg/utils"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)


// TagI
// 	GetRepositoryTag(context.Context, utils.RepositoryTagName) (*artifactpb.RepositoryTag, error)
// UpsertRepositoryTag(context.Context, *artifactpb.RepositoryTag) (*artifactpb.RepositoryTag, error)
type TagI interface {
	GetRepositoryTag(context.Context, utils.RepositoryTagName) (*artifactpb.RepositoryTag, error)
	UpsertRepositoryTag(context.Context, *artifactpb.RepositoryTag) (*artifactpb.RepositoryTag, error)
}

// GetRepositoryTag fetches the tag information from the repository_tag table.
// The name param is the resource name of the tag, e.g.
// `repositories/admin/hello-world/tags/0.1.1-beta`.
func (r *Repository) GetRepositoryTag(_ context.Context, name utils.RepositoryTagName) (*artifactpb.RepositoryTag, error) {
	repo, tagID, err := name.ExtractRepositoryAndID()
	if err != nil {
		return nil, err
	}

	record := new(repositoryTag)
	if result := r.db.Model(record).
		Where("name = ?", repositoryTagName(repo, tagID)).
		First(record); result.Error != nil {

		if result.Error == gorm.ErrRecordNotFound {
			return nil, customerror.ErrNotFound
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

	return &artifactpb.RepositoryTag{
		Name:       tag.GetName(),
		Id:         tag.GetId(),
		Digest:     record.Digest,
		UpdateTime: timestamppb.New(record.UpdateTime),
	}, nil
}
