package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/instill-ai/artifact-backend/pkg/acl"
	"github.com/instill-ai/artifact-backend/pkg/customerror"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/milvus"
	"github.com/instill-ai/artifact-backend/pkg/minio"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/utils"
	pb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	mgmtPB "github.com/instill-ai/protogen-go/core/mgmt/v1beta"
	pipelinev1beta "github.com/instill-ai/protogen-go/vdp/pipeline/v1beta"
	"github.com/redis/go-redis/v9"
)

const (
	defaultPageSize = 10
	maxPageSize     = 100
)

// Service implements the Artifact domain use cases.
type Service struct {
	Repository     repository.RepositoryI
	MinIO          minio.MinioI
	MgmtPrv        mgmtPB.MgmtPrivateServiceClient
	PipelinePub    pipelinev1beta.PipelinePublicServiceClient
	registryClient RegistryClient
	// refactor: we need redis interface in the future to mock the redis client
	RedisClient  *redis.Client
	MilvusClient milvus.MilvusClientI
	ACLClient    acl.ACLClient
}

// NewService initiates a service instance
func NewService(
	r repository.RepositoryI,
	mc minio.MinioI,
	mgmtPrv mgmtPB.MgmtPrivateServiceClient,
	pipelinePub pipelinev1beta.PipelinePublicServiceClient,
	rgc RegistryClient,
	rc *redis.Client,
	milvusClient milvus.MilvusClientI,
	aclClient acl.ACLClient,

) *Service {

	return &Service{
		Repository:     r,
		MinIO:          mc,
		MgmtPrv:        mgmtPrv,
		PipelinePub:    pipelinePub,
		registryClient: rgc,
		RedisClient:    rc,
		MilvusClient:   milvusClient,
		ACLClient:      aclClient,
	}
}

func (s *Service) populateMissingRepositoryTags(ctx context.Context, name utils.RepositoryTagName, repo string, id string) (*pb.RepositoryTag, error) {
	digest, err := s.registryClient.GetTagDigest(ctx, repo, id)
	if err != nil {
		return nil, err
	}
	rt := &pb.RepositoryTag{Name: string(name), Id: id, Digest: digest}
	if _, err := s.CreateRepositoryTag(ctx, &pb.CreateRepositoryTagRequest{
		Tag: &pb.RepositoryTag{
			Name:   string(name),
			Id:     id,
			Digest: digest,
		},
	}); err != nil {
		return nil, err
	}

	return rt, nil
}

// ListRepositoryTags fetches and paginates the tags of a repository in a
// remote distribution registry.
func (s *Service) ListRepositoryTags(ctx context.Context, req *pb.ListRepositoryTagsRequest) (*pb.ListRepositoryTagsResponse, error) {

	logger, _ := logger.GetZapLogger(ctx)

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

	totalSize := len(tagIDs)
	var paginatedIDs []string
	switch {
	case idx0 >= totalSize:
	case idx1 > totalSize:
		paginatedIDs = tagIDs[idx0:]
	default:
		paginatedIDs = tagIDs[idx0:idx1]
	}

	tags := make([]*pb.RepositoryTag, 0, len(paginatedIDs))
	for _, id := range paginatedIDs {
		name := utils.NewRepositoryTagName(repo, id)
		rt, err := s.Repository.GetRepositoryTag(ctx, name)
		if err != nil {
			if !errors.Is(err, customerror.ErrNotFound) {
				return nil, fmt.Errorf("failed to fetch tag %s: %w", id, err)
			}

			// The source of truth for tags is the registry. The local
			// repository only holds extra information we'll aggregate to the
			// tag ID list. If no record is found locally, we create the missing
			// record.
			rt, err = s.populateMissingRepositoryTags(ctx, name, repo, id)
			if err != nil {
				logger.Warn(fmt.Sprintf("Create missing tag record error: %v", err))
				rt = &pb.RepositoryTag{Name: string(name), Id: id}
			}
		}

		tags = append(tags, rt)
	}

	return &pb.ListRepositoryTagsResponse{
		PageSize:  int32(pageSize),
		Page:      int32(page),
		TotalSize: int32(totalSize),
		Tags:      tags,
	}, nil
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
	name := utils.RepositoryTagName(req.GetTag().GetName())
	_, id, err := name.ExtractRepositoryAndID()
	if err != nil || id != req.GetTag().GetId() {
		return nil, fmt.Errorf("invalid tag name")
	}

	// Clear output-only values.
	tag := req.GetTag()
	tag.UpdateTime = nil

	storedTag, err := s.Repository.UpsertRepositoryTag(ctx, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to upsert tag %s: %w", tag.GetId(), err)
	}

	return &pb.CreateRepositoryTagResponse{Tag: storedTag}, nil
}

// GetRepositoryTag retrieve the information of a repository tag.
func (s *Service) GetRepositoryTag(ctx context.Context, req *pb.GetRepositoryTagRequest) (*pb.GetRepositoryTagResponse, error) {
	logger, _ := logger.GetZapLogger(ctx)

	name := utils.RepositoryTagName(req.GetName())
	repo, id, err := name.ExtractRepositoryAndID()
	if err != nil {
		return nil, fmt.Errorf("invalid tag name")
	}

	rt, err := s.Repository.GetRepositoryTag(ctx, name)
	if err != nil {
		if !errors.Is(err, customerror.ErrNotFound) {
			return nil, err
		}
		rt, err = s.populateMissingRepositoryTags(ctx, name, repo, id)
		if err != nil {
			logger.Warn(fmt.Sprintf("Create missing tag record error: %v", err))
			return nil, err
		}
	}

	return &pb.GetRepositoryTagResponse{Tag: rt}, nil
}

func (s *Service) DeleteRepositoryTag(ctx context.Context, req *pb.DeleteRepositoryTagRequest) (*pb.DeleteRepositoryTagResponse, error) {
	name := utils.RepositoryTagName(req.GetName())
	repo, id, err := name.ExtractRepositoryAndID()
	if err != nil {
		return nil, fmt.Errorf("invalid tag name")
	}

	rt, err := s.Repository.GetRepositoryTag(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("failed to find existing tag %s: %w", id, err)
	}

	if err := s.registryClient.DeleteTag(ctx, repo, rt.Digest); err != nil {
		return nil, err
	}

	if err := s.Repository.DeleteRepositoryTag(ctx, rt.Digest); err != nil {
		return nil, err
	}

	return &pb.DeleteRepositoryTagResponse{}, nil
}
