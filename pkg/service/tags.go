package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/gofrs/uuid"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/utils"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
)

func (s *service) DeleteRepositoryTag(ctx context.Context, req *artifactpb.DeleteRepositoryTagRequest) (*artifactpb.DeleteRepositoryTagResponse, error) {
	name := utils.RepositoryTagName(req.GetName())
	repo, id, err := name.ExtractRepositoryAndID()
	if err != nil {
		return nil, fmt.Errorf("invalid tag name")
	}

	rt, err := s.repository.GetRepositoryTag(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("failed to find existing tag %s: %w", id, err)
	}

	if err := s.registryClient.DeleteTag(ctx, repo, rt.Digest); err != nil {
		return nil, err
	}

	if err := s.repository.DeleteRepositoryTag(ctx, rt.Digest); err != nil {
		return nil, err
	}

	return &artifactpb.DeleteRepositoryTagResponse{}, nil
}

// CreateRepositoryTag stores the tag information of a pushed repository
// content.
func (s *service) CreateRepositoryTag(ctx context.Context, req *artifactpb.CreateRepositoryTagRequest) (*artifactpb.CreateRepositoryTagResponse, error) {
	name := utils.RepositoryTagName(req.GetTag().GetName())
	_, id, err := name.ExtractRepositoryAndID()
	if err != nil || id != req.GetTag().GetId() {
		return nil, fmt.Errorf("invalid tag name")
	}

	// Clear output-only values.
	tag := req.GetTag()
	tag.UpdateTime = nil

	storedTag, err := s.repository.UpsertRepositoryTag(ctx, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to upsert tag %s: %w", tag.GetId(), err)
	}

	return &artifactpb.CreateRepositoryTagResponse{Tag: storedTag}, nil
}

// GetRepositoryTag retrieve the information of a repository tag.
func (s *service) GetRepositoryTag(ctx context.Context, req *artifactpb.GetRepositoryTagRequest) (*artifactpb.GetRepositoryTagResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	name := utils.RepositoryTagName(req.GetName())
	repo, id, err := name.ExtractRepositoryAndID()
	if err != nil {
		return nil, fmt.Errorf("invalid tag name")
	}

	rt, err := s.repository.GetRepositoryTag(ctx, name)
	if err != nil {
		if !errors.Is(err, errorsx.ErrNotFound) {
			return nil, err
		}
		rt, err = s.populateMissingRepositoryTags(ctx, name, repo, id)
		if err != nil {
			logger.Warn(fmt.Sprintf("Create missing tag record error: %v", err))
			return nil, err
		}
	}

	return &artifactpb.GetRepositoryTagResponse{Tag: rt}, nil
}

// ListRepositoryTags fetches and paginates the tags of a repository in a
// remote distribution registry.
func (s *service) ListRepositoryTags(ctx context.Context, req *artifactpb.ListRepositoryTagsRequest) (*artifactpb.ListRepositoryTagsResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

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

	tags := make([]*artifactpb.RepositoryTag, 0, len(paginatedIDs))
	for _, id := range paginatedIDs {
		name := utils.NewRepositoryTagName(repo, id)
		rt, err := s.repository.GetRepositoryTag(ctx, name)
		if err != nil {
			if !errors.Is(err, errorsx.ErrNotFound) {
				return nil, fmt.Errorf("failed to fetch tag %s: %w", id, err)
			}

			// The source of truth for tags is the registry. The local
			// repository only holds extra information we'll aggregate to the
			// tag ID list. If no record is found locally, we create the missing
			// record.
			rt, err = s.populateMissingRepositoryTags(ctx, name, repo, id)
			if err != nil {
				logger.Warn(fmt.Sprintf("Create missing tag record error: %v", err))
				rt = &artifactpb.RepositoryTag{Name: string(name), Id: id}
			}
		}

		tags = append(tags, rt)
	}

	return &artifactpb.ListRepositoryTagsResponse{
		PageSize:  int32(pageSize),
		Page:      int32(page),
		TotalSize: int32(totalSize),
		Tags:      tags,
	}, nil
}

func (s *service) populateMissingRepositoryTags(ctx context.Context, name utils.RepositoryTagName, repo string, id string) (*artifactpb.RepositoryTag, error) {
	digest, err := s.registryClient.GetTagDigest(ctx, repo, id)
	if err != nil {
		return nil, err
	}
	rt := &artifactpb.RepositoryTag{Name: string(name), Id: id, Digest: digest}
	if _, err := s.CreateRepositoryTag(ctx, &artifactpb.CreateRepositoryTagRequest{
		Tag: &artifactpb.RepositoryTag{
			Name:   string(name),
			Id:     id,
			Digest: digest,
		},
	}); err != nil {
		return nil, err
	}

	return rt, nil
}

// UpdateCatalogFileTags updates the tags for a catalog file
func (s *service) UpdateCatalogFileTags(ctx context.Context, req *artifactpb.UpdateCatalogFileTagsRequest) (*artifactpb.UpdateCatalogFileTagsResponse, error) {
	// Validate input
	if req.GetNamespaceId() == "" {
		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: namespace ID is required", errorsx.ErrInvalidArgument),
			"Namespace ID is required.",
		)
	}
	if req.GetCatalogId() == "" {
		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: catalog ID is required", errorsx.ErrInvalidArgument),
			"Catalog ID is required.",
		)
	}
	if req.GetFileUid() == "" {
		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: file UID is required", errorsx.ErrInvalidArgument),
			"File UID is required.",
		)
	}

	// Get namespace and knowledge base
	ns, err := s.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		return nil, fmt.Errorf("fetching namespace: %w", err)
	}

	kb, err := s.repository.GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, req.CatalogId)
	if err != nil {
		return nil, fmt.Errorf("fetching knowledge base: %w", err)
	}

	// Check permissions
	granted, err := s.ACLClient().CheckPermission(ctx, "knowledgebase", kb.UID, "writer")
	if err != nil {
		return nil, fmt.Errorf("checking permissions: %w", err)
	}
	if !granted {
		return nil, errorsx.ErrUnauthenticated
	}

	// Get file
	fileUID := uuid.FromStringOrNil(req.GetFileUid())
	files, err := s.repository.GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{fileUID})
	if err != nil {
		return nil, fmt.Errorf("fetching file: %w", err)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("%w: file not found", errorsx.ErrNotFound)
	}

	file := files[0]
	if file.KnowledgeBaseUID != kb.UID {
		return nil, fmt.Errorf("file does not belong to the specified catalog")
	}

	// Sanitize tags
	tags := make([]string, 0, len(req.GetTags()))
	for _, tag := range req.GetTags() {
		if len(tag) == 0 {
			continue
		}
		tags = append(tags, tag)
	}

	// Check if collection supports tags before updating
	hasTags, err := s.vectorDB.CheckTagsMetadata(ctx, kb.UID)
	if err != nil {
		return nil, fmt.Errorf("checking tags metadata: %w", err)
	}
	if !hasTags {
		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: tags are not supported for this knowledge base", errorsx.ErrInvalidArgument),
			"Catalog doesn't support tags.",
		)
	}

	// Detect no-op operation
	if areTagsEqual(file.Tags, tags) {
		// Tags are the same, return current file without updating
		return &artifactpb.UpdateCatalogFileTagsResponse{
			File: file.ToPBFile(),
		}, nil
	}

	// Update tags in database
	if err := s.repository.UpdateKnowledgeBaseFileTags(ctx, fileUID, tags); err != nil {
		return nil, fmt.Errorf("updating tags in database: %w", err)
	}

	file.Tags = tags

	// Update embeddings with new tags
	if err := s.updateEmbeddingsWithFileTags(ctx, file); err != nil {
		return nil, fmt.Errorf("updating tags in vector database: %w", err)
	}

	// Get updated file
	updatedFiles, err := s.repository.GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{fileUID})
	if err != nil {
		return nil, fmt.Errorf("fetching updated file: %w", err)
	}
	if len(updatedFiles) == 0 {
		return nil, fmt.Errorf("updated file not found")
	}

	// Convert to protobuf
	return &artifactpb.UpdateCatalogFileTagsResponse{
		File: file.ToPBFile(),
	}, nil
}

// NOTE: after implementing Temporal activities, carry out vector tag updates
// asynchronously.
func (s *service) updateEmbeddingsWithFileTags(ctx context.Context, file repository.KnowledgeBaseFile) error {
	// Get existing embeddings for the file
	embeddings, err := s.repository.ListEmbeddingsByKbFileUID(ctx, file.UID)
	if err != nil {
		return fmt.Errorf("fetching embeddings: %w", err)
	}

	if len(embeddings) == 0 {
		// No embeddings to update
		return nil
	}

	// Convert repository embeddings to domain embeddings with updated tags
	vectors := make([]Embedding, len(embeddings))
	for i, emb := range embeddings {
		vectors[i] = Embedding{
			SourceTable:  emb.SourceTable,
			SourceUID:    emb.SourceUID.String(),
			EmbeddingUID: emb.UID.String(),
			Vector:       emb.Vector,
			FileUID:      emb.KbFileUID,
			FileName:     file.Name,
			FileType:     emb.FileType,
			ContentType:  emb.ContentType,
			Tags:         file.Tags,
		}
	}

	collection := KBCollectionName(file.KnowledgeBaseUID)
	if err := s.vectorDB.InsertVectorsInCollection(ctx, collection, vectors); err != nil {
		return fmt.Errorf("updating embeddings in vector database: %w", err)
	}

	return nil
}

// areTagsEqual compares two tag arrays for equality.
// It sorts both arrays alphabetically and then compares them element by element.
// This function handles the case where tags might be in different orders.
func areTagsEqual(tags1, tags2 []string) bool {
	// Quick length check for performance optimization
	if len(tags1) != len(tags2) {
		return false
	}

	// If both are empty, they're equal
	if len(tags1) == 0 {
		return true
	}

	// Create copies to avoid modifying the original slices
	sorted1 := make([]string, len(tags1))
	sorted2 := make([]string, len(tags2))
	copy(sorted1, tags1)
	copy(sorted2, tags2)

	// Sort both arrays alphabetically
	sort.Strings(sorted1)
	sort.Strings(sorted2)

	// Compare element by element
	for i := range sorted1 {
		if sorted1[i] != sorted2[i] {
			return false
		}
	}

	return true
}
