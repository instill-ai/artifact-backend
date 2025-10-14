package worker

import (
	"context"
	"encoding/json"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	errorsx "github.com/instill-ai/x/errors"
)

// getFileByUID is a helper function to retrieve a single file by UID.
// It returns the file or an error if not found.
func getFileByUID(ctx context.Context, repo repository.Repository, fileUID types.FileUIDType) (repository.KnowledgeBaseFileModel, error) {
	files, err := repo.GetKnowledgeBaseFilesByFileUIDs(ctx, []types.FileUIDType{fileUID})
	if err != nil {
		return repository.KnowledgeBaseFileModel{}, errorsx.AddMessage(err, "Unable to retrieve file information. Please try again.")
	}
	if len(files) == 0 {
		err := errorsx.AddMessage(errorsx.ErrNotFound, "File not found. It may have been deleted.")
		return repository.KnowledgeBaseFileModel{}, err
	}
	return files[0], nil
}

// extractRequestMetadata extracts the gRPC metadata from a file's ExternalMetadataE
// and returns it as metadata.MD that can be used to create an authenticated context.
func extractRequestMetadata(externalMetadata *structpb.Struct) (metadata.MD, error) {
	md := metadata.MD{}
	if externalMetadata == nil {
		return md, nil
	}

	if externalMetadata.Fields[constant.MetadataRequestKey] == nil {
		return md, nil
	}

	// In order to simplify the code translating metadata.MD <->
	// structpb.Struct, JSON marshalling is used. This is less efficient than
	// leveraging the knowledge about the metadata structure (a
	// map[string][]string), but readability has been prioritized.
	j, err := externalMetadata.Fields[constant.MetadataRequestKey].GetStructValue().MarshalJSON()
	if err != nil {
		return nil, errorsx.AddMessage(err, "Unable to process authentication metadata. Please try again.")
	}

	if err := json.Unmarshal(j, &md); err != nil {
		return nil, errorsx.AddMessage(err, "Unable to process authentication metadata. Please try again.")
	}

	return md, nil
}

// CreateAuthenticatedContext creates a context with the authentication metadata
// from the file's ExternalMetadata. This allows activities to make authenticated
// calls to other services (like pipeline-backend).
// CreateAuthenticatedContext creates an authenticated context from external metadata
func CreateAuthenticatedContext(ctx context.Context, externalMetadata *structpb.Struct) (context.Context, error) {
	md, err := extractRequestMetadata(externalMetadata)
	if err != nil {
		return ctx, errorsx.AddMessage(err, "Authentication failed. Please try again.")
	}

	if len(md) == 0 {
		return ctx, nil
	}

	return metadata.NewOutgoingContext(ctx, md), nil
}
