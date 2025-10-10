package worker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/config"
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

// saveChunksToDBOnly saves text chunks to database with placeholder destinations,
// then uploads to MinIO, and finally updates the destinations in the database.
// This ensures MinIO uploads happen AFTER the DB transaction commits, preventing
// orphaned files if the transaction rolls back.
// Returns a map of textChunkUID -> text chunk content that was saved to MinIO.
func saveChunksToDBOnly(
	ctx context.Context,
	repo repository.Repository,
	minioClient repository.ObjectStorage,
	kbUID types.KBUIDType, kbFileUID types.FileUIDType, sourceUID types.SourceUIDType,
	sourceTable string,
	summaryChunks, contentChunks []types.TextChunk,
	fileType string,
) (map[string][]byte, error) {
	textChunks := make([]*repository.TextChunkModel, len(summaryChunks)+len(contentChunks))
	texts := make([]string, len(summaryChunks)+len(contentChunks))

	for i, c := range summaryChunks {
		textChunks[i] = &repository.TextChunkModel{
			SourceUID:   sourceUID,
			SourceTable: sourceTable,
			StartPos:    0,
			EndPos:      0,
			ContentDest: "pending", // Placeholder, will be updated after MinIO save
			Tokens:      c.Tokens,
			Retrievable: true,
			InOrder:     i,
			KBUID:       kbUID,
			KBFileUID:   kbFileUID,
			FileType:    fileType,
			ContentType: string(types.SummaryContentType),
		}
		texts[i] = c.Text
	}
	for i, c := range contentChunks {
		ii := i + len(summaryChunks)
		textChunks[ii] = &repository.TextChunkModel{
			SourceUID:   sourceUID,
			SourceTable: sourceTable,
			StartPos:    c.Start,
			EndPos:      c.End,
			Reference:   c.Reference,
			ContentDest: "pending", // Placeholder, will be updated after MinIO save
			Tokens:      c.Tokens,
			Retrievable: true,
			InOrder:     ii,
			KBUID:       kbUID,
			KBFileUID:   kbFileUID,
			FileType:    fileType,
			ContentType: string(types.ChunkContentType),
		}

		texts[ii] = c.Text
	}

	// Step 1: Save text chunks to database with placeholder destinations
	// Pass nil callback to avoid MinIO operations within the transaction
	createdChunks, err := repo.DeleteAndCreateTextChunks(ctx, kbFileUID, textChunks, nil)
	if err != nil {
		return nil, errorsx.AddMessage(err, "Unable to save text chunk records. Please try again.")
	}

	// Step 2: Upload text chunks to MinIO after DB transaction commits
	destinations := make(map[string]string, len(createdChunks))
	for i, chunk := range createdChunks {
		chunkUID := chunk.UID.String()

		// Construct the MinIO path using the format: kb-{kbUID}/file-{fileUID}/chunk/{chunkUID}.md
		basePath := fmt.Sprintf("kb-%s/file-%s/chunk", kbUID.String(), kbFileUID.String())
		path := fmt.Sprintf("%s/%s.md", basePath, chunkUID)

		// Encode text chunk content to base64
		base64Content := base64.StdEncoding.EncodeToString([]byte(texts[i]))

		// Save text chunk content to MinIO
		err := minioClient.UploadBase64File(
			ctx,
			config.Config.Minio.BucketName,
			path,
			base64Content,
			"text/markdown",
		)
		if err != nil {
			return nil, errorsx.AddMessage(err, fmt.Sprintf("Unable to save text chunk (ID: %s) to storage. Please try again.", chunkUID))
		}

		destinations[chunkUID] = path
	}

	// Step 3: Update text chunk destinations in database
	err = repo.UpdateTextChunkDestinations(ctx, destinations)
	if err != nil {
		return nil, errorsx.AddMessage(err, "Unable to update text chunk references. Please try again.")
	}

	// Build map of textChunkUID -> content for return value
	chunksToSave := make(map[string][]byte, len(createdChunks))
	for i, chunk := range createdChunks {
		chunksToSave[chunk.UID.String()] = []byte(texts[i])
	}

	return chunksToSave, nil
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
