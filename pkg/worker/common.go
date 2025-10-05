package worker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/gofrs/uuid"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/minio"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"

	errorsx "github.com/instill-ai/x/errors"
)

// getFileByUID is a helper function to retrieve a single file by UID.
// It returns the file or an error if not found.
func getFileByUID(ctx context.Context, repo repository.RepositoryI, fileUID uuid.UUID) (repository.KnowledgeBaseFile, error) {
	files, err := repo.GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{fileUID})
	if err != nil {
		return repository.KnowledgeBaseFile{}, fmt.Errorf("failed to get file: %s", errorsx.MessageOrErr(err))
	}
	if len(files) == 0 {
		return repository.KnowledgeBaseFile{}, fmt.Errorf("file not found: %s", fileUID.String())
	}
	return files[0], nil
}

// saveChunksToDBOnly saves chunks to database with placeholder destinations.
// Returns a map of chunkUID -> chunk content that needs to be saved to MinIO.
func saveChunksToDBOnly(
	ctx context.Context,
	repo repository.RepositoryI,
	minioClient minio.MinioI,
	kbUID, kbFileUID, sourceUID uuid.UUID,
	sourceTable string,
	summaryChunks, contentChunks []service.Chunk,
	fileType string,
) (map[string][]byte, error) {
	textChunks := make([]*repository.TextChunk, len(summaryChunks)+len(contentChunks))
	texts := make([]string, len(summaryChunks)+len(contentChunks))

	for i, c := range summaryChunks {
		textChunks[i] = &repository.TextChunk{
			SourceUID:   sourceUID,
			SourceTable: sourceTable,
			StartPos:    0,
			EndPos:      0,
			ContentDest: "pending", // Placeholder, will be updated after MinIO save
			Tokens:      c.Tokens,
			Retrievable: true,
			InOrder:     i,
			KbUID:       kbUID,
			KbFileUID:   kbFileUID,
			FileType:    fileType,
			ContentType: string(constant.SummaryContentType),
		}
		texts[i] = c.Text
	}
	for i, c := range contentChunks {
		ii := i + len(summaryChunks)
		textChunks[ii] = &repository.TextChunk{
			SourceUID:   sourceUID,
			SourceTable: sourceTable,
			StartPos:    c.Start,
			EndPos:      c.End,
			Reference:   c.Reference,
			ContentDest: "pending", // Placeholder, will be updated after MinIO save
			Tokens:      c.Tokens,
			Retrievable: true,
			InOrder:     ii,
			KbUID:       kbUID,
			KbFileUID:   kbFileUID,
			FileType:    fileType,
			ContentType: string(constant.ChunkContentType),
		}

		texts[ii] = c.Text
	}

	// Create a callback that saves chunks to MinIO and returns destinations
	// This callback is executed within the database transaction, ensuring atomicity
	callback := func(chunkUIDs []string) (map[string]string, error) {
		destinations := make(map[string]string, len(chunkUIDs))

		for i, chunkUID := range chunkUIDs {
			// Construct the MinIO path using the format: kb-{kbUID}/file-{fileUID}/chunk/{chunkUID}.md
			basePath := fmt.Sprintf("kb-%s/file-%s/chunk", kbUID.String(), kbFileUID.String())
			path := fmt.Sprintf("%s/%s.md", basePath, chunkUID)

			// Encode chunk content to base64
			base64Content := base64.StdEncoding.EncodeToString([]byte(texts[i]))

			// Save chunk content to MinIO
			err := minioClient.UploadBase64File(
				ctx,
				config.Config.Minio.BucketName,
				path,
				base64Content,
				"text/markdown",
			)
			if err != nil {
				return nil, fmt.Errorf("failed to save chunk %s to MinIO: %s", chunkUID, errorsx.MessageOrErr(err))
			}

			destinations[chunkUID] = path
		}

		return destinations, nil
	}

	// Save chunks to database and MinIO atomically
	// Delete old chunks and create new ones
	createdChunks, err := repo.DeleteAndCreateChunks(ctx, kbFileUID, textChunks, callback)
	if err != nil {
		return nil, fmt.Errorf("storing chunk records in repository: %s", errorsx.MessageOrErr(err))
	}

	// Build map of chunkUID -> content for MinIO save (for backward compatibility)
	// This is now just for returning the data, as MinIO save already happened in the callback
	chunksToSave := make(map[string][]byte, len(createdChunks))
	for i, chunk := range createdChunks {
		chunksToSave[chunk.UID.String()] = []byte(texts[i])
	}

	return chunksToSave, nil
}

// extractRequestMetadata extracts the gRPC metadata from a file's ExternalMetadata
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
		return nil, fmt.Errorf("marshalling metadata: %s", errorsx.MessageOrErr(err))
	}

	if err := json.Unmarshal(j, &md); err != nil {
		return nil, fmt.Errorf("unmarshalling metadata: %s", errorsx.MessageOrErr(err))
	}

	return md, nil
}

// createAuthenticatedContext creates a context with the authentication metadata
// from the file's ExternalMetadata. This allows activities to make authenticated
// calls to other services (like pipeline-backend).
func createAuthenticatedContext(ctx context.Context, externalMetadata *structpb.Struct) (context.Context, error) {
	md, err := extractRequestMetadata(externalMetadata)
	if err != nil {
		return ctx, fmt.Errorf("extracting request metadata: %s", errorsx.MessageOrErr(err))
	}

	if len(md) == 0 {
		return ctx, nil
	}

	return metadata.NewOutgoingContext(ctx, md), nil
}
