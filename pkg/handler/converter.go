package handler

import (
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/resource"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	mgmtpb "github.com/instill-ai/protogen-go/core/mgmt/v1beta"
	filetype "github.com/instill-ai/x/file"
)

// Reserved tag prefixes that users cannot set directly.
// These are managed by the system.
var reservedTagPrefixes = []string{
	"agent:",   // Reserved for agent-backend (e.g., agent:collection:{uid})
	"instill-", // Reserved for internal system use
}

// validateUserTags checks that user-provided tags don't use reserved prefixes.
func validateUserTags(tags []string) error {
	for _, tag := range tags {
		for _, prefix := range reservedTagPrefixes {
			if strings.HasPrefix(tag, prefix) {
				return status.Errorf(codes.InvalidArgument,
					"tags with prefix '%s' are reserved for system use", prefix)
			}
		}
	}
	return nil
}

// extractCollectionUIDs extracts collection UIDs from tags with prefix "agent:collection:".
func extractCollectionUIDs(tags []string) []string {
	const collectionTagPrefix = "agent:collection:"
	var collectionUIDs []string
	for _, tag := range tags {
		if strings.HasPrefix(tag, collectionTagPrefix) {
			uid := strings.TrimPrefix(tag, collectionTagPrefix)
			if uid != "" {
				collectionUIDs = append(collectionUIDs, uid)
			}
		}
	}
	return collectionUIDs
}

// convertKBToCatalogPB converts database KnowledgeBase to protobuf KnowledgeBase.
// Following the pattern from pipeline/model/mgmt backends, the `name` field
// is computed dynamically rather than stored in the database.
func convertKBToCatalogPB(kb *repository.KnowledgeBaseModel, ns *resource.Namespace, owner *mgmtpb.Owner, creator *mgmtpb.User) *artifactpb.KnowledgeBase {
	ownerName := ns.Name()

	knowledgeBase := &artifactpb.KnowledgeBase{
		Uid:         kb.UID.String(),
		Id:          kb.KBID,                                                  // Database field KBID maps to protobuf id
		Name:        fmt.Sprintf("%s/knowledge-bases/%s", ownerName, kb.KBID), // Computed dynamically!
		Description: kb.Description,
		CreateTime:  timestamppb.New(*kb.CreateTime),
		UpdateTime:  timestamppb.New(*kb.UpdateTime),
		OwnerName:   ownerName,
		OwnerUid:    kb.NamespaceUID,
		Owner:       owner,
		Creator:     creator,
		Tags:        kb.Tags,
	}

	// Handle nullable creator UID (nil for system-created KBs like instill-agent)
	if kb.CreatorUID != nil {
		creatorUIDStr := kb.CreatorUID.String()
		knowledgeBase.CreatorUid = &creatorUIDStr
	}

	// Handle optional fields
	// Check if ActiveCollectionUID is not zero (nil UUID)
	if kb.ActiveCollectionUID.String() != "00000000-0000-0000-0000-000000000000" {
		knowledgeBase.ActiveCollectionUid = kb.ActiveCollectionUID.String()
	}

	return knowledgeBase
}

// convertKBFileToPB converts database KnowledgeBaseFile to protobuf File.
// The `name` field is computed dynamically following other backends' patterns.
func convertKBFileToPB(kbf *repository.KnowledgeBaseFileModel, ns *resource.Namespace, kb *repository.KnowledgeBaseModel, owner *mgmtpb.Owner, creator *mgmtpb.User) *artifactpb.File {
	ownerName := ns.Name()
	fileIDStr := kbf.UID.String()

	file := &artifactpb.File{
		Uid:              fileIDStr,
		Id:               fileIDStr,                                                                    // For files, id = uid
		Name:             fmt.Sprintf("%s/knowledge-bases/%s/files/%s", ownerName, kb.KBID, fileIDStr), // Computed!
		Filename:         kbf.Filename,                                                                 // Database "filename" field is the user's filename
		Type:             convertFileType(kbf.FileType),
		CreateTime:       timestamppb.New(*kbf.CreateTime),
		UpdateTime:       timestamppb.New(*kbf.UpdateTime),
		OwnerUid:         kbf.NamespaceUID.String(),
		OwnerName:        ownerName,
		Owner:            owner,
		CreatorUid:       kbf.CreatorUID.String(),
		Creator:          creator,
		KnowledgeBaseUid: kbf.KBUID.String(),
		Size:             kbf.Size,
		ProcessStatus:    convertFileProcessStatus(kbf.ProcessStatus),
	}

	// Handle optional fields
	if kbf.DeleteTime.Valid {
		file.DeleteTime = timestamppb.New(kbf.DeleteTime.Time)
	}

	if len(kbf.Tags) > 0 {
		file.Tags = kbf.Tags
		// Extract collection UIDs from tags with prefix "agent:collection:"
		file.CollectionUids = extractCollectionUIDs(kbf.Tags)
	}

	if kbf.ExternalMetadataUnmarshal != nil {
		file.ExternalMetadata = kbf.ExternalMetadataUnmarshal
	}

	// Note: TotalChunks and TotalTokens are computed aggregates, not stored directly in the file model
	// These would need to be fetched separately if needed

	return file
}

// Helper conversion functions

func convertFileType(dbType string) artifactpb.File_Type {
	// Map database file type string to protobuf enum
	// Expects "TYPE_*" format (e.g., "TYPE_PDF")
	return filetype.ConvertFileTypeString(dbType)
}

func convertFileProcessStatus(status string) artifactpb.FileProcessStatus {
	switch status {
	case "FILE_PROCESS_STATUS_NOTSTARTED":
		return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_NOTSTARTED
	case "FILE_PROCESS_STATUS_CHUNKING":
		return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING
	case "FILE_PROCESS_STATUS_EMBEDDING":
		return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING
	case "FILE_PROCESS_STATUS_COMPLETED":
		return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED
	case "FILE_PROCESS_STATUS_FAILED":
		return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_FAILED
	case "FILE_PROCESS_STATUS_PROCESSING":
		return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_PROCESSING
	default:
		// Legacy statuses (WAITING, CONVERTING, SUMMARIZING) return UNSPECIFIED
		return artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_UNSPECIFIED
	}
}
