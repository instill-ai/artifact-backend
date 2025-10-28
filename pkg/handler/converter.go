package handler

import (
	"fmt"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/resource"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// convertKBToCatalogPB converts database KnowledgeBase to protobuf KnowledgeBase.
// Following the pattern from pipeline/model/mgmt backends, the `name` field
// is computed dynamically rather than stored in the database.
func convertKBToCatalogPB(kb *repository.KnowledgeBaseModel, ns *resource.Namespace) *artifactpb.KnowledgeBase {
	ownerName := ns.Name()

	knowledgeBase := &artifactpb.KnowledgeBase{
		Uid:         kb.UID.String(),
		Id:          kb.KBID,                                                  // Database field KBID maps to protobuf id
		Name:        fmt.Sprintf("%s/knowledge-bases/%s", ownerName, kb.KBID), // Computed dynamically!
		Description: kb.Description,
		CreateTime:  timestamppb.New(*kb.CreateTime),
		UpdateTime:  timestamppb.New(*kb.UpdateTime),
		OwnerName:   ownerName,
		Tags:        kb.Tags,
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
func convertKBFileToPB(kbf *repository.KnowledgeBaseFileModel, ns *resource.Namespace, kb *repository.KnowledgeBaseModel) *artifactpb.File {
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
		OwnerUid:         kbf.Owner.String(),
		CreatorUid:       kbf.CreatorUID.String(),
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
	switch dbType {
	case "FILE_TYPE_TEXT":
		return artifactpb.File_TYPE_TEXT
	case "FILE_TYPE_PDF":
		return artifactpb.File_TYPE_PDF
	case "FILE_TYPE_MARKDOWN":
		return artifactpb.File_TYPE_MARKDOWN
	case "FILE_TYPE_PNG":
		return artifactpb.File_TYPE_PNG
	case "FILE_TYPE_JPEG":
		return artifactpb.File_TYPE_JPEG
	case "FILE_TYPE_JPG":
		return artifactpb.File_TYPE_JPG
	case "FILE_TYPE_HTML":
		return artifactpb.File_TYPE_HTML
	case "FILE_TYPE_DOCX":
		return artifactpb.File_TYPE_DOCX
	case "FILE_TYPE_DOC":
		return artifactpb.File_TYPE_DOC
	case "FILE_TYPE_PPT":
		return artifactpb.File_TYPE_PPT
	case "FILE_TYPE_PPTX":
		return artifactpb.File_TYPE_PPTX
	case "FILE_TYPE_XLSX":
		return artifactpb.File_TYPE_XLSX
	case "FILE_TYPE_XLS":
		return artifactpb.File_TYPE_XLS
	case "FILE_TYPE_CSV":
		return artifactpb.File_TYPE_CSV
	default:
		return artifactpb.File_TYPE_UNSPECIFIED
	}
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
