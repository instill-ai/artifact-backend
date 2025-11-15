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
	// Supports both formats: "TYPE_*" (new) and "FILE_TYPE_*" (legacy)
	switch dbType {
	case "TYPE_TEXT", "FILE_TYPE_TEXT":
		return artifactpb.File_TYPE_TEXT
	case "TYPE_PDF", "FILE_TYPE_PDF":
		return artifactpb.File_TYPE_PDF
	case "TYPE_MARKDOWN", "FILE_TYPE_MARKDOWN":
		return artifactpb.File_TYPE_MARKDOWN
	case "TYPE_PNG", "FILE_TYPE_PNG":
		return artifactpb.File_TYPE_PNG
	case "TYPE_JPEG", "FILE_TYPE_JPEG":
		return artifactpb.File_TYPE_JPEG
	case "TYPE_GIF", "FILE_TYPE_GIF":
		return artifactpb.File_TYPE_GIF
	case "TYPE_WEBP", "FILE_TYPE_WEBP":
		return artifactpb.File_TYPE_WEBP
	case "TYPE_TIFF", "FILE_TYPE_TIFF":
		return artifactpb.File_TYPE_TIFF
	case "TYPE_HEIC", "FILE_TYPE_HEIC":
		return artifactpb.File_TYPE_HEIC
	case "TYPE_HEIF", "FILE_TYPE_HEIF":
		return artifactpb.File_TYPE_HEIF
	case "TYPE_AVIF", "FILE_TYPE_AVIF":
		return artifactpb.File_TYPE_AVIF
	case "TYPE_BMP", "FILE_TYPE_BMP":
		return artifactpb.File_TYPE_BMP
	case "TYPE_MP3", "FILE_TYPE_MP3":
		return artifactpb.File_TYPE_MP3
	case "TYPE_WAV", "FILE_TYPE_WAV":
		return artifactpb.File_TYPE_WAV
	case "TYPE_AAC", "FILE_TYPE_AAC":
		return artifactpb.File_TYPE_AAC
	case "TYPE_OGG", "FILE_TYPE_OGG":
		return artifactpb.File_TYPE_OGG
	case "TYPE_FLAC", "FILE_TYPE_FLAC":
		return artifactpb.File_TYPE_FLAC
	case "TYPE_AIFF", "FILE_TYPE_AIFF":
		return artifactpb.File_TYPE_AIFF
	case "TYPE_M4A", "FILE_TYPE_M4A":
		return artifactpb.File_TYPE_M4A
	case "TYPE_WMA", "FILE_TYPE_WMA":
		return artifactpb.File_TYPE_WMA
	case "TYPE_WEBM_AUDIO", "FILE_TYPE_WEBM_AUDIO":
		return artifactpb.File_TYPE_WEBM_AUDIO
	case "TYPE_MP4", "FILE_TYPE_MP4":
		return artifactpb.File_TYPE_MP4
	case "TYPE_AVI", "FILE_TYPE_AVI":
		return artifactpb.File_TYPE_AVI
	case "TYPE_MOV", "FILE_TYPE_MOV":
		return artifactpb.File_TYPE_MOV
	case "TYPE_FLV", "FILE_TYPE_FLV":
		return artifactpb.File_TYPE_FLV
	case "TYPE_WEBM_VIDEO", "FILE_TYPE_WEBM_VIDEO":
		return artifactpb.File_TYPE_WEBM_VIDEO
	case "TYPE_WMV", "FILE_TYPE_WMV":
		return artifactpb.File_TYPE_WMV
	case "TYPE_MKV", "FILE_TYPE_MKV":
		return artifactpb.File_TYPE_MKV
	case "TYPE_HTML", "FILE_TYPE_HTML":
		return artifactpb.File_TYPE_HTML
	case "TYPE_DOCX", "FILE_TYPE_DOCX":
		return artifactpb.File_TYPE_DOCX
	case "TYPE_DOC", "FILE_TYPE_DOC":
		return artifactpb.File_TYPE_DOC
	case "TYPE_PPT", "FILE_TYPE_PPT":
		return artifactpb.File_TYPE_PPT
	case "TYPE_PPTX", "FILE_TYPE_PPTX":
		return artifactpb.File_TYPE_PPTX
	case "TYPE_XLSX", "FILE_TYPE_XLSX":
		return artifactpb.File_TYPE_XLSX
	case "TYPE_XLS", "FILE_TYPE_XLS":
		return artifactpb.File_TYPE_XLS
	case "TYPE_CSV", "FILE_TYPE_CSV":
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
