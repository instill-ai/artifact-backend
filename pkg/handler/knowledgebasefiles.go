package handler

import (
	"context"
	"fmt"
	"strings"

	"github.com/gofrs/uuid"
	"github.com/instill-ai/artifact-backend/pkg/customerror"
	"github.com/instill-ai/artifact-backend/pkg/logger" // Add this import
	"github.com/instill-ai/artifact-backend/pkg/repository"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// 15MB
const maxFileSize = 15 * 1024 * 1024

// 1GB
const KnowledgeBaseMaxUsage = 1024 * 1024 * 1024

func (ph *PublicHandler) UploadKnowledgeBaseFile(ctx context.Context, req *artifactpb.UploadKnowledgeBaseFileRequest) (*artifactpb.UploadKnowledgeBaseFileResponse, error) {
	log, _ := logger.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		err := fmt.Errorf("failed to get user id from header: %v. err: %w", err, customerror.ErrUnauthenticated)
		return nil, err
	}
	err = checkUploadKnowledgeBaseFileRequest(req)
	if err != nil {
		return nil, err
	}

	if strings.Contains(req.File.Name, "/") {
		return nil, fmt.Errorf("file name cannot contain '/'. err: %w", customerror.ErrInvalidArgument)
	}
	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		log.Error(
			"failed to get namespace ",
			zap.Error(err),
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
			zap.String("auth_uid", authUID))
		return nil, fmt.Errorf("failed to get namespace. err: %w", err)
	}
	// ACL - check user's permission to write knowledge base
	kb, err := ph.service.Repository.GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID.String(), req.KbId)
	if err != nil {
		log.Error("failed to get knowledge base", zap.Error(err))
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}
	granted, err := ph.service.ACLClient.CheckPermission(ctx, "knowledgebase", kb.UID, "writer")
	if err != nil {
		log.Error("failed to check permission", zap.Error(err))
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err)
	}
	if !granted {
		log.Error("no permission to delete knowledge base")
		return nil, fmt.Errorf(ErrorDeleteKnowledgeBaseMsg, customerror.ErrNoPermission)
	}

	// upload file to minio and database
	var res *repository.KnowledgeBaseFile
	{
		creatorUID, err := uuid.FromString(authUID)
		if err != nil {
			log.Error("failed to parse creator uid", zap.Error(err))
			return nil, err
		}

		fileSize, _ := getFileSize(req.File.Content)
		// check if file size is more than 15MB
		if fileSize > maxFileSize {
			return nil, fmt.Errorf("file size is more than 15MB. err: %w", customerror.ErrInvalidArgument)
		}

		if kb.Usage+fileSize > KnowledgeBaseMaxUsage {
			return nil, fmt.Errorf("knowledge base 1 GB exceeded. err: %w", customerror.ErrInvalidArgument)
		}

		destination := ph.service.MinIO.GetUploadedFilePathInKnowledgeBase(kb.UID.String(), req.File.Name)
		kbFile := repository.KnowledgeBaseFile{
			Name:             req.File.Name,
			Type:             artifactpb.FileType_name[int32(req.File.Type)],
			Owner:            ns.NsUID,
			CreatorUID:       creatorUID,
			KnowledgeBaseUID: kb.UID,
			Destination:      destination,
			ProcessStatus:    artifactpb.FileProcessStatus_name[int32(artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_NOTSTARTED)],
			Size:             fileSize,
		}

		// create knowledge base file
		res, err = ph.service.Repository.CreateKnowledgeBaseFile(ctx, kbFile, func(FileUID string) error {
			// upload file to minio
			err := ph.service.MinIO.UploadBase64File(ctx, destination, req.File.Content, fileTypeConvertToMime(req.File.Type))
			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			log.Error("failed to create knowledge base file", zap.Error(err))
			return nil, err
		}

		// increase knowledge base usage
		err = ph.service.Repository.IncreaseKnowledgeBaseUsage(ctx, kb.UID.String(), int(fileSize))
		if err != nil {
			log.Error("failed to increase knowledge base usage", zap.Error(err))
			return nil, err
		}
	}

	return &artifactpb.UploadKnowledgeBaseFileResponse{
		File: &artifactpb.File{
			FileUid:       res.UID.String(),
			OwnerUid:      res.Owner.String(),
			CreatorUid:    res.CreatorUID.String(),
			KbUid:         res.KnowledgeBaseUID.String(),
			Name:          res.Name,
			Type:          req.File.Type,
			CreateTime:    timestamppb.New(*res.CreateTime),
			UpdateTime:    timestamppb.New(*res.UpdateTime),
			ProcessStatus: artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_NOTSTARTED,
			Size:          res.Size,
			TotalChunks:   0,
			TotalTokens:   0,
		},
	}, nil
}

// getFileSize returns the size of the file in bytes and a human-readable string
func getFileSize(base64String string) (int64, string) {
	// Get the length of the base64 string
	base64Length := len(base64String)

	// Calculate the size of the decoded data
	// The actual size is approximately 3/4 of the base64 string length
	decodedSize := base64Length / 4 * 3

	// Remove padding characters
	if base64String[base64Length-1] == '=' {
		decodedSize--
		if base64String[base64Length-2] == '=' {
			decodedSize--
		}
	}

	// Convert to appropriate unit
	const unit = 1024
	if decodedSize < unit {
		return int64(decodedSize), fmt.Sprintf("%d B", decodedSize)
	}
	div, exp := int64(unit), 0
	for n := int64(decodedSize) / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	size := float64(decodedSize) / float64(div)
	return int64(decodedSize), fmt.Sprintf("%.1f %cB", size, "KMGTPE"[exp])
}

func checkUploadKnowledgeBaseFileRequest(req *artifactpb.UploadKnowledgeBaseFileRequest) error {
	if req.GetNamespaceId() == "" {
		return fmt.Errorf("owner uid is required. err: %w", ErrCheckRequiredFields)
	} else if req.KbId == "" {
		return fmt.Errorf("knowledge base uid is required. err: %w", ErrCheckRequiredFields)
	} else if req.File == nil {
		return fmt.Errorf("file is required. err: %w", ErrCheckRequiredFields)
	} else if req.File.Name == "" {
		return fmt.Errorf("file name is required. err: %w", ErrCheckRequiredFields)
	} else if req.File.Content == "" {
		return fmt.Errorf("file content is required. err: %w", ErrCheckRequiredFields)
	} else if req.File.Type == 0 {
		return fmt.Errorf("file type is required. err: %w", ErrCheckRequiredFields)
	} else if !checkValidFileType(req.File.Type) {
		return fmt.Errorf("file type is not supported. err: %w", customerror.ErrInvalidArgument)
	}

	return nil
}

// check if type in pdf, markdown or text
func checkValidFileType(t artifactpb.FileType) bool {
	if t == artifactpb.FileType_FILE_TYPE_PDF ||
		t == artifactpb.FileType_FILE_TYPE_MARKDOWN ||
		t == artifactpb.FileType_FILE_TYPE_TEXT {
		return true
	}
	return false
}

func (ph *PublicHandler) ListKnowledgeBaseFiles(ctx context.Context, req *artifactpb.ListKnowledgeBaseFilesRequest) (*artifactpb.ListKnowledgeBaseFilesResponse, error) {

	log, _ := logger.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		log.Error("failed to get user id from header", zap.Error(err))
		err := fmt.Errorf("failed to get user id from header: %v. err: %w", err, customerror.ErrUnauthenticated)
		return nil, err
	}

	// ACL - check if the creator can list files in this knowledge base. ACL using uid to check the certain namespace resource.
	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		log.Error(
			"failed to get namespace ",
			zap.Error(err),
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
			zap.String("auth_uid", authUID))
		return nil, fmt.Errorf("failed to get namespace. err: %w", err)
	}
	// ACL - check user's permission to write knowledge base
	kb, err := ph.service.Repository.GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID.String(), req.KbId)
	if err != nil {
		log.Error("failed to get knowledge base", zap.Error(err))
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}
	granted, err := ph.service.ACLClient.CheckPermission(ctx, "knowledgebase", kb.UID, "reader")
	if err != nil {
		log.Error("failed to check permission", zap.Error(err))
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err)
	}
	if !granted {
		log.Error("no permission to delete knowledge base")
		return nil, fmt.Errorf(ErrorDeleteKnowledgeBaseMsg, customerror.ErrNoPermission)
	}

	// fetch the knowledge base files
	var files []*artifactpb.File
	var totalSize int
	var nextPageToken string
	{
		if req.Filter == nil {
			req.Filter = &artifactpb.ListKnowledgeBaseFilesFilter{
				FileUids: []string{},
			}
		}
		kbFiles, size, nextToken, err := ph.service.Repository.ListKnowledgeBaseFiles(ctx, authUID, ns.NsUID.String(), kb.UID.String(), req.PageSize, req.PageToken, req.Filter.FileUids)
		if err != nil {
			log.Error("failed to list knowledge base files", zap.Error(err))
			return nil, err
		}
		// get the tokens and chunks using the source table and source uid
		sources, err := ph.service.Repository.GetSourceTableAndUIDByFileUIDs(ctx, kbFiles)
		if err != nil {
			log.Error("failed to find source table and source uid by file uid", zap.Error(err))
			return nil, err
		}

		totalTokens, err := ph.service.Repository.GetFilesTotalTokens(ctx, sources)
		if err != nil {
			log.Error("failed to get files total tokens", zap.Error(err))
			return nil, err
		}

		totalChunks, err := ph.service.Repository.GetTotalChunksBySources(ctx, sources)
		if err != nil {
			log.Error("failed to get files total chunks", zap.Error(err))
			return nil, err
		}
		totalSize = size
		nextPageToken = nextToken
		for _, kbFile := range kbFiles {
			files = append(files, &artifactpb.File{
				FileUid:       kbFile.UID.String(),
				OwnerUid:      kbFile.Owner.String(),
				CreatorUid:    kbFile.CreatorUID.String(),
				KbUid:         kbFile.KnowledgeBaseUID.String(),
				Name:          kbFile.Name,
				Type:          artifactpb.FileType(artifactpb.FileType_value[kbFile.Type]),
				CreateTime:    timestamppb.New(*kbFile.CreateTime),
				UpdateTime:    timestamppb.New(*kbFile.UpdateTime),
				ProcessStatus: artifactpb.FileProcessStatus(artifactpb.FileProcessStatus_value[kbFile.ProcessStatus]),
				Size:          kbFile.Size,
				TotalChunks:   int32(totalChunks[kbFile.UID]),
				TotalTokens:   int32(totalTokens[kbFile.UID]),
			})
		}
	}

	return &artifactpb.ListKnowledgeBaseFilesResponse{
		Files:         files,
		TotalSize:     int32(totalSize),
		NextPageToken: nextPageToken,
		Filter:        req.Filter,
	}, nil
}

func (ph *PublicHandler) DeleteKnowledgeBaseFile(
	ctx context.Context,
	req *artifactpb.DeleteKnowledgeBaseFileRequest) (
	*artifactpb.DeleteKnowledgeBaseFileResponse, error) {
	log, _ := logger.GetZapLogger(ctx)
	// authUID, err := getUserUIDFromContext(ctx)
	// if err != nil {
	// 	err := fmt.Errorf("failed to get user id from header: %v. err: %w", err, customerror.ErrUnauthenticated)
	// 	return nil, err
	// }

	// ACL - check user's permission to write knowledge base of kb file
	kbfs, err := ph.service.Repository.GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{uuid.FromStringOrNil(req.FileUid)})
	if err != nil && len(kbfs) == 0 {
		log.Error("failed to get knowledge base", zap.Error(err))
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}
	granted, err := ph.service.ACLClient.CheckPermission(ctx, "knowledgebase", kbfs[0].KnowledgeBaseUID, "writer")
	if err != nil {
		log.Error("failed to check permission", zap.Error(err))
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err)
	}
	if !granted {
		log.Error("no permission to delete knowledge base")
		return nil, fmt.Errorf(ErrorDeleteKnowledgeBaseMsg, customerror.ErrNoPermission)
	}
	// check if file uid is empty
	if req.FileUid == "" {
		return nil, fmt.Errorf("file uid is required. err: %w", customerror.ErrInvalidArgument)
	}

	fuid, err := uuid.FromString(req.FileUid)
	if err != nil {
		return nil, fmt.Errorf("failed to parse file uid. err: %w", customerror.ErrInvalidArgument)
	}

	// get the file by uid
	files, err := ph.service.Repository.GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{fuid})
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("file not found. err: %w", customerror.ErrNotFound)
	}

	err = ph.service.Repository.DeleteKnowledgeBaseFile(ctx, req.FileUid)
	if err != nil {
		return nil, err
	}

	// decrease knowledge base usage
	err = ph.service.Repository.IncreaseKnowledgeBaseUsage(ctx, files[0].KnowledgeBaseUID.String(), int(-files[0].Size))
	if err != nil {
		return nil, err
	}

	return &artifactpb.DeleteKnowledgeBaseFileResponse{
		FileUid: req.FileUid,
	}, nil

}

func (ph *PublicHandler) ProcessKnowledgeBaseFiles(ctx context.Context, req *artifactpb.ProcessKnowledgeBaseFilesRequest) (*artifactpb.ProcessKnowledgeBaseFilesResponse, error) {
	// uid, err := getUserIDFromContext(ctx)
	// if err != nil {
	// 	err := fmt.Errorf("failed to get user id from header: %v. err: %w", err, customerror.ErrUnauthenticated)
	// 	return nil, err
	// }

	// ACL - check if the uid can process file. ACL.
	// chekc the fiels's kb_uid and use kb_uid to check if user has write permission
	fileUUIDs := make([]uuid.UUID, 0, len(req.FileUids))
	for _, fileUID := range req.FileUids {
		fuid, err := uuid.FromString(fileUID)
		if err != nil {
			return nil, fmt.Errorf("failed to parse file uid. err: %w", customerror.ErrInvalidArgument)
		}
		fileUUIDs = append(fileUUIDs, fuid)
	}
	kbfs, err := ph.service.Repository.GetKnowledgeBaseFilesByFileUIDs(ctx, fileUUIDs)
	if err != nil {
		return nil, err
	}
	if len(kbfs) == 0 {
		return nil, fmt.Errorf("file not found. err: %w", customerror.ErrNotFound)
	}
	// check write permission for the knowledge base
	for _, kbf := range kbfs {
		granted, err := ph.service.ACLClient.CheckPermission(ctx, "knowledgebase", kbf.KnowledgeBaseUID, "writer")
		if err != nil {
			return nil, err
		}
		if !granted {
			return nil, fmt.Errorf("no permission to process knowledge base file.fileUID:%s err: %w", kbf.UID, customerror.ErrNoPermission)
		}
	}

	files, err := ph.service.Repository.ProcessKnowledgeBaseFiles(ctx, req.FileUids)
	if err != nil {
		return nil, err
	}

	// populate the files into response
	var resFiles []*artifactpb.File
	for _, file := range files {
		resFiles = append(resFiles, &artifactpb.File{
			FileUid:       file.UID.String(),
			OwnerUid:      file.Owner.String(),
			CreatorUid:    file.CreatorUID.String(),
			KbUid:         file.KnowledgeBaseUID.String(),
			Name:          file.Name,
			Type:          artifactpb.FileType(artifactpb.FileType_value[file.Type]),
			CreateTime:    timestamppb.New(*file.CreateTime),
			UpdateTime:    timestamppb.New(*file.UpdateTime),
			ProcessStatus: artifactpb.FileProcessStatus(artifactpb.FileProcessStatus_value[file.ProcessStatus]),
		})
	}
	return &artifactpb.ProcessKnowledgeBaseFilesResponse{
		Files: resFiles,
	}, nil
}

func fileTypeConvertToMime(t artifactpb.FileType) string {
	switch t {
	case artifactpb.FileType_FILE_TYPE_PDF:
		return "application/pdf"
	case artifactpb.FileType_FILE_TYPE_MARKDOWN:
		return "text/markdown"
	case artifactpb.FileType_FILE_TYPE_TEXT:
		return "text/plain"
	default:
		return "application/octet-stream"
	}
}
