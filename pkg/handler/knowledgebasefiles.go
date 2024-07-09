package handler

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/instill-ai/artifact-backend/pkg/customerror"
	"github.com/instill-ai/artifact-backend/pkg/logger" // Add this import
	"github.com/instill-ai/artifact-backend/pkg/repository"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	mgmtpb "github.com/instill-ai/protogen-go/core/mgmt/v1beta"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (ph *PublicHandler) UploadKnowledgeBaseFile(ctx context.Context, req *artifactpb.UploadKnowledgeBaseFileRequest) (*artifactpb.UploadKnowledgeBaseFileResponse, error) {
	log, _ := logger.GetZapLogger(ctx)
	uid, err := getUserUIDFromContext(ctx)
	if err != nil {
		err := fmt.Errorf("failed to get user id from header: %v. err: %w", err, customerror.ErrUnauthenticated)
		return nil, err
	}
	err = checkUploadKnowledgeBaseFileRequest(req)
	if err != nil {
		return nil, err
	}

	// TODO: ACL - check if the creator can upload file to this knowledge base. ACL.
	// .....

	// get the owner uid from the mgmt service
	var ownerUID string
	{
		// get the owner uid from the mgmt service
		ownerUID, err = ph.getOwnerUID(ctx, req.OwnerId)
		if err != nil {
			log.Error("failed to get owner uid", zap.Error(err))
			return nil, err
		}
	}

	// upload file to minio
	var kb *repository.KnowledgeBase
	var filePathName string
	{
		kb, err = ph.service.Repository.GetKnowledgeBaseByOwnerAndID(ctx, ownerUID, req.KbId)
		if err != nil {
			return nil, fmt.Errorf("failed to get knowledge base by owner and id. err: %w", err)
		}
		// check if the name has "/" which may cause folder creation in minio
		if strings.Contains(req.File.Name, "/") {
			return nil, fmt.Errorf("file name cannot contain '/'. err: %w", customerror.ErrInvalidArgument)
		}
		filePathName = kb.UID.String() + "/" + req.File.Name
		err = ph.service.MinIO.UploadBase64File(ctx, filePathName, req.File.Content, fileTypeConvertToMime(req.File.Type))
		if err != nil {
			return nil, err
		}
	}

	// create metadata in db
	var res *repository.KnowledgeBaseFile
	{
		creatorUID, err := uuid.Parse(uid)
		if err != nil {
			log.Error("failed to parse creator uid", zap.Error(err))
			return nil, err
		}
		// turn ownerUIDUuID to uuid
		ownerUIDUuid, err := uuid.Parse(ownerUID)
		if err != nil {
			log.Error("failed to parse owner uid", zap.Error(err))
			return nil, err
		}
		kbFile := repository.KnowledgeBaseFile{
			Name:             req.File.Name,
			Type:             artifactpb.FileType_name[int32(req.File.Type)],
			Owner:            ownerUIDUuid,
			CreatorUID:       creatorUID,
			KnowledgeBaseUID: kb.UID,
			Destination:      filePathName,
			ProcessStatus:    artifactpb.FileProcessStatus_name[int32(artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_NOTSTARTED)],
		}
		res, err = ph.service.Repository.CreateKnowledgeBaseFile(ctx, kbFile)
		if err != nil {
			err := ph.service.MinIO.DeleteFile(ctx, filePathName)
			if err != nil {
				log.Error("failed to delete file in minio", zap.Error(err))
			}
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
		},
	}, nil
}

func checkUploadKnowledgeBaseFileRequest(req *artifactpb.UploadKnowledgeBaseFileRequest) error {
	if req.OwnerId == "" {
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
	fmt.Println("ListKnowledgeBaseFiles>>>", req)
	uid, err := getUserUIDFromContext(ctx)
	if err != nil {
		log.Error("failed to get user id from header", zap.Error(err))
		err := fmt.Errorf("failed to get user id from header: %v. err: %w", err, customerror.ErrUnauthenticated)
		return nil, err
	}

	// TODO: ACL - check if the creator can list files in this knowledge base. ACL using uid to check the certain namespace resource.
	// acl, err := ph.service.ACL.CheckPermission(ctx, uid, "knowledgeBase", req.KbId, "read")

	// get the owner uid from the mgmt service
	var ownerUID string
	{
		// get the owner uid from the mgmt service
		ownerUID, err = ph.getOwnerUID(ctx, req.OwnerId)
		if err != nil {
			log.Error("failed to get owner uid", zap.Error(err))
			return nil, err
		}
	}

	// get the kb uid from the knowledge base table
	var kbUID string
	{
		kb, err := ph.service.Repository.GetKnowledgeBaseByOwnerAndID(ctx, ownerUID, req.KbId)
		if err != nil {
			log.Error("failed to get knowledge base by owner and id", zap.Error(err))
			return nil, err
		}
		kbUID = kb.UID.String()
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
		kbFiles, size, nextToken, err := ph.service.Repository.ListKnowledgeBaseFiles(ctx, uid, ownerUID, kbUID, req.PageSize, req.PageToken, req.Filter.FileUids)
		if err != nil {
			log.Error("failed to list knowledge base files", zap.Error(err))
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

	// uid, err := getUserIDFromContext(ctx)
	// if err != nil {
	// 	err := fmt.Errorf("failed to get user id from header: %v. err: %w", err, customerror.ErrUnauthenticated)
	// 	return nil, err
	// }

	// TODO: ACL - check if the uid can delete file. ACL.

	err := ph.service.Repository.DeleteKnowledgeBaseFile(ctx, req.FileUid)
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

	// TODO: ACL - check if the uid can process file. ACL.
	// ....

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

// use mgmt service to get the user uid
// REFACTOR: when mgmt support owner.uid get by owner.id, we can optimize this
func (ph *PublicHandler) getOwnerUID(ctx context.Context, ownerID string) (string, error) {
	log, _ := logger.GetZapLogger(ctx)

	// get from user rpc
	ownerUID, err := ph.service.MgmtPrv.GetUserAdmin(ctx, &mgmtpb.GetUserAdminRequest{Name: "users/" + ownerID})
	if err != nil {
		log.Error("error occurred when get user from mgmt", zap.Error(err))
		return "", err
	} else {
		if ownerUID.User != nil {
			if ownerUID.User.Uid != nil {
				return *ownerUID.User.Uid, nil
			}
		}
	}

	// get from org rpc
	orgUID, err := ph.service.MgmtPrv.GetOrganizationAdmin(ctx, &mgmtpb.GetOrganizationAdminRequest{Name: "organizations/" + ownerID})
	if err != nil {
		log.Error("error occurred when get organization from mgmt", zap.Error(err))
		return "", err
	} else {
		if orgUID.Organization != nil {
			return orgUID.Organization.Uid, nil
		}
	}
	return "", fmt.Errorf("failed to get owner uid from users and orgs. err: %w", customerror.ErrNotFound)
}
