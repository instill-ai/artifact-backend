package handler

import (
	"context"
	"fmt"
	"strings"

	"github.com/gofrs/uuid"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/customerror"
	"github.com/instill-ai/artifact-backend/pkg/logger" // Add this import
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/resource"
	"github.com/instill-ai/artifact-backend/pkg/utils"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
)

func (ph *PublicHandler) UploadCatalogFile(ctx context.Context, req *artifactpb.UploadCatalogFileRequest) (*artifactpb.UploadCatalogFileResponse, error) {
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

	// determine the file type by its extension
	req.File.Type = DetermineFileType(req.File.Name)
	if req.File.Type == artifactpb.FileType_FILE_TYPE_UNSPECIFIED {
		return nil, fmt.Errorf("file extension is not supported. name: %s err: %w",
			req.File.Name, customerror.ErrInvalidArgument)
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
	// ACL - check user's permission to write catalog
	kb, err := ph.service.Repository.GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, req.CatalogId)
	if err != nil {
		log.Error("failed to get catalog", zap.Error(err))
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}
	granted, err := ph.service.ACLClient.CheckPermission(ctx, "knowledgebase", kb.UID, "writer")
	if err != nil {
		log.Error("failed to check permission", zap.Error(err))
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err)
	}
	if !granted {
		log.Error("no permission to upload file in catalog")
		return nil, fmt.Errorf("no permission to upload file. %w", customerror.ErrNoPermission)
	}

	// get all kbs in the namespace
	kbs, err := ph.service.Repository.ListKnowledgeBases(ctx, ns.NsUID.String())
	if err != nil {
		log.Error("failed to list catalog", zap.Error(err))
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}
	totalUsageInNamespace := int64(0)
	for _, kb := range kbs {
		totalUsageInNamespace += kb.Usage
	}
	// get tier of the namespace
	tier, err := ph.service.GetNamespaceTier(ctx, ns)
	if err != nil {
		log.Error("failed to get namespace tier", zap.Error(err))
		return nil, fmt.Errorf("failed to get namespace tier. err: %w", err)
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

		// check if file size is more than 150MB
		if fileSize > int64(tier.GetMaxUploadFileSize()) {
			return nil, fmt.Errorf(
				"file size is more than %v. err: %w",
				tier.GetMaxUploadFileSize(),
				customerror.ErrInvalidArgument)
		}

		// check if total usage in namespace
		quota, humanReadable := tier.GetFileStorageTotalQuota()
		if totalUsageInNamespace+fileSize > int64(quota) {
			return nil, fmt.Errorf(
				"file storage total quota exceeded. max: %v. tier:%v, err: %w",
				humanReadable, tier.String(), customerror.ErrInvalidArgument)
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

		// create catalog file in database
		res, err = ph.service.Repository.CreateKnowledgeBaseFile(ctx, kbFile, func(FileUID string) error {
			// upload file to minio
			err := ph.service.MinIO.UploadBase64File(ctx, destination, req.File.Content, fileTypeConvertToMime(req.File.Type))
			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			log.Error("failed to create catalog file", zap.Error(err))
			return nil, err
		}

		// increase catalog usage. need to increase after the file is created.
		// Note: in the future, we need to increase the usage in transaction with creating the file.
		err = ph.service.Repository.IncreaseKnowledgeBaseUsage(ctx, nil, kb.UID.String(), int(fileSize))
		if err != nil {
			log.Error("failed to increase catalog usage", zap.Error(err))
			return nil, err
		}
	}

	return &artifactpb.UploadCatalogFileResponse{
		File: &artifactpb.File{
			FileUid:       res.UID.String(),
			OwnerUid:      res.Owner.String(),
			CreatorUid:    res.CreatorUID.String(),
			CatalogUid:    res.KnowledgeBaseUID.String(),
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

func checkUploadKnowledgeBaseFileRequest(req *artifactpb.UploadCatalogFileRequest) error {
	if req.GetNamespaceId() == "" {
		return fmt.Errorf("owner uid is required. err: %w", ErrCheckRequiredFields)
	} else if req.CatalogId == "" {
		return fmt.Errorf("catalog uid is required. err: %w", ErrCheckRequiredFields)
	} else if req.File == nil {
		return fmt.Errorf("file is required. err: %w", ErrCheckRequiredFields)
	} else if req.File.Name == "" {
		return fmt.Errorf("file name is required. err: %w", ErrCheckRequiredFields)
	} else if req.File.Content == "" {
		return fmt.Errorf("file content is required. err: %w", ErrCheckRequiredFields)
	}

	return nil
}

func (ph *PublicHandler) ListCatalogFiles(ctx context.Context, req *artifactpb.ListCatalogFilesRequest) (*artifactpb.ListCatalogFilesResponse, error) {

	log, _ := logger.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		log.Error("failed to get user id from header", zap.Error(err))
		err := fmt.Errorf("failed to get user id from header: %v. err: %w", err, customerror.ErrUnauthenticated)
		return nil, err
	}

	// ACL - check if the creator can list files in this catalog. ACL using uid to check the certain namespace resource.
	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		log.Error(
			"failed to get namespace ",
			zap.Error(err),
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
			zap.String("auth_uid", authUID))
		return nil, fmt.Errorf("failed to get namespace. err: %w", err)
	}
	// ACL - check user's permission to write catalog
	kb, err := ph.service.Repository.GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, req.CatalogId)
	if err != nil {
		log.Error("failed to get catalog", zap.Error(err))
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}
	granted, err := ph.service.ACLClient.CheckPermission(ctx, "knowledgebase", kb.UID, "reader")
	if err != nil {
		log.Error("failed to check permission", zap.Error(err))
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err)
	}
	if !granted {
		log.Error("no permission to list catalog files")
		return nil, fmt.Errorf("no permission to list catalog files. %w", customerror.ErrNoPermission)
	}

	// fetch the catalog files
	var files []*artifactpb.File
	var totalSize int
	var nextPageToken string
	{
		if req.Filter == nil {
			req.Filter = &artifactpb.ListCatalogFilesFilter{
				FileUids: []string{},
			}
		}
		kbFiles, size, nextToken, err := ph.service.Repository.ListKnowledgeBaseFiles(ctx, authUID, ns.NsUID.String(), kb.UID.String(), req.PageSize, req.PageToken, req.Filter.FileUids)
		if err != nil {
			log.Error("failed to list catalog files", zap.Error(err))
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
				CatalogUid:    kbFile.KnowledgeBaseUID.String(),
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

	return &artifactpb.ListCatalogFilesResponse{
		Files:         files,
		TotalSize:     int32(totalSize),
		NextPageToken: nextPageToken,
		Filter:        req.Filter,
	}, nil
}

func (ph *PublicHandler) DeleteCatalogFile(
	ctx context.Context,
	req *artifactpb.DeleteCatalogFileRequest) (
	*artifactpb.DeleteCatalogFileResponse, error) {
	log, _ := logger.GetZapLogger(ctx)
	// authUID, err := getUserUIDFromContext(ctx)
	// if err != nil {
	// 	err := fmt.Errorf("failed to get user id from header: %v. err: %w", err, customerror.ErrUnauthenticated)
	// 	return nil, err
	// }

	// ACL - check user's permission to write catalog of kb file
	kbfs, err := ph.service.Repository.GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{uuid.FromStringOrNil(req.FileUid)})
	if err != nil {
		log.Error("failed to get catalog files", zap.Error(err))
		return nil, fmt.Errorf("failed to get catalog files. err: %w", err)
	} else if len(kbfs) == 0 {
		return nil, fmt.Errorf("file not found. err: %w", customerror.ErrNotFound)
	}
	granted, err := ph.service.ACLClient.CheckPermission(ctx, "knowledgebase", kbfs[0].KnowledgeBaseUID, "writer")
	if err != nil {
		log.Error("failed to check permission", zap.Error(err))
		return nil, fmt.Errorf("failed to check permission. err: %w", err)
	}
	if !granted {
		log.Error("no permission to delete catalog file")
		return nil, fmt.Errorf("no permission to delete catalog file. err: %w", customerror.ErrNoPermission)
	}
	// check if file uid is empty
	if req.FileUid == "" {
		return nil, fmt.Errorf("file uid is required. err: %w", customerror.ErrInvalidArgument)
	}

	fUID, err := uuid.FromString(req.FileUid)
	if err != nil {
		return nil, fmt.Errorf("failed to parse file uid. err: %w", customerror.ErrInvalidArgument)
	}

	// get the file by uid
	files, err := ph.service.Repository.GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{fUID})
	if err != nil {
		return nil, err
	} else if len(files) == 0 {
		return nil, fmt.Errorf("file not found. err: %w", customerror.ErrNotFound)
	}

	startSignal := make(chan bool)
	// TODO: need to use clean worker in the future
	go utils.GoRecover(
		func() {
			// Create a new context to prevent the parent context from being cancelled
			ctx := context.TODO()
			log, _ := logger.GetZapLogger(ctx)
			canStart := <-startSignal
			if !canStart {
				log.Info("DeleteCatalogFile: received stop signal")
				return
			}
			log.Info("DeleteCatalogFile: start deleting file from minio, database and milvus")
			allPass := true
			// Delete the file from MinIO
			objectPaths := []string{}
			// Add the knowledge base file in MinIO to the list of objects to delete
			objectPaths = append(objectPaths, files[0].Destination)
			// Add the converted file in MinIO to the list of objects to delete
			cf, err := ph.service.Repository.GetConvertedFileByFileUID(ctx, fUID)
			if err != nil {
				if err != gorm.ErrRecordNotFound {
					log.Error("failed to get converted file by file uid", zap.Error(err))
					allPass = false
				}
			} else if cf != nil {
				objectPaths = append(objectPaths, cf.Destination)
			}
			// Add the chunks in MinIO to the list of objects to delete
			chunks, err := ph.service.Repository.ListChunksByKbFileUID(ctx, fUID)
			if err != nil {
				log.Error("failed to get chunks by kb file uid", zap.Error(err))
				allPass = false
			} else if len(chunks) > 0 {
				for _, chunk := range chunks {
					objectPaths = append(objectPaths, chunk.ContentDest)
				}
			}
			// Delete the embeddings in Milvus (this better to be done first)
			embUIDs := []string{}
			embeddings, _ := ph.service.Repository.ListEmbeddingsByKbFileUID(ctx, fUID)
			for _, emb := range embeddings {
				embUIDs = append(embUIDs, emb.UID.String())
			}
			err = ph.service.MilvusClient.DeleteEmbeddingsInKb(ctx, files[0].KnowledgeBaseUID.String(), embUIDs)
			if err != nil {
				log.Error("failed to delete embeddings in milvus", zap.Error(err))
				allPass = false
			}

			// Delete the files in MinIO
			errChan := ph.service.MinIO.DeleteFiles(ctx, objectPaths)
			for err := range errChan {
				if err != nil {
					log.Error("failed to delete files in minio", zap.Error(err))
					allPass = false
				}
			}
			// Delete the converted file in PostgreSQL
			err = ph.service.Repository.HardDeleteConvertedFileByFileUID(ctx, fUID)
			if err != nil {
				log.Error("failed to delete converted file in postgreSQL", zap.Error(err))
				allPass = false
			}
			// Delete the chunks in PostgreSQL
			err = ph.service.Repository.HardDeleteChunksByKbFileUID(ctx, fUID)
			if err != nil {
				log.Error("failed to delete chunks in postgreSQL", zap.Error(err))
				allPass = false
			}
			// Delete the embeddings in PostgreSQL
			err = ph.service.Repository.HardDeleteEmbeddingsByKbFileUID(ctx, fUID)
			if err != nil {
				log.Error("failed to delete embeddings in postgreSQL", zap.Error(err))
				allPass = false
			}
			if allPass {
				log.Info("DeleteCatalogFile: successfully deleted file from minio, database and milvus", zap.String("file_uid", fUID.String()))
			} else {
				log.Error("DeleteCatalogFile: failed to delete file from minio, database and milvus", zap.String("file_uid", fUID.String()))
			}
		},
		"DeleteCatalogFile",
	)

	err = ph.service.Repository.DeleteKnowledgeBaseFileAndDecreaseUsage(ctx, fUID)
	if err != nil {
		log.Error("failed to delete knowledge base file and decrease usage", zap.Error(err))
		startSignal <- false
		return nil, err
	}
	// start the background deletion
	startSignal <- true

	return &artifactpb.DeleteCatalogFileResponse{
		FileUid: fUID.String(),
	}, nil

}

func (ph *PublicHandler) ProcessCatalogFiles(ctx context.Context, req *artifactpb.ProcessCatalogFilesRequest) (*artifactpb.ProcessCatalogFilesResponse, error) {

	log, _ := logger.GetZapLogger(ctx)
	// ACL - check if the uid can process file. ACL.
	// check the file's kb_uid and use kb_uid to check if user has write permission
	fileUUIDs := make([]uuid.UUID, 0, len(req.FileUids))
	for _, fileUID := range req.FileUids {
		fUID, err := uuid.FromString(fileUID)
		if err != nil {
			return nil, fmt.Errorf("failed to parse file uid. err: %w", customerror.ErrInvalidArgument)
		}
		fileUUIDs = append(fileUUIDs, fUID)
	}
	kbfs, err := ph.service.Repository.GetKnowledgeBaseFilesByFileUIDs(ctx, fileUUIDs)
	if err != nil {
		return nil, err
	} else if len(kbfs) == 0 {
		return nil, fmt.Errorf("file not found. err: %w", customerror.ErrNotFound)
	}
	// check write permission for the catalog
	for _, kbf := range kbfs {
		granted, err := ph.service.ACLClient.CheckPermission(ctx, "knowledgebase", kbf.KnowledgeBaseUID, "writer")
		if err != nil {
			return nil, err
		}
		if !granted {
			return nil, fmt.Errorf("no permission to process catalog file.fileUID:%s err: %w", kbf.UID, customerror.ErrNoPermission)
		}
	}

	// check auth user has access to the requester
	err = ph.service.ACLClient.CheckRequesterPermission(ctx)
	if err != nil {
		log.Error("failed to check requester permission", zap.Error(err))
		return nil, fmt.Errorf("failed to check requester permission. err: %w", err)
	}

	requesterUID := resource.GetRequestSingleHeader(ctx, constant.HeaderRequesterUIDKey)
	requesterUUID := uuid.FromStringOrNil(requesterUID)

	files, err := ph.service.Repository.ProcessKnowledgeBaseFiles(ctx, req.FileUids, requesterUUID)
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
			CatalogUid:    file.KnowledgeBaseUID.String(),
			Name:          file.Name,
			Type:          artifactpb.FileType(artifactpb.FileType_value[file.Type]),
			CreateTime:    timestamppb.New(*file.CreateTime),
			UpdateTime:    timestamppb.New(*file.UpdateTime),
			ProcessStatus: artifactpb.FileProcessStatus(artifactpb.FileProcessStatus_value[file.ProcessStatus]),
		})
	}
	return &artifactpb.ProcessCatalogFilesResponse{
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
	case artifactpb.FileType_FILE_TYPE_DOC:
		return "application/msword"
	case artifactpb.FileType_FILE_TYPE_DOCX:
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	case artifactpb.FileType_FILE_TYPE_HTML:
		return "text/html"
	case artifactpb.FileType_FILE_TYPE_PPT:
		return "application/vnd.ms-powerpoint"
	case artifactpb.FileType_FILE_TYPE_PPTX:
		return "application/vnd.openxmlformats-officedocument.presentationml.presentation"
	case artifactpb.FileType_FILE_TYPE_XLSX:
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	default:
		return "application/octet-stream"
	}
}

// DetermineFileType determine the file type by its extension
func DetermineFileType(fileName string) artifactpb.FileType {
	if strings.HasSuffix(fileName, ".pdf") {
		return artifactpb.FileType_FILE_TYPE_PDF
	} else if strings.HasSuffix(fileName, ".md") {
		return artifactpb.FileType_FILE_TYPE_MARKDOWN
	} else if strings.HasSuffix(fileName, ".txt") {
		return artifactpb.FileType_FILE_TYPE_TEXT
	} else if strings.HasSuffix(fileName, ".doc") {
		return artifactpb.FileType_FILE_TYPE_DOC
	} else if strings.HasSuffix(fileName, ".docx") {
		return artifactpb.FileType_FILE_TYPE_DOCX
	} else if strings.HasSuffix(fileName, ".html") {
		return artifactpb.FileType_FILE_TYPE_HTML
	} else if strings.HasSuffix(fileName, ".ppt") {
		return artifactpb.FileType_FILE_TYPE_PPT
	} else if strings.HasSuffix(fileName, ".pptx") {
		return artifactpb.FileType_FILE_TYPE_PPTX
	} else if strings.HasSuffix(fileName, ".xlsx") {
		return artifactpb.FileType_FILE_TYPE_XLSX
	}
	return artifactpb.FileType_FILE_TYPE_UNSPECIFIED
}
