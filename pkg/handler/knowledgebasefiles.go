package handler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/minio"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"
	"github.com/instill-ai/x/resource"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	constantx "github.com/instill-ai/x/constant"
	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
)

// UploadCatalogFile adds a file to a catalog.
func (ph *PublicHandler) UploadCatalogFile(ctx context.Context, req *artifactpb.UploadCatalogFileRequest) (*artifactpb.UploadCatalogFileResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		err := fmt.Errorf("failed to get user id from header: %v. err: %w", err, errorsx.ErrUnauthenticated)
		return nil, err
	}

	hasObject, err := checkUploadKnowledgeBaseFileRequest(req)
	if err != nil {
		return nil, err
	}

	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		logger.Error(
			"failed to get namespace ",
			zap.Error(err),
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
			zap.String("auth_uid", authUID))
		return nil, fmt.Errorf("failed to get namespace. err: %w", err)
	}
	// ACL - check user's permission to write catalog
	kb, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, req.CatalogId)
	if err != nil {
		logger.Error("failed to get catalog", zap.Error(err))
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}
	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", kb.UID, "writer")
	if err != nil {
		logger.Error("failed to check permission", zap.Error(err))
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err)
	}
	if !granted {
		return nil, fmt.Errorf("%w: no permission over catalog", errorsx.ErrUnauthorized)
	}

	// get all kbs in the namespace
	kbs, err := ph.service.Repository().ListKnowledgeBases(ctx, ns.NsUID.String())
	if err != nil {
		logger.Error("failed to list catalog", zap.Error(err))
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}
	totalUsageInNamespace := int64(0)
	for _, kb := range kbs {
		totalUsageInNamespace += kb.Usage
	}

	// Because file processing is done by the worker, which pulls records from
	// the database, we need to use the file ExternalMetadata field in the file
	// to propagate the context.
	//
	// This field will be used internally and should be transparent to API
	// users.
	md, err := appendRequestMetadata(ctx, req.GetFile().GetExternalMetadata())
	if err != nil {
		return nil, fmt.Errorf("appending request metadata to context: %w", err)
	}

	// upload file to minio and database
	kbFile := repository.KnowledgeBaseFile{
		Name:                      req.GetFile().GetName(),
		Type:                      req.File.Type.String(),
		Owner:                     ns.NsUID,
		KnowledgeBaseUID:          kb.UID,
		ProcessStatus:             artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_NOTSTARTED.String(),
		ExternalMetadataUnmarshal: md,
	}

	if req.GetFile().GetConvertingPipeline() != "" {
		// TODO jvallesm: validate existence, permissions & recipe of provided
		// pipeline.
		if _, err := service.PipelineReleaseFromName(req.GetFile().GetConvertingPipeline()); err != nil {
			err = fmt.Errorf("%w: invalid conversion pipeline format: %w", errorsx.ErrInvalidArgument, err)
			return nil, errorsx.AddMessage(
				err,
				`Conversion pipeline must have the format "{namespaceID}/{pipelineID}@{version}"`,
			)
		}

		kbFile.ExtraMetaDataUnmarshal = &repository.ExtraMetaData{
			ConvertingPipe: req.GetFile().GetConvertingPipeline(),
		}
	}

	if !hasObject {
		// check file name length based on character count
		if len(req.File.Name) > 255 {
			return nil, fmt.Errorf("file name is too long. max length is 255. name: %s err: %w",
				req.File.Name, errorsx.ErrInvalidArgument)
		}
		// determine the file type by its extension
		req.File.Type = determineFileType(req.File.Name)
		if req.File.Type == artifactpb.FileType_FILE_TYPE_UNSPECIFIED {
			return nil, fmt.Errorf("%w: unsupported file extension", errorsx.ErrInvalidArgument)
		}

		if strings.Contains(req.File.Name, "/") {
			return nil, fmt.Errorf("%w: file name cannot contain slashes ('/')", errorsx.ErrInvalidArgument)
		}

		creatorUID, err := uuid.FromString(authUID)
		if err != nil {
			return nil, fmt.Errorf("parsing creator UID: %w", err)
		}

		// upload the file to MinIO and create the object in the object table
		objectUID, err := ph.uploadBase64FileToMinIO(ctx, ns.NsID, ns.NsUID, creatorUID, req.File.Name, req.File.Content, req.File.Type)
		if err != nil {
			return nil, fmt.Errorf("fetching upload URL: %w", err)
		}
		destination := minio.GetBlobObjectPath(ns.NsUID, objectUID)

		kbFile.CreatorUID = creatorUID
		kbFile.Destination = destination

		fileSize, _ := getFileSize(req.File.Content)
		kbFile.Size = fileSize
	} else {
		object, err := ph.service.Repository().GetObjectByUID(ctx, uuid.FromStringOrNil(req.GetFile().GetObjectUid()))
		if err != nil {
			logger.Error("failed to get catalog object with provided UID", zap.Error(err))
			return nil, err
		}

		if !object.IsUploaded {
			if !strings.HasPrefix(object.Destination, "ns-") {
				return nil, fmt.Errorf("file has not been uploaded yet")
			}

			// check if file exists in minio
			_, err := ph.service.MinIO().GetFile(ctx, minio.BlobBucketName, object.Destination)
			if err != nil {
				logger.Error("failed to get file from minio", zap.Error(err))
				return nil, err
			}
			object.IsUploaded = true
		}

		kbFile.Name = object.Name
		kbFile.CreatorUID = object.CreatorUID
		kbFile.Destination = object.Destination
		kbFile.Size = object.Size

		req.File.Type = determineFileType(object.Name)
		kbFile.Type = req.File.Type.String()
	}

	maxSizeBytes := service.MaxUploadFileSizeMB << 10 << 10
	if kbFile.Size > maxSizeBytes {
		err := fmt.Errorf("%w: max file size exceeded", errorsx.ErrInvalidArgument)
		msg := fmt.Sprintf("Uploaded files can not exceed %d MB.", service.MaxUploadFileSizeMB)
		return nil, errorsx.AddMessage(err, msg)
	}

	// sanitize tags
	tags := req.GetFile().GetTags()
	for _, tag := range tags {
		// skip empty tags
		if len(tag) == 0 {
			continue
		}
	}

	if len(tags) > 0 {
		// Check if the knowledge base supports tags
		hasTags, err := ph.service.VectorDB().CheckTagsMetadata(ctx, kb.UID)
		if err != nil {
			return nil, fmt.Errorf("checking tags metadata: %w", err)
		}

		if !hasTags {
			err := fmt.Errorf("%w: tags are not supported for this knowledge base", errorsx.ErrInvalidArgument)
			return nil, errorsx.AddMessage(err, "Legacy catalogs don't support tags")
		}

		kbFile.Tags = tags
	}

	// create catalog file in database
	res, err := ph.service.Repository().CreateKnowledgeBaseFile(ctx, kbFile, nil)
	if err != nil {
		return nil, fmt.Errorf("creating catalog file: %w", err)
	}

	// increase catalog usage. need to increase after the file is created.
	// TODO: increase the usage in transaction with creating the file.
	err = ph.service.Repository().IncreaseKnowledgeBaseUsage(ctx, nil, kb.UID.String(), int(kbFile.Size))
	if err != nil {
		return nil, fmt.Errorf("increasing catalog usage: %w", err)
	}

	return &artifactpb.UploadCatalogFileResponse{
		File: &artifactpb.File{
			FileUid:            res.UID.String(),
			OwnerUid:           res.Owner.String(),
			CreatorUid:         res.CreatorUID.String(),
			CatalogUid:         res.KnowledgeBaseUID.String(),
			Name:               res.Name,
			Type:               req.File.Type,
			CreateTime:         timestamppb.New(*res.CreateTime),
			UpdateTime:         timestamppb.New(*res.UpdateTime),
			ProcessStatus:      artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_NOTSTARTED,
			Size:               res.Size,
			TotalChunks:        0,
			TotalTokens:        0,
			ExternalMetadata:   res.PublicExternalMetadataUnmarshal(),
			ObjectUid:          req.File.ObjectUid,
			ConvertingPipeline: res.ConvertingPipeline(),
			Tags:               res.Tags,
		},
	}, nil
}

// appendRequestMetadata appends the gRPC metadata present in the context to
// the provided ExternalMetadata under the key constant.MetadataRequestKey.
func appendRequestMetadata(ctx context.Context, externalMetadata *structpb.Struct) (*structpb.Struct, error) {
	if externalMetadata == nil {
		externalMetadata = &structpb.Struct{
			Fields: make(map[string]*structpb.Value, 1),
		}
	}

	md, hasMetadata := metadata.FromIncomingContext(ctx)
	if !hasMetadata {
		return externalMetadata, nil
	}

	// In order to simplify the code translating metadata.MD <->
	// structpb.Struct, JSON marshalling is used. This is less efficient than
	// leveraging the knowledge about the metadata structure (a
	// map[string][]string), but readability has been prioritized.
	j, err := json.Marshal(md)
	if err != nil {
		return nil, fmt.Errorf("marshalling metadata: %w", err)
	}

	mdStruct := new(structpb.Struct)
	if err := mdStruct.UnmarshalJSON(j); err != nil {
		return nil, fmt.Errorf("unmarshalling metadata into struct: %w", err)
	}

	externalMetadata.Fields[constant.MetadataRequestKey] = structpb.NewStructValue(mdStruct)
	return externalMetadata, nil
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

// Check if objectUID is provided, and all other required fields if not
func checkUploadKnowledgeBaseFileRequest(req *artifactpb.UploadCatalogFileRequest) (hasObject bool, _ error) {
	if req.GetNamespaceId() == "" {
		return false, fmt.Errorf("%w: owner UID is required", errorsx.ErrInvalidArgument)
	}

	if req.GetCatalogId() == "" {
		return false, fmt.Errorf("%w: catalog UID is required", errorsx.ErrInvalidArgument)
	}

	if req.GetFile().GetObjectUid() == "" {
		// File upload doesn't reference object, so request must contain the
		// file contents.
		if req.GetFile().GetName() == "" {
			return false, fmt.Errorf("%w: file name is required", errorsx.ErrInvalidArgument)
		}
		if req.GetFile().GetContent() == "" {
			return false, fmt.Errorf("%w: file content is required", errorsx.ErrInvalidArgument)
		}

		return false, nil
	}

	return true, nil
}

// MoveFileToCatalog moves a file from one catalog to another within the same namespace.
// It copies the file content and metadata to the target catalog and deletes
// the file from the source catalog.
func (ph *PublicHandler) MoveFileToCatalog(ctx context.Context, req *artifactpb.MoveFileToCatalogRequest) (*artifactpb.MoveFileToCatalogResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Validate authentication and request parameters
	_, err := getUserUIDFromContext(ctx)
	if err != nil {
		err := fmt.Errorf("failed to get user uid from header: %v. err: %w", err, errorsx.ErrUnauthenticated)
		return nil, err
	}
	if req.FileUid == "" {
		return nil, fmt.Errorf("file uid is required. err: %w", errorsx.ErrInvalidArgument)
	}
	if req.ToCatalogId == "" {
		return nil, fmt.Errorf("to catalog id is required. err: %w", errorsx.ErrInvalidArgument)
	}

	// Step 1: Verify source file exists and check namespace permissions
	sourceFiles, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{uuid.FromStringOrNil(req.FileUid)})
	if err != nil || len(sourceFiles) == 0 {
		logger.Error("file not found", zap.Error(err))
		return nil, fmt.Errorf("file not found. err: %w", errorsx.ErrNotFound)
	}

	sourceFile := sourceFiles[0]
	// Verify namespace exists and get its details
	reqNamespace, err := ph.service.GetNamespaceByNsID(ctx, req.NamespaceId)
	if err != nil {
		logger.Error("failed to get namespace uid from source file", zap.Error(err))
		return nil, fmt.Errorf("failed to get namespace uid from source file. err: %w", err)
	}
	// Ensure file movement occurs within the same namespace
	if reqNamespace.NsUID.String() != sourceFile.Owner.String() {
		return nil, fmt.Errorf("source file is not in the same namespace. err: %w", errorsx.ErrInvalidArgument)
	}

	// Step 2: Verify target catalog exists
	targetCatalog, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, reqNamespace.NsUID, req.ToCatalogId)
	if err != nil {
		logger.Error("target catalog not found", zap.Error(err))
		return nil, fmt.Errorf("target catalog not found. err: %w", err)
	}

	// Step 3: Retrieve file content from MinIO storage
	fileContent, err := ph.service.MinIO().GetFile(ctx, config.Config.Minio.BucketName, sourceFile.Destination)
	if err != nil {
		logger.Error("failed to get file content from MinIO", zap.Error(err))
		return nil, fmt.Errorf("failed to get file content from MinIO. err: %w", err)
	}

	// Prepare file content and metadata for upload
	fileContentBase64 := base64.StdEncoding.EncodeToString(fileContent)
	fileType := artifactpb.FileType(artifactpb.FileType_value[sourceFile.Type])
	externalMetadata := sourceFile.PublicExternalMetadataUnmarshal()

	// Step 4: Create file in target catalog
	uploadReq := &artifactpb.UploadCatalogFileRequest{
		NamespaceId: req.NamespaceId,
		CatalogId:   targetCatalog.KbID,
		File: &artifactpb.File{
			Name:             sourceFile.Name,
			Content:          fileContentBase64,
			Type:             fileType,
			ExternalMetadata: externalMetadata,
			Tags:             sourceFile.Tags,
		},
	}

	uploadResp, err := ph.UploadCatalogFile(ctx, uploadReq)
	if err != nil {
		logger.Error("failed to upload file to target catalog", zap.Error(err))
		return nil, fmt.Errorf("failed to upload file to target catalog. err: %w", err)
	}

	// process the file
	processReq := &artifactpb.ProcessCatalogFilesRequest{
		FileUids: []string{uploadResp.File.FileUid},
	}
	_, err = ph.ProcessCatalogFiles(ctx, processReq)
	if err != nil {
		logger.Error("failed to process file", zap.Error(err))
		return nil, fmt.Errorf("failed to process file. err: %w", err)
	}

	// Step 5: delete the source file
	deleteReq := &artifactpb.DeleteCatalogFileRequest{
		FileUid: sourceFile.UID.String(),
	}
	_, err = ph.DeleteCatalogFile(ctx, deleteReq)
	if err != nil {
		logger.Error("failed to delete file from original catalog",
			zap.String("file_uid", sourceFile.UID.String()),
			zap.Error(err))
	}

	// Return the UID of the newly created file
	return &artifactpb.MoveFileToCatalogResponse{
		FileUid: uploadResp.File.FileUid,
	}, nil
}

// ListCatalogFiles lists the files in a catalog
func (ph *PublicHandler) ListCatalogFiles(ctx context.Context, req *artifactpb.ListCatalogFilesRequest) (*artifactpb.ListCatalogFilesResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		logger.Error("failed to get user id from header", zap.Error(err))
		err := fmt.Errorf("failed to get user id from header: %v. err: %w", err, errorsx.ErrUnauthenticated)
		return nil, err
	}

	// ACL - check if the creator can list files in this catalog. ACL using uid to check the certain namespace resource.
	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		logger.Error(
			"failed to get namespace ",
			zap.Error(err),
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
			zap.String("auth_uid", authUID))
		return nil, fmt.Errorf("failed to get namespace. err: %w", err)
	}
	// ACL - check user's permission to write catalog
	kb, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, req.CatalogId)
	if err != nil {
		logger.Error("failed to get catalog", zap.Error(err))
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}
	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", kb.UID, "reader")
	if err != nil {
		logger.Error("failed to check permission", zap.Error(err))
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err)
	}
	if !granted {
		return nil, fmt.Errorf("%w: no permission over catalog", errorsx.ErrUnauthorized)
	}

	kbFileList, err := ph.service.Repository().ListKnowledgeBaseFiles(ctx, repository.KnowledgeBaseFileListParams{
		OwnerUID:      ns.NsUID.String(),
		KbUID:         kb.UID.String(),
		PageSize:      int(req.GetPageSize()),
		PageToken:     req.GetPageToken(),
		FileUIDs:      req.GetFilter().GetFileUids(),
		ProcessStatus: req.GetFilter().GetProcessStatus(),
	})
	if err != nil {
		return nil, fmt.Errorf("fetching file list: %w", err)
	}

	// Get the tokens and chunks using the source table and source UID.
	sources, err := ph.service.Repository().GetSourceTableAndUIDByFileUIDs(ctx, kbFileList.Files)
	if err != nil {
		return nil, fmt.Errorf("fetching sources: %w", err)
	}

	totalTokens, err := ph.service.Repository().GetFilesTotalTokens(ctx, sources)
	if err != nil {
		return nil, fmt.Errorf("fetching tokens: %w", err)
	}

	totalChunks, err := ph.service.Repository().GetTotalChunksBySources(ctx, sources)
	if err != nil {
		return nil, fmt.Errorf("fetching chunks: %w", err)
	}

	files := make([]*artifactpb.File, 0, len(kbFileList.Files))
	for _, kbFile := range kbFileList.Files {
		objectUID := uuid.FromStringOrNil(strings.TrimPrefix(strings.Split(kbFile.Destination, "/")[1], "obj-"))

		// Runtime migration for legacy files: files uploaded before the new object-based flow
		// were stored in the "uploaded-file" folder.
		// This migration:
		// 1. Downloads the file from the old location
		// 2. Re-uploads it using the new object-based flow
		// 3. Updates the catalog file destination to reference the new object
		// This ensures consistent data structure across both upload flows.
		// This runtime migration will happen only once for each file.
		//
		// TODO: this is just a temporary solution, our Console need to
		// adopt the new flow. So the old flow can be deprecated and
		// removed.
		if strings.Split(kbFile.Destination, "/")[1] == "uploaded-file" {
			fileName := strings.Split(kbFile.Destination, "/")[2]

			content, err := ph.service.MinIO().GetFile(ctx, config.Config.Minio.BucketName, kbFile.Destination)
			if err != nil {
				return nil, fmt.Errorf("fetching file blob: %w", err)
			}
			contentBase64 := base64.StdEncoding.EncodeToString(content)
			fileType := artifactpb.FileType(artifactpb.FileType_value[kbFile.Type])

			objectUID, err = ph.uploadBase64FileToMinIO(ctx, ns.NsID, ns.NsUID, ns.NsUID, fileName, contentBase64, fileType)
			if err != nil {
				return nil, fmt.Errorf("uploading migrated file to MinIO: %w", err)
			}

			newDestination := minio.GetBlobObjectPath(ns.NsUID, objectUID)
			fmt.Println("newDestination", newDestination)
			_, err = ph.service.Repository().UpdateKnowledgeBaseFile(ctx, kbFile.UID.String(), map[string]any{
				repository.KnowledgeBaseFileColumn.Destination: newDestination,
			})
			if err != nil {
				return nil, fmt.Errorf("updating migrated object: %w", err)
			}

		}

		downloadURL := ""
		response, err := ph.service.GetDownloadURL(ctx, &artifactpb.GetObjectDownloadURLRequest{
			NamespaceId: ns.NsID,
			ObjectUid:   objectUID.String(),
		}, ns.NsUID, ns.NsID)
		if err == nil {
			downloadURL = response.GetDownloadUrl()
		}

		file := &artifactpb.File{
			FileUid:            kbFile.UID.String(),
			OwnerUid:           kbFile.Owner.String(),
			CreatorUid:         kbFile.CreatorUID.String(),
			CatalogUid:         kbFile.KnowledgeBaseUID.String(),
			Name:               kbFile.Name,
			Type:               artifactpb.FileType(artifactpb.FileType_value[kbFile.Type]),
			CreateTime:         timestamppb.New(*kbFile.CreateTime),
			UpdateTime:         timestamppb.New(*kbFile.UpdateTime),
			ProcessStatus:      artifactpb.FileProcessStatus(artifactpb.FileProcessStatus_value[kbFile.ProcessStatus]),
			Size:               kbFile.Size,
			ExternalMetadata:   kbFile.PublicExternalMetadataUnmarshal(),
			TotalChunks:        int32(totalChunks[kbFile.UID]),
			TotalTokens:        int32(totalTokens[kbFile.UID]),
			ObjectUid:          objectUID.String(),
			Summary:            string(kbFile.Summary),
			DownloadUrl:        downloadURL,
			ConvertingPipeline: kbFile.ConvertingPipeline(),
			Tags:               kbFile.Tags,
		}

		if kbFile.ExtraMetaDataUnmarshal != nil && kbFile.ExtraMetaDataUnmarshal.Length != nil {
			fileType := artifactpb.FileType(artifactpb.FileType_value[kbFile.Type])
			file.Length = &artifactpb.File_Position{
				Unit:        getPositionUnit(fileType),
				Coordinates: kbFile.ExtraMetaDataUnmarshal.Length,
			}
		}

		files = append(files, file)
	}

	return &artifactpb.ListCatalogFilesResponse{
		Files:         files,
		TotalSize:     int32(kbFileList.TotalCount),
		PageSize:      int32(len(kbFileList.Files)),
		NextPageToken: kbFileList.NextPageToken,
		Filter:        req.Filter,
	}, nil
}

// GetCatalogFile gets a file in a catalog
func (ph *PublicHandler) GetCatalogFile(ctx context.Context, req *artifactpb.GetCatalogFileRequest) (*artifactpb.GetCatalogFileResponse, error) {

	files, err := ph.ListCatalogFiles(ctx, &artifactpb.ListCatalogFilesRequest{
		NamespaceId: req.NamespaceId,
		CatalogId:   req.CatalogId,
		PageSize:    1,
		PageToken:   "",
		Filter: &artifactpb.ListCatalogFilesFilter{
			FileUids: []string{req.FileUid},
		},
	})
	if err != nil {
		return nil, err
	}
	if len(files.Files) == 0 {
		return nil, fmt.Errorf("file not found. err: %w", errorsx.ErrNotFound)
	}

	return &artifactpb.GetCatalogFileResponse{
		File: files.Files[0],
	}, nil

}

// DeleteCatalogFile deletes a file in a catalog
func (ph *PublicHandler) DeleteCatalogFile(ctx context.Context, req *artifactpb.DeleteCatalogFileRequest) (*artifactpb.DeleteCatalogFileResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Get authenticated user UID for proper context in cleanup operations
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		logger.Error("failed to get user id from header", zap.Error(err))
		return nil, fmt.Errorf("failed to get user id from header: %v. err: %w", err, errorsx.ErrUnauthenticated)
	}

	// ACL - check user's permission to write catalog of kb file
	kbfs, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{uuid.FromStringOrNil(req.FileUid)})
	if err != nil {
		logger.Error("failed to get catalog files", zap.Error(err))
		return nil, fmt.Errorf("failed to get catalog files. err: %w", err)
	} else if len(kbfs) == 0 {
		return nil, fmt.Errorf("file not found. err: %w", errorsx.ErrNotFound)
	}
	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", kbfs[0].KnowledgeBaseUID, "writer")
	if err != nil {
		logger.Error("failed to check permission", zap.Error(err))
		return nil, fmt.Errorf("failed to check permission. err: %w", err)
	}
	if !granted {
		return nil, fmt.Errorf("%w: no permission over catalog", errorsx.ErrUnauthorized)
	}
	// check if file uid is empty
	if req.FileUid == "" {
		return nil, fmt.Errorf("file uid is required. err: %w", errorsx.ErrInvalidArgument)
	}

	fUID, err := uuid.FromString(req.FileUid)
	if err != nil {
		return nil, fmt.Errorf("failed to parse file uid. err: %w", errorsx.ErrInvalidArgument)
	}

	// get the file by uid
	files, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{fUID})
	if err != nil {
		return nil, err
	} else if len(files) == 0 {
		return nil, fmt.Errorf("file not found. err: %w", errorsx.ErrNotFound)
	}

	// Try to trigger cleanup workflow first - if this fails, we don't want to soft-delete
	// the file record as it would leave orphaned resources
	if ph.service.CleanupFileWorkflow() != nil {
		// Generate workflow ID for tracking
		workflowID := uuid.Must(uuid.NewV4()).String()

		param := service.CleanupFileWorkflowParam{
			FileUID:             fUID,
			IncludeOriginalFile: true,
			UserUID:             uuid.FromStringOrNil(authUID),
			WorkflowID:          workflowID,
		}

		err = ph.service.CleanupFileWorkflow().Execute(ctx, param)
		if err != nil {
			logger.Error("Failed to trigger cleanup workflow - aborting file deletion to prevent resource leaks",
				zap.String("fileUID", fUID.String()),
				zap.String("workflowID", workflowID),
				zap.Error(err))
			return nil, fmt.Errorf("failed to start cleanup workflow: %w", err)
		}
		logger.Info("Cleanup workflow triggered successfully",
			zap.String("fileUID", fUID.String()),
			zap.String("workflowID", workflowID))
	} else {
		// Temporal client is nil - perform immediate cleanup to prevent resource leaks
		logger.Warn("Temporal client is nil, performing immediate cleanup",
			zap.String("fileUID", fUID.String()))

		if err := ph.performImmediateCleanup(ctx, files[0]); err != nil {
			logger.Error("Failed to perform immediate cleanup",
				zap.String("fileUID", fUID.String()),
				zap.Error(err))
			return nil, fmt.Errorf("failed to perform cleanup: %w", err)
		}
	}

	// Only soft-delete the file record after cleanup is successfully initiated or completed
	err = ph.service.Repository().DeleteKnowledgeBaseFileAndDecreaseUsage(ctx, fUID)
	if err != nil {
		logger.Error("failed to delete knowledge base file and decrease usage", zap.Error(err))
		return nil, err
	}

	return &artifactpb.DeleteCatalogFileResponse{
		FileUid: fUID.String(),
	}, nil

}

// ProcessCatalogFiles triggers the conversion, chunking, embedding and
// summarizing process for a set of files.
func (ph *PublicHandler) ProcessCatalogFiles(ctx context.Context, req *artifactpb.ProcessCatalogFilesRequest) (*artifactpb.ProcessCatalogFilesResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	// ACL - check if the uid can process file. ACL.
	// check the file's kb_uid and use kb_uid to check if user has write permission
	fileUUIDs := make([]uuid.UUID, 0, len(req.FileUids))
	for _, fileUID := range req.FileUids {
		fUID, err := uuid.FromString(fileUID)
		if err != nil {
			return nil, fmt.Errorf("failed to parse file uid. err: %w", errorsx.ErrInvalidArgument)
		}
		fileUUIDs = append(fileUUIDs, fUID)
	}
	kbfs, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, fileUUIDs)
	if err != nil {
		return nil, err
	} else if len(kbfs) == 0 {
		return nil, fmt.Errorf("file not found. err: %w", errorsx.ErrNotFound)
	}
	// check write permission for the catalog
	for _, kbf := range kbfs {
		granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", kbf.KnowledgeBaseUID, "writer")
		if err != nil {
			return nil, err
		}
		if !granted {
			return nil, fmt.Errorf("%w: no permission over catalog", errorsx.ErrUnauthorized)
		}
	}

	// check auth user has access to the requester
	err = ph.service.ACLClient().CheckRequesterPermission(ctx)
	if err != nil {
		logger.Error("failed to check requester permission", zap.Error(err))
		return nil, fmt.Errorf("failed to check requester permission. err: %w", err)
	}

	requesterUID := resource.GetRequestSingleHeader(ctx, constantx.HeaderRequesterUIDKey)
	requesterUUID := uuid.FromStringOrNil(requesterUID)

	files, err := ph.service.Repository().ProcessKnowledgeBaseFiles(ctx, req.FileUids, requesterUUID)
	if err != nil {
		return nil, err
	}

	// Trigger Temporal workflows for each file
	if ph.service.ProcessFileWorkflow() != nil {
		for _, file := range files {
			param := service.ProcessFileWorkflowParam{
				FileUID:          file.UID,
				KnowledgeBaseUID: file.KnowledgeBaseUID,
				UserUID:          file.Owner,
				RequesterUID:     file.RequesterUID,
			}

			err := ph.service.ProcessFileWorkflow().Execute(ctx, param)
			if err != nil {
				logger.Error("Failed to start file processing workflow",
					zap.String("fileUID", file.UID.String()),
					zap.Error(err))
				// Continue processing other files even if one fails
			}
		}
	}

	// populate the files into response
	var resFiles []*artifactpb.File
	for _, file := range files {

		objectUID := uuid.FromStringOrNil(strings.TrimPrefix(strings.Split(file.Destination, "/")[1], "obj-"))

		resFiles = append(resFiles, &artifactpb.File{
			FileUid:            file.UID.String(),
			OwnerUid:           file.Owner.String(),
			CreatorUid:         file.CreatorUID.String(),
			CatalogUid:         file.KnowledgeBaseUID.String(),
			Name:               file.Name,
			Type:               artifactpb.FileType(artifactpb.FileType_value[file.Type]),
			CreateTime:         timestamppb.New(*file.CreateTime),
			UpdateTime:         timestamppb.New(*file.UpdateTime),
			ProcessStatus:      artifactpb.FileProcessStatus(artifactpb.FileProcessStatus_value[file.ProcessStatus]),
			ObjectUid:          objectUID.String(),
			ConvertingPipeline: file.ConvertingPipeline(),
			Tags:               file.Tags,
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
	case artifactpb.FileType_FILE_TYPE_XLS:
		return "application/vnd.ms-excel"
	case artifactpb.FileType_FILE_TYPE_CSV:
		return "text/csv"
	default:
		return "application/octet-stream"
	}
}

func determineFileType(fileName string) artifactpb.FileType {
	fileNameLower := strings.ToLower(fileName)
	if strings.HasSuffix(fileNameLower, ".pdf") {
		return artifactpb.FileType_FILE_TYPE_PDF
	} else if strings.HasSuffix(fileNameLower, ".md") {
		return artifactpb.FileType_FILE_TYPE_MARKDOWN
	} else if strings.HasSuffix(fileNameLower, ".txt") {
		return artifactpb.FileType_FILE_TYPE_TEXT
	} else if strings.HasSuffix(fileNameLower, ".doc") {
		return artifactpb.FileType_FILE_TYPE_DOC
	} else if strings.HasSuffix(fileNameLower, ".docx") {
		return artifactpb.FileType_FILE_TYPE_DOCX
	} else if strings.HasSuffix(fileNameLower, ".html") {
		return artifactpb.FileType_FILE_TYPE_HTML
	} else if strings.HasSuffix(fileNameLower, ".ppt") {
		return artifactpb.FileType_FILE_TYPE_PPT
	} else if strings.HasSuffix(fileNameLower, ".pptx") {
		return artifactpb.FileType_FILE_TYPE_PPTX
	} else if strings.HasSuffix(fileNameLower, ".xlsx") {
		return artifactpb.FileType_FILE_TYPE_XLSX
	} else if strings.HasSuffix(fileNameLower, ".xls") {
		return artifactpb.FileType_FILE_TYPE_XLS
	} else if strings.HasSuffix(fileNameLower, ".csv") {
		return artifactpb.FileType_FILE_TYPE_CSV
	}
	return artifactpb.FileType_FILE_TYPE_UNSPECIFIED
}

// getPositionUnit returns the appropriate unit for file position based on file type
func getPositionUnit(fileType artifactpb.FileType) artifactpb.File_Position_Unit {
	switch fileType {
	case artifactpb.FileType_FILE_TYPE_TEXT,
		artifactpb.FileType_FILE_TYPE_MARKDOWN,
		artifactpb.FileType_FILE_TYPE_HTML,
		artifactpb.FileType_FILE_TYPE_CSV:
		return artifactpb.File_Position_UNIT_CHARACTER
	case artifactpb.FileType_FILE_TYPE_PDF,
		artifactpb.FileType_FILE_TYPE_DOCX,
		artifactpb.FileType_FILE_TYPE_DOC,
		artifactpb.FileType_FILE_TYPE_PPT,
		artifactpb.FileType_FILE_TYPE_PPTX,
		artifactpb.FileType_FILE_TYPE_XLSX,
		artifactpb.FileType_FILE_TYPE_XLS:
		return artifactpb.File_Position_UNIT_PAGE
	}

	return artifactpb.File_Position_UNIT_UNSPECIFIED
}

// GetFileSummary returns the summary of the file
func (ph *PublicHandler) GetFileSummary(ctx context.Context, req *artifactpb.GetFileSummaryRequest) (*artifactpb.GetFileSummaryResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	_, err := getUserUIDFromContext(ctx)
	if err != nil {
		logger.Error("failed to get user id from header", zap.Error(err))
		return nil, fmt.Errorf("failed to get user id from header: %v. err: %w", err, errorsx.ErrUnauthenticated)
	}

	// Check if user can access the namespace
	_, err = ph.service.GetNamespaceAndCheckPermission(ctx, req.NamespaceId)
	if err != nil {
		logger.Error("failed to get namespace and check permission", zap.Error(err))
		return nil, fmt.Errorf("failed to get namespace and check permission: %w", err)
	}

	kbFiles, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{uuid.FromStringOrNil(req.FileUid)}, repository.KnowledgeBaseFileColumn.Summary)
	if err != nil || len(kbFiles) == 0 {
		logger.Error("file not found", zap.Error(err))
		return nil, fmt.Errorf("file not found. err: %w", errorsx.ErrNotFound)
	}

	return &artifactpb.GetFileSummaryResponse{
		Summary: string(kbFiles[0].Summary),
	}, nil
}

// uploadBase64FileToMinIO uploads a base64-encoded file to MinIO and updates the object in the database.
//
// This function bridges the legacy upload flow with the new upload flow:
// - Legacy flow: Users upload files directly without using the getUploadUrl API, bypassing object creation in the object table
// - New flow: Users first get an upload URL, create an object in the object table, then bind the catalog file to this object
//
// This middleware enables legacy uploads to maintain compatibility with the new data structure by:
// 1. Using the getUploadUrl API to obtain object UID and destination
// 2. Uploading the base64 file to MinIO
// 3. Creating the object record in the database
//
// This ensures both flows result in the same consistent data structure.
func (ph *PublicHandler) uploadBase64FileToMinIO(ctx context.Context, nsID string, nsUID, creatorUID uuid.UUID, fileName string, content string, fileType artifactpb.FileType) (uuid.UUID, error) {
	logger, _ := logx.GetZapLogger(ctx)
	response, err := ph.service.GetUploadURL(ctx, &artifactpb.GetObjectUploadURLRequest{
		NamespaceId: nsID,
		ObjectName:  fileName,
	}, nsUID, fileName, creatorUID)
	if err != nil {
		logger.Error("failed to get upload URL", zap.Error(err))
		return uuid.Nil, fmt.Errorf("failed to get upload URL. err: %w", err)
	}
	objectUID := uuid.FromStringOrNil(response.Object.Uid)
	destination := minio.GetBlobObjectPath(nsUID, objectUID)
	err = ph.service.MinIO().UploadBase64File(ctx, minio.BlobBucketName, destination, content, fileTypeConvertToMime(fileType))
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to upload file to MinIO. err: %w", err)
	}
	decodedContent, err := base64.StdEncoding.DecodeString(content)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to decode file content. err: %w", err)
	}
	objectSize := int64(len(decodedContent))

	object, err := ph.service.Repository().GetObjectByUID(ctx, objectUID)
	if err != nil {
		logger.Error("failed to get object by uid", zap.Error(err))
		return uuid.Nil, fmt.Errorf("failed to get object by uid. err: %w", err)
	}
	object.Size = objectSize
	object.IsUploaded = true

	_, err = ph.service.Repository().UpdateObject(ctx, *object)
	if err != nil {
		logger.Error("failed to update object", zap.Error(err))
		return uuid.Nil, fmt.Errorf("failed to update object. err: %w", err)
	}
	return objectUID, nil
}

// UpdateCatalogFileTags updates the tags for a catalog file
func (ph *PublicHandler) UpdateCatalogFileTags(ctx context.Context, req *artifactpb.UpdateCatalogFileTagsRequest) (*artifactpb.UpdateCatalogFileTagsResponse, error) {
	// Call the service layer to update the tags
	// The service layer handles all validation, permission checks, and business logic
	return ph.service.UpdateCatalogFileTags(ctx, req)
}

// performImmediateCleanup performs immediate cleanup of file resources when Temporal is unavailable
// This serves as a fallback mechanism to prevent resource leaks
func (ph *PublicHandler) performImmediateCleanup(ctx context.Context, file repository.KnowledgeBaseFile) error {
	logger, _ := logx.GetZapLogger(ctx)
	fileUID := file.UID
	kbUID := file.KnowledgeBaseUID

	logger.Info("Starting immediate cleanup of file resources",
		zap.String("fileUID", fileUID.String()),
		zap.String("kbUID", kbUID.String()))

	var cleanupErrors []string

	// 1. Cleanup original file from MinIO
	if file.Destination != "" {
		err := ph.service.MinIO().DeleteFile(ctx, config.Config.Minio.BucketName, file.Destination)
		if err != nil {
			logger.Error("Failed to delete original file from MinIO",
				zap.String("fileUID", fileUID.String()),
				zap.String("destination", file.Destination),
				zap.Error(err))
			cleanupErrors = append(cleanupErrors, fmt.Sprintf("original file: %v", err))
		} else {
			logger.Info("Deleted original file from MinIO",
				zap.String("fileUID", fileUID.String()),
				zap.String("destination", file.Destination))
		}
	}

	// 2. Cleanup converted files if they exist
	convertedFile, err := ph.service.Repository().GetConvertedFileByFileUID(ctx, fileUID)
	if err == nil && convertedFile != nil {
		err = ph.service.DeleteConvertedFileByFileUID(ctx, kbUID, fileUID)
		if err != nil {
			logger.Error("Failed to delete converted file from MinIO",
				zap.String("fileUID", fileUID.String()),
				zap.Error(err))
			cleanupErrors = append(cleanupErrors, fmt.Sprintf("converted file: %v", err))
		} else {
			logger.Info("Deleted converted file from MinIO",
				zap.String("fileUID", fileUID.String()))
		}

		// Delete the converted file record from database
		err = ph.service.Repository().HardDeleteConvertedFileByFileUID(ctx, fileUID)
		if err != nil {
			logger.Error("Failed to delete converted file record",
				zap.String("fileUID", fileUID.String()),
				zap.Error(err))
			cleanupErrors = append(cleanupErrors, fmt.Sprintf("converted file record: %v", err))
		}
	}

	// 3. Cleanup text chunks if they exist
	chunks, err := ph.service.Repository().ListChunksByKbFileUID(ctx, fileUID)
	if err == nil && len(chunks) > 0 {
		err = ph.service.DeleteTextChunksByFileUID(ctx, kbUID, fileUID)
		if err != nil {
			logger.Error("Failed to delete chunks from MinIO",
				zap.String("fileUID", fileUID.String()),
				zap.Int("chunkCount", len(chunks)),
				zap.Error(err))
			cleanupErrors = append(cleanupErrors, fmt.Sprintf("chunks: %v", err))
		} else {
			logger.Info("Deleted chunks from MinIO",
				zap.String("fileUID", fileUID.String()),
				zap.Int("chunkCount", len(chunks)))
		}

		// Delete chunk records from database
		err = ph.service.Repository().HardDeleteChunksByKbFileUID(ctx, fileUID)
		if err != nil {
			logger.Error("Failed to delete chunk records",
				zap.String("fileUID", fileUID.String()),
				zap.Error(err))
			cleanupErrors = append(cleanupErrors, fmt.Sprintf("chunk records: %v", err))
		}
	}

	// 4. Cleanup embeddings if they exist
	// This also cleans up any orphaned embeddings in Milvus that might not have database records
	collection := service.KBCollectionName(kbUID)
	err = ph.service.VectorDB().DeleteEmbeddingsWithFileUID(ctx, collection, fileUID)
	if err != nil {
		logger.Error("Failed to delete embeddings from Milvus",
			zap.String("fileUID", fileUID.String()),
			zap.Error(err))
		cleanupErrors = append(cleanupErrors, fmt.Sprintf("embeddings in Milvus: %v", err))
	} else {
		logger.Info("Deleted embeddings from Milvus by fileUID",
			zap.String("fileUID", fileUID.String()))
	}

	// Delete embedding records from database
	err = ph.service.Repository().HardDeleteEmbeddingsByKbFileUID(ctx, fileUID)
	if err != nil {
		logger.Error("Failed to delete embedding records",
			zap.String("fileUID", fileUID.String()),
			zap.Error(err))
		cleanupErrors = append(cleanupErrors, fmt.Sprintf("embedding records: %v", err))
	}

	// Log final results
	if len(cleanupErrors) > 0 {
		logger.Warn("Immediate cleanup completed with some errors",
			zap.String("fileUID", fileUID.String()),
			zap.Strings("errors", cleanupErrors))
		// Return error to prevent file deletion if cleanup failed
		return fmt.Errorf("cleanup failed with errors: %v", cleanupErrors)
	}

	logger.Info("Immediate cleanup completed successfully",
		zap.String("fileUID", fileUID.String()))
	return nil
}
