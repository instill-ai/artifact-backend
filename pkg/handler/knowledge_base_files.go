package handler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/gofrs/uuid"

	"github.com/instill-ai/artifact-backend/pkg/types"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/pipeline"
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
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get namespace. err: %w", err),
			"Unable to access the specified namespace. Please check the namespace ID and try again.",
		)
	}
	// ACL - check user's permission to write catalog
	kb, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, req.CatalogId)
	if err != nil {
		logger.Error("failed to get catalog", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf(ErrorListKnowledgeBasesMsg, err),
			"Unable to access the specified catalog. Please check the catalog ID and try again.",
		)
	}
	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", kb.UID, "writer")
	if err != nil {
		logger.Error("failed to check permission", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err),
			"Unable to verify access permissions. Please try again.",
		)
	}
	if !granted {
		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: no permission over catalog", errorsx.ErrUnauthorized),
			"You don't have permission to upload files to this catalog. Please contact the owner for access.",
		)
	}

	// get all kbs in the namespace
	kbs, err := ph.service.Repository().ListKnowledgeBases(ctx, ns.NsUID.String())
	if err != nil {
		logger.Error("failed to list catalog", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf(ErrorListKnowledgeBasesMsg, err),
			"Unable to retrieve catalog information. Please try again.",
		)
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
		return nil, errorsx.AddMessage(
			fmt.Errorf("appending request metadata to context: %w", err),
			"Unable to process request. Please try again.",
		)
	}

	// upload file to minio and database
	// Inherit RAG version from parent knowledge base
	kbFile := repository.KnowledgeBaseFileModel{
		Name:                      req.GetFile().GetName(),
		FileType:                  req.File.Type.String(),
		Owner:                     ns.NsUID,
		KBUID:                     kb.UID,
		ProcessStatus:             artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_NOTSTARTED.String(),
		ExternalMetadataUnmarshal: md,
	}

	if req.GetFile().GetConvertingPipeline() != "" {
		// TODO jvallesm: validate existence, permissions & recipe of provided
		// pipeline.
		if _, err := pipeline.PipelineReleaseFromName(req.GetFile().GetConvertingPipeline()); err != nil {
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
			return nil, errorsx.AddMessage(
				fmt.Errorf("file name is too long. max length is 255. name: %s err: %w",
					req.File.Name, errorsx.ErrInvalidArgument),
				"File name is too long. Please use a name with 255 characters or less.",
			)
		}
		// determine the file type by its extension
		req.File.Type = determineFileType(req.File.Name)
		if req.File.Type == artifactpb.File_TYPE_UNSPECIFIED {
			return nil, errorsx.AddMessage(
				fmt.Errorf("%w: unsupported file extension", errorsx.ErrInvalidArgument),
				"Unsupported file type. Please upload a supported file format.",
			)
		}

		if strings.Contains(req.File.Name, "/") {
			return nil, errorsx.AddMessage(
				fmt.Errorf("%w: file name cannot contain slashes ('/')", errorsx.ErrInvalidArgument),
				"File name cannot contain slashes ('/'). Please rename the file and try again.",
			)
		}

		creatorUID, err := uuid.FromString(authUID)
		if err != nil {
			return nil, errorsx.AddMessage(
				fmt.Errorf("parsing creator UID: %w", err),
				"Invalid user session. Please log in again and try again.",
			)
		}

		// upload the file to MinIO and create the object in the object table
		objectUID, err := ph.uploadBase64FileToMinIO(ctx, ns.NsID, ns.NsUID, creatorUID, req.File.Name, req.File.Content, req.File.Type)
		if err != nil {
			return nil, errorsx.AddMessage(
				fmt.Errorf("fetching upload URL: %w", err),
				"Unable to upload file. Please check your network connection and try again.",
			)
		}
		destination := repository.GetBlobObjectPath(ns.NsUID, objectUID)

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
				return nil, errorsx.AddMessage(
					fmt.Errorf("file has not been uploaded yet"),
					"File upload is not complete. Please wait for the upload to finish and try again.",
				)
			}

			// check if file exists in minio
			_, err := ph.service.Repository().GetFile(ctx, repository.BlobBucketName, object.Destination)
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
		kbFile.FileType = req.File.Type.String()
	}

	maxSizeBytes := service.MaxUploadFileSizeMB << 10 << 10
	if kbFile.Size > maxSizeBytes {
		err := fmt.Errorf("%w: max file size exceeded", errorsx.ErrInvalidArgument)
		msg := fmt.Sprintf("Uploaded files can not exceed %d MB.", service.MaxUploadFileSizeMB)
		return nil, errorsx.AddMessage(err, msg)
	}

	// create catalog file in database
	res, err := ph.service.Repository().CreateKnowledgeBaseFile(ctx, kbFile, nil)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("creating catalog file: %w", err),
			"Unable to add file to catalog. Please try again.",
		)
	}

	// increase catalog usage. need to increase after the file is created.
	// TODO: increase the usage in transaction with creating the file.
	err = ph.service.Repository().IncreaseKnowledgeBaseUsage(ctx, nil, kb.UID.String(), int(kbFile.Size))
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("increasing catalog usage: %w", err),
			"File uploaded but catalog statistics update failed. The file is available for use.",
		)
	}

	// DUAL PROCESSING: Check if dual processing is needed
	// Dual processing is needed in three scenarios:
	// 1. Phase 2 (updating): Staging KB exists - full dual processing with new config
	// 2. Phase 3 (swapping): Staging KB exists - file synchronization for consistency
	// 3. Phase 6 (retention): Rollback KB exists - full dual processing with old config
	//
	// This ensures:
	// - Files uploaded during update are available in both production and staging
	// - Files added during swapping are synchronized to both KBs (for clean swap)
	// - Files added during retention period work after rollback (no data loss)
	dualTarget, err := ph.service.Repository().GetDualProcessingTarget(ctx, kb)
	if err != nil {
		// Log error but don't fail upload - fall back to normal processing
		logger.Warn("Failed to check dual processing requirements, falling back to normal processing",
			zap.Error(err),
			zap.String("kbUID", kb.UID.String()))
	} else if dualTarget.IsNeeded {
		logger.Info("Dual processing required for uploaded file",
			zap.String("fileUID", res.UID.String()),
			zap.String("prodKBUID", kb.UID.String()),
			zap.String("targetKBUID", dualTarget.TargetKB.UID.String()),
			zap.String("phase", dualTarget.Phase),
			zap.String("description", dualTarget.Description))

		// Trigger dual processing asynchronously
		// Don't wait for completion - return response immediately
		go func() {
			// Create new context for async operation (detached from request context)
			asyncCtx := context.Background()

			// Copy necessary metadata for authentication/authorization
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				asyncCtx = metadata.NewIncomingContext(asyncCtx, md)
			}

			// Create a duplicate file record for target KB (staging or rollback)
			// This file record will reference the same original file in MinIO
			// but will have different processed outputs (chunks, embeddings)
			targetFile := repository.KnowledgeBaseFileModel{
				Name:                      res.Name,
				FileType:                  res.FileType,
				Owner:                     res.Owner,
				CreatorUID:                res.CreatorUID,
				KBUID:                     dualTarget.TargetKB.UID, // Different KB (staging or rollback)
				Destination:               res.Destination,         // Same source file
				Size:                      res.Size,
				ProcessStatus:             artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_NOTSTARTED.String(),
				ExternalMetadataUnmarshal: res.ExternalMetadataUnmarshal,
				ExtraMetaDataUnmarshal:    res.ExtraMetaDataUnmarshal,
			}

			targetFileRes, err := ph.service.Repository().CreateKnowledgeBaseFile(asyncCtx, targetFile, nil)
			if err != nil {
				logger.Error("Failed to create target file record during dual processing",
					zap.Error(err),
					zap.String("targetKBUID", dualTarget.TargetKB.UID.String()),
					zap.String("phase", dualTarget.Phase))
				return
			}

			// Increase target KB usage
			err = ph.service.Repository().IncreaseKnowledgeBaseUsage(asyncCtx, nil, dualTarget.TargetKB.UID.String(), int(targetFile.Size))
			if err != nil {
				logger.Warn("Failed to increase target KB usage",
					zap.Error(err),
					zap.String("phase", dualTarget.Phase))
				// Non-fatal, continue with processing
			}

			logger.Info("Created target file record, starting dual processing",
				zap.String("prodFileUID", res.UID.String()),
				zap.String("targetFileUID", targetFileRes.UID.String()),
				zap.String("phase", dualTarget.Phase))

			// Trigger dual processing workflows
			// IMPORTANT: Process files in parallel goroutines to avoid blocking
			// Processing behavior depends on phase:
			// - Phase 2 (updating): Full processing for both with respective configs
			// - Phase 3 (swapping): Minimal sync (file record created, limited processing)
			// - Phase 6 (retention): Full processing for both with respective configs

			var wg sync.WaitGroup
			wg.Add(2)

			// Process production file for production KB
			go func() {
				defer wg.Done()
				err := ph.service.ProcessFile(
					asyncCtx,
					kb.UID, // Production KB
					[]types.FileUIDType{res.UID},
					types.UserUIDType(uuid.FromStringOrNil(authUID)),
					types.RequesterUIDType(uuid.FromStringOrNil(authUID)),
				)
				if err != nil {
					logger.Error("Production file processing failed during dual processing",
						zap.Error(err),
						zap.String("prodFileUID", res.UID.String()),
						zap.String("phase", dualTarget.Phase))
				}
			}()

			// Process target file for target KB
			go func() {
				defer wg.Done()
				err := ph.service.ProcessFile(
					asyncCtx,
					dualTarget.TargetKB.UID, // Target KB (staging or rollback)
					[]types.FileUIDType{targetFileRes.UID},
					types.UserUIDType(uuid.FromStringOrNil(authUID)),
					types.RequesterUIDType(uuid.FromStringOrNil(authUID)),
				)
				if err != nil {
					logger.Error("Target file processing failed during dual processing",
						zap.Error(err),
						zap.String("targetFileUID", targetFileRes.UID.String()),
						zap.String("phase", dualTarget.Phase))
				}
			}()

			wg.Wait()

			logger.Info("Dual processing completed successfully",
				zap.String("prodFileUID", res.UID.String()),
				zap.String("targetFileUID", targetFileRes.UID.String()),
				zap.String("phase", dualTarget.Phase))
		}()
	}

	return &artifactpb.UploadCatalogFileResponse{
		File: &artifactpb.File{
			FileUid:            res.UID.String(),
			OwnerUid:           res.Owner.String(),
			CreatorUid:         res.CreatorUID.String(),
			CatalogUid:         res.KBUID.String(),
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
		return nil, errorsx.AddMessage(
			fmt.Errorf("marshalling metadata: %w", err),
			"Unable to process request metadata. Please try again.",
		)
	}

	mdStruct := new(structpb.Struct)
	if err := mdStruct.UnmarshalJSON(j); err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("unmarshalling metadata into struct: %w", err),
			"Unable to process request metadata. Please try again.",
		)
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
		return nil, errorsx.AddMessage(
			fmt.Errorf("file uid is required. err: %w", errorsx.ErrInvalidArgument),
			"File ID is required. Please specify which file to move.",
		)
	}
	if req.ToCatalogId == "" {
		return nil, errorsx.AddMessage(
			fmt.Errorf("to catalog id is required. err: %w", errorsx.ErrInvalidArgument),
			"Target catalog ID is required. Please specify the destination catalog.",
		)
	}

	// Step 1: Verify source file exists and check namespace permissions
	sourceFiles, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []types.FileUIDType{types.FileUIDType(uuid.FromStringOrNil(req.FileUid))})
	if err != nil || len(sourceFiles) == 0 {
		logger.Error("file not found", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("file not found: %w", errorsx.ErrNotFound),
			"File not found. Please check the file ID and try again.",
		)
	}

	sourceFile := sourceFiles[0]
	// Verify namespace exists and get its details
	reqNamespace, err := ph.service.GetNamespaceByNsID(ctx, req.NamespaceId)
	if err != nil {
		logger.Error("failed to get namespace uid from source file", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get namespace: %w", err),
			"Unable to access the specified namespace. Please check the namespace ID and try again.",
		)
	}
	// Ensure file movement occurs within the same namespace
	if reqNamespace.NsUID.String() != sourceFile.Owner.String() {
		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: source file is not in the same namespace", errorsx.ErrInvalidArgument),
			"File can only be moved within the same namespace. Please select a target catalog in the same namespace.",
		)
	}

	// Step 2: Verify target catalog exists
	targetCatalog, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, reqNamespace.NsUID, req.ToCatalogId)
	if err != nil {
		logger.Error("target catalog not found", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("target catalog not found: %w", err),
			"Target catalog not found. Please check the catalog ID and try again.",
		)
	}

	// Step 3: Retrieve file content from MinIO storage
	fileContent, err := ph.service.Repository().GetFile(ctx, config.Config.Minio.BucketName, sourceFile.Destination)
	if err != nil {
		logger.Error("failed to get file content from MinIO", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get file content: %w", err),
			"Unable to retrieve file content. Please try again.",
		)
	}

	// Prepare file content and metadata for upload
	fileContentBase64 := base64.StdEncoding.EncodeToString(fileContent)
	fileType := artifactpb.File_Type(artifactpb.File_Type_value[sourceFile.FileType])
	externalMetadata := sourceFile.PublicExternalMetadataUnmarshal()

	// Step 4: Create file in target catalog
	uploadReq := &artifactpb.UploadCatalogFileRequest{
		NamespaceId: req.NamespaceId,
		CatalogId:   targetCatalog.KBID,
		File: &artifactpb.File{
			Name:             sourceFile.Name,
			Content:          fileContentBase64,
			Type:             fileType,
			ExternalMetadata: externalMetadata,
		},
	}

	uploadResp, err := ph.UploadCatalogFile(ctx, uploadReq)
	if err != nil {
		logger.Error("failed to upload file to target catalog", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to upload file to target catalog: %w", err),
			"Unable to upload file to the target catalog. Please try again.",
		)
	}

	// process the file
	processReq := &artifactpb.ProcessCatalogFilesRequest{
		FileUids: []string{uploadResp.File.FileUid},
	}
	_, err = ph.ProcessCatalogFiles(ctx, processReq)
	if err != nil {
		logger.Error("failed to process file", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to process file: %w", err),
			"File moved but processing failed. The file is in the target catalog but may need to be processed manually.",
		)
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
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get namespace: %w", err),
			"Unable to access the specified namespace. Please check the namespace ID and try again.",
		)
	}
	// ACL - check user's permission to write catalog
	kb, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, req.CatalogId)
	if err != nil {
		logger.Error("failed to get catalog", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf(ErrorListKnowledgeBasesMsg, err),
			"Unable to access the specified catalog. Please check the catalog ID and try again.",
		)
	}
	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", kb.UID, "reader")
	if err != nil {
		logger.Error("failed to check permission", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err),
			"Unable to verify access permissions. Please try again.",
		)
	}
	if !granted {
		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: no permission over catalog", errorsx.ErrUnauthorized),
			"You don't have permission to view this catalog. Please contact the owner for access.",
		)
	}

	kbFileList, err := ph.service.Repository().ListKnowledgeBaseFiles(ctx, repository.KnowledgeBaseFileListParams{
		OwnerUID:      ns.NsUID.String(),
		KBUID:         kb.UID.String(),
		PageSize:      int(req.GetPageSize()),
		PageToken:     req.GetPageToken(),
		FileUIDs:      req.GetFilter().GetFileUids(),
		ProcessStatus: req.GetFilter().GetProcessStatus(),
	})
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("fetching file list: %w", err),
			"Unable to retrieve file list. Please try again.",
		)
	}

	// Get the tokens and chunks using the source table and source UID.
	sources, err := ph.service.Repository().GetSourceTableAndUIDByFileUIDs(ctx, kbFileList.Files)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("fetching sources: %w", err),
			"Unable to retrieve file metadata. Please try again.",
		)
	}

	totalTokens, err := ph.service.Repository().GetFilesTotalTokens(ctx, sources)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("fetching tokens: %w", err),
			"Unable to retrieve token counts. Please try again.",
		)
	}

	totalChunks, err := ph.service.Repository().GetTotalTextChunksBySources(ctx, sources)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("fetching chunks: %w", err),
			"Unable to retrieve chunk counts. Please try again.",
		)
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

			content, err := ph.service.Repository().GetFile(ctx, config.Config.Minio.BucketName, kbFile.Destination)
			if err != nil {
				return nil, errorsx.AddMessage(
					fmt.Errorf("fetching file blob: %w", err),
					"Unable to retrieve file content. Please try again.",
				)
			}
			contentBase64 := base64.StdEncoding.EncodeToString(content)
			fileType := artifactpb.File_Type(artifactpb.File_Type_value[kbFile.FileType])

			objectUID, err = ph.uploadBase64FileToMinIO(ctx, ns.NsID, ns.NsUID, ns.NsUID, fileName, contentBase64, fileType)
			if err != nil {
				return nil, errorsx.AddMessage(
					fmt.Errorf("uploading migrated file to MinIO: %w", err),
					"Unable to migrate file. Please try again.",
				)
			}

			newDestination := repository.GetBlobObjectPath(ns.NsUID, objectUID)
			fmt.Println("newDestination", newDestination)
			_, err = ph.service.Repository().UpdateKnowledgeBaseFile(ctx, kbFile.UID.String(), map[string]any{
				repository.KnowledgeBaseFileColumn.Destination: newDestination,
			})
			if err != nil {
				return nil, errorsx.AddMessage(
					fmt.Errorf("updating migrated object: %w", err),
					"Unable to complete file migration. Please try again.",
				)
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
			CatalogUid:         kbFile.KBUID.String(),
			Name:               kbFile.Name,
			Type:               artifactpb.File_Type(artifactpb.File_Type_value[kbFile.FileType]),
			CreateTime:         timestamppb.New(*kbFile.CreateTime),
			UpdateTime:         timestamppb.New(*kbFile.UpdateTime),
			ProcessStatus:      artifactpb.FileProcessStatus(artifactpb.FileProcessStatus_value[kbFile.ProcessStatus]),
			Size:               kbFile.Size,
			ExternalMetadata:   kbFile.PublicExternalMetadataUnmarshal(),
			TotalChunks:        int32(totalChunks[kbFile.UID]),
			TotalTokens:        int32(totalTokens[kbFile.UID]),
			ObjectUid:          objectUID.String(),
			Summary:            "", // Summary is now stored as a separate converted_file, use GetFileSummary API to retrieve
			DownloadUrl:        downloadURL,
			ConvertingPipeline: kbFile.ConvertingPipeline(),
		}

		// Include error message if processing failed
		if kbFile.ExtraMetaDataUnmarshal != nil && kbFile.ExtraMetaDataUnmarshal.FailReason != "" {
			file.ProcessOutcome = kbFile.ExtraMetaDataUnmarshal.FailReason
		}

		if kbFile.ExtraMetaDataUnmarshal != nil && kbFile.ExtraMetaDataUnmarshal.Length != nil {
			fileType := artifactpb.File_Type(artifactpb.File_Type_value[kbFile.FileType])
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
		return nil, errorsx.AddMessage(
			fmt.Errorf("file not found: %w", errorsx.ErrNotFound),
			"File not found. Please check the file ID and try again.",
		)
	}

	return &artifactpb.GetCatalogFileResponse{
		File: files.Files[0],
	}, nil

}

// DeleteCatalogFile deletes a file in a catalog
func (ph *PublicHandler) DeleteCatalogFile(ctx context.Context, req *artifactpb.DeleteCatalogFileRequest) (*artifactpb.DeleteCatalogFileResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Get authenticated user UID for ACL checks
	_, err := getUserUIDFromContext(ctx)
	if err != nil {
		logger.Error("failed to get user id from header", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get user id from header: %v: %w", err, errorsx.ErrUnauthenticated),
			"Authentication failed. Please log in and try again.",
		)
	}

	// ACL - check user's permission to write catalog of kb file
	kbfs, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []types.FileUIDType{types.FileUIDType(uuid.FromStringOrNil(req.FileUid))})
	if err != nil {
		logger.Error("failed to get catalog files", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get catalog files: %w", err),
			"Unable to retrieve file information. Please try again.",
		)
	} else if len(kbfs) == 0 {
		return nil, errorsx.AddMessage(
			fmt.Errorf("file not found: %w", errorsx.ErrNotFound),
			"File not found. Please check the file ID and try again.",
		)
	}

	// Get the KB to determine if it's a staging/rollback KB
	kb, err := ph.service.Repository().GetKnowledgeBaseByUID(ctx, kbfs[0].KBUID)
	if err != nil {
		logger.Error("failed to get knowledge base", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get knowledge base: %w", err),
			"Unable to retrieve catalog information. Please try again.",
		)
	}

	// For staging/rollback KBs (staging=true), check permission against the production KB
	// This is necessary because:
	// 1. Staging/rollback KBs are system-managed entities
	// 2. Users have permission on the production KB, not on temporary staging/rollback KBs
	// 3. File operations on staging/rollback KBs (via dual processing/deletion) should be allowed
	//    if the user has permission on the production KB
	var aclCheckKBUID types.KBUIDType
	if kb.Staging {
		// Extract production catalog ID from staging/rollback ID
		// Format: {production-catalog-id}-staging or {production-catalog-id}-rollback
		prodCatalogID := kb.KBID
		if len(prodCatalogID) > 8 {
			if prodCatalogID[len(prodCatalogID)-8:] == "-staging" {
				prodCatalogID = prodCatalogID[:len(prodCatalogID)-8]
			} else if len(prodCatalogID) > 9 && prodCatalogID[len(prodCatalogID)-9:] == "-rollback" {
				prodCatalogID = prodCatalogID[:len(prodCatalogID)-9]
			}
		}

		// Get production KB UID for ACL check
		prodKB, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, types.OwnerUIDType(uuid.FromStringOrNil(kb.Owner)), prodCatalogID)
		if err != nil {
			logger.Error("failed to get production KB for ACL check",
				zap.String("stagingKBID", kb.KBID),
				zap.String("prodCatalogID", prodCatalogID),
				zap.Error(err))
			return nil, errorsx.AddMessage(
				fmt.Errorf("failed to get production catalog for permission check: %w", err),
				"Unable to verify access permissions for this file. Please try again.",
			)
		}
		aclCheckKBUID = prodKB.UID
		logger.Info("Checking permission against production KB for staging/rollback file deletion",
			zap.String("fileKBUID", kbfs[0].KBUID.String()),
			zap.String("prodKBUID", aclCheckKBUID.String()))
	} else {
		// Normal production KB - check permission directly
		aclCheckKBUID = kbfs[0].KBUID
	}

	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", aclCheckKBUID, "writer")
	if err != nil {
		logger.Error("failed to check permission", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to check permission: %w", err),
			"Unable to verify access permissions. Please try again.",
		)
	}
	if !granted {
		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: no permission over catalog", errorsx.ErrUnauthorized),
			"You don't have permission to delete files from this catalog. Please contact the owner for access.",
		)
	}
	// check if file uid is empty
	if req.FileUid == "" {
		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: file uid is required", errorsx.ErrInvalidArgument),
			"File ID is required. Please specify which file to delete.",
		)
	}

	fUID, err := uuid.FromString(req.FileUid)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to parse file uid: %w", errorsx.ErrInvalidArgument),
			"Invalid file ID format. Please check the file ID and try again.",
		)
	}

	// get the file by uid
	files, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []types.FileUIDType{types.FileUIDType(fUID)})
	if err != nil {
		return nil, err
	} else if len(files) == 0 {
		return nil, errorsx.AddMessage(
			fmt.Errorf("file not found: %w", errorsx.ErrNotFound),
			"File not found. Please check the file ID and try again.",
		)
	}

	// Soft-delete the file record first to make it immediately invisible to users
	// This ensures a responsive user experience regardless of cleanup workflow status
	err = ph.service.Repository().DeleteKnowledgeBaseFileAndDecreaseUsage(ctx, fUID)
	if err != nil {
		logger.Error("failed to delete knowledge base file and decrease usage", zap.Error(err))
		return nil, err
	}

	// DUAL DELETION: Check if dual deletion is needed (kb was already retrieved for ACL check above)
	// Similar to dual processing for uploads, we need to delete from both KBs:
	// 1. Phase 2 (updating): Delete from both production and staging
	// 2. Phase 3 (swapping): Delete from both for synchronization
	// 3. Phase 6 (retention): Delete from both production and rollback
	logger.Info("Checking dual deletion requirements",
		zap.String("kbUID", kb.UID.String()),
		zap.String("kbID", kb.KBID),
		zap.String("status", kb.UpdateStatus),
		zap.Bool("staging", kb.Staging))

	dualTarget, err := ph.service.Repository().GetDualProcessingTarget(ctx, kb)
	if err != nil {
		logger.Warn("Failed to check dual deletion requirements",
			zap.Error(err),
			zap.String("kbUID", kb.UID.String()))
	} else if !dualTarget.IsNeeded {
		logger.Info("Dual deletion NOT needed - file only deleted from single KB",
			zap.String("fileUID", fUID.String()),
			zap.String("kbUID", kb.UID.String()))
	} else if dualTarget.IsNeeded {
		logger.Info("Dual deletion required",
			zap.String("fileUID", fUID.String()),
			zap.String("prodKBUID", kb.UID.String()),
			zap.String("targetKBUID", dualTarget.TargetKB.UID.String()),
			zap.String("phase", dualTarget.Phase))

		// Find the corresponding file in target KB (by name)
		// Files in staging/rollback KB share the same name as production
		targetFiles, err := ph.service.Repository().GetKnowledgeBaseFilesByName(ctx, dualTarget.TargetKB.UID, files[0].Name)
		if err != nil {
			logger.Warn("Failed to find target file for dual deletion",
				zap.Error(err),
				zap.String("targetKBUID", dualTarget.TargetKB.UID.String()),
				zap.String("fileName", files[0].Name))
		} else if len(targetFiles) > 0 {
			targetFile := targetFiles[0]

			// Soft-delete the target file
			err = ph.service.Repository().DeleteKnowledgeBaseFileAndDecreaseUsage(ctx, targetFile.UID)
			if err != nil {
				logger.Error("Failed to delete target file during dual deletion",
					zap.Error(err),
					zap.String("targetFileUID", targetFile.UID.String()),
					zap.String("phase", dualTarget.Phase))
				// Non-fatal - production file is already deleted
			} else {
				logger.Info("Target file deleted successfully during dual deletion",
					zap.String("targetFileUID", targetFile.UID.String()),
					zap.String("phase", dualTarget.Phase))

				// Trigger cleanup workflow for target file
				targetWorkflowID := uuid.Must(uuid.NewV4()).String()
				backgroundCtx := context.Background()

				err = ph.service.CleanupFile(backgroundCtx, targetFile.UID, targetFile.Owner, targetFile.RequesterUID, targetWorkflowID, true)
				if err != nil {
					logger.Error("Failed to trigger cleanup workflow for target file",
						zap.String("targetFileUID", targetFile.UID.String()),
						zap.String("workflowID", targetWorkflowID),
						zap.Error(err))
				}
			}
		} else {
			logger.Info("No corresponding target file found for dual deletion (may not have been created yet)",
				zap.String("fileName", files[0].Name),
				zap.String("targetKBUID", dualTarget.TargetKB.UID.String()))
		}
	}

	// Fire-and-forget: trigger background cleanup workflow for production file
	// If this fails, log the error but don't fail the user's delete request
	// Generate workflow ID for tracking
	workflowID := uuid.Must(uuid.NewV4()).String()

	// Use a detached context to prevent request cancellation from affecting workflow start
	// This ensures the cleanup workflow can be triggered even if the client disconnects
	backgroundCtx := context.Background()

	// Trigger cleanup workflow
	err = ph.service.CleanupFile(backgroundCtx, fUID, files[0].Owner, files[0].RequesterUID, workflowID, true)
	if err != nil {
		// Log the error but don't fail the user's request
		// The file has already been soft-deleted from their perspective
		logger.Error("Failed to trigger cleanup workflow - resources may be orphaned and require manual cleanup",
			zap.String("fileUID", fUID.String()),
			zap.String("workflowID", workflowID),
			zap.Error(err))
	} else {
		logger.Info("Cleanup workflow triggered successfully",
			zap.String("fileUID", fUID.String()),
			zap.String("workflowID", workflowID))
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
	fileUIDs := make([]types.FileUIDType, 0, len(req.FileUids))
	for _, fileUID := range req.FileUids {
		fUID, err := uuid.FromString(fileUID)
		if err != nil {
			return nil, errorsx.AddMessage(
				fmt.Errorf("failed to parse file uid: %w", errorsx.ErrInvalidArgument),
				"Invalid file ID format. Please check the file IDs and try again.",
			)
		}
		fileUIDs = append(fileUIDs, types.FileUIDType(fUID))
	}
	kbfs, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, fileUIDs)
	if err != nil {
		return nil, err
	} else if len(kbfs) == 0 {
		return nil, errorsx.AddMessage(
			fmt.Errorf("file not found: %w", errorsx.ErrNotFound),
			"No files found with the provided IDs. Please check the file IDs and try again.",
		)
	}
	// check write permission for the catalog
	for _, kbf := range kbfs {
		granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", kbf.KBUID, "writer")
		if err != nil {
			return nil, err
		}
		if !granted {
			return nil, errorsx.AddMessage(
				fmt.Errorf("%w: no permission over catalog", errorsx.ErrUnauthorized),
				"You don't have permission to process files in this catalog. Please contact the owner for access.",
			)
		}
	}

	// check auth user has access to the requester
	err = ph.service.ACLClient().CheckRequesterPermission(ctx)
	if err != nil {
		logger.Error("failed to check requester permission", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to check requester permission: %w", err),
			"Unable to verify requester permissions. Please try again.",
		)
	}

	requesterUID := resource.GetRequestSingleHeader(ctx, constantx.HeaderRequesterUIDKey)
	requesterUUID := uuid.FromStringOrNil(requesterUID)

	files, err := ph.service.Repository().ProcessKnowledgeBaseFiles(ctx, req.FileUids, requesterUUID)
	if err != nil {
		return nil, err
	}

	// Collect file information for batch processing
	if len(files) == 0 {
		return &artifactpb.ProcessCatalogFilesResponse{Files: []*artifactpb.File{}}, nil
	}

	// Extract file UIDs and common metadata
	fileUIDs = make([]types.FileUIDType, 0, len(files))
	kbUID := files[0].KBUID
	ownerUID := files[0].Owner
	requesterUIDFromFile := files[0].RequesterUID

	for _, file := range files {
		fileUIDs = append(fileUIDs, file.UID)
	}

	// Trigger Temporal workflow once for all files (batch processing)
	err = ph.service.ProcessFile(ctx, kbUID, fileUIDs, ownerUID, requesterUIDFromFile)
	if err != nil {
		logger.Error("Failed to start batch file processing workflow",
			zap.Int("fileCount", len(fileUIDs)),
			zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to start batch file processing workflow: %w", err),
			"Unable to start file processing. Please try again.",
		)
	}

	logger.Info("Batch file processing workflow started successfully",
		zap.Int("fileCount", len(fileUIDs)))

	// populate the files into response
	var resFiles []*artifactpb.File
	for _, file := range files {

		objectUID := uuid.FromStringOrNil(strings.TrimPrefix(strings.Split(file.Destination, "/")[1], "obj-"))

		resFile := &artifactpb.File{
			FileUid:            file.UID.String(),
			OwnerUid:           file.Owner.String(),
			CreatorUid:         file.CreatorUID.String(),
			CatalogUid:         file.KBUID.String(),
			Name:               file.Name,
			Type:               artifactpb.File_Type(artifactpb.File_Type_value[file.FileType]),
			CreateTime:         timestamppb.New(*file.CreateTime),
			UpdateTime:         timestamppb.New(*file.UpdateTime),
			ProcessStatus:      artifactpb.FileProcessStatus(artifactpb.FileProcessStatus_value[file.ProcessStatus]),
			ObjectUid:          objectUID.String(),
			ConvertingPipeline: file.ConvertingPipeline(),
		}

		// Include error message if processing failed
		if file.ExtraMetaDataUnmarshal != nil && file.ExtraMetaDataUnmarshal.FailReason != "" {
			resFile.ProcessOutcome = file.ExtraMetaDataUnmarshal.FailReason
		}

		resFiles = append(resFiles, resFile)
	}
	return &artifactpb.ProcessCatalogFilesResponse{
		Files: resFiles,
	}, nil
}

func fileTypeConvertToMime(t artifactpb.File_Type) string {
	switch t {
	case artifactpb.File_TYPE_PDF:
		return "application/pdf"
	case artifactpb.File_TYPE_MARKDOWN:
		return "text/markdown"
	case artifactpb.File_TYPE_TEXT:
		return "text/plain"
	case artifactpb.File_TYPE_DOC:
		return "application/msword"
	case artifactpb.File_TYPE_DOCX:
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	case artifactpb.File_TYPE_HTML:
		return "text/html"
	case artifactpb.File_TYPE_PPT:
		return "application/vnd.ms-powerpoint"
	case artifactpb.File_TYPE_PPTX:
		return "application/vnd.openxmlformats-officedocument.presentationml.presentation"
	case artifactpb.File_TYPE_XLSX:
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case artifactpb.File_TYPE_XLS:
		return "application/vnd.ms-excel"
	case artifactpb.File_TYPE_CSV:
		return "text/csv"
	default:
		return "application/octet-stream"
	}
}

func determineFileType(fileName string) artifactpb.File_Type {
	fileNameLower := strings.ToLower(fileName)
	if strings.HasSuffix(fileNameLower, ".pdf") {
		return artifactpb.File_TYPE_PDF
	} else if strings.HasSuffix(fileNameLower, ".md") {
		return artifactpb.File_TYPE_MARKDOWN
	} else if strings.HasSuffix(fileNameLower, ".txt") {
		return artifactpb.File_TYPE_TEXT
	} else if strings.HasSuffix(fileNameLower, ".doc") {
		return artifactpb.File_TYPE_DOC
	} else if strings.HasSuffix(fileNameLower, ".docx") {
		return artifactpb.File_TYPE_DOCX
	} else if strings.HasSuffix(fileNameLower, ".html") {
		return artifactpb.File_TYPE_HTML
	} else if strings.HasSuffix(fileNameLower, ".ppt") {
		return artifactpb.File_TYPE_PPT
	} else if strings.HasSuffix(fileNameLower, ".pptx") {
		return artifactpb.File_TYPE_PPTX
	} else if strings.HasSuffix(fileNameLower, ".xlsx") {
		return artifactpb.File_TYPE_XLSX
	} else if strings.HasSuffix(fileNameLower, ".xls") {
		return artifactpb.File_TYPE_XLS
	} else if strings.HasSuffix(fileNameLower, ".csv") {
		return artifactpb.File_TYPE_CSV
	}
	return artifactpb.File_TYPE_UNSPECIFIED
}

// getPositionUnit returns the appropriate unit for file position based on file type
func getPositionUnit(fileType artifactpb.File_Type) artifactpb.File_Position_Unit {
	switch fileType {
	case artifactpb.File_TYPE_TEXT,
		artifactpb.File_TYPE_MARKDOWN,
		artifactpb.File_TYPE_HTML,
		artifactpb.File_TYPE_CSV:
		return artifactpb.File_Position_UNIT_CHARACTER
	case artifactpb.File_TYPE_PDF,
		artifactpb.File_TYPE_DOCX,
		artifactpb.File_TYPE_DOC,
		artifactpb.File_TYPE_PPT,
		artifactpb.File_TYPE_PPTX,
		artifactpb.File_TYPE_XLSX,
		artifactpb.File_TYPE_XLS:
		return artifactpb.File_Position_UNIT_PAGE
	}

	return artifactpb.File_Position_UNIT_UNSPECIFIED
}

// GetFileSummary returns the summary of the file from the summary converted_file
func (ph *PublicHandler) GetFileSummary(ctx context.Context, req *artifactpb.GetFileSummaryRequest) (*artifactpb.GetFileSummaryResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	_, err := getUserUIDFromContext(ctx)
	if err != nil {
		logger.Error("failed to get user id from header", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get user id from header: %v: %w", err, errorsx.ErrUnauthenticated),
			"Authentication failed. Please log in and try again.",
		)
	}

	// Check if user can access the namespace
	_, err = ph.service.GetNamespaceAndCheckPermission(ctx, req.NamespaceId)
	if err != nil {
		logger.Error("failed to get namespace and check permission", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get namespace and check permission: %w", err),
			"Unable to access the specified namespace. Please check permissions and try again.",
		)
	}

	fileUID := uuid.FromStringOrNil(req.FileUid)

	// Verify file exists
	kbFiles, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []types.FileUIDType{types.FileUIDType(fileUID)})
	if err != nil || len(kbFiles) == 0 {
		logger.Error("file not found", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("file not found: %w", errorsx.ErrNotFound),
			"File not found. Please check the file ID and try again.",
		)
	}

	// Get the SUMMARY converted file using explicit type query
	summaryConvertedFile, err := ph.service.Repository().GetConvertedFileByFileUIDAndType(
		ctx,
		types.FileUIDType(fileUID),
		artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_SUMMARY,
	)
	if err != nil {
		logger.Error("failed to get summary converted file", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("summary not yet available: %w", errorsx.ErrNotFound),
			"File summary not yet available. The file may still be processing.",
		)
	}

	// Fetch summary content from MinIO
	content, err := ph.service.Repository().GetFile(ctx, config.Config.Minio.BucketName, summaryConvertedFile.Destination)
	if err != nil {
		logger.Error("failed to get summary from MinIO",
			zap.String("destination", summaryConvertedFile.Destination),
			zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to retrieve summary content: %w", err),
			"Unable to retrieve file summary. Please try again later.",
		)
	}

	return &artifactpb.GetFileSummaryResponse{
		Summary: string(content),
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
func (ph *PublicHandler) uploadBase64FileToMinIO(ctx context.Context, nsID string, nsUID, creatorUID types.CreatorUIDType, fileName string, content string, fileType artifactpb.File_Type) (types.ObjectUIDType, error) {
	logger, _ := logx.GetZapLogger(ctx)
	response, err := ph.service.GetUploadURL(ctx, &artifactpb.GetObjectUploadURLRequest{
		NamespaceId: nsID,
		ObjectName:  fileName,
	}, nsUID, fileName, creatorUID)
	if err != nil {
		logger.Error("failed to get upload URL", zap.Error(err))
		return uuid.Nil, errorsx.AddMessage(
			fmt.Errorf("failed to get upload URL: %w", err),
			"Unable to prepare file upload. Please try again.",
		)
	}
	objectUID := uuid.FromStringOrNil(response.Object.Uid)
	destination := repository.GetBlobObjectPath(nsUID, objectUID)
	err = ph.service.Repository().UploadBase64File(ctx, repository.BlobBucketName, destination, content, fileTypeConvertToMime(fileType))
	if err != nil {
		return uuid.Nil, errorsx.AddMessage(
			fmt.Errorf("failed to upload file to MinIO: %w", err),
			"Unable to upload file to storage. Please try again.",
		)
	}
	decodedContent, err := base64.StdEncoding.DecodeString(content)
	if err != nil {
		return uuid.Nil, errorsx.AddMessage(
			fmt.Errorf("failed to decode file content: %w", err),
			"Invalid file content format. Please try again.",
		)
	}
	objectSize := int64(len(decodedContent))

	object, err := ph.service.Repository().GetObjectByUID(ctx, objectUID)
	if err != nil {
		logger.Error("failed to get object by uid", zap.Error(err))
		return uuid.Nil, errorsx.AddMessage(
			fmt.Errorf("failed to get object by uid: %w", err),
			"Unable to retrieve uploaded file information. Please try again.",
		)
	}
	object.Size = objectSize
	object.IsUploaded = true

	_, err = ph.service.Repository().UpdateObject(ctx, *object)
	if err != nil {
		logger.Error("failed to update object", zap.Error(err))
		return uuid.Nil, errorsx.AddMessage(
			fmt.Errorf("failed to update object: %w", err),
			"Unable to finalize file upload. Please try again.",
		)
	}
	return objectUID, nil
}
