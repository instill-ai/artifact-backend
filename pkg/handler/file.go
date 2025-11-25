package handler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gofrs/uuid"

	"go.einride.tech/aip/filtering"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/pipeline"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/repository/object"
	"github.com/instill-ai/artifact-backend/pkg/service"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
	filetype "github.com/instill-ai/x/file"
	logx "github.com/instill-ai/x/log"
)

// CreateFile adds a file to a knowledge base (AIP-compliant version of UploadKnowledgeBaseFile).
// It handles file upload, validation, ACL checks, dual processing for staging/rollback KBs,
// and auto-triggers the processing workflow.
func (ph *PublicHandler) CreateFile(ctx context.Context, req *artifactpb.CreateFileRequest) (*artifactpb.CreateFileResponse, error) {
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
	// ACL - check user's permission to write knowledge base
	kb, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, req.KnowledgeBaseId)
	if err != nil {
		logger.Error("failed to get knowledge base", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf(ErrorListKnowledgeBasesMsg, err),
			"Unable to access the specified knowledge base. Please check the knowledge base ID and try again.",
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
			fmt.Errorf("%w: no permission over knowledge base", errorsx.ErrUnauthorized),
			"You don't have permission to upload files to this knowledge base. Please contact the owner for access.",
		)
	}

	// CRITICAL PHASE CHECK: Block file uploads ONLY during validation phase
	// When KB is in KNOWLEDGE_BASE_UPDATE_STATUS_VALIDATING status, it's in the final validation phase where
	// both production and staging KBs must remain absolutely identical.
	// File operations are still allowed during KNOWLEDGE_BASE_UPDATE_STATUS_SWAPPING status with dual processing.
	if kb.UpdateStatus == artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_VALIDATING.String() {
		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: knowledge base is in critical update phase", errorsx.ErrRateLimiting),
			fmt.Sprintf("Knowledge base is currently being validated (phase: %s). Please wait a moment and try again.", kb.UpdateStatus),
		)
	}

	// get all kbs in the namespace
	kbs, err := ph.service.Repository().ListKnowledgeBases(ctx, ns.NsUID.String())
	if err != nil {
		logger.Error("failed to list knowledge base", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf(ErrorListKnowledgeBasesMsg, err),
			"Unable to retrieve knowledge base information. Please try again.",
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
		Filename:                  req.GetFile().GetFilename(),
		FileType:                  req.File.Type.String(),
		Owner:                     ns.NsUID,
		KBUID:                     kb.UID,
		ProcessStatus:             artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_NOTSTARTED.String(),
		ExternalMetadataUnmarshal: md,
		Tags:                      repository.TagsArray(req.GetFile().GetTags()),
	}

	if req.GetFile().GetConvertingPipeline() != "" {
		// TODO jvallesm: validate existence, permissions & recipe of provided
		// pipeline.
		if _, err := pipeline.ReleaseFromName(req.GetFile().GetConvertingPipeline()); err != nil {
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
		if len(req.File.Filename) > 255 {
			return nil, errorsx.AddMessage(
				fmt.Errorf("file name is too long. max length is 255. name: %s err: %w",
					req.File.Filename, errorsx.ErrInvalidArgument),
				"File name is too long. Please use a name with 255 characters or less.",
			)
		}

		// Determine file type if not explicitly provided
		if req.File.Type == artifactpb.File_TYPE_UNSPECIFIED {
			req.File.Type = determineFileType(req.File.Filename)
			if req.File.Type == artifactpb.File_TYPE_UNSPECIFIED {
				return nil, errorsx.AddMessage(
					fmt.Errorf("%w: unsupported file extension", errorsx.ErrInvalidArgument),
					"Unsupported file type. Please upload a supported file format.",
				)
			}
		}

		// Special handling for WebM files - determine if audio-only or video by inspecting content
		// Only do this if type wasn't explicitly specified or if it was inferred as WEBM_VIDEO
		if strings.HasSuffix(strings.ToLower(req.File.Filename), ".webm") && req.File.Content != "" {
			// If type was explicitly set to WEBM_AUDIO or WEBM_VIDEO, respect it
			// Otherwise, detect from content
			if req.File.Type == artifactpb.File_TYPE_WEBM_VIDEO {
				detectedType := detectWebMType(req.File.Content)
				req.File.Type = detectedType
				logger.Info("Detected WebM type from content",
					zap.String("filename", req.File.Filename),
					zap.String("detectedType", detectedType.String()))
			}
		}

		// Update kbFile.FileType after determining the type
		kbFile.FileType = req.File.Type.String()

		if strings.Contains(req.File.Filename, "/") {
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
		objectUID, err := ph.uploadBase64FileToMinIO(ctx, ns.NsID, ns.NsUID, creatorUID, req.File.Filename, req.File.Content, req.File.Type)
		if err != nil {
			return nil, errorsx.AddMessage(
				fmt.Errorf("fetching upload URL: %w", err),
				"Unable to upload file. Please check your network connection and try again.",
			)
		}
		destination := object.GetBlobObjectPath(ns.NsUID, objectUID)

		kbFile.CreatorUID = creatorUID
		kbFile.Destination = destination

		fileSize, _ := getFileSize(req.File.Content)
		kbFile.Size = fileSize
	} else {
		obj, err := ph.service.Repository().GetObjectByUID(ctx, uuid.FromStringOrNil(req.GetFile().GetObjectUid()))
		if err != nil {
			logger.Error("failed to get knowledge base object with provided UID", zap.Error(err))
			return nil, err
		}

		if !obj.IsUploaded {
			if !strings.HasPrefix(obj.Destination, "ns-") {
				return nil, errorsx.AddMessage(
					fmt.Errorf("file has not been uploaded yet"),
					"File upload is not complete. Please wait for the upload to finish and try again.",
				)
			}

			// Check if file exists in minio and get its metadata
			fileMetadata, err := ph.service.Repository().GetMinIOStorage().GetFileMetadata(ctx, object.BlobBucketName, obj.Destination)
			if err != nil {
				logger.Error("failed to get file from minio", zap.Error(err))
				return nil, err
			}
			obj.IsUploaded = true

			// Update object size from MinIO if it's 0
			if obj.Size == 0 && fileMetadata.Size > 0 {
				obj.Size = fileMetadata.Size
				_, err = ph.service.Repository().UpdateObject(ctx, *obj)
				if err != nil {
					logger.Warn("failed to update object size", zap.Error(err))
				} else {
					logger.Info("updated object size from MinIO", zap.Int64("size", obj.Size))
				}
			}
		}

		kbFile.Filename = obj.Name
		kbFile.CreatorUID = obj.CreatorUID
		kbFile.Destination = obj.Destination
		kbFile.Size = obj.Size

		req.File.Type = determineFileType(obj.Name)

		// Special handling for WebM files - determine if audio-only or video by inspecting content
		if strings.HasSuffix(strings.ToLower(obj.Name), ".webm") {
			// Read the file to detect codec type
			// Note: GetFile reads entire file, but detectWebMType only checks first 8KB
			fileBytes, err := ph.service.Repository().GetMinIOStorage().GetFile(ctx, object.BlobBucketName, obj.Destination)
			if err == nil && len(fileBytes) > 0 {
				// Encode to base64 for detectWebMType function
				base64Content := base64.StdEncoding.EncodeToString(fileBytes)
				req.File.Type = detectWebMType(base64Content)
				logger.Info("Detected WebM type from content",
					zap.String("filename", obj.Name),
					zap.String("type", req.File.Type.String()))
			} else {
				logger.Warn("Failed to read WebM file for type detection, defaulting to video",
					zap.String("filename", obj.Name),
					zap.Error(err))
			}
			// If we can't read the file, keep the default WEBM_VIDEO type
		}

		kbFile.FileType = req.File.Type.String()
	}

	maxSizeBytes := service.MaxUploadFileSizeMB << 10 << 10
	if kbFile.Size > maxSizeBytes {
		err := fmt.Errorf("%w: max file size exceeded", errorsx.ErrInvalidArgument)
		msg := fmt.Sprintf("Uploaded files can not exceed %d MB.", service.MaxUploadFileSizeMB)
		return nil, errorsx.AddMessage(err, msg)
	}

	// create knowledge base file in database
	// Note: CreateKnowledgeBaseFile now atomically handles both file creation
	// and knowledge base usage increment in a single transaction
	res, err := ph.service.Repository().CreateKnowledgeBaseFile(ctx, kbFile, nil)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("creating knowledge base file: %w", err),
			"Unable to add file to knowledge base. Please try again.",
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

	logger.Info("Checking dual processing requirements",
		zap.String("fileUID", res.UID.String()),
		zap.String("filename", res.Filename),
		zap.String("kbUID", kb.UID.String()),
		zap.String("kbID", kb.KBID),
		zap.String("updateStatus", kb.UpdateStatus))

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
	} else {
		logger.Info("Dual processing NOT needed - single KB processing only",
			zap.String("fileUID", res.UID.String()),
			zap.String("filename", res.Filename),
			zap.String("kbUID", kb.UID.String()),
			zap.String("kbID", kb.KBID),
			zap.String("updateStatus", kb.UpdateStatus))
	}

	if dualTarget != nil && dualTarget.IsNeeded {
		// CRITICAL FIX: Make dual processing FULLY SYNCHRONOUS to prevent goroutine starvation under heavy load
		// Previously used async goroutines for both file record creation AND workflow queueing,
		// but under parallel test execution (13 tests), Go runtime wouldn't schedule goroutines,
		// causing staging files to never be created or processed.
		//
		// Synchronous execution adds minimal latency (~60-100ms total):
		// - File record creation: ~10-50ms (DB write)
		// - Workflow queueing: ~10-50ms (Temporal RPC, returns immediately)
		// The actual processing happens asynchronously in Temporal anyway, so no blocking.

		logger.Info("Dual processing started (fully synchronous: file record + workflow queueing)",
			zap.String("prodFileUID", res.UID.String()),
			zap.String("prodFileName", res.Filename),
			zap.String("prodKBUID", kb.UID.String()),
			zap.String("targetKBUID", dualTarget.TargetKB.UID.String()),
			zap.String("phase", dualTarget.Phase))

		// Create a duplicate file record for target KB (staging or rollback)
		// This file record will reference the same original file in MinIO
		// but will have different processed outputs (chunks, embeddings)
		targetFile := repository.KnowledgeBaseFileModel{
			Filename:                  res.Filename,
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

		// CRITICAL FIX: Retry dual file creation up to 3 times with exponential backoff
		// Under sustained load, DB connections or transactions can fail temporarily.
		// Without retries, SynchronizeKBActivity gets stuck forever waiting for file counts to match.
		var targetFileRes *repository.KnowledgeBaseFileModel
		maxRetries := 3
		for attempt := 1; attempt <= maxRetries; attempt++ {
			targetFileRes, err = ph.service.Repository().CreateKnowledgeBaseFile(ctx, targetFile, nil)
			if err == nil {
				break // Success
			}
			if attempt < maxRetries {
				backoffDuration := time.Duration(100*(1<<uint(attempt-1))) * time.Millisecond // 100ms, 200ms, 400ms
				logger.Warn("Failed to create target file record during dual processing, retrying...",
					zap.Error(err),
					zap.String("prodFileName", res.Filename),
					zap.String("targetKBUID", dualTarget.TargetKB.UID.String()),
					zap.Int("attempt", attempt),
					zap.Int("maxRetries", maxRetries),
					zap.Duration("backoff", backoffDuration))
				time.Sleep(backoffDuration)
			}
		}

		if err != nil {
			logger.Error("Failed to create target file record during dual processing after retries",
				zap.Error(err),
				zap.String("prodFileName", res.Filename),
				zap.String("prodFileUID", res.UID.String()),
				zap.String("targetKBUID", dualTarget.TargetKB.UID.String()),
				zap.String("targetKBID", dualTarget.TargetKB.KBID),
				zap.String("phase", dualTarget.Phase),
				zap.Int("attempts", maxRetries))
			// Log error but don't fail the upload - production file upload succeeded
		} else {
			// Target KB usage is automatically incremented by CreateKnowledgeBaseFile
			// in the same transaction as file creation (atomic operation)
			logger.Info("Created target file record for dual processing",
				zap.String("prodFileUID", res.UID.String()),
				zap.String("targetFileUID", targetFileRes.UID.String()),
				zap.String("phase", dualTarget.Phase))

			// SEQUENTIAL PROCESSING: Target file processing will be triggered by the production workflow
			// AFTER the production file completes successfully. This ensures:
			// 1. Production is processed first (correct priority for user-facing KB)
			// 2. Target processing waits for production to complete (proper synchronization)
			// 3. No race conditions with converted file unique constraints
			// 4. Simpler recovery logic (if production fails, target won't be processed)
			//
			// The production workflow's ProcessFileWorkflow will:
			// - Detect dual-processing is needed via GetFileMetadataActivity (checks KB update_status)
			// - After production completes, call FindTargetFileByNameActivity to locate target file by name
			// - Trigger a child ProcessFileWorkflow for the target file (fire-and-forget pattern)
			logger.Info("Target file record created, will be processed after production file completes",
				zap.String("prodFileUID", res.UID.String()),
				zap.String("targetFileUID", targetFileRes.UID.String()),
				zap.String("targetKBUID", dualTarget.TargetKB.UID.String()),
				zap.String("phase", dualTarget.Phase))
		}
	}

	// AUTO-TRIGGER PROCESSING: Immediately start processing the uploaded file
	// This guarantees production KB never has NOTSTARTED files, which is critical for:
	// 1. Sequential dual-processing: Staging files depend on production completing
	// 2. KB update synchronization: Must ensure all files are processing/completed before swap
	// 3. User experience: Automatic processing without requiring separate API call
	//
	// The ProcessCatalogFiles API is now deprecated (kept for backward compatibility only).
	logger.Info("Auto-triggering file processing",
		zap.String("fileUID", res.UID.String()),
		zap.String("filename", res.Filename),
		zap.String("kbUID", kb.UID.String()))

	ownerUID := types.UserUIDType(ns.NsUID)
	requesterUID := types.RequesterUIDType(uuid.FromStringOrNil(authUID))

	err = ph.service.ProcessFile(ctx, kb.UID, []types.FileUIDType{res.UID}, ownerUID, requesterUID)
	if err != nil {
		// Non-fatal: File is uploaded, user can manually trigger via ProcessCatalogFiles if needed
		logger.Error("Failed to auto-trigger file processing",
			zap.Error(err),
			zap.String("fileUID", res.UID.String()),
			zap.String("filename", res.Filename),
			zap.String("kbUID", kb.UID.String()),
			zap.String("kbID", kb.KBID),
			zap.String("updateStatus", kb.UpdateStatus))
		// Don't fail the upload - file record exists and can be processed later
	} else {
		logger.Info("File processing started successfully (auto-trigger)",
			zap.String("fileUID", res.UID.String()),
			zap.String("filename", res.Filename),
			zap.String("kbUID", kb.UID.String()),
			zap.String("kbID", kb.KBID),
			zap.String("updateStatus", kb.UpdateStatus),
			zap.Bool("hasDualProcessing", dualTarget != nil && dualTarget.IsNeeded))
	}

	return &artifactpb.CreateFileResponse{
		File: &artifactpb.File{
			Uid:                res.UID.String(),
			Id:                 res.UID.String(),
			OwnerUid:           res.Owner.String(),
			CreatorUid:         res.CreatorUID.String(),
			KnowledgeBaseUid:   res.KBUID.String(),
			Name:               fmt.Sprintf("namespaces/%s/knowledge-bases/%s/files/%s", req.NamespaceId, req.KnowledgeBaseId, res.UID.String()),
			Filename:           res.Filename,
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

// ListFiles lists files in a knowledge base with pagination and filtering (AIP-compliant version of ListKnowledgeBaseFiles).
// Supports filtering by file IDs and process status, with token/chunk count enrichment.
func (ph *PublicHandler) ListFiles(ctx context.Context, req *artifactpb.ListFilesRequest) (*artifactpb.ListFilesResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		logger.Error("failed to get user id from header", zap.Error(err))
		err := fmt.Errorf("failed to get user id from header: %v. err: %w", err, errorsx.ErrUnauthenticated)
		return nil, err
	}

	// ACL - check if the creator can list files in this knowledge base. ACL using uid to check the certain namespace resource.
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
	// ACL - check user's permission to write knowledge base
	kb, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, req.KnowledgeBaseId)
	if err != nil {
		logger.Error("failed to get knowledge base", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf(ErrorListKnowledgeBasesMsg, err),
			"Unable to access the specified knowledge base. Please check the knowledge base ID and try again.",
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
			fmt.Errorf("%w: no permission over knowledge base", errorsx.ErrUnauthorized),
			"You don't have permission to view this knowledge base. Please contact the owner for access.",
		)
	}

	// Parse AIP-160 filter expression
	declarations, err := filtering.NewDeclarations([]filtering.DeclarationOption{
		filtering.DeclareStandardFunctions(),
		filtering.DeclareIdent("uid", filtering.TypeString),
		filtering.DeclareIdent("id", filtering.TypeString),
		filtering.DeclareIdent("process_status", filtering.TypeString),
	}...)
	if err != nil {
		logger.Error("failed to create filter declarations", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("filter configuration error: %w", err),
			"Unable to configure filter. Please try again.",
		)
	}

	filter, err := filtering.ParseFilter(req, declarations)
	if err != nil {
		logger.Error("failed to parse filter", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("invalid filter expression: %w", err),
			"Unable to parse filter. Please check the filter syntax and try again.",
		)
	}

	kbFileList, err := ph.service.Repository().ListKnowledgeBaseFiles(ctx, repository.KnowledgeBaseFileListParams{
		OwnerUID:  ns.NsUID.String(),
		KBUID:     kb.UID.String(),
		PageSize:  int(req.GetPageSize()),
		PageToken: req.GetPageToken(),
		Filter:    filter,
	})
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("fetching file list: %w", err),
			"Unable to retrieve file list. Please try again.",
		)
	}

	// Get the tokens and chunks using the source table and source UID.
	sources, err := ph.service.Repository().GetContentByFileUIDs(ctx, kbFileList.Files)
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
		// 3. Updates the knowledge base file destination to reference the new object
		// This ensures consistent data structure across both upload flows.
		// This runtime migration will happen only once for each file.
		//
		// TODO: this is just a temporary solution, our Console need to
		// adopt the new flow. So the old flow can be deprecated and
		// removed.
		if strings.Split(kbFile.Destination, "/")[1] == "uploaded-file" {
			filename := strings.Split(kbFile.Destination, "/")[2]

			content, err := ph.service.Repository().GetMinIOStorage().GetFile(ctx, config.Config.Minio.BucketName, kbFile.Destination)
			if err != nil {
				return nil, errorsx.AddMessage(
					fmt.Errorf("fetching file blob: %w", err),
					"Unable to retrieve file content. Please try again.",
				)
			}
			contentBase64 := base64.StdEncoding.EncodeToString(content)
			fileType := artifactpb.File_Type(artifactpb.File_Type_value[kbFile.FileType])

			objectUID, err = ph.uploadBase64FileToMinIO(ctx, ns.NsID, ns.NsUID, ns.NsUID, filename, contentBase64, fileType)
			if err != nil {
				return nil, errorsx.AddMessage(
					fmt.Errorf("uploading migrated file to MinIO: %w", err),
					"Unable to migrate file. Please try again.",
				)
			}

			newDestination := object.GetBlobObjectPath(ns.NsUID, objectUID)
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
			Uid:                kbFile.UID.String(),
			Id:                 kbFile.UID.String(),
			OwnerUid:           kbFile.Owner.String(),
			CreatorUid:         kbFile.CreatorUID.String(),
			KnowledgeBaseUid:   kbFile.KBUID.String(),
			Name:               fmt.Sprintf("namespaces/%s/knowledge-bases/%s/files/%s", req.NamespaceId, req.KnowledgeBaseId, kbFile.UID.String()),
			Filename:           kbFile.Filename,
			Type:               artifactpb.File_Type(artifactpb.File_Type_value[kbFile.FileType]),
			CreateTime:         timestamppb.New(*kbFile.CreateTime),
			UpdateTime:         timestamppb.New(*kbFile.UpdateTime),
			ProcessStatus:      artifactpb.FileProcessStatus(artifactpb.FileProcessStatus_value[kbFile.ProcessStatus]),
			Size:               kbFile.Size,
			ExternalMetadata:   kbFile.PublicExternalMetadataUnmarshal(),
			TotalChunks:        int32(totalChunks[kbFile.UID]),
			TotalTokens:        int32(totalTokens[kbFile.UID]),
			ObjectUid:          objectUID.String(),
			DownloadUrl:        downloadURL,
			ConvertingPipeline: kbFile.ConvertingPipeline(),
			Tags:               []string(kbFile.Tags),
		}

		// Include status message (error or success message)
		if kbFile.ExtraMetaDataUnmarshal != nil && kbFile.ExtraMetaDataUnmarshal.StatusMessage != "" {
			file.ProcessOutcome = kbFile.ExtraMetaDataUnmarshal.StatusMessage
		}

		if kbFile.ExtraMetaDataUnmarshal != nil && len(kbFile.ExtraMetaDataUnmarshal.Length) > 0 {
			fileType := artifactpb.File_Type(artifactpb.File_Type_value[kbFile.FileType])
			file.Length = &artifactpb.File_Position{
				Unit:        getPositionUnit(fileType),
				Coordinates: kbFile.ExtraMetaDataUnmarshal.Length,
			}
		}

		files = append(files, file)
	}

	return &artifactpb.ListFilesResponse{
		Files:         files,
		TotalSize:     int32(kbFileList.TotalCount),
		PageSize:      int32(len(kbFileList.Files)),
		NextPageToken: kbFileList.NextPageToken,
	}, nil
}

// GetFile retrieves a file from a knowledge base with support for different views (AIP-compliant version of GetKnowledgeBaseFile).
// Supports VIEW_BASIC, VIEW_FULL (metadata), VIEW_SUMMARY, VIEW_CONTENT, VIEW_STANDARD_FILE_TYPE (standardized files), VIEW_ORIGINAL_FILE_TYPE (original files), and VIEW_CACHE (Gemini cache).
func (ph *PublicHandler) GetFile(ctx context.Context, req *artifactpb.GetFileRequest) (*artifactpb.GetFileResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Get the file metadata first
	filterExpr := fmt.Sprintf(`id = "%s"`, req.FileId)
	pageSize := int32(1)
	files, err := ph.ListFiles(ctx, &artifactpb.ListFilesRequest{
		NamespaceId:     req.NamespaceId,
		KnowledgeBaseId: req.KnowledgeBaseId,
		PageSize:        &pageSize,
		Filter:          &filterExpr,
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

	file := files.Files[0]

	// Handle view-specific content
	var derivedResourceURI *string
	view := req.GetView()

	// For VIEW_BASIC and VIEW_FULL, only return metadata (no derived content)
	if view == artifactpb.File_VIEW_BASIC || view == artifactpb.File_VIEW_FULL || view == artifactpb.File_VIEW_UNSPECIFIED {
		return &artifactpb.GetFileResponse{
			File:               file,
			DerivedResourceUri: nil,
		}, nil
	}

	// Get namespace and KB for MinIO/cache access
	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		logger.Warn("failed to get namespace for view content", zap.Error(err))
		return &artifactpb.GetFileResponse{File: file}, nil
	}

	kb, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, req.KnowledgeBaseId)
	if err != nil {
		logger.Warn("failed to get knowledge base for view content", zap.Error(err))
		return &artifactpb.GetFileResponse{File: file}, nil
	}

	fileUID := uuid.FromStringOrNil(req.FileId)
	kbFiles, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []types.FileUIDType{types.FileUIDType(fileUID)})
	if err != nil || len(kbFiles) == 0 {
		logger.Warn("failed to get file for view content", zap.Error(err))
		return &artifactpb.GetFileResponse{File: file}, nil
	}
	kbFile := kbFiles[0]

	// Check which storage provider is requested
	storageProvider := req.GetStorageProvider()
	useGCSStorage := storageProvider == artifactpb.File_STORAGE_PROVIDER_GCS

	// Helper function to get file URL (MinIO or GCS)
	getFileURL := func(bucket, objectPath, filename, contentType string) (string, error) {
		if useGCSStorage {
			// Use GCS storage for on-demand access
			gcsStorage := ph.service.Repository().GetGCSStorage()
			if gcsStorage == nil {
				return "", errorsx.AddMessage(
					fmt.Errorf("GCS is not configured"),
					"GCS storage is not configured. Please configure GCS in the system settings or use STORAGE_PROVIDER_MINIO.",
				)
			}

			// Use the same object path as MinIO for consistency
			// objectPath already contains the full path (e.g., kb-{uuid}/file-{uuid}/original-file)
			gcsObjectPath := objectPath
			gcsBucket := config.Config.GCS.Bucket

			// TTL for on-demand GCS files (10 minutes)
			const gcsTTL = 10 * time.Minute

			// Check Redis cache first for fast existence check
			fileExistsInGCS := false
			cacheExists, err := ph.service.Repository().CheckGCSFileExists(ctx, kb.UID, kbFile.UID, view.String())
			if err != nil {
				logger.Warn("Failed to check GCS file cache in Redis",
					zap.Error(err),
					zap.String("fileUID", kbFile.UID.String()))
			} else if cacheExists {
				fileExistsInGCS = true
				logger.Debug("File exists in GCS (Redis cache hit)",
					zap.String("gcsPath", gcsObjectPath),
					zap.String("view", view.String()))
			} else {
				// Redis cache miss - check GCS directly
				logger.Debug("Redis cache miss, checking GCS directly",
					zap.String("fileUID", kbFile.UID.String()),
					zap.String("view", view.String()))

				_, err := gcsStorage.GetFileMetadata(ctx, gcsBucket, gcsObjectPath)
				if err == nil {
					fileExistsInGCS = true
					logger.Debug("File already exists in GCS (direct check)",
						zap.String("gcsPath", gcsObjectPath))

					// Update Redis cache for future requests
					now := time.Now()
					gcsInfo := &repository.GCSFileInfo{
						KBUIDStr:      kb.UID.String(),
						FileUIDStr:    kbFile.UID.String(),
						View:          view.String(),
						GCSBucket:     gcsBucket,
						GCSObjectPath: gcsObjectPath,
						UploadTime:    now,
						ExpiresAt:     now.Add(gcsTTL),
					}
					if err := ph.service.Repository().SetGCSFileInfo(ctx, kb.UID, kbFile.UID, view.String(), gcsInfo, gcsTTL); err != nil {
						logger.Warn("Failed to update GCS file cache in Redis",
							zap.Error(err))
					}
				}
			}

			// Upload to GCS if not exists
			if !fileExistsInGCS {
				logger.Info("Uploading file to GCS",
					zap.String("gcsPath", gcsObjectPath),
					zap.String("sourceObject", objectPath),
					zap.String("view", view.String()))

				// Get file from MinIO (primary storage)
				fileContent, err := ph.service.Repository().GetMinIOStorage().GetFile(ctx, bucket, objectPath)
				if err != nil {
					return "", errorsx.AddMessage(
						fmt.Errorf("failed to read file from MinIO: %w", err),
						"Unable to read file for GCS upload. Please try again.",
					)
				}

				// Upload to GCS using GCS storage client
				base64Content := base64.StdEncoding.EncodeToString(fileContent)
				err = gcsStorage.UploadBase64File(ctx, gcsBucket, gcsObjectPath, base64Content, contentType)
				if err != nil {
					return "", errorsx.AddMessage(
						fmt.Errorf("failed to upload file to GCS: %w", err),
						"Unable to upload file to GCS. Please check GCS configuration and try again.",
					)
				}

				logger.Info("File uploaded to GCS successfully",
					zap.String("gcsPath", gcsObjectPath),
					zap.Int("size", len(fileContent)))

				// Store GCS file metadata in Redis with 10-minute TTL for cleanup
				now := time.Now()
				gcsInfo := &repository.GCSFileInfo{
					KBUIDStr:      kb.UID.String(),
					FileUIDStr:    kbFile.UID.String(),
					View:          view.String(),
					GCSBucket:     gcsBucket,
					GCSObjectPath: gcsObjectPath,
					UploadTime:    now,
					ExpiresAt:     now.Add(gcsTTL),
				}
				if err := ph.service.Repository().SetGCSFileInfo(ctx, kb.UID, kbFile.UID, view.String(), gcsInfo, gcsTTL); err != nil {
					logger.Warn("Failed to store GCS file info in Redis for TTL cleanup",
						zap.Error(err),
						zap.String("gcsPath", gcsObjectPath))
					// Non-fatal error - continue with URL generation
				} else {
					logger.Debug("Stored GCS file info in Redis with TTL",
						zap.String("gcsPath", gcsObjectPath),
						zap.Duration("ttl", gcsTTL))
				}
			}

			// Return native gs:// URI for VertexAI cache creation
			// VertexAI requires gs:// URIs (not signed HTTPS URLs) to access files with its own credentials
			gcsURI := fmt.Sprintf("gs://%s/%s", gcsBucket, gcsObjectPath)

			logger.Info("GCS URI generated for VertexAI access",
				zap.String("gcsURI", gcsURI),
				zap.String("path", gcsObjectPath))
			return gcsURI, nil
		}

		// Use MinIO storage (default)
		minioURL, err := ph.service.Repository().GetMinIOStorage().GetPresignedURLForDownload(
			ctx,
			bucket,
			objectPath,
			filename,
			contentType,
			15*time.Minute,
		)
		if err != nil {
			return "", err
		}

		// Encode MinIO presigned URL to be accessible through API gateway
		gatewayURL, err := service.EncodeBlobURL(minioURL)
		if err != nil {
			return "", err
		}

		return gatewayURL, nil
	}

	// Generate view-specific content with proper download headers
	switch view {
	case artifactpb.File_VIEW_SUMMARY:
		// Get converted summary file and generate pre-signed URL with proper headers
		convertedFile, err := ph.service.Repository().GetConvertedFileByFileUIDAndType(
			ctx,
			kbFile.UID,
			artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_SUMMARY,
		)
		if err == nil && convertedFile != nil {
			// Generate filename for download
			filename := fmt.Sprintf("%s-summary.md", kbFile.Filename)
			fileURL, err := getFileURL(
				config.Config.Minio.BucketName,
				convertedFile.Destination,
				filename,
				convertedFile.ContentType, // Usually "text/markdown"
			)
			if err != nil {
				// If GCS is explicitly requested but fails, return error instead of falling back
				if useGCSStorage {
					return nil, err
				}
				logger.Warn("failed to generate file URL for summary", zap.Error(err))
			} else {
				derivedResourceURI = &fileURL
			}
		} else {
			logger.Info("summary not available for file", zap.String("fileUID", kbFile.UID.String()))
		}

	case artifactpb.File_VIEW_CONTENT:
		// Get converted content file and generate pre-signed URL with proper headers
		convertedFile, err := ph.service.Repository().GetConvertedFileByFileUIDAndType(
			ctx,
			kbFile.UID,
			artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_CONTENT,
		)
		if err == nil && convertedFile != nil {
			// Generate filename for download
			filename := fmt.Sprintf("%s-content.md", kbFile.Filename)
			fileURL, err := getFileURL(
				config.Config.Minio.BucketName,
				convertedFile.Destination,
				filename,
				convertedFile.ContentType, // Usually "text/markdown"
			)
			if err != nil {
				// If GCS is explicitly requested but fails, return error instead of falling back
				if useGCSStorage {
					return nil, err
				}
				logger.Warn("failed to generate file URL for content", zap.Error(err))
			} else {
				derivedResourceURI = &fileURL
			}
		} else {
			logger.Info("content not available for file", zap.String("fileUID", kbFile.UID.String()))
		}

	case artifactpb.File_VIEW_STANDARD_FILE_TYPE:
		// Get standardized file:
		// - Documents → PDF
		// - Images → PNG
		// - Audio → OGG
		// - Video → MP4

		// Parse file type to determine which converted file type to query
		fileType, ok := artifactpb.File_Type_value[kbFile.FileType]
		if !ok {
			fileType = int32(artifactpb.File_TYPE_UNSPECIFIED)
		}
		fileProtoType := artifactpb.File_Type(fileType)

		// Determine the converted file type and file extension based on media category
		convertedFileType, fileExtension, _ := filetype.GetConvertedFileTypeInfo(fileProtoType)

		if convertedFileType == artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_UNSPECIFIED {
			logger.Warn("unsupported file type for VIEW_STANDARD_FILE_TYPE",
				zap.String("fileType", kbFile.FileType),
				zap.String("fileUID", kbFile.UID.String()))
			convertedFileType = artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_UNSPECIFIED
			fileExtension = ""
		}

		if convertedFileType != artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_UNSPECIFIED {
			convertedFile, err := ph.service.Repository().GetConvertedFileByFileUIDAndType(
				ctx,
				kbFile.UID,
				convertedFileType,
			)
			if err == nil && convertedFile != nil {
				// Generate filename for download with appropriate extension
				filename := fmt.Sprintf("%s.%s", kbFile.Filename, fileExtension)
				fileURL, err := getFileURL(
					config.Config.Minio.BucketName,
					convertedFile.Destination,
					filename,
					convertedFile.ContentType,
				)
				if err != nil {
					// If GCS is explicitly requested but fails, return error instead of falling back
					if useGCSStorage {
						return nil, err
					}
					logger.Warn("failed to generate file URL for standardized file", zap.Error(err))
				} else {
					derivedResourceURI = &fileURL
					logger.Debug("generated standardized file URL",
						zap.String("fileType", fileExtension),
						zap.String("convertedType", convertedFileType.String()))
				}
			} else {
				logger.Info("standardized file not available",
					zap.String("fileUID", kbFile.UID.String()),
					zap.String("convertedFileType", convertedFileType.String()))
			}
		}

	case artifactpb.File_VIEW_ORIGINAL_FILE_TYPE:
		// Get the original uploaded file
		// Parse file type from stored string
		fileType, ok := artifactpb.File_Type_value[kbFile.FileType]
		if !ok {
			fileType = int32(artifactpb.File_TYPE_UNSPECIFIED)
		}
		fileProtoType := artifactpb.File_Type(fileType)

		// Get the appropriate bucket for this file
		bucket := object.BucketFromDestination(kbFile.Destination)

		// Get MIME type for the original file
		contentType := filetype.FileTypeToMimeType(fileProtoType)

		// Generate file URL for the original file
		fileURL, err := getFileURL(
			bucket,
			kbFile.Destination,
			kbFile.Filename,
			contentType,
		)
		if err != nil {
			// If GCS is explicitly requested but fails, return error instead of falling back
			if useGCSStorage {
				return nil, err
			}
			logger.Warn("failed to generate file URL for original file", zap.Error(err))
		} else {
			derivedResourceURI = &fileURL
		}

	case artifactpb.File_VIEW_CACHE:
		// Get or create Gemini cache for the file with automatic TTL renewal
		// This endpoint provides cache names for efficient AI operations
		logger.Info("GetFile with VIEW_CACHE",
			zap.String("fileUID", kbFile.UID.String()),
			zap.String("kbUID", kb.UID.String()))

		// Check if cache already exists in Redis
		fileUIDs := []types.FileUIDType{kbFile.UID}
		cacheMetadata, _ := ph.service.Repository().GetCacheMetadata(ctx, kb.UID, fileUIDs)

		if cacheMetadata != nil {
			// Cache exists - renew it
			logger.Info("Cache exists, renewing TTL",
				zap.String("cacheName", cacheMetadata.CacheName),
				zap.Bool("cachedContextEnabled", cacheMetadata.CachedContextEnabled))

			renewedCache, err := ph.service.RenewFileCache(ctx, kb.UID, kbFile.UID, cacheMetadata.CacheName)
			if err != nil {
				logger.Warn("Failed to renew cache, will return existing cache name",
					zap.Error(err),
					zap.String("cacheName", cacheMetadata.CacheName))
				// Return existing cache name even if renewal failed
				derivedResourceURI = &cacheMetadata.CacheName
			} else {
				// Successfully renewed
				logger.Info("Cache TTL renewed successfully",
					zap.String("cacheName", renewedCache.CacheName),
					zap.Time("newExpireTime", renewedCache.ExpireTime))
				derivedResourceURI = &renewedCache.CacheName
			}
		} else {
			// Cache doesn't exist - create it
			logger.Info("Cache not found, creating new cache",
				zap.String("fileUID", kbFile.UID.String()))

			// Parse file type
			fileType, ok := artifactpb.File_Type_value[kbFile.FileType]
			if !ok {
				fileType = int32(artifactpb.File_TYPE_UNSPECIFIED)
			}
			fileProtoType := artifactpb.File_Type(fileType)

			// Determine bucketName and objectName
			// For cache creation, we prefer converted files if available
			var bucketName, objectName string

			// Try to use converted PDF for documents (better for caching)
			convertedFile, err := ph.service.Repository().GetConvertedFileByFileUIDAndType(
				ctx,
				kbFile.UID,
				artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_DOCUMENT,
			)
			if err == nil && convertedFile != nil {
				bucketName = config.Config.Minio.BucketName
				objectName = convertedFile.Destination
				fileProtoType = artifactpb.File_TYPE_PDF
				logger.Info("Using converted PDF for cache creation",
					zap.String("destination", objectName))
			} else {
				// Use original file
				bucketName = object.BucketFromDestination(kbFile.Destination)
				objectName = kbFile.Destination
			}

			// Create cache using service method
			cacheResult, err := ph.service.GetOrCreateFileCache(
				ctx,
				kb.UID,
				kbFile.UID,
				bucketName,
				objectName,
				fileProtoType,
				kbFile.Filename,
			)

			if err != nil {
				// Cache creation failed
				logger.Warn("Failed to create cache",
					zap.Error(err),
					zap.String("fileUID", kbFile.UID.String()))

				// Return error to client for unsupported file types
				// For other errors, return empty response (graceful degradation)
				if errors.Is(err, errorsx.ErrInvalidArgument) {
					return nil, err
				}
				// For other errors, log and continue with empty derived_resource_uri
				emptyString := ""
				derivedResourceURI = &emptyString
			} else {
				// Successfully created cache
				logger.Info("Cache created successfully",
					zap.String("cacheName", cacheResult.CacheName),
					zap.Bool("cachedContextEnabled", cacheResult.CachedContextEnabled),
					zap.Time("expireTime", cacheResult.ExpireTime))

				// Return cache name for both cached and uncached modes
				// For uncached mode, CacheName will be empty string
				derivedResourceURI = &cacheResult.CacheName
			}
		}

	}

	return &artifactpb.GetFileResponse{
		File:               file,
		DerivedResourceUri: derivedResourceURI,
	}, nil
}

// DeleteFile deletes a file from a knowledge base (AIP-compliant version of DeleteKnowledgeBaseFile).
// Handles soft deletion, dual deletion for staging/rollback KBs, and triggers cleanup workflows.
func (ph *PublicHandler) DeleteFile(ctx context.Context, req *artifactpb.DeleteFileRequest) (*artifactpb.DeleteFileResponse, error) {
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

	// ACL - check user's permission to write knowledge base of kb file
	kbfs, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []types.FileUIDType{types.FileUIDType(uuid.FromStringOrNil(req.FileId))})
	if err != nil {
		logger.Error("failed to get knowledge base files", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get knowledge base files: %w", err),
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
			"Unable to retrieve knowledge base information. Please try again.",
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
		// Use parent_kb_uid to get production KB UID for ACL check
		if kb.ParentKBUID == nil {
			logger.Error("staging/rollback KB missing parent_kb_uid",
				zap.String("stagingKBID", kb.KBID),
				zap.String("stagingKBUID", kb.UID.String()))
			return nil, errorsx.AddMessage(
				fmt.Errorf("staging/rollback KB missing parent reference"),
				"Unable to verify access permissions for this file. Please try again.",
			)
		}
		aclCheckKBUID = *kb.ParentKBUID
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
			fmt.Errorf("%w: no permission over knowledge base", errorsx.ErrUnauthorized),
			"You don't have permission to delete files from this knowledge base. Please contact the owner for access.",
		)
	}

	// CRITICAL PHASE CHECK: Block file deletions during synchronization/validation/swap
	// When KB is in "swapping" or "validating" status, it's in a critical phase where
	// both production and staging KBs must remain absolutely identical for safe swap.
	// Any file modifications during this phase would break the synchronization guarantee.
	// We check the production KB status (even for staging/rollback files)
	var checkKB *repository.KnowledgeBaseModel
	if kb.Staging {
		// For staging/rollback files, check the production KB status
		checkKB, err = ph.service.Repository().GetKnowledgeBaseByUID(ctx, aclCheckKBUID)
		if err != nil {
			logger.Error("failed to get production KB for critical phase check",
				zap.String("prodKBUID", aclCheckKBUID.String()),
				zap.Error(err))
			return nil, errorsx.AddMessage(
				fmt.Errorf("failed to verify knowledge base status: %w", err),
				"Unable to verify knowledge base status. Please try again.",
			)
		}
	} else {
		checkKB = kb
	}

	// CRITICAL PHASE CHECK: Block file deletions ONLY during validation phase
	// When KB is in KNOWLEDGE_BASE_UPDATE_STATUS_VALIDATING status, it's in the final validation phase where
	// both production and staging KBs must remain absolutely identical.
	// File operations are still allowed during KNOWLEDGE_BASE_UPDATE_STATUS_SWAPPING status with dual processing.
	if checkKB.UpdateStatus == artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_VALIDATING.String() {
		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: knowledge base is in critical update phase", errorsx.ErrRateLimiting),
			fmt.Sprintf("Knowledge base is currently being validated (phase: %s). Please wait a moment and try again.", checkKB.UpdateStatus),
		)
	}

	// check if file uid is empty
	if req.FileId == "" {
		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: file uid is required", errorsx.ErrInvalidArgument),
			"File ID is required. Please specify which file to delete.",
		)
	}

	fUID, err := uuid.FromString(req.FileId)
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
		// CRITICAL: Retry logic to handle race condition where staging file is being created
		// During dual processing, the staging file might not exist yet when deletion is triggered
		// We retry up to 30 times (30 seconds total) to find the file before giving up
		// INCREASED from 10s to 30s to handle heavy concurrent load scenarios where file
		// creation takes longer due to DB transaction commit timing and goroutine scheduling
		var targetFiles []repository.KnowledgeBaseFileModel
		maxRetries := 30
		for attempt := 0; attempt < maxRetries; attempt++ {
			targetFiles, err = ph.service.Repository().GetKnowledgeBaseFilesByName(ctx, dualTarget.TargetKB.UID, files[0].Filename)
			if err != nil {
				logger.Warn("Failed to find target file for dual deletion",
					zap.Error(err),
					zap.String("targetKBUID", dualTarget.TargetKB.UID.String()),
					zap.String("filename", files[0].Filename),
					zap.Int("attempt", attempt+1))
				break // Stop retrying on error
			}

			if len(targetFiles) > 0 {
				logger.Info("Found target file for dual deletion",
					zap.String("targetFileUID", targetFiles[0].UID.String()),
					zap.Int("attempt", attempt+1))
				break // Found the file, stop retrying
			}

			// File not found yet - it might be in the process of being created
			// Use exponential backoff: 100ms, 200ms, 400ms, 800ms, 1s (capped)
			// Total: ~30s max with smart backoff to reduce log noise
			if attempt < maxRetries-1 {
				// Log only on first few attempts and then every 5th attempt to reduce noise
				if attempt < 3 || (attempt+1)%5 == 0 {
					logger.Info("Target file not found yet, retrying dual deletion lookup",
						zap.String("filename", files[0].Filename),
						zap.String("targetKBUID", dualTarget.TargetKB.UID.String()),
						zap.Int("attempt", attempt+1),
						zap.Int("maxRetries", maxRetries))
				}
				// Exponential backoff capped at 1s
				sleepDuration := time.Duration(100*(1<<uint(attempt))) * time.Millisecond
				if sleepDuration > time.Second {
					sleepDuration = time.Second
				}
				time.Sleep(sleepDuration)
			}
		}

		if len(targetFiles) > 0 {
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
			logger.Warn("No corresponding target file found for dual deletion after retries",
				zap.String("filename", files[0].Filename),
				zap.String("targetKBUID", dualTarget.TargetKB.UID.String()),
				zap.Int("retriesAttempted", maxRetries),
				zap.String("warning", "This may cause validation mismatch if the file is created later"))
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

	return &artifactpb.DeleteFileResponse{
		FileId: fUID.String(),
	}, nil

}

// UpdateFile updates a file's metadata fields based on the provided field mask.
// Supports updating: external_metadata, tags
func (ph *PublicHandler) UpdateFile(ctx context.Context, req *artifactpb.UpdateFileRequest) (*artifactpb.UpdateFileResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Validate authentication
	_, err := getUserUIDFromContext(ctx)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get user id from header: %v: %w", err, errorsx.ErrUnauthenticated),
			"Authentication failed. Please log in and try again.",
		)
	}

	// Get namespace
	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("fetching namespace: %w", err),
			"Unable to access the specified namespace. Please check the namespace ID and try again.",
		)
	}

	// Get knowledge base
	kb, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, req.KnowledgeBaseId)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("fetching knowledge base: %w", err),
			"Unable to access the specified knowledge base. Please check the knowledge base ID and try again.",
		)
	}

	// Check ACL permissions
	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", kb.UID, "writer")
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("checking permissions: %w", err),
			"Unable to verify access permissions. Please try again.",
		)
	}
	if !granted {
		return nil, errorsx.AddMessage(
			errorsx.ErrUnauthenticated,
			"You don't have permission to update files in this knowledge base. Please contact the owner for access.",
		)
	}

	// Get file
	fileUID := uuid.FromStringOrNil(req.FileId)
	kbFiles, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []types.FileUIDType{types.FileUIDType(fileUID)})
	if err != nil || len(kbFiles) == 0 {
		return nil, errorsx.AddMessage(
			fmt.Errorf("file not found: %w", errorsx.ErrNotFound),
			"File not found. Please check the file ID and try again.",
		)
	}

	// Build update map based on field mask
	updates := make(map[string]any)

	if req.UpdateMask != nil {
		for _, path := range req.UpdateMask.Paths {
			switch path {
			case "external_metadata":
				// Update external metadata
				updates[repository.KnowledgeBaseFileColumn.ExternalMetadata] = req.File.ExternalMetadata
			case "tags":
				// Update tags
				updates[repository.KnowledgeBaseFileColumn.Tags] = req.File.Tags
			default:
				logger.Warn("unsupported field path in update mask", zap.String("path", path))
			}
		}
	} else {
		// If no update mask, update external metadata by default
		updates[repository.KnowledgeBaseFileColumn.ExternalMetadata] = req.File.ExternalMetadata
	}

	if len(updates) == 0 {
		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: no fields to update", errorsx.ErrInvalidArgument),
			"No valid fields specified for update. Please check the update mask.",
		)
	}

	// Perform the update
	updatedFile, err := ph.service.Repository().UpdateKnowledgeBaseFile(ctx, fileUID.String(), updates)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("updating file: %w", err),
			"Unable to update file. Please try again.",
		)
	}

	// If tags were updated, sync them to Milvus embeddings
	if _, tagsUpdated := updates[repository.KnowledgeBaseFileColumn.Tags]; tagsUpdated {
		// Get the active collection UID for this KB
		collectionID := constant.KBCollectionName(kb.ActiveCollectionUID)

		// Update tags in Milvus for all embeddings of this file
		if err := ph.service.Repository().UpdateEmbeddingTagsForFile(ctx, collectionID, fileUID, updatedFile.Tags); err != nil {
			logger.Warn("Failed to update embedding tags in Milvus (file tags in DB were updated)",
				zap.String("fileUID", fileUID.String()),
				zap.Error(err))
			// Don't fail the request - DB tags were updated successfully
			// Milvus tags will be resynced if file is reprocessed
		}
	}

	// Convert to protobuf
	pbFile := convertKBFileToPB(updatedFile, ns, kb)

	return &artifactpb.UpdateFileResponse{
		File: pbFile,
	}, nil
}

// ReprocessFile triggers reprocessing of a file.
// This will regenerate all converted files, chunks, embeddings with the current KB configuration.
func (ph *PublicHandler) ReprocessFile(ctx context.Context, req *artifactpb.ReprocessFileRequest) (*artifactpb.ReprocessFileResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get user id: %w", errorsx.ErrUnauthenticated),
			"Authentication required. Please log in and try again.",
		)
	}

	// Get namespace
	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		logger.Error("failed to get namespace", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get namespace: %w", err),
			"Unable to access the specified namespace. Please check the namespace ID and try again.",
		)
	}

	// Get knowledge base
	kb, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, req.KnowledgeBaseId)
	if err != nil {
		logger.Error("failed to get knowledge base", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get knowledge base: %w", err),
			"Unable to access the specified knowledge base. Please check the knowledge base ID and try again.",
		)
	}

	// ACL - check user's permission to write knowledge base
	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", kb.UID, "writer")
	if err != nil {
		logger.Error("failed to check permission", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to check permission: %w", err),
			"Unable to verify access permissions. Please try again.",
		)
	}
	if !granted {
		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: no permission to reprocess files", errorsx.ErrUnauthorized),
			"You don't have permission to reprocess files in this knowledge base. Please contact the owner for access.",
		)
	}

	// Get file
	fileUID := uuid.FromStringOrNil(req.FileId)
	if fileUID == uuid.Nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: invalid file ID format", errorsx.ErrInvalidArgument),
			"Invalid file ID. Please provide a valid file identifier.",
		)
	}

	files, err := ph.service.Repository().GetKnowledgeBaseFilesByFileUIDs(ctx, []types.FileUIDType{fileUID})
	if err != nil {
		logger.Error("failed to get file", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get file: %w", err),
			"Unable to find the specified file. It may have been deleted.",
		)
	}
	if len(files) == 0 {
		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: file not found", errorsx.ErrNotFound),
			"Unable to find the specified file. It may have been deleted.",
		)
	}
	file := files[0]

	// Verify file belongs to this KB
	if file.KBUID != kb.UID {
		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: file does not belong to this knowledge base", errorsx.ErrInvalidArgument),
			"The file does not belong to the specified knowledge base.",
		)
	}

	// Log if file is already processing - we allow force reprocessing to handle stuck files
	if file.ProcessStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_PROCESSING.String() ||
		file.ProcessStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING.String() ||
		file.ProcessStatus == artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING.String() {
		logger.Warn("Force reprocessing file that is currently being processed",
			zap.String("fileUID", file.UID.String()),
			zap.String("currentStatus", file.ProcessStatus),
			zap.String("message", "This will restart the file processing workflow from scratch"))
	}

	// CRITICAL: Block reprocessing during validation phase (same as file upload)
	if kb.UpdateStatus == artifactpb.KnowledgeBaseUpdateStatus_KNOWLEDGE_BASE_UPDATE_STATUS_VALIDATING.String() {
		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: knowledge base is in critical update phase", errorsx.ErrRateLimiting),
			"Knowledge base is currently being validated. Please wait a moment and try again.",
		)
	}

	logger.Info("Starting file reprocessing",
		zap.String("fileUID", file.UID.String()),
		zap.String("filename", file.Filename),
		zap.String("currentStatus", file.ProcessStatus),
		zap.String("kbUID", kb.UID.String()))

	// Update file status to PROCESSING before triggering the workflow
	ownerUID := types.UserUIDType(ns.NsUID)
	requesterUID := types.RequesterUIDType(uuid.FromStringOrNil(authUID))

	updatedFiles, err := ph.service.Repository().ProcessKnowledgeBaseFiles(ctx, []string{file.UID.String()}, requesterUID)
	if err != nil {
		logger.Error("Failed to update file status to PROCESSING",
			zap.Error(err),
			zap.String("fileUID", file.UID.String()))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to update file status: %w", err),
			"Unable to prepare file for reprocessing. Please try again.",
		)
	}

	if len(updatedFiles) == 0 {
		return nil, errorsx.AddMessage(
			fmt.Errorf("file not found after status update"),
			"File not found. It may have been deleted.",
		)
	}

	updatedFile := updatedFiles[0]

	// Trigger file processing workflow
	err = ph.service.ProcessFile(ctx, kb.UID, []types.FileUIDType{file.UID}, ownerUID, requesterUID)
	if err != nil {
		logger.Error("Failed to trigger file reprocessing",
			zap.Error(err),
			zap.String("fileUID", file.UID.String()),
			zap.String("filename", file.Filename))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to start reprocessing: %w", err),
			"Unable to start file reprocessing. Please try again.",
		)
	}

	logger.Info("File reprocessing started successfully",
		zap.String("fileUID", file.UID.String()),
		zap.String("filename", file.Filename),
		zap.String("kbUID", kb.UID.String()))

	// Convert to protobuf response with updated file status
	pbFile := convertKBFileToPB(&updatedFile, ns, kb)

	return &artifactpb.ReprocessFileResponse{
		File:    pbFile,
		Message: "File reprocessing started successfully. The file will be reprocessed with the current knowledge base configuration.",
	}, nil
}

// ========================================================================
// HELPER FUNCTIONS
// ========================================================================

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

// detectWebMType inspects WebM file content to determine if it's audio-only or video
// WebM files can contain audio-only or audio+video streams
// Returns FILE_TYPE_WEBM_AUDIO for audio-only, FILE_TYPE_WEBM_VIDEO for video
func detectWebMType(base64Content string) artifactpb.File_Type {
	// Decode the base64 content to inspect the file header
	decoded, err := base64.StdEncoding.DecodeString(base64Content)
	if err != nil {
		// If we can't decode, default to video
		return artifactpb.File_TYPE_WEBM_VIDEO
	}

	// WebM is based on Matroska container format
	// We need at least 512 bytes to reliably detect tracks
	// However, for a simple heuristic, we can check for video codec identifiers in the first few KB
	// Video codecs: VP8 (V_VP8), VP9 (V_VP9), AV1 (V_AV1)
	// Audio codecs: Opus (A_OPUS), Vorbis (A_VORBIS)

	checkSize := min(len(decoded), 8192) // Check first 8KB

	header := string(decoded[:checkSize])

	// Look for video codec identifiers in the header
	// These are typically encoded as ASCII strings in the Matroska/WebM structure
	hasVideo := strings.Contains(header, "V_VP8") ||
		strings.Contains(header, "V_VP9") ||
		strings.Contains(header, "V_AV1") ||
		strings.Contains(header, "V_MPEG4")

	if hasVideo {
		return artifactpb.File_TYPE_WEBM_VIDEO
	}

	// If no video codec found, treat as audio-only
	return artifactpb.File_TYPE_WEBM_AUDIO
}

func determineFileType(filename string) artifactpb.File_Type {
	// Use centralized file type detection from x/file package
	// Pass empty contentType since this function only has filename
	return filetype.DetermineFileType("", filename)
}

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

// Check if objectUID is provided, and all other required fields if not
func checkUploadKnowledgeBaseFileRequest(req *artifactpb.CreateFileRequest) (hasObject bool, _ error) {
	if req.GetNamespaceId() == "" {
		return false, fmt.Errorf("%w: owner UID is required", errorsx.ErrInvalidArgument)
	}

	if req.GetKnowledgeBaseId() == "" {
		return false, fmt.Errorf("%w: knowledge base UID is required", errorsx.ErrInvalidArgument)
	}

	if req.GetFile().GetObjectUid() == "" {
		// File upload doesn't reference object, so request must contain the
		// file contents.
		if req.GetFile().GetFilename() == "" {
			return false, fmt.Errorf("%w: filename is required", errorsx.ErrInvalidArgument)
		}
		if req.GetFile().GetContent() == "" {
			return false, fmt.Errorf("%w: file content is required", errorsx.ErrInvalidArgument)
		}

		return false, nil
	}

	return true, nil
}

// MoveFileToKnowledgeBase moves a file from one knowledge base to another within the same namespace.
// It copies the file content and metadata to the target knowledge base and deletes
// the file from the source knowledge base.
func (ph *PublicHandler) uploadBase64FileToMinIO(ctx context.Context, nsID string, nsUID, creatorUID types.CreatorUIDType, filename string, content string, fileType artifactpb.File_Type) (types.ObjectUIDType, error) {
	logger, _ := logx.GetZapLogger(ctx)
	response, err := ph.service.GetUploadURL(ctx, &artifactpb.GetObjectUploadURLRequest{
		NamespaceId: nsID,
		ObjectName:  filename,
	}, nsUID, filename, creatorUID)
	if err != nil {
		logger.Error("failed to get upload URL", zap.Error(err))
		return uuid.Nil, errorsx.AddMessage(
			fmt.Errorf("failed to get upload URL: %w", err),
			"Unable to prepare file upload. Please try again.",
		)
	}
	objectUID := uuid.FromStringOrNil(response.Object.Uid)
	destination := object.GetBlobObjectPath(nsUID, objectUID)
	err = ph.service.Repository().GetMinIOStorage().UploadBase64File(ctx, object.BlobBucketName, destination, content, filetype.FileTypeToMimeType(fileType))
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
