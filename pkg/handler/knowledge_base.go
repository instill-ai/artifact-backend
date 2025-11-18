package handler

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/gofrs/uuid"

	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	constantx "github.com/instill-ai/x/constant"
	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
)

type ErrorMsg map[int]string

const ErrorCreateKnowledgeBaseMsg = "failed to create knowledge base: %w"
const ErrorListKnowledgeBasesMsg = "failed to get knowledge bases: %w "
const ErrorUpdateKnowledgeBaseMsg = "failed to update knowledge base: %w"
const ErrorDeleteKnowledgeBaseMsg = "failed to delete knowledge base: %w"

// CreateKnowledgeBase creates a knowledge base
func (ph *PublicHandler) CreateKnowledgeBase(ctx context.Context, req *artifactpb.CreateKnowledgeBaseRequest) (*artifactpb.CreateKnowledgeBaseResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		err := fmt.Errorf("failed to get user id from header: %v. err: %w", err, errorsx.ErrUnauthenticated)
		return nil, err
	}

	// ACL  check user's permission to create knowledge base in the user or org context(namespace)
	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		logger.Error(
			"failed to get namespace",
			zap.Error(err),
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
			zap.String("auth_uid", authUID))
		return nil, fmt.Errorf(ErrorCreateKnowledgeBaseMsg, err)
	}
	err = ph.service.CheckNamespacePermission(ctx, ns)
	if err != nil {
		logger.Error(
			"failed to check namespace permission",
			zap.Error(err),
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
			zap.String("auth_uid", authUID))
		return nil, fmt.Errorf(ErrorCreateKnowledgeBaseMsg, err)
	}

	// check id if it is empty
	if req.Id == "" {
		return nil, fmt.Errorf("knowledge base id is required. err: %w", errorsx.ErrInvalidArgument)
	}
	nameOk := isValidName(req.Id)
	if !nameOk {
		msg := "the knowledge base id should be lowercase without any space or special character besides the hyphen, " +
			"it can not start with number or hyphen, and should be less than 32 characters. id: %v. err: %w"
		return nil, fmt.Errorf(msg, req.Id, errorsx.ErrInvalidArgument)
	}

	creatorUUID, err := uuid.FromString(authUID)
	if err != nil {
		logger.Error("failed to parse creator uid", zap.String("uid", authUID), zap.Error(err))
		return nil, err
	}

	// Determine system to use
	// System ID defines how the knowledge base will be created based on the system's
	// RAG configurations including AI model family, embedding vector dimensionality,
	// chunking method, and other RAG-related settings
	var system *repository.SystemModel
	if req.SystemId != nil && *req.SystemId != "" {
		// Get system record from system table using the specified ID
		// This retrieves both the UID (for FK) and config (for dimensionality)
		system, err = ph.service.Repository().GetSystem(ctx, *req.SystemId)
	} else {
		// No system_id specified, use the default system
		system, err = ph.service.Repository().GetDefaultSystem(ctx)
	}
	if err != nil {
		if req.SystemId != nil && *req.SystemId != "" {
			return nil, fmt.Errorf("getting system for ID %q: %w", *req.SystemId, err)
		}
		return nil, fmt.Errorf("getting default system: %w", err)
	}

	systemConfig, err := system.GetConfigJSON()
	if err != nil {
		return nil, fmt.Errorf("parsing system config for ID %q: %w", system.ID, err)
	}

	logger.Info("Using system config",
		zap.String("systemID", system.ID),
		zap.Bool("is_default", system.IsDefault),
		zap.String("modelFamily", systemConfig.RAG.Embedding.ModelFamily),
		zap.Uint32("dimensionality", systemConfig.RAG.Embedding.Dimensionality))

	// external service call - create knowledge base collection and set ACL in openFGA
	// CRITICAL: collectionUID is passed directly from the transaction (can't query KB - it's uncommitted!)
	callExternalService := func(kbUID types.KBUIDType, collectionUID types.KBUIDType) error {
		// Create collection with active_collection_uid (not kb.UID!)
		// After Fix 3, active_collection_uid is always unique (NOT equal to kb.UID)
		err := ph.service.Repository().CreateCollection(ctx, constant.KBCollectionName(collectionUID), systemConfig.RAG.Embedding.Dimensionality)
		if err != nil {
			return fmt.Errorf("creating vector database collection: %w", err)
		}

		// Set ACL owner with KB UID (not active_collection_uid)
		err = ph.service.ACLClient().SetOwner(ctx, "knowledgebase", kbUID, string(ns.NsType), ns.NsUID)
		if err != nil {
			return fmt.Errorf("setting knowledge base owner: %w", err)
		}

		return nil
	}

	// if knowledge base type is not set, set it to persistent
	if req.GetType() == artifactpb.KnowledgeBaseType_KNOWLEDGE_BASE_TYPE_UNSPECIFIED {
		req.Type = artifactpb.KnowledgeBaseType_KNOWLEDGE_BASE_TYPE_PERSISTENT
	}

	// create knowledge base

	dbData, err := ph.service.Repository().CreateKnowledgeBase(
		ctx,
		repository.KnowledgeBaseModel{
			KBID:              req.Id,
			Description:       req.Description,
			Tags:              req.Tags,
			Owner:             ns.NsUID.String(),
			CreatorUID:        creatorUUID,
			KnowledgeBaseType: req.GetType().String(),
			SystemUID:         system.UID,
		},
		callExternalService,
	)
	if err != nil {
		return nil, err
	}

	activeCollectionUID := dbData.ActiveCollectionUID.String()

	knowledgeBase := &artifactpb.KnowledgeBase{
		Name:                fmt.Sprintf("namespaces/%s/knowledge-bases/%s", req.GetNamespaceId(), dbData.KBID),
		Uid:                 dbData.UID.String(),
		Id:                  dbData.KBID,
		Description:         dbData.Description,
		Tags:                dbData.Tags,
		OwnerName:           dbData.Owner,
		CreateTime:          timestamppb.New(*dbData.CreateTime),
		UpdateTime:          timestamppb.New(*dbData.UpdateTime),
		DownstreamApps:      []string{},
		TotalFiles:          0,
		TotalTokens:         0,
		UsedStorage:         0,
		ActiveCollectionUid: activeCollectionUID,
		EmbeddingConfig: &artifactpb.KnowledgeBase_EmbeddingConfig{
			ModelFamily:    systemConfig.RAG.Embedding.ModelFamily,
			Dimensionality: systemConfig.RAG.Embedding.Dimensionality,
		},
	}

	return &artifactpb.CreateKnowledgeBaseResponse{KnowledgeBase: knowledgeBase}, nil
}

// ListKnowledgeBases lists the knowledge bases
func (ph *PublicHandler) ListKnowledgeBases(ctx context.Context, req *artifactpb.ListKnowledgeBasesRequest) (*artifactpb.ListKnowledgeBasesResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	// get user id from context
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {

		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}

	// ACL - check user(authUid)'s permission to list knowledge bases in
	// the user or org context(namespace)
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
	err = ph.service.CheckNamespacePermission(ctx, ns)
	if err != nil {
		logger.Error(
			"failed to check namespace permission",
			zap.Error(err),
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
			zap.String("auth_uid", authUID))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to check namespace permission: %w", err),
			"You don't have permission to access this namespace. Please contact the owner for access.",
		)
	}

	// Use ListKnowledgeBasesByTypeWithConfig to get KBs with their system configs
	dbData, err := ph.service.Repository().ListKnowledgeBasesByTypeWithConfig(ctx, ns.NsUID.String(), artifactpb.KnowledgeBaseType_KNOWLEDGE_BASE_TYPE_PERSISTENT)
	if err != nil {
		logger.Error("failed to get knowledge bases", zap.Error(err))
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}

	kbUIDs := make([]types.KBUIDType, len(dbData))
	for i, kb := range dbData {
		kbUIDs[i] = kb.UID
	}

	// Get file counts for each KB
	fileCounts := make(map[types.KBUIDType]int64)
	for _, kbUID := range kbUIDs {
		count, err := ph.service.Repository().GetFileCountByKnowledgeBaseUID(ctx, kbUID, "")
		if err != nil {
			logger.Error("failed to get file count", zap.Error(err), zap.String("kbUID", kbUID.String()))
			return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
		}
		fileCounts[kbUID] = count
	}

	tokenCounts, err := ph.service.Repository().GetTotalTokensByListKBUIDs(ctx, kbUIDs)
	if err != nil {
		logger.Error("failed to get token counts", zap.Error(err))
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}
	kbs := make([]*artifactpb.KnowledgeBase, len(dbData))
	for i, kb := range dbData {
		activeCollectionUID := kb.ActiveCollectionUID.String()

		kbs[i] = &artifactpb.KnowledgeBase{
			Uid:                 kb.UID.String(),
			Name:                fmt.Sprintf("namespaces/%s/knowledge-bases/%s", req.GetNamespaceId(), kb.KBID),
			Id:                  kb.KBID,
			Description:         kb.Description,
			Tags:                kb.Tags,
			CreateTime:          timestamppb.New(*kb.CreateTime),
			UpdateTime:          timestamppb.New(*kb.UpdateTime),
			OwnerName:           kb.Owner,
			DownstreamApps:      []string{},
			TotalFiles:          uint32(fileCounts[kb.UID]),
			TotalTokens:         uint32(tokenCounts[kb.UID]),
			UsedStorage:         uint64(kb.Usage),
			ActiveCollectionUid: activeCollectionUID,
			EmbeddingConfig: &artifactpb.KnowledgeBase_EmbeddingConfig{
				ModelFamily:    kb.SystemConfig.RAG.Embedding.ModelFamily,
				Dimensionality: kb.SystemConfig.RAG.Embedding.Dimensionality,
			},
		}

	}
	return &artifactpb.ListKnowledgeBasesResponse{
		KnowledgeBases: kbs,
	}, nil
}

// UpdateKnowledgeBase updates a knowledge base
func (ph *PublicHandler) UpdateKnowledgeBase(ctx context.Context, req *artifactpb.UpdateKnowledgeBaseRequest) (*artifactpb.UpdateKnowledgeBaseResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		logger.Error("failed to get user id from header", zap.Error(err))
		return nil, err
	}
	// check name if it is empty
	if req.KnowledgeBaseId == "" {
		logger.Error("KBID is empty", zap.Error(errorsx.ErrInvalidArgument))
		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: KBID is empty", errorsx.ErrInvalidArgument),
			"Knowledge Base ID is required. Please provide a knowledge base ID.",
		)
	}

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
	// ACL - check user's permission to update knowledge base
	kb, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, req.KnowledgeBaseId)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf(ErrorListKnowledgeBasesMsg, err),
			"Unable to access the specified knowledge base. Please check the knowledge base ID and try again.",
		)
	}
	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", kb.UID, "writer")
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err),
			"Unable to verify access permissions. Please try again.",
		)
	}
	if !granted {
		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: no permission over knowledge base", errorsx.ErrUnauthorized),
			"You don't have permission to update this knowledge base. Please contact the owner for access.",
		)
	}

	// update knowledge base
	kb, err = ph.service.Repository().UpdateKnowledgeBase(
		ctx,
		req.GetKnowledgeBaseId(),
		ns.NsUID.String(),
		repository.KnowledgeBaseModel{
			Description: req.GetKnowledgeBase().GetDescription(),
			Tags:        req.GetKnowledgeBase().GetTags(),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("updating knowledge base: %w", err)
	}

	// Fetch KB with system config for the response
	kbWithConfig, err := ph.service.Repository().GetKnowledgeBaseByUIDWithConfig(ctx, kb.UID)
	if err != nil {
		return nil, fmt.Errorf("failed to get KB with config: %w", err)
	}

	fileCount, err := ph.service.Repository().GetFileCountByKnowledgeBaseUID(ctx, kb.UID, "")
	if err != nil {
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}
	fileCounts := map[types.KBUIDType]int64{kb.UID: fileCount}

	tokenCounts, err := ph.service.Repository().GetTotalTokensByListKBUIDs(ctx, []types.KBUIDType{kb.UID})
	if err != nil {
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}

	// populate response
	knowledgeBase := &artifactpb.KnowledgeBase{
		Name:           fmt.Sprintf("namespaces/%s/knowledge-bases/%s", req.GetNamespaceId(), kb.KBID),
		Uid:            kb.UID.String(),
		Id:             kb.KBID,
		Description:    kb.Description,
		Tags:           kb.Tags,
		CreateTime:     timestamppb.New(*kb.CreateTime),
		UpdateTime:     timestamppb.New(*kb.UpdateTime),
		OwnerName:      kb.Owner,
		DownstreamApps: []string{},
		TotalFiles:     uint32(fileCounts[kb.UID]),
		TotalTokens:    uint32(tokenCounts[kb.UID]),
		UsedStorage:    uint64(kb.Usage),
		EmbeddingConfig: &artifactpb.KnowledgeBase_EmbeddingConfig{
			ModelFamily:    kbWithConfig.SystemConfig.RAG.Embedding.ModelFamily,
			Dimensionality: kbWithConfig.SystemConfig.RAG.Embedding.Dimensionality,
		},
	}

	return &artifactpb.UpdateKnowledgeBaseResponse{KnowledgeBase: knowledgeBase}, nil
}

// DeleteKnowledgeBase deletes a knowledge base
func (ph *PublicHandler) DeleteKnowledgeBase(ctx context.Context, req *artifactpb.DeleteKnowledgeBaseRequest) (*artifactpb.DeleteKnowledgeBaseResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
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
			"You don't have permission to delete this knowledge base. Please contact the owner for access.",
		)
	}

	// CRITICAL: Block deletion if update workflow is in progress (for production KBs only)
	// Deleting a production KB during an update can cause race conditions where:
	// - File processing workflows try to save to dropped Milvus collections
	// - Staging/rollback KBs become orphaned
	// - Validation fails with inconsistent state
	// NOTE: Staging KBs are exempt from this check as they're temporary resources
	if !kb.Staging && repository.IsUpdateInProgress(kb.UpdateStatus) {
		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: knowledge base update in progress", errorsx.ErrRateLimiting),
			fmt.Sprintf("Knowledge base is currently being updated (status: %s). Please wait for the update to complete or abort it before deleting.", kb.UpdateStatus),
		)
	}

	// CRITICAL: Also check if there's an active workflow for production KBs
	// This protects against race conditions where the workflow has started but status
	// hasn't been written to DB yet (or is being retried after a failure)
	// NOTE: Staging/rollback KBs are exempt - they're temporary and should be deletable
	if !kb.Staging && kb.UpdateWorkflowID != "" {
		// Verify the workflow is actually running (not completed/failed)
		// If we can't check the workflow status, block deletion to be safe
		logger.Warn("Knowledge base has an active workflow ID, blocking deletion for safety",
			zap.String("knowledgeBaseID", req.KnowledgeBaseId),
			zap.String("knowledgeBaseUID", kb.UID.String()),
			zap.String("workflowID", kb.UpdateWorkflowID))

		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: knowledge base update workflow is active", errorsx.ErrRateLimiting),
			fmt.Sprintf("Knowledge base has an active update workflow (ID: %s). Please wait for the update to complete or abort it before deleting.", kb.UpdateWorkflowID),
		)
	}

	// CRITICAL: For production KBs, block deletion if files are being processed
	// For staging/rollback KBs, deletion will use a transaction with row-level locking
	// to atomically delete the KB and CASCADE delete all files
	if !kb.Staging {
		inProgressStatuses := artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_PROCESSING.String() + "," +
			artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING.String() + "," +
			artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING.String()

		inProgressCount, err := ph.service.Repository().GetFileCountByKnowledgeBaseUID(ctx, kb.UID, inProgressStatuses)
		if err != nil {
			logger.Error("failed to check for in-progress files", zap.Error(err))
			return nil, errorsx.AddMessage(
				fmt.Errorf("failed to verify file processing status: %w", err),
				"Unable to verify if files are being processed. Please try again.",
			)
		}

		if inProgressCount > 0 {
			// Production KB: Block deletion to protect user data
			logger.Warn("Production knowledge base has in-progress file operations, blocking deletion",
				zap.String("knowledgeBaseID", req.KnowledgeBaseId),
				zap.String("knowledgeBaseUID", kb.UID.String()),
				zap.Int64("inProgressCount", inProgressCount))

			return nil, errorsx.AddMessage(
				fmt.Errorf("%w: files are being processed", errorsx.ErrRateLimiting),
				fmt.Sprintf("Knowledge base has %d files currently being processed. Please wait for processing to complete before deleting, or cancel the file processing operations first.", inProgressCount),
			)
		}
	}

	deletedKb, err := ph.service.Repository().DeleteKnowledgeBase(ctx, ns.NsUID.String(), req.KnowledgeBaseId)
	if err != nil {
		logger.Error("failed to delete knowledge base", zap.Error(err))
		return nil, err
	}

	// Get system configuration for the response
	system, err := ph.service.Repository().GetSystemByUID(ctx, deletedKb.SystemUID)
	if err != nil {
		logger.Error("failed to get system config for deleted knowledge base", zap.Error(err))
		return nil, fmt.Errorf("failed to retrieve system config: %w", err)
	}
	systemConfig, err := system.GetConfigJSON()
	if err != nil {
		logger.Error("failed to parse system config for deleted knowledge base", zap.Error(err))
		return nil, fmt.Errorf("failed to parse system config: %w", err)
	}

	// Trigger Temporal workflow for background cleanup
	// At this point, we've verified no files are actively processing and no update is in progress
	if err := ph.service.CleanupKnowledgeBase(ctx, kb.UID); err != nil {
		logger.Error("failed to trigger cleanup workflow", zap.Error(err), zap.String("knowledge_base_id", kb.UID.String()))
		// Don't fail the request - cleanup will be retried by Temporal
	}

	return &artifactpb.DeleteKnowledgeBaseResponse{
		KnowledgeBase: &artifactpb.KnowledgeBase{
			Name:           fmt.Sprintf("namespaces/%s/knowledge-bases/%s", req.GetNamespaceId(), deletedKb.KBID),
			Uid:            deletedKb.UID.String(),
			Id:             deletedKb.KBID,
			Description:    deletedKb.Description,
			Tags:           deletedKb.Tags,
			CreateTime:     timestamppb.New(*deletedKb.CreateTime),
			UpdateTime:     timestamppb.New(*deletedKb.UpdateTime),
			OwnerName:      deletedKb.Owner,
			DownstreamApps: []string{},
			TotalFiles:     0,
			TotalTokens:    0,
			UsedStorage:    0,
			EmbeddingConfig: &artifactpb.KnowledgeBase_EmbeddingConfig{
				ModelFamily:    systemConfig.RAG.Embedding.ModelFamily,
				Dimensionality: systemConfig.RAG.Embedding.Dimensionality,
			},
		},
	}, nil
}

// GetKnowledgeBase returns the details of a specific knowledge base
func (ph *PublicHandler) GetKnowledgeBase(ctx context.Context, req *artifactpb.GetKnowledgeBaseRequest) (*artifactpb.GetKnowledgeBaseResponse, error) {
	// Get namespace
	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		return nil, fmt.Errorf("getting namespace: %w", err)
	}

	// Get knowledge base from repository
	kb, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, req.GetKnowledgeBaseId())
	if err != nil {
		return nil, fmt.Errorf("fetching knowledge base: %w", err)
	}

	// Check permissions
	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", kb.UID, "reader")
	switch {
	case err != nil:
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err)
	case !granted:
		return nil, fmt.Errorf("%w: no permission over knowledge base", errorsx.ErrUnauthorized)
	}

	// Convert to protobuf using the new converter
	pbKnowledgeBase := convertKBToCatalogPB(kb, ns)

	return &artifactpb.GetKnowledgeBaseResponse{
		KnowledgeBase: pbKnowledgeBase,
	}, nil
}

func getUserUIDFromContext(ctx context.Context) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if v, ok := md[strings.ToLower(constantx.HeaderUserUIDKey)]; ok {
		return v[0], nil
	}
	return "", errorsx.AddMessage(
		fmt.Errorf("user id not found in context: %w", errorsx.ErrUnauthenticated),
		"Authentication failed. Please log in and try again.",
	)
}

// The ID should be lowercase without any space or special character besides
// the hyphen, it can not start with number or hyphen, and should be less
// than 32 characters.
func isValidName(name string) bool {
	// Define the regular expression pattern
	pattern := "^[a-z][-a-z_0-9]{0,31}$"
	// Compile the regular expression
	re := regexp.MustCompile(pattern)
	// Match the name against the regular expression
	return re.MatchString(name)
}
