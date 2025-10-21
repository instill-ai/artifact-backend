package handler

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"time"

	"github.com/gofrs/uuid"

	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"

	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/types"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	constantx "github.com/instill-ai/x/constant"
	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
)

var alphabet = "abcdefghijklmnopqrstuvwxyz"

type ErrorMsg map[int]string

const ErrorCreateKnowledgeBaseMsg = "failed to create catalog: %w"
const ErrorListKnowledgeBasesMsg = "failed to get catalogs: %w "
const ErrorUpdateKnowledgeBaseMsg = "failed to update catalog: %w"
const ErrorDeleteKnowledgeBaseMsg = "failed to delete catalog: %w"

// CreateCatalog creates a catalog
func (ph *PublicHandler) CreateCatalog(ctx context.Context, req *artifactpb.CreateCatalogRequest) (*artifactpb.CreateCatalogResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		err := fmt.Errorf("failed to get user id from header: %v. err: %w", err, errorsx.ErrUnauthenticated)
		return nil, err
	}

	// ACL  check user's permission to create catalog in the user or org context(namespace)
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

	// check name if it is empty
	if req.Name == "" {
		req.Name = generateID()
	}
	nameOk := isValidName(req.Name)
	if !nameOk {
		msg := "the catalog name should be lowercase without any space or special character besides the hyphen, " +
			"it can not start with number or hyphen, and should be less than 32 characters. name: %v. err: %w"
		return nil, fmt.Errorf(msg, req.Name, errorsx.ErrInvalidArgument)
	}

	creatorUUID, err := uuid.FromString(authUID)
	if err != nil {
		logger.Error("failed to parse creator uid", zap.String("uid", authUID), zap.Error(err))
		return nil, err
	}

	// Get default embedding configuration from system_metadata
	// This ensures new KBs use the current system-wide standard
	// (e.g., OpenAI/1536 during migration, Gemini/3072 after migration)
	defaultConfig, err := ph.service.Repository().GetDefaultEmbeddingConfig(ctx, "default")
	if err != nil {
		return nil, fmt.Errorf("getting default embedding config: %w", err)
	}

	// external service call - create catalog collection and set ACL in openFAG
	callExternalService := func(kbUID types.KBUIDType) error {
		// Create collection with dimensionality from default embedding config
		err := ph.service.Repository().CreateCollection(ctx, constant.KBCollectionName(kbUID), defaultConfig.Dimensionality)
		if err != nil {
			return fmt.Errorf("creating vector database collection: %w", err)
		}

		// set the owner of the catalog
		err = ph.service.ACLClient().SetOwner(ctx, "knowledgebase", kbUID, string(ns.NsType), ns.NsUID)
		if err != nil {
			return fmt.Errorf("setting catalog owner: %w", err)
		}

		return nil
	}

	// if catalog type is not set, set it to persistent
	if req.GetType() == artifactpb.CatalogType_CATALOG_TYPE_UNSPECIFIED {
		req.Type = artifactpb.CatalogType_CATALOG_TYPE_PERSISTENT
	}

	// create catalog

	dbData, err := ph.service.Repository().CreateKnowledgeBase(
		ctx,
		repository.KnowledgeBaseModel{
			Name: req.Name,
			// make name as kbID
			KBID:            req.Name,
			Description:     req.Description,
			Tags:            req.Tags,
			Owner:           ns.NsUID.String(),
			CreatorUID:      creatorUUID,
			CatalogType:     req.GetType().String(),
			EmbeddingConfig: *defaultConfig, // Use system default
		},
		callExternalService,
	)
	if err != nil {
		return nil, err
	}

	activeCollectionUID := dbData.ActiveCollectionUID.String()

	catalog := &artifactpb.Catalog{
		Name:                dbData.Name,
		CatalogUid:          dbData.UID.String(),
		CatalogId:           dbData.KBID,
		Description:         dbData.Description,
		Tags:                dbData.Tags,
		OwnerName:           dbData.Owner,
		CreateTime:          dbData.CreateTime.String(),
		UpdateTime:          dbData.UpdateTime.String(),
		DownstreamApps:      []string{},
		TotalFiles:          0,
		TotalTokens:         0,
		UsedStorage:         0,
		ActiveCollectionUid: activeCollectionUID,
		EmbeddingConfig: &artifactpb.Catalog_EmbeddingConfig{
			ModelFamily:    dbData.EmbeddingConfig.ModelFamily,
			Dimensionality: dbData.EmbeddingConfig.Dimensionality,
		},
	}

	return &artifactpb.CreateCatalogResponse{Catalog: catalog}, nil
}

// ListCatalogs lists the catalogs
func (ph *PublicHandler) ListCatalogs(ctx context.Context, req *artifactpb.ListCatalogsRequest) (*artifactpb.ListCatalogsResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	// get user id from context
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {

		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}

	// ACL - check user(authUid)'s permission to list catalogs in
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

	dbData, err := ph.service.Repository().ListKnowledgeBasesByCatalogType(ctx, ns.NsUID.String(), artifactpb.CatalogType_CATALOG_TYPE_PERSISTENT)
	if err != nil {
		logger.Error("failed to get catalogs", zap.Error(err))
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
	kbs := make([]*artifactpb.Catalog, len(dbData))
	for i, kb := range dbData {
		activeCollectionUID := kb.ActiveCollectionUID.String()

		kbs[i] = &artifactpb.Catalog{
			CatalogUid:          kb.UID.String(),
			Name:                kb.Name,
			CatalogId:           kb.KBID,
			Description:         kb.Description,
			Tags:                kb.Tags,
			CreateTime:          kb.CreateTime.String(),
			UpdateTime:          kb.UpdateTime.String(),
			OwnerName:           kb.Owner,
			DownstreamApps:      []string{},
			TotalFiles:          uint32(fileCounts[kb.UID]),
			TotalTokens:         uint32(tokenCounts[kb.UID]),
			UsedStorage:         uint64(kb.Usage),
			ActiveCollectionUid: activeCollectionUID,
			EmbeddingConfig: &artifactpb.Catalog_EmbeddingConfig{
				ModelFamily:    kb.EmbeddingConfig.ModelFamily,
				Dimensionality: kb.EmbeddingConfig.Dimensionality,
			},
		}

	}
	return &artifactpb.ListCatalogsResponse{
		Catalogs: kbs,
	}, nil
}

// UpdateCatalog updates a catalog
func (ph *PublicHandler) UpdateCatalog(ctx context.Context, req *artifactpb.UpdateCatalogRequest) (*artifactpb.UpdateCatalogResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		logger.Error("failed to get user id from header", zap.Error(err))
		return nil, err
	}
	// check name if it is empty
	if req.CatalogId == "" {
		logger.Error("KBID is empty", zap.Error(errorsx.ErrInvalidArgument))
		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: KBID is empty", errorsx.ErrInvalidArgument),
			"Catalog ID is required. Please provide a catalog ID.",
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
	// ACL - check user's permission to update catalog
	kb, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, req.CatalogId)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf(ErrorListKnowledgeBasesMsg, err),
			"Unable to access the specified catalog. Please check the catalog ID and try again.",
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
			fmt.Errorf("%w: no permission over catalog", errorsx.ErrUnauthorized),
			"You don't have permission to update this catalog. Please contact the owner for access.",
		)
	}

	// update catalog
	kb, err = ph.service.Repository().UpdateKnowledgeBase(
		ctx,
		req.GetCatalogId(),
		ns.NsUID.String(),
		repository.KnowledgeBaseModel{
			Description: req.GetDescription(),
			Tags:        req.GetTags(),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("updating catalog: %w", err)
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
	catalog := &artifactpb.Catalog{
		Name:           kb.Name,
		CatalogId:      kb.KBID,
		Description:    kb.Description,
		Tags:           kb.Tags,
		CreateTime:     kb.CreateTime.String(),
		UpdateTime:     kb.UpdateTime.String(),
		OwnerName:      kb.Owner,
		DownstreamApps: []string{},
		TotalFiles:     uint32(fileCounts[kb.UID]),
		TotalTokens:    uint32(tokenCounts[kb.UID]),
		UsedStorage:    uint64(kb.Usage),
		EmbeddingConfig: &artifactpb.Catalog_EmbeddingConfig{
			ModelFamily:    kb.EmbeddingConfig.ModelFamily,
			Dimensionality: kb.EmbeddingConfig.Dimensionality,
		},
	}

	return &artifactpb.UpdateCatalogResponse{Catalog: catalog}, nil
}

// DeleteCatalog deletes a catalog
func (ph *PublicHandler) DeleteCatalog(ctx context.Context, req *artifactpb.DeleteCatalogRequest) (*artifactpb.DeleteCatalogResponse, error) {
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
			"You don't have permission to delete this catalog. Please contact the owner for access.",
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
			fmt.Errorf("%w: catalog update in progress", errorsx.ErrRateLimiting),
			fmt.Sprintf("Catalog is currently being updated (status: %s). Please wait for the update to complete or abort it before deleting.", kb.UpdateStatus),
		)
	}

	// CRITICAL: Also check if there's an active workflow for production KBs
	// This protects against race conditions where the workflow has started but status
	// hasn't been written to DB yet (or is being retried after a failure)
	// NOTE: Staging/rollback KBs are exempt - they're temporary and should be deletable
	if !kb.Staging && kb.UpdateWorkflowID != "" {
		// Verify the workflow is actually running (not completed/failed)
		// If we can't check the workflow status, block deletion to be safe
		logger.Warn("Catalog has an active workflow ID, blocking deletion for safety",
			zap.String("catalogID", req.CatalogId),
			zap.String("catalogUID", kb.UID.String()),
			zap.String("workflowID", kb.UpdateWorkflowID))

		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: catalog update workflow is active", errorsx.ErrRateLimiting),
			fmt.Sprintf("Catalog has an active update workflow (ID: %s). Please wait for the update to complete or abort it before deleting.", kb.UpdateWorkflowID),
		)
	}

	// CRITICAL: Wait for any in-progress file processing to complete before cleanup
	// This prevents race conditions where file workflows try to access resources
	// (Milvus collections, DB records) that are being deleted by cleanup workflow
	inProgressStatuses := artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_PROCESSING.String() + "," +
		artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CONVERTING.String() + "," +
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
		logger.Warn("Catalog has in-progress file operations, blocking deletion",
			zap.String("catalogID", req.CatalogId),
			zap.String("catalogUID", kb.UID.String()),
			zap.Int64("inProgressCount", inProgressCount))

		return nil, errorsx.AddMessage(
			fmt.Errorf("%w: files are being processed", errorsx.ErrRateLimiting),
			fmt.Sprintf("Catalog has %d files currently being processed. Please wait for processing to complete before deleting, or cancel the file processing operations first.", inProgressCount),
		)
	}

	deletedKb, err := ph.service.Repository().DeleteKnowledgeBase(ctx, ns.NsUID.String(), req.CatalogId)
	if err != nil {
		logger.Error("failed to delete catalog", zap.Error(err))
		return nil, err
	}

	// Trigger Temporal workflow for background cleanup
	// At this point, we've verified no files are actively processing and no update is in progress
	if err := ph.service.CleanupKnowledgeBase(ctx, kb.UID); err != nil {
		logger.Error("failed to trigger cleanup workflow", zap.Error(err), zap.String("catalog_id", kb.UID.String()))
		// Don't fail the request - cleanup will be retried by Temporal
	}

	return &artifactpb.DeleteCatalogResponse{
		Catalog: &artifactpb.Catalog{
			Name:           deletedKb.Name,
			CatalogId:      deletedKb.KBID,
			Description:    deletedKb.Description,
			Tags:           deletedKb.Tags,
			CreateTime:     deletedKb.CreateTime.String(),
			UpdateTime:     deletedKb.UpdateTime.String(),
			OwnerName:      deletedKb.Owner,
			DownstreamApps: []string{},
			TotalFiles:     0,
			TotalTokens:    0,
			UsedStorage:    0,
			EmbeddingConfig: &artifactpb.Catalog_EmbeddingConfig{
				ModelFamily:    deletedKb.EmbeddingConfig.ModelFamily,
				Dimensionality: deletedKb.EmbeddingConfig.Dimensionality,
			},
		},
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

func generateID() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	id := make([]byte, 8)
	for i := range id {
		id[i] = alphabet[r.Intn(len(alphabet))]
	}

	return string(id)
}
