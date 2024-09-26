package handler

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/customerror"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"
	"github.com/instill-ai/artifact-backend/pkg/utils"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

type ErrorMsg map[int]string

const ErrorCreateKnowledgeBaseMsg = "failed to create catalog: %w"
const ErrorListKnowledgeBasesMsg = "failed to get catalogs: %w "
const ErrorUpdateKnowledgeBaseMsg = "failed to update catalog: %w"
const ErrorDeleteKnowledgeBaseMsg = "failed to delete catalog: %w"

// Note: in the future, we might have different max count for different user types
const KnowledgeBaseMaxCount = 3

func (ph *PublicHandler) CreateCatalog(ctx context.Context, req *artifactpb.CreateCatalogRequest) (*artifactpb.CreateCatalogResponse, error) {
	log, _ := logger.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		err := fmt.Errorf("failed to get user id from header: %v. err: %w", err, customerror.ErrUnauthenticated)
		return nil, err
	}
	startTime := time.Now()

	// ACL  check user's permission to create catalog in the user or org context(namespace)
	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		log.Error(
			"failed to check namespace permission",
			zap.Error(err),
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
			zap.String("auth_uid", authUID))
		return nil, fmt.Errorf(ErrorCreateKnowledgeBaseMsg, err)
	}
	err = ph.service.CheckNamespacePermission(ctx, ns)
	if err != nil {
		log.Error(
			"failed to check namespace permission",
			zap.Error(err),
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
			zap.String("auth_uid", authUID))
		return nil, fmt.Errorf(ErrorCreateKnowledgeBaseMsg, err)
	}
	// check if user has reached the maximum number of catalogs
	// note: the simple implementation have race condition to bypass the check,
	// but it is okay for now
	kbCount, err := ph.service.Repository.GetKnowledgeBaseCountByOwner(ctx, ns.NsUID.String())
	if err != nil {
		log.Error("failed to get catalog count", zap.Error(err))
		return nil, fmt.Errorf(ErrorCreateKnowledgeBaseMsg, err)
	}
	tier, err := ph.service.GetNamespaceTier(ctx, ns)
	if err != nil {
		log.Error("failed to get namespace tier", zap.Error(err))
		return nil, fmt.Errorf(ErrorCreateKnowledgeBaseMsg, err)
	}
	if kbCount >= int64(tier.GetPrivateCatalogLimit()) {
		err := fmt.Errorf(
			"user has reached the %v maximum number of catalogs. current tier:%v ",
			tier.GetPrivateCatalogLimit(), tier)
		return nil, err
	}

	// check name if it is empty
	if req.Name == "" {
		err := fmt.Errorf("name is required. err: %w", ErrCheckRequiredFields)
		return nil, err
	}
	nameOk := isValidName(req.Name)
	if !nameOk {
		msg := "the catalog name should be lowercase without any space or special character besides the hyphen, " +
			"it can not start with number or hyphen, and should be less than 32 characters. name: %v. err: %w"
		return nil, fmt.Errorf(msg, req.Name, customerror.ErrInvalidArgument)
	}

	creatorUUID, err := uuid.FromString(authUID)
	if err != nil {
		log.Error("failed to parse creator uid", zap.String("uid", authUID), zap.Error(err))
		return nil, err
	}

	// external service call - create catalog collection and set ACL in openFAG
	callExternalService := func(kbUID string) error {
		err := ph.service.MilvusClient.CreateKnowledgeBaseCollection(ctx, kbUID)
		if err != nil {
			log.Error("failed to create collection in milvus", zap.Error(err))
			return err
		}

		// set the owner of the catalog
		kbUIDuuid, err := uuid.FromString(kbUID)
		if err != nil {
			log.Error("failed to parse kb uid", zap.String("kb_uid", kbUID), zap.Error(err))
			return err
		}
		err = ph.service.ACLClient.SetOwner(ctx, "knowledgebase", kbUIDuuid, string(ns.NsType), ns.NsUID)
		if err != nil {
			log.Error("failed to set owner in openFAG", zap.Error(err))
			return err
		}

		return nil
	}

	// create catalog
	dbData, err := ph.service.Repository.CreateKnowledgeBase(ctx,
		repository.KnowledgeBase{
			Name: req.Name,
			// make name as kbID
			KbID:        req.Name,
			Description: req.Description,
			Tags:        req.Tags,
			Owner:       ns.NsUID.String(),
			CreatorUID:  creatorUUID,
		}, callExternalService,
	)
	if err != nil {
		return nil, err
	}

	payload, err := protojson.Marshal(req)
	if err != nil {
		return nil, err
	}
	catalogRun := ph.logCatalogRunStart(ctx, dbData.UID, repository.RunAction(artifactpb.CatalogRunAction_CATALOG_RUN_ACTION_CREATE), &startTime, payload)
	ph.logCatalogRunCompleted(ctx, catalogRun.UID, startTime)

	return &artifactpb.CreateCatalogResponse{
		Catalog: &artifactpb.Catalog{
			Name:                dbData.Name,
			CatalogId:           dbData.KbID,
			Description:         dbData.Description,
			Tags:                dbData.Tags,
			OwnerName:           dbData.Owner,
			CreateTime:          dbData.CreateTime.String(),
			UpdateTime:          dbData.UpdateTime.String(),
			ConvertingPipelines: []string{"preset/indexing-convert-pdf"},
			SplittingPipelines:  []string{"preset/indexing-split-text", "preset/indexing-split-markdown"},
			EmbeddingPipelines:  []string{"preset/indexing-embed"},
			DownstreamApps:      []string{},
			TotalFiles:          0,
			TotalTokens:         0,
			UsedStorage:         0,
		},
	}, nil
}

func (ph *PublicHandler) ListCatalogs(ctx context.Context, req *artifactpb.ListCatalogsRequest) (*artifactpb.ListCatalogsResponse, error) {
	log, _ := logger.GetZapLogger(ctx)
	// get user id from context
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {

		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}

	// ACL - check user(authUid)'s permission to list catalogs in
	// the user or org context(namespace)
	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		log.Error(
			"failed to get namespace ",
			zap.Error(err),
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
			zap.String("auth_uid", authUID))
		return nil, fmt.Errorf("failed to get namespace. err: %w", err)
	}
	err = ph.service.CheckNamespacePermission(ctx, ns)
	if err != nil {
		log.Error(
			"failed to check namespace permission",
			zap.Error(err),
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
			zap.String("auth_uid", authUID))
		return nil, fmt.Errorf("failed to check namespace permission. err:%w", err)
	}

	dbData, err := ph.service.Repository.ListKnowledgeBases(ctx, ns.NsUID.String())
	if err != nil {
		log.Error("failed to get catalogs", zap.Error(err))
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}

	kbUIDuuid := make([]uuid.UUID, len(dbData))
	for i, kb := range dbData {
		kbUIDuuid[i] = kb.UID
	}

	fileCounts, err := ph.service.Repository.GetCountFilesByListKnowledgeBaseUID(ctx, kbUIDuuid)
	if err != nil {
		log.Error("failed to get file counts", zap.Error(err))
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}
	tokenCounts, err := ph.service.Repository.GetTotalTokensByListKBUIDs(ctx, kbUIDuuid)
	if err != nil {
		log.Error("failed to get token counts", zap.Error(err))
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}
	kbs := make([]*artifactpb.Catalog, len(dbData))
	for i, kb := range dbData {
		kbs[i] = &artifactpb.Catalog{
			CatalogUid:          kb.UID.String(),
			Name:                kb.Name,
			CatalogId:           kb.KbID,
			Description:         kb.Description,
			Tags:                kb.Tags,
			CreateTime:          kb.CreateTime.String(),
			UpdateTime:          kb.UpdateTime.String(),
			OwnerName:           kb.Owner,
			ConvertingPipelines: []string{service.NamespaceID + "/" + service.ConvertPDFToMDPipelineID},
			SplittingPipelines: []string{
				service.NamespaceID + "/" + service.TextSplitPipelineID,
				service.NamespaceID + "/" + service.MdSplitPipelineID},
			EmbeddingPipelines: []string{service.NamespaceID + "/" + service.TextEmbedPipelineID},
			DownstreamApps:     []string{},
			TotalFiles:         uint32(fileCounts[kb.UID]),
			TotalTokens:        uint32(tokenCounts[kb.UID]),
			UsedStorage:        uint64(kb.Usage),
		}
	}
	return &artifactpb.ListCatalogsResponse{
		Catalogs: kbs,
	}, nil
}
func (ph *PublicHandler) UpdateCatalog(ctx context.Context, req *artifactpb.UpdateCatalogRequest) (*artifactpb.UpdateCatalogResponse, error) {
	log, _ := logger.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		log.Error("failed to get user id from header", zap.Error(err))
		return nil, err
	}
	// check name if it is empty
	if req.CatalogId == "" {
		log.Error("kb_id is empty", zap.Error(ErrCheckRequiredFields))
		return nil, fmt.Errorf("kb_id is empty. err: %w", ErrCheckRequiredFields)
	}

	startTime := time.Now()

	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		log.Error(
			"failed to get namespace ",
			zap.Error(err),
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
			zap.String("auth_uid", authUID))
		return nil, fmt.Errorf("failed to get namespace. err: %w", err)
	}
	// ACL - check user's permission to update catalog
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
		log.Error("no permission to update catalog")
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, customerror.ErrNoPermission)
	}

	payload, err := protojson.Marshal(req)
	if err != nil {
		return nil, err
	}
	catalogRun := ph.logCatalogRunStart(ctx, kb.UID, repository.RunAction(artifactpb.CatalogRunAction_CATALOG_RUN_ACTION_UPDATE), &startTime, payload)
	defer func() {
		if err != nil {
			ph.logCatalogRunError(ctx, catalogRun.UID, err, startTime)
		}
	}()

	// update catalog
	kb, err = ph.service.Repository.UpdateKnowledgeBase(
		ctx,
		ns.NsUID.String(),
		repository.KnowledgeBase{
			// Name:        req.KbId,
			KbID:        req.CatalogId,
			Description: req.Description,
			Tags:        req.Tags,
			Owner:       ns.NsUID.String(),
		},
	)
	if err != nil {
		log.Error("failed to update catalog", zap.Error(err))
		return nil, err
	}
	fileCounts, err := ph.service.Repository.GetCountFilesByListKnowledgeBaseUID(ctx, []uuid.UUID{kb.UID})
	if err != nil {
		log.Error("failed to get file counts", zap.Error(err))
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}
	tokenCounts, err := ph.service.Repository.GetTotalTokensByListKBUIDs(ctx, []uuid.UUID{kb.UID})
	if err != nil {
		log.Error("failed to get token counts", zap.Error(err))
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}

	ph.logCatalogRunCompleted(ctx, catalogRun.UID, startTime)
	// populate response
	return &artifactpb.UpdateCatalogResponse{
		Catalog: &artifactpb.Catalog{
			Name:                kb.Name,
			CatalogId:           kb.KbID,
			Description:         kb.Description,
			Tags:                kb.Tags,
			CreateTime:          kb.CreateTime.String(),
			UpdateTime:          kb.UpdateTime.String(),
			OwnerName:           kb.Owner,
			ConvertingPipelines: []string{service.NamespaceID + "/" + service.ConvertPDFToMDPipelineID},
			SplittingPipelines: []string{
				service.NamespaceID + "/" + service.TextSplitPipelineID,
				service.NamespaceID + "/" + service.MdSplitPipelineID},
			EmbeddingPipelines: []string{service.NamespaceID + "/" + service.TextEmbedPipelineID},
			DownstreamApps:     []string{},
			TotalFiles:         uint32(fileCounts[kb.UID]),
			TotalTokens:        uint32(tokenCounts[kb.UID]),
			UsedStorage:        uint64(kb.Usage),
		},
	}, nil
}
func (ph *PublicHandler) DeleteCatalog(ctx context.Context, req *artifactpb.DeleteCatalogRequest) (*artifactpb.DeleteCatalogResponse, error) {
	log, _ := logger.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {

		return nil, err
	}

	startTime := time.Now()

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
		log.Error("no permission to delete catalog")
		return nil, fmt.Errorf(ErrorDeleteKnowledgeBaseMsg, customerror.ErrNoPermission)
	}

	payload, err := protojson.Marshal(req)
	if err != nil {
		return nil, err
	}
	catalogRun := ph.logCatalogRunStart(ctx, kb.UID, repository.RunAction(artifactpb.CatalogRunAction_CATALOG_RUN_ACTION_DELETE), &startTime, payload)

	startSignal := make(chan bool)
	// TODO: in the future, we should delete the catalog using clean up worker
	go utils.GoRecover(func() {
		ctx := context.TODO()
		log, _ := logger.GetZapLogger(ctx)
		// wait for the catalog to be deleted in postgres
		canStart := <-startSignal
		if !canStart {
			log.Error("failed to delete catalog in background", zap.String("catalog_id", kb.UID.String()))
			return
		}
		log.Info("DeleteCatalog starts in background", zap.String("catalog_id", kb.UID.String()))
		allPass := true

		defer func() {
			if err != nil {
				ph.logCatalogRunError(ctx, catalogRun.UID, err, startTime)
			} else {
				ph.logCatalogRunCompleted(ctx, catalogRun.UID, startTime)

			}
		}()
		//  delete files in minIO
		err = <-ph.service.MinIO.DeleteKnowledgeBase(ctx, kb.UID.String())
		if err != nil {
			log.Error("failed to delete files in minIO in background", zap.Error(err))
			allPass = false
		}

		// delete the collection in milvus
		err = ph.service.MilvusClient.DropKnowledgeBaseCollection(ctx, kb.UID.String())
		if err != nil {
			log.Error("failed to delete collection in milvus in background", zap.Error(err))
			allPass = false
		}

		//  delete all files in postgres
		err = ph.service.Repository.DeleteAllKnowledgeBaseFiles(ctx, kb.UID.String())
		if err != nil {
			log.Error("failed to delete files in postgres in background", zap.Error(err))
			allPass = false
		}
		//  delete converted files in postgres
		err = ph.service.Repository.DeleteAllConvertedFilesInKb(ctx, kb.UID)
		if err != nil {
			log.Error("failed to delete converted files in postgres in background", zap.Error(err))
			allPass = false
		}
		//  delete all chunks in postgres
		err = ph.service.Repository.HardDeleteChunksByKbUID(ctx, kb.UID)
		if err != nil {
			log.Error("failed to delete chunks in postgres in background", zap.Error(err))
			allPass = false
		}

		//  delete all embedding in postgres
		err = ph.service.Repository.HardDeleteEmbeddingsByKbUID(ctx, kb.UID)
		if err != nil {
			log.Error("failed to delete embeddings in postgres in background", zap.Error(err))
			allPass = false
		}
		// delete acl. Note: we need to delete the acl after deleting the catalog
		err = ph.service.ACLClient.Purge(ctx, "knowledgebase", kb.UID)
		if err != nil {
			log.Error("failed to purge catalog", zap.Error(err))
			allPass = false
		}
		if allPass {
			log.Info("successfully deleted catalog in background", zap.String("catalog_id", kb.UID.String()))
		} else {
			log.Error("failed to delete catalog in background", zap.String("catalog_id", kb.UID.String()))
		}
	}, "DeleteCatalog")

	deletedKb, err := ph.service.Repository.DeleteKnowledgeBase(ctx, ns.NsUID.String(), req.CatalogId)
	if err != nil {
		log.Error("failed to delete catalog", zap.Error(err))
		startSignal <- false // todo: maybe only start the go routine when err == nil
		ph.logCatalogRunError(ctx, catalogRun.UID, err, startTime)
		return nil, err
	}
	// start the background deletion
	startSignal <- true

	return &artifactpb.DeleteCatalogResponse{
		Catalog: &artifactpb.Catalog{
			Name:                deletedKb.Name,
			CatalogId:           deletedKb.KbID,
			Description:         deletedKb.Description,
			Tags:                deletedKb.Tags,
			CreateTime:          deletedKb.CreateTime.String(),
			UpdateTime:          deletedKb.UpdateTime.String(),
			OwnerName:           deletedKb.Owner,
			ConvertingPipelines: []string{},
			EmbeddingPipelines:  []string{},
			DownstreamApps:      []string{},
			TotalFiles:          0,
			TotalTokens:         0,
			UsedStorage:         0,
		},
	}, nil
}
func getUserUIDFromContext(ctx context.Context) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if v, ok := md[strings.ToLower(constant.HeaderUserUIDKey)]; ok {
		return v[0], nil
	}
	return "", fmt.Errorf("user id not found in context. err: %w", customerror.ErrUnauthenticated)
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
