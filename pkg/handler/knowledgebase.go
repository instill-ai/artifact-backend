package handler

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"

	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/service"
	"github.com/instill-ai/artifact-backend/pkg/utils"
	"github.com/instill-ai/x/constant"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
)

var alphabet = "abcdefghijklmnopqrstuvwxyz"

type ErrorMsg map[int]string

const ErrorCreateKnowledgeBaseMsg = "failed to create catalog: %w"
const ErrorListKnowledgeBasesMsg = "failed to get catalogs: %w "
const ErrorUpdateKnowledgeBaseMsg = "failed to update catalog: %w"
const ErrorDeleteKnowledgeBaseMsg = "failed to delete catalog: %w"

// Note: in the future, we might have different max count for different user types
const KnowledgeBaseMaxCount = 3

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

	// external service call - create catalog collection and set ACL in openFAG
	callExternalService := func(kbUID uuid.UUID) error {
		err := ph.service.VectorDB().CreateCollection(ctx, service.KBCollectionName(kbUID))
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

	// Read conversion pipelines from request.
	convertingPipelines, err := sanitizeConvertingPipelines(req.GetConvertingPipelines())
	if err != nil {
		return nil, err
	}

	// create catalog
	dbData, err := ph.service.Repository().CreateKnowledgeBase(
		ctx,
		repository.KnowledgeBase{
			Name: req.Name,
			// make name as kbID
			KbID:                req.Name,
			Description:         req.Description,
			Tags:                req.Tags,
			Owner:               ns.NsUID.String(),
			CreatorUID:          creatorUUID,
			CatalogType:         req.GetType().String(),
			ConvertingPipelines: convertingPipelines,
		},
		callExternalService,
	)
	if err != nil {
		return nil, err
	}

	catalog := &artifactpb.Catalog{
		Name:                dbData.Name,
		CatalogUid:          dbData.UID.String(),
		CatalogId:           dbData.KbID,
		Description:         dbData.Description,
		Tags:                dbData.Tags,
		OwnerName:           dbData.Owner,
		CreateTime:          dbData.CreateTime.String(),
		UpdateTime:          dbData.UpdateTime.String(),
		ConvertingPipelines: dbData.ConvertingPipelines,
		SummarizingPipelines: []string{
			service.GenerateSummaryPipeline.Name(),
		},
		SplittingPipelines: []string{
			service.ChunkTextPipeline.Name(),
			service.ChunkMDPipeline.Name(),
		},
		EmbeddingPipelines: []string{
			service.EmbedTextPipeline.Name(),
		},
		DownstreamApps: []string{},
		TotalFiles:     0,
		TotalTokens:    0,
		UsedStorage:    0,
	}

	if len(dbData.ConvertingPipelines) == 0 {
		catalog.ConvertingPipelines = service.DefaultConversionPipelines.Names()
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
		return nil, fmt.Errorf("failed to get namespace. err: %w", err)
	}
	err = ph.service.CheckNamespacePermission(ctx, ns)
	if err != nil {
		logger.Error(
			"failed to check namespace permission",
			zap.Error(err),
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
			zap.String("auth_uid", authUID))
		return nil, fmt.Errorf("failed to check namespace permission. err:%w", err)
	}

	dbData, err := ph.service.Repository().ListKnowledgeBasesByCatalogType(ctx, ns.NsUID.String(), artifactpb.CatalogType_CATALOG_TYPE_PERSISTENT)
	if err != nil {
		logger.Error("failed to get catalogs", zap.Error(err))
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}

	kbUIDuuid := make([]uuid.UUID, len(dbData))
	for i, kb := range dbData {
		kbUIDuuid[i] = kb.UID
	}

	fileCounts, err := ph.service.Repository().GetCountFilesByListKnowledgeBaseUID(ctx, kbUIDuuid)
	if err != nil {
		logger.Error("failed to get file counts", zap.Error(err))
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}
	tokenCounts, err := ph.service.Repository().GetTotalTokensByListKBUIDs(ctx, kbUIDuuid)
	if err != nil {
		logger.Error("failed to get token counts", zap.Error(err))
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
			ConvertingPipelines: kb.ConvertingPipelines,
			SummarizingPipelines: []string{
				service.GenerateSummaryPipeline.Name(),
			},
			SplittingPipelines: []string{
				service.ChunkTextPipeline.Name(),
				service.ChunkMDPipeline.Name(),
			},
			EmbeddingPipelines: []string{
				service.EmbedTextPipeline.Name(),
			},
			DownstreamApps: []string{},
			TotalFiles:     uint32(fileCounts[kb.UID]),
			TotalTokens:    uint32(tokenCounts[kb.UID]),
			UsedStorage:    uint64(kb.Usage),
		}

		if len(kb.ConvertingPipelines) == 0 {
			kbs[i].ConvertingPipelines = service.DefaultConversionPipelines.Names()
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
		logger.Error("kb_id is empty", zap.Error(errorsx.ErrInvalidArgument))
		return nil, fmt.Errorf("kb_id is empty. err: %w", errorsx.ErrInvalidArgument)
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
	// ACL - check user's permission to update catalog
	kb, err := ph.service.Repository().GetKnowledgeBaseByOwnerAndKbID(ctx, ns.NsUID, req.CatalogId)
	if err != nil {
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}
	granted, err := ph.service.ACLClient().CheckPermission(ctx, "knowledgebase", kb.UID, "writer")
	if err != nil {
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err)
	}
	if !granted {
		return nil, fmt.Errorf("%w: no permission over catalog", errorsx.ErrUnauthorized)
	}

	convertingPipelines, err := sanitizeConvertingPipelines(req.GetConvertingPipelines())
	if err != nil {
		return nil, err
	}

	// update catalog
	kb, err = ph.service.Repository().UpdateKnowledgeBase(
		ctx,
		req.GetCatalogId(),
		ns.NsUID.String(),
		repository.KnowledgeBase{
			Description:         req.GetDescription(),
			Tags:                req.GetTags(),
			ConvertingPipelines: convertingPipelines,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("updating catalog: %w", err)
	}

	fileCounts, err := ph.service.Repository().GetCountFilesByListKnowledgeBaseUID(ctx, []uuid.UUID{kb.UID})
	if err != nil {
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}
	tokenCounts, err := ph.service.Repository().GetTotalTokensByListKBUIDs(ctx, []uuid.UUID{kb.UID})
	if err != nil {
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}

	// populate response
	catalog := &artifactpb.Catalog{
		Name:                kb.Name,
		CatalogId:           kb.KbID,
		Description:         kb.Description,
		Tags:                kb.Tags,
		CreateTime:          kb.CreateTime.String(),
		UpdateTime:          kb.UpdateTime.String(),
		OwnerName:           kb.Owner,
		ConvertingPipelines: kb.ConvertingPipelines,
		SummarizingPipelines: []string{
			service.GenerateSummaryPipeline.Name(),
		},
		SplittingPipelines: []string{
			service.ChunkTextPipeline.Name(),
			service.ChunkMDPipeline.Name(),
		},
		EmbeddingPipelines: []string{
			service.EmbedTextPipeline.Name(),
		},
		DownstreamApps: []string{},
		TotalFiles:     uint32(fileCounts[kb.UID]),
		TotalTokens:    uint32(tokenCounts[kb.UID]),
		UsedStorage:    uint64(kb.Usage),
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

	startSignal := make(chan bool)
	// TODO: in the future, we should delete the catalog using clean up worker
	go utils.GoRecover(func() {
		ctx := context.TODO()
		logger, _ := logx.GetZapLogger(ctx)
		// wait for the catalog to be deleted in postgres
		canStart := <-startSignal
		if !canStart {
			logger.Error("failed to delete catalog in background", zap.String("catalog_id", kb.UID.String()))
			return
		}
		logger.Info("DeleteCatalog starts in background", zap.String("catalog_id", kb.UID.String()))
		allPass := true
		//  delete files in minIO
		err = <-ph.service.MinIO().DeleteKnowledgeBase(ctx, kb.UID.String())
		if err != nil {
			logger.Error("failed to delete files in minIO in background", zap.Error(err))
			allPass = false
		}

		// delete the collection in milvus
		err = ph.service.VectorDB().DropCollection(ctx, service.KBCollectionName(kb.UID))
		if err != nil {
			logger.Error("failed to delete collection in milvus in background", zap.Error(err))
			allPass = false
		}

		//  delete all files in postgres
		err = ph.service.Repository().DeleteAllKnowledgeBaseFiles(ctx, kb.UID.String())
		if err != nil {
			logger.Error("failed to delete files in postgres in background", zap.Error(err))
			allPass = false
		}
		//  delete converted files in postgres
		err = ph.service.Repository().DeleteAllConvertedFilesInKb(ctx, kb.UID)
		if err != nil {
			logger.Error("failed to delete converted files in postgres in background", zap.Error(err))
			allPass = false
		}
		//  delete all chunks in postgres
		err = ph.service.Repository().HardDeleteChunksByKbUID(ctx, kb.UID)
		if err != nil {
			logger.Error("failed to delete chunks in postgres in background", zap.Error(err))
			allPass = false
		}

		//  delete all embedding in postgres
		err = ph.service.Repository().HardDeleteEmbeddingsByKbUID(ctx, kb.UID)
		if err != nil {
			logger.Error("failed to delete embeddings in postgres in background", zap.Error(err))
			allPass = false
		}
		// delete acl. Note: we need to delete the acl after deleting the catalog
		err = ph.service.ACLClient().Purge(ctx, "knowledgebase", kb.UID)
		if err != nil {
			logger.Error("failed to purge catalog", zap.Error(err))
			allPass = false
		}
		if allPass {
			logger.Info("successfully deleted catalog in background", zap.String("catalog_id", kb.UID.String()))
		} else {
			logger.Error("failed to delete catalog in background", zap.String("catalog_id", kb.UID.String()))
		}
	}, "DeleteCatalog")

	deletedKb, err := ph.service.Repository().DeleteKnowledgeBase(ctx, ns.NsUID.String(), req.CatalogId)
	if err != nil {
		logger.Error("failed to delete catalog", zap.Error(err))
		startSignal <- false
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
	return "", fmt.Errorf("user id not found in context. err: %w", errorsx.ErrUnauthenticated)
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

// sanitizeConvertingPipelines validates an input array of strings that
// represent the conversion pipelines of a catalog. It checks the string format
// is correct.
// TODO we also want to validate the existence of the pipelines, permissions of
// the requester over that pipeline and the validity of its recipe.
func sanitizeConvertingPipelines(pipelines []string) ([]string, error) {
	validPipelines := make([]string, 0, len(pipelines))
	for _, pipelineName := range pipelines {
		// Console passes an empty string to reset the catalog conversion
		// pipeline to the default one.
		if pipelineName == "" {
			continue
		}

		// Remove duplicates.
		if slices.Contains(validPipelines, pipelineName) {
			continue
		}

		if _, err := service.PipelineReleaseFromName(pipelineName); err != nil {
			err = fmt.Errorf("%w: invalid conversion pipeline format: %w", errorsx.ErrInvalidArgument, err)
			return nil, errorsx.AddMessage(
				err,
				`Conversion pipeline must have the format "{namespaceID}/{pipelineID}@{version}"`,
			)
		}

		validPipelines = append(validPipelines, pipelineName)
	}

	return validPipelines, nil
}
