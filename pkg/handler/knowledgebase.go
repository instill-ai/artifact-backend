package handler

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"

	"github.com/gofrs/uuid"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/customerror"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

type ErrorMsg map[int]string

const ErrorCreateKnowledgeBaseMsg = "failed to create knowledge base: %w"
const ErrorListKnowledgeBasesMsg = "failed to get knowledge bases: %w "
const ErrorUpdateKnowledgeBaseMsg = "failed to update knowledge base: %w"
const ErrorDeleteKnowledgeBaseMsg = "failed to delete knowledge base: %w"

// Note: in the future, we might have different max count for different user types
const KnowledgeBaseMaxCount = 3

func (ph *PublicHandler) CreateKnowledgeBase(ctx context.Context, req *artifactpb.CreateKnowledgeBaseRequest) (*artifactpb.CreateKnowledgeBaseResponse, error) {
	log, _ := logger.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		err := fmt.Errorf("failed to get user id from header: %v. err: %w", err, customerror.ErrUnauthenticated)
		return nil, err
	}

	// ACL  check user's permission to create knowledge base in the user or org context(namespace)
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
	// check if user has reached the maximum number of knowledge bases
	// note: the simple implementation have race condition to bypass the check,
	// but it is okay for now
	kbCount, err := ph.service.Repository.GetKnowledgeBaseCountByOwner(ctx, authUID)
	if err != nil {
		log.Error("failed to get knowledge base count", zap.Error(err))
		return nil, fmt.Errorf(ErrorCreateKnowledgeBaseMsg, err)
	}
	if kbCount >= KnowledgeBaseMaxCount {
		err := fmt.Errorf("user has reached the 3 maximum number of knowledge bases: %v. ", kbCount)
		return nil, err
	}

	// check name if it is empty
	if req.Name == "" {
		err := fmt.Errorf("name is required. err: %w", ErrCheckRequiredFields)
		return nil, err
	}
	nameOk := isValidName(req.Name)
	if !nameOk {
		msg := "kb name is invalid: %v. err: %w"
		return nil, fmt.Errorf(msg, req.Name, customerror.ErrInvalidArgument)
	}

	// // get the owner uid from the mgmt service
	// var ownerUUID string
	// {
	// 	// get the owner uid from the mgmt service
	// 	ownerUUID, err = ph.getOwnerUID(ctx, req.OwnerId)
	// 	if err != nil {
	// 		log.Error("failed to get owner uid", zap.Error(err))
	// 		return nil, err
	// 	}
	// }

	creatorUUID, err := uuid.FromString(authUID)
	if err != nil {
		log.Error("failed to parse creator uid", zap.String("uid", authUID), zap.Error(err))
		return nil, err
	}

	// external service call - create knowledge base collection and set ACL in openFAG
	callExternalService := func(kbUID string) error {
		err := ph.service.MilvusClient.CreateKnowledgeBaseCollection(ctx, kbUID)
		if err != nil {
			log.Error("failed to create collection in milvus", zap.Error(err))
			return err
		}

		// set the owner of the knowledge base
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

	// create knowledge base
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

	return &artifactpb.CreateKnowledgeBaseResponse{
		KnowledgeBase: &artifactpb.KnowledgeBase{
			Name:                dbData.Name,
			KbId:                dbData.KbID,
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

func (ph *PublicHandler) ListKnowledgeBases(ctx context.Context, req *artifactpb.ListKnowledgeBasesRequest) (*artifactpb.ListKnowledgeBasesResponse, error) {
	log, _ := logger.GetZapLogger(ctx)
	// get user id from context
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {

		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}

	// ACL - check user(authUid)'s permission to list knowledge bases in
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
		log.Error("failed to get knowledge bases", zap.Error(err))
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
	kbs := make([]*artifactpb.KnowledgeBase, len(dbData))
	for i, kb := range dbData {
		kbs[i] = &artifactpb.KnowledgeBase{
			Name:                kb.Name,
			KbId:                kb.KbID,
			Description:         kb.Description,
			Tags:                kb.Tags,
			CreateTime:          kb.CreateTime.String(),
			UpdateTime:          kb.UpdateTime.String(),
			OwnerName:           kb.Owner,
			ConvertingPipelines: []string{"preset/indexing-convert-pdf"},
			SplittingPipelines:  []string{"preset/indexing-split-text", "preset/indexing-split-markdown"},
			EmbeddingPipelines:  []string{"preset/indexing-embed"},
			DownstreamApps:      []string{},
			TotalFiles:          uint32(fileCounts[kb.UID]),
			TotalTokens:         uint32(tokenCounts[kb.UID]),
			UsedStorage:         uint64(kb.Usage),
		}
	}
	return &artifactpb.ListKnowledgeBasesResponse{
		KnowledgeBases: kbs,
	}, nil
}
func (ph *PublicHandler) UpdateKnowledgeBase(ctx context.Context, req *artifactpb.UpdateKnowledgeBaseRequest) (*artifactpb.UpdateKnowledgeBaseResponse, error) {
	log, _ := logger.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		log.Error("failed to get user id from header", zap.Error(err))
		return nil, err
	}
	// check name if it is empty
	if req.KbId == "" {
		log.Error("kb_id is empty", zap.Error(ErrCheckRequiredFields))
		return nil, fmt.Errorf("kb_id is empty. err: %w", ErrCheckRequiredFields)
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
	// ACL - check user's permission to update knowledge base
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
		log.Error("no permission to update knowledge base")
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, customerror.ErrNoPermission)
	}

	// update knowledge base
	kb, err = ph.service.Repository.UpdateKnowledgeBase(
		ctx,
		ns.NsUID.String(),
		repository.KnowledgeBase{
			// Name:        req.KbId,
			KbID:        req.KbId,
			Description: req.Description,
			Tags:        req.Tags,
			Owner:       ns.NsUID.String(),
		},
	)
	if err != nil {
		log.Error("failed to update knowledge base", zap.Error(err))
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
	// populate response
	return &artifactpb.UpdateKnowledgeBaseResponse{
		KnowledgeBase: &artifactpb.KnowledgeBase{
			Name:                kb.Name,
			KbId:                kb.KbID,
			Description:         kb.Description,
			Tags:                kb.Tags,
			CreateTime:          kb.CreateTime.String(),
			UpdateTime:          kb.UpdateTime.String(),
			OwnerName:           kb.Owner,
			ConvertingPipelines: []string{"preset/indexing-convert-pdf"},
			SplittingPipelines:  []string{"preset/indexing-split-text", "preset/indexing-split-markdown"},
			EmbeddingPipelines:  []string{"preset/indexing-embed"},
			DownstreamApps:      []string{},
			TotalFiles:          uint32(fileCounts[kb.UID]),
			TotalTokens:         uint32(tokenCounts[kb.UID]),
			UsedStorage:         uint64(kb.Usage),
		},
	}, nil
}
func (ph *PublicHandler) DeleteKnowledgeBase(ctx context.Context, req *artifactpb.DeleteKnowledgeBaseRequest) (*artifactpb.DeleteKnowledgeBaseResponse, error) {
	log, _ := logger.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {

		return nil, err
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

	deletedKb, err := ph.service.Repository.DeleteKnowledgeBase(ctx, ns.NsUID.String(), req.KbId)
	if err != nil {
		return nil, err
	}
	err = ph.service.ACLClient.Purge(ctx, "knowledgebase", deletedKb.UID)
	if err != nil {
		log.Error("failed to purge knowledge base", zap.Error(err))
		return nil, fmt.Errorf(ErrorDeleteKnowledgeBaseMsg, err)
	}

	return &artifactpb.DeleteKnowledgeBaseResponse{
		KnowledgeBase: &artifactpb.KnowledgeBase{
			Name:                deletedKb.Name,
			KbId:                deletedKb.KbID,
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
	pattern := `^[a-z]([a-z-]{0,30}[a-z])?$`
	// Compile the regular expression
	re := regexp.MustCompile(pattern)
	// Match the name against the regular expression
	return re.MatchString(name)
}
