package handler

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"

	"github.com/google/uuid"
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

	// TODO: ACL  check user's permission to create knowledge base in the user or org context
	// 1. if it is user namespace, it is okay
	// 2. if it is org namespace, check if the user has permission to create knowledge base in the org
	// ....

	// check if user has reached the maximum number of knowledge bases
	// note: the simple implementation have race condition to bypass the check, but it is okay for now
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

	// get the owner uid from the mgmt service
	var ownerUUID string
	{
		// get the owner uid from the mgmt service
		ownerUUID, err = ph.getOwnerUID(ctx, req.OwnerId)
		if err != nil {
			log.Error("failed to get owner uid", zap.Error(err))
			return nil, err
		}
	}

	creatorUUID, err := uuid.Parse(authUID)
	if err != nil {
		log.Error("failed to parse creator uid", zap.String("uid", authUID), zap.Error(err))
		return nil, err
	}

	// external service call - create knowledge base collection and set ACL in openFAG
	callExternalService := func(kbUID string) error {
		err = ph.service.MilvusClient.CreateKnowledgeBaseCollection(ctx, kbUID)
		if err != nil {
			log.Error("failed to create collection in milvus", zap.Error(err))
			return err
		}

		// TODO: ACL - set the owner of the knowledge base
		// ....

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
			Owner:       ownerUUID,
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
	_, err := getUserUIDFromContext(ctx)
	if err != nil {

		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}

	// get the owner uid from the mgmt service
	var ownerUUID string
	{
		// get the owner uid from the mgmt service
		ownerUUID, err = ph.getOwnerUID(ctx, req.OwnerId)
		if err != nil {
			log.Error("failed to get owner uid", zap.Error(err))
			return nil, err
		}
	}

	// TODO: ACL - check user(authUid)'s permission to list knowledge bases
	// ....

	dbData, err := ph.service.Repository.ListKnowledgeBases(ctx, ownerUUID)
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

	// get the owner uid from the mgmt service
	var ownerUUID string
	{
		// get the owner uid from the mgmt service
		ownerUUID, err = ph.getOwnerUID(ctx, req.OwnerId)
		if err != nil {
			log.Error("failed to get owner uid", zap.Error(err))
			return nil, err
		}
	}

	// TODO: ACL - check user's permission to update knowledge base
	_ = authUID
	// check if knowledge base exists
	kb, err := ph.service.Repository.UpdateKnowledgeBase(
		ctx,
		ownerUUID,
		repository.KnowledgeBase{
			// Name:        req.KbId,
			KbID:        req.KbId,
			Description: req.Description,
			Tags:        req.Tags,
			Owner:       ownerUUID,
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
			UsedStorage: uint64(kb.Usage),
		},
	}, nil
}
func (ph *PublicHandler) DeleteKnowledgeBase(ctx context.Context, req *artifactpb.DeleteKnowledgeBaseRequest) (*artifactpb.DeleteKnowledgeBaseResponse, error) {
	log, _ := logger.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {

		return nil, err
	}
	// get the owner uid from the mgmt service
	var ownerUUID string
	{
		// get the owner uid from the mgmt service
		ownerUUID, err = ph.getOwnerUID(ctx, req.OwnerId)
		if err != nil {
			log.Error("failed to get owner uid", zap.Error(err))
			return nil, err
		}
	}
	// TODO: ACL - check user's permission to delete knowledge base
	_ = authUID

	deletedKb, err := ph.service.Repository.DeleteKnowledgeBase(ctx, ownerUUID, req.KbId)
	if err != nil {

		return nil, err
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
