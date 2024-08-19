package handler

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/instill-ai/artifact-backend/pkg/customerror"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// CreateConversation creates a new conversation
func (ph *PublicHandler) CreateConversation(ctx context.Context, req *artifactpb.CreateConversationRequest) (*artifactpb.CreateConversationResponse, error) {
	log, _ := logger.GetZapLogger(ctx)

	// Get user ID from context
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get user id from header: %v", err)
	}

	nameOk := isValidName(req.GetConversationId())
	if !nameOk {
		msg := "the conversation id should be lowercase without any space or special character besides the hyphen, " +
			"it can not start with number or hyphen, and should be less than 32 characters. name: %v. err: %w"
		return nil, fmt.Errorf(msg, req.GetConversationId(), customerror.ErrInvalidArgument)
	}

	// ACL - check user's permission to create conversation in the namespace
	ns, catalog, err := ph.service.CheckCatalogUserPermission(ctx, req.GetNamespaceId(), req.GetCatalogId(), authUID)
	if err != nil {
		log.Error(
			"failed to check user permission",
			zap.Error(err),
			zap.String("namespace_id", req.GetNamespaceId()),
			zap.String("auth_uid", authUID),
		)
		return nil, fmt.Errorf("failed to check user permission: %w", err)
	}

	// Create conversation
	conversation, err := ph.service.Repository.CreateConversation(ctx, repository.Conversation{
		NamespaceUID: ns.NsUID,
		CatalogUID:   catalog.UID,
		ID:           req.GetConversationId(),
	})
	if err != nil {
		log.Error("failed to create conversation", zap.Error(err))
		return nil, fmt.Errorf("failed to create conversation: %w", err)
	}

	return &artifactpb.CreateConversationResponse{
		Conversation: convertToProtoConversation(conversation, req.GetNamespaceId(), req.GetCatalogId()),
	}, nil
}

// ListConversations lists conversations for a given catalog
func (ph *PublicHandler) ListConversations(ctx context.Context, req *artifactpb.ListConversationsRequest) (*artifactpb.ListConversationsResponse, error) {
	log, _ := logger.GetZapLogger(ctx)

	// Get user ID from context
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get user id from header: %v", err)
	}

	// ACL - check user's permission to list conversations in the catalog
	ns, catalog, err := ph.service.CheckCatalogUserPermission(ctx, req.GetNamespaceId(), req.GetCatalogId(), authUID)
	if err != nil {
		log.Error(
			"failed to check user permission",
			zap.Error(err),
			zap.String("namespace_id", req.GetNamespaceId()),
			zap.String("auth_uid", authUID),
		)
		return nil, fmt.Errorf("failed to check user permission: %w", err)
	}

	// Get conversations
	conversations, totalCount, nextPageToken, err := ph.service.Repository.ListConversations(ctx, ns.NsUID, catalog.UID, req.GetPageSize(), req.GetPageToken())
	if err != nil {
		log.Error("failed to list conversations", zap.Error(err))
		return nil, fmt.Errorf("failed to list conversations: %w", err)
	}

	// Convert to proto conversations
	protoConversations := make([]*artifactpb.Conversation, len(conversations))
	for i, conv := range conversations {
		protoConversations[i] = convertToProtoConversation(conv, req.GetNamespaceId(), req.GetCatalogId())
	}

	return &artifactpb.ListConversationsResponse{
		Conversations: protoConversations,
		NextPageToken: nextPageToken,
		TotalSize:     int32(totalCount),
	}, nil
}

// UpdateConversation updates an existing conversation
func (ph *PublicHandler) UpdateConversation(ctx context.Context, req *artifactpb.UpdateConversationRequest) (*artifactpb.UpdateConversationResponse, error) {
	log, _ := logger.GetZapLogger(ctx)

	// Get user ID from context
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get user id from header: %v", err)
	}

	// ACL - check user's permission to update conversation in the catalog
	ns, catalog, err := ph.service.CheckCatalogUserPermission(ctx, req.GetNamespaceId(), req.GetCatalogId(), authUID)
	if err != nil {
		log.Error(
			"failed to check user permission",
			zap.Error(err),
			zap.String("namespace_id", req.GetNamespaceId()),
			zap.String("auth_uid", authUID),
		)
		return nil, fmt.Errorf("failed to check user permission: %w", err)
	}

	// Get the existing conversation
	existingConv, err := ph.service.Repository.GetConversationByID(ctx, ns.NsUID, catalog.UID, req.GetConversationId())
	if err != nil {
		log.Error("failed to get existing conversation", zap.Error(err))
		return nil, fmt.Errorf("failed to get existing conversation: %w", err)
	}

	// Update conversation
	updatedConv, err := ph.service.Repository.UpdateConversationByUpdateMap(ctx, existingConv.UID, map[string]interface{}{
		repository.ConversationColumn.ID: req.GetNewConversationId(),
	})
	if err != nil {
		log.Error("failed to update conversation", zap.Error(err))
		return nil, fmt.Errorf("failed to update conversation: %w", err)
	}

	return &artifactpb.UpdateConversationResponse{
		Conversation: convertToProtoConversation(updatedConv, req.GetNamespaceId(), req.GetCatalogId()),
	}, nil
}

// DeleteConversation deletes an existing conversation
func (ph *PublicHandler) DeleteConversation(ctx context.Context, req *artifactpb.DeleteConversationRequest) (*artifactpb.DeleteConversationResponse, error) {
	log, _ := logger.GetZapLogger(ctx)

	// Get user ID from context
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get user id from header: %v", err)
	}

	// ACL - check user's permission to delete conversation in the namespace
	ns, catalog, err := ph.service.CheckCatalogUserPermission(ctx, req.GetNamespaceId(), req.GetCatalogId(), authUID)
	if err != nil {
		log.Error(
			"failed to check user permission",
			zap.Error(err),
			zap.String("namespace_id", req.GetNamespaceId()),
			zap.String("auth_uid", authUID),
		)
		return nil, fmt.Errorf("failed to check user permission: %w", err)
	}
	// Delete conversation
	err = ph.service.Repository.SoftDeleteConversation(ctx, ns.NsUID, catalog.UID, req.GetConversationId())
	if err != nil {
		log.Error("failed to delete conversation", zap.Error(err))
		return nil, fmt.Errorf("failed to delete conversation: %w", err)
	}

	return &artifactpb.DeleteConversationResponse{}, nil
}

// Helper function to convert repository.Conversation to artifactpb.Conversation
func convertToProtoConversation(conv *repository.Conversation, nsID, catalogID string) *artifactpb.Conversation {

	return &artifactpb.Conversation{
		Uid:         conv.UID.String(),
		NamespaceId: nsID,
		CatalogId:   catalogID,
		Id:          conv.ID,
		CreateTime:  timestamppb.New(conv.CreateTime),
		UpdateTime:  timestamppb.New(conv.UpdateTime),
	}
}
