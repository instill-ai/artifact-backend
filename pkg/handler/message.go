package handler

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/gofrs/uuid"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// CreateMessage creates a new message in a conversation
func (ph *PublicHandler) CreateMessage(ctx context.Context, req *artifactpb.CreateMessageRequest) (*artifactpb.CreateMessageResponse, error) {
	log, _ := logger.GetZapLogger(ctx)

	// Get user ID from context
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get user id from header: %v", err)
	}

	// ACL - check user's permission to create message in the catalog
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

	// Create message
	message, err := ph.service.Repository.CreateMessage(ctx, repository.Message{
		NamespaceUID:    ns.NsUID,
		CatalogUID:      catalog.UID,
		ConversationUID: existingConv.UID,
		Content:         req.GetContent(),
		Role:            req.GetRole(),
		Type:            req.GetType().String(),
	})
	if err != nil {
		log.Error("failed to create message", zap.Error(err))
		return nil, fmt.Errorf("failed to create message: %w", err)
	}

	return &artifactpb.CreateMessageResponse{
		Message: convertToProtoMessage(message),
	}, nil
}

// ListMessages lists messages in a conversation
func (ph *PublicHandler) ListMessages(ctx context.Context, req *artifactpb.ListMessagesRequest) (*artifactpb.ListMessagesResponse, error) {
	log, _ := logger.GetZapLogger(ctx)

	// Get user ID from context
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get user id from header: %v", err)
	}

	// ACL - check user's permission to list messages in the catalog
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

	// Get messages
	messages, nextPageToken, totalCount, err := ph.service.Repository.ListMessages(
		ctx, ns.NsUID, catalog.UID, existingConv.UID, req.GetLatestK(),
		req.GetPageSize(), req.GetPageToken(), req.GetIncludeSystemMessages())
	if err != nil {
		log.Error("failed to list messages", zap.Error(err))
		return nil, fmt.Errorf("failed to list messages: %w", err)
	}

	// Convert to proto messages
	protoMessages := make([]*artifactpb.Message, len(messages))
	for i, msg := range messages {
		protoMessages[i] = convertToProtoMessage(msg)
	}

	return &artifactpb.ListMessagesResponse{
		Messages:      protoMessages,
		NextPageToken: nextPageToken,
		TotalSize:     int32(totalCount),
	}, nil
}

// UpdateMessage updates an existing message
func (ph *PublicHandler) UpdateMessage(ctx context.Context, req *artifactpb.UpdateMessageRequest) (*artifactpb.UpdateMessageResponse, error) {
	log, _ := logger.GetZapLogger(ctx)

	// Get user ID from context
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get user id from header: %v", err)
	}

	// ACL - check user's permission to update message in the catalog
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
	// turn message uid to uuid
	messageUUID, err := uuid.FromString(req.GetMessageUid())
	if err != nil {
		log.Error("failed to convert message uid to uuid", zap.Error(err))
		return nil, fmt.Errorf("failed to convert message uid to uuid: %w", err)
	}
	// Get the existing message
	existingMsg, err := ph.service.Repository.GetMessageByUID(ctx, messageUUID)
	if err != nil {
		log.Error("failed to get existing message", zap.Error(err))
		return nil, fmt.Errorf("failed to get existing message: %w", err)
	}

	if existingMsg.NamespaceUID != ns.NsUID || existingMsg.CatalogUID != catalog.UID {
		log.Error("message does not belong to the namespace or catalog.", zap.String("namespace id", ns.NsID), zap.String("catalog id", req.CatalogId))
		return nil, fmt.Errorf("message does not belong to the catalog")
	}

	// Update message
	updatedMsg, err := ph.service.Repository.UpdateMessageByUpdateMap(ctx, messageUUID, map[string]interface{}{
		repository.MessageColumn.Content: req.GetContent()})

	if err != nil {
		log.Error("failed to update message", zap.Error(err))
		return nil, fmt.Errorf("failed to update message: %w", err)
	}

	return &artifactpb.UpdateMessageResponse{
		Message: convertToProtoMessage(updatedMsg),
	}, nil
}

// DeleteMessage deletes an existing message
func (ph *PublicHandler) DeleteMessage(ctx context.Context, req *artifactpb.DeleteMessageRequest) (*artifactpb.DeleteMessageResponse, error) {
	log, _ := logger.GetZapLogger(ctx)

	// Get user ID from context
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get user id from header: %v", err)
	}

	// ACL - check user's permission to delete message in the catalog
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
	// turn message uid to uuid
	messageUUID, err := uuid.FromString(req.GetMessageUid())
	if err != nil {
		log.Error("failed to convert message uid to uuid", zap.Error(err))
		return nil, fmt.Errorf("failed to convert message uid to uuid: %w", err)
	}
	// Get the existing message
	existingMsg, err := ph.service.Repository.GetMessageByUID(ctx, messageUUID)
	if err != nil {
		log.Error("failed to get existing message", zap.Error(err))
		return nil, fmt.Errorf("failed to get existing message: %w", err)
	}
	if existingMsg.NamespaceUID != ns.NsUID || existingMsg.CatalogUID != catalog.UID {
		log.Error("message does not belong to the namespace or catalog.", zap.String("namespace id", ns.NsID), zap.String("catalog id", req.CatalogId))
		return nil, fmt.Errorf("message does not belong to the catalog")
	}
	// Delete message
	err = ph.service.Repository.DeleteMessage(ctx, messageUUID)
	if err != nil {
		log.Error("failed to delete message", zap.Error(err))
		return nil, fmt.Errorf("failed to delete message: %w", err)
	}

	return &artifactpb.DeleteMessageResponse{}, nil
}

// Helper function to convert repository.Message to artifactpb.Message
func convertToProtoMessage(msg *repository.Message) *artifactpb.Message {
	return &artifactpb.Message{
		Uid:             msg.UID.String(),
		CatalogUid:      msg.CatalogUID.String(),
		ConversationUid: msg.ConversationUID.String(),
		Content:         msg.Content,
		Role:            msg.Role,
		Type:            artifactpb.Message_MessageType(artifactpb.Message_MessageType_value[msg.Type]),
		CreateTime:      timestamppb.New(msg.CreateTime),
		UpdateTime:      timestamppb.New(msg.UpdateTime),
	}
}
