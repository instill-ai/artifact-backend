package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"gorm.io/gorm"
)

type ConversationI interface {
	ConversationTableName() string
	CreateConversation(ctx context.Context, conv Conversation) (*Conversation, error)
	ListConversations(ctx context.Context, namespaceUID uuid.UUID, catalogUID uuid.UUID, pageSize int32, nextPageToken string) ([]*Conversation, int, string, error)
	GetConversationByID(ctx context.Context, namespaceUID uuid.UUID, catalogUID uuid.UUID, conversationID string) (*Conversation, error)
	UpdateConversationByUpdateMap(ctx context.Context, convUID uuid.UUID, updateMap map[string]interface{}) (*Conversation, error)
	SoftDeleteConversation(ctx context.Context, namespaceUID uuid.UUID, catalogUID uuid.UUID, conversationID string) error
	GetConversationByUID(ctx context.Context, convUID uuid.UUID) (*Conversation, error)
}

type Conversation struct {
	UID          uuid.UUID  `gorm:"column:uid;type:uuid;default:gen_random_uuid();primaryKey" json:"uid"`
	NamespaceUID uuid.UUID  `gorm:"column:namespace_uid;not null" json:"namespace_uid"`
	CatalogUID   uuid.UUID  `gorm:"column:catalog_uid;not null" json:"catalog_uid"`
	ID           string     `gorm:"column:id;not null" json:"id"`
	CreateTime   time.Time  `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime   time.Time  `gorm:"column:update_time;not null;autoUpdateTime" json:"update_time"`
	DeleteTime   *time.Time `gorm:"column:delete_time" json:"delete_time"`
}

type ConversationColumns struct {
	UID          string
	NamespaceUID string
	CatalogUID   string
	ID           string
	CreateTime   string
	UpdateTime   string
	DeleteTime   string
}

var ConversationColumn = ConversationColumns{
	UID:          "uid",
	NamespaceUID: "namespace_uid",
	CatalogUID:   "catalog_uid",
	ID:           "id",
	CreateTime:   "create_time",
	UpdateTime:   "update_time",
	DeleteTime:   "delete_time",
}

func (r *Repository) ConversationTableName() string {
	return "conversation"
}

func (r *Repository) CreateConversation(ctx context.Context, conv Conversation) (*Conversation, error) {
	if err := r.db.WithContext(ctx).Create(&conv).Error; err != nil {
		if strings.Contains(err.Error(), "violates unique constraint") {
			return nil, fmt.Errorf("a conversation already exists with the same ID")
		}
		return nil, fmt.Errorf("failed to create conversation: %w", err)
	}
	return &conv, nil
}

func (r *Repository) ListConversations(ctx context.Context, namespaceUID uuid.UUID, catalogUID uuid.UUID, pageSize int32, nextPageToken string) ([]*Conversation, int, string, error) {
	var conversations []*Conversation
	var totalCount int64

	whereClause := fmt.Sprintf("%v = ? AND %v = ? AND %v IS NULL", ConversationColumn.NamespaceUID, ConversationColumn.CatalogUID, ConversationColumn.DeleteTime)
	query := r.db.Model(&Conversation{}).Where(whereClause, namespaceUID, catalogUID)

	if err := query.Count(&totalCount).Error; err != nil {
		return nil, 0, "", err
	}

	if pageSize > 100 {
		pageSize = 100
	} else if pageSize <= 0 {
		pageSize = 10
	}
	pageSizeAddOne := pageSize + 1
	query = query.Limit(int(pageSizeAddOne))

	if nextPageToken != "" {
		tokenUUID, err := uuid.FromString(nextPageToken)
		if err != nil {
			return nil, 0, "", fmt.Errorf("invalid next_page_token format(UUID): %v", err)
		}
		conv, err := r.GetConversationByUID(ctx, tokenUUID)
		if err != nil {
			return nil, 0, "", fmt.Errorf("invalid next_page_token: %v", err)
		}
		whereClause := fmt.Sprintf("%v >= ?", ConversationColumn.CreateTime)
		query = query.Where(whereClause, conv.CreateTime)
	}

	if err := query.Order(ConversationColumn.CreateTime).Find(&conversations).Error; err != nil {
		return nil, 0, "", err
	}

	newNextPageToken := ""
	if len(conversations) == int(pageSizeAddOne) {
		newNextPageToken = conversations[pageSizeAddOne-1].UID.String()
		conversations = conversations[:pageSizeAddOne-1]
	}

	return conversations, int(totalCount), newNextPageToken, nil
}

func (r *Repository) GetConversationByID(ctx context.Context, namespaceUID uuid.UUID, catalogUID uuid.UUID, conversationID string) (*Conversation, error) {
	var conversation Conversation
	whereClause := fmt.Sprintf("%v = ? AND %v = ? AND %v = ? AND %v IS NULL", ConversationColumn.NamespaceUID, ConversationColumn.CatalogUID, ConversationColumn.ID, ConversationColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereClause, namespaceUID, catalogUID, conversationID).First(&conversation).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, fmt.Errorf("conversation not found. namespaceUID: %v, catalogUID: %v, conversationID: %v", namespaceUID, catalogUID, conversationID)
		}
		return nil, err
	}
	return &conversation, nil
}

// UpdateConversationByUpdateMap updates an existing conversation using map.
func (r *Repository) UpdateConversationByUpdateMap(ctx context.Context, convUID uuid.UUID, updateMap map[string]interface{}) (*Conversation, error) {
	if err := r.db.WithContext(ctx).Model(&Conversation{}).Where(ConversationColumn.UID+" = ?", convUID).Updates(updateMap).Error; err != nil {
		return nil, fmt.Errorf("failed to update conversation: %w", err)
	}
	return r.GetConversationByUID(ctx, convUID)
}

func (r *Repository) SoftDeleteConversation(ctx context.Context, namespaceUID uuid.UUID, catalogUID uuid.UUID, conversationID string) error {
	whereClause := fmt.Sprintf("%v = ? AND %v = ? AND %v = ? AND %v IS NULL", ConversationColumn.NamespaceUID, ConversationColumn.CatalogUID, ConversationColumn.ID, ConversationColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Model(&Conversation{}).Where(whereClause, namespaceUID, catalogUID, conversationID).Update(ConversationColumn.DeleteTime, time.Now()).Error; err != nil {
		return fmt.Errorf("failed to delete conversation: %w", err)
	}
	return nil
}

// GetConversationByUID returns a conversation by its UID.
func (r *Repository) GetConversationByUID(ctx context.Context, convUID uuid.UUID) (*Conversation, error) {
	var conversation Conversation
	if err := r.db.WithContext(ctx).Where(ConversationColumn.UID+" = ?", convUID).First(&conversation).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, fmt.Errorf("conversation not found. UID: %v", convUID)
		}
		return nil, err
	}
	return &conversation, nil
}
