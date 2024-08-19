package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"gorm.io/gorm"
)

type MessageI interface {
	MessageTableName() string
	CreateMessage(ctx context.Context, msg Message) (*Message, error)
	ListMessages(ctx context.Context, namespaceUID, catalogUID, conversationUID uuid.UUID, latestK int32, pageSize int32, pageToken string, includeSystemMessages bool) ([]*Message, string, int64, error)
	GetMessageByUID(ctx context.Context, messageUID uuid.UUID) (*Message, error)
	UpdateMessage(ctx context.Context, msg Message) (*Message, error)
	UpdateMessageByUpdateMap(ctx context.Context, messageUID uuid.UUID, updateMap map[string]interface{}) (*Message, error)
	DeleteMessage(ctx context.Context, messageUID uuid.UUID) error
}

type Message struct {
	UID             uuid.UUID  `gorm:"column:uid;type:uuid;default:gen_random_uuid();primaryKey" json:"uid"`
	NamespaceUID    uuid.UUID  `gorm:"column:namespace_uid;not null" json:"namespace_id"`
	CatalogUID      uuid.UUID  `gorm:"column:catalog_uid;not null" json:"catalog_uid"`
	ConversationUID uuid.UUID  `gorm:"column:conversation_uid;not null" json:"conversation_uid"`
	Content         string     `gorm:"column:content;type:text" json:"content"`
	Role            string     `gorm:"column:role;not null" json:"role"`
	Type            string     `gorm:"column:type;not null" json:"type"`
	CreateTime      time.Time  `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime      time.Time  `gorm:"column:update_time;not null;autoUpdateTime" json:"update_time"`
	DeleteTime      *time.Time `gorm:"column:delete_time" json:"delete_time"`
}

const reservedWordSystem = "system"

type MessageColumns struct {
	UID             string
	NamespaceUID    string
	CatalogUID      string
	ConversationUID string
	Content         string
	Role            string
	Type            string
	CreateTime      string
	UpdateTime      string
	DeleteTime      string
}

var MessageColumn = MessageColumns{
	UID:             "uid",
	NamespaceUID:    "namespace_uid",
	CatalogUID:      "catalog_uid",
	ConversationUID: "conversation_uid",
	Content:         "content",
	Role:            "role",
	Type:            "type",
	CreateTime:      "create_time",
	UpdateTime:      "update_time",
	DeleteTime:      "delete_time",
}

func (r *Repository) MessageTableName() string {
	return "message"
}

func (r *Repository) CreateMessage(ctx context.Context, msg Message) (*Message, error) {
	if err := r.db.WithContext(ctx).Create(&msg).Error; err != nil {
		return nil, fmt.Errorf("failed to create message: %w", err)
	}
	return &msg, nil
}

func (r *Repository) ListMessages(
	ctx context.Context, NsUID, catalogUID, conversationUID uuid.UUID, latestK int32, pageSize int32, pageToken string, includeSystemMessages bool) (
	[]*Message, string, int64, error) {
	var messages []*Message
	var totalCount int64

	baseWhereClause := fmt.Sprintf("%v = ? AND %v = ? AND %v = ? AND %v IS NULL", MessageColumn.NamespaceUID, MessageColumn.CatalogUID, MessageColumn.ConversationUID, MessageColumn.DeleteTime)

	// Count total messages
	countQuery := r.db.Model(&Message{}).Where(baseWhereClause, NsUID, catalogUID, conversationUID)
	if !includeSystemMessages {
		countQuery = countQuery.Where(MessageColumn.Role+" != ?", reservedWordSystem)
	}
	if err := countQuery.Count(&totalCount).Error; err != nil {
		return nil, "", 0, fmt.Errorf("error counting total messages: %v", err)
	}

	// Query for messages
	query := r.db.Model(&Message{}).Where(baseWhereClause, NsUID, catalogUID, conversationUID)

	if !includeSystemMessages {
		query = query.Where(MessageColumn.Role+" != ?", reservedWordSystem)
	}

	if pageSize > 100 {
		pageSize = 100
	} else if pageSize <= 0 {
		pageSize = 10
	}
	pageSizeAddOne := pageSize + 1
	query = query.Limit(int(pageSizeAddOne))

	if pageToken != "" {
		tokenUUID, err := uuid.FromString(pageToken)
		if err != nil {
			return nil, "", 0, fmt.Errorf("invalid page_token format(UUID): %v", err)
		}
		firstMsg, err := r.GetMessageByUID(ctx, tokenUUID)
		if err != nil {
			return nil, "", 0, fmt.Errorf("error getting message by page_token: %v", err)
		}
		whereClause := fmt.Sprintf("%v >= ?", MessageColumn.CreateTime)
		query = query.Where(whereClause, firstMsg.CreateTime)
	}

	if latestK > 0 {
		query = query.Order(MessageColumn.CreateTime + " DESC").Limit(int(latestK))
	} else {
		query = query.Order(MessageColumn.CreateTime + " ASC")
	}

	if err := query.Find(&messages).Error; err != nil {
		return nil, "", 0, err
	}

	newPageToken := ""
	if len(messages) == int(pageSizeAddOne) {
		newPageToken = messages[pageSizeAddOne-1].UID.String()
		messages = messages[:pageSizeAddOne-1]
	}

	return messages, newPageToken, totalCount, nil
}

func (r *Repository) GetMessageByUID(ctx context.Context, messageUID uuid.UUID) (*Message, error) {
	var message Message
	whereClause := fmt.Sprintf("%v = ? AND %v IS NULL", MessageColumn.UID, MessageColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(whereClause, messageUID).First(&message).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, fmt.Errorf("message not found")
		}
		return nil, err
	}
	return &message, nil
}

func (r *Repository) UpdateMessage(ctx context.Context, msg Message) (*Message, error) {
	if err := r.db.WithContext(ctx).Save(&msg).Error; err != nil {
		return nil, fmt.Errorf("failed to update message: %w", err)
	}
	return &msg, nil
}

// UpdateMessageByUpdateMap updates message by update map
func (r *Repository) UpdateMessageByUpdateMap(ctx context.Context, messageUID uuid.UUID, updateMap map[string]interface{}) (*Message, error) {
	if err := r.db.WithContext(ctx).Model(&Message{}).Where(MessageColumn.UID+" = ? AND "+MessageColumn.DeleteTime+" IS NULL", messageUID).Updates(updateMap).Error; err != nil {
		return nil, fmt.Errorf("failed to update message: %w", err)
	}
	return r.GetMessageByUID(ctx, messageUID)
}

func (r *Repository) DeleteMessage(ctx context.Context, messageUID uuid.UUID) error {
	whereClause := fmt.Sprintf("%v = ? AND %v IS NULL", MessageColumn.UID, MessageColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Model(&Message{}).Where(whereClause, messageUID).Update(MessageColumn.DeleteTime, time.Now().UTC()).Error; err != nil {
		return fmt.Errorf("failed to delete message: %w", err)
	}
	return nil
}
