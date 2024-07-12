package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	"gorm.io/gorm"
)

type KnowledgeBaseFileI interface {
	// KnowledgeBaseFileTableName returns the table name of the KnowledgeBaseFile
	KnowledgeBaseFileTableName() string
	// CreateKnowledgeBaseFile creates a new knowledge base file
	CreateKnowledgeBaseFile(ctx context.Context, kb KnowledgeBaseFile, externalServiceCall func(FileUID string) error) (*KnowledgeBaseFile, error)
	// ListKnowledgeBaseFiles lists the knowledge base files by owner UID, knowledge base UID, and page size
	ListKnowledgeBaseFiles(ctx context.Context, uid string, ownerUID string, kbUID string, pageSize int32, nextPageToken string, filesUID []string) ([]KnowledgeBaseFile, int, string, error)
	// DeleteKnowledgeBaseFile deletes the knowledge base file by file UID
	DeleteKnowledgeBaseFile(ctx context.Context, fileUID string) error
	// ProcessKnowledgeBaseFiles updates the process status of the files
	ProcessKnowledgeBaseFiles(ctx context.Context, fileUids []string) ([]KnowledgeBaseFile, error)
	// GetIncompleteFile returns the files that are not yet processed
	GetIncompleteFile(ctx context.Context) []KnowledgeBaseFile
	// UpdateKnowledgeBaseFile updates the data and retrieves the latest data
	UpdateKnowledgeBaseFile(ctx context.Context, fileUID string, updateMap map[string]interface{}) (*KnowledgeBaseFile, error)
	// GetCountFilesByListKnowledgeBaseUID returns the number of files associated with the knowledge base UID
	GetCountFilesByListKnowledgeBaseUID(ctx context.Context, kbUIDs []uuid.UUID) (map[uuid.UUID]int64, error)
}

type KnowledgeBaseFile struct {
	UID              uuid.UUID `gorm:"column:uid;type:uuid;default:gen_random_uuid();primaryKey" json:"uid"`
	Owner            uuid.UUID `gorm:"column:owner;type:uuid;not null" json:"owner"`
	KnowledgeBaseUID uuid.UUID `gorm:"column:kb_uid;type:uuid;not null" json:"kb_uid"`
	CreatorUID       uuid.UUID `gorm:"column:creator_uid;type:uuid;not null" json:"creator_uid"`
	Name             string    `gorm:"column:name;size:255;not null" json:"name"`
	// Type is defined in the grpc proto file
	Type          string `gorm:"column:type;not null" json:"type"`
	Destination   string `gorm:"column:destination;size:255;not null" json:"destination"`
	ProcessStatus string `gorm:"column:process_status;size:100;not null" json:"process_status"`
	ExtraMetaData string `gorm:"column:extra_meta_data;type:jsonb" json:"extra_meta_data"`
	// Content not used yet
	Content    []byte     `gorm:"column:content;type:bytea" json:"content"`
	CreateTime *time.Time `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime *time.Time `gorm:"column:update_time;not null;autoUpdateTime" json:"update_time"` // Use autoUpdateTime
	DeleteTime *time.Time `gorm:"column:delete_time" json:"delete_time"`
	// Size
	Size int64 `gorm:"column:size" json:"size"`
}

// table columns map
type KnowledgeBaseFileColumns struct {
	UID              string
	Owner            string
	KnowledgeBaseUID string
	CreatorUID       string
	Name             string
	Type             string
	Destination      string
	ProcessStatus    string
	CreateTime       string
	ExtraMetaData    string
	UpdateTime       string
	DeleteTime       string
}

var KnowledgeBaseFileColumn = KnowledgeBaseFileColumns{
	UID:              "uid",
	Owner:            "owner",
	KnowledgeBaseUID: "kb_uid",
	CreatorUID:       "creator_uid",
	Name:             "name",
	Type:             "type",
	Destination:      "destination",
	ProcessStatus:    "process_status",
	ExtraMetaData:    "extra_meta_data",
	CreateTime:       "create_time",
	UpdateTime:       "update_time",
	DeleteTime:       "delete_time",
}

// KnowledgeBaseFileTableName returns the table name of the KnowledgeBaseFile
func (r *Repository) KnowledgeBaseFileTableName() string {
	return "knowledge_base_file"
}

func (r *Repository) CreateKnowledgeBaseFile(ctx context.Context, kb KnowledgeBaseFile, externalServiceCall func(FileUID string) error) (*KnowledgeBaseFile, error) {
	// check if the file already exists in the same knowledge base and not delete
	var existingFile KnowledgeBaseFile
	whereClause := fmt.Sprintf("%s = ? AND %s = ? AND %v is NULL", KnowledgeBaseFileColumn.KnowledgeBaseUID, KnowledgeBaseFileColumn.Name, KnowledgeBaseFileColumn.DeleteTime)
	if err := r.db.Where(whereClause, kb.KnowledgeBaseUID, kb.Name).First(&existingFile).Error; err != nil {
		if err != gorm.ErrRecordNotFound {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("file already exists in the knowledge base. file: {%v}", kb.Name)
	}

	exist, err := r.checkIfKnowledgeBaseExists(ctx, kb.KnowledgeBaseUID.String())
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, fmt.Errorf("knowledge base does not exist. kb.uid:{%v}", kb.KnowledgeBaseUID.String())
	}

	kb.ExtraMetaData = "{}"

	// Use a transaction to create the knowledge base file and call the external service
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Create the knowledge base file
		if err := tx.Create(&kb).Error; err != nil {
			return err
		}

		// Call the external service
		if externalServiceCall != nil {
			if err := externalServiceCall(kb.UID.String()); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &kb, nil
}

func (r *Repository) ListKnowledgeBaseFiles(ctx context.Context, uid string, ownerUID string, kbUID string, pageSize int32, nextPageToken string, fileUIDs []string) ([]KnowledgeBaseFile, int, string, error) {
	var kbs []KnowledgeBaseFile
	var totalCount int64

	// Initial query with owner and knowledge base uid and delete time is null
	whereClause := fmt.Sprintf("%v = ? AND %v = ? AND %v is NULL", KnowledgeBaseFileColumn.Owner, KnowledgeBaseFileColumn.KnowledgeBaseUID, KnowledgeBaseFileColumn.DeleteTime)
	query := r.db.Model(&KnowledgeBaseFile{}).Where(whereClause, ownerUID, kbUID)

	// Apply file UID filter if provided
	if len(fileUIDs) > 0 {
		whereClause := fmt.Sprintf("%v IN ?", KnowledgeBaseFileColumn.UID)
		query = query.Where(whereClause, fileUIDs)
	}

	// Count the total number of matching records
	if err := query.Count(&totalCount).Error; err != nil {
		return nil, 0, "", err
	}

	// Apply pagination. page size's default value is 10 and cap to 100.
	if pageSize > 100 {
		pageSize = 100
	} else if pageSize <= 0 {
		pageSize = 10
	}

	query = query.Limit(int(pageSize))

	if nextPageToken != "" {
		// Assuming next_page_token is the `create_time` timestamp of the last record from the previous page
		if parsedTime, err := time.Parse(time.RFC3339, nextPageToken); err == nil {
			whereClause := fmt.Sprintf("%v > ?", KnowledgeBaseFileColumn.CreateTime)
			query = query.Where(whereClause, parsedTime)
		} else {
			return nil, 0, "", fmt.Errorf("invalid next_page_token format(RFC3339): %v", err)
		}
	}

	// Fetch the records
	if err := query.Find(&kbs).Error; err != nil {
		return nil, 0, "", err
	}

	// Determine the next page token
	newNextPageToken := ""
	if len(kbs) > 0 {
		newNextPageToken = kbs[len(kbs)-1].CreateTime.Format(time.RFC3339)
	}
	return kbs, int(totalCount), newNextPageToken, nil
}

// delete the file which is to set the delete time
func (r *Repository) DeleteKnowledgeBaseFile(ctx context.Context, fileUID string) error {
	currentTime := time.Now()
	whereClause := fmt.Sprintf("%v = ? AND %v is NULL", KnowledgeBaseFileColumn.UID, KnowledgeBaseFileColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Model(&KnowledgeBaseFile{}).
		Where(whereClause, fileUID).
		Update(KnowledgeBaseFileColumn.DeleteTime, currentTime).Error; err != nil {
		return err
	}
	return nil
}

// ProcessKnowledgeBaseFiles updates the process status of the files
func (r *Repository) ProcessKnowledgeBaseFiles(ctx context.Context, fileUIDs []string) ([]KnowledgeBaseFile, error) {
	// Update the process status of the files
	waitingStatus := artifactpb.FileProcessStatus_name[int32(artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_WAITING)]
	if err := r.db.WithContext(ctx).Model(&KnowledgeBaseFile{}).
		Where(KnowledgeBaseFileColumn.UID+" IN ?", fileUIDs).
		Update(KnowledgeBaseFileColumn.ProcessStatus, waitingStatus).Error; err != nil {
		return nil, err
	}

	// Retrieve the updated records
	var files []KnowledgeBaseFile
	if err := r.db.WithContext(ctx).Where(KnowledgeBaseFileColumn.UID+" IN ?", fileUIDs).Find(&files).Error; err != nil {
		return nil, err
	}

	return files, nil
}

// get the incomplete files
func (r *Repository) GetIncompleteFile(ctx context.Context) []KnowledgeBaseFile {
	var files []KnowledgeBaseFile
	whereClause := fmt.Sprintf("%v NOT IN ? AND %v is null", KnowledgeBaseFileColumn.ProcessStatus, KnowledgeBaseFileColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(
		whereClause, []string{
			artifactpb.FileProcessStatus_name[int32(artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_NOTSTARTED)],
			artifactpb.FileProcessStatus_name[int32(artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_COMPLETED)]}).
		Find(&files).Error; err != nil {
		return nil
	}
	return files
}

// UpdateKnowledgeBaseFile updates the data and retrieves the latest data
func (r *Repository) UpdateKnowledgeBaseFile(ctx context.Context, fileUID string, updateMap map[string]interface{}) (*KnowledgeBaseFile, error) {
	var updatedFile KnowledgeBaseFile

	// Use a transaction to update and then fetch the latest data
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Update the data
		if err := tx.Model(&KnowledgeBaseFile{}).
			Where(KnowledgeBaseFileColumn.UID+" = ?", fileUID).
			Updates(updateMap).Error; err != nil {
			return err
		}

		// Fetch the latest data
		if err := tx.Where(KnowledgeBaseFileColumn.UID+" = ?", fileUID).First(&updatedFile).Error; err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &updatedFile, nil
}

// CountFilesByListKnowledgeBaseUID returns the number of files associated with the knowledge base UID
func (r *Repository) GetCountFilesByListKnowledgeBaseUID(ctx context.Context, kbUIDs []uuid.UUID) (map[uuid.UUID]int64, error) {
	var results []struct {
		KnowledgeBaseUID uuid.UUID `gorm:"column:kb_uid"`
		Count            int64     `gorm:"column:count"`
	}

	selectClause := fmt.Sprintf("%v, COUNT(*) as count", KnowledgeBaseFileColumn.KnowledgeBaseUID)
	whereClause := fmt.Sprintf("%v IN ? AND %v IS NULL", KnowledgeBaseFileColumn.KnowledgeBaseUID, KnowledgeBaseFileColumn.DeleteTime)

	// Adjust the query to match the structure and requirements of your database and tables
	err := r.db.Table(r.KnowledgeBaseFileTableName()).
		Select(selectClause).
		Where(whereClause, kbUIDs).
		Group(KnowledgeBaseFileColumn.KnowledgeBaseUID).
		Find(&results).Error

	if err != nil {
		return nil, fmt.Errorf("error querying database: %w", err)
	}

	counts := make(map[uuid.UUID]int64)
	for _, result := range results {
		counts[result.KnowledgeBaseUID] = result.Count
	}

	return counts, nil
}
