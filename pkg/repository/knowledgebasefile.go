package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type KnowledgeBaseFileI interface {
	// KnowledgeBaseFileTableName returns the table name of the KnowledgeBaseFile
	KnowledgeBaseFileTableName() string
	// CreateKnowledgeBaseFile creates a new knowledge base file
	CreateKnowledgeBaseFile(ctx context.Context, kb KnowledgeBaseFile, externalServiceCall func(FileUID string) error) (*KnowledgeBaseFile, error)
	// ListKnowledgeBaseFiles lists the knowledge base files by owner UID, knowledge base UID, and page size
	ListKnowledgeBaseFiles(ctx context.Context, uid string, ownerUID string, kbUID string, pageSize int32, nextPageToken string, filesUID []string) ([]KnowledgeBaseFile, int, string, error)
	// GetKnowledgebaseFileByKbUIDAndFileID returns the knowledge base file by knowledge base ID and file ID
	GetKnowledgebaseFileByKbUIDAndFileID(ctx context.Context, kbUID uuid.UUID, fileID string) (*KnowledgeBaseFile, error)
	// GetKnowledgeBaseFilesByFileUIDs returns the knowledge base files by file UIDs
	GetKnowledgeBaseFilesByFileUIDs(ctx context.Context, fileUIDs []uuid.UUID, columns ...string) ([]KnowledgeBaseFile, error)
	// DeleteKnowledgeBaseFile deletes the knowledge base file by file UID
	DeleteKnowledgeBaseFile(ctx context.Context, fileUID string) error
	// DeleteAllKnowledgeBaseFiles deletes all files in the knowledge base
	DeleteAllKnowledgeBaseFiles(ctx context.Context, kbUID string) error
	// ProcessKnowledgeBaseFiles updates the process status of the files
	ProcessKnowledgeBaseFiles(ctx context.Context, fileUIDs []string, requester uuid.UUID) ([]KnowledgeBaseFile, error)
	// GetNeedProcessFiles returns the files that are not yet processed
	GetNeedProcessFiles(ctx context.Context) []KnowledgeBaseFile
	// UpdateKnowledgeBaseFile updates the data and retrieves the latest data
	UpdateKnowledgeBaseFile(ctx context.Context, fileUID string, updateMap map[string]interface{}) (*KnowledgeBaseFile, error)
	// GetCountFilesByListKnowledgeBaseUID returns the number of files associated with the knowledge base UID
	GetCountFilesByListKnowledgeBaseUID(ctx context.Context, kbUIDs []KbUID) (map[KbUID]int64, error)
	// GetSourceTableAndUIDByFileUIDs returns the source table and uid by file UID list
	GetSourceTableAndUIDByFileUIDs(ctx context.Context, files []KnowledgeBaseFile) (map[FileUID]struct {
		SourceTable string
		SourceUID   uuid.UUID
	}, error)
	// GetTruthSourceByFileUID returns the truth source file destination of minIO by file UID
	GetTruthSourceByFileUID(ctx context.Context, fileUID uuid.UUID) (*SourceMeta, error)
	// UpdateKbFileExtraMetaData updates the extra meta data of the knowledge base file
	UpdateKbFileExtraMetaData(ctx context.Context, fileUID uuid.UUID, failureReason, convertingPipe, chunkingPipe, embeddingPipe string, processingTime, convertingTime, chunkingTime, embeddingTime *int64) error
	// DeleteKnowledgeBaseFileAndDecreaseUsage deletes the knowledge base file and decreases the knowledge base usage
	DeleteKnowledgeBaseFileAndDecreaseUsage(ctx context.Context, fileUID uuid.UUID) error
}

type KbUID = uuid.UUID

type KnowledgeBaseFile struct {
	UID uuid.UUID `gorm:"column:uid;type:uuid;default:gen_random_uuid();primaryKey" json:"uid"`
	// the knowledge base file is under the owner(namespace)
	Owner            uuid.UUID `gorm:"column:owner;type:uuid;not null" json:"owner"`
	KnowledgeBaseUID uuid.UUID `gorm:"column:kb_uid;type:uuid;not null" json:"kb_uid"`
	CreatorUID       uuid.UUID `gorm:"column:creator_uid;type:uuid;not null" json:"creator_uid"`
	Name             string    `gorm:"column:name;size:255;not null" json:"name"`
	// Type is defined in the grpc proto file
	Type        string `gorm:"column:type;not null" json:"type"`
	Destination string `gorm:"column:destination;size:255;not null" json:"destination"`
	// Process status is defined in the grpc proto file
	ProcessStatus string `gorm:"column:process_status;size:100;not null" json:"process_status"`
	// Note: use ExtraMetaDataMarshal method to marshal and unmarshal. do not populate this field directly
	// this field is used internally for the extra meta data of the file
	ExtraMetaData string `gorm:"column:extra_meta_data;type:jsonb" json:"extra_meta_data"`
	// Content not used yet
	Content    []byte     `gorm:"column:content;type:bytea" json:"content"`
	CreateTime *time.Time `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime *time.Time `gorm:"column:update_time;not null;autoUpdateTime" json:"update_time"` // Use autoUpdateTime
	DeleteTime *time.Time `gorm:"column:delete_time" json:"delete_time"`
	// Size
	Size int64 `gorm:"column:size" json:"size"`
	// Process requester UID
	RequesterUID uuid.UUID `gorm:"column:requester_uid;type:uuid;"`
	// This filed is not stored in the database. It is used to unmarshal the ExtraMetaData field
	ExtraMetaDataUnmarshal *ExtraMetaData `gorm:"-" json:"extra_meta_data_unmarshal"`
	// this field is used to let external service store the external metadata of file.
	ExternalMetadata string `gorm:"column:external_metadata;type:jsonb" json:"external_metadata"`
	// This field is not stored in the database. It is used to unmarshal the ExternalMetadata field
	ExternalMetadataUnmarshal *structpb.Struct `gorm:"-" json:"external_metadata_unmarshal"`
}

type ExtraMetaData struct {
	FailReason     string `json:"fail_reason"`
	ConvertingPipe string `json:"converting_pipe"`
	EmbeddingPipe  string `json:"embedding_pipe"`
	ChunkingPipe   string `json:"chunking_pipe"`
	ProcessingTime int64  `json:"processing_time"`
	ConvertingTime int64  `json:"converting_time"`
	ChunkingTime   int64  `json:"chunking_time"`
	EmbeddingTime  int64  `json:"embedding_time"`
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
	RequesterUID     string
	Size             string
	ExternalMetadata string
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
	Size:             "size",
	RequesterUID:     "requester_uid",
	ExternalMetadata: "external_metadata",
}

// ExtraMetaDataMarshal marshals the ExtraMetaData struct to a JSON string
func (kf *KnowledgeBaseFile) ExtraMetaDataMarshal() error {
	if kf.ExtraMetaDataUnmarshal == nil {
		kf.ExtraMetaData = "{}"
		return nil
	}
	data, err := json.Marshal(kf.ExtraMetaDataUnmarshal)
	if err != nil {
		return err
	}
	kf.ExtraMetaData = string(data)
	return nil
}

// ExtraMetaDataUnmarshal unmarshal the ExtraMetaData JSON string to a struct
func (kf *KnowledgeBaseFile) ExtraMetaDataUnmarshalFunc() error {
	var data ExtraMetaData
	if kf.ExtraMetaData == "" {
		kf.ExtraMetaDataUnmarshal = nil
		return nil
	}
	if err := json.Unmarshal([]byte(kf.ExtraMetaData), &data); err != nil {
		return err
	}
	kf.ExtraMetaDataUnmarshal = &data
	return nil
}

// ExternalMetadataToJSON converts structpb.Struct to JSON string for DB storage
func (kf *KnowledgeBaseFile) ExternalMetadataToJSON() error {
	if kf.ExternalMetadataUnmarshal == nil {
		kf.ExternalMetadata = "{}"
		return nil
	}

	jsonBytes, err := protojson.Marshal(kf.ExternalMetadataUnmarshal)
	if err != nil {
		return fmt.Errorf("failed to marshal external metadata to JSON: %v", err)
	}

	kf.ExternalMetadata = string(jsonBytes)
	return nil
}

// JSONToExternalMetadata converts JSON string from DB to structpb.Struct
func (kf *KnowledgeBaseFile) JSONToExternalMetadata() error {
	if kf.ExternalMetadata == "" {
		kf.ExternalMetadataUnmarshal = nil
		return nil
	}

	s := &structpb.Struct{}
	if err := protojson.Unmarshal([]byte(kf.ExternalMetadata), s); err != nil {
		return fmt.Errorf("failed to unmarshal external metadata from JSON: %v", err)
	}

	kf.ExternalMetadataUnmarshal = s
	return nil
}

// GORM hooks
func (kf *KnowledgeBaseFile) BeforeCreate(tx *gorm.DB) (err error) {
	if err := kf.ExtraMetaDataMarshal(); err != nil {
		return err
	}
	return kf.ExternalMetadataToJSON()
}

func (kf *KnowledgeBaseFile) BeforeSave(tx *gorm.DB) (err error) {
	if err := kf.ExtraMetaDataMarshal(); err != nil {
		return err
	}
	return kf.ExternalMetadataToJSON()
}

func (kf *KnowledgeBaseFile) BeforeUpdate(tx *gorm.DB) (err error) {
	if err := kf.ExtraMetaDataMarshal(); err != nil {
		return err
	}
	return kf.ExternalMetadataToJSON()
}

func (kf *KnowledgeBaseFile) AfterFind(tx *gorm.DB) (err error) {
	if err := kf.ExtraMetaDataUnmarshalFunc(); err != nil {
		return err
	}
	return kf.JSONToExternalMetadata()
}

// KnowledgeBaseFileTableName returns the table name of the KnowledgeBaseFile
func (r *Repository) KnowledgeBaseFileTableName() string {
	return "knowledge_base_file"
}

func (r *Repository) CreateKnowledgeBaseFile(ctx context.Context, kb KnowledgeBaseFile, externalServiceCall func(FileUID string) error) (*KnowledgeBaseFile, error) {
	// check if the file already exists in the same knowledge base and not delete
	var existingFile KnowledgeBaseFile
	whereClause := fmt.Sprintf("%s = ? AND %s = ? AND %v is NULL",
		KnowledgeBaseFileColumn.KnowledgeBaseUID, KnowledgeBaseFileColumn.Name, KnowledgeBaseFileColumn.DeleteTime)
	if err := r.db.Where(whereClause, kb.KnowledgeBaseUID, kb.Name).First(&existingFile).Error; err != nil {
		if err != gorm.ErrRecordNotFound {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("file already exists in the catalog. file: {%v}", kb.Name)
	}

	exist, err := r.checkIfKnowledgeBaseExists(ctx, kb.KnowledgeBaseUID.String())
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, fmt.Errorf("catalog does not exist. catalog.uid:{%v}", kb.KnowledgeBaseUID.String())
	}

	// kb.ExtraMetaData = "{}"

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
	pageSizeAddOne := pageSize + 1 // to get the next page token
	query = query.Limit(int(pageSizeAddOne))

	if nextPageToken != "" {
		tokenUUID, err := uuid.FromString(nextPageToken)
		if err != nil {
			return nil, 0, "", fmt.Errorf("invalid next_page_token format(UUID): %v", err)
		}
		// Assuming next_page_token is the `uid` of the last record from the previous page
		kbfs, err := r.GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{tokenUUID})
		if err != nil {
			return nil, 0, "", fmt.Errorf("failed to get catalog files by next page token: %v", err)
		} else if len(kbfs) == 0 {
			return nil, 0, "", fmt.Errorf("no catalog file found by next page token")
		}
		// whereClause
		whereClause := fmt.Sprintf("%v >= ?", KnowledgeBaseFileColumn.CreateTime)
		query = query.Where(whereClause, kbfs[0].CreateTime)
	}

	// Order by create time
	query = query.Order(fmt.Sprintf("%v ASC", KnowledgeBaseFileColumn.CreateTime))

	// Fetch the records
	if err := query.Find(&kbs).Error; err != nil {
		return nil, 0, "", err
	}

	// Determine the next page token
	newNextPageToken := ""
	if len(kbs) == int(pageSizeAddOne) {
		newNextPageToken = kbs[pageSizeAddOne-1].UID.String()
		kbs = kbs[:pageSizeAddOne-1]
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

// hard delete all files in the catalog
func (r *Repository) DeleteAllKnowledgeBaseFiles(ctx context.Context, kbUID string) error {
	whereClause := fmt.Sprintf("%v = ?", KnowledgeBaseFileColumn.KnowledgeBaseUID)
	if err := r.db.WithContext(ctx).Model(&KnowledgeBaseFile{}).
		Where(whereClause, kbUID).
		Delete(&KnowledgeBaseFile{}).Error; err != nil {
		return err
	}
	return nil
}

// ProcessKnowledgeBaseFiles updates the process status of the files
func (r *Repository) ProcessKnowledgeBaseFiles(
	ctx context.Context, fileUIDs []string, requester uuid.UUID) (
	[]KnowledgeBaseFile, error) {
	// Update the process status of the files
	waitingStatus := artifactpb.FileProcessStatus_name[int32(artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_WAITING)]
	updates := map[string]interface{}{
		KnowledgeBaseFileColumn.ProcessStatus: waitingStatus,
		KnowledgeBaseFileColumn.RequesterUID:  requester,
	}
	if err := r.db.WithContext(ctx).Model(&KnowledgeBaseFile{}).
		Where(KnowledgeBaseFileColumn.UID+" IN ?", fileUIDs).
		Updates(updates).Error; err != nil {
		return nil, err
	}

	// Retrieve the updated records
	var files []KnowledgeBaseFile
	if err := r.db.WithContext(ctx).Where(KnowledgeBaseFileColumn.UID+" IN ?", fileUIDs).Find(&files).Error; err != nil {
		return nil, err
	}

	return files, nil
}

// GetNeedProcessFiles
func (r *Repository) GetNeedProcessFiles(ctx context.Context) []KnowledgeBaseFile {
	var files []KnowledgeBaseFile
	whereClause := fmt.Sprintf("%v IN ? AND %v is null", KnowledgeBaseFileColumn.ProcessStatus, KnowledgeBaseFileColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(
		whereClause, []string{
			artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_WAITING.String(),
			artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CONVERTING.String(),
			artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_EMBEDDING.String(),
			artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CHUNKING.String(),
		}).
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

// CountFilesByListKnowledgeBaseUID returns the number of files associated with the catalog UID
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

// GetSourceTableAndUIDByFileUIDs returns the source table and uid by file UID list
func (r *Repository) GetSourceTableAndUIDByFileUIDs(ctx context.Context, files []KnowledgeBaseFile) (
	map[FileUID]struct {
		SourceTable string
		SourceUID   uuid.UUID
	}, error) {
	logger, _ := logger.GetZapLogger(ctx)
	result := make(map[uuid.UUID]struct {
		SourceTable string
		SourceUID   uuid.UUID
	})
	for _, file := range files {
		// find the source table and source uid by file uid
		// check if the file is is text or markdown
		switch file.Type {
		case artifactpb.FileType_FILE_TYPE_TEXT.String(), artifactpb.FileType_FILE_TYPE_MARKDOWN.String():
			result[file.UID] = struct {
				SourceTable string
				SourceUID   uuid.UUID
			}{
				SourceTable: r.KnowledgeBaseFileTableName(),
				SourceUID:   file.UID,
			}
		case artifactpb.FileType_FILE_TYPE_PDF.String(),
			artifactpb.FileType_FILE_TYPE_HTML.String(),
			artifactpb.FileType_FILE_TYPE_DOC.String(),
			artifactpb.FileType_FILE_TYPE_DOCX.String(),
			artifactpb.FileType_FILE_TYPE_PPT.String(),
			artifactpb.FileType_FILE_TYPE_PPTX.String(),
			artifactpb.FileType_FILE_TYPE_XLSX.String(),
			artifactpb.FileType_FILE_TYPE_XLS.String(),
			artifactpb.FileType_FILE_TYPE_CSV.String():
			convertedFile, err := r.GetConvertedFileByFileUID(ctx, file.UID)
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					continue
				} else {
					logger.Error("failed to get converted file by file uid", zap.Error(err))
					return map[uuid.UUID]struct {
						SourceTable string
						SourceUID   uuid.UUID
					}{}, err
				}
			}
			result[file.UID] = struct {
				SourceTable string
				SourceUID   uuid.UUID
			}{
				SourceTable: r.ConvertedFileTableName(),
				SourceUID:   convertedFile.UID,
			}
		}
	}

	return result, nil
}

func (r *Repository) GetKnowledgeBaseFilesByFileUIDs(
	ctx context.Context, fileUIDs []uuid.UUID, columns ...string) ([]KnowledgeBaseFile, error) {
	var files []KnowledgeBaseFile
	// Convert UUIDs to strings as GORM works with strings in queries
	var stringUIDs []string
	for _, uid := range fileUIDs {
		stringUIDs = append(stringUIDs, uid.String())
	}
	where := fmt.Sprintf("%v IN ? AND %v IS NULL", KnowledgeBaseFileColumn.UID, KnowledgeBaseFileColumn.DeleteTime)
	query := r.db.WithContext(ctx)
	if len(columns) > 0 {
		query = query.Select(columns)
	}
	// Query the database for files with the given UIDs
	if err := query.Where(where, stringUIDs).Find(&files).Error; err != nil {
		// If GORM returns ErrRecordNotFound, it's not considered an error in this context
		if err == gorm.ErrRecordNotFound {
			return []KnowledgeBaseFile{}, nil
		}
		// Return any other error that might have occurred during the query
		return nil, err
	}

	// Return the found files, or an empty slice if none were found
	return files, nil
}

// GetKnowledgebaseFileByKbIDAndFileID returns the knowledge base file by knowledge base ID and file ID
func (r *Repository) GetKnowledgebaseFileByKbUIDAndFileID(ctx context.Context, kbUID uuid.UUID, fileID string) (*KnowledgeBaseFile, error) {
	var file KnowledgeBaseFile
	where := fmt.Sprintf("%v = ? AND %v = ? AND %v IS NULL",
		KnowledgeBaseFileColumn.KnowledgeBaseUID, KnowledgeBaseFileColumn.Name, KnowledgeBaseFileColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Where(where, kbUID, fileID).First(&file).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, fmt.Errorf("file not found by catalog ID: %v and file ID: %v", kbUID, fileID)
		}
		return nil, err
	}
	return &file, nil
}

type SourceMeta struct {
	KbUID      uuid.UUID
	Dest       string
	CreateTime time.Time
}

// GetTruthSourceByFileUID returns the truth source file destination of minIO by file UID
// and support all file type. if the file type is text or markdown, the destination is the file destination.
// if the file type is pdf, get the converted file destination
func (r *Repository) GetTruthSourceByFileUID(ctx context.Context, fileUID uuid.UUID) (*SourceMeta, error) {
	logger, _ := logger.GetZapLogger(ctx)
	// get the file type by file uid
	var file KnowledgeBaseFile
	where := fmt.Sprintf("%v = ?", KnowledgeBaseFileColumn.UID)
	if err := r.db.WithContext(ctx).Where(where, fileUID).First(&file).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, fmt.Errorf("file not found by file uid: %v", fileUID)
		}
		return nil, err
	}
	// assign truth source file destination and create time
	var kbUID uuid.UUID
	var dest string
	var createTime time.Time
	switch file.Type {
	// if the file type is text or markdown, the destination is the file destination
	case artifactpb.FileType_FILE_TYPE_TEXT.String(), artifactpb.FileType_FILE_TYPE_MARKDOWN.String():
		kbUID = file.KnowledgeBaseUID
		dest = file.Destination
		createTime = *file.CreateTime
	// if the file type is pdf, get the converted file destination
	case artifactpb.FileType_FILE_TYPE_PDF.String(),
		artifactpb.FileType_FILE_TYPE_HTML.String(),
		artifactpb.FileType_FILE_TYPE_DOC.String(),
		artifactpb.FileType_FILE_TYPE_DOCX.String(),
		artifactpb.FileType_FILE_TYPE_PPT.String(),
		artifactpb.FileType_FILE_TYPE_PPTX.String(),
		artifactpb.FileType_FILE_TYPE_XLSX.String(),
		artifactpb.FileType_FILE_TYPE_XLS.String(),
		artifactpb.FileType_FILE_TYPE_CSV.String():
		convertedFile, err := r.GetConvertedFileByFileUID(ctx, fileUID)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				err = fmt.Errorf(`
				Single source not found for the file UID.
				It might be due to the file-to-single-source process not being completed yet
				or the file does not exist. err: %w`, err)
				logger.Error("converted file not found", zap.String("file_uid", fileUID.String()), zap.Error(err))
				return nil, err
			}
			return nil, err
		}
		kbUID = convertedFile.KbUID
		dest = convertedFile.Destination
		createTime = *convertedFile.CreateTime
	}

	return &SourceMeta{
		Dest:       dest,
		CreateTime: createTime,
		KbUID:      kbUID,
	}, nil
}

// UpdateKbFileExtraMetaData fetch the knowledge base file and lock the row for update,
// it will keep the original data and only update the provided params
// parameters: `fileUID` is the file UID, `failedReason` is the reason for the failure,
// `convertingPipe` is the converting pipe name, `embeddingPipe` is the embedding pipe name,
// `chunkingPipe` is the chunking pipe name, `embeddingPipe` is the embedding pipe name.
// `processingTime` is the processing time, `chunkingTime` is the chunking time, `embeddingTime` is the embedding time.
func (r *Repository) UpdateKbFileExtraMetaData(
	ctx context.Context,
	fileUID uuid.UUID,
	failureReason,
	convertingPipe,
	chunkingPipe,
	embeddingPipe string,
	processingTime, convertingTime, chunkingTime, embeddingTime *int64) error {
	var kb KnowledgeBaseFile

	// Use GORM's Transaction function
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Lock the row for update within the transaction
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(KnowledgeBaseFileColumn.UID+" = ?", fileUID).First(&kb).Error; err != nil {
			return err
		}

		// Unmarshal the existing ExtraMetaData
		if err := kb.ExtraMetaDataUnmarshalFunc(); err != nil {
			return err
		}

		// Update the ExtraMetaData fields
		if kb.ExtraMetaDataUnmarshal == nil {
			kb.ExtraMetaDataUnmarshal = &ExtraMetaData{}
		}
		if failureReason != "" {
			kb.ExtraMetaDataUnmarshal.FailReason = failureReason
		}
		if convertingPipe != "" {
			kb.ExtraMetaDataUnmarshal.ConvertingPipe = convertingPipe
		}
		if chunkingPipe != "" {
			kb.ExtraMetaDataUnmarshal.ChunkingPipe = chunkingPipe
		}
		if embeddingPipe != "" {
			kb.ExtraMetaDataUnmarshal.EmbeddingPipe = embeddingPipe
		}
		if processingTime != nil {
			kb.ExtraMetaDataUnmarshal.ProcessingTime = *processingTime
		}
		if convertingTime != nil {
			kb.ExtraMetaDataUnmarshal.ConvertingTime = *convertingTime
		}
		if chunkingTime != nil {
			kb.ExtraMetaDataUnmarshal.ChunkingTime = *chunkingTime
		}
		if embeddingTime != nil {
			kb.ExtraMetaDataUnmarshal.EmbeddingTime = *embeddingTime
		}
		// Marshal the updated ExtraMetaData
		if err := kb.ExtraMetaDataMarshal(); err != nil {
			return err
		}

		// Save the updated KnowledgeBaseFile within the transaction
		if err := tx.Save(&kb).Error; err != nil {
			return err
		}

		// Returning nil commits the transaction
		return nil
	})

	// Return the result of the transaction (either nil or an error)
	return err
}

// DeleteKnowledgeBaseFileAndDecreaseUsage delete the knowledge base file and decrease the knowledge base usage
func (r *Repository) DeleteKnowledgeBaseFileAndDecreaseUsage(ctx context.Context, fileUID uuid.UUID) error {
	currentTime := time.Now()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// get the knowledge base file
		file, err := r.GetKnowledgeBaseFilesByFileUIDs(ctx, []uuid.UUID{fileUID}, KnowledgeBaseFileColumn.KnowledgeBaseUID, KnowledgeBaseFileColumn.Size)
		if err != nil {
			return err
		} else if len(file) == 0 {
			return fmt.Errorf("file not found by file uid: %v", fileUID)
		}
		whereClause := fmt.Sprintf("%v = ? AND %v is NULL", KnowledgeBaseFileColumn.UID, KnowledgeBaseFileColumn.DeleteTime)
		if err := tx.Model(&KnowledgeBaseFile{}).
			Where(whereClause, fileUID).
			Update(KnowledgeBaseFileColumn.DeleteTime, currentTime).Error; err != nil {
			return err
		}
		// decrease the knowledge base usage
		err = r.IncreaseKnowledgeBaseUsage(ctx, tx, file[0].KnowledgeBaseUID.String(), int(-file[0].Size))
		if err != nil {
			return err
		}
		return nil
	})
}
