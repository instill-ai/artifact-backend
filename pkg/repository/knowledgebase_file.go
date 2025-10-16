package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gofrs/uuid"

	"github.com/instill-ai/artifact-backend/pkg/types"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/instill-ai/artifact-backend/pkg/constant"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	logx "github.com/instill-ai/x/log"
)

const (
	// KnowledgeBaseFileTableName is the table name for knowledge base files
	KnowledgeBaseFileTableName = "knowledge_base_file"
)

type KnowledgeBaseFile interface {
	// CreateKnowledgeBaseFile creates a new knowledge base file
	CreateKnowledgeBaseFile(ctx context.Context, kb KnowledgeBaseFileModel, externalServiceCall func(fileUID string) error) (*KnowledgeBaseFileModel, error)
	// ListKnowledgeBaseFiles returns a list of knowledge base files.
	ListKnowledgeBaseFiles(context.Context, KnowledgeBaseFileListParams) (*KnowledgeBaseFileList, error)
	// GetKnowledgeBaseFilesByFileUIDs returns the knowledge base files by file UIDs
	GetKnowledgeBaseFilesByFileUIDs(ctx context.Context, fileUIDs []types.FileUIDType, columns ...string) ([]KnowledgeBaseFileModel, error)
	// DeleteKnowledgeBaseFile deletes the knowledge base file by file UID
	DeleteKnowledgeBaseFile(ctx context.Context, fileUID string) error
	// DeleteAllKnowledgeBaseFiles deletes all files in the knowledge base
	DeleteAllKnowledgeBaseFiles(ctx context.Context, kbUID string) error
	// ProcessKnowledgeBaseFiles updates the process status of the files
	ProcessKnowledgeBaseFiles(ctx context.Context, fileUIDs []string, requester types.RequesterUIDType) ([]KnowledgeBaseFileModel, error)
	// UpdateKnowledgeBaseFile updates the data and retrieves the latest data
	UpdateKnowledgeBaseFile(ctx context.Context, fileUID string, updateMap map[string]any) (*KnowledgeBaseFileModel, error)
	// GetCountFilesByListKnowledgeBaseUID returns the number of files associated with the knowledge base UID
	GetCountFilesByListKnowledgeBaseUID(ctx context.Context, kbUIDs []types.KBUIDType) (map[types.KBUIDType]int64, error)
	// GetSourceTableAndUIDByFileUIDs returns the source table and uid by file UID list
	GetSourceTableAndUIDByFileUIDs(ctx context.Context, files []KnowledgeBaseFileModel) (map[types.FileUIDType]struct {
		SourceTable string
		SourceUID   types.SourceUIDType
	}, error)
	// GetSourceByFileUID returns the content converted file metadata by file UID
	GetSourceByFileUID(ctx context.Context, fileUID types.FileUIDType) (*SourceMeta, error)
	// UpdateKnowledgeFileMetadata updates the metadata fields of a knowledge base file.
	UpdateKnowledgeFileMetadata(_ context.Context, fileUID types.FileUIDType, _ ExtraMetaData) error
	// DeleteKnowledgeBaseFileAndDecreaseUsage deletes the knowledge base file and decreases the knowledge base usage
	DeleteKnowledgeBaseFileAndDecreaseUsage(ctx context.Context, fileUID types.FileUIDType) error

	// Deprecated methods

	// GetKnowledgebaseFileByKBUIDAndFileID returns the knowledge base file by
	// knowledge base ID and file ID. File ID (filename) isn't unique by
	// catalog anymore, so this method is deprecated. Files should be identified by their UID.
	GetKnowledgebaseFileByKBUIDAndFileID(ctx context.Context, kbUID types.KBUIDType, fileID string) (*KnowledgeBaseFileModel, error)
}

type KnowledgeBaseFileModel struct {
	UID types.FileUIDType `gorm:"column:uid;type:uuid;default:gen_random_uuid();primaryKey" json:"uid"`
	// the knowledge base file is under the owner(namespace)
	Owner      types.NamespaceUIDType `gorm:"column:owner;type:uuid;not null" json:"owner"`
	KBUID      types.KBUIDType        `gorm:"column:kb_uid;type:uuid;not null" json:"kb_uid"`
	CreatorUID types.CreatorUIDType   `gorm:"column:creator_uid;type:uuid;not null" json:"creator_uid"`
	Name       string                 `gorm:"column:name;size:255;not null" json:"name"`
	// FileType stores the FileType enum string (e.g., "FILE_TYPE_PDF", "FILE_TYPE_TEXT")
	FileType string `gorm:"column:file_type;not null" json:"file_type"`
	// Destination is the path in the MinIO bucket
	Destination string `gorm:"column:destination;size:255;not null" json:"destination"`
	// Process status is defined in the grpc proto file
	ProcessStatus string `gorm:"column:process_status;size:100;not null" json:"process_status"`
	// Note: use ExtraMetaDataMarshal method to marshal and unmarshal. do not populate this field directly
	// this field is used internally for the extra meta data of the file
	ExtraMetaData string `gorm:"column:extra_meta_data;type:jsonb" json:"extra_meta_data"`
	// Note: Content and summary are now stored as separate converted_file records and chunks in MinIO
	// They are no longer duplicated in this table
	CreateTime *time.Time `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime *time.Time `gorm:"column:update_time;not null;autoUpdateTime" json:"update_time"` // Use autoUpdateTime
	DeleteTime *time.Time `gorm:"column:delete_time" json:"delete_time"`
	// Size
	Size int64 `gorm:"column:size" json:"size"`
	// Process requester UID
	RequesterUID types.RequesterUIDType `gorm:"column:requester_uid;type:uuid;"`
	// This field is not stored in the database. It is used to unmarshal the ExtraMetaData field
	ExtraMetaDataUnmarshal *ExtraMetaData `gorm:"-" json:"extra_meta_data_unmarshal"`
	// This field is used to let external service store the external metadata of file.
	ExternalMetadata string `gorm:"column:external_metadata;type:jsonb" json:"external_metadata"`
	// This field is not stored in the database. It is used to unmarshal the ExternalMetadata field
	ExternalMetadataUnmarshal *structpb.Struct `gorm:"-" json:"external_metadata_unmarshal"`
	// Array of tags associated with the file
	Tags []string `gorm:"column:tags;type:VARCHAR(255)[]" json:"tags"`
}

// TableName overrides the default table name for GORM
func (KnowledgeBaseFileModel) TableName() string {
	return KnowledgeBaseFileTableName
}

type ExtraMetaData struct {
	FailReason      string `json:"fail_reason"`
	ConvertingPipe  string `json:"converting_pipe"`
	SummarizingPipe string `json:"summarizing_pipe"`
	EmbeddingPipe   string `json:"embedding_pipe"`
	ChunkingPipe    string `json:"chunking_pipe"`
	ProcessingTime  int64  `json:"processing_time"`
	ConvertingTime  int64  `json:"converting_time"`
	SummarizingTime int64  `json:"summarizing_time"`
	ChunkingTime    int64  `json:"chunking_time"`
	EmbeddingTime   int64  `json:"embedding_time"`

	// Length of the file. The unit will depend on the filetype (e.g. pages,
	// milliseconds).
	Length []uint32 `json:"length,omitempty"`
}

// table columns map
type KnowledgeBaseFileColumns struct {
	UID              string
	Owner            string
	KnowledgeBaseUID string
	CreatorUID       string
	Name             string
	FileType         string
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
	FileType:         "file_type",
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

// ConvertingPipeline extracts the conversion pipeline, if present, from the
// file metadata.
func (kf KnowledgeBaseFileModel) ConvertingPipeline() *string {
	if kf.ExtraMetaDataUnmarshal == nil || kf.ExtraMetaDataUnmarshal.ConvertingPipe == "" {
		return nil
	}

	return &kf.ExtraMetaDataUnmarshal.ConvertingPipe
}

// ExtraMetaDataMarshal marshals the ExtraMetaData struct to a JSON string
func (kf *KnowledgeBaseFileModel) ExtraMetaDataMarshal() error {
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
func (kf *KnowledgeBaseFileModel) ExtraMetaDataUnmarshalFunc() error {
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
func (kf *KnowledgeBaseFileModel) ExternalMetadataToJSON() error {
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
func (kf *KnowledgeBaseFileModel) JSONToExternalMetadata() error {
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

// PublicExternalMetadataUnmarshal returns a copy of the file's
// ExternalMetadataUnmarshal property with the private fields removed.
// ExternalMetadata is used internally to propagate the context from a file
// request in the server (e.g. upload) to the worker, but such information
// shouldn't be exposed publicly.
func (kf *KnowledgeBaseFileModel) PublicExternalMetadataUnmarshal() *structpb.Struct {
	original := kf.ExternalMetadataUnmarshal
	if original == nil || original.Fields[constant.MetadataRequestKey] == nil {
		return original
	}

	md := proto.Clone(original).(*structpb.Struct)
	delete(md.Fields, constant.MetadataRequestKey)
	return md
}

// GORM hooks
func (kf *KnowledgeBaseFileModel) BeforeCreate(tx *gorm.DB) (err error) {
	if err := kf.ExtraMetaDataMarshal(); err != nil {
		return err
	}
	return kf.ExternalMetadataToJSON()
}

func (kf *KnowledgeBaseFileModel) BeforeSave(tx *gorm.DB) (err error) {
	if err := kf.ExtraMetaDataMarshal(); err != nil {
		return err
	}
	return kf.ExternalMetadataToJSON()
}

func (kf *KnowledgeBaseFileModel) BeforeUpdate(tx *gorm.DB) (err error) {
	if err := kf.ExtraMetaDataMarshal(); err != nil {
		return err
	}
	return kf.ExternalMetadataToJSON()
}

func (kf *KnowledgeBaseFileModel) AfterFind(tx *gorm.DB) (err error) {
	if err := kf.ExtraMetaDataUnmarshalFunc(); err != nil {
		return err
	}
	return kf.JSONToExternalMetadata()
}

func (r *repository) CreateKnowledgeBaseFile(ctx context.Context, kb KnowledgeBaseFileModel, externalServiceCall func(fileUID string) error) (*KnowledgeBaseFileModel, error) {
	exists, err := r.checkIfKnowledgeBaseExists(ctx, kb.KBUID)
	if err != nil {
		return nil, fmt.Errorf("checking knowledge base existence: %w", err)
	}

	if !exists {
		return nil, fmt.Errorf("catalog does not exist")
	}

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

// KnowledgeBaseFileListParams contains the params to fetch a list of knowledge
// base files.
type KnowledgeBaseFileListParams struct {
	OwnerUID string
	KBUID    string

	// Optional filters
	FileUIDs      []string
	ProcessStatus artifactpb.FileProcessStatus

	// Pagination
	PageSize  int
	PageToken string
}

// KnowledgeBaseFileList contains a list of knowledge base files.
type KnowledgeBaseFileList struct {
	Files         []KnowledgeBaseFileModel
	TotalCount    int
	NextPageToken string
}

// ListKnowledgeBaseFiles returns a paginated list of files within a knowledge
// base. The list can optionally be filtered by file UIDs or status.
func (r *repository) ListKnowledgeBaseFiles(ctx context.Context, params KnowledgeBaseFileListParams) (*KnowledgeBaseFileList, error) {
	var kbs []KnowledgeBaseFileModel
	var totalCount int64

	q := r.db.Model(&KnowledgeBaseFileModel{}).
		Where("owner = ?", params.OwnerUID).
		Where("kb_uid = ?", params.KBUID).
		Where("delete_time is NULL")

	if len(params.FileUIDs) > 0 {
		q = q.Where("uid IN ?", params.FileUIDs)
	}

	if params.ProcessStatus != 0 {
		q = q.Where("process_status = ?", params.ProcessStatus.String())
	}

	// Count the total number of matching records
	if err := q.Count(&totalCount).Error; err != nil {
		return nil, err
	}

	// Apply pagination. page size's default value is 10 and cap to 100.
	if params.PageSize <= 0 {
		params.PageSize = 10
	}

	if params.PageSize > 100 {
		params.PageSize = 100
	}

	// We fetch one extra record to build the next page token.
	q = q.Limit(params.PageSize + 1)

	if params.PageToken != "" {
		tokenUUID, err := uuid.FromString(params.PageToken)
		if err != nil {
			return nil, fmt.Errorf("invalid next page token format: %w", err)
		}

		// Next page token is the UID of the first record in the requested page.
		kbfs, err := r.GetKnowledgeBaseFilesByFileUIDs(ctx, []types.FileUIDType{tokenUUID})
		if err != nil {
			return nil, fmt.Errorf("building query from token: %w", err)
		}

		if len(kbfs) == 0 {
			return nil, fmt.Errorf("invalid next page token")
		}

		q = q.Where("create_time <= ?", kbfs[0].CreateTime)
	}

	// TODO INS-8162: the repository method (and the upstream handler) should
	// take an `ordering` parameter so clients can choose the sorting.
	q = q.Order("create_time DESC")

	// Fetch the records
	if err := q.Find(&kbs).Error; err != nil {
		return nil, fmt.Errorf("fetching records: %w", err)
	}

	resp := &KnowledgeBaseFileList{
		Files:      kbs,
		TotalCount: int(totalCount),
	}

	if len(kbs) > params.PageSize {
		resp.NextPageToken = kbs[params.PageSize].UID.String()
		resp.Files = kbs[:params.PageSize]
	}

	return resp, nil
}

// delete the file which is to set the delete time
func (r *repository) DeleteKnowledgeBaseFile(ctx context.Context, fileUID string) error {
	currentTime := time.Now()
	whereClause := fmt.Sprintf("%v = ? AND %v is NULL", KnowledgeBaseFileColumn.UID, KnowledgeBaseFileColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Model(&KnowledgeBaseFileModel{}).
		Where(whereClause, fileUID).
		Update(KnowledgeBaseFileColumn.DeleteTime, currentTime).Error; err != nil {
		return err
	}
	return nil
}

// hard delete all files in the catalog
func (r *repository) DeleteAllKnowledgeBaseFiles(ctx context.Context, kbUID string) error {
	whereClause := fmt.Sprintf("%v = ?", KnowledgeBaseFileColumn.KnowledgeBaseUID)
	if err := r.db.WithContext(ctx).Model(&KnowledgeBaseFileModel{}).
		Where(whereClause, kbUID).
		Delete(&KnowledgeBaseFileModel{}).Error; err != nil {
		return err
	}
	return nil
}

// ProcessKnowledgeBaseFiles updates the process status of the files
func (r *repository) ProcessKnowledgeBaseFiles(
	ctx context.Context,
	fileUIDs []string,
	requester types.RequesterUIDType,
) ([]KnowledgeBaseFileModel, error) {

	db := r.db.WithContext(ctx)

	// Update the process status of the files
	updates := map[string]any{
		"process_status": artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_CONVERTING.String(),
		"requester_uid":  requester,
		// Clear previous failure reason
		"extra_meta_data": gorm.Expr("COALESCE(extra_meta_data, '{}'::jsonb) || ?::jsonb", `{"fail_reason": ""}`),
	}

	if err := db.Model(&KnowledgeBaseFileModel{}).Where("uid IN ?", fileUIDs).Updates(updates).Error; err != nil {
		return nil, fmt.Errorf("updating records: %w", err)
	}

	// Retrieve the updated records
	var files []KnowledgeBaseFileModel
	if err := db.Where("uid IN ?", fileUIDs).Find(&files).Error; err != nil {
		return nil, fmt.Errorf("retrieving updated records: %w", err)
	}

	return files, nil
}

// UpdateKnowledgeBaseFile updates the data and retrieves the latest data
func (r *repository) UpdateKnowledgeBaseFile(ctx context.Context, fileUID string, updateMap map[string]any) (*KnowledgeBaseFileModel, error) {
	var updatedFile KnowledgeBaseFileModel

	// Use a transaction to update and then fetch the latest data
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Update the data
		if err := tx.Model(&KnowledgeBaseFileModel{}).
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
func (r *repository) GetCountFilesByListKnowledgeBaseUID(ctx context.Context, kbUIDs []types.KBUIDType) (map[types.KBUIDType]int64, error) {
	var results []struct {
		KnowledgeBaseUID types.KBUIDType `gorm:"column:kb_uid"`
		Count            int64           `gorm:"column:count"`
	}

	selectClause := fmt.Sprintf("%v, COUNT(*) as count", KnowledgeBaseFileColumn.KnowledgeBaseUID)
	whereClause := fmt.Sprintf("%v IN ? AND %v IS NULL", KnowledgeBaseFileColumn.KnowledgeBaseUID, KnowledgeBaseFileColumn.DeleteTime)

	// Adjust the query to match the structure and requirements of your database and tables
	err := r.db.Table(KnowledgeBaseFileTableName).
		Select(selectClause).
		Where(whereClause, kbUIDs).
		Group(KnowledgeBaseFileColumn.KnowledgeBaseUID).
		Find(&results).Error

	if err != nil {
		return nil, fmt.Errorf("error querying database: %w", err)
	}

	counts := make(map[types.KBUIDType]int64)
	for _, result := range results {
		counts[result.KnowledgeBaseUID] = result.Count
	}

	return counts, nil
}

// GetSourceTableAndUIDByFileUIDs returns the source table and uid by file UID list
func (r *repository) GetSourceTableAndUIDByFileUIDs(ctx context.Context, files []KnowledgeBaseFileModel) (
	map[types.FileUIDType]struct {
		SourceTable string
		SourceUID   types.SourceUIDType
	}, error) {
	logger, _ := logx.GetZapLogger(ctx)
	result := make(map[types.FileUIDType]struct {
		SourceTable string
		SourceUID   types.SourceUIDType
	})
	for _, file := range files {
		// find the source table and source uid by file uid
		// All file types now use converted_file as the source after the refactoring
		// TEXT/MARKDOWN files also have converted file records (pointing to original files)
		convertedFile, err := r.GetConvertedFileByFileUID(ctx, file.UID)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Skip files without converted records
				continue
			} else {
				logger.Error("failed to get converted file by file uid", zap.Error(err))
				return map[types.FileUIDType]struct {
					SourceTable string
					SourceUID   types.SourceUIDType
				}{}, err
			}
		}
		result[file.UID] = struct {
			SourceTable string
			SourceUID   types.SourceUIDType
		}{
			SourceTable: ConvertedFileTableName,
			SourceUID:   convertedFile.UID,
		}
	}

	return result, nil
}

func (r *repository) GetKnowledgeBaseFilesByFileUIDs(
	ctx context.Context,
	fileUIDs []types.FileUIDType,
	columns ...string,
) ([]KnowledgeBaseFileModel, error) {

	var files []KnowledgeBaseFileModel
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
			return []KnowledgeBaseFileModel{}, nil
		}
		// Return any other error that might have occurred during the query
		return nil, err
	}

	// Return the found files, or an empty slice if none were found
	return files, nil
}

// SourceMeta represents the metadata of the source file
type SourceMeta struct {
	OriginalFileUID  types.FileUIDType `json:"original_file_uid"`  // OriginalFileUID is the UID of the original file
	OriginalFileName string            `json:"original_file_name"` // OriginalFileName is the name of the original file
	KBUID            types.KBUIDType   `json:"kb_uid"`             // KBUID is the UID of the knowledge base
	Dest             string            `json:"dest"`               // Dest is the destination of the source file
	CreateTime       time.Time         `json:"create_time"`        // CreateTime is the creation time of the source file
	UpdateTime       time.Time         `json:"update_time"`        // UpdateTime is the update time of the source file
}

// GetSourceByFileUID returns the content converted file metadata by file UID.
// All file types have converted files for consistency.
// For the /source endpoint, this returns the CONTENT converted file, not the summary.
// We order by create_time ASC to get the first (content) converted file, as the summary is created later.
// TODO: Rename this to GetContentByFileUID
func (r *repository) GetSourceByFileUID(ctx context.Context, fileUID types.FileUIDType) (*SourceMeta, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Get the original file metadata
	var file KnowledgeBaseFileModel
	where := fmt.Sprintf("%v = ?", KnowledgeBaseFileColumn.UID)
	if err := r.db.WithContext(ctx).Where(where, fileUID).First(&file).Error; err != nil {
		return nil, fmt.Errorf("fetching file: %w", err)
	}

	// Get the CONTENT converted file using explicit type query
	// All file types now have converted files stored with explicit type markers
	// For plaintext files, the content is identical to the original
	// For other files (PDF/DOC/etc.), the content is the markdown conversion
	where = fmt.Sprintf("%s = ? AND %s = ?", ConvertedFileColumn.FileUID, ConvertedFileColumn.ConvertedType)
	var convertedFile ConvertedFileModel
	contentTypeStr := ConvertedFileTypeToString(artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_CONTENT)
	err := r.db.WithContext(ctx).Where(where, fileUID, contentTypeStr).First(&convertedFile).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = fmt.Errorf(`
			Content source not found for the file UID.
			It might be due to the file conversion process not being completed yet
			or the file does not exist. err: %w`, err)
			logger.Error("content converted file not found", zap.String("file_uid", fileUID.String()), zap.Error(err))
			return nil, err
		}
		return nil, err
	}

	return &SourceMeta{
		OriginalFileUID:  file.UID,
		OriginalFileName: file.Name,
		Dest:             convertedFile.Destination,
		CreateTime:       *convertedFile.CreateTime,
		UpdateTime:       *convertedFile.UpdateTime,
		KBUID:            convertedFile.KBUID,
	}, nil
}

// UpdateKnowledgeFileMetadata updates the metadata fields of a knowledge base file.
// Only the nonzero values in the request will be used in the update, keeping
// the existing values for the rest of the fields.
// This method performs an update lock in the record to avoid collisions with
// other requests.
func (r *repository) UpdateKnowledgeFileMetadata(ctx context.Context, fileUID types.FileUIDType, updates ExtraMetaData) error {
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var file KnowledgeBaseFileModel

		// Lock the row for update within the transaction
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where(KnowledgeBaseFileColumn.UID+" = ?", fileUID).
			First(&file).Error

		if err != nil {
			return fmt.Errorf("fetching file: %w", err)
		}

		// Unmarshal the existing metadata.
		if err := file.ExtraMetaDataUnmarshalFunc(); err != nil {
			return fmt.Errorf("extracting metadata: %w", err)
		}

		// Update the ExtraMetaData fields.
		if file.ExtraMetaDataUnmarshal == nil {
			file.ExtraMetaDataUnmarshal = &ExtraMetaData{}
		}

		if updates.FailReason != "" {
			file.ExtraMetaDataUnmarshal.FailReason = updates.FailReason
		}
		if updates.ConvertingPipe != "" {
			file.ExtraMetaDataUnmarshal.ConvertingPipe = updates.ConvertingPipe
		}
		if updates.SummarizingPipe != "" {
			file.ExtraMetaDataUnmarshal.SummarizingPipe = updates.SummarizingPipe
		}
		if updates.ChunkingPipe != "" {
			file.ExtraMetaDataUnmarshal.ChunkingPipe = updates.ChunkingPipe
		}
		if updates.EmbeddingPipe != "" {
			file.ExtraMetaDataUnmarshal.EmbeddingPipe = updates.EmbeddingPipe
		}
		if updates.ProcessingTime != 0 {
			file.ExtraMetaDataUnmarshal.ProcessingTime = updates.ProcessingTime
		}
		if updates.ConvertingTime != 0 {
			file.ExtraMetaDataUnmarshal.ConvertingTime = updates.ConvertingTime
		}
		if updates.SummarizingTime != 0 {
			file.ExtraMetaDataUnmarshal.SummarizingTime = updates.SummarizingTime
		}
		if updates.ChunkingTime != 0 {
			file.ExtraMetaDataUnmarshal.ChunkingTime = updates.ChunkingTime
		}
		if updates.EmbeddingTime != 0 {
			file.ExtraMetaDataUnmarshal.EmbeddingTime = updates.EmbeddingTime
		}
		if len(updates.Length) > 0 {
			file.ExtraMetaDataUnmarshal.Length = updates.Length
		}

		// Marshal the updated ExtraMetaData.
		if err := file.ExtraMetaDataMarshal(); err != nil {
			return fmt.Errorf("marshalling metadata: %w", err)
		}

		// Save the updated KnowledgeBaseFileModel within the transaction
		if err := tx.Save(&file).Error; err != nil {
			return fmt.Errorf("storing record: %w", err)
		}

		return nil
	})

	return err
}

// DeleteKnowledgeBaseFileAndDecreaseUsage delete the knowledge base file and decrease the knowledge base usage
func (r *repository) DeleteKnowledgeBaseFileAndDecreaseUsage(ctx context.Context, fileUID types.FileUIDType) error {
	currentTime := time.Now()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// get the knowledge base file
		file, err := r.GetKnowledgeBaseFilesByFileUIDs(ctx, []types.FileUIDType{fileUID}, KnowledgeBaseFileColumn.KnowledgeBaseUID, KnowledgeBaseFileColumn.Size)
		if err != nil {
			return err
		} else if len(file) == 0 {
			return fmt.Errorf("file not found by file uid: %v", fileUID)
		}
		whereClause := fmt.Sprintf("%v = ? AND %v is NULL", KnowledgeBaseFileColumn.UID, KnowledgeBaseFileColumn.DeleteTime)
		if err := tx.Model(&KnowledgeBaseFileModel{}).
			Where(whereClause, fileUID).
			Update(KnowledgeBaseFileColumn.DeleteTime, currentTime).Error; err != nil {
			return err
		}
		// decrease the knowledge base usage
		err = r.IncreaseKnowledgeBaseUsage(ctx, tx, file[0].KBUID.String(), int(-file[0].Size))
		if err != nil {
			return err
		}
		return nil
	})
}

// GetKnowledgebaseFileByKBUIDAndFileID returns the knowledge base file by
// knowledge base ID and file ID.
// NOTE: this method is deprecated, files should be accessed by UID.
func (r *repository) GetKnowledgebaseFileByKBUIDAndFileID(ctx context.Context, kbUID types.KBUIDType, fileID string) (*KnowledgeBaseFileModel, error) {
	var file KnowledgeBaseFileModel
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
