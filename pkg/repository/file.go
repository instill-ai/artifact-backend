package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"go.einride.tech/aip/filtering"

	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/types"
	"github.com/instill-ai/artifact-backend/pkg/utils"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
	logx "github.com/instill-ai/x/log"
)

const (
	// FileTableName is the table name for files
	FileTableName = "file"
)

// File interface defines the methods for the file table
type File interface {
	// CreateFile creates a new file and associates it with a knowledge base.
	// The file is namespace-scoped, and the KB association is stored in the junction table.
	CreateFile(ctx context.Context, file FileModel, kbUID types.KnowledgeBaseUIDType, externalServiceCall func(fileUID string) error) (*FileModel, error)
	// ListFiles returns a list of files.
	ListFiles(context.Context, KnowledgeBaseFileListParams) (*FileList, error)
	// GetFilesByFileUIDs returns the files by file UIDs
	GetFilesByFileUIDs(ctx context.Context, fileUIDs []types.FileUIDType, columns ...string) ([]FileModel, error)
	// GetFilesByFileUIDsIncludingDeleted returns files by UIDs, INCLUDING soft-deleted files
	// Used for embedding activities to generate embeddings even if file was deleted during processing
	GetFilesByFileUIDsIncludingDeleted(ctx context.Context, fileUIDs []types.FileUIDType, columns ...string) ([]FileModel, error)
	// GetFilesByFileIDs returns files by their hash-based IDs (slug format like 'my-file-abc12345').
	// This is more robust than UUIDs for LLM interactions as the IDs are human-readable and less prone to hallucination.
	// The function also checks aliases for backward compatibility when display_name is renamed.
	GetFilesByFileIDs(ctx context.Context, fileIDs []string, columns ...string) ([]FileModel, error)
	// GetFileByIDOrAlias looks up a file by its canonical ID or any of its aliases within a KB
	// Aliases preserve old URLs when display_name is renamed
	GetFileByIDOrAlias(ctx context.Context, kbUID types.KBUIDType, id string) (*FileModel, error)
	// GetFilesByName retrieves files by name in a specific KB
	// Used for dual deletion to find corresponding files in staging/rollback KB
	GetFilesByName(ctx context.Context, kbUID types.KBUIDType, filename string) ([]FileModel, error)
	// DeleteFile deletes the file by file UID
	DeleteFile(ctx context.Context, fileUID string) error
	// DeleteAllFiles deletes all files
	DeleteAllFiles(ctx context.Context, kbUID string) error
	// ProcessFiles updates the process status of the files
	ProcessFiles(ctx context.Context, fileUIDs []string, requester types.RequesterUIDType) ([]FileModel, error)
	// UpdateFile updates the data and retrieves the latest data
	UpdateFile(ctx context.Context, fileUID string, updateMap map[string]any) (*FileModel, error)
	// GetFileCountByKnowledgeBaseUID returns the number of files for a KB, optionally filtered by process status
	// If processStatus is empty, returns all non-deleted files
	// If processStatus is provided (e.g., "FILE_PROCESS_STATUS_COMPLETED"), returns only files with that status
	GetFileCountByKnowledgeBaseUID(ctx context.Context, kbUID types.KBUIDType, processStatus string) (int64, error)
	// GetFileCountByKnowledgeBaseUIDIncludingDeleted counts files INCLUDING soft-deleted ones
	// Used by cleanup workflows to wait for in-progress workflows to complete before dropping collections
	GetFileCountByKnowledgeBaseUIDIncludingDeleted(ctx context.Context, kbUID types.KBUIDType, processStatus string) (int64, error)
	// GetNotStartedFileCount counts files in NOTSTARTED status (regardless of creation time)
	// Used by synchronization to detect files waiting to start processing
	GetNotStartedFileCount(ctx context.Context, kbUID types.KBUIDType) (int64, error)
	// GetNotStartedFileCountExcluding counts NOTSTARTED files excluding specific UIDs (for recently reconciled files)
	// Used during synchronization to exclude files that were just created by reconciliation
	GetNotStartedFileCountExcluding(ctx context.Context, kbUID types.KBUIDType, excludeUIDs []types.FileUIDType) (int64, error)
	// GetContentByFileUIDs returns the content converted file source table and uid by file UID list
	GetContentByFileUIDs(ctx context.Context, files []FileModel) (map[types.FileUIDType]struct {
		SourceTable string
		SourceUID   types.SourceUIDType
	}, error)
	// GetSourceByFileUID returns the content converted file metadata by file UID
	GetSourceByFileUID(ctx context.Context, fileUID types.FileUIDType) (*SourceMeta, error)
	// UpdateFileMetadata updates the metadata fields of a file.
	UpdateFileMetadata(_ context.Context, fileUID types.FileUIDType, _ ExtraMetaData) error
	// UpdateFileUsageMetadata updates the usage metadata (AI token counts) for a file.
	// According to Gemini API docs: https://ai.google.dev/gemini-api/docs/tokens?lang=python
	UpdateFileUsageMetadata(_ context.Context, fileUID types.FileUIDType, _ UsageMetadata) error
	// DeleteFileAndDecreaseUsage deletes the file and decreases the knowledge base usage
	DeleteFileAndDecreaseUsage(ctx context.Context, fileUID types.FileUIDType) error
	// GetKnowledgeBaseUIDsForFile returns all KB UIDs associated with a file via the junction table
	GetKnowledgeBaseUIDsForFile(ctx context.Context, fileUID types.FileUIDType) ([]types.KnowledgeBaseUIDType, error)
	// GetKnowledgeBaseIDsForFiles returns a map of file UID to KB IDs (hash-based IDs) for batch lookup
	GetKnowledgeBaseIDsForFiles(ctx context.Context, fileUIDs []types.FileUIDType) (map[types.FileUIDType][]string, error)

	// ResetFileStatusesByKBUID resets all files in a KB to NOTSTARTED status for re-embedding.
	// Returns the number of files affected.
	ResetFileStatusesByKBUID(ctx context.Context, kbUID types.KBUIDType) (int64, error)

	// Deprecated methods

	// GetFileByKBUIDAndFileID returns the file by
	// knowledge base ID and file ID. File ID (filename) isn't unique by
	// knowledge base any more, so this method is deprecated. Files should be identified by their UID.
	GetFileByKBUIDAndFileID(ctx context.Context, kbUID types.KBUIDType, fileID string) (*FileModel, error)
}

// FileModel is the model for the file table
// Field ordering follows AIP standard: name (derived), id, display_name, slug, aliases, description
type FileModel struct {
	UID types.FileUIDType `gorm:"column:uid;type:uuid;default:gen_random_uuid();primaryKey" json:"uid"`
	// Field 2: Immutable canonical ID with prefix (e.g., "fil-8f3A2k9E7c1")
	ID string `gorm:"column:id;size:255;not null" json:"id"`
	// Field 3: Human-readable display name for UI (filename)
	DisplayName string `gorm:"column:display_name;size:255;not null" json:"display_name"`
	// Field 4: URL-friendly slug without prefix
	Slug string `gorm:"column:slug;size:255" json:"slug"`
	// Field 5: Previous slugs for backward compatibility with old URLs
	Aliases AliasesArray `gorm:"column:aliases;type:text[]" json:"aliases"`
	// Field 6: Optional description
	Description string `gorm:"column:description" json:"description"`
	// NamespaceUID is the namespace that owns this file
	NamespaceUID types.NamespaceUIDType `gorm:"column:namespace_uid;type:uuid;not null" json:"namespace_uid"`
	CreatorUID   types.CreatorUIDType   `gorm:"column:creator_uid;type:uuid;not null" json:"creator_uid"`
	// FileType stores the FileType enum string (e.g., "TYPE_PDF", "TYPE_TEXT")
	FileType string `gorm:"column:file_type;not null" json:"file_type"`
	// StoragePath is the path in the MinIO bucket
	StoragePath string `gorm:"column:storage_path;size:255;not null" json:"storage_path"`
	// ObjectUID is the foreign key reference to the object table.
	// Populated when file is created from a blob storage upload via object_uid.
	ObjectUID *types.ObjectUIDType `gorm:"column:object_uid;type:uuid" json:"object_uid"`
	// Process status is defined in the grpc proto file
	ProcessStatus string `gorm:"column:process_status;size:100;not null" json:"process_status"`
	// Note: use ExtraMetaDataMarshal method to marshal and unmarshal. do not populate this field directly
	// this field is used internally for the extra meta data of the file
	ExtraMetaData string `gorm:"column:extra_meta_data;type:jsonb" json:"extra_meta_data"`
	// Note: Content and summary are now stored as separate converted_file records and chunks in MinIO
	// They are no longer duplicated in this table
	CreateTime *time.Time     `gorm:"column:create_time;not null;default:CURRENT_TIMESTAMP" json:"create_time"`
	UpdateTime *time.Time     `gorm:"column:update_time;not null;autoUpdateTime" json:"update_time"` // Use autoUpdateTime
	DeleteTime gorm.DeletedAt `gorm:"column:delete_time;index" json:"delete_time"`
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
	Tags TagsArray `gorm:"column:tags;type:VARCHAR(255)[]" json:"tags"`
	// AI usage metadata from content and summary generation (stored as JSONB)
	UsageMetadata string `gorm:"column:usage_metadata;type:jsonb" json:"usage_metadata"`
	// This field is not stored in the database. It is used to unmarshal the UsageMetadata field
	UsageMetadataUnmarshal *UsageMetadata `gorm:"-" json:"usage_metadata_unmarshal"`
}

// TableName overrides the default table name for GORM
func (FileModel) TableName() string {
	return FileTableName
}

// FileKnowledgeBase represents the many-to-many relationship between files and knowledge bases
type FileKnowledgeBase struct {
	FileUID   types.FileUIDType          `gorm:"column:file_uid;type:uuid;primaryKey" json:"file_uid"`
	KBUID     types.KnowledgeBaseUIDType `gorm:"column:kb_uid;type:uuid;primaryKey" json:"kb_uid"`
	CreatedAt time.Time                  `gorm:"column:created_at;not null;default:now()" json:"created_at"`
}

// TableName overrides the default table name for GORM
func (FileKnowledgeBase) TableName() string {
	return "file_knowledge_base"
}

// ExtraMetaData is the extra meta data of the file
type ExtraMetaData struct {
	// StatusMessage stores status messages for all processing stages (success or failure)
	// Examples: "File processing completed successfully", "Conversion failed: invalid format"
	StatusMessage string `json:"status_message,omitempty"`

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

// UsageMetadata stores AI usage metadata from content and summary generation
// Format: {"content": {...}, "summary": {...}}
type UsageMetadata struct {
	Content map[string]interface{} `json:"content,omitempty"`
	Summary map[string]interface{} `json:"summary,omitempty"`
}

// FileColumns is the columns for the file table
type FileColumns struct {
	UID              string
	ID               string
	Owner            string
	CreatorUID       string
	DisplayName      string
	Slug             string
	Aliases          string
	FileType         string
	StoragePath      string
	ProcessStatus    string
	CreateTime       string
	ExtraMetaData    string
	UpdateTime       string
	DeleteTime       string
	RequesterUID     string
	Size             string
	ExternalMetadata string
	Tags             string
	UsageMetadata    string
}

// FileColumn is the columns for the file table
var FileColumn = FileColumns{
	UID:              "uid",
	ID:               "id",
	Owner:            "owner",
	CreatorUID:       "creator_uid",
	DisplayName:      "display_name",
	Slug:             "slug",
	Aliases:          "aliases",
	FileType:         "file_type",
	StoragePath:      "storage_path",
	ProcessStatus:    "process_status",
	ExtraMetaData:    "extra_meta_data",
	CreateTime:       "create_time",
	UpdateTime:       "update_time",
	DeleteTime:       "delete_time",
	Size:             "size",
	RequesterUID:     "requester_uid",
	ExternalMetadata: "external_metadata",
	Tags:             "tags",
	UsageMetadata:    "usage_metadata",
}

// ConvertingPipeline extracts the conversion pipeline, if present, from the
// file metadata.
func (kf FileModel) ConvertingPipeline() *string {
	if kf.ExtraMetaDataUnmarshal == nil || kf.ExtraMetaDataUnmarshal.ConvertingPipe == "" {
		return nil
	}

	return &kf.ExtraMetaDataUnmarshal.ConvertingPipe
}

// ExtraMetaDataMarshal marshals the ExtraMetaData struct to a JSON string
func (kf *FileModel) ExtraMetaDataMarshal() error {
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

// ExtraMetaDataUnmarshalFunc unmarshals the ExtraMetaData JSON string to a struct
func (kf *FileModel) ExtraMetaDataUnmarshalFunc() error {
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
func (kf *FileModel) ExternalMetadataToJSON() error {
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
func (kf *FileModel) JSONToExternalMetadata() error {
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
func (kf *FileModel) PublicExternalMetadataUnmarshal() *structpb.Struct {
	original := kf.ExternalMetadataUnmarshal
	if original == nil || original.Fields[constant.MetadataRequestKey] == nil {
		return original
	}

	md := proto.Clone(original).(*structpb.Struct)
	delete(md.Fields, constant.MetadataRequestKey)
	return md
}

// UsageMetadataMarshal marshals the UsageMetadata struct to a JSON string
func (kf *FileModel) UsageMetadataMarshal() error {
	if kf.UsageMetadataUnmarshal == nil {
		kf.UsageMetadata = "{}"
		return nil
	}
	data, err := json.Marshal(kf.UsageMetadataUnmarshal)
	if err != nil {
		return fmt.Errorf("failed to marshal usage metadata: %v", err)
	}
	kf.UsageMetadata = string(data)
	return nil
}

// UsageMetadataUnmarshalFunc unmarshals the UsageMetadata JSON string to a struct
func (kf *FileModel) UsageMetadataUnmarshalFunc() error {
	if kf.UsageMetadata == "" || kf.UsageMetadata == "{}" {
		kf.UsageMetadataUnmarshal = nil
		return nil
	}
	var data UsageMetadata
	if err := json.Unmarshal([]byte(kf.UsageMetadata), &data); err != nil {
		return fmt.Errorf("failed to unmarshal usage metadata: %v", err)
	}
	kf.UsageMetadataUnmarshal = &data
	return nil
}

// BeforeCreate is a GORM hook that marshals the ExtraMetaData, ExternalMetadata, and UsageMetadata fields
// and sets default ID and Slug if not provided
func (kf *FileModel) BeforeCreate(tx *gorm.DB) (err error) {
	// Generate prefixed canonical ID if not provided (AIP standard)
	if kf.ID == "" {
		kf.ID = utils.GeneratePrefixedResourceID(utils.PrefixFile, uuid.UUID(kf.UID))
		tx.Statement.SetColumn("ID", kf.ID)
	}
	// Generate slug from display name if not provided
	if kf.Slug == "" && kf.DisplayName != "" {
		kf.Slug = utils.GenerateSlug(kf.DisplayName)
		tx.Statement.SetColumn("Slug", kf.Slug)
	}
	if err := kf.ExtraMetaDataMarshal(); err != nil {
		return err
	}
	if err := kf.ExternalMetadataToJSON(); err != nil {
		return err
	}
	return kf.UsageMetadataMarshal()
}

// BeforeSave is a GORM hook that marshals the ExtraMetaData, ExternalMetadata, and UsageMetadata fields
func (kf *FileModel) BeforeSave(tx *gorm.DB) (err error) {
	if err := kf.ExtraMetaDataMarshal(); err != nil {
		return err
	}
	if err := kf.ExternalMetadataToJSON(); err != nil {
		return err
	}
	return kf.UsageMetadataMarshal()
}

// BeforeUpdate is a GORM hook that marshals the ExtraMetaData, ExternalMetadata, and UsageMetadata fields
func (kf *FileModel) BeforeUpdate(tx *gorm.DB) (err error) {
	if err := kf.ExtraMetaDataMarshal(); err != nil {
		return err
	}
	if err := kf.ExternalMetadataToJSON(); err != nil {
		return err
	}
	return kf.UsageMetadataMarshal()
}

// AfterFind is a GORM hook that unmarshals the ExtraMetaData, ExternalMetadata, and UsageMetadata fields
func (kf *FileModel) AfterFind(tx *gorm.DB) (err error) {
	if err := kf.ExtraMetaDataUnmarshalFunc(); err != nil {
		return err
	}
	if err := kf.JSONToExternalMetadata(); err != nil {
		return err
	}
	return kf.UsageMetadataUnmarshalFunc()
}

// CreateFile creates a new file and associates it with a knowledge base.
// The file is namespace-scoped, and the KB association is stored in the junction table.
func (r *repository) CreateFile(ctx context.Context, file FileModel, kbUID types.KnowledgeBaseUIDType, externalServiceCall func(fileUID string) error) (*FileModel, error) {
	// CRITICAL: Use transaction with row-level locking to prevent race conditions
	// Without locking, this race can occur:
	// 1. Check: KB exists ✓
	// 2. DeleteKB starts and locks KB row
	// 3. File created (succeeds because no FK constraint)
	// 4. DeleteKB completes (CASCADE doesn't affect file - no FK)
	// 5. Result: Zombie file referencing deleted KB
	//
	// With locking (SELECT ... FOR UPDATE):
	// 1. Transaction locks KB row
	// 2. DeleteKB blocks waiting for lock
	// 3. File created
	// 4. KB usage incremented (atomic with file creation)
	// 5. Transaction commits, releases lock
	// 6. DeleteKB proceeds and CASCADE deletes file ✓
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Lock the KB row with SELECT ... FOR UPDATE to prevent concurrent deletion
		var existingKB KnowledgeBaseModel
		whereString := fmt.Sprintf("%v = ? AND %s IS NULL", KnowledgeBaseColumn.UID, KnowledgeBaseColumn.DeleteTime)
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(whereString, kbUID).First(&existingKB).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				return fmt.Errorf("knowledge base does not exist or has been deleted")
			}
			return fmt.Errorf("checking knowledge base existence: %w", err)
		}

		// Create the file (namespace-scoped)
		if err := tx.Create(&file).Error; err != nil {
			return err
		}

		// Create the file-KB association in the junction table
		association := FileKnowledgeBase{
			FileUID: file.UID,
			KBUID:   kbUID,
		}
		if err := tx.Create(&association).Error; err != nil {
			return fmt.Errorf("creating file-kb association: %w", err)
		}

		// Increase knowledge base usage in same transaction
		// This prevents race conditions and makes the operation atomic with file creation
		where := fmt.Sprintf("%v = ?", KnowledgeBaseColumn.UID)
		expr := fmt.Sprintf("%v + ?", KnowledgeBaseColumn.Usage)
		if err := tx.Model(&KnowledgeBaseModel{}).Where(where, kbUID.String()).Update(KnowledgeBaseColumn.Usage, gorm.Expr(expr, int(file.Size))).Error; err != nil {
			return fmt.Errorf("increasing knowledge base usage: %w", err)
		}

		// Call the external service
		if externalServiceCall != nil {
			if err := externalServiceCall(file.UID.String()); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &file, nil
}

// KnowledgeBaseFileListParams contains the params to fetch a list of knowledge
// base files.
type KnowledgeBaseFileListParams struct {
	OwnerUID string
	KBUID    string

	// AIP-160 filter expression
	Filter filtering.Filter

	// Pagination
	PageSize  int
	PageToken string
}

// FileList contains a list of files.
type FileList struct {
	Files         []FileModel
	TotalCount    int
	NextPageToken string
}

// ListFiles returns a paginated list of files within a knowledge
// base. The list can optionally be filtered by AIP-160 filter expressions.
// Files are joined with the file_knowledge_base junction table to filter by KB.
// If KBUID is empty, all files in the namespace are listed without KB filtering.
func (r *repository) ListFiles(ctx context.Context, params KnowledgeBaseFileListParams) (*FileList, error) {
	var files []FileModel
	var totalCount int64

	q := r.db.Model(&FileModel{})

	if params.KBUID != "" {
		// Join with junction table to filter by KB when KBUID is provided
		// file.namespace_uid = ? AND file_knowledge_base.kb_uid = ?
		q = q.Joins("INNER JOIN file_knowledge_base ON file_knowledge_base.file_uid = file.uid").
			Where("file.namespace_uid = ? AND file_knowledge_base.kb_uid = ?", params.OwnerUID, params.KBUID)
	} else {
		// List all files in the namespace without KB filtering
		q = q.Where("file.namespace_uid = ?", params.OwnerUID)
	}

	// Apply AIP-160 filter if provided
	var expr *clause.Expr
	var err error
	if expr, err = r.TranspileFilter(params.Filter); err != nil {
		return nil, fmt.Errorf("transpiling filter: %w", err)
	}

	if expr != nil {
		// Apply the filter expression directly using Where with SQL and Vars
		q = q.Where(expr.SQL, expr.Vars...)
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
		kbfs, err := r.GetFilesByFileUIDs(ctx, []types.FileUIDType{tokenUUID})
		if err != nil {
			return nil, fmt.Errorf("building query from token: %w", err)
		}

		if len(kbfs) == 0 {
			return nil, fmt.Errorf("invalid next page token")
		}

		q = q.Where("file.update_time <= ?", kbfs[0].UpdateTime)
	}

	// TODO INS-8162: the repository method (and the upstream handler) should
	// take an `ordering` parameter so clients can choose the sorting.
	q = q.Order("file.update_time DESC")

	// Fetch the records
	if err := q.Find(&files).Error; err != nil {
		return nil, fmt.Errorf("fetching records: %w", err)
	}

	resp := &FileList{
		Files:      files,
		TotalCount: int(totalCount),
	}

	if len(files) > params.PageSize {
		resp.NextPageToken = files[params.PageSize].UID.String()
		resp.Files = files[:params.PageSize]
	}

	return resp, nil
}

// delete the file which is to set the delete time
func (r *repository) DeleteFile(ctx context.Context, fileUID string) error {
	currentTime := time.Now()
	whereClause := fmt.Sprintf("%v = ? AND %v is NULL", FileColumn.UID, FileColumn.DeleteTime)
	if err := r.db.WithContext(ctx).Model(&FileModel{}).
		Where(whereClause, fileUID).
		Update(FileColumn.DeleteTime, currentTime).Error; err != nil {
		return err
	}
	return nil
}

// hard delete all files in the knowledge base
// Uses the junction table to find files associated with the KB.
// Junction table entries are CASCADE deleted when files are deleted.
func (r *repository) DeleteAllFiles(ctx context.Context, kbUID string) error {
	// Delete files that are associated with this KB via the junction table
	// Use subquery to find file UIDs associated with the KB
	subQuery := r.db.Table("file_knowledge_base").Select("file_uid").Where("kb_uid = ?", kbUID)
	if err := r.db.WithContext(ctx).Unscoped().
		Where("uid IN (?)", subQuery).
		Delete(&FileModel{}).Error; err != nil {
		return err
	}
	return nil
}

// ResetFileStatusesByKBUID resets all files in a KB to NOTSTARTED status for re-embedding.
// This is used when resetting a KB's embeddings to enable BM25 support.
// Returns the number of files affected.
func (r *repository) ResetFileStatusesByKBUID(ctx context.Context, kbUID types.KBUIDType) (int64, error) {
	// Use subquery to find file UIDs associated with the KB
	subQuery := r.db.Table("file_knowledge_base").Select("file_uid").Where("kb_uid = ?", kbUID)

	result := r.db.WithContext(ctx).
		Model(&FileModel{}).
		Where("uid IN (?) AND delete_time IS NULL", subQuery).
		Updates(map[string]any{
			"process_status": artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_NOTSTARTED.String(),
		})

	if result.Error != nil {
		return 0, fmt.Errorf("failed to reset file statuses: %w", result.Error)
	}

	return result.RowsAffected, nil
}

// ProcessFiles updates the process status of the files
func (r *repository) ProcessFiles(
	ctx context.Context,
	fileUIDs []string,
	requester types.RequesterUIDType,
) ([]FileModel, error) {

	db := r.db.WithContext(ctx)

	// Update the process status of the files
	updates := map[string]any{
		"process_status": artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_PROCESSING.String(),
		"requester_uid":  requester,
		// Clear previous status message
		"extra_meta_data": gorm.Expr("COALESCE(extra_meta_data, '{}'::jsonb) || ?::jsonb", `{"status_message": ""}`),
	}

	if err := db.Model(&FileModel{}).Where("uid IN ?", fileUIDs).Updates(updates).Error; err != nil {
		return nil, fmt.Errorf("updating records: %w", err)
	}

	// Retrieve the updated records
	var files []FileModel
	if err := db.Where("uid IN ?", fileUIDs).Find(&files).Error; err != nil {
		return nil, fmt.Errorf("retrieving updated records: %w", err)
	}

	return files, nil
}

// UpdateFile updates the data and retrieves the latest data
func (r *repository) UpdateFile(ctx context.Context, fileUID string, updateMap map[string]any) (*FileModel, error) {
	var updatedFile FileModel

	// Convert tags if present to TagsArray type for proper PostgreSQL array handling
	if tags, ok := updateMap["tags"]; ok {
		if tagSlice, ok := tags.([]string); ok {
			updateMap["tags"] = TagsArray(tagSlice)
		}
	}

	// Use a transaction to update and then fetch the latest data
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Update the data
		if err := tx.Model(&FileModel{}).
			Where(FileColumn.UID+" = ?", fileUID).
			Updates(updateMap).Error; err != nil {
			return err
		}

		// Fetch the latest data
		if err := tx.Where(FileColumn.UID+" = ?", fileUID).First(&updatedFile).Error; err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &updatedFile, nil
}

// GetNotStartedFileCount counts files in NOTSTARTED status (regardless of creation time)
// Used by synchronization to detect files waiting to start processing.
// With sequential dual-processing, staging files legitimately stay NOTSTARTED
// until production completes, so we don't filter by age.
// Uses the junction table to filter by KB.
func (r *repository) GetNotStartedFileCount(ctx context.Context, kbUID types.KBUIDType) (int64, error) {
	var count int64

	err := r.db.WithContext(ctx).
		Table(FileTableName).
		Joins("INNER JOIN file_knowledge_base ON file_knowledge_base.file_uid = file.uid").
		Where("file_knowledge_base.kb_uid = ? AND file.delete_time IS NULL AND file.process_status = ?",
			kbUID,
			artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_NOTSTARTED.String()).
		Count(&count).
		Error

	if err != nil {
		return 0, fmt.Errorf("failed to count NOTSTARTED files: %w", err)
	}

	return count, nil
}

// GetNotStartedFileCountExcluding counts files in NOTSTARTED status excluding specific file UIDs
// This is used during synchronization to exclude files that were just created by reconciliation
// and haven't been picked up by Temporal yet (avoiding false positives)
// Uses the junction table to filter by KB.
func (r *repository) GetNotStartedFileCountExcluding(ctx context.Context, kbUID types.KBUIDType, excludeUIDs []types.FileUIDType) (int64, error) {
	var count int64

	query := r.db.WithContext(ctx).
		Table(FileTableName).
		Joins("INNER JOIN file_knowledge_base ON file_knowledge_base.file_uid = file.uid").
		Where("file_knowledge_base.kb_uid = ? AND file.delete_time IS NULL AND file.process_status = ?",
			kbUID,
			artifactpb.FileProcessStatus_FILE_PROCESS_STATUS_NOTSTARTED.String())

	// Exclude specific file UIDs if provided
	if len(excludeUIDs) > 0 {
		query = query.Where("file.uid NOT IN ?", excludeUIDs)
	}

	err := query.Count(&count).Error
	if err != nil {
		return 0, fmt.Errorf("failed to count NOTSTARTED files (excluding %d UIDs): %w", len(excludeUIDs), err)
	}

	return count, nil
}

// GetFileCountByKnowledgeBaseUID counts files by KB UID, optionally filtered by process status.
// Uses the junction table to filter by KB.
func (r *repository) GetFileCountByKnowledgeBaseUID(ctx context.Context, kbUID types.KBUIDType, processStatus string) (int64, error) {
	var count int64

	query := r.db.WithContext(ctx).
		Table(FileTableName).
		Joins("INNER JOIN file_knowledge_base ON file_knowledge_base.file_uid = file.uid").
		Where("file_knowledge_base.kb_uid = ? AND file.delete_time IS NULL", kbUID)

	// Add process status filter if specified
	// Support comma-separated list of statuses for IN clause
	if processStatus != "" {
		statuses := strings.Split(processStatus, ",")
		if len(statuses) == 1 {
			// Single status: use exact match
			query = query.Where("file.process_status = ?", processStatus)
		} else {
			// Multiple statuses: use IN clause
			query = query.Where("file.process_status IN ?", statuses)
		}
	}

	err := query.Count(&count).Error
	if err != nil {
		return 0, fmt.Errorf("error counting files: %w", err)
	}

	return count, nil
}

// GetFileCountByKnowledgeBaseUIDIncludingDeleted counts files by KB UID and process status, INCLUDING soft-deleted files
// This is used by cleanup workflows to wait for in-progress file processing to complete before dropping collections
// CRITICAL: Must include deleted files because:
// 1. When KB is deleted, files are CASCADE soft-deleted
// 2. But file processing workflows continue running (designed to handle soft-deleted files)
// 3. Cleanup must wait for these workflows to complete before dropping the Milvus collection
// Uses the junction table to filter by KB.
func (r *repository) GetFileCountByKnowledgeBaseUIDIncludingDeleted(ctx context.Context, kbUID types.KBUIDType, processStatus string) (int64, error) {
	var count int64

	// Do NOT filter by delete_time - we need to count soft-deleted files too
	query := r.db.WithContext(ctx).
		Table(FileTableName).
		Joins("INNER JOIN file_knowledge_base ON file_knowledge_base.file_uid = file.uid").
		Where("file_knowledge_base.kb_uid = ?", kbUID)

	// Add process status filter if specified
	// Support comma-separated list of statuses for IN clause
	if processStatus != "" {
		statuses := strings.Split(processStatus, ",")
		if len(statuses) == 1 {
			// Single status: use exact match
			query = query.Where("file.process_status = ?", processStatus)
		} else {
			// Multiple statuses: use IN clause
			query = query.Where("file.process_status IN ?", statuses)
		}
	}

	err := query.Count(&count).Error
	if err != nil {
		return 0, fmt.Errorf("error counting files (including deleted): %w", err)
	}

	return count, nil
}

// GetContentByFileUIDs returns the content converted file source table and uid by file UID list
func (r *repository) GetContentByFileUIDs(ctx context.Context, files []FileModel) (
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
		// We specifically need the CONTENT converted file for chunks and tokens
		convertedFile, err := r.GetConvertedFileByFileUIDAndType(ctx, file.UID, artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_CONTENT)
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

func (r *repository) GetFilesByFileUIDs(
	ctx context.Context,
	fileUIDs []types.FileUIDType,
	columns ...string,
) ([]FileModel, error) {

	var files []FileModel
	// Convert UUIDs to strings as GORM works with strings in queries
	var stringUIDs []string
	for _, uid := range fileUIDs {
		stringUIDs = append(stringUIDs, uid.String())
	}
	where := fmt.Sprintf("%v IN ? AND %v IS NULL", FileColumn.UID, FileColumn.DeleteTime)
	query := r.db.WithContext(ctx)
	if len(columns) > 0 {
		query = query.Select(columns)
	}
	// Query the database for files with the given UIDs
	if err := query.Where(where, stringUIDs).Find(&files).Error; err != nil {
		// If GORM returns ErrRecordNotFound, it's not considered an error in this context
		if err == gorm.ErrRecordNotFound {
			return []FileModel{}, nil
		}
		// Return any other error that might have occurred during the query
		return nil, err
	}

	// Return the found files, or an empty slice if none were found
	return files, nil
}

// GetFilesByFileUIDsIncludingDeleted retrieves files by UIDs, INCLUDING soft-deleted files
// This is needed for embedding activities which may run after a file has been soft-deleted during
// dual deletion. The embeddings must still be generated for existing chunks.
func (r *repository) GetFilesByFileUIDsIncludingDeleted(
	ctx context.Context,
	fileUIDs []types.FileUIDType,
	columns ...string,
) ([]FileModel, error) {

	var files []FileModel
	// Convert UUIDs to strings as GORM works with strings in queries
	var stringUIDs []string
	for _, uid := range fileUIDs {
		stringUIDs = append(stringUIDs, uid.String())
	}
	// CRITICAL: Do NOT filter by delete_time - include soft-deleted files
	// Use .Unscoped() to bypass gorm.DeletedAt filtering
	where := fmt.Sprintf("%v IN ?", FileColumn.UID)
	query := r.db.WithContext(ctx).Unscoped() // Include soft-deleted files
	if len(columns) > 0 {
		query = query.Select(columns)
	}
	// Query the database for files with the given UIDs
	if err := query.Where(where, stringUIDs).Find(&files).Error; err != nil {
		// If GORM returns ErrRecordNotFound, it's not considered an error in this context
		if err == gorm.ErrRecordNotFound {
			return []FileModel{}, nil
		}
		// Return any other error that might have occurred during the query
		return nil, err
	}

	// Return the found files, or an empty slice if none were found
	return files, nil
}

// GetFilesByFileIDs returns files by their IDs (hash-based slug or UUID).
// Supported formats:
// 1. Hash-based IDs (e.g., 'my-file-abc12345') - preferred format, less prone to LLM hallucination
// 2. UUID strings (e.g., 'aff8544b-3ccc-4e73-bbe5-8ec922446542') - legacy format, searched by uid column
// The function also checks aliases for backward compatibility when display_name is renamed.
// Unlike GetFileByIDOrAlias, this does NOT require a KB UID since file IDs include a unique hash suffix.
func (r *repository) GetFilesByFileIDs(
	ctx context.Context,
	fileIDs []string,
	columns ...string,
) ([]FileModel, error) {
	if len(fileIDs) == 0 {
		return []FileModel{}, nil
	}

	var files []FileModel

	// Build query: check id, aliases, AND uid (for legacy UUID lookups)
	// Since file IDs include a unique hash suffix (e.g., 'my-file-abc12345'), they're globally unique
	// across all KBs, so we don't need to filter by KB UID.
	query := r.db.WithContext(ctx)
	if len(columns) > 0 {
		query = query.Select(columns)
	}

	// Query files where id matches OR any of the fileIDs is in the aliases array OR uid matches (for UUIDs)
	// PostgreSQL syntax: ? = ANY(aliases), uid::text = ? for UUID comparison
	// We need to build an OR condition for each fileID checking id, aliases, and uid
	var conditions []string
	var args []interface{}
	for _, fid := range fileIDs {
		// Check if fid looks like a UUID (36 chars with dashes in right places)
		if len(fid) == 36 && fid[8] == '-' && fid[13] == '-' && fid[18] == '-' && fid[23] == '-' {
			// For UUIDs, also check the uid column
			conditions = append(conditions, "(id = ? OR ? = ANY(aliases) OR uid::text = ?)")
			args = append(args, fid, fid, fid)
		} else {
			// For non-UUIDs, only check id and aliases
			conditions = append(conditions, "(id = ? OR ? = ANY(aliases))")
			args = append(args, fid, fid)
		}
	}

	whereClause := fmt.Sprintf("(%s) AND delete_time IS NULL", strings.Join(conditions, " OR "))
	if err := query.Where(whereClause, args...).Find(&files).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return []FileModel{}, nil
		}
		return nil, fmt.Errorf("failed to get files by IDs: %w", err)
	}

	return files, nil
}

// GetFileByIDOrAlias looks up a file by its canonical ID or any of its aliases within a KB.
// Aliases preserve old URLs when display_name is renamed.
// Uses the junction table to filter by KB.
func (r *repository) GetFileByIDOrAlias(ctx context.Context, kbUID types.KBUIDType, id string) (*FileModel, error) {
	var file FileModel
	// Join with junction table to filter by KB
	err := r.db.WithContext(ctx).
		Joins("INNER JOIN file_knowledge_base ON file_knowledge_base.file_uid = file.uid").
		Where("file_knowledge_base.kb_uid = ? AND file.delete_time IS NULL AND (file.id = ? OR ? = ANY(file.aliases))", kbUID, id, id).
		First(&file).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, fmt.Errorf("file not found: %s", id)
		}
		return nil, fmt.Errorf("failed to get file: %w", err)
	}
	return &file, nil
}

// GetFilesByName retrieves files by name in a specific KB
// Used for dual deletion to find corresponding files in staging/rollback KB
// Uses the junction table to filter by KB.
func (r *repository) GetFilesByName(
	ctx context.Context,
	kbUID types.KBUIDType,
	filename string,
) ([]FileModel, error) {
	var files []FileModel

	// Join with junction table to filter by KB
	// GORM's DeletedAt automatically filters out soft-deleted files, so no manual check needed
	if err := r.db.WithContext(ctx).
		Joins("INNER JOIN file_knowledge_base ON file_knowledge_base.file_uid = file.uid").
		Where("file_knowledge_base.kb_uid = ? AND file.display_name = ?", kbUID, filename).
		Find(&files).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return []FileModel{}, nil
		}
		return nil, fmt.Errorf("querying files by name: %w", err)
	}

	return files, nil
}

// SourceMeta represents the metadata of the source file
type SourceMeta struct {
	OriginalFileUID  types.FileUIDType          `json:"original_file_uid"`  // OriginalFileUID is the UID of the original file
	OriginalFileName string                     `json:"original_file_name"` // OriginalFileName is the name of the original file
	KnowledgeBaseUID types.KnowledgeBaseUIDType `json:"knowledge_base_uid"` // KnowledgeBaseUID is the UID of the knowledge base
	StoragePath      string                     `json:"storage_path"`       // StoragePath is the path in blob storage
	CreateTime       time.Time                  `json:"create_time"`        // CreateTime is the creation time of the source file
	UpdateTime       time.Time                  `json:"update_time"`        // UpdateTime is the update time of the source file
}

// GetSourceByFileUID returns the content converted file metadata by file UID.
// All file types have converted files for consistency.
// For the /source endpoint, this returns the CONTENT converted file, not the summary.
// We order by create_time ASC to get the first (content) converted file, as the summary is created later.
// TODO: Rename this to GetContentByFileUID
func (r *repository) GetSourceByFileUID(ctx context.Context, fileUID types.FileUIDType) (*SourceMeta, error) {
	logger, _ := logx.GetZapLogger(ctx)

	// Get the original file metadata
	var file FileModel
	where := fmt.Sprintf("%v = ?", FileColumn.UID)
	if err := r.db.WithContext(ctx).Where(where, fileUID).First(&file).Error; err != nil {
		return nil, fmt.Errorf("fetching file: %w", err)
	}

	// Get the CONTENT converted file using explicit type query
	// All file types now have converted files stored with explicit type markers
	// For plaintext files, the content is identical to the original
	// For other files (PDF/DOC/etc.), the content is the markdown conversion
	where = fmt.Sprintf("%s = ? AND %s = ?", ConvertedFileColumn.FileUID, ConvertedFileColumn.ConvertedType)
	var convertedFile ConvertedFileModel
	err := r.db.WithContext(ctx).Where(where, fileUID, artifactpb.ConvertedFileType_CONVERTED_FILE_TYPE_CONTENT.String()).First(&convertedFile).Error
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
		OriginalFileName: file.DisplayName,
		StoragePath:      convertedFile.StoragePath,
		CreateTime:       *convertedFile.CreateTime,
		UpdateTime:       *convertedFile.UpdateTime,
		KnowledgeBaseUID: convertedFile.KnowledgeBaseUID,
	}, nil
}

// UpdateFileMetadata updates the metadata fields of a file.
// Only the nonzero values in the request will be used in the update, keeping
// the existing values for the rest of the fields.
// This method performs an update lock in the record to avoid collisions with
// other requests.
func (r *repository) UpdateFileMetadata(ctx context.Context, fileUID types.FileUIDType, updates ExtraMetaData) error {
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var file FileModel

		// Lock the row for update within the transaction
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where(FileColumn.UID+" = ?", fileUID).
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

		// CRITICAL: Always update pipeline fields (even to empty strings)
		// This allows switching between pipeline route and AI client route:
		// - Pipeline route: sets pipeline names (e.g., "preset/indexing-generate-content@v1.4.0")
		// - AI client route: clears pipelines by passing empty strings
		// Without this fix, reprocessing with AI client would leave old pipeline values in place
		file.ExtraMetaDataUnmarshal.ConvertingPipe = updates.ConvertingPipe
		file.ExtraMetaDataUnmarshal.SummarizingPipe = updates.SummarizingPipe
		file.ExtraMetaDataUnmarshal.ChunkingPipe = updates.ChunkingPipe
		file.ExtraMetaDataUnmarshal.EmbeddingPipe = updates.EmbeddingPipe

		// For other fields, only update if non-zero/non-empty (keep existing logic)
		if updates.StatusMessage != "" {
			file.ExtraMetaDataUnmarshal.StatusMessage = updates.StatusMessage
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

		// Save the updated FileModel within the transaction
		if err := tx.Save(&file).Error; err != nil {
			return fmt.Errorf("storing record: %w", err)
		}

		return nil
	})

	return err
}

// UpdateFileUsageMetadata updates the usage metadata for a file
// This stores AI usage metadata (token counts) from content and summary processing
// According to Gemini API docs: https://ai.google.dev/gemini-api/docs/tokens?lang=python
func (r *repository) UpdateFileUsageMetadata(ctx context.Context, fileUID types.FileUIDType, usageMetadata UsageMetadata) error {
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var file FileModel

		// Lock the row for update within the transaction
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where(FileColumn.UID+" = ?", fileUID).
			First(&file).Error

		if err != nil {
			return fmt.Errorf("fetching file: %w", err)
		}

		// Set the usage metadata (will be marshaled by GORM hooks on Save)
		file.UsageMetadataUnmarshal = &usageMetadata

		// Marshal the usage metadata
		if err := file.UsageMetadataMarshal(); err != nil {
			return fmt.Errorf("marshalling usage metadata: %w", err)
		}

		// Save the updated FileModel within the transaction
		if err := tx.Save(&file).Error; err != nil {
			return fmt.Errorf("storing record: %w", err)
		}

		return nil
	})

	return err
}

// DeleteFileAndDecreaseUsage delete the file and decrease the knowledge base usage
// Since files can belong to multiple KBs, usage is decreased for ALL associated KBs.
func (r *repository) DeleteFileAndDecreaseUsage(ctx context.Context, fileUID types.FileUIDType) error {
	currentTime := time.Now()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// get the file
		file, err := r.GetFilesByFileUIDs(ctx, []types.FileUIDType{fileUID}, FileColumn.Size)
		if err != nil {
			return err
		} else if len(file) == 0 {
			return fmt.Errorf("file not found by file uid: %v", fileUID)
		}

		// Get all KB associations from junction table
		var associations []FileKnowledgeBase
		if err := tx.Where("file_uid = ?", fileUID).Find(&associations).Error; err != nil {
			return fmt.Errorf("getting file-kb associations: %w", err)
		}

		whereClause := fmt.Sprintf("%v = ? AND %v is NULL", FileColumn.UID, FileColumn.DeleteTime)
		if err := tx.Model(&FileModel{}).
			Where(whereClause, fileUID).
			Update(FileColumn.DeleteTime, currentTime).Error; err != nil {
			return err
		}

		// Decrease usage for all associated KBs
		for _, assoc := range associations {
			err = r.IncreaseKnowledgeBaseUsage(ctx, tx, assoc.KBUID.String(), int(-file[0].Size))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// GetFileByKBUIDAndFileID returns the file by
// knowledge base ID and file ID.
// NOTE: this method is deprecated, files should be accessed by UID.
// Uses the junction table to filter by KB.
func (r *repository) GetFileByKBUIDAndFileID(ctx context.Context, kbUID types.KBUIDType, fileID string) (*FileModel, error) {
	var file FileModel
	// Join with junction table to filter by KB
	if err := r.db.WithContext(ctx).
		Joins("INNER JOIN file_knowledge_base ON file_knowledge_base.file_uid = file.uid").
		Where("file_knowledge_base.kb_uid = ? AND file.display_name = ? AND file.delete_time IS NULL", kbUID, fileID).
		First(&file).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, fmt.Errorf("file not found by knowledge base ID: %v and file ID: %v", kbUID, fileID)
		}
		return nil, err
	}
	return &file, nil
}

// GetKnowledgeBaseUIDsForFile returns all KB UIDs associated with a file via the junction table
// Only returns non-deleted (delete_time IS NULL) KBs to ensure permission checks
// are performed against valid, accessible knowledge bases.
// Note: We include staging/rollback KBs (staging=true) because:
// - Rollback KBs contain valid file data that users can access
// - Only soft-deleted KBs should be excluded from permission checks
func (r *repository) GetKnowledgeBaseUIDsForFile(ctx context.Context, fileUID types.FileUIDType) ([]types.KnowledgeBaseUIDType, error) {
	// Get all file-KB associations
	var associations []FileKnowledgeBase
	if err := r.db.WithContext(ctx).Where("file_uid = ?", fileUID).Find(&associations).Error; err != nil {
		return nil, fmt.Errorf("getting KB associations for file: %w", err)
	}

	if len(associations) == 0 {
		return []types.KnowledgeBaseUIDType{}, nil
	}

	// Extract KB UIDs from associations
	kbUIDsToCheck := make([]types.KnowledgeBaseUIDType, len(associations))
	for i, assoc := range associations {
		kbUIDsToCheck[i] = assoc.KBUID
	}

	// Filter out soft-deleted KBs by checking which ones still exist and are not deleted
	// This ensures permission checks are only performed against valid, accessible KBs
	var validKBs []KnowledgeBaseModel
	if err := r.db.WithContext(ctx).
		Where("uid IN ? AND delete_time IS NULL", kbUIDsToCheck).
		Find(&validKBs).Error; err != nil {
		return nil, fmt.Errorf("filtering valid KBs: %w", err)
	}

	// Build result with only valid (non-deleted) KB UIDs
	result := make([]types.KnowledgeBaseUIDType, len(validKBs))
	for i, kb := range validKBs {
		result[i] = types.KnowledgeBaseUIDType(kb.UID)
	}

	return result, nil
}

// GetKnowledgeBaseIDsForFiles returns a map of file UID to KB IDs (hash-based IDs like "kb-xxx")
// for efficient batch lookup when listing files without a KB filter.
func (r *repository) GetKnowledgeBaseIDsForFiles(ctx context.Context, fileUIDs []types.FileUIDType) (map[types.FileUIDType][]string, error) {
	if len(fileUIDs) == 0 {
		return make(map[types.FileUIDType][]string), nil
	}

	// Query junction table to get file-to-KB UID mappings
	var associations []FileKnowledgeBase
	if err := r.db.WithContext(ctx).Where("file_uid IN ?", fileUIDs).Find(&associations).Error; err != nil {
		return nil, fmt.Errorf("getting KB associations for files: %w", err)
	}

	// Collect unique KB UIDs
	kbUIDSet := make(map[types.KnowledgeBaseUIDType]struct{})
	for _, assoc := range associations {
		kbUIDSet[assoc.KBUID] = struct{}{}
	}
	kbUIDs := make([]types.KnowledgeBaseUIDType, 0, len(kbUIDSet))
	for uid := range kbUIDSet {
		kbUIDs = append(kbUIDs, uid)
	}

	// Batch fetch KBs to get their IDs
	kbUIDToID := make(map[types.KnowledgeBaseUIDType]string)
	if len(kbUIDs) > 0 {
		var kbs []KnowledgeBaseModel
		if err := r.db.WithContext(ctx).Where("uid IN ?", kbUIDs).Find(&kbs).Error; err != nil {
			return nil, fmt.Errorf("fetching KBs by UIDs: %w", err)
		}
		for _, kb := range kbs {
			kbUIDToID[kb.UID] = kb.ID
		}
	}

	// Build result map: file UID -> []KB IDs
	result := make(map[types.FileUIDType][]string)
	for _, fileUID := range fileUIDs {
		result[fileUID] = []string{}
	}
	for _, assoc := range associations {
		if kbID, ok := kbUIDToID[assoc.KBUID]; ok {
			result[assoc.FileUID] = append(result[assoc.FileUID], kbID)
		}
	}

	return result, nil
}
