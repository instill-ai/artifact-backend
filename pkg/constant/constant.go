package constant

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
	TB
)

// Constants for resource owner
const DefaultUserID string = "admin"

type FileType string

var DocumentFileType FileType = "document"
var VideoFileType FileType = "video"
var ImageFileType FileType = "image"
var AudioFileType FileType = "audio"

type ContentType string

var ChunkContentType ContentType = "chunk"
var SummaryContentType ContentType = "summary"
var AugmentedContentType ContentType = "augmented"

// MetadataRequestKey is the key where the file upload request metadata is
// stored in the ExternalMetadata property of a file.
const MetadataRequestKey = "instill-request"
