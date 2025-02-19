package constant

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
	TB
)

const MaxPayloadSize = 1024 * 1024 * 256

// Constants for resource owner
const DefaultUserID string = "admin"

// Header keys
const HeaderUserUIDKey = "Instill-User-Uid"
const HeaderRequesterUIDKey = "Instill-Requester-Uid"
const HeaderAuthTypeKey = "Instill-Auth-Type"
const HeaderVisitorUIDKey = "Instill-Visitor-Uid"

type FileType string

var DocumentFileType FileType = "document"
var VideoFileType FileType = "video"
var ImageFileType FileType = "image"
var AudioFileType FileType = "audio"

type ContentType string

var ChunkContentType ContentType = "chunk"
var SummaryContentType ContentType = "summary"
var AugmentedContentType ContentType = "augmented"

