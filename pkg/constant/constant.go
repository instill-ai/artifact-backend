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

// HeaderBackendKey identifies one of the backend services as the agent for the
// request. This is used only in private requests and its purpose is
// capturing EE requests to lift some restrictions.
//
// TODO jvallesm: this header is shared across several services, so it should
// be defined in a common package. However, we don't have the enterprise
// equivalent of instill-ai/x. For now, the value of constant will need to be
// copied in several services.
const HeaderBackendKey = "Instill-Backend"

// AgentBackend identifies requests from Instill Agent.
const AgentBackend = "agent-backend"
