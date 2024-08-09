package constant

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
	TB
)

const MaxPayloadSize = 1024 * 1024 * 32

// Constants for resource owner
const DefaultUserID string = "admin"

// Header keys
const HeaderUserUIDKey = "Instill-User-Uid"
const HeaderRequesterUIDKey = "Instill-Requester-Uid"
const HeaderAuthTypeKey = "Instill-Auth-Type"
const HeaderVisitorUIDKey = "Instill-Visitor-Uid"
