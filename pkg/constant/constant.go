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
