package constant

import (
	"strings"

	"github.com/instill-ai/artifact-backend/pkg/types"
)

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
	TB
)

// Constants for resource owner
const DefaultUserID string = "admin"

// MetadataRequestKey is the key where the file upload request metadata is
// stored in the ExternalMetadata property of a file.
const MetadataRequestKey = "instill-request"

// KBCollectionName converts a knowledge base UID to a valid Milvus collection name.
// For historical reasons, collection names can only contain numbers, letters
// and underscores, so UUID is here converted to a valid collection name.
func KBCollectionName(uid types.KBUIDType) string {
	const kbCollectionPrefix = "kb_"
	return kbCollectionPrefix + strings.ReplaceAll(uid.String(), "-", "_")
}
