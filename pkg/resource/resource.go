package resource

import (
	"fmt"

	"github.com/instill-ai/artifact-backend/pkg/types"
)

// NamespaceType represents the type of the namespace
type NamespaceType string

const (
	// User represents a user namespace
	User NamespaceType = "users"
	// Organization represents an organization namespace
	Organization NamespaceType = "organizations"
)

// Namespace represents a namespace
type Namespace struct {
	// NsType represents the type of the namespace
	NsType NamespaceType
	// NsID represents the ID of the namespace
	NsID string
	// NsUID represents the UID of the namespace
	NsUID types.NamespaceUIDType
}

// Name returns the name of the namespace
func (ns Namespace) Name() string {
	return fmt.Sprintf("%s/%s", ns.NsType, ns.NsID)
}

// Permalink returns the permalink of the namespace
func (ns Namespace) Permalink() string {
	return fmt.Sprintf("%s/%s", ns.NsType, ns.NsUID.String())
}
