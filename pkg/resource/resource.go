package resource

import (
	"fmt"

	"github.com/gofrs/uuid"
)

type NamespaceType string

const (
	User         NamespaceType = "users"
	Organization NamespaceType = "organizations"
)

type Namespace struct {
	NsType NamespaceType
	NsID   string
	NsUID  uuid.UUID
}

func (ns Namespace) Name() string {
	return fmt.Sprintf("%s/%s", ns.NsType, ns.NsID)
}
func (ns Namespace) Permalink() string {
	return fmt.Sprintf("%s/%s", ns.NsType, ns.NsUID.String())
}
