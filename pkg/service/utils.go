package service

import (
	"context"

	"github.com/gofrs/uuid"

	"github.com/instill-ai/artifact-backend/pkg/resource"
	"github.com/instill-ai/x/constant"

	resourcex "github.com/instill-ai/x/resource"
)

// CheckNamespacePermission checks if the user has permission to access the namespace.
func (s *service) CheckNamespacePermission(ctx context.Context, ns *resource.Namespace) error {
	// TODO: optimize ACL model
	if ns.NsType == "organizations" {
		// check if the user is a member of the organization
		granted, err := s.aclClient.CheckPermission(ctx, "organization", ns.NsUID, "member")
		if err != nil {
			return err
		}
		if !granted {
			return ErrNoPermission
		}
		// check if the user is the owner of the namespace
	} else if ns.NsUID != uuid.FromStringOrNil(resourcex.GetRequestSingleHeader(ctx, constant.HeaderUserUIDKey)) {
		return ErrNoPermission
	}
	return nil
}

const (
	defaultPageSize = 10
	maxPageSize     = 100
)

func pageInRange(page int32) int {
	if page <= 0 {
		return 0
	}

	return int(page)
}

func pageSizeInRange(pageSize int32) int {
	if pageSize <= 0 {
		return defaultPageSize
	}

	if pageSize > maxPageSize {
		return maxPageSize
	}

	return int(pageSize)
}
