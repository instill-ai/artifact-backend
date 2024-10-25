package service

import (
	"context"

	"github.com/gofrs/uuid"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/resource"
)

// CheckNamespacePermission checks if the user has permission to access the namespace.
func (s *Service) CheckNamespacePermission(ctx context.Context, ns *resource.Namespace) error {
	// TODO: optimize ACL model
	if ns.NsType == "organizations" {
		// check if the user is a member of the organization
		granted, err := s.ACLClient.CheckPermission(ctx, "organization", ns.NsUID, "member")
		if err != nil {
			return err
		}
		if !granted {
			return ErrNoPermission
		}
		// check if the user is the owner of the namespace
	} else if ns.NsUID != uuid.FromStringOrNil(resource.GetRequestSingleHeader(ctx, constant.HeaderUserUIDKey)) {
		return ErrNoPermission
	}
	return nil
}
