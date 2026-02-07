package service

import (
	"context"

	"github.com/gofrs/uuid"

	"github.com/instill-ai/artifact-backend/pkg/resource"
	"github.com/instill-ai/x/constant"

	errorsx "github.com/instill-ai/x/errors"
	resourcex "github.com/instill-ai/x/resource"
)

// IsTrustedBackendRequest checks if the request comes from a trusted backend service
// (e.g., agent-backend). The "Instill-Backend" header is set by SetupServiceContext
// for internal gRPC calls between backends and is NOT forwarded by the API gateway,
// so it's safe to trust for bypassing FGA checks.
func IsTrustedBackendRequest(ctx context.Context) bool {
	return resourcex.GetRequestSingleHeader(ctx, "Instill-Backend") != ""
}

// CheckNamespacePermission checks if the user has permission to access the namespace.
func (s *service) CheckNamespacePermission(ctx context.Context, ns *resource.Namespace) error {
	// Trusted backend-to-backend calls (e.g., agent-backend autofill workers)
	// bypass FGA since the calling service already verified permissions.
	if IsTrustedBackendRequest(ctx) {
		return nil
	}

	// TODO: optimize ACL model
	if ns.NsType == "organizations" {
		// check if the user is a member of the organization
		granted, err := s.aclClient.CheckPermission(ctx, "organization", ns.NsUID, "member")
		if err != nil {
			return err
		}
		if !granted {
			return errorsx.ErrUnauthenticated
		}
		// check if the user is the owner of the namespace
	} else if ns.NsUID != uuid.FromStringOrNil(resourcex.GetRequestSingleHeader(ctx, constant.HeaderUserUIDKey)) {
		return errorsx.ErrUnauthenticated
	}
	return nil
}
