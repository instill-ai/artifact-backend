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

// CheckNamespacePermission validates that the caller can access resources
// within the given namespace. This is the namespace-level gate; per-resource
// access is further controlled by FGA CheckPermission.
//
// Access methods (in priority order):
//
//  1. Trusted backend (Instill-Backend header) — service-to-service calls.
//     The calling service already verified permissions at its own level.
//
//  2. Visitor (Instill-Auth-Type: visitor) — anonymous users accessing
//     public resources via capability token. Visitors don't belong to any
//     namespace; their access is gated entirely by per-resource FGA tuples
//     (visitor:* reader). The capability token was validated by the API
//     gateway before these headers were set.
//
//  3. User (Instill-Auth-Type: user) — authenticated users. Must be an org
//     member (for organizations) or the namespace owner (for personal
//     namespaces).
//
// Note: authenticated users accessing OTHER namespaces via capability tokens
// (cross-workspace share links) are handled upstream by the share-link /
// capability-context flow. artifact-backend receives those requests as
// trusted S2S calls (case 1).
func (s *service) CheckNamespacePermission(ctx context.Context, ns *resource.Namespace) error {
	if IsTrustedBackendRequest(ctx) {
		return nil
	}

	authType := resourcex.GetRequestSingleHeader(ctx, constant.HeaderAuthTypeKey)
	if authType == "visitor" {
		return nil
	}

	if ns.NsType == "organizations" {
		granted, err := s.aclClient.CheckPermission(ctx, "organization", ns.NsUID, "member")
		if err != nil {
			return err
		}
		if !granted {
			return errorsx.ErrUnauthenticated
		}
	} else if ns.NsUID != uuid.FromStringOrNil(resourcex.GetRequestSingleHeader(ctx, constant.HeaderUserUIDKey)) {
		return errorsx.ErrUnauthenticated
	}
	return nil
}
