package service

import (
	"context"
	"testing"

	"github.com/gofrs/uuid"
	"google.golang.org/grpc/metadata"

	"github.com/instill-ai/artifact-backend/pkg/resource"
	"github.com/instill-ai/x/constant"
)

func ctxWithHeaders(headers map[string]string) context.Context {
	md := metadata.New(headers)
	return metadata.NewIncomingContext(context.Background(), md)
}

// ============================================================
// IsTrustedBackendRequest
// ============================================================

func TestIsTrustedBackendRequest_WithHeader(t *testing.T) {
	ctx := ctxWithHeaders(map[string]string{
		"Instill-Backend": "agent-backend",
	})
	if !IsTrustedBackendRequest(ctx) {
		t.Error("request with Instill-Backend header should be trusted")
	}
}

func TestIsTrustedBackendRequest_WithoutHeader(t *testing.T) {
	ctx := ctxWithHeaders(map[string]string{})
	if IsTrustedBackendRequest(ctx) {
		t.Error("request without Instill-Backend header should not be trusted")
	}
}

// ============================================================
// CheckNamespacePermission — namespace-level access gate
// ============================================================

func TestCheckNamespacePermission_TrustedBackendBypasses(t *testing.T) {
	svc := &service{}
	ctx := ctxWithHeaders(map[string]string{
		"Instill-Backend": "agent-backend",
	})
	ns := &resource.Namespace{NsType: "organizations", NsUID: uuid.Must(uuid.NewV4())}

	err := svc.CheckNamespacePermission(ctx, ns)
	if err != nil {
		t.Errorf("trusted backend should bypass namespace permission, got: %v", err)
	}
}

func TestCheckNamespacePermission_VisitorBypasses(t *testing.T) {
	svc := &service{}
	ctx := ctxWithHeaders(map[string]string{
		constant.HeaderAuthTypeKey:   "visitor",
		constant.HeaderVisitorUIDKey: "b6e1f5c3-7a2d-4e89-9f01-abc123def456",
	})
	ns := &resource.Namespace{NsType: "organizations", NsUID: uuid.Must(uuid.NewV4())}

	err := svc.CheckNamespacePermission(ctx, ns)
	if err != nil {
		t.Errorf("visitor should bypass namespace permission, got: %v", err)
	}
}

func TestCheckNamespacePermission_UserInOwnPersonalNamespace(t *testing.T) {
	userUID := uuid.Must(uuid.FromString("a1b2c3d4-e5f6-7890-abcd-ef1234567890"))
	svc := &service{}
	ctx := ctxWithHeaders(map[string]string{
		constant.HeaderAuthTypeKey: "user",
		constant.HeaderUserUIDKey:  userUID.String(),
	})
	ns := &resource.Namespace{NsType: "users", NsUID: userUID}

	err := svc.CheckNamespacePermission(ctx, ns)
	if err != nil {
		t.Errorf("user in own namespace should be allowed, got: %v", err)
	}
}

func TestCheckNamespacePermission_UserInOtherPersonalNamespace(t *testing.T) {
	svc := &service{}
	ctx := ctxWithHeaders(map[string]string{
		constant.HeaderAuthTypeKey: "user",
		constant.HeaderUserUIDKey:  "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
	})
	otherUserUID := uuid.Must(uuid.FromString("ffffffff-ffff-ffff-ffff-ffffffffffff"))
	ns := &resource.Namespace{NsType: "users", NsUID: otherUserUID}

	err := svc.CheckNamespacePermission(ctx, ns)
	if err == nil {
		t.Fatal("user should not access another user's personal namespace")
	}
}

func TestCheckNamespacePermission_VisitorAccessesPersonalNamespace(t *testing.T) {
	svc := &service{}
	ctx := ctxWithHeaders(map[string]string{
		constant.HeaderAuthTypeKey:   "visitor",
		constant.HeaderVisitorUIDKey: "b6e1f5c3-7a2d-4e89-9f01-abc123def456",
	})
	ns := &resource.Namespace{NsType: "users", NsUID: uuid.Must(uuid.NewV4())}

	err := svc.CheckNamespacePermission(ctx, ns)
	if err != nil {
		t.Errorf("visitor should bypass even for personal namespaces (FGA is the gate), got: %v", err)
	}
}

func TestCheckNamespacePermission_VisitorAccessesOrgNamespace(t *testing.T) {
	svc := &service{}
	ctx := ctxWithHeaders(map[string]string{
		constant.HeaderAuthTypeKey:   "visitor",
		constant.HeaderVisitorUIDKey: "b6e1f5c3-7a2d-4e89-9f01-abc123def456",
	})
	ns := &resource.Namespace{NsType: "organizations", NsUID: uuid.Must(uuid.NewV4())}

	err := svc.CheckNamespacePermission(ctx, ns)
	if err != nil {
		t.Errorf("visitor should bypass org namespace check (FGA is the gate), got: %v", err)
	}
}
