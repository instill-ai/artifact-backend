package handler

import (
	"context"
	"fmt"
	"strings"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"

	artifactpb "github.com/instill-ai/protogen-go/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
)

// parseObjectFromName parses a resource name of format "namespaces/{namespace}/objects/{object}"
// and returns the namespace_id and object_id
func parseObjectFromName(name string) (namespaceID, objectID string, err error) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != "namespaces" || parts[2] != "objects" {
		return "", "", fmt.Errorf("invalid object name format, expected namespaces/{namespace}/objects/{object}")
	}
	return parts[1], parts[3], nil
}

// GetObjectUploadURL returns the upload URL for an object.
func (ph *PublicHandler) GetObjectUploadURL(ctx context.Context, req *artifactpb.GetObjectUploadURLRequest) (*artifactpb.GetObjectUploadURLResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get user id from header: %v: %w", err, errorsx.ErrUnauthenticated),
			"Authentication failed. Please log in and try again.",
		)
	}
	creatorUID, err := uuid.FromString(authUID)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to parse creator uid: %w", err),
			"Invalid user session. Please log in again.",
		)
	}

	// Parse namespace ID from parent (format: namespaces/{namespace})
	namespaceID, err := parseNamespaceFromParent(req.GetParent())
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("invalid parent format: %w", err),
			"Invalid parent format. Expected format: namespaces/{namespace}",
		)
	}

	ns, err := ph.service.GetNamespaceByNsID(ctx, namespaceID)
	if err != nil {
		logger.Error(
			"failed to get namespace ",
			zap.Error(err),
			zap.String("owner_id(ns_id)", namespaceID),
			zap.String("auth_uid", authUID))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get namespace: %w", err),
			"Unable to access the specified namespace. Please check the namespace ID and try again.",
		)
	}
	// ACL - check user's permission to upload object in the namespace
	err = ph.service.CheckNamespacePermission(ctx, ns)
	if err != nil {
		logger.Error(
			"failed to check namespace permission",
			zap.Error(err),
			zap.String("owner_id(ns_id)", namespaceID),
			zap.String("auth_uid", authUID))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to check namespace permission: %w", err),
			"You don't have permission to upload to this namespace. Please contact the owner for access.",
		)
	}

	// Call the service to get the upload URL
	response, err := ph.service.GetUploadURL(ctx, req, ns.NsUID, ns.NsID, creatorUID)
	if err != nil {
		logger.Error("failed to get upload URL", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get upload URL: %w", err),
			"Unable to generate upload URL. Please try again.",
		)
	}

	return response, nil
}

// GetObjectDownloadURL returns the download URL for an object.
func (ph *PublicHandler) GetObjectDownloadURL(ctx context.Context, req *artifactpb.GetObjectDownloadURLRequest) (*artifactpb.GetObjectDownloadURLResponse, error) {
	logger, _ := logx.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get user id from header: %v: %w", err, errorsx.ErrUnauthenticated),
			"Authentication failed. Please log in and try again.",
		)
	}

	// Parse the AIP-compliant name field
	namespaceID, objectID, err := parseObjectFromName(req.GetName())
	if err != nil {
		return nil, errorsx.AddMessage(
			fmt.Errorf("invalid name format: %w", err),
			"Invalid object name format. Expected format: namespaces/{namespace}/objects/{object}",
		)
	}

	ns, err := ph.service.GetNamespaceByNsID(ctx, namespaceID)
	if err != nil {
		logger.Error(
			"failed to get namespace ",
			zap.Error(err),
			zap.String("owner_id(ns_id)", namespaceID),
			zap.String("auth_uid", authUID))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get namespace: %w", err),
			"Unable to access the specified namespace. Please check the namespace ID and try again.",
		)
	}

	// ACL - check user's permission to download object from the namespace
	err = ph.service.CheckNamespacePermission(ctx, ns)
	if err != nil {
		logger.Error(
			"failed to check namespace permission",
			zap.Error(err),
			zap.String("owner_id(ns_id)", namespaceID),
			zap.String("auth_uid", authUID))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to check namespace permission: %w", err),
			"You don't have permission to download from this namespace. Please contact the owner for access.",
		)
	}

	// Call the service to get the download URL
	response, err := ph.service.GetDownloadURL(ctx, objectID, ns.NsUID, ns.NsID, req.GetUrlExpireDays(), req.GetDownloadFilename())
	if err != nil {
		logger.Error("failed to get download URL", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get download URL: %w", err),
			"Unable to generate download URL. Please try again.",
		)
	}

	return response, nil
}
