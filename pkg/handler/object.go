package handler

import (
	"context"
	"fmt"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
	errorsx "github.com/instill-ai/x/errors"
	logx "github.com/instill-ai/x/log"
)

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

	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		logger.Error(
			"failed to get namespace ",
			zap.Error(err),
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
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
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
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

	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		logger.Error(
			"failed to get namespace ",
			zap.Error(err),
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
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
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
			zap.String("auth_uid", authUID))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to check namespace permission: %w", err),
			"You don't have permission to download from this namespace. Please contact the owner for access.",
		)
	}

	// Call the service to get the download URL
	response, err := ph.service.GetDownloadURL(ctx, req, ns.NsUID, ns.NsID)
	if err != nil {
		logger.Error("failed to get download URL", zap.Error(err))
		return nil, errorsx.AddMessage(
			fmt.Errorf("failed to get download URL: %w", err),
			"Unable to generate download URL. Please try again.",
		)
	}

	return response, nil
}
