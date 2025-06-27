package handler

import (
	"context"
	"fmt"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/pkg/customerror"
	"github.com/instill-ai/x/log"

	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// GetObjectUploadURL returns the upload URL for an object.
func (ph *PublicHandler) GetObjectUploadURL(ctx context.Context, req *artifactpb.GetObjectUploadURLRequest) (*artifactpb.GetObjectUploadURLResponse, error) {
	log, _ := log.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		err := fmt.Errorf("failed to get user id from header: %v. err: %w", err, customerror.ErrUnauthenticated)
		return nil, err
	}
	creatorUID, err := uuid.FromString(authUID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse creator uid. err: %w", err)
	}

	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		log.Error(
			"failed to get namespace ",
			zap.Error(err),
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
			zap.String("auth_uid", authUID))
		return nil, fmt.Errorf("failed to get namespace. err: %w", err)
	}
	// ACL - check user's permission to upload object in the namespace
	err = ph.service.CheckNamespacePermission(ctx, ns)
	if err != nil {
		log.Error(
			"failed to check namespace permission",
			zap.Error(err),
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
			zap.String("auth_uid", authUID))
		return nil, fmt.Errorf("failed to check namespace permission. err: %w", err)
	}

	// Call the service to get the upload URL
	response, err := ph.service.GetUploadURL(ctx, req, ns.NsUID, ns.NsID, creatorUID)
	if err != nil {
		log.Error("failed to get upload URL", zap.Error(err))
		return nil, fmt.Errorf("failed to get upload URL. err: %w", err)
	}

	return response, nil
}

// GetObjectDownloadURL returns the download URL for an object.
func (ph *PublicHandler) GetObjectDownloadURL(ctx context.Context, req *artifactpb.GetObjectDownloadURLRequest) (*artifactpb.GetObjectDownloadURLResponse, error) {
	log, _ := log.GetZapLogger(ctx)
	authUID, err := getUserUIDFromContext(ctx)
	if err != nil {
		err := fmt.Errorf("failed to get user id from header: %v. err: %w", err, customerror.ErrUnauthenticated)
		return nil, err
	}

	ns, err := ph.service.GetNamespaceByNsID(ctx, req.GetNamespaceId())
	if err != nil {
		log.Error(
			"failed to get namespace ",
			zap.Error(err),
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
			zap.String("auth_uid", authUID))
		return nil, fmt.Errorf("failed to get namespace. err: %w", err)
	}

	// ACL - check user's permission to download object from the namespace
	err = ph.service.CheckNamespacePermission(ctx, ns)
	if err != nil {
		log.Error(
			"failed to check namespace permission",
			zap.Error(err),
			zap.String("owner_id(ns_id)", req.GetNamespaceId()),
			zap.String("auth_uid", authUID))
		return nil, fmt.Errorf("failed to check namespace permission. err: %w", err)
	}

	// Call the service to get the download URL
	response, err := ph.service.GetDownloadURL(ctx, req, ns.NsUID, ns.NsID)
	if err != nil {
		log.Error("failed to get download URL", zap.Error(err))
		return nil, fmt.Errorf("failed to get download URL. err: %w", err)
	}

	return response, nil
}
