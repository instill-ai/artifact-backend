package handler

import (
	"context"
	"time"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
	"gopkg.in/guregu/null.v4"

	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/artifact-backend/pkg/resource"

	"github.com/instill-ai/x/constant"

	runpb "github.com/instill-ai/protogen-go/common/run/v1alpha"
	resourcex "github.com/instill-ai/x/resource"
)

func (ph *PublicHandler) logCatalogRunStart(ctx context.Context, catalogUID uuid.UUID, runAction repository.RunAction, startTime *time.Time, payload []byte) *repository.CatalogRun {
	runSource := repository.RunSource(runpb.RunSource_RUN_SOURCE_API)
	userAgentValue, ok := runpb.RunSource_value[resource.GetRequestSingleHeader(ctx, constant.HeaderUserAgent)]
	if ok {
		runSource = repository.RunSource(userAgentValue)
	}

	requesterUID, userUID := resourcex.GetRequesterUIDAndUserUID(ctx)

	start := time.Now()
	if startTime != nil {
		start = *startTime
	}

	catalogRun := &repository.CatalogRun{
		CatalogUID:   catalogUID,
		Status:       repository.RunStatus(runpb.RunStatus_RUN_STATUS_PROCESSING),
		Source:       runSource,
		Action:       runAction,
		RunnerUID:    uuid.FromStringOrNil(userUID),
		RequesterUID: uuid.FromStringOrNil(requesterUID),
		Payload:      payload,
		StartedTime:  start,
	}

	created, err := ph.service.Repository.CreateCatalogRun(ctx, catalogRun)
	if err != nil {
		log, _ := logger.GetZapLogger(ctx)
		log.Error("failed to log catalog run start", zap.String("catalogUID", catalogUID.String()), zap.Error(err), zap.String("action", string(runAction)))
	}
	return created
}

func (ph *PublicHandler) logCatalogRunCompleted(ctx context.Context, catalogRunUID uuid.UUID, startTime time.Time) {
	now := time.Now()

	catalogRunUpdates := &repository.CatalogRun{}
	catalogRunUpdates.Status = repository.RunStatus(runpb.RunStatus_RUN_STATUS_COMPLETED)
	catalogRunUpdates.CompletedTime = null.TimeFrom(now)
	catalogRunUpdates.TotalDuration = null.IntFrom(now.Sub(startTime).Milliseconds())

	if err := ph.service.Repository.UpdateCatalogRun(ctx, catalogRunUID, catalogRunUpdates); err != nil {
		log, _ := logger.GetZapLogger(ctx)
		log.Error("failed to log catalog run completed", zap.String("catalogUID", catalogRunUID.String()), zap.Error(err))
	}
}

func (ph *PublicHandler) logCatalogRunError(ctx context.Context, catalogRunUID uuid.UUID, err error, startTime time.Time) {
	now := time.Now()
	catalogRunUpdates := &repository.CatalogRun{
		Status:        repository.RunStatus(runpb.RunStatus_RUN_STATUS_FAILED),
		CompletedTime: null.TimeFrom(now),
		TotalDuration: null.IntFrom(now.Sub(startTime).Milliseconds()),
		Error:         null.StringFrom(err.Error()),
	}

	if err = ph.service.Repository.UpdateCatalogRun(ctx, catalogRunUID, catalogRunUpdates); err != nil {
		log, _ := logger.GetZapLogger(ctx)
		log.Error("failed to log catalog run error", zap.String("catalogUID", catalogRunUID.String()), zap.Error(err))
	}
}
