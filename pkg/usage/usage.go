package usage

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/utils"

	mgmtpb "github.com/instill-ai/protogen-go/core/mgmt/v1beta"
	usagepb "github.com/instill-ai/protogen-go/core/usage/v1beta"
	usageclient "github.com/instill-ai/usage-client/client"
	usagereporter "github.com/instill-ai/usage-client/reporter"
	logx "github.com/instill-ai/x/log"
)

// Usage interface
type Usage interface {
	RetrieveUsageData() any
	StartReporter(ctx context.Context)
	TriggerSingleReporter(ctx context.Context)
}

type usage struct {
	mgmtPrivateServiceClient mgmtpb.MgmtPrivateServiceClient
	redisClient              *redis.Client
	reporter                 usagereporter.Reporter
	serviceVersion           string
}

const maxPageSize = 100

// NewUsage initiates a usage instance
func NewUsage(ctx context.Context, mu mgmtpb.MgmtPrivateServiceClient, rc *redis.Client, usc usagepb.UsageServiceClient, serviceVersion string) Usage {
	logger, _ := logx.GetZapLogger(ctx)

	var defaultOwnerUID string
	if resp, err := mu.GetUserAdmin(ctx, &mgmtpb.GetUserAdminRequest{UserId: constant.DefaultUserID}); err == nil {
		defaultOwnerUID = resp.GetUser().GetUid()
	} else {
		logger.Error("Failed to get default user, usage reporter will not start", zap.Error(err))
		return nil // Return nil instead of continuing with empty defaultOwnerUID
	}

	artifactReporter, err := usageclient.InitReporter(ctx, usc, usagepb.Session_SERVICE_ARTIFACT, config.Config.Server.Edition, serviceVersion, defaultOwnerUID)
	if err != nil {
		logger.Error("Failed to initialize artifact reporter", zap.Error(err))
		return nil
	}

	return &usage{
		mgmtPrivateServiceClient: mu,
		redisClient:              rc,
		reporter:                 artifactReporter,
		serviceVersion:           serviceVersion,
	}
}

func (u *usage) RetrieveUsageData() any {

	ctx := context.Background()
	logger, _ := logx.GetZapLogger(ctx)

	logger.Debug("Retrieve usage data...")

	pbArtifactUsageData := []*usagepb.ArtifactUsageData_UserUsageData{}

	// Roll over all users and update the metrics with the cached uuid
	userPageToken := ""
	pageSizeMax := int32(maxPageSize)
	for {
		userResp, err := u.mgmtPrivateServiceClient.ListUsersAdmin(ctx, &mgmtpb.ListUsersAdminRequest{
			PageSize:  &pageSizeMax,
			PageToken: &userPageToken,
		})
		if err != nil {
			logger.Error(fmt.Sprintf("[mgmt-backend: ListUsersAdmin] %s", err))
			break
		}

		// Roll all artifact resources on a user
		for _, user := range userResp.GetUsers() {
			pbArtifactUsageData = append(pbArtifactUsageData, &usagepb.ArtifactUsageData_UserUsageData{
				OwnerUid:  user.GetUid(),
				OwnerType: mgmtpb.OwnerType_OWNER_TYPE_USER,
			})
		}

		if userResp.NextPageToken == "" {
			break
		} else {
			userPageToken = userResp.NextPageToken
		}
	}

	// Roll over all orgs and update the metrics with the cached uuid
	orgPageToken := ""
	for {
		orgResp, err := u.mgmtPrivateServiceClient.ListOrganizationsAdmin(ctx, &mgmtpb.ListOrganizationsAdminRequest{
			PageSize:  &pageSizeMax,
			PageToken: &orgPageToken,
		})
		if err != nil {
			logger.Error(fmt.Sprintf("[mgmt-backend: ListOrganizationsAdmin] %s", err))
			break
		}

		// Roll all model resources on an org
		for _, org := range orgResp.GetOrganizations() {
			pbArtifactUsageData = append(pbArtifactUsageData, &usagepb.ArtifactUsageData_UserUsageData{
				OwnerUid:  org.GetUid(),
				OwnerType: mgmtpb.OwnerType_OWNER_TYPE_ORGANIZATION,
			})
		}

		if orgResp.NextPageToken == "" {
			break
		} else {
			orgPageToken = orgResp.NextPageToken
		}
	}

	logger.Debug("Send retrieved usage data...")

	return &usagepb.SessionReport_ArtifactUsageData{
		ArtifactUsageData: &usagepb.ArtifactUsageData{
			Usages: pbArtifactUsageData,
		},
	}
}

func (u *usage) StartReporter(ctx context.Context) {
	if u.reporter == nil {
		return
	}

	logger, _ := logx.GetZapLogger(ctx)

	var defaultOwnerUID string
	if resp, err := u.mgmtPrivateServiceClient.GetUserAdmin(ctx, &mgmtpb.GetUserAdminRequest{UserId: constant.DefaultUserID}); err == nil {
		defaultOwnerUID = resp.GetUser().GetUid()
	} else {
		logger.Error(err.Error())
		return
	}
	go utils.GoRecover(func() {
		func() {
			time.Sleep(5 * time.Second)
			err := usageclient.StartReporter(ctx, u.reporter, usagepb.Session_SERVICE_ARTIFACT, config.Config.Server.Edition, u.serviceVersion, defaultOwnerUID, u.RetrieveUsageData)
			if err != nil {
				logger.Error(fmt.Sprintf("unable to start reporter: %v\n", err))
			}
		}()
	}, "UsageReporter")
}

func (u *usage) TriggerSingleReporter(ctx context.Context) {
	if u.reporter == nil {
		return
	}

	logger, _ := logx.GetZapLogger(ctx)

	var defaultOwnerUID string
	if resp, err := u.mgmtPrivateServiceClient.GetUserAdmin(ctx, &mgmtpb.GetUserAdminRequest{UserId: constant.DefaultUserID}); err == nil {
		defaultOwnerUID = resp.GetUser().GetUid()
	} else {
		logger.Error(err.Error())
		return
	}

	err := usageclient.SingleReporter(ctx, u.reporter, usagepb.Session_SERVICE_ARTIFACT, config.Config.Server.Edition, u.serviceVersion, defaultOwnerUID, u.RetrieveUsageData())
	if err != nil {
		logger.Error(fmt.Sprintf("unable to trigger single reporter: %v\n", err))
	}
}
