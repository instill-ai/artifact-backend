package usage

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/utils"
	"github.com/instill-ai/x/log"

	mgmtpb "github.com/instill-ai/protogen-go/core/mgmt/v1beta"
	usagepb "github.com/instill-ai/protogen-go/core/usage/v1beta"
	usageclient "github.com/instill-ai/usage-client/client"
	usagereporter "github.com/instill-ai/usage-client/reporter"
)

// Usage interface
type Usage interface {
	RetrieveArtifactUsageData() any
	StartReporter(ctx context.Context)
	TriggerSingleReporter(ctx context.Context)
}

type usage struct {
	mgmtPrivateServiceClient mgmtpb.MgmtPrivateServiceClient
	redisClient              *redis.Client
	artifactReporter         usagereporter.Reporter
	serviceVersion           string
}

const maxPageSize = 100

// NewUsage initiates a usage instance
func NewUsage(ctx context.Context, mu mgmtpb.MgmtPrivateServiceClient, rc *redis.Client, usc usagepb.UsageServiceClient, serviceVersion string) Usage {
	logger, _ := log.GetZapLogger(ctx)

	var defaultOwnerUID string
	if resp, err := mu.GetUserAdmin(ctx, &mgmtpb.GetUserAdminRequest{UserId: constant.DefaultUserID}); err == nil {
		defaultOwnerUID = resp.GetUser().GetUid()
	} else {
		logger.Error(err.Error())
	}

	artifactReporter, err := usageclient.InitReporter(ctx, usc, usagepb.Session_SERVICE_ARTIFACT, config.Config.Server.Edition, serviceVersion, defaultOwnerUID)
	if err != nil {
		logger.Error(err.Error())
		return nil
	}

	return &usage{
		mgmtPrivateServiceClient: mu,
		redisClient:              rc,
		artifactReporter:         artifactReporter,
		serviceVersion:           serviceVersion,
	}
}

func (u *usage) RetrieveArtifactUsageData() interface{} {

	ctx := context.Background()
	logger, _ := log.GetZapLogger(ctx)

	logger.Debug("Retrieve usage data...")

	pbArtifactUsageData := []*usagepb.ArtifactUsageData_UserUsageData{}

	// Roll over all users and update the metrics with the cached uuid
	userPageToken := ""
	userPageSizeMax := int32(maxPageSize)
	for {
		userResp, err := u.mgmtPrivateServiceClient.ListUsersAdmin(ctx, &mgmtpb.ListUsersAdminRequest{
			PageSize:  &userPageSizeMax,
			PageToken: &userPageToken,
		})
		if err != nil {
			logger.Error(fmt.Sprintf("[mgmt-backend: ListUsersAdmin] %s", err))
			break
		}

		// Roll all artifact resources on a user
		// for _, user := range userResp.GetUsers() {
		//TODO: implement the logic to retrieve the app usage data
		// }

		if userResp.NextPageToken == "" {
			break
		} else {
			userPageToken = userResp.NextPageToken
		}
	}

	// Roll over all orgs and update the metrics with the cached uuid
	orgPageToken := ""
	orgPageSizeMax := int32(maxPageSize)
	for {
		orgResp, err := u.mgmtPrivateServiceClient.ListOrganizationsAdmin(ctx, &mgmtpb.ListOrganizationsAdminRequest{
			PageSize:  &orgPageSizeMax,
			PageToken: &orgPageToken,
		})
		if err != nil {
			logger.Error(fmt.Sprintf("[mgmt-backend: ListOrganizationsAdmin] %s", err))
			break
		}

		// Roll all artifact resources on an org
		// for _, org := range orgResp.GetOrganizations() {
		//TODO: implement the logic to retrieve the app usage data
		// }

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
	if u.artifactReporter == nil {
		return
	}

	logger, _ := log.GetZapLogger(ctx)

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
			err := usageclient.StartReporter(ctx, u.artifactReporter, usagepb.Session_SERVICE_ARTIFACT, config.Config.Server.Edition, u.serviceVersion, defaultOwnerUID, u.RetrieveArtifactUsageData)
			if err != nil {
				logger.Error(fmt.Sprintf("unable to start reporter: %v\n", err))
			}
		}()
	}, "UsageReporter")
}

func (u *usage) TriggerSingleReporter(ctx context.Context) {
	if u.artifactReporter == nil {
		return
	}

	logger, _ := log.GetZapLogger(ctx)

	var defaultOwnerUID string
	if resp, err := u.mgmtPrivateServiceClient.GetUserAdmin(ctx, &mgmtpb.GetUserAdminRequest{UserId: constant.DefaultUserID}); err == nil {
		defaultOwnerUID = resp.GetUser().GetUid()
	} else {
		logger.Error(err.Error())
		return
	}

	err := usageclient.SingleReporter(ctx, u.artifactReporter, usagepb.Session_SERVICE_ARTIFACT, config.Config.Server.Edition, u.serviceVersion, defaultOwnerUID, u.RetrieveArtifactUsageData())
	if err != nil {
		logger.Error(fmt.Sprintf("unable to trigger single reporter: %v\n", err))
	}
}
