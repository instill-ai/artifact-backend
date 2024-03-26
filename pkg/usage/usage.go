package usage

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v9"

	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/logger"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	"github.com/instill-ai/x/repo"

	mgmtPB "github.com/instill-ai/protogen-go/core/mgmt/v1beta"
	usagePB "github.com/instill-ai/protogen-go/core/usage/v1beta"
	usageClient "github.com/instill-ai/usage-client/client"
	usageReporter "github.com/instill-ai/usage-client/reporter"
)

// Usage interface
type Usage interface {
	RetrieveArtifactUsageData() interface{}
	StartReporter(ctx context.Context)
	TriggerSingleReporter(ctx context.Context)
}

type usage struct {
	repository               repository.Repository
	mgmtPrivateServiceClient mgmtPB.MgmtPrivateServiceClient
	redisClient              *redis.Client
	artifactReporter         usageReporter.Reporter
	version                  string
}

const maxPageSize = 100

// NewUsage initiates a usage instance
func NewUsage(ctx context.Context, r repository.Repository, mu mgmtPB.MgmtPrivateServiceClient, rc *redis.Client, usc usagePB.UsageServiceClient) Usage {
	logger, _ := logger.GetZapLogger(ctx)

	version, err := repo.ReadReleaseManifest("release-please/manifest.json")
	if err != nil {
		logger.Error(err.Error())
		return nil
	}

	var defaultOwnerUID string
	if resp, err := mu.GetUserAdmin(ctx, &mgmtPB.GetUserAdminRequest{Name: constant.DefaultUserID}); err == nil {
		defaultOwnerUID = resp.GetUser().GetUid()
	} else {
		logger.Error(err.Error())
	}

	artifactReporter, err := usageClient.InitReporter(ctx, usc, usagePB.Session_SERVICE_ARTIFACT, config.Config.Server.Edition, version, defaultOwnerUID)
	if err != nil {
		logger.Error(err.Error())
		return nil
	}

	return &usage{
		repository:               r,
		mgmtPrivateServiceClient: mu,
		redisClient:              rc,
		artifactReporter:         artifactReporter,
		version:                  version,
	}
}

func (u *usage) RetrieveArtifactUsageData() interface{} {

	ctx := context.Background()
	logger, _ := logger.GetZapLogger(ctx)

	logger.Debug("Retrieve usage data...")

	pbArtifactUsageData := []*usagePB.ArtifactUsageData_UserUsageData{}

	// Roll over all users and update the metrics with the cached uuid
	userPageToken := ""
	userPageSizeMax := int32(maxPageSize)
	for {
		userResp, err := u.mgmtPrivateServiceClient.ListUsersAdmin(ctx, &mgmtPB.ListUsersAdminRequest{
			PageSize:  &userPageSizeMax,
			PageToken: &userPageToken,
		})
		if err != nil {
			logger.Error(fmt.Sprintf("[mgmt-backend: ListUsersAdmin] %s", err))
			break
		}

		// Roll all artifact resources on a user
		// for _, user := range userResp.GetUsers() {
		//TODO: implement the logic to retrieve the artifact usage data
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
		orgResp, err := u.mgmtPrivateServiceClient.ListOrganizationsAdmin(ctx, &mgmtPB.ListOrganizationsAdminRequest{
			PageSize:  &orgPageSizeMax,
			PageToken: &orgPageToken,
		})
		if err != nil {
			logger.Error(fmt.Sprintf("[mgmt-backend: ListOrganizationsAdmin] %s", err))
			break
		}

		// Roll all artifact resources on an org
		// for _, org := range orgResp.GetOrganizations() {
		//TODO: implement the logic to retrieve the artifact usage data
		// }

		if orgResp.NextPageToken == "" {
			break
		} else {
			orgPageToken = orgResp.NextPageToken
		}
	}

	logger.Debug("Send retrieved usage data...")

	return &usagePB.SessionReport_ArtifactUsageData{
		ArtifactUsageData: &usagePB.ArtifactUsageData{
			Usages: pbArtifactUsageData,
		},
	}
}

func (u *usage) StartReporter(ctx context.Context) {
	if u.artifactReporter == nil {
		return
	}

	logger, _ := logger.GetZapLogger(ctx)

	var defaultOwnerUID string
	if resp, err := u.mgmtPrivateServiceClient.GetUserAdmin(ctx, &mgmtPB.GetUserAdminRequest{Name: constant.DefaultUserID}); err == nil {
		defaultOwnerUID = resp.GetUser().GetUid()
	} else {
		logger.Error(err.Error())
		return
	}

	go func() {
		time.Sleep(5 * time.Second)
		err := usageClient.StartReporter(ctx, u.artifactReporter, usagePB.Session_SERVICE_ARTIFACT, config.Config.Server.Edition, u.version, defaultOwnerUID, u.RetrieveArtifactUsageData)
		if err != nil {
			logger.Error(fmt.Sprintf("unable to start reporter: %v\n", err))
		}
	}()
}

func (u *usage) TriggerSingleReporter(ctx context.Context) {
	if u.artifactReporter == nil {
		return
	}

	logger, _ := logger.GetZapLogger(ctx)

	var defaultOwnerUID string
	if resp, err := u.mgmtPrivateServiceClient.GetUserAdmin(ctx, &mgmtPB.GetUserAdminRequest{Name: constant.DefaultUserID}); err == nil {
		defaultOwnerUID = resp.GetUser().GetUid()
	} else {
		logger.Error(err.Error())
		return
	}

	err := usageClient.SingleReporter(ctx, u.artifactReporter, usagePB.Session_SERVICE_ARTIFACT, config.Config.Server.Edition, u.version, defaultOwnerUID, u.RetrieveArtifactUsageData())
	if err != nil {
		logger.Error(fmt.Sprintf("unable to trigger single reporter: %v\n", err))
	}
}
