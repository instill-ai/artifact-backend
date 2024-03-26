package httpclient

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/instill-ai/artifact-backend/config"
	"github.com/instill-ai/artifact-backend/pkg/logger"
)

const (
	reqTimeout    = time.Second * 30
	maxRetryCount = 3
	retryDelay    = 100 * time.Millisecond
)

// RegistryClient interacts with the Docker Registry HTTP V2 API.
type RegistryClient struct {
	*resty.Client
}

// NewRegistryClient returns an initialized registry HTTP client.
func NewRegistryClient(ctx context.Context) *RegistryClient {
	l, _ := logger.GetZapLogger(ctx)
	baseURL := fmt.Sprintf("http://%s:%d", config.Config.Registry.Host, config.Config.Registry.Port)

	r := resty.New().
		SetLogger(l.Sugar()).
		SetBaseURL(baseURL).
		SetTimeout(reqTimeout).
		SetTransport(&http.Transport{
			DisableKeepAlives: true,
		}).
		SetRetryCount(maxRetryCount).
		SetRetryWaitTime(retryDelay)

	return &RegistryClient{Client: r}
}

type tagList struct {
	Tags []string `json:"tags"`
}

// ListTags calls the GET /v2/<name>/tags/list endpoint, where <name> is a
// repository.
func (c *RegistryClient) ListTags(ctx context.Context, repository string) ([]string, error) {
	var resp tagList

	tagsPath := fmt.Sprintf("/v2/%s/tags/list", repository)
	r := c.R().SetContext(ctx).SetResult(&resp)
	if _, err := r.Get(tagsPath); err != nil {
		return nil, fmt.Errorf("couldn't connect with registry: %w", err)
	}

	return resp.Tags, nil
}
