package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	qt "github.com/frankban/quicktest"
)

const deadlineDuration = 20 * time.Millisecond

func TestPrivateHandler_IngestMinIOAuditLogs(t *testing.T) {
	c := qt.New(t)

	testcases := []struct {
		name    string
		bucket  string
		action  string
		hasLogs bool
	}{
		{
			name:   "ok - filter milvus bucket",
			bucket: "core-milvus",
			action: "HeadObject",
		},
		{
			name:   "ok - filter automatic expiration",
			bucket: "instill-ai-vdp",
			action: "ILMExpiry",
		},
		{
			name:    "ok",
			bucket:  "instill-ai-vdp",
			action:  "HeadObject",
			hasLogs: true,
		},
	}

	for _, tc := range testcases {
		c.Run(tc.name, func(c *qt.C) {
			c.Parallel()

			zCore, zLogs := observer.New(zap.InfoLevel)
			h := NewPrivateHandler(nil, zap.New(zCore))

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				h.IngestMinIOAuditLogs(w, r, nil)
			}))
			c.Cleanup(srv.Close)

			auditLog := minIOAuditLog{
				Time:      time.Now().UTC().String(),
				AccessKey: "minio-user",
				UserAgent: "artifact-backend/dev",
			}
			auditLog.API.Bucket = tc.bucket
			auditLog.API.Name = tc.action
			payload, err := json.Marshal(auditLog)
			c.Assert(err, qt.IsNil)

			resp, err := http.Post(srv.URL, "application/json", bytes.NewReader(payload))
			c.Assert(err, qt.IsNil)
			defer resp.Body.Close()

			c.Assert(resp.StatusCode, qt.Equals, http.StatusOK)

			// Wait for logs to be written asynchronously.
			deadline := time.After(deadlineDuration)

		FOR_LOOP:
			for zLogs.Len() == 0 {
				select {
				case <-deadline:
					break FOR_LOOP
				default:
					time.Sleep(time.Millisecond)
				}
			}

			if !tc.hasLogs {
				c.Check(zLogs.Len(), qt.Equals, 0)
				return
			}

			c.Assert(zLogs.Len(), qt.Equals, 1)

			log := zLogs.All()[0].ContextMap()
			c.Check(log["body"], qt.Equals, auditLog)
		})
	}
}
