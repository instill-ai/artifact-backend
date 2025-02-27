package handler

import (
	"encoding/json"
	"io"
	"net/http"

	"go.uber.org/zap"
)

// IngestMinIOAuditLogs receives and logs the MinIO audit logs in order to
// track which actions are performed and by whom on MinIO.
// The server's audit log webhook points to this endpoint so, even if is placed
// under artifact-backend, the logs reflect the actions from any MinIO client.
// In the future, this might be extracted to a dedicated service or a lambda /
// cloud run function.
func (h *PrivateHandler) IngestMinIOAuditLogs(w http.ResponseWriter, r *http.Request, _ map[string]string) {
	body, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		h.log.Error("Failed to read MinIO audit log body", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var auditLog minIOAuditLog
	if err := json.Unmarshal(body, &auditLog); err != nil {
		h.log.Error("Failed to unmarshal MinIO audit log", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	go func() {
		if auditLog.isExcluded() {
			return
		}

		h.log.Info("MinIO audit log", zap.Any("body", auditLog))

	}()

	w.WriteHeader(http.StatusOK)
}

type minIOAuditLog struct {
	Time string `json:"time"`
	API  struct {
		Name   string `json:"name"`
		Bucket string `json:"bucket"`
		Object string `json:"object"`
		Status string `json:"status"`
	} `json:"api"`
	RemoteHost    string `json:"remotehost"`
	AccessKey     string `json:"accessKey"`
	UserAgent     string `json:"userAgent"`
	RequestHeader struct {
		InstillUserUID string `json:"X-Amz-Meta-Instill-User-Uid,omitempty"`
	} `json:"requestHeader,omitempty"`
}

const (
	// We're interested in the audit logs related to user data, handled by the
	// Instill AI services. Milvus also interacts with MinIO, which generates noise
	// when tracking the operations in the buckets that contain user data.
	// TODO: In the future we might want to use different MinIO instances for the
	// *-backend services and for Milvus.
	excludedMinIOBucket = "core-milvus"
	// We use bucket lifecycle rules to expire objects automatically. MinIO
	// will report this action when it expires an object, but it doesn't
	// reflect any action from our services, so we filter these logs out.
	excludedActionILM = "ILMExpiry"
)

func (l minIOAuditLog) isExcluded() bool {
	return l.API.Bucket == excludedMinIOBucket || l.API.Name == excludedActionILM
}
