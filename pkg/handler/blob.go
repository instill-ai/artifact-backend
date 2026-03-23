package handler

import (
	"net/http"

	"go.uber.org/zap"
)

// ResolveBlobByObjectID resolves a stable object_id to a fresh presigned URL.
// This internal endpoint is called by the API gateway blob plugin when it
// encounters an object_id (rather than a base64-encoded presigned URL) in a
// blob-urls path segment. No ACL checks are performed because this endpoint
// is only exposed on the private server port.
func (h *PrivateHandler) ResolveBlobByObjectID(w http.ResponseWriter, r *http.Request, pathParams map[string]string) {
	objectID := pathParams["object_id"]
	if objectID == "" {
		http.Error(w, "object_id is required", http.StatusBadRequest)
		return
	}

	presignedURL, err := h.service.ResolveBlobPresignedURL(r.Context(), objectID)
	if err != nil {
		h.logger.Error("Failed to resolve blob by object ID",
			zap.Error(err),
			zap.String("object_id", objectID),
		)
		http.Error(w, "object not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(presignedURL))
}
