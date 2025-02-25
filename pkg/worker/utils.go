package worker

import (
	"strings"

	"github.com/instill-ai/artifact-backend/pkg/minio"
)

func checkIfUploadedByBlobURL(destination string) string {
	if strings.Contains(destination, "uploaded-file") {
		return minio.KnowledgeBaseBucketName
	} else {
		return minio.BlobBucketName
	}
}
