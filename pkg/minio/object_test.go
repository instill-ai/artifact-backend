package minio

// import (
// 	"context"
// 	"fmt"
// 	"io"
// 	"log"
// 	"net/http"
// 	"strings"
// 	"testing"
// 	"time"

// 	"github.com/gofrs/uuid"
// 	"github.com/instill-ai/artifact-backend/config"
// 	"github.com/minio/minio-go"
// 	"github.com/minio/minio-go/pkg/encrypt"
// )

// // Test presigned URL for upload
// func TestMinio_TestMakePresignedURLForUpload(t *testing.T) {
// 	log.Println("Setting up Minio client for testing")
// 	var err error
// 	testMinioClient, err := NewMinioClientAndInitBucket(config.MinioConfig{
// 		Host:     "localhost",
// 		Port:     "19000",
// 		RootUser: "minioadmin",
// 		RootPwd:  "minioadmin",
// 	})
// 	if err != nil {
// 		log.Fatalf("Failed to initialize Minio client for testing: %v", err)
// 	}
// 	// create a namespaceUUID and objectUUID
// 	namespaceUUID, err := uuid.NewV4()
// 	if err != nil {
// 		t.Fatalf("failed to create namespaceUUID: %v", err)
// 	}
// 	objectUUID, err := uuid.NewV4()
// 	if err != nil {
// 		t.Fatalf("failed to create objectUUID: %v", err)
// 	}
// 	presignedURL, err := testMinioClient.MakePresignedURLForUpload(context.TODO(), namespaceUUID, objectUUID, 1*time.Hour)
// 	if err != nil {
// 		t.Fatalf("failed to make presigned URL for upload: %v", err)
// 	}
// 	// just get path and query from presignedURL
// 	// parsedURL, err := url.Parse(presignedURL)
// 	// if err != nil {
// 	// 	t.Fatalf("failed to parse presigned URL: %v", err)
// 	// }
// 	// fmt.Println("presignedURL: ", parsedURL.Path, parsedURL.Query())

// 	// upload a test file with content "test" to the presigned URL using http client
// 	client := &http.Client{}
// 	req, err := http.NewRequest("PUT", presignedURL.String(), strings.NewReader("test"))
// 	if err != nil {
// 		t.Fatalf("failed to create request: %v", err)
// 	}
// 	req.Header.Set("Content-Type", "text/plain")
// 	resp, err := client.Do(req)
// 	if err != nil {
// 		t.Fatalf("failed to upload file: %v", err)
// 	}
// 	fmt.Println("resp code: ", resp.StatusCode)
// 	// resp range in header
// 	fmt.Println("resp range: ", resp.Header.Get("Content-Range"))
// 	// read the body 100 bytes at a time until EOF
// 	buf := make([]byte, 100)
// 	for {
// 		n, err := resp.Body.Read(buf)
// 		if err != nil && err != io.EOF {
// 			t.Fatalf("failed to read response body: %v", err)
// 		}
// 		fmt.Println("read: ", string(buf[:n]))
// 		if err == io.EOF {
// 			break
// 		}
// 	}

// 	// check if the file is uploaded to the Minio bucket
// 	objectInfo, err := testMinioClient.client.StatObject(BlobBucketName, GetBlobObjectPath(namespaceUUID, objectUUID), minio.StatObjectOptions{})
// 	if err != nil {
// 		t.Fatalf("failed to stat object: %v", err)
// 	}
// 	// list objectsInfo field by field
// 	// print name of object
// 	fmt.Println("ObjectInfo:")
// 	fmt.Printf("  Key: %v\n", objectInfo.Key)
// 	fmt.Printf("  Size: %v\n", objectInfo.Size)
// 	fmt.Printf("  ContentType: %v\n", objectInfo.ContentType)
// 	// fmt.Printf("Metadata: %v\n", objectInfo.Metadata)
// 	// fmt.Printf("Owner: %v\n", objectInfo.Owner)
// 	// fmt.Printf("StorageClass: %v\n", objectInfo.StorageClass)

// 	// check if the content is "test"
// 	data, err := testMinioClient.GetFile(context.TODO(), BlobBucketName, GetBlobObjectPath(namespaceUUID, objectUUID))
// 	if err != nil {
// 		t.Fatalf("failed to get file: %v", err)
// 	}
// 	fmt.Println("data: ", string(data))

// 	// delete the test file from the Minio bucket
// 	err = testMinioClient.client.RemoveObject(BlobBucketName, GetBlobObjectPath(namespaceUUID, objectUUID))
// 	if err != nil {
// 		t.Fatalf("failed to delete object: %v", err)
// 	}
// }

// // TestMinio_TestMakePresignedURLForDownload
// func TestMinio_TestMakePresignedURLForDownload(t *testing.T) {
// }

// // TestMinio_TestMultiPartUpload
// func TestMinio_TestMultiPartUpload(t *testing.T) {
// 	log.Println("Setting up Minio client for testing")
// 	var err error
// 	testMinioClient, err := NewMinioClientAndInitBucket(config.MinioConfig{
// 		Host:     "localhost",
// 		Port:     "19000",
// 		RootUser: "minioadmin",
// 		RootPwd:  "minioadmin",
// 	})
// 	if err != nil {
// 		log.Fatalf("Failed to initialize Minio client for testing: %v", err)
// 	}
// 	client := testMinioClient.GetClient()
// 	namespaceUUID, err := uuid.NewV4()
// 	if err != nil {
// 		t.Fatalf("failed to create namespaceUUID: %v", err)
// 	}
// 	objectUUID, err := uuid.NewV4()
// 	if err != nil {
// 		t.Fatalf("failed to create objectUUID: %v", err)
// 	}
// 	dest, err := minio.NewDestinationInfo(BlobBucketName, GetBlobObjectPath(namespaceUUID, objectUUID), encrypt.NewSSE(), nil)
// 	if err != nil {
// 		t.Fatalf("failed to create destination info: %v", err)
// 	}
// 	sources := []minio.SourceInfo{
// 		minio.NewSourceInfo(BlobBucketName, GetBlobObjectPath(namespaceUUID, objectUUID), nil),
// 	}
// 	client.ComposeObjectWithProgress(dest, sources, nil)

// }
