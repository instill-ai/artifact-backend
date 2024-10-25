package minio

// Note: this test can only be run locally with minio server
// import (
// 	"bytes"
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
// 	presignedURL, err := testMinioClient.GetPresignedURLForUpload(context.TODO(), namespaceUUID, objectUUID, 1*time.Hour)
// 	if err != nil {
// 		t.Fatalf("failed to make presigned URL for upload: %v", err)
// 	}
// 	path := presignedURL.Path
// 	query := presignedURL.RawQuery
// 	fmt.Println("presignedURL: ", presignedURL.String())
// 	fmt.Println("path: ", path)
// 	fmt.Println("query: ", query)
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
// 	// print the resp all headers. with key and value
// 	fmt.Println("resp header: ")
// 	for key, value := range resp.Header {
// 		// value is a slice of string, print the combined string
// 		fmt.Println("  ", key, ": ", strings.Join(value, ","))
// 	}
// 	// read the body 100 bytes at a time until EOF
// 	buf := make([]byte, 100)
// 	for {
// 		n, err := resp.Body.Read(buf)
// 		if err != nil && err != io.EOF {
// 			t.Fatalf("failed to read response body: %v", err)
// 		}
// 		if err == io.EOF {
// 			fmt.Println("EOF already reached")
// 			break
// 		}
// 		fmt.Println("read buf: ", string(buf[:n]))
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
// 	// Set up test environment
// 	log.Println("Setting up Minio client for testing")
// 	testMinioClient, err := NewMinioClientAndInitBucket(config.MinioConfig{
// 		Host:     "localhost",
// 		Port:     "19000",
// 		RootUser: "minioadmin",
// 		RootPwd:  "minioadmin",
// 	})
// 	if err != nil {
// 		t.Fatalf("Failed to initialize Minio client for testing: %v", err)
// 	}

// 	// Create test data
// 	namespaceUUID, err := uuid.NewV4()
// 	if err != nil {
// 		t.Fatalf("Failed to create namespaceUUID: %v", err)
// 	}
// 	objectUUID, err := uuid.NewV4()
// 	if err != nil {
// 		t.Fatalf("Failed to create objectUUID: %v", err)
// 	}
// 	testContent := []byte("test content for download")

// 	// Upload test file to Minio
// 	_, err = testMinioClient.client.PutObject(BlobBucketName, GetBlobObjectPath(namespaceUUID, objectUUID), bytes.NewReader(testContent), int64(len(testContent)), minio.PutObjectOptions{ContentType: "text/plain"})
// 	if err != nil {
// 		t.Fatalf("Failed to upload test object: %v", err)
// 	}

// 	// Test GetPresignedURLForDownload
// 	ctx := context.Background()
// 	presignedURL, err := testMinioClient.GetPresignedURLForDownload(ctx, namespaceUUID, objectUUID, time.Hour)
// 	if err != nil {
// 		t.Fatalf("Failed to get presigned URL for download: %v", err)
// 	}

// 	// Verify the presigned URL
// 	if presignedURL == nil {
// 		t.Fatal("Presigned URL is nil")
// 	}
// 	if !strings.Contains(presignedURL.Path, GetBlobObjectPath(namespaceUUID, objectUUID)) {
// 		t.Errorf("Presigned URL path does not contain expected object path. Got: %s", presignedURL.Path)
// 	}

// 	// Use the presigned URL to download the object
// 	req, err := http.NewRequest("GET", presignedURL.String(), nil)
// 	if err != nil {
// 		t.Fatalf("Failed to create request: %v", err)
// 	}

// 	// Test with range header
// 	req.Header.Set("Range", "bytes=0-9")

// 	client := &http.Client{}
// 	resp, err := client.Do(req)
// 	if err != nil {
// 		t.Fatalf("Failed to download object using presigned URL: %v", err)
// 	}
// 	defer resp.Body.Close()

// 	if resp.StatusCode != http.StatusPartialContent {
// 		t.Errorf("Unexpected status code. Expected 206, got: %d", resp.StatusCode)
// 	}

// 	// print the resp all headers. with key and value
// 	fmt.Println("resp header: ")
// 	for key, value := range resp.Header {
// 		// value is a slice of string, print the combined string
// 		fmt.Println("  ", key, ": ", strings.Join(value, ","))
// 	}

// 	downloadedContent, err := io.ReadAll(resp.Body)
// 	if err != nil {
// 		t.Fatalf("Failed to read downloaded content: %v", err)
// 	}

// 	// print the downloaded content
// 	fmt.Println("downloadedContent: ", string(downloadedContent))

// 	expectedContent := testContent[:10]
// 	if !bytes.Equal(downloadedContent, expectedContent) {
// 		t.Errorf("Downloaded content does not match expected content. Expected: %s, Got: %s", string(expectedContent), string(downloadedContent))
// 	}

// 	// Clean up: delete the test file from the Minio bucket
// 	err = testMinioClient.client.RemoveObject(BlobBucketName, GetBlobObjectPath(namespaceUUID, objectUUID))
// 	if err != nil {
// 		t.Fatalf("Failed to delete test object: %v", err)
// 	}
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
