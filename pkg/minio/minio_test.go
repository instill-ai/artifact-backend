package minio

// import (
// 	"context"
// 	"fmt"
// 	"mime"
// 	"testing"
// )

// func TestMinio_GetFile(t *testing.T) {
// 	fmt.Println("Starting TestMinio_GetFile")
// 	fmt.Println("testMinioClient.client.ListBuckets()")
// 	bs, err := testMinioClient.client.ListBuckets()
// 	if err != nil {
// 		t.Fatalf("failed to list buckets: %v", err)
// 	}
// 	fmt.Println("buckets: ", bs)
// 	data, err := testMinioClient.GetFile(context.TODO(), "08ee98ec-381e-485e-b784-d765e7ae195d/text.txt")
// 	if err != nil {
// 		t.Fatalf("failed to get file: %v", err)
// 	}
// 	fmt.Println("data: ", string(data))
// }

// func TestMime(t *testing.T) {
// 	fmt.Println("Starting TestMime")
// 	mimeType := mime.TypeByExtension(".markdown")
// 	if mimeType == "" {
// 		mimeType = "application/octet-stream" // Default MIME type
// 	}
// 	fmt.Println("mimeType: ", mimeType)
// }

// func TestDuplicateCreateSameBucket(t *testing.T) {
// 	fmt.Println("Starting TestDuplicateCreateSameBucket")
// 	err := testMinioClient.client.MakeBucket("test-bucket-2", "us-east-1")
// 	if err != nil {
// 		t.Fatalf("failed to create first bucket: %v", err)
// 	}
// 	err = testMinioClient.client.MakeBucket("test-bucket-3", "us-east-1")
// 	if err != nil {
// 		t.Fatalf("failed to create second bucket: %v", err)
// 	}

// }
