package minio

// import (
// 	"log"
// 	"os"
// 	"testing"

// 	"github.com/instill-ai/artifact-backend/config"
// )

// var testMinioClient *Minio

// func TestMain(m *testing.M) {
//     // Setup
// 	log.Println("Setting up Minio client for testing")
//     var err error
//     testMinioClient, err = NewMinioClientAndInitBucket(config.MinioConfig{
// 		Host:       "localhost",
// 		Port:       "19000",
// 		RootUser:   "minioadmin",
// 		RootPwd:    "minioadmin",
// 		BucketName: "instill-ai-knowledge-bases",
// 	})
//     if err != nil {
//         log.Fatalf("Failed to initialize Minio client for testing: %v", err)
//     }

//     // Run tests
//     exitCode := m.Run()

//     // Cleanup (if necessary)
//     // For example, you might want to remove test files from the bucket

//     // Exit
//     os.Exit(exitCode)
// }
