package acl

// import (
// 	"context"
// 	"fmt"
// 	"os"
// 	"testing"

// 	"github.com/gofrs/uuid"
// 	"github.com/instill-ai/artifact-backend/config"
// )

// func TestOpenFGA_connection(t *testing.T) {
// 	// init
// 	os.Args = []string{"", "-file", "../../config/config_local.yaml"}
// 	err := config.Init()
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	clinet, conn := InitOpenFGAClient(context.TODO(), "127.0.0.1", 9999)
// 	defer conn.Close()
// 	ACLClient := NewACLClient(clinet, clinet, nil)
// 	objectType := "knowledgebase"
// 	objectUID := uuid.FromStringOrNil("b6fa1e84-3b45-4bf0-9d8d-32d814cdfe23")
// 	fmt.Println("objectUID", objectUID)
// 	ownerType := "organizations"
// 	ownerUID, err := uuid.FromString("480bc04e-ca4b-4160-bce9-f1074494c8f4")
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	fmt.Println("ownerUID", ownerUID)
// 	err = ACLClient.SetOwner(context.TODO(), objectType, objectUID, ownerType, ownerUID)
// 	if err != nil {
// 		t.Error(err)
// 	}
// }
