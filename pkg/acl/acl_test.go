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
// 	config.Init()
// 	clinet, conn := InitOpenFGAClient(context.TODO(), "127.0.0.1", 18081)
// 	defer conn.Close()
// 	ACLClient := NewACLClient(clinet, clinet, nil)
// 	objectType := "pipeline"
// 	objectUID, _ := uuid.NewV1()
// 	fmt.Println("objectUID", objectUID)
// 	ownerType := "user"
// 	ownerUID, _ := uuid.NewV1()
// 	fmt.Println("ownerUID", ownerUID)
// 	err := ACLClient.SetOwner(context.TODO(), objectType, objectUID, ownerType, ownerUID)
// 	if err != nil {
// 		t.Error(err)
// 	}
// }
