package worker

import (
	"context"
	"fmt"
	"testing"

	grpcclient "github.com/instill-ai/artifact-backend/pkg/client/grpc"
	pipelinev1beta "github.com/instill-ai/protogen-go/vdp/pipeline/v1beta"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"
)

// curl -X POST 'http://localhost:8080/v1beta/users/admin/pipelines/test-grpc-calling/trigger' \
// --header 'Content-Type: application/json' \
// --header 'Authorization: Bearer {{api token}}' \
//
//	--data '{
//	        "inputs": [
//	                {
//	                        "my-test-input": "Please put your value here"
//	                }
//	        ]
//	}'
func TestLocalPipeline(t *testing.T) {
	pipelinePublicGrpcConn, err := grpcclient.NewGRPCConn("localhost:8081", "", "")
	if err != nil {
		t.Fatalf("failed to create grpc connection: %v", err)
	}
	defer pipelinePublicGrpcConn.Close()
	// Replace "your-user-uid" with the actual UID you want to send
	// admin is special user uid. need the real user uid(uuid format)
	md := metadata.New(map[string]string{"Instill-User-Uid": "admin", "Instill-Auth-Type": "user"})
	ctx := metadata.NewOutgoingContext(context.Background(), md)
	pipelinePublicServiceClient := pipelinev1beta.NewPipelinePublicServiceClient(pipelinePublicGrpcConn)
	req := &pipelinev1beta.TriggerUserPipelineRequest{
		Name: "users/admin/pipelines/test-grpc-calling",
		Data: []*pipelinev1beta.TriggerData{
			{
				Variable: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"my-test-input": {
							Kind: &structpb.Value_StringValue{
								StringValue: "qqqq",
							},
						},
					},
				},
				// Secret: map[string]string{
				// 	"key": "value",
				// },
			},
		},
	}
	res, err := pipelinePublicServiceClient.TriggerUserPipeline(ctx, req)
	if err != nil {
		t.Fatalf("failed to trigger pipeline: %v", err)
	}
	t.Logf("pipeline triggered successfully")
	fmt.Println(res)
}

// test cloud pipeline
