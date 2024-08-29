package grpcclient

// import (
// 	"context"
// 	"encoding/base64"
// 	"fmt"
// 	"io"
// 	"os"
// 	"testing"
// 	"time"

// 	"github.com/instill-ai/artifact-backend/pkg/service"
// 	pipelinev1beta "github.com/instill-ai/protogen-go/vdp/pipeline/v1beta"
// 	"google.golang.org/grpc/metadata"
// 	"google.golang.org/protobuf/types/known/structpb"
// )

// // curl -X POST 'http://localhost:8080/v1beta/users/admin/pipelines/test-grpc-calling/trigger' \
// // -H 'Content-Type: application/json' \
// // -H 'Authorization: Bearer {{api token}}' \
// // -d '{"inputs": [{"my_test_input": "Please put your value here"}]}'
// func TestPipeline(t *testing.T) {
// 	pipelinePublicGrpcConn, err := NewGRPCConn("localhost:8081", "", "")
// 	if err != nil {
// 		t.Fatalf("failed to create grpc connection: %v", err)
// 	}
// 	defer pipelinePublicGrpcConn.Close()
// 	// Replace "your-user-uid" with the actual UID you want to send
// 	md := metadata.New(map[string]string{"Instill-User-Uid": "admin", "Instill-Auth-Type": "user"})
// 	ctx := metadata.NewOutgoingContext(context.Background(), md)
// 	pipelinePublicServiceClient := pipelinev1beta.NewPipelinePublicServiceClient(pipelinePublicGrpcConn)
// 	req := &pipelinev1beta.TriggerAsyncNamespacePipelineRequest{
// 		NamespaceId: "admin",
// 		PipelineId:  "test-grpc-calling",
// 		Data: []*pipelinev1beta.TriggerData{
// 			{
// 				Variable: &structpb.Struct{
// 					Fields: map[string]*structpb.Value{
// 						"my_test_input": {
// 							Kind: &structpb.Value_StringValue{
// 								StringValue: "qqqq",
// 							},
// 						},
// 					},
// 				},
// 				// Secret: map[string]string{
// 				// 	"key": "value",
// 				// },
// 			},
// 		},
// 	}
// 	res, err := pipelinePublicServiceClient.TriggerAsyncNamespacePipeline(ctx, req)
// 	if err != nil {
// 		t.Fatalf("failed to trigger pipeline: %v", err)
// 	}
// 	t.Logf("pipeline triggered successfully")
// 	fmt.Println(res)
// }

// //	curl -X POST 'https://api.instill.tech/v1beta/organizations/preset/pipelines/indexing-embed/releases/v1.0.0/trigger' \
// //	     -H 'Authorization: Bearer {{api token}}' \
// //	     -H 'accept: application/json' \
// //	     -H 'content-type: application/json' \
// //	     -d '{"inputs":[{"chunk_input":"test"}]}'
// //
// //	curl -X POST 'http://localhost:18081/v1beta/organizations/preset/pipelines/indexing-embed/releases/v1.0.0/trigger' \
// //		 -H 'Instill-User-Uid: {{user's uuid}}'\
// //		 -H 'Instill-Auth-Type: user'\
// //	     -H 'accept: application/json' \
// //	     -H 'content-type: application/json' \
// //	     -d '{"inputs":[{"chunk_input":"test"}]}'
// func TestCEPresetEmbeddingPipelineReleaseRequest(t *testing.T) {
// 	pipelinePublicGrpcConn, err := NewGRPCConn("localhost:8081", "", "")
// 	if err != nil {
// 		t.Fatalf("failed to create grpc connection: %v", err)
// 	}
// 	defer pipelinePublicGrpcConn.Close()
// 	// Replace "your-user-uid" with the actual UID you want to send
// 	md := metadata.New(map[string]string{"Instill-User-Uid": "admin", "Instill-Auth-Type": "user"})
// 	ctx := metadata.NewOutgoingContext(context.Background(), md)
// 	pipelinePublicServiceClient := pipelinev1beta.NewPipelinePublicServiceClient(pipelinePublicGrpcConn)
// 	req := &pipelinev1beta.TriggerNamespacePipelineReleaseRequest{
// 		NamespaceId: service.NamespaceID,
// 		PipelineId:  service.TextEmbedPipelineID,
// 		ReleaseId:   service.TextEmbedVersion,
// 		Inputs: []*structpb.Struct{
// 			{Fields: map[string]*structpb.Value{"chunk_input": {Kind: &structpb.Value_StringValue{StringValue: "test"}}}},
// 			{Fields: map[string]*structpb.Value{"chunk_input": {Kind: &structpb.Value_StringValue{StringValue: "test"}}}},
// 			{Fields: map[string]*structpb.Value{"chunk_input": {Kind: &structpb.Value_StringValue{StringValue: "test"}}}},
// 			{Fields: map[string]*structpb.Value{"chunk_input": {Kind: &structpb.Value_StringValue{StringValue: "test"}}}},
// 			// {Fields: map[string]*structpb.Value{"chunk_input": {Kind: &structpb.Value_StringValue{StringValue: "test"}}}},
// 			// {Fields: map[string]*structpb.Value{"chunk_input": {Kind: &structpb.Value_StringValue{StringValue: "test"}}}},
// 		},
// 	}
// 	// print the time of calling pipeline
// 	startTime := time.Now()
// 	res, err := pipelinePublicServiceClient.TriggerNamespacePipelineRelease(ctx, req)
// 	if err != nil {
// 		t.Fatalf("failed to trigger pipeline: %v", err)
// 	}
// 	elapsedTime := time.Since(startTime)
// 	fmt.Println("elapsed time:", elapsedTime)
// 	vector, err := service.GetVectorsFromResponse(res)
// 	if err != nil {
// 		t.Fatalf("failed to trigger pipeline: %v", err)
// 	}
// 	t.Logf("pipeline triggered successfully")
// 	fmt.Println("length of vector", len(vector[0]))
// 	// first 10 elements of the vector
// 	fmt.Println("vector", vector[0][:10])
// }

// func TestCEPresetConvertPDF2MdPipeReleaseRequest(t *testing.T) {
// 	testFile := "../../../test.xlsx"
// 	resultFile := "../../../test_converted_xlsx_.md"
// 	// print the current folder
// 	dir, err := os.Getwd()
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	fmt.Println("current working director:", dir)
// 	pipelinePublicGrpcConn, err := NewGRPCConn("localhost:8081", "", "")
// 	if err != nil {
// 		t.Fatalf("failed to create grpc connection: %v", err)
// 	}
// 	defer pipelinePublicGrpcConn.Close()
// 	// Replace "your-user-uid" with the actual UID you want to send
// 	md := metadata.New(map[string]string{"Instill-User-Uid": "admin", "Instill-Auth-Type": "user"})
// 	ctx := metadata.NewOutgoingContext(context.Background(), md)
// 	pipelinePublicServiceClient := pipelinev1beta.NewPipelinePublicServiceClient(pipelinePublicGrpcConn)

// 	base64PDF, err := readFileToBase64(testFile)
// 	if err != nil {
// 		t.Fatalf("failed to read pdf file: %v", err)
// 	}
// 	req := &pipelinev1beta.TriggerNamespacePipelineReleaseRequest{
// 		NamespaceId: service.NamespaceID,
// 		PipelineId:  service.ConvertPDFToMDPipelineID,
// 		ReleaseId:   service.PDFToMDVersion,
// 		Inputs:      []*structpb.Struct{{Fields: map[string]*structpb.Value{"document_input": {Kind: &structpb.Value_StringValue{StringValue: base64PDF}}}}},
// 	}
// 	res, err := pipelinePublicServiceClient.TriggerNamespacePipelineRelease(ctx, req)
// 	if err != nil {
// 		t.Fatalf("failed to trigger pipeline: %v", err)
// 	}
// 	t.Logf("pipeline triggered successfully")
// 	// check if the string is length of 100
// 	if len(res.Outputs[0].GetFields()["convert_result"].GetStringValue()) >= 100 {
// 		fmt.Println("convert result\n", res.Outputs[0].GetFields()["convert_result"].GetStringValue()[:100])
// 	} else {
// 		// print the whole string
// 		fmt.Println("convert result\n", res.Outputs[0].GetFields()["convert_result"].GetStringValue())
// 	}
// 	// store the result to a file
// 	err = os.WriteFile(resultFile, []byte(res.Outputs[0].GetFields()["convert_result"].GetStringValue()), 0644)
// 	if err != nil {
// 		t.Fatalf("failed to write file: %v", err)
// 	}

// }

// // readFileToBase64 read the pdf file and convert it to base64
// func readFileToBase64(path string) (string, error) {
// 	// Open the file
// 	file, err := os.Open(path)
// 	if err != nil {
// 		fmt.Println("failed to open file: ", err)
// 		return "", err
// 	}
// 	defer file.Close()

// 	// Read the file
// 	data, err := io.ReadAll(file)
// 	if err != nil {
// 		fmt.Println("failed to read file: ", err)
// 		return "", err
// 	}

// 	// Encode as base64
// 	return base64.StdEncoding.EncodeToString(data), nil
// }

// // test markdown split pipeline on ce
// func TestCEPresetMarkdownSplitPipeReleaseRequest(t *testing.T) {
// 	// print the current folder
// 	dir, err := os.Getwd()
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	fmt.Println("current working director:", dir)
// 	pipelinePublicGrpcConn, err := NewGRPCConn("localhost:8081", "", "")
// 	if err != nil {
// 		t.Fatalf("failed to create grpc connection: %v", err)
// 	}
// 	defer pipelinePublicGrpcConn.Close()
// 	// Replace "your-user-uid" with the actual UID you want to send
// 	md := metadata.New(map[string]string{"Instill-User-Uid": "admin", "Instill-Auth-Type": "user"})
// 	ctx := metadata.NewOutgoingContext(context.Background(), md)
// 	pipelinePublicServiceClient := pipelinev1beta.NewPipelinePublicServiceClient(pipelinePublicGrpcConn)

// 	mdString, err := readMdToString("../../../test_files/test_converted_pdf_.md")
// 	if err != nil {
// 		t.Fatalf("failed to read pdf file: %v", err)
// 	}
// 	req := &pipelinev1beta.TriggerNamespacePipelineReleaseRequest{
// 		NamespaceId: service.NamespaceID,
// 		PipelineId:  service.MdSplitPipelineID,
// 		ReleaseId:   service.MdSplitVersion,
// 		Inputs: []*structpb.Struct{
// 			{
// 				Fields: map[string]*structpb.Value{
// 					"md_input":         {Kind: &structpb.Value_StringValue{StringValue: mdString}},
// 					"max_chunk_length": {Kind: &structpb.Value_NumberValue{NumberValue: 800}},
// 					"chunk_overlap":    {Kind: &structpb.Value_NumberValue{NumberValue: 200}},
// 				},
// 			},
// 		},
// 	}
// 	res, err := pipelinePublicServiceClient.TriggerNamespacePipelineRelease(ctx, req)
// 	if err != nil {
// 		t.Fatalf("failed to trigger pipeline: %v", err)
// 	}
// 	t.Logf("pipeline triggered successfully")
// 	for i, v := range res.Outputs[0].Fields["split_result"].GetListValue().GetValues() {
// 		fmt.Println("chunk", i)
// 		endPos := v.GetStructValue().Fields["end-position"].GetNumberValue()
// 		startPos := v.GetStructValue().Fields["start-position"].GetNumberValue()
// 		text := v.GetStructValue().Fields["text"].GetStringValue()
// 		fmt.Println("endPos", endPos)
// 		fmt.Println("startPos", startPos)
// 		fmt.Println("text", text[:10])
// 	}
// }

// // readMdToString read the markdown file and convert it to string
// func readMdToString(path string) (string, error) {
// 	// Open the file
// 	file, err := os.Open(path)
// 	if err != nil {
// 		fmt.Println("failed to open file: ", err)
// 		return "", err
// 	}
// 	defer file.Close()

// 	// Read the file
// 	data, err := io.ReadAll(file)
// 	if err != nil {
// 		fmt.Println("failed to read file: ", err)
// 		return "", err
// 	}

// 	return string(data), nil
// }

// // test Text split pipeline on ce
// func TestCEPresetTextSplitPipeReleaseRequest(t *testing.T) {
// 	// print the current folder
// 	dir, err := os.Getwd()
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	fmt.Println("current working director:", dir)
// 	pipelinePublicGrpcConn, err := NewGRPCConn("localhost:8081", "", "")
// 	if err != nil {
// 		t.Fatalf("failed to create grpc connection: %v", err)
// 	}
// 	defer pipelinePublicGrpcConn.Close()
// 	// Replace "your-user-uid" with the actual UID you want to send
// 	md := metadata.New(map[string]string{"Instill-User-Uid": "admin", "Instill-Auth-Type": "user"})
// 	ctx := metadata.NewOutgoingContext(context.Background(), md)
// 	pipelinePublicServiceClient := pipelinev1beta.NewPipelinePublicServiceClient(pipelinePublicGrpcConn)

// 	mdString, err := readMdToString("../../../test_.md")
// 	if err != nil {
// 		t.Fatalf("failed to read pdf file: %v", err)
// 	}
// 	req := &pipelinev1beta.TriggerNamespacePipelineReleaseRequest{
// 		NamespaceId: service.NamespaceID,
// 		PipelineId:  service.TextSplitPipelineID,
// 		ReleaseId:   service.TextSplitVersion,
// 		Inputs: []*structpb.Struct{
// 			{
// 				Fields: map[string]*structpb.Value{
// 					"text_input":    {Kind: &structpb.Value_StringValue{StringValue: mdString}},
// 					"chunk_length":  {Kind: &structpb.Value_NumberValue{NumberValue: 200}},
// 					"chunk_overlap": {Kind: &structpb.Value_NumberValue{NumberValue: 50}},
// 				},
// 			},
// 		},
// 	}
// 	res, err := pipelinePublicServiceClient.TriggerNamespacePipelineRelease(ctx, req)
// 	if err != nil {
// 		t.Fatalf("failed to trigger pipeline: %v", err)
// 	}
// 	t.Logf("pipeline triggered successfully")
// 	for i, v := range res.Outputs[0].Fields["split_result"].GetListValue().GetValues() {
// 		fmt.Println("chunk", i)
// 		endPos := v.GetStructValue().Fields["end-position"].GetNumberValue()
// 		startPos := v.GetStructValue().Fields["start-position"].GetNumberValue()
// 		text := v.GetStructValue().Fields["text"].GetStringValue()
// 		fmt.Println("endPos", endPos)
// 		fmt.Println("startPos", startPos)
// 		fmt.Println("text", text[:10])
// 	}
// }

// // test question answering pipeline on ce
// func TestCEPresetQAReleaseRequest(t *testing.T) {
// 	// print the current folder
// 	dir, err := os.Getwd()
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	fmt.Println("current working director:", dir)
// 	pipelinePublicGrpcConn, err := NewGRPCConn("localhost:8081", "", "")
// 	if err != nil {
// 		t.Fatalf("failed to create grpc connection: %v", err)
// 	}
// 	defer pipelinePublicGrpcConn.Close()
// 	// Replace "your-user-uid" with the actual UID you want to send
// 	md := metadata.New(map[string]string{
// 		"Instill-User-Uid":      "admin",
// 		"Instill-Auth-Type":     "user",
// 		"Instill-Requester-Uid": "",
// 	})
// 	ctx := metadata.NewOutgoingContext(context.Background(), md)
// 	pipelinePublicServiceClient := pipelinev1beta.NewPipelinePublicServiceClient(pipelinePublicGrpcConn)

// 	req := &pipelinev1beta.TriggerNamespacePipelineReleaseRequest{
// 		NamespaceId: service.NamespaceID,
// 		PipelineId:  service.RetrievingQnA,
// 		ReleaseId:   service.QAVersion,
// 		Inputs: []*structpb.Struct{
// 			{
// 				Fields: map[string]*structpb.Value{
// 					"retrieved_chunk": {Kind: &structpb.Value_StringValue{StringValue: "file's name is ABC"}},
// 					"user_question":   {Kind: &structpb.Value_StringValue{StringValue: "What is the name of the file?"}},
// 				},
// 			},
// 		},
// 	}
// 	res, err := pipelinePublicServiceClient.TriggerNamespacePipelineRelease(ctx, req)
// 	if err != nil {
// 		t.Fatalf("failed to trigger pipeline: %v", err)
// 	}
// 	t.Logf("pipeline triggered successfully")
// 	fmt.Println("QA result\n", res.Outputs[0].GetFields()["assistant_reply"].GetStringValue())
// }
