package handler

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"google.golang.org/grpc/metadata"

	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

// todo: add error codes in the error message
const ErrorCreateKnowledgeBaseCode = 1000
const ErrorGetKnowledgeBasesCode = 1001
const ErrorUpdateKnowledgeBaseCode = 1002
const ErrorDeleteKnowledgeBaseCode = 1003

type ErrorMsg map[int]string
const ErrorCreateKnowledgeBaseMsg = "failed to create knowledge base: %v"
const ErrorListKnowledgeBasesMsg = "failed to get knowledge bases: %v "
const ErrorUpdateKnowledgeBaseMsg = "failed to update knowledge base: %v"
const ErrorDeleteKnowledgeBaseMsg = "failed to delete knowledge base: %v"

func (ph *PublicHandler) CreateKnowledgeBase(ctx context.Context, req *artifactpb.CreateKnowledgeBaseRequest) (*artifactpb.CreateKnowledgeBaseResponse, error) {

	uid, err := getUserIDFromContext(ctx)
	if err != nil {
		msg := fmt.Sprintf("failed to get user id from header: %v", err)
		return nil, fmt.Errorf(ErrorCreateKnowledgeBaseMsg, msg)
	}
	// check name if it is empty
	if req.Name == "" {
		msg := "name is required"
		return nil, fmt.Errorf(ErrorCreateKnowledgeBaseMsg, msg)
	}
	nameOk := isValidName(req.Name)
	if !nameOk {
		msg := "name is invalid: " + req.Name
		return nil, fmt.Errorf(ErrorCreateKnowledgeBaseMsg, msg)
	}
	res, err := ph.service.Repository.CreateKnowledgeBase(ctx,
		repository.KnowledgeBase{
			Name:        req.Name,
			KbID:        toIDStyle(req.Name),
			Description: req.Description,
			Tags:        req.Tags,
			Owner:       uid,
		},
	)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf(ErrorCreateKnowledgeBaseMsg, err))
	}
	return &artifactpb.CreateKnowledgeBaseResponse{
		Body: &artifactpb.KnowledgeBase{
			Name:        res.Name,
			Id:          res.KbID,
			Description: res.Description,
			Tags:        res.Tags,
			OwnerName:   res.Owner,
			CreateTime:  res.CreateTime.String(),
			UpdateTime:  res.UpdateTime.String(),
		}, ErrorMsg: "", StatusCode: 0,
	}, nil
}
func (ph *PublicHandler) ListKnowledgeBases(ctx context.Context, _ *artifactpb.ListKnowledgeBasesRequest) (*artifactpb.ListKnowledgeBasesResponse, error) {

	// get user id from context
	uid, err := getUserIDFromContext(ctx)
	if err != nil {

		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}
	res, err := ph.service.Repository.ListKnowledgeBases(ctx, uid)
	if err != nil {
		return nil, fmt.Errorf(ErrorListKnowledgeBasesMsg, err)
	}

	kbs := make([]*artifactpb.KnowledgeBase, len(res))
	for i, kb := range res {
		kbs[i] = &artifactpb.KnowledgeBase{
			Name:        kb.Name,
			Id:          kb.KbID,
			Description: kb.Description,
			Tags:        kb.Tags,
			CreateTime:  kb.CreateTime.String(),
			UpdateTime:  kb.UpdateTime.String(),
			OwnerName:   kb.Owner,
		}
	}
	return &artifactpb.ListKnowledgeBasesResponse{
		Body: &artifactpb.KnowledgeBasesList{
			KnowledgeBases: kbs,
		},
		ErrorMsg: "", StatusCode: 0,
	}, nil
}
func (ph *PublicHandler) UpdateKnowledgeBase(ctx context.Context, req *artifactpb.UpdateKnowledgeBaseRequest) (*artifactpb.UpdateKnowledgeBaseResponse, error) {
	uid, err := getUserIDFromContext(ctx)
	if err != nil {

		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err)
	}
	// check name if it is empty
	if req.Name == "" {
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, "name is required")
	}
	nameOk := isValidName(req.Name)
	if !nameOk {
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, "name is invalid: "+req.Name)
	}
	// check if knowledge base exists
	res, err := ph.service.Repository.UpdateKnowledgeBase(
		ctx,
		uid,
		repository.KnowledgeBase{
			Name:        req.Name,
			KbID:        req.Id,
			Description: req.Description,
			Tags:        req.Tags,
			Owner:       uid,
		},
	)
	if err != nil {
		return nil, fmt.Errorf(ErrorUpdateKnowledgeBaseMsg, err)
	}
	// populate response
	return &artifactpb.UpdateKnowledgeBaseResponse{
		Body: &artifactpb.KnowledgeBase{
			Name:        res.Name,
			Id:          res.KbID,
			Description: res.Description,
			Tags:        res.Tags,
			CreateTime:  res.CreateTime.String(),
			UpdateTime:  res.UpdateTime.String(),
			OwnerName:   res.Owner,
		}, ErrorMsg: "", StatusCode: 0,
	}, nil
}
func (ph *PublicHandler) DeleteKnowledgeBase(ctx context.Context, req *artifactpb.DeleteKnowledgeBaseRequest) (*artifactpb.DeleteKnowledgeBaseResponse, error) {

	uid, err := getUserIDFromContext(ctx)
	if err != nil {

		return nil, fmt.Errorf(ErrorDeleteKnowledgeBaseMsg, err)
	}
	err = ph.service.Repository.DeleteKnowledgeBase(ctx, uid, req.Id)
	if err != nil {

		return nil, fmt.Errorf(ErrorDeleteKnowledgeBaseMsg, err)
	}
	return &artifactpb.DeleteKnowledgeBaseResponse{
		ErrorMsg: "", StatusCode: 0,
	}, nil
}
func getUserIDFromContext(ctx context.Context) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if v, ok := md[strings.ToLower(constant.HeaderUserUIDKey)]; ok {
		return v[0], nil
	}
	return "", fmt.Errorf("user id not found in context")
}

func isValidName(name string) bool {
	name = strings.ToLower(name) // Convert the name to lowercase for case-insensitive matching
	// Define the regular expression pattern
	pattern := `^[a-z0-9 _-]+$`
	// Compile the regular expression
	re := regexp.MustCompile(pattern)
	// Match the name against the regular expression
	return re.MatchString(name)
}

// toIDStyle converts a name to an ID style by replacing spaces with underscores
// and ensuring it only contains lowercase letters, underscores, and hyphens.
func toIDStyle(name string) string {

	// Replace spaces with underscores
	id := strings.ReplaceAll(name, " ", "_")

	// Convert to lowercase
	id = strings.ToLower(id)

	return id
}
