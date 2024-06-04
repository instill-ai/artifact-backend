package handler

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"google.golang.org/grpc/metadata"

	"github.com/instill-ai/artifact-backend/pkg/constant"
	"github.com/instill-ai/artifact-backend/pkg/customerror"
	"github.com/instill-ai/artifact-backend/pkg/repository"
	artifactpb "github.com/instill-ai/protogen-go/artifact/artifact/v1alpha"
)

type ErrorMsg map[int]string

const ErrorCreateKnowledgeBaseMsg = "failed to create knowledge base: %w"
const ErrorListKnowledgeBasesMsg = "failed to get knowledge bases: %w "
const ErrorUpdateKnowledgeBaseMsg = "failed to update knowledge base: %w"
const ErrorDeleteKnowledgeBaseMsg = "failed to delete knowledge base: %w"

func (ph *PublicHandler) CreateKnowledgeBase(ctx context.Context, req *artifactpb.CreateKnowledgeBaseRequest) (*artifactpb.CreateKnowledgeBaseResponse, error) {

	uid, err := getUserIDFromContext(ctx)
	if err != nil {
		err := fmt.Errorf("failed to get user id from header: %v. err: %w", err, customerror.ErrUnauthenticated)
		return nil, err
	}
	// check name if it is empty
	if req.Name == "" {
		err := fmt.Errorf("name is required. err: %w", ErrCheckRequiredFields)
		return nil, err
	}
	nameOk := isValidName(req.Name)
	if !nameOk {
		msg := "name is invalid: %v. err: %w"
		return nil, fmt.Errorf(msg, req.Name, customerror.ErrInvalidArgument)
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
		return nil, err
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
		return nil, err
	}
	// check name if it is empty
	if req.Name == "" {
		return nil, fmt.Errorf("name is empty. err: %w", ErrCheckRequiredFields)
	}
	nameOk := isValidName(req.Name)
	if !nameOk {
		return nil, fmt.Errorf("name: %s is invalid. err: %w", req.Name, customerror.ErrInvalidArgument)
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
		return nil, err
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

		return nil, err
	}
	err = ph.service.Repository.DeleteKnowledgeBase(ctx, uid, req.Id)
	if err != nil {

		return nil, err
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
	return "", fmt.Errorf("user id not found in context. err: %w", customerror.ErrUnauthenticated)
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
