package middleware

import (
	"context"
	"errors"
	"runtime/debug"

	"github.com/jackc/pgconn"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"

	grpcmiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcrecovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"

	"github.com/instill-ai/artifact-backend/pkg/acl"
	"github.com/instill-ai/artifact-backend/pkg/handler"
	"github.com/instill-ai/artifact-backend/pkg/service"
	"github.com/instill-ai/x/errmsg"

	errdomain "github.com/instill-ai/artifact-backend/pkg/errors"
)

// RecoveryInterceptorOpt - panic handler
func RecoveryInterceptorOpt() grpcrecovery.Option {
	return grpcrecovery.WithRecoveryHandler(func(p interface{}) (err error) {
		debug.PrintStack()
		return status.Errorf(codes.Unknown, "panic triggered: %v", p)
	})
}

// UnaryAppendMetadataAndErrorCodeInterceptor - append metadata for unary
func UnaryAppendMetadataAndErrorCodeInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Internal, "can not extract metadata")
	}

	newCtx := metadata.NewIncomingContext(ctx, md)
	h, err := handler(newCtx, req)

	return h, AsGRPCError(err)
}

// StreamAppendMetadataInterceptor - append metadata for stream
func StreamAppendMetadataInterceptor(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		return status.Error(codes.Internal, "can not extract metadata")
	}

	newCtx := metadata.NewIncomingContext(stream.Context(), md)
	wrapped := grpcmiddleware.WrapServerStream(stream)
	wrapped.WrappedContext = newCtx

	err := handler(srv, wrapped)

	return err
}

// AsGRPCError sets the gRPC status and error message according to the error
// type and metadata.
func AsGRPCError(err error) error {
	if err == nil {
		return nil
	}

	// If it's already a status, respect the code.
	// If, additionally, an end-user message has been set for the error, it has
	// has priority over the status message.
	if st, ok := status.FromError(err); ok {
		if msg := errmsg.Message(err); msg != "" {
			// This conversion is used to preserve the status details.
			p := st.Proto()
			p.Message = msg
			st = status.FromProto(p)
		}

		return st.Err()
	}

	var pgErr *pgconn.PgError
	var code codes.Code
	switch {
	case
		errors.As(err, &pgErr) && pgErr.Code == "23505",
		errors.Is(err, gorm.ErrDuplicatedKey),
		errors.Is(err, errdomain.ErrAlreadyExists):

		code = codes.AlreadyExists
	case
		errors.Is(err, gorm.ErrRecordNotFound),
		errors.Is(err, errdomain.ErrNotFound),
		errors.Is(err, acl.ErrMembershipNotFound):

		code = codes.NotFound
	case
		errors.Is(err, bcrypt.ErrMismatchedHashAndPassword),
		errors.Is(err, handler.ErrCheckUpdateImmutableFields),
		errors.Is(err, handler.ErrCheckOutputOnlyFields),
		errors.Is(err, handler.ErrCheckRequiredFields),
		errors.Is(err, errdomain.ErrExceedMaxBatchSize),
		errors.Is(err, handler.ErrFieldMask),
		errors.Is(err, handler.ErrResourceID),
		errors.Is(err, handler.ErrSematicVersion),
		errors.Is(err, handler.ErrConnectorNamespace),
		errors.Is(err, handler.ErrUpdateMask),
		errors.Is(err, handler.ErrInvalidKnowledgeBaseName),
		errors.Is(err, errdomain.ErrInvalidArgument):
		code = codes.InvalidArgument
	case
		errors.Is(err, errdomain.ErrUnauthorized):

		code = codes.PermissionDenied
	case
		errors.Is(err, errdomain.ErrUnauthenticated):

		code = codes.Unauthenticated

	case
		errors.Is(err, errdomain.ErrRateLimiting):
		code = codes.ResourceExhausted
	case
		errors.Is(err, service.ErrObjectNotUploaded):
		code = codes.Unavailable
	default:
		code = codes.Unknown
	}

	return status.Error(code, errmsg.MessageOrErr(err))
}
