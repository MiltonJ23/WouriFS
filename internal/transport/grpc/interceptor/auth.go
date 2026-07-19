package interceptor

import (
	"context"
	"log"
	"strings"

	"github.com/MiltonJ23/WouriFS/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// contextKey is unexported so only this package can define keys of this type,
// preventing cross-package context key collisions and spoofing.
type contextKey string

// payloadContextKey is the key used to store the validated identity in the gRPC request context.
const payloadContextKey contextKey = "wourifs-auth-payload"

type AuthInterceptor struct {
	token domain.TokenManager
}

type WrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func NewAuthInterceptor(tm domain.TokenManager) *AuthInterceptor {
	return &AuthInterceptor{token: tm}
}

// Unary returns the interceptor for standard gRPC requests
func (i *AuthInterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		newCtx, err := i.authorize(ctx)
		if err != nil {
			return nil, err
		}
		return handler(newCtx, req)
	}
}

// Stream returns the interceptor for stream gRPC requests
func (i *AuthInterceptor) Stream() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		newCtx, err := i.authorize(ss.Context())
		if err != nil {
			return err
		}

		wrappedStream := &WrappedServerStream{
			ServerStream: ss,
			ctx:          newCtx,
		}
		return handler(srv, wrappedStream)
	}
}

func (i *AuthInterceptor) authorize(ctx context.Context) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "security violation: metadata is not provided")
	}

	values := md["authorization"]
	if len(values) == 0 {
		return nil, status.Errorf(codes.Unauthenticated, "security violation: authorization token is not provided")
	}

	accessToken := values[0]
	if !strings.HasPrefix(accessToken, "Bearer ") {
		return nil, status.Errorf(codes.Unauthenticated, "security violation: invalid authorization token format")
	}

	tokenStr := strings.TrimPrefix(accessToken, "Bearer ")

	payload, payloadValidationErr := i.token.VerifyToken(ctx, tokenStr)
	if payloadValidationErr != nil {
		log.Printf("auth interceptor: token verification failed: %v", payloadValidationErr)
		return nil, status.Errorf(codes.Unauthenticated, "security violation: invalid authorization token")
	}
	// now we inject the context into the payload to ensure namespace isolation
	return context.WithValue(ctx, payloadContextKey, payload), nil
}

func PayloadFromContext(ctx context.Context) (*domain.TokenPayload, error) {
	payload, ok := ctx.Value(payloadContextKey).(*domain.TokenPayload)
	if !ok {
		return nil, status.Errorf(codes.Internal, "security violation: payload not found in context or corrupted")
	}
	return payload, nil
}

func (w *WrappedServerStream) Context() context.Context {
	return w.ctx
}

// DevNoAuthInterceptor returns a gRPC interceptor that injects a default
// payload for development and testing. Every request gets the same identity.
// NEVER use in production — this bypasses all namespace isolation.
func DevNoAuthInterceptor() grpc.UnaryServerInterceptor {
	payload := &domain.TokenPayload{
		UserID:    "dev-user",
		Username:  "developer",
		Namespace: "/",
	}
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		return handler(context.WithValue(ctx, payloadContextKey, payload), req)
	}
}

// SetPayloadInContext injects a token payload into a context for testing.
// Exported so that business-logic handlers tested outside this package can be
// provided with a properly-keyed payload without bypassing auth enforcement.
func SetPayloadInContext(ctx context.Context, payload *domain.TokenPayload) context.Context {
	return context.WithValue(ctx, payloadContextKey, payload)
}
