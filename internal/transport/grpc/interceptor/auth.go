package interceptor

import (
	"context"
	"strings"

	"github.com/MiltonJ23/WouriFS/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// contextKey is a private key that will ensure that our context doesn't get override or erased or accessed by Context spoofing of another package by error thus compromising our creds
type contextKey string

// PayloadContextKey is the key used to store the secure identity in the gRPC request.
const PayloadContextKey contextKey = "wourifs-auth-payload"

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
		return nil, status.Errorf(codes.Unauthenticated, "security violation: invalid authorization token: %v", payloadValidationErr)
	}
	// now we inject the context into the payload to ensure namespace isolation
	return context.WithValue(ctx, PayloadContextKey, payload), nil
}

func PayloadFromContext(ctx context.Context) (*domain.TokenPayload, error) {
	payload, ok := ctx.Value(PayloadContextKey).(*domain.TokenPayload)
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "security violation: payload not found in context or corrupted")
	}
	return payload, nil
}

func (w *WrappedServerStream) Context() context.Context {
	return w.ctx
}
