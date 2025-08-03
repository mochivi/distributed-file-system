package grpcutil

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/mochivi/distributed-file-system/internal/apperr"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrorsInterceptor is a gRPC unary interceptor that inspects errors and translates them.
func ErrorsInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	resp, err := handler(ctx, req)
	if err == nil {
		return resp, nil
	}

	// Attempt to cast the returned error to the application's custom error type.
	var appErr *apperr.AppError
	if errors.As(err, &appErr) {
		// This is the custom error, so we can translate it directly.
		return nil, status.Error(appErr.Code, appErr.Message)
	}

	// This is an unexpected error type (e.g., a panic recovered as an error).
	return nil, status.Error(codes.Internal, "an unexpected internal error occurred")
}

func NewLoggingInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()

		// Generate a request ID for each request
		requestID := generateRequestID()
		requestLogger := logging.ExtendLogger(logger, slog.String(common.LogRequestID, requestID))

		requestLogger.Info("Request started",
			slog.String(common.LogMethod, info.FullMethod),
			slog.Time(common.LogTimestamp, start))

		// Attach logger to context
		ctx = logging.WithLogger(ctx, requestLogger)

		resp, err := handler(ctx, req)

		duration := time.Since(start)

		// Log the outcome - but DON'T inspect the error details here
		// The ErrorsInterceptor has already handled error translation and logging
		if err != nil {
			// Just log that it failed, error details already logged by ErrorsInterceptor
			requestLogger.Info("Request completed with error",
				slog.String(common.LogMethod, info.FullMethod),
				slog.String(common.LogError, err.Error()),
				slog.Duration(common.LogDuration, duration),
				slog.String(common.LogStatus, status.Code(err).String()))
		} else {
			requestLogger.Info("Request completed successfully",
				slog.String(common.LogMethod, info.FullMethod),
				slog.Duration(common.LogDuration, duration))
		}

		return resp, err
	}
}

func generateRequestID() string {
	return uuid.NewString()
}
