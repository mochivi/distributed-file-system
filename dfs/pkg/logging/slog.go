package logging

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/utils"
)

type contextKey string

const loggerKey contextKey = "logger"

// TextLogger can be used during development for more readable logs
func SetupTextLogger(logLevel slog.Level) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level:     logLevel,
		AddSource: true,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Custom timestamp format
			if a.Key == slog.TimeKey {
				return slog.Attr{
					Key:   "time",
					Value: slog.StringValue(a.Value.Time().Format("2006-01-02 15:04:05.000")),
				}
			}
			return a
		},
	}

	handler := slog.NewTextHandler(os.Stdout, opts)

	return slog.New(handler)
}

// NewTestLogger is a test logger that is used for testing
func NewTestLogger(logLevel slog.Level, discard bool) *slog.Logger {
	if discard {
		return slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return SetupTextLogger(logLevel)
}

// Initialize your service with configured logger
func InitLogger() (*slog.Logger, error) {
	var logger *slog.Logger

	logLevelStr := utils.GetEnvString("LOG_LEVEL", "warn")
	var logLevel slog.Level
	switch logLevelStr {
	case "debug":
		logLevel = slog.LevelDebug
	case "info":
		logLevel = slog.LevelInfo
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		return nil, errors.New("invalid log level")
	}
	logger = SetupTextLogger(logLevel)

	if logger == nil {
		return nil, errors.New("logger is nil")
	}

	// Set as default logger -- for calling slog.Info etc
	slog.SetDefault(logger)

	return logger, nil
}

func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, logger)
}

func FromContext(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(loggerKey).(*slog.Logger); ok {
		return logger
	}
	return slog.Default() // Fallback to default logger
}

func FromContextWith(ctx context.Context, kvs ...any) (context.Context, *slog.Logger) {
	logger := FromContext(ctx)        // Get logger fom context
	logger = logger.With(kvs...)      // Add new fields
	newCtx := WithLogger(ctx, logger) // Attach to new context already
	return newCtx, logger
}

func FromContextWithOperation(ctx context.Context, operation string, kvs ...any) (context.Context, *slog.Logger) {
	logger := FromContext(ctx) // Get logger fom context
	attrs := append(kvs, slog.String(common.LogOperation, operation))
	logger = logger.With(attrs...)
	newCtx := WithLogger(ctx, logger) // Attach to new context already
	return newCtx, logger
}

// Extend logger with additional attributes
func ExtendLogger(logger *slog.Logger, kvs ...any) *slog.Logger {
	return logger.With(kvs...)
}

// Add some operation specific attributes to the logger
func OperationLogger(logger *slog.Logger, operation string, kvs ...any) *slog.Logger {
	attrs := append(kvs, slog.String(common.LogOperation, operation))
	return ExtendLogger(logger, attrs...)
}

func ServiceLogger(logger *slog.Logger, service string, kvs ...any) *slog.Logger {
	attrs := append(kvs, slog.String(common.LogService, service))
	return ExtendLogger(logger, attrs...)
}
