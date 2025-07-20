package logging

import (
	"errors"
	"log/slog"
	"os"

	"github.com/mochivi/distributed-file-system/pkg/utils"
)

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
func NewTestLogger(logLevel slog.Level) *slog.Logger {
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

// Extend logger with additional attributes
func ExtendLogger(logger *slog.Logger, kvs ...any) *slog.Logger {
	return logger.With(kvs...)
}

// Add some operation specific attributes to the logger
func OperationLogger(logger *slog.Logger, operation string, kvs ...any) *slog.Logger {
	attrs := append(kvs, slog.String("operation", operation))
	return ExtendLogger(logger, attrs...)
}

func ServiceLogger(logger *slog.Logger, service string, kvs ...any) *slog.Logger {
	attrs := append(kvs, slog.String("service", service))
	return ExtendLogger(logger, attrs...)
}
