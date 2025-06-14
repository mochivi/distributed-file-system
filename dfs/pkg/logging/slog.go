package logging

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/mochivi/distributed-file-system/pkg/utils"
)

// SetupLogger configures slog with detailed output and node identification
func SetupLogger(nodeID string, logLevel slog.Level) *slog.Logger {
	// Create a custom handler options
	opts := &slog.HandlerOptions{
		Level:     logLevel,
		AddSource: true, // This adds source file and line number
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Customize timestamp format
			if a.Key == slog.TimeKey {
				return slog.Attr{
					Key:   "timestamp",
					Value: slog.StringValue(a.Value.Time().Format(time.RFC3339Nano)),
				}
			}
			// Customize level display
			if a.Key == slog.LevelKey {
				return slog.Attr{
					Key:   "level",
					Value: slog.StringValue(a.Value.String()),
				}
			}
			// Customize source information for better readability
			if a.Key == slog.SourceKey {
				source := a.Value.Any().(*slog.Source)
				return slog.Attr{
					Key: "source",
					Value: slog.StringValue(
						fmt.Sprintf("%s:%d", source.File, source.Line),
					),
				}
			}
			return a
		},
	}

	// Create JSON handler for structured logging
	handler := slog.NewJSONHandler(os.Stdout, opts)

	// Create logger with node ID as a persistent attribute
	logger := slog.New(handler).With(
		slog.String("node_id", nodeID),
		slog.String("service", "your-service-name"), // Optional: add service name
	)

	return logger
}

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

// Initialize your service with configured logger
func InitLogger() (*slog.Logger, error) {
	logLevel := slog.LevelInfo
	var logger *slog.Logger

	environment := utils.GetEnvString("ENVIRONMENT", "development")
	switch environment {
	case "development":
		logLevel = slog.LevelDebug
		logger = SetupTextLogger(logLevel)
	case "production":
		logLevel = slog.LevelInfo
		logger = SetupTextLogger(logLevel)
	default:
		return nil, errors.New("invalid environment")
	}

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
