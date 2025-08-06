package apperr

import (
	"fmt"

	"google.golang.org/grpc/codes"
)

// AppError is the standard error type for the application.
type AppError struct {
	// Code is the canonical gRPC code for this error.
	Code codes.Code

	// Message is a user-facing error message.
	Message string

	// Err is the underlying wrapped error, for internal logging.
	Err error
}

// AppErrorTranslator is an interface that errors can implement to translate
// themselves into an AppError. This allows for decentralized error translation.
type AppErrorTranslator interface {
	ToAppError() *AppError
}

func (e *AppError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

func (e *AppError) Unwrap() error {
	return e.Err
}

// New creates a new AppError without a wrapped error.
func New(code codes.Code, msg string) *AppError {
	return &AppError{Code: code, Message: msg}
}

// Wrap creates a new AppError that wraps an existing error.
func Wrap(code codes.Code, msg string, err error) *AppError {
	return &AppError{Code: code, Message: msg, Err: err}
}

// --- Pre-defined application Level errors  ---

func NotFound(resource, id string, err error) *AppError {
	return Wrap(codes.NotFound, fmt.Sprintf("%s with id '%s' not found", resource, id), err)
}

func InvalidArgument(msg string, err error) *AppError {
	return Wrap(codes.InvalidArgument, fmt.Sprintf("%s: %v", msg, err), err)
}

func ResourceExhausted(msg string, err error) *AppError {
	return Wrap(codes.ResourceExhausted, fmt.Sprintf("%s: %v", msg, err), err)
}

func Internal(err error) *AppError {
	// The public message for an internal error should always be generic.
	// The original error `err` is for internal logging.
	return Wrap(codes.Internal, fmt.Sprintf("an unexpected internal error occurred: %v", err), err)
}
