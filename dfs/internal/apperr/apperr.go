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

func NotFound(resource, id string) *AppError {
	return New(codes.NotFound, fmt.Sprintf("%s with id '%s' not found", resource, id))
}

func InvalidArgument(msg string, err error) *AppError {
	return Wrap(codes.InvalidArgument, msg, err)
}

func ResourceExhausted(msg string) *AppError {
	return New(codes.ResourceExhausted, msg)
}

func Internal(err error) *AppError {
	// The public message for an internal error should always be generic.
	// The original error `err` is for internal logging.
	return Wrap(codes.Internal, "an unexpected internal error occurred", err)
}
