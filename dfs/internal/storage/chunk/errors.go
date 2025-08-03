package chunk

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/mochivi/distributed-file-system/internal/apperr"
	"google.golang.org/grpc/codes"
)

// Sentinel errors - not FS related
var (
	// Package-specific kinds (not directly mapped from the OS)
	ErrInvalidChunkID = errors.New("invalid chunk id")
)

type FSErrorKind uint8

const (
	KindOpFailed FSErrorKind = iota
	// File-system related kinds
	KindNotFound
	KindPermission
	KindResourceExhausted
	KindAlreadyExists
	KindIO
	KindInvalidArgument
	KindInvalidPath
)

func (k FSErrorKind) String() string {
	switch k {
	case KindNotFound:
		return "not found"
	case KindPermission:
		return "permission denied"
	case KindResourceExhausted:
		return "resource exhausted"
	case KindAlreadyExists:
		return "already exists"
	case KindIO:
		return "I/O error"
	case KindInvalidArgument:
		return "invalid argument"
	case KindInvalidPath:
		return "invalid path"
	default:
		return "operation failed"
	}
}

// mapFSErrorKind translates underlying OS/afero errors into FsError values so
// the rest of the codebase can reason about them in a backend-agnostic way.
func mapFSErrorKind(err error) FSErrorKind {
	switch {
	case errors.Is(err, os.ErrNotExist):
		return KindNotFound
	case errors.Is(err, os.ErrPermission), errors.Is(err, syscall.EACCES), errors.Is(err, syscall.EPERM):
		return KindPermission
	case errors.Is(err, os.ErrExist):
		return KindAlreadyExists
	case errors.Is(err, syscall.ENOSPC):
		return KindResourceExhausted
	case errors.Is(err, syscall.EIO):
		return KindIO
	case errors.Is(err, syscall.EINVAL):
		return KindInvalidArgument
	case errors.Is(err, syscall.ENOTDIR):
		return KindInvalidPath
	default:
		return KindOpFailed
	}
}

type FsError struct {
	Kind    FSErrorKind        // the package, backend agnostic error kind
	Message string             // the context from where the error occured
	Backend StorageBackendType // the backend type
	Err     error              // the underlying error
}

func NewFsError(msg string, err error, backend StorageBackendType) *FsError {
	kind := mapFSErrorKind(err)
	return &FsError{
		Message: fmt.Sprintf("%s: %s", msg, kind.String()),
		Backend: backend,
		Kind:    kind,
		Err:     err,
	}
}

func (e *FsError) Error() string {
	if e == nil {
		return "<nil>"
	}
	msg := e.Message
	if e.Err != nil {
		msg = fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return msg
}

func (e *FsError) Unwrap() error { return e.Err }

// Implement the apperr.ToAppError interface
// Allows the error interceptor to automatically translates custom errors into grpc errors
func (e *FsError) ToAppError() *apperr.AppError {
	var code codes.Code
	msg := e.Message

	switch e.Kind {
	case KindNotFound:
		code = codes.NotFound
	case KindPermission:
		code = codes.PermissionDenied
	case KindResourceExhausted:
		code = codes.ResourceExhausted
	case KindAlreadyExists:
		code = codes.AlreadyExists
	case KindInvalidArgument, KindInvalidPath:
		code = codes.InvalidArgument
	case KindIO, KindOpFailed:
		code = codes.Internal
	default:
		return apperr.Internal(e)
	}

	return apperr.Wrap(code, msg, e)
}
