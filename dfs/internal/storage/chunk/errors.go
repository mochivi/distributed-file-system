package chunk

import (
	"errors"
	"os"
	"syscall"
)

// These errors should be backend agnostic but relate to the file system.
// Which is the reason why we translate FS errors into our own errors.
// Same errors could happen on different backends but return different errors.

var (
	ErrOpFailed          = errors.New("operation failed")
	ErrNotFound          = errors.New("chunk not found")
	ErrPermission        = errors.New("permission denied")
	ErrResourceExhausted = errors.New("resource exhausted")
	ErrNotExist          = errors.New("does not exist")
	ErrAlreadyExists     = errors.New("already exists")
	ErrIOError           = errors.New("I/O error")
	ErrInvalidArgument   = errors.New("invalid argument")
	ErrInvalidPath       = errors.New("invalid path")
)

func handleFsError(err error) error {
	switch err {
	case os.ErrNotExist:
		return ErrNotFound
	case os.ErrPermission:
		return ErrPermission
	case os.ErrExist:
		return ErrAlreadyExists
	case syscall.ENOSPC:
		return ErrResourceExhausted
	case syscall.EIO:
		return ErrIOError
	case syscall.EACCES:
		return ErrPermission
	case syscall.EPERM:
		return ErrPermission
	case syscall.EINVAL:
		return ErrInvalidArgument
	case syscall.ENOTDIR:
		return ErrInvalidPath
	default:
		return ErrOpFailed
	}
}

// These errors are specific to this package but not related to the file system.
var (
	ErrInvalidChunkID  = errors.New("invalid chunk ID")
	ErrSerializeHeader = errors.New("failed to serialize chunk header")
)
