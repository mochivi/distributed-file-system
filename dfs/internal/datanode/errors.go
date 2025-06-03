package datanode

import (
	"fmt"
	"log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Custom error types for better error handling
type ChunkError struct {
	ChunkID string
	Code    codes.Code
	Message string
	Err     error
}

func (e ChunkError) Error() string {
	return fmt.Sprintf("chunk operation failed [%s]: %s (code: %s)", e.ChunkID, e.Message, e.Code)
}

func (e ChunkError) Unwrap() error {
	return e.Err
}

// Specific error types
type ChunkNotFoundError struct {
	ChunkID string
	Message string
}

func (e ChunkNotFoundError) Error() string {
	return fmt.Sprintf("chunk '%s' not found: %s", e.ChunkID, e.Message)
}

type ServerUnavailableError struct {
	Message string
}

func (e ServerUnavailableError) Error() string {
	return fmt.Sprintf("server unavailable: %s", e.Message)
}

type RequestTimeoutError struct {
	ChunkID string
	Message string
}

func (e RequestTimeoutError) Error() string {
	return fmt.Sprintf("request timeout for chunk '%s': %s", e.ChunkID, e.Message)
}

type InternalServerError struct {
	Message string
}

func (e *InternalServerError) Error() string {
	return fmt.Sprintf("internal server error: %s", e.Message)
}

func handlegRPCError(err error, chunkID string) error {
	if err == nil {
		return nil
	}

	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.NotFound:
			log.Printf("Chunk %s not found: %s", chunkID, st.Message())
			return &ChunkNotFoundError{
				ChunkID: chunkID,
				Message: st.Message(),
			}

		case codes.Internal:
			log.Printf("Internal server error: %s", st.Message())
			return &InternalServerError{
				Message: st.Message(),
			}

		case codes.Unavailable:
			log.Printf("Server unavailable: %s", st.Message())
			return &ServerUnavailableError{
				Message: st.Message(),
			}

		case codes.DeadlineExceeded:
			log.Printf("Request timeout for chunk %s: %s", chunkID, st.Message())
			return &RequestTimeoutError{
				ChunkID: chunkID,
				Message: st.Message(),
			}

		case codes.InvalidArgument:
			log.Printf("Invalid argument for chunk %s: %s", chunkID, st.Message())
			return fmt.Errorf("invalid request for chunk '%s': %s", chunkID, st.Message())

		case codes.PermissionDenied:
			log.Printf("Permission denied for chunk %s: %s", chunkID, st.Message())
			return fmt.Errorf("access denied for chunk '%s': %s", chunkID, st.Message())

		case codes.ResourceExhausted:
			log.Printf("Resource exhausted for chunk %s: %s", chunkID, st.Message())
			return fmt.Errorf("server overloaded, please retry later (chunk: %s): %s", chunkID, st.Message())

		default:
			log.Printf("Unexpected gRPC error for chunk %s (code: %s): %s", chunkID, st.Code(), st.Message())
			return &ChunkError{
				ChunkID: chunkID,
				Code:    st.Code(),
				Message: st.Message(),
				Err:     err,
			}
		}
	} else {
		// Not a gRPC status error (network error, etc.)
		log.Printf("Non-gRPC error for chunk %s: %v", chunkID, err)
		return fmt.Errorf("network or connection error for chunk '%s': %w", chunkID, err)
	}
}
