package streaming

import (
	"errors"
	"fmt"

	"github.com/mochivi/distributed-file-system/internal/common"
)

var (
	// Data source errors
	ErrFrameReadFailed  = errors.New("failed to read frame from chunk source")
	ErrFrameWriteFailed = errors.New("failed to write frame to session buffer")
	ErrChecksumMismatch = errors.New("checksum mismatch")

	// Network/streaming errors
	ErrStreamSendFailed    = errors.New("failed to send stream frame")
	ErrStreamReceiveFailed = errors.New("failed to receive stream frame")
	ErrStreamClosed        = errors.New("stream was closed unexpectedly")
	ErrStreamTimeout       = errors.New("stream operation timed out")

	// Session management errors
	ErrSessionNotFound      = errors.New("session not found")
	ErrSessionExpired       = errors.New("session expired")
	ErrSessionAlreadyExists = errors.New("session already exists")

	// Ack errors
	ErrAckSendFailed = errors.New("failed to send ack")
)

type StreamingError struct {
	Op        string                    // Operation that failed
	Err       error                     // Underlying error
	SessionID common.StreamingSessionID // Context
	ChunkID   string                    // Context
	Offset    int64                     // Context for streaming
}

func (e *StreamingError) Error() string {
	if e.SessionID != "" && e.ChunkID != "" {
		return fmt.Sprintf("%s (session: %s, chunk: %s, offset: %d): %v",
			e.Op, e.SessionID, e.ChunkID, e.Offset, e.Err)
	}
	return fmt.Sprintf("%s: %v", e.Op, e.Err)
}

func (e *StreamingError) Unwrap() error {
	return e.Err
}

// Helper constructors for common error scenarios
func NewChunkFrameReadError(sessionID common.StreamingSessionID, chunkID string, offset int64, err error) *StreamingError {
	return &StreamingError{
		Op:        "chunk_frame_read",
		Err:       fmt.Errorf("%w: %v", ErrFrameReadFailed, err),
		SessionID: sessionID,
		ChunkID:   chunkID,
		Offset:    offset,
	}
}

func NewChunkFrameWriteError(sessionID common.StreamingSessionID, chunkID string, offset int, err error) *StreamingError {
	return &StreamingError{
		Op:        "chunk_frame_write",
		Err:       fmt.Errorf("%w: %v", ErrFrameWriteFailed, err),
		SessionID: sessionID,
		ChunkID:   chunkID,
		Offset:    int64(offset),
	}
}

func NewStreamSendError(sessionID common.StreamingSessionID, chunkID string, offset int64, err error) *StreamingError {
	return &StreamingError{
		Op:        "stream_send",
		Err:       fmt.Errorf("%w: %v", ErrStreamSendFailed, err),
		SessionID: sessionID,
		ChunkID:   chunkID,
		Offset:    offset,
	}
}

func NewStreamReceiveError(sessionID common.StreamingSessionID, offset int, err error) *StreamingError {
	return &StreamingError{
		Op:        "stream_receive",
		Err:       fmt.Errorf("%w: %v", ErrStreamReceiveFailed, err),
		SessionID: sessionID,
		Offset:    int64(offset),
	}
}
