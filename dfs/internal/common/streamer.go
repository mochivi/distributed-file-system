package common

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"time"

	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
)

const (
	DEFAULT_CHUNK_STREAM_SIZE = 256 * 1024 // Default 256KB chunks
)

type Streamer struct {
}

func NewStreamer(logger *slog.Logger) *Streamer {
	return &Streamer{}
}

type StreamChunkParams struct {
	// Retrieve session
	SessionID string

	// Chunk data
	ChunkID  string
	Checksum string
	Data     []byte

	// Streamer configuration
	MaxChunkRetries int
	ChunkStreamSize int
}

// Initiate a stream to send a chunk to a datanode
func (s *Streamer) StreamChunk(ctx context.Context, stream grpc.BidiStreamingClient[proto.ChunkDataStream, proto.ChunkDataAck],
	logger *slog.Logger, params StreamChunkParams) error {

	logger = logging.OperationLogger(logger, "stream_chunk", slog.String("session_id", params.SessionID))
	logger.Debug("Initializing chunk data stream")

	// Stream data in chunks
	chunkStreamSize := params.ChunkStreamSize
	if chunkStreamSize == 0 {
		chunkStreamSize = DEFAULT_CHUNK_STREAM_SIZE
	}
	logger.Debug("Chunk data stream created", slog.Int("chunk_size_KB", chunkStreamSize/1024))

	totalSize := len(params.Data)
	offset := 0
	hasher := sha256.New()
	iteration := 1 // just for logging purposes

	for offset < totalSize {
		// Calculate chunk size for this iteration
		remainingBytes := totalSize - offset
		currentChunkStreamSize := chunkStreamSize
		if remainingBytes < currentChunkStreamSize {
			currentChunkStreamSize = remainingBytes
		}

		// Extract chunk data
		chunkData := params.Data[offset : offset+currentChunkStreamSize]
		isFinal := (offset + currentChunkStreamSize) >= totalSize

		// Calculate partial checksum for this chunk
		partialChecksum := CalculateChecksum(chunkData)
		hasher.Write(chunkData)

		// Retry logic for individual chunks
		var ack ChunkDataAck
		retryCount := 0

		// Retry the same chunk piece if it fails
		logger.Debug("Sending chunk data", slog.Int("iteration", iteration), slog.Int("offset", offset), slog.Int("chunk_size", currentChunkStreamSize))
		for retryCount < params.MaxChunkRetries {
			// Create stream message
			streamMsg := &ChunkDataStream{
				SessionID:       params.SessionID,
				ChunkID:         params.ChunkID,
				Data:            chunkData,
				Offset:          offset,
				IsFinal:         isFinal,
				PartialChecksum: partialChecksum,
			}

			// Send chunk piece
			if err := stream.Send(streamMsg.ToProto()); err != nil {
				return fmt.Errorf("failed to send chunk at offset %d: %w", offset, err)
			}

			// Wait for acknowledgment
			resp, err := stream.Recv()
			if err != nil {
				logger.Debug("Failed to receive stream response", slog.Int("retry_count", retryCount), slog.String("error", err.Error()))
				retryCount++
				if retryCount >= params.MaxChunkRetries {
					return fmt.Errorf("failed to receive stream response after %d retries: %w", params.MaxChunkRetries, err)
				}
				time.Sleep(time.Duration(retryCount) * time.Second) // Exponential backoff
				continue
			}

			ack = ChunkDataAckFromProto(resp)

			if !ack.Success {
				logger.Debug("Chunk data failed", slog.Int("retry_count", retryCount), slog.String("error", ack.Message))
				retryCount++
				if retryCount >= params.MaxChunkRetries {
					return fmt.Errorf("chunk failed after %d retries: %s", params.MaxChunkRetries, ack.Message)
				}
				time.Sleep(time.Duration(retryCount) * time.Second)
				continue
			}

			// Validate bytes received
			expectedBytes := offset + currentChunkStreamSize
			if ack.BytesReceived != expectedBytes {
				logger.Debug("Byte count mismatch", slog.Int("expected", expectedBytes), slog.Int("got", ack.BytesReceived))
				retryCount++
				if retryCount >= params.MaxChunkRetries {
					return fmt.Errorf("byte count mismatch after %d retries: expected %d, got %d",
						params.MaxChunkRetries, expectedBytes, ack.BytesReceived)
				}
				time.Sleep(time.Duration(retryCount) * time.Second)
				continue
			}

			// Success - break out of retry loop
			break
		}

		// Handle backpressure - server needs time to flush to disk
		if !ack.ReadyForNext && !isFinal {
			select {
			case <-time.After(time.Duration(500) * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Update offset
		offset += currentChunkStreamSize

		// Check for context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		iteration++
	}

	// Close the stream and wait for response
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("failed to close stream: %w", err)
	}

	// Wait for and receive the final response
	finalResp, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive stream response: %w", err)
	}
	finalAck := ChunkDataAckFromProto(finalResp)

	// Validate if checksum after all partial sections are added up still matches the original
	calculatedChecksum := fmt.Sprintf("%x", hasher.Sum(nil))
	if calculatedChecksum != params.Checksum {
		return fmt.Errorf("request checksum mismatch: expected %s, calculated %s",
			params.Checksum, calculatedChecksum)
	}

	if !finalAck.Success {
		return fmt.Errorf("stream failed: %s", finalAck.Message)
	}

	logger.Info("Chunk data stream completed successfully")
	return nil
}
