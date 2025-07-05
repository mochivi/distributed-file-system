package streamer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
)

type IStreamer interface {
	SendChunkStream(ctx context.Context, stream grpc.BidiStreamingClient[proto.ChunkDataStream, proto.ChunkDataAck],
		logger *slog.Logger, params UploadChunkStreamParams) ([]*common.NodeInfo, error)
	ReceiveChunkStream(ctx context.Context, stream grpc.ServerStreamingClient[proto.ChunkDataStream],
		buffer *bytes.Buffer, logger *slog.Logger, params DownloadChunkStreamParams) error
}

type Streamer struct {
	Config config.StreamerConfig
}

func NewStreamer(config config.StreamerConfig) *Streamer {
	return &Streamer{Config: config}
}

type UploadChunkStreamParams struct {
	// Retrieve session
	SessionID string

	// Chunk data
	ChunkHeader common.ChunkHeader
	Data        []byte
}

type DownloadChunkStreamParams struct {
	SessionID   string
	ChunkHeader common.ChunkHeader
}

// Initiate a stream to send a chunk to a datanode
func (s *Streamer) SendChunkStream(ctx context.Context, stream grpc.BidiStreamingClient[proto.ChunkDataStream, proto.ChunkDataAck],
	logger *slog.Logger, params UploadChunkStreamParams) ([]*common.NodeInfo, error) {

	logger = logging.OperationLogger(logger, "stream_chunk", slog.String("session_id", params.SessionID))
	logger.Debug("Initializing chunk data stream")

	// Stream data in chunks
	logger.Debug("Chunk data stream created", slog.Int("chunk_size_KB", s.Config.ChunkStreamSize/1024))

	totalSize := len(params.Data)
	offset := 0
	hasher := sha256.New()
	iteration := 1 // just for logging purposes

	for offset < totalSize {
		// Calculate chunk size for this iteration
		remainingBytes := totalSize - offset
		currentChunkStreamSize := s.Config.ChunkStreamSize
		if remainingBytes < currentChunkStreamSize {
			currentChunkStreamSize = remainingBytes
		}

		// Extract chunk data
		chunkData := params.Data[offset : offset+currentChunkStreamSize]
		isFinal := (offset + currentChunkStreamSize) >= totalSize

		// Calculate partial checksum for this chunk
		partialChecksum := chunk.CalculateChecksum(chunkData)
		hasher.Write(chunkData)

		// Retry logic for individual chunks
		var ack common.ChunkDataAck
		retryCount := 0

		// Retry the same chunk piece if it fails
		// logger.Debug("Sending chunk data", slog.Int("iteration", iteration), slog.Int("offset", offset), slog.Int("chunk_size", currentChunkStreamSize))
		for retryCount < s.Config.MaxChunkRetries {
			// Create stream message
			streamMsg := &common.ChunkDataStream{
				SessionID:       params.SessionID,
				ChunkID:         params.ChunkHeader.ID,
				Data:            chunkData,
				Offset:          offset,
				IsFinal:         isFinal,
				PartialChecksum: partialChecksum,
			}

			// Send chunk piece
			if err := stream.Send(streamMsg.ToProto()); err != nil {
				return nil, fmt.Errorf("failed to send chunk at offset %d: %w", offset, err)
			}

			// Wait for acknowledgment
			resp, err := stream.Recv()
			if err != nil {
				logger.Debug("Failed to receive stream response", slog.Int("retry_count", retryCount), slog.String("error", err.Error()))
				retryCount++
				if retryCount >= s.Config.MaxChunkRetries {
					return nil, fmt.Errorf("failed to receive stream response after %d retries: %w", s.Config.MaxChunkRetries, err)
				}
				time.Sleep(time.Duration(retryCount) * time.Second) // Exponential backoff
				continue
			}

			ack = common.ChunkDataAckFromProto(resp)

			if !ack.Success {
				logger.Debug("Chunk data failed", slog.Int("retry_count", retryCount), slog.String("error", ack.Message))
				retryCount++
				if retryCount >= s.Config.MaxChunkRetries {
					return nil, fmt.Errorf("chunk failed after %d retries: %s", s.Config.MaxChunkRetries, ack.Message)
				}
				time.Sleep(time.Duration(retryCount) * time.Second)
				continue
			}

			// Validate bytes received
			expectedBytes := offset + currentChunkStreamSize
			if ack.BytesReceived != expectedBytes {
				logger.Debug("Byte count mismatch", slog.Int("expected", expectedBytes), slog.Int("got", ack.BytesReceived))
				retryCount++
				if retryCount >= s.Config.MaxChunkRetries {
					return nil, fmt.Errorf("byte count mismatch after %d retries: expected %d, got %d",
						s.Config.MaxChunkRetries, expectedBytes, ack.BytesReceived)
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
			case <-time.After(s.Config.BackpressureTime):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		// Update offset
		offset += currentChunkStreamSize

		// Check for context cancellation
		select {
		case <-ctx.Done():
			logger.Info("Chunk data stream cancelled by client")
			return nil, ctx.Err()
		default:
		}

		iteration++
	}

	// Close the stream and wait for response
	if err := stream.CloseSend(); err != nil {
		return nil, fmt.Errorf("failed to close stream: %w", err)
	}

	// Wait for and receive the final response
	finalStreamResp, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("failed to receive stream response: %w", err)
	}
	finalStreamAck := common.ChunkDataAckFromProto(finalStreamResp)

	// Validate if checksum after all partial sections are added up still matches the original
	calculatedChecksum := fmt.Sprintf("%x", hasher.Sum(nil))
	if calculatedChecksum != params.ChunkHeader.Checksum {
		return nil, fmt.Errorf("request checksum mismatch: expected %s, calculated %s",
			params.ChunkHeader.Checksum, calculatedChecksum)
	}

	if !finalStreamAck.Success {
		return nil, fmt.Errorf("stream failed: %s", finalStreamAck.Message)
	}

	// If streamer is being used by a client, must wait for another final stream with the replicas information
	if s.Config.WaitReplicas {
		attempt := 0
		var finalReplicasAck common.ChunkDataAck
		for attempt < 3 {
			finalReplicasResp, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					logger.Debug("Retrying to receive replicas response", slog.Int("attempt", attempt))
					attempt++
					time.Sleep(time.Duration(attempt) * 2 * time.Second) // Exponential backoff
					continue
				}
				return nil, fmt.Errorf("failed to receive replicas response: %w", err)
			}

			finalReplicasAck = common.ChunkDataAckFromProto(finalReplicasResp)
			if !finalReplicasAck.Success {
				return nil, fmt.Errorf("datanode failed to replicate chunk: %s", finalReplicasAck.Message)
			}
			break
		}

		logger.Info("Replicas received", slog.Any("replicas", finalReplicasAck.Replicas))
		return finalReplicasAck.Replicas, nil
	}

	logger.Info("Chunk data stream completed successfully")
	return nil, nil
}

func (s *Streamer) ReceiveChunkStream(ctx context.Context, stream grpc.ServerStreamingClient[proto.ChunkDataStream],
	buffer *bytes.Buffer, logger *slog.Logger, params DownloadChunkStreamParams) error {

	logger = logging.OperationLogger(logger, "receive_chunk_stream", slog.String("session_id", params.SessionID))
	logger.Debug("Initializing chunk data stream")

	for {
		protoChunkDataStream, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("failed to receive chunk data: %w", err)
		}
		chunkStream := common.ChunkDataStreamFromProto(protoChunkDataStream)

		if _, err := buffer.Write(chunkStream.Data); err != nil {
			return fmt.Errorf("failed to write chunk data to buffer: %w", err)
		}

		if chunkStream.IsFinal {
			break
		}
	}

	return nil
}
