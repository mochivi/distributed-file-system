package streaming

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
)

// Initiate a stream to send a chunk to a datanode
func (s *clientStreamer) SendChunkStream(ctx context.Context, stream grpc.BidiStreamingClient[proto.ChunkDataStream, proto.ChunkDataAck],
	logger *slog.Logger, params UploadChunkStreamParams) ([]*common.NodeInfo, error) {

	logger = logging.OperationLogger(logger, "stream_chunk", slog.String("session_id", params.SessionID), slog.String("chunk_id", params.ChunkHeader.ID))
	logger.Debug("Initializing chunk data stream")

	// Stream data in chunks
	logger.Debug("Chunk data stream created", slog.Int("chunk_size_KB", s.config.ChunkStreamSize/1024))

	totalSize := len(params.Data)
	offset := 0
	hasher := sha256.New()
	iteration := 1 // just for logging purposes

	for offset < totalSize {
		streamFrameLogger := logging.ExtendLogger(logger, slog.Int("iteration", iteration), slog.Int("offset", offset), slog.Int("total_size", totalSize))

		// Calculate chunk size for this iteration
		remainingBytes := totalSize - offset
		currentChunkStreamSize := min(remainingBytes, s.config.ChunkStreamSize)

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
		for retryCount < s.config.MaxChunkRetries {
			retryCountLogger := logging.ExtendLogger(streamFrameLogger, slog.Int("retry_count", retryCount))
			retryCountLogger.Debug("Sending chunk data")

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
				retryCountLogger.Error("Failed to send chunk", slog.String("error", err.Error()))
				return nil, fmt.Errorf("failed to send chunk at offset %d: %w", offset, err)
			}

			// Wait for acknowledgment
			resp, err := stream.Recv()
			if err != nil && err != io.EOF {
				retryCountLogger.Error("Failed to receive stream response", slog.String("error", err.Error()))
				retryCount++
				if retryCount >= s.config.MaxChunkRetries {
					return nil, fmt.Errorf("failed to receive stream response after %d retries: %w", s.config.MaxChunkRetries, err)
				}
				time.Sleep(time.Duration(retryCount) * time.Second) // Exponential backoff
				continue
			}

			ack = common.ChunkDataAckFromProto(resp)

			if !ack.Success {
				retryCountLogger.Error("Chunk data failed", slog.String("error", ack.Message))
				retryCount++
				if retryCount >= s.config.MaxChunkRetries {
					return nil, fmt.Errorf("chunk failed after %d retries: %s", s.config.MaxChunkRetries, ack.Message)
				}
				time.Sleep(time.Duration(retryCount) * time.Second)
				continue
			}

			// Validate bytes received
			expectedBytes := offset + currentChunkStreamSize
			if ack.BytesReceived != expectedBytes {
				retryCountLogger.Error("Byte count mismatch", slog.Int("expected", expectedBytes), slog.Int("got", ack.BytesReceived))
				retryCount++
				if retryCount >= s.config.MaxChunkRetries {
					return nil, fmt.Errorf("byte count mismatch after %d retries: expected %d, got %d",
						s.config.MaxChunkRetries, expectedBytes, ack.BytesReceived)
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
			case <-time.After(s.config.BackpressureTime):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		// Update offset
		offset += currentChunkStreamSize

		// Check for context cancellation
		select {
		case <-ctx.Done():
			streamFrameLogger.Info("Chunk data stream cancelled by client")
			return nil, ctx.Err()
		default:
		}

		iteration++
	}

	// Close the stream and wait for response
	if err := stream.CloseSend(); err != nil {
		logger.Error("Failed to close stream", slog.String("error", err.Error()))
		return nil, fmt.Errorf("failed to close stream: %w", err)
	}

	// Wait for and receive the final response
	finalStreamResp, err := stream.Recv()
	if err != nil && err != io.EOF {
		logger.Error("Failed to receive stream response", slog.String("error", err.Error()))
		return nil, fmt.Errorf("failed to receive stream response: %w", err)
	}
	finalStreamAck := common.ChunkDataAckFromProto(finalStreamResp)

	// Validate if checksum after all partial sections are added up still matches the original
	calculatedChecksum := fmt.Sprintf("%x", hasher.Sum(nil))
	if calculatedChecksum != params.ChunkHeader.Checksum {
		logger.Error("Request checksum mismatch", slog.String("expected", params.ChunkHeader.Checksum), slog.String("calculated", calculatedChecksum))
		return nil, fmt.Errorf("request checksum mismatch: expected %s, calculated %s",
			params.ChunkHeader.Checksum, calculatedChecksum)
	}

	if !finalStreamAck.Success {
		logger.Error("Stream failed", slog.String("error", finalStreamAck.Message))
		return nil, fmt.Errorf("stream failed: %s", finalStreamAck.Message)
	}

	// If streamer is being used by a client, must wait for another final stream with the replicas information
	if s.config.WaitReplicas {
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
				logger.Error("Failed to receive replicas response", slog.String("error", err.Error()))
				return nil, fmt.Errorf("failed to receive replicas response: %w", err)
			}

			finalReplicasAck = common.ChunkDataAckFromProto(finalReplicasResp)

			if !finalReplicasAck.Success {
				logger.Error("Datanode failed to replicate chunk", slog.String("error", finalReplicasAck.Message))
				return nil, fmt.Errorf("datanode failed to replicate chunk: %s", finalReplicasAck.Message)
			}
			break
		}

		logger.Debug("Replicas received", slog.Any("replicas", finalReplicasAck.Replicas))
		return finalReplicasAck.Replicas, nil
	}

	logger.Debug("Chunk data stream completed successfully")
	return nil, nil
}

func (s *clientStreamer) ReceiveChunkStream(ctx context.Context, stream grpc.ServerStreamingClient[proto.ChunkDataStream],
	buffer *bytes.Buffer, logger *slog.Logger, params DownloadChunkStreamParams) error {

	logger = logging.OperationLogger(logger, "receive_chunk_stream", slog.String("session_id", params.SessionID))
	logger.Debug("Initializing chunk data stream")

	for {
		protoChunkDataStream, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				logger.Debug("Download stream closed by client")
				return nil
			}
			logger.Error("Failed to receive chunk data", slog.String("error", err.Error()))
			return fmt.Errorf("failed to receive chunk data: %w", err)
		}
		chunkStream := common.ChunkDataStreamFromProto(protoChunkDataStream)

		_, _ = buffer.Write(chunkStream.Data) // err is always nil

		if chunkStream.IsFinal {
			break
		}
	}

	return nil
}
