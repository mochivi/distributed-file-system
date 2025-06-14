package datanode

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/logging"
)

type ReplicationManager struct {
	Config ReplicateManagerConfig
	logger *slog.Logger
}

func NewReplicationManager(config ReplicateManagerConfig, logger *slog.Logger) *ReplicationManager {
	logger = logging.ExtendLogger(logger, slog.String("component", "replication_manager"))
	return &ReplicationManager{
		Config: config,
		logger: logger,
	}
}

// paralellReplicate replicates the chunk to the given nodes in parallel
func (rm *ReplicationManager) paralellReplicate(nodes []*common.DataNodeInfo, req common.ReplicateChunkRequest, data []byte, requiredReplicas int) error {
	logger := logging.OperationLogger(rm.logger, "send_replicate_chunk", slog.String("chunk_id", req.ChunkID))

	if len(nodes) == 0 {
		return fmt.Errorf("no node endpoints provided")
	}

	// Create clients
	var clients []*DataNodeClient
	for _, node := range nodes {
		client, err := NewDataNodeClient(node)
		if err != nil {
			return fmt.Errorf("failed to create client for %s - [%s]  %v", node.ID, node.Endpoint(), err)
		}
		if client == nil {
			return fmt.Errorf("client for %v is nil", node)
		}
		clients = append(clients, client)
	}
	logger.Debug(fmt.Sprintf("Created connection to %d clients", len(clients)))

	// Clean up all clients
	defer func() {
		for _, client := range clients {
			client.Close()
		}
	}()

	var (
		wg            sync.WaitGroup
		acceptedCount atomic.Int64
		semaphore     = make(chan struct{}, requiredReplicas)
	)
	errChan := make(chan error, len(clients))

	for i, client := range clients {
		clientLogger := logging.ExtendLogger(logger, slog.String("client_id", client.Node.ID), slog.String("client_address", client.Node.Endpoint()))
		// Stop starting new goroutines if we already have enough replicas
		if int(acceptedCount.Load()) >= requiredReplicas {
			break
		}

		semaphore <- struct{}{} // Acquire slot (blocks if channel full)
		wg.Add(1)
		acceptedCount.Add(1)

		clientLogger.Debug("Replicating to client")
		go func(clientIndex int, c *DataNodeClient) {
			defer func() {
				<-semaphore // Release slot
				wg.Done()
			}()

			ctx, cancel := context.WithTimeout(context.Background(), rm.Config.ReplicateTimeout)
			defer cancel()

			if err := rm.replicate(ctx, c, req, data, clientLogger); err != nil {
				errChan <- fmt.Errorf("replication failed for client %d: %v", clientIndex, err)
				return
			}

			clientLogger.Debug("Replication succeeded")
		}(i, client)
	}

	wg.Wait()
	close(errChan)

	// Check if we achieved required replicas
	finalAccepted := acceptedCount.Load()
	if int(finalAccepted) < requiredReplicas {
		// Collect errors for debugging
		var errors []error
		for err := range errChan {
			errors = append(errors, err)
		}
		return fmt.Errorf("insufficient replicas: got %d, required %d. Errors: %v",
			finalAccepted, requiredReplicas, errors)
	}

	return nil
}

func (rm *ReplicationManager) replicate(ctx context.Context, client *DataNodeClient, req common.ReplicateChunkRequest, data []byte, clientLogger *slog.Logger) error {
	// Request replication session
	resp, err := client.ReplicateChunk(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to request replication: %v", err)
	}

	if !resp.Accept {
		return fmt.Errorf("replication request rejected: %s", resp.Message)
	}
	clientLogger.Debug("Replication request accepted")

	// Stream the chunk data
	streamLogger := logging.OperationLogger(clientLogger, "stream_chunk_data", slog.String("session_id", resp.SessionID))
	if err := rm.streamChunkData(ctx, client, resp.SessionID, req, data, streamLogger); err != nil {
		return fmt.Errorf("failed to stream chunk data: %v", err)
	}

	return nil
}

// This is the side that is responsible for sending the chunk data to the client
func (rm *ReplicationManager) streamChunkData(ctx context.Context, client *DataNodeClient, sessionID string,
	req common.ReplicateChunkRequest, data []byte, streamLogger *slog.Logger) error {

	streamLogger.Debug("Initializing chunk data stream")
	stream, err := client.StreamChunkData(ctx)
	if err != nil {
		return fmt.Errorf("failed to create stream: %v", err)
	}

	// Stream data in chunks
	chunkSize := rm.Config.ChunkStreamSize
	if chunkSize == 0 {
		chunkSize = 256 * 1024 // Default 64KB chunks
	}
	streamLogger.Debug("Chunk data stream created", slog.Int("chunk_size_KB", chunkSize/1024))

	totalSize := len(data)
	offset := 0
	hasher := sha256.New()
	iteration := 1 // just for logging purposes

	for offset < totalSize {
		// Calculate chunk size for this iteration
		remainingBytes := totalSize - offset
		currentChunkSize := chunkSize
		if remainingBytes < currentChunkSize {
			currentChunkSize = remainingBytes
		}

		// Extract chunk data
		chunkData := data[offset : offset+currentChunkSize]
		isFinal := (offset + currentChunkSize) >= totalSize

		// Calculate partial checksum for this chunk
		partialChecksum := common.CalculateChecksum(chunkData)
		hasher.Write(chunkData)

		// Retry logic for individual chunks
		var ack common.ChunkDataAck
		retryCount := 0

		// Retry the same chunk piece if it fails
		streamLogger.Debug("Sending chunk data", slog.Int("iteration", iteration), slog.Int("offset", offset), slog.Int("chunk_size", currentChunkSize))
		for retryCount < rm.Config.MaxChunkRetries {
			// Create stream message
			streamMsg := &common.ChunkDataStream{
				SessionID:       sessionID,
				ChunkID:         req.ChunkID,
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
				streamLogger.Debug("Failed to receive stream response", slog.Int("retry_count", retryCount), slog.String("error", err.Error()))
				retryCount++
				if retryCount >= rm.Config.MaxChunkRetries {
					return fmt.Errorf("failed to receive stream response after %d retries: %w", rm.Config.MaxChunkRetries, err)
				}
				time.Sleep(time.Duration(retryCount) * time.Second) // Exponential backoff
				continue
			}

			ack = common.ChunkDataAckFromProto(resp)

			if !ack.Success {
				streamLogger.Debug("Chunk data failed", slog.Int("retry_count", retryCount), slog.String("error", ack.Message))
				retryCount++
				if retryCount >= rm.Config.MaxChunkRetries {
					return fmt.Errorf("chunk failed after %d retries: %s", rm.Config.MaxChunkRetries, ack.Message)
				}
				time.Sleep(time.Duration(retryCount) * time.Second)
				continue
			}

			// Validate bytes received
			expectedBytes := offset + currentChunkSize
			if ack.BytesReceived != expectedBytes {
				streamLogger.Debug("Byte count mismatch", slog.Int("expected", expectedBytes), slog.Int("got", ack.BytesReceived))
				retryCount++
				if retryCount >= rm.Config.MaxChunkRetries {
					return fmt.Errorf("byte count mismatch after %d retries: expected %d, got %d",
						rm.Config.MaxChunkRetries, expectedBytes, ack.BytesReceived)
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
		offset += currentChunkSize

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
	finalAck := common.ChunkDataAckFromProto(finalResp)

	// Validate if checksum after all partial sections are added up still matches the original
	calculatedChecksum := fmt.Sprintf("%x", hasher.Sum(nil))
	if calculatedChecksum != req.Checksum {
		return fmt.Errorf("request checksum mismatch: expected %s, calculated %s",
			req.Checksum, calculatedChecksum)
	}

	if !finalAck.Success {
		return fmt.Errorf("stream failed: %s", finalAck.Message)
	}

	streamLogger.Info("Chunk data stream completed successfully")
	return nil
}
