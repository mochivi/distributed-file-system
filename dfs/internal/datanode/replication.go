package datanode

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
)

type ReplicationManager struct {
	Config       ReplicateManagerConfig
	NodeSelector common.NodeSelector
}

func NewReplicationManager(config ReplicateManagerConfig, nodeSelector common.NodeSelector) *ReplicationManager {
	return &ReplicationManager{
		Config:       config,
		NodeSelector: nodeSelector,
	}
}

func (rm *ReplicationManager) paralellReplicate(req common.ReplicateChunkRequest, data []byte, requiredReplicas int) error {
	nodes := rm.NodeSelector.SelectBestNodes(requiredReplicas + 3)
	if len(nodes) == 0 {
		return fmt.Errorf("no node endpoints provided")
	}

	// Create clients
	var clients []*DataNodeClient
	var clientsMutex sync.Mutex
	defer func() {
		// Clean up all clients
		clientsMutex.Lock()
		for _, client := range clients {
			client.Close()
		}
		clientsMutex.Unlock()
	}()

	for _, node := range nodes {
		endpoint := fmt.Sprintf("%s:%d", node.IPAddress, node.Port)
		client, err := NewDataNodeClient(endpoint)
		if err != nil {
			return fmt.Errorf("failed to create client for %s: %v", endpoint, err)
		}
		if client == nil {
			return fmt.Errorf("client for %s is nil", endpoint)
		}

		clientsMutex.Lock()
		clients = append(clients, client)
		clientsMutex.Unlock()
	}

	var wg sync.WaitGroup
	errChan := make(chan error, len(clients))
	var acceptedCount atomic.Int64

	for i, rangeClient := range clients {
		wg.Add(1)
		go func(clientIndex int, c *DataNodeClient) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), rm.Config.ReplicateTimeout)
			defer cancel()

			if err := rm.replicate(ctx, c, req, data); err != nil {
				errChan <- fmt.Errorf("replication failed for client %d: %v", clientIndex, err)
				return
			}

			acceptedCount.Add(1)
		}(i, rangeClient)
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

func (rm *ReplicationManager) replicate(ctx context.Context, client *DataNodeClient, req common.ReplicateChunkRequest, data []byte) error {
	// Request replication session
	resp, err := client.ReplicateChunk(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to request replication: %v", err)
	}

	if !resp.Accept {
		return fmt.Errorf("replication request rejected: %s", resp.Message)
	}

	// Stream the chunk data
	if err := rm.streamChunkData(ctx, client, resp.SessionID, req, data); err != nil {
		return fmt.Errorf("failed to stream chunk data: %v", err)
	}

	return nil
}

func (rm *ReplicationManager) streamChunkData(ctx context.Context, client *DataNodeClient, sessionID string, req common.ReplicateChunkRequest, data []byte) error {
	stream, err := client.StreamChunkData(ctx)
	if err != nil {
		return fmt.Errorf("failed to create stream: %v", err)
	}

	// Stream data in chunks
	chunkSize := rm.Config.ChunkStreamSize
	if chunkSize == 0 {
		chunkSize = 64 * 1024 // Default 64KB chunks
	}

	totalSize := len(data)
	offset := 0
	hasher := sha256.New()

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

		for retryCount < rm.Config.MaxChunkRetries {
			// Create stream message
			streamMsg := &common.ChunkDataStream{
				SessionID:       sessionID,
				ChunkID:         req.ChunkID,
				Data:            chunkData,
				Offset:          int(offset),
				IsFinal:         isFinal,
				PartialChecksum: partialChecksum,
			}

			// Send chunk
			if err := stream.Send(streamMsg.ToProto()); err != nil {
				return fmt.Errorf("failed to send chunk at offset %d: %w", offset, err)
			}

			// Wait for acknowledgment
			resp, err := stream.Recv()
			if err != nil {
				retryCount++
				if retryCount >= rm.Config.MaxChunkRetries {
					return fmt.Errorf("failed to receive stream response after %d retries: %w", rm.Config.MaxChunkRetries, err)
				}
				time.Sleep(time.Duration(retryCount) * time.Second) // Exponential backoff
				continue
			}

			ack := common.ChunkDataAckFromProto(resp)

			if !ack.Success {
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

	// Update with server side calculated final checksum return
	PLACEHOLDER_checksum := "placeholder"
	calculatedChecksum := fmt.Sprintf("%x", hasher.Sum(nil))
	if PLACEHOLDER_checksum != calculatedChecksum {
		return fmt.Errorf("final checksum mismatch: expected %s, calculated %s",
			PLACEHOLDER_checksum, calculatedChecksum)
	}

	// Validate against original checksum
	if req.Checksum != "" && req.Checksum != calculatedChecksum {
		return fmt.Errorf("request checksum mismatch: expected %s, calculated %s",
			req.Checksum, calculatedChecksum)
	}

	if !finalAck.Success {
		return fmt.Errorf("stream failed: %s", finalAck.Message)
	}

	return nil
}
