package datanode

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DataNodeServer struct {
	proto.UnimplementedDataNodeServiceServer

	store              storage.ChunkStorage
	replicationManager IReplicationManager
	sessionManager     ISessionManager

	config DataNodeConfig
}

func NewDataNodeServer(store storage.ChunkStorage, replicationManager IReplicationManager) *DataNodeServer {
	return &DataNodeServer{
		store:              store,
		replicationManager: replicationManager,
	}
}

// StoreChunk is received only if the node is a primary receiver
// 1. Verify chunk checksum is the same as expected
// 2. Store chunk, given storage method
// 3. Kickoff replication
func (s *DataNodeServer) StoreChunk(ctx context.Context, pb *proto.StoreChunkRequest) (*proto.StoreChunkResponse, error) {
	req := StoreChunkRequestFromProto(pb)

	if !common.VerifyChecksum(req.Data, req.Checksum) {
		return StoreChunkResponse{
			Success: false,
			Message: "checksum does not match",
		}.ToProto(), nil
	}

	if err := s.store.Store(req.ChunkID, req.Data); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to store chunk: %v", err)
	}

	if err := s.replicationManager.replicate(req.ChunkID, req.Data, 3); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to replicate chunk: %v", err)
	}

	return StoreChunkResponse{
		Success: true,
		Message: "chunk stored",
	}.ToProto(), nil
}

// Any node, replica or primary, can serve chunk reads at any moment
func (s *DataNodeServer) RetrieveChunk(ctx context.Context, pb *proto.RetrieveChunkRequest) (*proto.RetrieveChunkResponse, error) {
	req := RetrieveChunkRequestFromProto(pb)

	// Move to store layer later
	if !s.store.Exists(req.ChunkID) {
		return nil, status.Errorf(codes.NotFound, "file not found")
	}

	data, err := s.store.Retrieve(req.ChunkID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to retrieve chunk: %v", err)
	}

	return RetrieveChunkResponse{
		Data:     data,
		Checksum: common.CalculateChecksum(data),
	}.ToProto(), nil
}

func (s *DataNodeServer) DeleteChunk(ctx context.Context, pb *proto.DeleteChunkRequest) (*proto.DeleteChunkResponse, error) {
	req := DeleteChunkRequestFromProto(pb)

	if !s.store.Exists(req.ChunkID) {
		return DeleteChunkResponse{
			Success: false,
			Message: "chunk not found",
		}.ToProto(), nil
	}

	if err := s.store.Delete(req.ChunkID); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete chunk: %v", err)
	}

	return DeleteChunkResponse{
		Success: true,
		Message: "chunk deleted",
	}.ToProto(), nil
}

// ReplicateChunk means node is currenly acting as a replica for some chunk
// The replication implementation here is paralell, which means this node does not need to care about forwarding
// the replication request to some new node, just replying with the status is enough
// 1. Check if node has enough capacity to handle storing the chunk
// 2. Send accept response to innitiate data stream for chunk
func (s *DataNodeServer) ReplicateChunk(ctx context.Context, pb *proto.ReplicateChunkRequest) (*proto.ReplicateChunkResponse, error) {
	req := ReplicateChunkRequestFromProto(pb)

	if req.ChunkID == "" || req.ChunkSize <= 0 {
		return ReplicateChunkResponse{
			Accept:  false,
			Message: "Invalid chunk metadata",
		}.ToProto(), nil
	}

	if !s.hasCapacity(req.ChunkSize) {
		return &proto.ReplicateChunkResponse{
			Accept:  false,
			Message: "Insufficient storage capacity",
		}, nil
	}

	sessionID := uuid.NewString()
	s.createStreamingSession(sessionID, req)

	return ReplicateChunkResponse{
		Accept:    true,
		Message:   "Ready to receive chunk data",
		SessionID: sessionID,
	}.ToProto(), nil
}

func (s *DataNodeServer) StreamChunkData(stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) error {
	var session *StreamingSession
	var buffer *bytes.Buffer
	var totalReceived int

	for {
		chunkpb, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Convert to internal struct
		chunk := ChunkDataStreamFromProto(chunkpb)

		// First chunk establishes session
		if session == nil {
			session = s.getStreamingSession(chunk.SessionID)
			if session == nil {
				return errors.New("invalid session")
			}

			// Create buffer with pre-defined capacity for performance
			buf := make([]byte, 0, session.ExpectedSize)
			buffer = bytes.NewBuffer(buf)
		}

		// Verify data integrity and ordering
		if chunk.Offset != totalReceived {
			return errors.New("data out of order")
		}

		// Write to buffer/temp file, update checksum
		buffer.Write(chunk.Data)
		session.Checksum.Write(chunk.Data)
		totalReceived += len(chunk.Data)

		// TODO: implement flushing buffer to disk whenever it hits the threshold
		// TODO: must setup temporary state materialization in some special folder
		// TODO: Ideally, we should avoid flushing to disk if there is enough available RAM
		// Flush to disk if buffer gets too big
		// if buffer.Len() >= int(s.config.FlushSize) {
		// 	if err := s.flushBufferToDisk(session, &buffer); err != nil {
		// 		return err
		// 	}
		// }

		// // Flow control decision
		// readyForNext := buffer.Len() < int(s.config.BufferThreshold)

		// Send acknowledgment with flow control
		ack := &ChunkDataAck{
			SessionID:     chunk.SessionID,
			Success:       true,
			BytesReceived: totalReceived,
			ReadyForNext:  true, // Requires flushing to be setup to work
		}

		if err := stream.Send(ack.ToProto()); err != nil {
			return err
		}

		// Handle final chunk
		if chunk.IsFinal {
			// Verify checksum immediately
			computedHash := session.Checksum.Sum(nil)
			expectedHash, _ := hex.DecodeString(session.ExpectedHash)

			if !bytes.Equal(computedHash, expectedHash) {
				return errors.New("checksum mismatch")
			}

			// Store the chunk immediately
			err := s.store.Store(session.ChunkID, buffer.Bytes())
			if err != nil {
				return err
			}

			// Send final ack with verification result
			finalAck := ChunkDataAck{
				SessionID:     chunk.SessionID,
				Success:       true,
				Message:       "Chunk stored successfully",
				BytesReceived: totalReceived,
			}
			return stream.Send(finalAck.ToProto())
		}
	}

	return nil
}

func (s *DataNodeServer) HealthCheck(ctx context.Context, pb *proto.HealthCheckRequest) (*proto.HealthCheckResponse, error) {
	return nil, nil
}

// Helper functions

func (s *DataNodeServer) hasCapacity(chunkSize int) bool {
	return true
}
