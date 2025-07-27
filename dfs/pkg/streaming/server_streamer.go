package streaming

import (
	"errors"
	"fmt"
	"io"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *serverStreamer) HandleFirstChunk(stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) (*streamingSession, error) {
	chunkpb, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("failed to receive chunk data: %w", err) // code internal
	}

	chunk := common.ChunkDataStreamFromProto(chunkpb)

	session, exists := s.sessionManager.GetSession(chunk.SessionID)
	if !exists {
		return nil, fmt.Errorf("invalid session") // code not found
	}

	// start streaming session and write first chunk
	session.startStreamingSession()
	if err := session.write(chunk); err != nil {
		return nil, fmt.Errorf("failed to write chunk: %w", err) // code internal
	}

	// Send first ack
	if err := s.sendAck(chunk, session.offset, stream); err != nil {
		return nil, err // code internal
	}

	// If the first chunk is the final chunk, handle it and return early
	if chunk.IsFinal {
		if err := s.handleFinalChunk(session); err != nil {
			return nil, err
		}
		if err := s.sendFinalAck(chunk, session.offset, stream); err != nil {
			return nil, err
		}
		return session, nil // No more chunks will arrive, but UploadChunkStream still needs the session data, so just return normally.
	}

	return session, nil
}

func (s *serverStreamer) ReceiveChunks(session *streamingSession,
	stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) ([]byte, error) {

	for {
		chunkpb, err := stream.Recv()

		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to receive chunk stream: %w", err)
		}

		chunk := common.ChunkDataStreamFromProto(chunkpb)

		if err := session.write(chunk); err != nil {
			return nil, fmt.Errorf("failed to write chunk: %w", err) // code internal
		}

		if err := s.sendAck(chunk, session.offset, stream); err != nil {
			return nil, err // todo
		}

		if chunk.IsFinal {
			if err := s.handleFinalChunk(session); err != nil {
				return nil, err // todo
			}
			if err := s.sendFinalAck(chunk, session.offset, stream); err != nil {
				return nil, err // todo
			}
			break
		}
	}

	return session.buffer.Bytes(), nil
}

func (s *serverStreamer) sendAck(chunk common.ChunkDataStream, bytesReceived int,
	stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) error {

	ack := &common.ChunkDataAck{
		SessionID:     chunk.SessionID,
		Success:       true,
		BytesReceived: bytesReceived,
		ReadyForNext:  true, // TODO: Requires flushing to be setup to work
	}

	if err := stream.Send(ack.ToProto()); err != nil {
		return fmt.Errorf("failed to send acknowledgment: %w", err) // code internal
	}

	return nil
}

func (s *serverStreamer) handleFinalChunk(session *streamingSession) error {
	err := session.finalizeSession()
	if err != nil {
		return err // code internal
	}

	return nil
}

func (s *serverStreamer) sendFinalAck(chunk common.ChunkDataStream, bytesReceived int,
	stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) error {

	finalAck := common.ChunkDataAck{
		SessionID:     chunk.SessionID,
		Success:       true,
		Message:       "Chunk received successfully",
		BytesReceived: bytesReceived,
	}

	if err := stream.Send(finalAck.ToProto()); err != nil {
		return fmt.Errorf("failed to send acknowledgment: %w", err)
	}

	return nil
}

// If receiving node is primary, it will replicate to other nodes before sending final acknowledgement
func (s *serverStreamer) SendFinalReplicasAck(session *streamingSession, replicaNodes []*common.NodeInfo,
	stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) error {

	finalReplicasAck := common.ChunkDataAck{
		SessionID: session.SessionID,
		Success:   true,
		Message:   "Chunk replicated successfully",
		Replicas:  replicaNodes,
	}

	if err := stream.Send(finalReplicasAck.ToProto()); err != nil {
		return status.Errorf(codes.Internal, "failed to send chunk data ack with replicas: %v", err)
	}
	return nil
}
