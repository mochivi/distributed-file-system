package streaming

import (
	"errors"
	"fmt"
	"io"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
)

// ------ Upload - data flowing from client ------

func (s *serverStreamer) HandleFirstChunkFrame(stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) (*streamingSession, error) {
	chunkpb, err := stream.Recv()
	if err != nil {
		return nil, NewChunkFrameReadError("", chunkpb.ChunkId, chunkpb.Offset, err)
	}

	chunk := common.ChunkDataStreamFromProto(chunkpb)

	session, err := s.sessionManager.GetSession(chunk.SessionID)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, chunk.SessionID)
	}

	// start streaming session and write first chunk
	session.startStreamingSession()
	if err := session.write(chunk); err != nil {
		return nil, NewChunkFrameWriteError("", chunk.ChunkID, chunk.Offset, err)
	}

	// Send first ack
	if err := s.sendAck(chunk, session.offset, stream); err != nil {
		return nil, err
	}

	// If the first chunk is the final chunk, handle it and return early.
	// The definitive final acknowledgement will be sent by the caller (e.g., the datanode server)
	// once the chunk has been durably stored (and replicated if required).
	if chunk.IsFinal {
		if err := s.handleFinalChunk(session); err != nil {
			return nil, err
		}
		return session, nil // caller will send final ack
	}

	return session, nil
}

func (s *serverStreamer) ReceiveChunkFrames(session *streamingSession,
	stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) ([]byte, error) {

	for {
		chunkpb, err := stream.Recv()

		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, NewStreamReceiveError(session.SessionID, session.offset, err)
		}

		chunk := common.ChunkDataStreamFromProto(chunkpb)

		if err := session.write(chunk); err != nil {
			return nil, NewChunkFrameWriteError(session.SessionID, chunk.ChunkID, chunk.Offset, err)
		}

		if err := s.sendAck(chunk, session.offset, stream); err != nil {
			return nil, err
		}

		if chunk.IsFinal {
			if err := s.handleFinalChunk(session); err != nil {
				return nil, err
			}
			break // caller will send final ack later
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
		return NewStreamSendError(chunk.SessionID, chunk.ChunkID, int64(chunk.Offset), err)
	}

	return nil
}

func (s *serverStreamer) handleFinalChunk(session *streamingSession) error {
	err := session.finalizeSession()
	if err != nil {
		return err // checksum did not match at the end
	}
	return nil
}

func (s *serverStreamer) SendFinalAck(sessionID string, bytesReceived int,
	stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) error {

	finalAck := common.ChunkDataAck{
		SessionID:     sessionID,
		Success:       true,
		Message:       "Chunk received successfully",
		BytesReceived: bytesReceived,
	}

	if err := stream.Send(finalAck.ToProto()); err != nil {
		return fmt.Errorf("%w: %s", ErrAckSendFailed, sessionID)
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
		return fmt.Errorf("%w: %s", ErrAckSendFailed, session.SessionID)
	}
	return nil
}

// ------ Download - data flowing to client ------

// SendChunkFrames is a pure function, as it requires no dependencies
// so, we keep it simple
func SendChunkFrames(reader io.Reader, streamFrameSize int, chunkID, sessionID string, totalSize int,
	stream grpc.ServerStreamingServer[proto.ChunkDataStream]) error {
	buffer := make([]byte, streamFrameSize)
	offset := int64(0)

	for {
		n, err := reader.Read(buffer)
		if err == io.EOF {
			break // We've sent the entire file.
		}
		if err != nil {
			return NewChunkFrameReadError(sessionID, chunkID, offset, err)
		}

		if err := stream.Send(&proto.ChunkDataStream{
			SessionId: sessionID,
			ChunkId:   chunkID,
			Data:      buffer[:n],
			Offset:    offset,
			IsFinal:   offset+int64(n) >= int64(totalSize),
		}); err != nil {
			return NewStreamSendError(sessionID, chunkID, offset, err)
		}

		offset += int64(n)
	}

	return nil
}
