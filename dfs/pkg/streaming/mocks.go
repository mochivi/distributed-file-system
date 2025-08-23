package streaming

import (
	"bytes"
	"context"
	"log/slog"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

type MockServerStreamer struct {
	mock.Mock
}

func (m *MockServerStreamer) HandleFirstChunkFrame(stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) (*streamingSession, error) {
	args := m.Called(stream)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*streamingSession), args.Error(1)
}

func (m *MockServerStreamer) Config() config.StreamerConfig {
	args := m.Called()
	return args.Get(0).(config.StreamerConfig)
}

func (m *MockServerStreamer) ReceiveChunkFrames(session *streamingSession, stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) ([]byte, error) {
	args := m.Called(session, stream)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockServerStreamer) SendFinalAck(sessionID common.StreamingSessionID, bytesReceived int, stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) error {
	args := m.Called(sessionID, bytesReceived, stream)
	return args.Error(0)
}

func (m *MockServerStreamer) SendFinalReplicasAck(session *streamingSession, replicaNodes []*common.NodeInfo, stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) error {
	args := m.Called(session, replicaNodes, stream)
	return args.Error(0)
}

type MockClientStreamer struct {
	mock.Mock
}

func (m *MockClientStreamer) SendChunkStream(ctx context.Context, stream grpc.BidiStreamingClient[proto.ChunkDataStream, proto.ChunkDataAck],
	logger *slog.Logger, params UploadChunkStreamParams) ([]*common.NodeInfo, error) {
	args := m.Called(ctx, stream, logger, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*common.NodeInfo), args.Error(1)
}

func (m *MockClientStreamer) ReceiveChunkStream(ctx context.Context, stream grpc.ServerStreamingClient[proto.ChunkDataStream],
	buffer *bytes.Buffer, logger *slog.Logger, params DownloadChunkStreamParams) error {
	args := m.Called(ctx, stream, buffer, logger, params)
	return args.Error(0)
}

func (m *MockClientStreamer) Config() *config.StreamerConfig {
	args := m.Called()
	return args.Get(0).(*config.StreamerConfig)
}

type MockStreamingSessionManager struct {
	mock.Mock
}

func (m *MockStreamingSessionManager) NewSession(ctx context.Context, chunkHeader common.ChunkHeader, propagate bool) *streamingSession {
	args := m.Called(ctx, chunkHeader, propagate)
	return args.Get(0).(*streamingSession)
}

func (m *MockStreamingSessionManager) GetSession(sessionID common.StreamingSessionID) (*streamingSession, error) {
	args := m.Called(sessionID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*streamingSession), args.Error(1)
}

func (m *MockStreamingSessionManager) Store(sessionID common.StreamingSessionID, session *streamingSession) error {
	args := m.Called(sessionID, session)
	return args.Error(0)
}

func (m *MockStreamingSessionManager) Load(sessionID common.StreamingSessionID) (*streamingSession, error) {
	args := m.Called(sessionID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*streamingSession), args.Error(1)
}

func (m *MockStreamingSessionManager) Delete(sessionID common.StreamingSessionID) {
	m.Called(sessionID)
}

func (m *MockStreamingSessionManager) ExistsForChunk(chunkID string) bool {
	args := m.Called(chunkID)
	return args.Bool(0)
}

func (m *MockStreamingSessionManager) LoadByChunk(chunkID string) (*streamingSession, error) {
	args := m.Called(chunkID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*streamingSession), args.Error(1)
}
