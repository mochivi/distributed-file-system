package streaming

import (
	"bytes"
	"context"
	"crypto/sha256"
	"hash"
	"log/slog"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
)

/* Client side streamer */

type ClientStreamer interface {
	SendChunkStream(ctx context.Context, stream grpc.BidiStreamingClient[proto.ChunkDataStream, proto.ChunkDataAck],
		logger *slog.Logger, params UploadChunkStreamParams) ([]*common.NodeInfo, error)
	ReceiveChunkStream(ctx context.Context, stream grpc.ServerStreamingClient[proto.ChunkDataStream],
		buffer *bytes.Buffer, logger *slog.Logger, params DownloadChunkStreamParams) error
	Config() *config.StreamerConfig
}

type clientStreamer struct {
	config *config.StreamerConfig
}

func (c *clientStreamer) Config() *config.StreamerConfig {
	return c.config
}

type UploadChunkStreamParams struct {
	// Retrieve session
	SessionID common.StreamingSessionID

	// Chunk data
	ChunkHeader common.ChunkHeader
	Data        []byte
}

type DownloadChunkStreamParams struct {
	SessionID   common.StreamingSessionID
	ChunkHeader common.ChunkHeader
}

func NewClientStreamer(config config.StreamerConfig) *clientStreamer {
	return &clientStreamer{config: &config}
}

/* Server side streamer */

// Factory for server streamer
type ServerStreamerFactory func(sessionManager SessionManager, config config.StreamerConfig) ServerStreamer

// ServerStreamer defines the interface for chunk streaming
type ServerStreamer interface {

	// Upload side -- receiving chunks
	HandleFirstChunkFrame(stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) (*streamingSession, error)
	ReceiveChunkFrames(session *streamingSession, stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) ([]byte, error)
	SendFinalAck(sessionID common.StreamingSessionID, bytesReceived int, stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) error
	SendFinalReplicasAck(session *streamingSession, replicaNodes []*common.NodeInfo, stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) error

	// Download side - sending chunks
}

// serverStreamer implements the ServerStreamer interface
// is used to send and receive chunk streams by the datanode
type serverStreamer struct {
	sessionManager SessionManager
	config         config.StreamerConfig
}

func NewServerStreamer(sessionManager SessionManager, config config.StreamerConfig) *serverStreamer {
	return &serverStreamer{
		sessionManager: sessionManager,
		config:         config,
	}
}

// SessionManager is used by the datanode to control currently active sessions
type SessionManager interface {
	NewSession(ctx context.Context, chunkHeader common.ChunkHeader, propagate bool) *streamingSession
	GetSession(sessionID common.StreamingSessionID) (*streamingSession, error)
	Store(sessionID common.StreamingSessionID, session *streamingSession) error
	Load(sessionID common.StreamingSessionID) (*streamingSession, error)
	Delete(sessionID common.StreamingSessionID)
	ExistsForChunk(chunkID string) bool
	LoadByChunk(chunkID string) (*streamingSession, error)
}

type SessionStatus int

func (s SessionStatus) IsValid() bool {
	switch s {
	case SessionCompleted, SessionFailed:
		return false
	default:
		return true
	}
}

const (
	SessionCreated SessionStatus = iota
	SessionActive
	SessionCompleted
	SessionFailed
)

// streamingSession controls the data flow during a chunk streaming session
// It could be broken down into uploadStreamingSession and downloadStreamingSession
// But this unifying approach works for now, even if the download session doesn't use all fields
type streamingSession struct {
	ctx context.Context

	SessionID common.StreamingSessionID
	CreatedAt time.Time
	ExpiresAt time.Time

	// Chunk metadata and replication identifier
	common.ChunkHeader
	Propagate bool

	// Runtime state
	buffer          *bytes.Buffer
	offset          int
	runningChecksum hash.Hash // Running checksum calculation

	// mu     sync.RWMutex
	Status SessionStatus
}

func NewStreamingSession(ctx context.Context, sessionID common.StreamingSessionID, chunkHeader common.ChunkHeader, propagate bool) *streamingSession {
	return &streamingSession{
		ctx:       ctx,
		SessionID: sessionID,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(1 * time.Minute),

		ChunkHeader: chunkHeader,
		Propagate:   propagate,

		buffer:          nil, // buffer is created when the session is started, based on the chunk size
		offset:          0,
		runningChecksum: sha256.New(),

		Status: SessionCreated,
	}
}
