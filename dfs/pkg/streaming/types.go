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
	SessionID string

	// Chunk data
	ChunkHeader common.ChunkHeader
	Data        []byte
}

type DownloadChunkStreamParams struct {
	SessionID   string
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
	HandleFirstChunk(stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) (*streamingSession, error)
	ReceiveChunks(session *streamingSession, stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) ([]byte, error)
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
	NewSession(chunkHeader common.ChunkHeader, propagate bool) *streamingSession
	GetSession(sessionID string) (*streamingSession, bool)
	Store(sessionID string, session *streamingSession) error
	Load(sessionID string) (*streamingSession, bool)
	Delete(sessionID string)
	ExistsForChunk(chunkID string) bool
	LoadByChunk(chunkID string) (*streamingSession, bool)
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
	SessionID string
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

func NewStreamingSession(sessionID string, chunkHeader common.ChunkHeader, propagate bool) *streamingSession {
	return &streamingSession{
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
