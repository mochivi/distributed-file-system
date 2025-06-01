package datanode

import (
	"bytes"
	"context"
	"hash"
	"sync"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
)

const (
	DEFAULT_REPLICAS = 3
)

type SessionStatus int

const (
	SessionActive SessionStatus = iota
	SessionCompleted
	SessionFailed
	SessionExpired
)

type DataNodeService interface {
	// Chunk operations
	StoreChunk(ctx context.Context, chunkID string, data []byte) error
	RetrieveChunk(ctx context.Context, chunkID string) ([]byte, error)
	DeleteChunk(ctx context.Context, chunkID string) error

	// Replication
	ReplicateChunk(ctx context.Context, chunkID string, sourceNode string) error
}

type NodeSelector interface {
	selectBestNodes(n int) []common.DataNodeInfo
}

type IReplicationManager interface {
	replicate(chunkID string, data []byte, requiredReplicas int) error
}

type ISessionManager interface {
	Store(sessionID string, session *StreamingSession)
	Load(sessionID string) (*StreamingSession, bool)
	Delete(sessionID string)
}

// StreamingSession controls the data flow during a chunk streaming session
type StreamingSession struct {
	SessionID    string
	ChunkID      string
	ExpectedSize int
	ExpectedHash string
	CreatedAt    time.Time
	ExpiresAt    time.Time

	// Runtime state
	BytesReceived int64
	Buffer        *bytes.Buffer
	Checksum      hash.Hash // Running checksum calculation

	// Concurrency control
	mutex  sync.RWMutex
	Status SessionStatus
}
