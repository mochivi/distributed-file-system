package datanode

import (
	"bytes"
	"context"
	"hash"
	"sync"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

// Implements the proto.DataNodeServiceServer interface
type DataNodeServer struct {
	proto.UnimplementedDataNodeServiceServer

	store              storage.ChunkStorage
	replicationManager IReplicationManager
	sessionManager     ISessionManager

	config DataNodeConfig
}

// Wrapper over the proto.DataNodeServiceClient interface
type DataNodeClient struct {
	client proto.DataNodeServiceClient
	conn   *grpc.ClientConn
}

type NodeSelector interface {
	selectBestNodes(n int) []common.DataNodeInfo
}

type IReplicationManager interface {
	paralellReplicate(req common.ReplicateChunkRequest, data []byte, requiredReplicas int) error
	replicate(ctx context.Context, client *DataNodeClient, req common.ReplicateChunkRequest, data []byte) error
	streamChunkData(ctx context.Context, client *DataNodeClient, sessionID string, req common.ReplicateChunkRequest, data []byte) error
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

func NewDataNodeServer(store storage.ChunkStorage, replicationManager IReplicationManager) *DataNodeServer {
	return &DataNodeServer{
		store:              store,
		replicationManager: replicationManager,
	}
}

func NewDataNodeClient(serverAddress string) (*DataNodeClient, error) {
	conn, err := grpc.NewClient(
		serverAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()), // Update to TLS in prod
	)
	if err != nil {
		return nil, err
	}

	client := proto.NewDataNodeServiceClient(conn)

	return &DataNodeClient{
		client: client,
		conn:   conn,
	}, nil
}
