package datanode

import (
	"context"

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

	Config DataNodeConfig
}

// Wrapper over the proto.DataNodeServiceClient interface
type DataNodeClient struct {
	client  proto.DataNodeServiceClient
	conn    *grpc.ClientConn
	address string
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

func NewDataNodeServer(store storage.ChunkStorage, replicationManager IReplicationManager, sessionManager ISessionManager,
	config DataNodeConfig) *DataNodeServer {
	return &DataNodeServer{
		store:              store,
		replicationManager: replicationManager,
		sessionManager:     sessionManager,
		Config:             config,
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
		client:  client,
		conn:    conn,
		address: serverAddress,
	}, nil
}
