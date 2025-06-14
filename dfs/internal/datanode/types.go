package datanode

import (
	"context"
	"log/slog"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage"
	"github.com/mochivi/distributed-file-system/pkg/logging"
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
	nodeManager        *common.NodeManager

	Config DataNodeConfig

	logger *slog.Logger
}

// Wrapper over the proto.DataNodeServiceClient interface
type DataNodeClient struct {
	client proto.DataNodeServiceClient
	conn   *grpc.ClientConn
	Node   *common.DataNodeInfo
}

type IReplicationManager interface {
	paralellReplicate(nodes []*common.DataNodeInfo, chunkMeta common.ChunkMeta, data []byte, requiredReplicas int) error
	replicate(ctx context.Context, client *DataNodeClient, req common.ChunkMeta, data []byte, clientLogger *slog.Logger) error
}

type ISessionManager interface {
	Store(sessionID string, session *StreamingSession) error
	Load(sessionID string) (*StreamingSession, bool)
	Delete(sessionID string)
	ExistsForChunk(chunkID string) bool
}

func NewDataNodeServer(store storage.ChunkStorage, replicationManager IReplicationManager, sessionManager ISessionManager,
	nodeManager *common.NodeManager, config DataNodeConfig, logger *slog.Logger) *DataNodeServer {
	datanodeLogger := logging.ExtendLogger(logger, slog.String("component", "datanode_server"))
	return &DataNodeServer{
		store:              store,
		replicationManager: replicationManager,
		sessionManager:     sessionManager,
		nodeManager:        nodeManager,
		Config:             config,
		logger:             datanodeLogger,
	}
}

func NewDataNodeClient(node *common.DataNodeInfo) (*DataNodeClient, error) {
	conn, err := grpc.NewClient(
		node.Endpoint(),
		grpc.WithTransportCredentials(insecure.NewCredentials()), // Update to TLS in prod
	)
	if err != nil {
		return nil, err
	}

	client := proto.NewDataNodeServiceClient(conn)

	return &DataNodeClient{
		client: client,
		conn:   conn,
		Node:   node,
	}, nil
}
