package coordinator

import (
	"sync"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const CHUNK_SIZE = 8 // 8MB default chunk size

// Implements proto.CoordinatorServiceServer interface
type Coordinator struct {
	proto.UnimplementedCoordinatorServiceServer // Embed

	metaStore   storage.MetadataStore
	dataNodes   map[string]*common.DataNodeInfo
	nodesMutex  sync.RWMutex
	replication int

	metadataManager *metadataManager
}

// Wrapper over the proto.CoordinatorServiceClient interface
type CoordinatorClient struct {
	client proto.CoordinatorServiceClient
	conn   *grpc.ClientConn
}

type metadataManager struct {
	sessions      map[string]metadataUploadSession
	commitTimeout time.Duration
}
type metadataUploadSession struct {
	id       string
	exp      time.Time
	fileInfo *common.FileInfo
}

// ChunkLocation represents where some chunk should be stored (primary node + endpoint)
type ChunkLocation struct {
	ChunkID  string
	NodeID   string
	Endpoint string
}

func ChunkLocationFromProto(pb *proto.ChunkLocation) ChunkLocation {
	return ChunkLocation{
		ChunkID:  pb.ChunkId,
		NodeID:   pb.NodeId,
		Endpoint: pb.Endpoint,
	}
}

func (cs *ChunkLocation) ToProto() *proto.ChunkLocation {
	return &proto.ChunkLocation{
		ChunkId:  cs.ChunkID,
		NodeId:   cs.NodeID,
		Endpoint: cs.Endpoint,
	}
}

func NewCoordinator(metaStore storage.MetadataStore, metadataManager *metadataManager, replication int) *Coordinator {
	return &Coordinator{
		metaStore:       metaStore,
		dataNodes:       make(map[string]*common.DataNodeInfo),
		replication:     replication,
		metadataManager: metadataManager,
	}
}

func NewCoordinatorClient(serverAddress string) (*CoordinatorClient, error) {
	conn, err := grpc.NewClient(
		serverAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()), // Update to TLS in prod
	)
	if err != nil {
		return nil, err
	}

	client := proto.NewCoordinatorServiceClient(conn)

	return &CoordinatorClient{
		client: client,
		conn:   conn,
	}, nil
}

func NewMetadataManager(commitTimeout int) *metadataManager {
	manager := &metadataManager{
		sessions:      make(map[string]metadataUploadSession),
		commitTimeout: time.Duration(commitTimeout),
	}

	return manager
}

func newMetadataUploadSession(sessionID string, exp time.Duration, fileInfo *common.FileInfo) metadataUploadSession {
	return metadataUploadSession{
		id:       sessionID,
		exp:      time.Now().Add(exp),
		fileInfo: fileInfo,
	}
}
