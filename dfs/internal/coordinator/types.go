package coordinator

import (
	"sync"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Implements proto.CoordinatorServiceServer interface
type Coordinator struct {
	proto.UnimplementedCoordinatorServiceServer // Embed

	// Coordinates data nodes access
	dataNodes  map[string]*common.DataNodeInfo
	nodesMutex sync.RWMutex

	// Coordinates metadata storage
	metaStore       storage.MetadataStore
	metadataManager *metadataManager // Coordinates when to actually commit metadata

	config CoordinatorConfig
}

func NewCoordinator(metaStore storage.MetadataStore, metadataManager *metadataManager, cfg CoordinatorConfig) *Coordinator {
	return &Coordinator{
		metaStore:       metaStore,
		dataNodes:       make(map[string]*common.DataNodeInfo),
		config:          cfg,
		metadataManager: metadataManager,
	}
}

// Wrapper over the proto.CoordinatorServiceClient interface
type CoordinatorClient struct {
	client proto.CoordinatorServiceClient
	conn   *grpc.ClientConn
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
