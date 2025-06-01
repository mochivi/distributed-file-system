package coordinator

import (
	"sync"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage"
	"github.com/mochivi/distributed-file-system/pkg/proto"
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

func NewCoordinator(metaStore storage.MetadataStore, metadataManager *metadataManager, replication int) *Coordinator {
	return &Coordinator{
		metaStore:       metaStore,
		dataNodes:       make(map[string]*common.DataNodeInfo),
		replication:     replication,
		metadataManager: metadataManager,
	}
}

// ChunkLocation represents where some chunk should be stored (primary node + endpoint)
type ChunkLocation struct {
	ChunkID  string
	NodeID   string
	Endpoint string
}

func NewChunkLocationFromProto(pb *proto.ChunkLocation) ChunkLocation {
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
