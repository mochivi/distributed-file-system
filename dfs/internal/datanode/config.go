package datanode

import (
	"time"

	"github.com/google/uuid"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/utils"
)

type DataNodeConfig struct {
	Info        common.DataNodeInfo
	Session     SessionManagerConfig
	Replication ReplicateManagerConfig
	DiskStorage chunk.DiskStorageConfig
}

type SessionManagerConfig struct {
	SessionTimeout time.Duration // timeout until chunk upload session times out
}

type ReplicateManagerConfig struct {
	ReplicateTimeout time.Duration // timeout until replication to another node is considered failed
	ChunkStreamSize  int
	MaxChunkRetries  int
}

func DefaultDatanodeConfig() DataNodeConfig {
	datanodeHost := utils.GetEnvString("DATANODE_HOST", "0.0.0.0")
	return DataNodeConfig{
		Info: common.DataNodeInfo{
			ID:        uuid.NewString(),
			IPAddress: datanodeHost,
			Port:      8081,
			Capacity:  10 * 1024 * 1024 * 1024, // gB
			Used:      0,
			Status:    common.NodeHealthy,
			LastSeen:  time.Now(),
		},

		Session: SessionManagerConfig{
			SessionTimeout: 1 * time.Minute,
		},

		Replication: ReplicateManagerConfig{
			ReplicateTimeout: 2 * time.Minute,
			ChunkStreamSize:  256 * 1024,
			MaxChunkRetries:  3,
		},
	}
}
