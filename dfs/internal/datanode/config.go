package datanode

import (
	"time"

	"github.com/google/uuid"
	"github.com/mochivi/distributed-file-system/internal/common"
)

type DataNodeConfig struct {
	Info        common.DataNodeInfo
	Session     SessionManagerConfig
	Replication ReplicateManagerConfig
}

type SessionManagerConfig struct {
	SessionTimeout time.Duration // timeout until chunk upload session times out
}

type ReplicateManagerConfig struct {
	ReplicateTimeout time.Duration // timeout until replication to another node is considered failed
	ChunkStreamSize  int           // kB
	MaxChunkRetries  int
}

func DefaultDatanodeConfig() DataNodeConfig {
	return DataNodeConfig{
		Info: common.DataNodeInfo{
			ID:        uuid.NewString(),
			IPAddress: "0.0.0.0",
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
			ReplicateTimeout: 30 * time.Second,
			ChunkStreamSize:  64,
			MaxChunkRetries:  3,
		},
	}
}
