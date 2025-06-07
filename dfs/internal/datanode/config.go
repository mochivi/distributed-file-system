package datanode

import (
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
)

type DataNodeConfig struct {
	Info        common.DataNodeInfo
	Coordinator struct { // This is a temporary struct, it represents where the data node can find the coordinator node (hardcoded for nwo)
		Host string
		Port int
	}
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
			ID:        "", // ID is only set after registering with coordinator
			IPAddress: "127.0.0.1",
			Port:      8081,
			Capacity:  10 * 1024 * 1024 * 1024, // gB
			Used:      0,
			Status:    common.NodeHealthy,
			LastSeen:  time.Now(),
		},

		Coordinator: struct {
			Host string
			Port int
		}{
			Host: "localhost",
			Port: 8080,
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
