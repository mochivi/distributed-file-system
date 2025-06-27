package config

import (
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/utils"
)

type DataNodeConfig struct {
	Info        common.DataNodeInfo
	Session     SessionManagerConfig
	Replication ReplicateManagerConfig
	DiskStorage DiskStorageConfig
}

type SessionManagerConfig struct {
	SessionTimeout time.Duration // timeout until chunk upload session times out
}

type ReplicateManagerConfig struct {
	ReplicateTimeout time.Duration // timeout until replication to another node is considered failed
}

type DiskStorageConfig struct {
	Enabled bool
	Kind    string // block storage, etc..
	RootDir string // full path must be used
}

func DefaultDatanodeConfig() DataNodeConfig {
	datanodeHost := utils.GetEnvString("DATANODE_HOST", "0.0.0.0")
	baseDir := utils.GetEnvString("DISK_STORAGE_BASE_DIR", "/app")

	return DataNodeConfig{
		Info: common.DataNodeInfo{
			ID:       uuid.NewString(),
			Host:     datanodeHost,
			Port:     8081,
			Capacity: 10 * 1024 * 1024 * 1024, // gB
			Used:     0,
			Status:   common.NodeHealthy,
			LastSeen: time.Now(),
		},

		Session: SessionManagerConfig{
			SessionTimeout: 1 * time.Minute,
		},

		Replication: ReplicateManagerConfig{
			ReplicateTimeout: 10 * time.Minute,
		},

		DiskStorage: DiskStorageConfig{
			Enabled: true,
			Kind:    "block",
			RootDir: filepath.Join(baseDir, "data"),
		},
	}
}
