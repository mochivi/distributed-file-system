package config

import (
	"path/filepath"
	"time"

	"github.com/mochivi/distributed-file-system/pkg/utils"
)

// DatanodeAppConfig is the root configuration for the Datanode service.
type DatanodeAppConfig struct {
	Node  DataNodeConfig  `mapstructure:"node" validate:"required"`
	Agent NodeAgentConfig `mapstructure:"cluster" validate:"required"`
}

type DataNodeConfig struct {
	Session     SessionManagerConfig   `mapstructure:"session" validate:"required"`
	Replication ReplicateManagerConfig `mapstructure:"replication" validate:"required"`
	DiskStorage DiskStorageConfig      `mapstructure:"disk_storage" validate:"required"`
	Streamer    StreamerConfig         `mapstructure:"streamer" validate:"required"`
}

type SessionManagerConfig struct {
	SessionTimeout time.Duration `mapstructure:"session_timeout" validate:"required,gt=0"` // timeout until chunk upload session times out
}

func DefaultSessionManagerConfig() SessionManagerConfig {
	return SessionManagerConfig{
		SessionTimeout: 1 * time.Minute,
	}
}

type ReplicateManagerConfig struct {
	ReplicateTimeout time.Duration `mapstructure:"replicate_timeout" validate:"required,gt=0"` // timeout until replication to another node is considered failed
}

func DefaultReplicateManagerConfig() ReplicateManagerConfig {
	return ReplicateManagerConfig{
		ReplicateTimeout: 10 * time.Minute,
	}
}

type DiskStorageConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Kind    string `mapstructure:"kind" validate:"required"`     // block storage, etc..
	RootDir string `mapstructure:"root_dir" validate:"required"` // full path must be used
}

func DefaultDiskStorageConfig() DiskStorageConfig {
	baseDir := utils.GetEnvString("DISK_STORAGE_BASE_DIR", "/app")

	return DiskStorageConfig{
		Enabled: true,
		Kind:    "block",
		RootDir: filepath.Join(baseDir, "data"),
	}
}

type StreamerConfig struct {
	MaxChunkRetries  int
	ChunkStreamSize  int
	BackpressureTime time.Duration
	WaitReplicas     bool // Only used by client, waits for the final stream with the replicas information, default is false when used by datanodes
}

func DefaultStreamerConfig(waitReplicas bool) StreamerConfig {
	return StreamerConfig{
		MaxChunkRetries: 3,
		ChunkStreamSize: 256 * 1024,
		WaitReplicas:    waitReplicas,
	}
}

func DefaultDatanodeAppConfig() DatanodeAppConfig {
	return DatanodeAppConfig{
		Node: DataNodeConfig{
			Session:     DefaultSessionManagerConfig(),
			Replication: DefaultReplicateManagerConfig(),
			DiskStorage: DefaultDiskStorageConfig(),
			Streamer:    DefaultStreamerConfig(false),
		},
		Agent: NodeAgentConfig{
			Heartbeat: DefaultHeartbeatControllerConfig(),
		},
	}
}
