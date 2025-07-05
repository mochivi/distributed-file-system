package config

import (
	"path/filepath"
	"time"

	"github.com/mochivi/distributed-file-system/pkg/utils"
)

// DatanodeAppConfig is the root configuration for the Datanode service.
type DatanodeAppConfig struct {
	Node  DataNodeConfig      `mapstructure:"node" validate:"required"`
	Agent DatanodeAgentConfig `mapstructure:"agent" validate:"required"`
}

func DefaultDatanodeAppConfig() DatanodeAppConfig {
	return DatanodeAppConfig{
		Node:  DefaultDataNodeConfig(),
		Agent: DefaultDatanodeAgentConfig(),
	}
}

type DataNodeConfig struct {
	StreamingSession StreamingSessionManagerConfig    `mapstructure:"streaming_session" validate:"required"`
	Replication      ParallelReplicationServiceConfig `mapstructure:"replication" validate:"required"`
	DiskStorage      DiskStorageConfig                `mapstructure:"disk_storage" validate:"required"`
	Streamer         StreamerConfig                   `mapstructure:"streamer" validate:"required"`
}

func DefaultDataNodeConfig() DataNodeConfig {
	return DataNodeConfig{
		StreamingSession: DefaultStreamingSessionManagerConfig(),
		Replication:      DefaultParallelReplicationServiceConfig(),
		DiskStorage:      DefaultDiskStorageConfig(),
		Streamer:         DefaultStreamerConfig(false),
	}
}

type StreamingSessionManagerConfig struct {
	SessionTimeout time.Duration `mapstructure:"session_timeout" validate:"required,gt=0"` // timeout until chunk upload session times out
}

func DefaultStreamingSessionManagerConfig() StreamingSessionManagerConfig {
	return StreamingSessionManagerConfig{
		SessionTimeout: 1 * time.Minute,
	}
}

type ParallelReplicationServiceConfig struct {
	ReplicateTimeout time.Duration `mapstructure:"replicate_timeout" validate:"required,gt=0"` // timeout until replication to another node is considered failed
}

func DefaultParallelReplicationServiceConfig() ParallelReplicationServiceConfig {
	return ParallelReplicationServiceConfig{
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
