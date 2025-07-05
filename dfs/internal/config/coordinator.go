package config

import (
	"time"

	"github.com/google/uuid"
)

// CoordinatorAppConfig is the root configuration for the Coordinator service.
type CoordinatorAppConfig struct {
	Coordinator CoordinatorConfig      `mapstructure:"coordinator" validate:"required"`
	Agent       CoordinatorAgentConfig `mapstructure:"agent" validate:"required"`
}

func DefaultCoordinatorAppConfig() CoordinatorAppConfig {
	return CoordinatorAppConfig{
		Coordinator: DefaultCoordinatorConfig(),
		Agent:       DefaultCoordinatorAgentConfig(),
	}
}

// Configuration for the coordinator node itself
type CoordinatorConfig struct {
	ID          string            `mapstructure:"id"`
	Host        string            `mapstructure:"host" validate:"required,hostname_rfc1123"`
	Port        int               `mapstructure:"port" validate:"required,gt=0,lt=65536"`
	ChunkSize   int               `mapstructure:"chunk_size" validate:"required,gt=0"`
	Replication ReplicationConfig `mapstructure:"replication" validate:"required"`
	Metadata    MetadataConfig    `mapstructure:"metadata" validate:"required"`
}

func DefaultCoordinatorConfig() CoordinatorConfig {
	return CoordinatorConfig{
		ID:          uuid.NewString(),
		Host:        "localhost",
		Port:        8080,
		ChunkSize:   8 * 1024 * 1024, // 8MB default chunksize
		Replication: DefaultReplicationConfig(),
		Metadata:    DefaultMetadataConfig(),
	}
}

type MetadataConfig struct {
	CommitTimeout time.Duration `mapstructure:"commit_timeout" validate:"required,gt=0"`
}

func DefaultMetadataConfig() MetadataConfig {
	return MetadataConfig{
		CommitTimeout: 15 * time.Minute,
	}
}

type ReplicationConfig struct {
	Factor int `mapstructure:"factor" validate:"required,gte=1"`
}

func DefaultReplicationConfig() ReplicationConfig {
	return ReplicationConfig{
		Factor: 3,
	}
}
