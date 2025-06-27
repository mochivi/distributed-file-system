package config

import (
	"time"

	"github.com/google/uuid"
)

type CoordinatorConfig struct {
	ID          string
	Host        string
	Port        int
	ChunkSize   int
	Replication ReplicationConfig
	Metadata    MetadataConfig
}

type MetadataConfig struct {
	CommitTimeout time.Duration
}

type ReplicationConfig struct {
	Factor int
}

func DefaultCoordinatorConfig() CoordinatorConfig {
	return CoordinatorConfig{
		ID:        uuid.NewString(),
		Host:      "localhost",
		Port:      8080,
		ChunkSize: 8 * 1024 * 1024, // 8MB default chunksize
		Replication: ReplicationConfig{
			Factor: 3,
		},
		Metadata: MetadataConfig{
			CommitTimeout: 15 * time.Minute,
		},
	}
}
