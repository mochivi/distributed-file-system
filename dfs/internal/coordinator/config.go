package coordinator

import "time"

type CoordinatorConfig struct {
	Host        string
	Port        int
	ChunkSize   int // in MB
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
		Host:      "localhost",
		Port:      8080,
		ChunkSize: 8, // 8MB default chunksize
		Replication: ReplicationConfig{
			Factor: 3,
		},
		Metadata: MetadataConfig{
			CommitTimeout: 15 * time.Second,
		},
	}
}
