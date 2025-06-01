package datanode

import "time"

type DataNodeConfig struct {
	SessionManagerConfig
}

type SessionManagerConfig struct {
	SessionTimeout time.Duration
}

type ReplicateManagerConfig struct {
	ReplicateTimeout time.Duration
	ChunkStreamSize  int
	MaxChunkRetries  int
}
