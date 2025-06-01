package datanode

import "time"

type DataNodeConfig struct {
	SessionManagerConfig
}

type SessionManagerConfig struct {
	SessionTimeout time.Duration
}
