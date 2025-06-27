package config

import (
	"time"
)

// Control loops configuration
type NodeAgentConfig struct {
	Heartbeat *HeartbeatControllerConfig `mapstructure:"heartbeat" validate:"required"`
}

func DefaultNodeAgentConfig() *NodeAgentConfig {
	return &NodeAgentConfig{
		Heartbeat: DefaultHeartbeatControllerConfig(),
	}
}

type HeartbeatControllerConfig struct {
	Interval time.Duration `mapstructure:"interval" validate:"required,gt=0"` // how often to send heartbeats to the coordinator
	Timeout  time.Duration `mapstructure:"timeout" validate:"required,gt=0"`  // how long to wait for a heartbeat response from the coordinator
}

func DefaultHeartbeatControllerConfig() *HeartbeatControllerConfig {
	return &HeartbeatControllerConfig{
		Interval: 30 * time.Second,
		Timeout:  10 * time.Second,
	}
}

type ClusterStateHistoryManagerConfig struct {
	MaxHistorySize int
}

func DefaultClusterStateHistoryManagerConfig() ClusterStateHistoryManagerConfig {
	return ClusterStateHistoryManagerConfig{
		MaxHistorySize: 1000,
	}
}
