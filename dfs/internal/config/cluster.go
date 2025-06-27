package config

import (
	"time"
)

// Control loops configuration
type ClusterNodeConfig struct {
	Heartbeat   *HeartbeatControllerConfig `mapstructure:"heartbeat" validate:"required"`
	NodeManager NodeManagerConfig          `mapstructure:"node_manager" validate:"required"`
}

func DefaultClusterNodeConfig() *ClusterNodeConfig {
	return &ClusterNodeConfig{
		Heartbeat:   DefaultHeartbeatControllerConfig(),
		NodeManager: DefaultNodeManagerConfig(),
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

type NodeManagerConfig struct {
	CoordinatorNodeManagerConfig CoordinatorNodeManagerConfig `mapstructure:"coordinator_node_manager"`
	DataNodeManagerConfig        DataNodeManagerConfig        `mapstructure:"data_node_manager"`
}

type CoordinatorNodeManagerConfig struct {
}

type DataNodeManagerConfig struct {
	MaxHistorySize int `mapstructure:"max_history_size" validate:"required,gt=0"` // max number of updates to keep in history
}

func DefaultDataNodeManagerConfig() DataNodeManagerConfig {
	return DataNodeManagerConfig{
		MaxHistorySize: 1000,
	}
}

func DefaultNodeManagerConfig() NodeManagerConfig {
	return NodeManagerConfig{
		CoordinatorNodeManagerConfig: CoordinatorNodeManagerConfig{},
		DataNodeManagerConfig:        DefaultDataNodeManagerConfig(),
	}
}
