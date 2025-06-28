package config

import (
	"time"

	"github.com/mochivi/distributed-file-system/pkg/utils"
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

type ManagementAPIConfig struct {
	Host string `mapstructure:"host" validate:"required,hostname_rfc1123"`
	Port int    `mapstructure:"port" validate:"required,gt=1000,lt=65536"`
}

func DefaultManagementAPIConfig() *ManagementAPIConfig {
	return &ManagementAPIConfig{
		Host: utils.GetEnvString("MANAGEMENT_API_HOST", "localhost"),
		Port: utils.GetEnvInt("MANAGEMENT_API_PORT", 8000),
	}
}
