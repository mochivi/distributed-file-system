package client

import (
	"log/slog"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/coordinator"
	"github.com/mochivi/distributed-file-system/pkg/logging"
)

// Client performs operations to reach coordinator or data nodes
type Client struct {
	coordinatorClient *coordinator.CoordinatorClient
	streamer          *common.Streamer
	logger            *slog.Logger
}

func NewClient(coordinatorClient *coordinator.CoordinatorClient, logger *slog.Logger) *Client {
	clientLogger := logging.ExtendLogger(logger, slog.String("component", "client"))
	streamer := common.NewStreamer(common.DefaultStreamerConfig())
	streamer.Config.WaitReplicas = true
	return &Client{coordinatorClient: coordinatorClient, streamer: streamer, logger: clientLogger}
}
