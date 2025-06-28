package client

import (
	"log/slog"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/pkg/logging"
)

// Client performs operations to reach coordinator or data nodes
type Client struct {
	coordinatorClient *clients.CoordinatorClient
	uploader          *Uploader
	downloader        *Downloader
	streamer          *common.Streamer
	logger            *slog.Logger
}

func NewClient(coordinatorClient *clients.CoordinatorClient, uploader *Uploader, downloader *Downloader, logger *slog.Logger) *Client {
	clientLogger := logging.ExtendLogger(logger, slog.String("component", "client"))
	streamer := common.NewStreamer(config.DefaultStreamerConfig())
	streamer.Config.WaitReplicas = true
	return &Client{coordinatorClient: coordinatorClient, streamer: streamer, logger: clientLogger, uploader: uploader, downloader: downloader}
}
