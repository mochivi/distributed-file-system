package client

import (
	"log/slog"

	"github.com/mochivi/distributed-file-system/internal/client/downloader"
	"github.com/mochivi/distributed-file-system/internal/client/uploader"
	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/streaming"
)

// Client performs operations to reach coordinator or data nodes
type Client struct {
	coordinatorClient clients.ICoordinatorClient
	uploader          *uploader.Uploader
	downloader        *downloader.Downloader
	streamer          streaming.ClientStreamer
	logger            *slog.Logger
}

func NewClient(coordinatorClient clients.ICoordinatorClient, uploader *uploader.Uploader, downloader *downloader.Downloader,
	streamer streaming.ClientStreamer, logger *slog.Logger) *Client {
	clientLogger := logging.ExtendLogger(logger, slog.String("component", "client"))

	return &Client{coordinatorClient: coordinatorClient, streamer: streamer, logger: clientLogger, uploader: uploader, downloader: downloader}
}
