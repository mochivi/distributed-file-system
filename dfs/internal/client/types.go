package client

import (
	"github.com/mochivi/distributed-file-system/internal/coordinator"
)

// Client performs operations to reach coordinator or data nodes
type Client struct {
	coordinatorClient *coordinator.CoordinatorClient
}

func NewClient(coordinatorClient *coordinator.CoordinatorClient) *Client {
	return &Client{coordinatorClient: coordinatorClient}
}
